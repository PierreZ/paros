//! The journal stores on a real filesystem (#206): the two contract suites
//! `paros` runs over the simulated disk, here over Tokio's storage provider
//! in a temporary directory, and a restart loop that drops a store
//! mid-batch, the way a killed process leaves it.
//!
//! The crash loops of `paros::journal`'s own tests stay on the simulated
//! disk: their fault models (unsynced sectors resolved at a crash, torn and
//! misdirected writes) need a disk the test can crash, which a real one is
//! not. What a real filesystem adds is the real thing underneath — files,
//! directories, `fsync`, rename — and that is what these exercise.

use moonpool_core::TokioStorageProvider;
use paros::journal::Durability;
use paros::{
    Ballot, Command, Config, Entry, JournalMatchmakerStorage, JournalStorage, JournalStoreConfig,
    LeaderUuid, LogStorage, MatchmakerStorage, MustSync, NodeId, Seq, Slot, Storage, Value,
    matchmaker_storage_contract_suite, storage_contract_suite,
};

type Node = JournalStorage<TokioStorageProvider>;
type Registry = JournalMatchmakerStorage<TokioStorageProvider>;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn config() -> Config {
    Config {
        peers: vec![NodeId(0)],
        ..Config::new(NodeId(0), paros::JournalIdentifier::UNSET)
    }
}

/// A small layout with a short checkpoint cadence, so the suites cross
/// segment rollovers, checkpoints and prefix drops on the real disk.
fn layout(durability: Durability) -> JournalStoreConfig {
    JournalStoreConfig {
        durability,
        ..JournalStoreConfig::small()
    }
}

fn dir(root: &tempfile::TempDir, name: &str) -> String {
    root.path().join(name).to_string_lossy().into_owned()
}

async fn open_node(dir: &str, store: JournalStoreConfig) -> Node {
    let mut node = JournalStorage::new(TokioStorageProvider::new(), dir, config(), store);
    node.boot_scan()
        .await
        .expect("a store on a real disk opens");
    node
}

async fn open_registry(dir: &str) -> Registry {
    let mut registry = JournalMatchmakerStorage::new(
        TokioStorageProvider::new(),
        dir,
        paros::JournalIdentifier::UNSET,
        layout(Durability::Batched),
    );
    registry
        .boot_scan()
        .await
        .expect("a registry on a real disk opens");
    registry
}

#[test]
fn journal_storage_passes_the_contract_suite_on_a_real_disk() {
    for durability in [Durability::Ordered, Durability::Batched] {
        let root = tempfile::tempdir().expect("tempdir");
        runtime().block_on(async {
            let mut instance = 0_u64;
            let fresh = || {
                instance += 1;
                let dir = dir(&root, &format!("node-{instance}"));
                async move { open_node(&dir, layout(durability)).await }
            };
            let reopen = |old: Node| {
                let dir = old.dir().to_string();
                drop(old);
                async move { open_node(&dir, layout(durability)).await }
            };
            Box::pin(storage_contract_suite(fresh, reopen)).await;
        });
    }
}

#[test]
fn journal_matchmaker_storage_passes_the_contract_suite_on_a_real_disk() {
    let root = tempfile::tempdir().expect("tempdir");
    runtime().block_on(async {
        let mut instance = 0_u64;
        let fresh = || {
            instance += 1;
            let dir = dir(&root, &format!("mm-{instance}"));
            async move { open_registry(&dir).await }
        };
        let reopen = |old: Registry| {
            let dir = old.dir().to_string();
            drop(old);
            async move { open_registry(&dir).await }
        };
        Box::pin(matchmaker_storage_contract_suite(fresh, reopen)).await;
    });
}

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

fn user(seq: u64) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(7),
        seq: Seq(seq),
        records: vec![Value(seq.to_le_bytes().to_vec())],
    })
}

/// A restart loop on the real disk: every round syncs one batch (the
/// acknowledged one), stages a second it never syncs, and drops the store —
/// a process killed between a write and its `fsync`. The reopened store
/// holds every acknowledged record, the format marker and the promise, and
/// nothing it was never asked to hold; across rounds the checkpoints drop
/// whole segments and the log still folds to the same state.
#[test]
fn a_store_dropped_mid_batch_reopens_with_every_acknowledged_write() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = dir(&root, "node");
    runtime().block_on(async {
        let store = layout(Durability::Ordered);
        let mut node = open_node(&path, store).await;
        node.format(&config()).await.expect("format");
        node.sync(MustSync::Sync).await.expect("sync format");
        let mut acknowledged: Vec<u64> = Vec::new();
        for round in 0..40_u64 {
            let slot = round * 2;
            node.persist_ballot(ballot(round + 1))
                .await
                .expect("ballot");
            node.append_accepted(Slot(slot), ballot(round + 1), user(slot))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            acknowledged.push(slot);
            // Staged, never synced: the kill lands before the fsync.
            node.append_accepted(Slot(slot + 1), ballot(round + 1), user(slot + 1))
                .await
                .expect("stage");
            drop(node);
            node = open_node(&path, store).await;
            assert_eq!(
                node.formatted_config(),
                Some(config()),
                "the marker and its configuration survive every restart"
            );
            assert_eq!(
                node.initial_state().0.max_promised_ballot,
                ballot(round + 1),
                "the synced promise survives"
            );
            for &slot in &acknowledged {
                assert_eq!(
                    node.accepted(Slot(slot)),
                    Some((ballot(slot / 2 + 1), user(slot))),
                    "acknowledged slot {slot} survives the restart"
                );
            }
            assert_eq!(
                node.accepted(Slot(slot + 1)),
                None,
                "a write never synced is not reported as accepted"
            );
            assert!(
                node.faulty_entries().is_empty(),
                "a clean kill leaves nothing damaged"
            );
        }
    });
}

/// The machine record on a real disk (#246): absent, then read back whole,
/// with no staged file left behind; a journals directory without a record
/// is a machine that lost its identity.
#[test]
fn the_machine_record_is_absent_then_read_back_whole() {
    let root = tempfile::tempdir().expect("tempdir");
    let disk = paros::machine::ProviderDisk::new(
        TokioStorageProvider::new(),
        dir(&root, "machine"),
        JournalStoreConfig::small(),
    );
    runtime().block_on(async {
        assert_eq!(disk.read_record().await, Ok(None));
        assert!(!disk.holds_journals().await);
        disk.write_record("node_id 3\n").await.expect("written");
        assert_eq!(disk.read_record().await, Ok(Some("node_id 3\n".into())));
        assert!(!root.path().join("machine/machine.tmp").exists());
        std::fs::create_dir_all(root.path().join("machine/journals")).expect("mkdir");
        assert!(
            disk.holds_journals().await,
            "a store without a record is amnesia"
        );
    });
}
