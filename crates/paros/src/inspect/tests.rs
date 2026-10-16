//! The inspection reads stores the shipped code wrote on the simulator's
//! disk, and writes nothing there (#365).

use std::collections::BTreeMap;
use std::future::Future;
use std::net::IpAddr;

use moonpool_core::{OpenOptions, StorageProvider};
use moonpool_sim::{SimStorageProvider, SimWorld, StorageConfiguration};
use paros_core::{
    Ballot, Command, Config, Control, Entry, JournalId, JournalIdentifier, JournalState,
    LeaderUuid, MatchmakerConfig, MatchmakerId, MustSync, NodeId, Seq, Slot, TenantId, Value,
};

use super::{inspect, names};
use crate::machine::{CellPlan, Class, MachineRecord, read_all, write_all};
use crate::matchmaker::MatchmakerStorage;
use crate::storage::LogStorage;
use crate::system::SystemCommand;
use crate::{Address, JournalMatchmakerStorage, JournalStorage, JournalStoreConfig};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}

/// Run `f` against a simulated disk, stepping the world until it finishes.
async fn run<F, Fut, T>(seed: u64, f: F) -> T
where
    F: FnOnce(SimStorageProvider) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut sim = SimWorld::new_with_seed(seed);
    sim.set_storage_config(StorageConfiguration::fast_local());
    let ip: IpAddr = "10.0.0.1".parse().expect("valid IP");
    let handle = tokio::spawn(f(sim.storage_provider(ip)));
    while !handle.is_finished() {
        while sim.pending_event_count() > 0 {
            sim.step();
        }
        tokio::task::yield_now().await;
    }
    handle.await.expect("task panicked")
}

const ME: NodeId = NodeId(0xA11CE);
const CONTROL: JournalIdentifier = JournalIdentifier {
    tenant: TenantId(71),
    journal: JournalId(72),
};
const ELECTION: JournalIdentifier = JournalIdentifier {
    tenant: TenantId(71),
    journal: JournalId(73),
};

fn ballot(round: u64) -> Ballot {
    Ballot { round, node: ME }
}

/// Every file under `dir`, recursively, with its bytes.
async fn snapshot(provider: &SimStorageProvider, dir: &str) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut todo = vec![dir.to_string()];
    while let Some(dir) = todo.pop() {
        for name in provider.list_dir(&dir).await.expect("list") {
            let path = format!("{dir}/{name}");
            if provider.list_dir(&path).await.is_ok() {
                todo.push(path);
            } else {
                let file = provider
                    .open(&path, OpenOptions::read_only())
                    .await
                    .expect("open");
                files.insert(path, read_all(&file).await.expect("read"));
            }
        }
    }
    files
}

/// Write the journal store of `journal` under `disk` with the shipped
/// store: formatted, a promise, `commands` accepted from slot 0, chosen up
/// to `chosen`.
async fn write_store(
    provider: &SimStorageProvider,
    disk: &str,
    journal: JournalIdentifier,
    commands: &[Command],
    chosen: Option<u64>,
) {
    let dir = format!("{disk}/journals/{}/{}", journal.tenant.0, journal.journal.0);
    let config = Config {
        peers: vec![ME],
        ..Config::new(ME, journal)
    };
    let mut store = JournalStorage::new(
        provider.clone(),
        dir,
        config.clone(),
        JournalStoreConfig::small(),
    );
    store.boot_scan().await.expect("boot");
    store.format(&config).await.expect("format");
    store.persist_ballot(ballot(4)).await.expect("promise");
    for (slot, command) in (0..).zip(commands) {
        store
            .append_accepted(Slot(slot), ballot(4), command.clone())
            .await
            .expect("accept");
    }
    if let Some(chosen) = chosen {
        store.set_chosen_index(Slot(chosen)).await.expect("chosen");
    }
    store.sync(MustSync::Sync).await.expect("sync");
}

/// Accept `command` at `slot` in the store of `journal` under `disk`,
/// written by [`write_store`] before.
async fn append(
    provider: &SimStorageProvider,
    disk: &str,
    journal: JournalIdentifier,
    slot: u64,
    command: Command,
) {
    let dir = format!("{disk}/journals/{}/{}", journal.tenant.0, journal.journal.0);
    let config = Config {
        peers: vec![ME],
        ..Config::new(ME, journal)
    };
    let mut store = JournalStorage::new(provider.clone(), dir, config, JournalStoreConfig::small());
    store.boot_scan().await.expect("boot");
    store
        .append_accepted(Slot(slot), ballot(4), command)
        .await
        .expect("accept");
    store.sync(MustSync::Sync).await.expect("sync");
}

fn claim() -> Command {
    Command::Control(Control::SetLeader {
        new: LeaderUuid(9),
        old: None,
    })
}

fn write(seq: u64, record: Vec<u8>) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(9),
        seq: Seq(seq),
        records: vec![Value(record)],
    })
}

/// A store's marker, scalars and slots come back, and the source disk is
/// byte for byte what it was: the open ran on the scratch copy.
#[test]
fn inspection_reads_a_store_and_writes_nothing() {
    runtime().block_on(run(1, |provider| async move {
        let journal = JournalIdentifier::new(TenantId(5), JournalId(6));
        let commands = [
            claim(),
            write(0, b"one".to_vec()),
            write(1, b"two".to_vec()),
        ];
        write_store(&provider, "disk", journal, &commands, Some(1)).await;
        let set = JournalIdentifier::new(TenantId(5), JournalId::UNSET);
        let mut mm = JournalMatchmakerStorage::new(
            provider.clone(),
            "disk/matchmakers/5",
            set,
            JournalStoreConfig::small(),
        );
        mm.boot_scan().await.expect("boot");
        let mm_config = MatchmakerConfig {
            id: MatchmakerId(ME.0),
            bootstrap: vec![MatchmakerId(ME.0)],
        };
        mm.format(&mm_config).await.expect("format");
        mm.sync().await.expect("sync");
        drop(mm);

        let before = snapshot(&provider, "disk").await;
        let image = inspect(
            &provider,
            "disk",
            JournalStoreConfig::small(),
            &provider,
            "scratch",
        )
        .await
        .expect("inspects");
        assert_eq!(snapshot(&provider, "disk").await, before, "nothing written");

        assert!(image.record.is_none(), "no machine record on this disk");
        assert_eq!(image.journals.len(), 1);
        assert_eq!(image.journals[0].journal, journal);
        let store = image.journals[0].store.as_ref().expect("reads");
        assert_eq!(store.formatted.as_ref().map(|c| c.journal), Some(journal));
        assert_eq!(store.promise, ballot(4));
        assert_eq!(store.chosen, Some(Slot(1)));
        assert_eq!(store.floor, Slot(0));
        assert_eq!(store.sealed, JournalState::default());
        assert!(store.faulty.is_empty());
        let slots: Vec<Slot> = store.accepted.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(slots, [Slot(0), Slot(1), Slot(2)]);
        assert_eq!(store.accepted[2].2, commands[2]);

        assert_eq!(image.matchmakers.len(), 1);
        assert_eq!(image.matchmakers[0].tenant, TenantId(5));
        let set = image.matchmakers[0].store.as_ref().expect("reads");
        assert_eq!(set.formatted, Some(mm_config));
        assert!(set.registered.is_empty());
        assert!(names(&image).is_none(), "no record names a cell");
    }));
}

/// A founding member's disk names its machines from its copy of the
/// registry, folded over its accepted prefix: the walk stops at the first
/// slot the store lacks.
#[test]
fn names_come_from_the_registry_prefix() {
    runtime().block_on(run(2, |provider| async move {
        let register = |id: NodeId, name: &str| {
            SystemCommand::RegisterNode {
                id,
                addr: "10.0.0.1:4500".to_string(),
                class: Class::Storage,
                capacity: 4,
                locality: crate::machine::Locality::zone("zone-a"),
                name: name.to_string(),
                incarnation: 0,
                machine_interface: Vec::new(),
            }
            .encode()
        };
        let other = NodeId(0xB0B);
        let third = NodeId(0xC0C);
        // No chosen index (a clean stop leaves it behind): the prefix still
        // names. Slot 3 is missing, so slot 4's registration names nothing.
        let commands = [
            claim(),
            write(0, register(ME, "alpha")),
            write(1, register(other, "beta")),
        ];
        write_store(&provider, "disk", CONTROL, &commands, None).await;
        append(
            &provider,
            "disk",
            CONTROL,
            4,
            write(2, register(third, "gamma")),
        )
        .await;
        write_store(&provider, "disk", ELECTION, &[], None).await;
        let addr = |a: &str| Address::parse(a).expect("address");
        let record = MachineRecord {
            node_id: ME,
            class: Class::Storage,
            capacity: 4,
            locality: crate::machine::Locality::zone("zone-a"),
            name: "alpha".to_string(),
            promised: ballot(1),
            plan: Some((
                ballot(1),
                CellPlan {
                    cell_id: 77,
                    members: vec![(other, addr("10.0.0.2:4500")), (ME, addr("10.0.0.1:4500"))],
                    control: CONTROL,
                    fleet: None,
                    election: ELECTION,
                    journals: vec![CONTROL, ELECTION],
                },
            )),
            admitted: None,
            created: Vec::new(),
        };
        let file = provider
            .open("disk/machine", OpenOptions::create_write())
            .await
            .expect("record");
        write_all(&file, record.render().as_bytes())
            .await
            .expect("write");
        drop(file);

        let image = inspect(
            &provider,
            "disk",
            JournalStoreConfig::small(),
            &provider,
            "scratch",
        )
        .await
        .expect("inspects");
        assert!(
            matches!(image.record, Some(Ok(_))),
            "the record reads: {:?}",
            image.record
        );
        let view = names(&image).expect("the registry is on this disk");
        assert_eq!(view.cell_id, 77);
        let named: BTreeMap<u64, String> = view
            .machines
            .iter()
            .map(|m| (m.node_id, m.name.clone()))
            .collect();
        assert_eq!(named.get(&ME.0).map(String::as_str), Some("alpha"));
        assert_eq!(named.get(&other.0).map(String::as_str), Some("beta"));
        assert!(
            !named.values().any(|n| n == "gamma"),
            "a slot past a hole names nothing"
        );
    }));
}
