//! The journal stores on the simulator's disk: the two contract suites, and
//! what damage the journal reports means to paros (faulty votes, crash
//! verdicts), the floor across reboots, and the format probe.
//!
//! The stores are provider-generic, so these run them over
//! `SimStorageProvider`, the disk the harness ships, stepped by hand beside
//! a current-thread runtime, exactly as `moonpool-journal`'s own tests do.
//! The journal's crash physics are `moonpool-journal`'s tests' and the
//! simulation's job.

use std::future::Future;
use std::net::IpAddr;

use moonpool_core::{OpenOptions, StorageFile, StorageProvider};
use moonpool_journal::Durability;
use moonpool_sim::{SimStorageProvider, SimWorld, StorageConfiguration};
use paros_core::{
    AcceptorConfig, Ballot, Command, Config, Entry, JournalId, JournalState, LeaderUuid, MustSync,
    NodeId, QuorumSystem, Registration, RegistryStorage, Seq, Slot, Storage, Value,
};

use super::{JournalMatchmakerStorage, JournalStorage, JournalStoreConfig, encode};
use crate::corruption::CorruptionVerdict;
use crate::matchmaker::{MatchmakerStorage, matchmaker_storage_contract_suite};
use crate::storage::{LogStorage, StorageError, StorageRecord, storage_contract_suite};

type Node = JournalStorage<SimStorageProvider>;
type Registry = JournalMatchmakerStorage<SimStorageProvider>;

fn ip() -> IpAddr {
    "10.0.0.1".parse().expect("valid IP")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}

fn sim(seed: u64) -> SimWorld {
    let mut sim = SimWorld::new_with_seed(seed);
    sim.set_storage_config(StorageConfiguration::fast_local());
    sim
}

/// Run `f` against `sim`'s storage, stepping the world until it finishes.
async fn run<F, Fut, T>(sim: &mut SimWorld, f: F) -> T
where
    F: FnOnce(SimStorageProvider) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handle = tokio::spawn(f(sim.storage_provider(ip())));
    while !handle.is_finished() {
        while sim.pending_event_count() > 0 {
            sim.step();
        }
        tokio::task::yield_now().await;
    }
    handle.await.expect("task panicked")
}

fn config() -> Config {
    Config {
        peers: vec![NodeId(0)],
        ..Config::new(NodeId(0), paros_core::JournalIdentifier::UNSET)
    }
}

fn store(durability: Durability) -> JournalStoreConfig {
    JournalStoreConfig {
        durability,
        ..JournalStoreConfig::small()
    }
}

const BOTH: [Durability; 2] = [Durability::Ordered, Durability::Batched];

async fn open_node(
    provider: SimStorageProvider,
    dir: &str,
    store: JournalStoreConfig,
) -> Result<Node, StorageError> {
    let mut node = JournalStorage::new(provider, dir, config(), store);
    node.boot_scan().await?;
    Ok(node)
}

async fn open_registry(provider: SimStorageProvider, dir: &str) -> Result<Registry, StorageError> {
    let mut registry = JournalMatchmakerStorage::new(
        provider,
        dir,
        paros_core::JournalIdentifier::UNSET,
        JournalStoreConfig::small(),
    );
    registry.boot_scan().await?;
    Ok(registry)
}

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(3),
    }
}

/// A command whose bytes are `byte` repeated, easy to find on disk.
fn user(seq: u64, byte: u8) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(7),
        seq: Seq(seq),
        records: vec![Value(vec![byte; 48])],
    })
}

#[test]
fn journal_storage_passes_the_contract_suite() {
    for durability in BOTH {
        runtime().block_on(async {
            let mut sim = sim(1);
            run(&mut sim, move |provider| async move {
                let mut instance = 0_u64;
                let fresh_provider = provider.clone();
                let fresh = move || {
                    instance += 1;
                    let provider = fresh_provider.clone();
                    let dir = format!("node-{instance}");
                    async move {
                        open_node(provider, &dir, store(durability))
                            .await
                            .expect("a fresh store opens")
                    }
                };
                let reopen = move |old: Node| {
                    let provider = provider.clone();
                    let dir = old.dir().to_string();
                    drop(old);
                    async move {
                        open_node(provider, &dir, store(durability))
                            .await
                            .expect("a clean store reopens")
                    }
                };
                Box::pin(storage_contract_suite(fresh, reopen)).await;
            })
            .await;
        });
    }
}

#[test]
fn journal_matchmaker_storage_passes_the_contract_suite() {
    runtime().block_on(async {
        let mut sim = sim(2);
        run(&mut sim, |provider| async move {
            let mut instance = 0_u64;
            let fresh_provider = provider.clone();
            let fresh = move || {
                instance += 1;
                let provider = fresh_provider.clone();
                let dir = format!("mm-{instance}");
                async move { open_registry(provider, &dir).await.expect("opens") }
            };
            let reopen = move |old: Registry| {
                let provider = provider.clone();
                let dir = old.dir().to_string();
                drop(old);
                async move { open_registry(provider, &dir).await.expect("reopens") }
            };
            Box::pin(matchmaker_storage_contract_suite(fresh, reopen)).await;
        })
        .await;
    });
}

fn is_segment(name: &str) -> bool {
    std::path::Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("wal"))
}

/// Flip one byte in the middle of the first occurrence of `needle` in any
/// segment file of `dir`: bit rot inside one specific entry. Returns
/// whether it hit.
async fn rot(provider: &SimStorageProvider, dir: &str, needle: &[u8]) -> bool {
    let mut names = provider.list_dir(dir).await.expect("list");
    names.sort();
    for name in names.into_iter().filter(|name| is_segment(name)) {
        let path = format!("{dir}/{name}");
        let file = provider
            .open(&path, OpenOptions::read_write())
            .await
            .expect("open segment");
        let size = usize::try_from(file.size().await.expect("size")).expect("small");
        let mut bytes = vec![0; size];
        file.read_at(0, &mut bytes).await.expect("read segment");
        if let Some(at) = bytes
            .windows(needle.len())
            .position(|window| window == needle)
        {
            let middle = at + needle.len() / 2;
            file.write_at(middle as u64, &[!bytes[middle]])
                .await
                .expect("flip");
            file.sync_all().await.expect("sync flip");
            return true;
        }
    }
    false
}

#[test]
fn a_damaged_accepted_entry_is_reported_faulty_mid_log_and_at_the_tail() {
    for durability in BOTH {
        runtime().block_on(async {
            let mut sim = sim(3);
            run(&mut sim, move |provider| async move {
                let store = store(durability);
                let mut node = open_node(provider.clone(), "n", store).await.expect("open");
                node.format(&config()).await.expect("format");
                node.persist_ballot(ballot(5)).await.expect("promise");
                for (slot, byte) in [(0, 0xA1), (1, 0xA2), (2, 0xA3)] {
                    node.append_accepted(Slot(slot), ballot(5), user(slot, byte))
                        .await
                        .expect("accept");
                    node.sync(MustSync::Sync).await.expect("sync");
                }
                drop(node);
                // Slot 1 rots mid-log; slot 2 is in the last batch: corrupt
                // when ordered, ambiguous when batched, faulty either way.
                assert!(rot(&provider, "n", &[0xA2; 48]).await);
                assert!(rot(&provider, "n", &[0xA3; 48]).await);
                let node = open_node(provider.clone(), "n", store)
                    .await
                    .expect("reboots");
                assert_eq!(
                    node.faulty_entries(),
                    vec![(Slot(1), ballot(5)), (Slot(2), ballot(5))],
                    "both damaged votes are reported with their identity"
                );
                assert!(
                    node.accepted(Slot(1)).is_none(),
                    "a faulty slot has no value"
                );
                assert_eq!(node.accepted(Slot(0)).map(|(b, _)| b), Some(ballot(5)));
                // The identity survives the next reboot too, and a repair
                // clears it.
                let mut node = open_node(provider.clone(), "n", store)
                    .await
                    .expect("reboots again");
                assert_eq!(node.faulty_entries().len(), 2, "kept across reboots");
                node.append_accepted(Slot(2), ballot(5), user(2, 0xB3))
                    .await
                    .expect("repair");
                node.sync(MustSync::Sync).await.expect("sync repair");
                let node = open_node(provider, "n", store)
                    .await
                    .expect("reboots repaired");
                assert_eq!(node.faulty_entries(), vec![(Slot(1), ballot(5))]);
                assert_eq!(node.accepted(Slot(2)).map(|(_, c)| c), Some(user(2, 0xB3)));
            })
            .await;
        });
    }
}

/// Slots arrive in any order, and the floor, the sealed state, the promise
/// and the chosen index come back after a reboot.
#[test]
fn laggy_slots_and_the_floor_survive_a_reboot() {
    runtime().block_on(async {
        let mut sim = sim(4);
        run(&mut sim, |provider| async move {
            let store = JournalStoreConfig::small();
            let mut node = open_node(provider.clone(), "f", store).await.expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(9)).await.expect("promise");
            for slot in [7, 2, 30, 5, 1] {
                node.append_accepted(Slot(slot), ballot(9), user(slot, 0x10))
                    .await
                    .expect("accept");
                node.sync(MustSync::Sync).await.expect("sync");
            }
            let sealed = JournalState {
                next_seq: Seq(4),
                ..JournalState::default()
            };
            node.set_chosen_index(Slot(7)).await.expect("chosen");
            node.truncate(Slot(3), sealed).await.expect("truncate");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            let node = open_node(provider, "f", store).await.expect("reboots");
            assert_eq!(node.first_slot(), Slot(3));
            assert_eq!(node.sealed_state(), sealed);
            let (hard, _) = node.initial_state();
            assert_eq!(hard.max_promised_ballot, ballot(9));
            assert_eq!(
                hard.chosen_index,
                Some(Slot(7)),
                "rode the truncation's metainfo"
            );
            assert!(node.accepted(Slot(2)).is_none(), "below the floor");
            assert!(node.accepted(Slot(5)).is_some() && node.accepted(Slot(30)).is_some());
            assert_eq!(node.last_slot(), Slot(30));
        })
        .await;
    });
}

/// A re-sent `Accept` the store already holds writes nothing; the same slot
/// at a new ballot, or with another command, is written.
#[test]
fn a_held_entry_re_accepted_writes_nothing() {
    runtime().block_on(async {
        let mut sim = sim(6);
        run(&mut sim, |provider| async move {
            let store = JournalStoreConfig::small();
            let mut node = open_node(provider.clone(), "re", store)
                .await
                .expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(2)).await.expect("promise");
            node.append_accepted(Slot(1), ballot(2), user(1, 0x31))
                .await
                .expect("accept");
            node.sync(MustSync::Sync).await.expect("sync");
            node.append_accepted(Slot(1), ballot(2), user(1, 0x31))
                .await
                .expect("re-accept");
            assert_eq!(node.staged_entries(), 0, "a held entry stages nothing");
            node.append_accepted(Slot(1), ballot(2), user(1, 0x32))
                .await
                .expect("another command");
            assert_eq!(node.staged_entries(), 1, "another command is written");
            node.sync(MustSync::Sync).await.expect("sync");
            node.persist_ballot(ballot(3)).await.expect("promise");
            node.append_accepted(Slot(1), ballot(3), user(1, 0x32))
                .await
                .expect("a higher ballot");
            assert_eq!(node.staged_entries(), 1, "a new ballot is written");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            let node = open_node(provider, "re", store).await.expect("reboots");
            assert_eq!(node.accepted(Slot(1)), Some((ballot(3), user(1, 0x32))));
        })
        .await;
    });
}

/// One sync can stage more entries than one segment holds (a node catching
/// up a long log after a reboot): it lands as several commits, and the
/// floor and the metainfo, in the last, survive the reboot with every entry.
#[test]
fn a_sync_larger_than_one_segment_lands_in_several_commits() {
    runtime().block_on(async {
        for durability in BOTH {
            let mut sim = sim(5);
            run(&mut sim, |provider| async move {
                let store = store(durability);
                let mut node = open_node(provider.clone(), "big", store)
                    .await
                    .expect("open");
                node.format(&config()).await.expect("format");
                node.sync(MustSync::Sync).await.expect("sync");
                node.persist_ballot(ballot(4)).await.expect("promise");
                // Three times the small shape's 512 records per segment.
                for slot in 0..1500 {
                    node.append_accepted(Slot(slot), ballot(4), user(slot, 0x22))
                        .await
                        .expect("accept");
                }
                let sealed = JournalState {
                    next_seq: Seq(10),
                    ..JournalState::default()
                };
                node.set_chosen_index(Slot(1400)).await.expect("chosen");
                node.truncate(Slot(10), sealed).await.expect("truncate");
                node.sync(MustSync::Sync)
                    .await
                    .expect("a sync past one segment commits");
                drop(node);
                let node = open_node(provider, "big", store).await.expect("reboots");
                let (hard, _) = node.initial_state();
                assert_eq!(hard.max_promised_ballot, ballot(4), "{durability:?}");
                assert_eq!(hard.chosen_index, Some(Slot(1400)), "{durability:?}");
                assert_eq!(node.first_slot(), Slot(10), "{durability:?}");
                assert_eq!(node.sealed_state(), sealed, "{durability:?}");
                assert!(node.accepted(Slot(9)).is_none(), "below the floor");
                assert!((10..1500).all(|s| node.accepted(Slot(s)).is_some()));
                assert_eq!(node.last_slot(), Slot(1499));
            })
            .await;
        }
    });
}

/// The format probe reads the marker without opening the store: nothing
/// where there is no journal (and nothing created), the marker once a
/// format is synced.
#[test]
fn peek_formatted_reads_the_marker_without_opening_the_store() {
    runtime().block_on(async {
        let mut sim = sim(6);
        run(&mut sim, |provider| async move {
            let id = paros_core::JournalIdentifier::UNSET;
            assert!(
                !Node::peek_formatted(&provider, "p", id)
                    .await
                    .expect("peek")
            );
            assert!(
                !provider.exists("p").await.expect("exists"),
                "nothing created"
            );
            let store = JournalStoreConfig::small();
            let mut node = open_node(provider.clone(), "p", store).await.expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(5)).await.expect("promise");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            assert!(
                Node::peek_formatted(&provider, "p", id)
                    .await
                    .expect("peek")
            );
        })
        .await;
    });
}

/// A store whose segment files are gone while its metainfo says batches were
/// durable is acknowledged history lost: a crash verdict, never an empty log.
#[test]
fn a_lost_segment_is_a_crash_verdict() {
    runtime().block_on(async {
        let mut sim = sim(5);
        run(&mut sim, |provider| async move {
            let store = JournalStoreConfig::small();
            let mut node = open_node(provider.clone(), "g", store).await.expect("open");
            node.format(&config()).await.expect("format");
            for slot in 0..4 {
                node.append_accepted(Slot(slot), ballot(1), user(slot, 0x20))
                    .await
                    .expect("accept");
                node.sync(MustSync::Sync).await.expect("sync");
            }
            node.persist_ballot(ballot(2)).await.expect("promise");
            node.sync(MustSync::Sync).await.expect("sync");
            drop(node);
            for name in provider.list_dir("g").await.expect("list") {
                if is_segment(&name) {
                    provider.delete(&format!("g/{name}")).await.expect("delete");
                }
            }
            match open_node(provider, "g", store).await {
                Err(StorageError::Corruption { verdict, .. }) => {
                    assert_eq!(verdict, CorruptionVerdict::Corrupted);
                }
                other => panic!("a lost log must not boot: {other:?}"),
            }
        })
        .await;
    });
}

fn belief(first: u64) -> Registration {
    Registration::belief(AcceptorConfig::new(
        (0..3).map(|n| NodeId(n + first)).collect::<Vec<_>>(),
        QuorumSystem::Majority,
    ))
}

/// The journal the registry tests register in.
const J: JournalId = JournalId(1);

/// The payload bytes of a registration, to find it on disk: its
/// registration bytes, which follow its journal in the payload.
fn needle(registration: &Registration) -> Vec<u8> {
    // Past the format byte: inside the payload the registration follows the
    // journal, with no format byte of its own.
    encode(registration)[1..].to_vec()
}

#[test]
fn a_damaged_registration_is_a_crash_and_a_collected_one_is_not() {
    runtime().block_on(async {
        let mut sim = sim(7);
        run(&mut sim, |provider| async move {
            let mut registry = open_registry(provider.clone(), "m").await.expect("open");
            for (round, first) in [(1, 0xC100), (2, 0xD200), (3, 0xE300)] {
                registry
                    .register(J, ballot(round), &belief(first))
                    .await
                    .expect("register");
                registry.sync().await.expect("sync");
            }
            drop(registry);
            assert!(rot(&provider, "m", &needle(&belief(0xD200))).await);
            match open_registry(provider.clone(), "m").await {
                Err(StorageError::Corruption {
                    record, verdict, ..
                }) => {
                    assert_eq!(record, StorageRecord::Registration(ballot(2)));
                    assert_eq!(verdict, CorruptionVerdict::Corrupted);
                }
                other => panic!("a damaged live registration must crash: {other:?}"),
            }
            // Collected below the watermark, the same damage is harmless.
            let mut other = open_registry(provider.clone(), "m2").await.expect("open");
            for (round, first) in [(1, 0xC100), (2, 0xD200)] {
                other
                    .register(J, ballot(round), &belief(first))
                    .await
                    .expect("register");
                other.sync().await.expect("sync");
            }
            drop(other);
            assert!(rot(&provider, "m2", &needle(&belief(0xD200))).await);
            assert!(
                open_registry(provider.clone(), "m2").await.is_err(),
                "live until collected"
            );
        })
        .await;
    });
}

#[test]
fn a_registration_below_the_watermark_is_collected_even_when_damaged() {
    runtime().block_on(async {
        let mut sim = sim(8);
        run(&mut sim, |provider| async move {
            let mut registry = open_registry(provider.clone(), "w").await.expect("open");
            for (round, first) in [(1, 0xC100), (2, 0xD200), (4, 0xF400)] {
                registry
                    .register(J, ballot(round), &belief(first))
                    .await
                    .expect("register");
                registry.sync().await.expect("sync");
            }
            registry
                .set_gc_watermark(J, ballot(3))
                .await
                .expect("raise");
            registry.sync().await.expect("sync");
            drop(registry);
            // The cleared registration's bytes may still be on disk; rot is
            // harmless there, and the live one reads back.
            let _ = rot(&provider, "w", &needle(&belief(0xD200))).await;
            let registry = open_registry(provider, "w").await.expect("boots");
            assert_eq!(registry.registered(), vec![(J, ballot(4))]);
            assert_eq!(registry.initial_state().gc_watermark(J), ballot(3));
        })
        .await;
    });
}
