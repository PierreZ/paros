//! The journal stores on the simulator's disk: the two contract suites, the
//! corruption table's rows as targeted damage, checkpoints, and a crash loop
//! under two fault models.
//!
//! The stores are provider-generic, so these run them over
//! `SimStorageProvider` — the disk the harness ships — stepped by hand
//! beside a current-thread runtime, exactly as `moonpool-journal`'s own
//! tests do.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::IpAddr;

use moonpool_core::{OpenOptions, StorageFile, StorageProvider};
use moonpool_sim::{SimStorageProvider, SimWorld, StorageConfiguration};
use paros_core::{
    AcceptorConfig, Ballot, ClientId, Command, Config, Entry, Generation, JournalState, MustSync,
    NodeId, QuorumSystem, Registration, RegistryStorage, Seq, Slot, Storage, Value,
};

use super::{JournalMatchmakerStorage, JournalStorage, JournalStoreConfig};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
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

/// A small layout with a short checkpoint cadence, so the suites cross
/// segment rollovers, checkpoints and prefix drops.
fn small(checkpoint_after: u64) -> JournalStoreConfig {
    JournalStoreConfig {
        checkpoint_after,
        ..JournalStoreConfig::small()
    }
}

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
    let mut registry = JournalMatchmakerStorage::new(provider, dir, small(16));
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
        generation: Generation(0),
        owner: ClientId(7),
        seq: Seq(seq),
        records: vec![Value(vec![byte; 48])],
    })
}

#[test]
fn journal_storage_passes_the_contract_suite() {
    for checkpoint_after in [1, 4, 1_000] {
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
                        open_node(provider, &dir, small(checkpoint_after))
                            .await
                            .expect("a fresh store opens")
                    }
                };
                let reopen = move |old: Node| {
                    let provider = provider.clone();
                    let dir = old.dir().to_string();
                    drop(old);
                    async move {
                        open_node(provider, &dir, small(checkpoint_after))
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

/// Flip one byte of the first occurrence of `needle` in any segment file of
/// `dir` — bit rot inside one specific record. Returns whether it hit.
async fn rot(provider: &SimStorageProvider, dir: &str, needle: &[u8]) -> bool {
    let mut names = provider.list_dir(dir).await.expect("list");
    names.sort();
    for name in names.into_iter().filter(|name| {
        std::path::Path::new(name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("wal"))
    }) {
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
            let offset = (at + needle.len() / 2) as u64;
            file.write_at(offset, &[!bytes[at + needle.len() / 2]])
                .await
                .expect("flip");
            file.sync_all().await.expect("sync flip");
            return true;
        }
    }
    false
}

/// Flip one payload byte of the entry at `index` of the journal's first
/// segment, found through its slot (v2 layout: slots of 64 bytes after two
/// header blocks, the entry's offset at bytes 16..20, a 64-byte entry
/// header before the payload).
async fn rot_entry(provider: &SimStorageProvider, dir: &str, index: u64) {
    let path = format!("{dir}/seg-{:020}.wal", super::GENESIS);
    let file = provider
        .open(&path, OpenOptions::read_write())
        .await
        .expect("open segment");
    let slot_at = 8192 + 64 * (index - super::GENESIS);
    let mut offset = [0; 4];
    file.read_at(slot_at + 16, &mut offset)
        .await
        .expect("read slot");
    let at = u64::from(u32::from_le_bytes(offset)) + 64 + 1;
    let mut byte = [0; 1];
    file.read_at(at, &mut byte).await.expect("read entry");
    file.write_at(at, &[!byte[0]]).await.expect("flip");
    file.sync_all().await.expect("sync flip");
}

#[test]
fn a_damaged_accepted_record_is_reported_faulty_mid_log_and_at_the_tail() {
    runtime().block_on(async {
        let mut sim = sim(3);
        run(&mut sim, |provider| async move {
            let store = small(1_000);
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
            // Slot 1 rots mid-log; slot 2 is the log's last entry — the
            // ambiguous row, kept rather than truncated.
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
            // The identity survives the next crash too, and a repair clears it.
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

/// A segment missing between the start and the tail is acknowledged
/// history gone, never the end of the log: the store refuses to boot with
/// a lost-write verdict (moonpool's `SegmentGap`).
#[test]
fn a_missing_middle_segment_is_a_lost_write() {
    runtime().block_on(async {
        let mut sim = sim(4);
        run(&mut sim, |provider| async move {
            let store = small(1_000);
            let mut node = open_node(provider.clone(), "g", store).await.expect("open");
            node.format(&config()).await.expect("format");
            node.persist_ballot(ballot(5)).await.expect("promise");
            let mut segments = Vec::new();
            for slot in 0..400_u64 {
                node.append_accepted(Slot(slot), ballot(5), user(slot, 0xC0))
                    .await
                    .expect("accept");
                node.sync(MustSync::Sync).await.expect("sync");
                segments = provider.list_dir("g").await.expect("list");
                segments.retain(|name| {
                    std::path::Path::new(name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("wal"))
                });
                if segments.len() >= 3 {
                    break;
                }
            }
            assert!(segments.len() >= 3, "the log spans three segments");
            drop(node);
            segments.sort();
            provider
                .delete(&format!("g/{}", segments[1]))
                .await
                .expect("delete the middle segment");
            provider.sync_dir("g").await.expect("sync dir");
            assert!(matches!(
                open_node(provider, "g", store).await,
                Err(StorageError::Corruption {
                    record: StorageRecord::Store,
                    fault: IntegrityFault::LostWrite,
                    verdict: CorruptionVerdict::Corrupted,
                })
            ));
        })
        .await;
    });
}

#[test]
fn checkpoints_drop_the_prefix_and_fold_back_to_the_same_state() {
    runtime().block_on(async {
        let mut sim = sim(5);
        run(&mut sim, |provider| async move {
            // Tiny segments: every few batches roll over, every 8 entries
            // checkpoint, so the prefix really goes.
            let store = small(8);
            let mut node = open_node(provider.clone(), "c", store).await.expect("open");
            node.format(&config()).await.expect("format");
            for round in 1..=300_u64 {
                node.persist_ballot(ballot(round)).await.expect("promise");
                node.set_chosen_index(Slot(round)).await.expect("index");
                node.append_accepted(Slot(round % 40 + round / 10), ballot(round), user(round, 1))
                    .await
                    .expect("accept");
                if round % 25 == 0 {
                    node.truncate(
                        Slot(round / 10),
                        JournalState {
                            owner: Some(ClientId(1)),
                            generation: Generation(1),
                            next_seq: Seq(round),
                            first_seq: Seq(round / 2),
                        },
                    )
                    .await
                    .expect("truncate");
                }
                node.sync(MustSync::Sync).await.expect("sync");
            }
            let start = node.journal.as_ref().expect("open").start_index();
            assert!(start > super::GENESIS, "checkpoints dropped the prefix");
            let before = node.image.clone();
            drop(node);
            let node = open_node(provider, "c", store).await.expect("reboots");
            assert_eq!(node.image, before, "a reboot folds back to the image");
            assert!(node.is_formatted());
        })
        .await;
    });
}

#[test]
fn a_damaged_registration_is_a_crash_and_a_collected_one_is_not() {
    runtime().block_on(async {
        let mut sim = sim(6);
        run(&mut sim, |provider| async move {
            let belief = |byte: u64| {
                Registration::belief(AcceptorConfig::new(
                    (0..3).map(|n| NodeId(n + byte)).collect::<Vec<_>>(),
                    QuorumSystem::Majority,
                ))
            };
            let mut registry = open_registry(provider.clone(), "m").await.expect("open");
            registry
                .register(ballot(1), &belief(0xC100))
                .await
                .expect("register");
            registry.sync().await.expect("sync");
            registry
                .register(ballot(2), &belief(0xD200))
                .await
                .expect("register");
            registry.sync().await.expect("sync");
            registry
                .register(ballot(3), &belief(0xE300))
                .await
                .expect("register");
            registry.sync().await.expect("sync");
            drop(registry);
            rot_entry(&provider, "m", 2).await;
            match open_registry(provider.clone(), "m").await {
                Err(StorageError::Corruption {
                    record, verdict, ..
                }) => {
                    assert_eq!(record, StorageRecord::Registration(ballot(2)));
                    assert_eq!(
                        verdict,
                        CorruptionVerdict::Corrupted,
                        "a later append proves the sync"
                    );
                }
                other => panic!("a damaged live registration must crash: {other:?}"),
            }
            // Damage in the last append batch is the undecidable row: a
            // crash before its sync leaves the same shape.
            let mut last = open_registry(provider.clone(), "m3").await.expect("open");
            last.register(ballot(1), &belief(0xC100))
                .await
                .expect("register");
            last.sync().await.expect("sync");
            last.register(ballot(2), &belief(0xD200))
                .await
                .expect("register");
            last.sync().await.expect("sync");
            drop(last);
            rot_entry(&provider, "m3", 2).await;
            match open_registry(provider.clone(), "m3").await {
                Err(StorageError::Corruption {
                    record, verdict, ..
                }) => {
                    assert_eq!(record, StorageRecord::Registration(ballot(2)));
                    assert_eq!(verdict, CorruptionVerdict::Undecidable);
                }
                other => panic!("a damaged last registration must crash: {other:?}"),
            }
            // Collected below the watermark, the same damage is harmless.
            let mut other = open_registry(provider.clone(), "m2").await.expect("open");
            other
                .register(ballot(1), &belief(0xC100))
                .await
                .expect("register");
            other.sync().await.expect("sync");
            other
                .register(ballot(2), &belief(0xD200))
                .await
                .expect("register");
            other.sync().await.expect("sync");
            other.set_gc_watermark(ballot(3)).await.expect("raise");
            other.sync().await.expect("sync");
            drop(other);
            rot_entry(&provider, "m2", 2).await;
            let other = open_registry(provider, "m2")
                .await
                .expect("a collected record's damage is harmless");
            assert!(other.registered_ballots().is_empty());
        })
        .await;
    });
}

// ---- the crash loop ---------------------------------------------------------

/// A tiny deterministic generator for the loop's own choices.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Which crash physics a seed runs under (`moonpool-journal`'s two).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Model {
    /// Sector-atomic crashes: nothing acknowledged may be lost or damaged.
    Paper,
    /// Moonpool's full physics: a synced sector being rewritten can be
    /// destroyed — the guarantee is detection, never wrong data.
    Harsh,
}

fn storage(model: Model, rng: &mut Rng) -> StorageConfiguration {
    let pick = |rng: &mut Rng, choices: &[f64]| {
        choices[usize::try_from(rng.below(choices.len() as u64)).expect("small")]
    };
    let mut config = StorageConfiguration::fast_local();
    config.clean_crash_probability = pick(rng, &[0.0, 0.1]);
    config.correlated_rollback_probability = pick(rng, &[0.0, 0.2]);
    config.garbage_fill_probability = pick(rng, &[0.0, 0.5, 1.0]);
    config.crash_lost_probability = 0.0;
    config.crash_latent_fault_probability = 0.0;
    config.shorn_write_probability = 0.0;
    if model == Model::Harsh {
        config.crash_lost_probability = pick(rng, &[0.02, 0.1, 0.3]);
        config.crash_latent_fault_probability = pick(rng, &[0.0, 0.02, 0.1]);
        config.shorn_write_probability = pick(rng, &[0.0, 0.05, 0.2]);
    }
    config
}

/// What the writer knows. `acked` is the state as of the last sync that
/// returned; `written` every `(ballot, command)` ever handed to a slot,
/// acknowledged or not; `pending` the slots written since the last sync.
#[derive(Clone, Default)]
struct Ledger {
    promise: Ballot,
    first: Slot,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    written: BTreeMap<Slot, Vec<(Ballot, Command)>>,
    pending: BTreeMap<Slot, Vec<Ballot>>,
    max_promise: Ballot,
}

type Shared = std::sync::Arc<std::sync::Mutex<Ledger>>;

fn seeds(default: u64) -> std::ops::RangeInclusive<u64> {
    let env = |name: &str| std::env::var(name).ok().and_then(|s| s.parse().ok());
    if let Some(seed) = env("PAROS_JOURNAL_CRASH_SEED") {
        return seed..=seed;
    }
    1..=env("PAROS_JOURNAL_CRASH_SEEDS").unwrap_or(default)
}

/// Tallies across seeds, to show the crashes landed where they matter.
#[derive(Debug, Default)]
struct Tally {
    boots: usize,
    /// Faulty entries reported (torn in-flight writes under the paper's
    /// model, rot too under the harsh one).
    faulty: usize,
    /// Boots whose log no longer began at genesis: a checkpoint dropped a
    /// prefix and the fold started from it.
    past_genesis: usize,
    /// Harsh-model boots refused with a crash verdict.
    refused: usize,
}

#[test]
fn under_the_papers_fault_model_nothing_acknowledged_is_lost() {
    let mut tally = Tally::default();
    for seed in seeds(120) {
        crash_loop(seed, Model::Paper, &mut tally);
    }
    eprintln!("paper model: {tally:?}");
    assert!(tally.faulty > 0, "no crash ever tore an accepted record");
    assert!(
        tally.past_genesis > 0,
        "no boot ever folded from a checkpoint"
    );
    assert_eq!(tally.refused, 0);
}

#[test]
fn under_moonpools_full_physics_a_boot_never_serves_wrong_data() {
    let mut tally = Tally::default();
    for seed in seeds(120) {
        crash_loop(seed, Model::Harsh, &mut tally);
    }
    eprintln!("harsh model: {tally:?}");
    assert!(tally.boots > tally.refused, "every harsh boot was refused");
}

/// Crash a writer repeatedly, judging every reboot against the ledger.
fn crash_loop(seed: u64, model: Model, tally: &mut Tally) {
    runtime().block_on(async {
        let mut sim = SimWorld::new_with_seed(seed);
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        sim.set_storage_config(storage(model, &mut rng));
        let ledger: Shared = Shared::default();
        for round in 0..8 {
            let at = format!("{model:?} seed {seed} round {round}");
            let judged = {
                let ledger = ledger.clone();
                run(&mut sim, move |provider| async move {
                    let node = open_node(provider, "wal", small(24)).await;
                    judge(node, &ledger, model, &at)
                })
                .await
            };
            tally.boots += 1;
            let Some((reported, past_genesis)) = judged else {
                tally.refused += 1;
                return;
            };
            tally.faulty += reported;
            tally.past_genesis += usize::from(past_genesis);
            let ops = 1 + rng.below(30);
            let plan: Vec<u64> = (0..ops).map(|_| rng.next()).collect();
            let handle = tokio::spawn(write(sim.storage_provider(ip()), plan, ledger.clone()));
            for _ in 0..rng.below(600) {
                if handle.is_finished() {
                    break;
                }
                if sim.pending_event_count() > 0 {
                    sim.step();
                }
                tokio::task::yield_now().await;
            }
            handle.abort();
            let _ = handle.await;
            sim.simulate_crash_for_process(ip(), true);
        }
    });
}

/// Judge a reboot and reset the ledger to what it found: the faulty entries
/// it reported and whether it folded past genesis. `None` ends the seed (a
/// harsh-model refusal).
fn judge(
    node: Result<Node, StorageError>,
    ledger: &Shared,
    model: Model,
    at: &str,
) -> Option<(usize, bool)> {
    let node = match node {
        Ok(node) => node,
        Err(error) => {
            assert_eq!(
                model,
                Model::Harsh,
                "{at}: the paper's model must always boot, got {error}"
            );
            return None;
        }
    };
    let mut ledger = ledger.lock().expect("ledger");
    let (hard_state, _) = node.initial_state();
    let promise = hard_state.max_promised_ballot;
    assert!(
        promise <= ledger.max_promise,
        "{at}: a promise nobody wrote"
    );
    // Never wrong data: every value served was written to that slot.
    for slot in node.image.accepted.keys() {
        let served = node.accepted(*slot).expect("listed");
        assert!(
            ledger
                .written
                .get(slot)
                .is_some_and(|w| w.contains(&served)),
            "{at}: slot {} serves a value never written there",
            slot.0
        );
    }
    for (slot, ballot) in node.faulty_entries() {
        assert!(
            ledger
                .written
                .get(&slot)
                .is_some_and(|w| w.iter().any(|(b, _)| *b == ballot)),
            "{at}: faulty slot {} names a ballot never written there",
            slot.0
        );
    }
    if model == Model::Paper {
        assert!(promise >= ledger.promise, "{at}: the promise regressed");
        assert!(
            node.first_slot() >= ledger.first,
            "{at}: an acknowledged truncation was lost"
        );
        for (slot, acked) in &ledger.accepted {
            if *slot < node.first_slot() {
                continue;
            }
            let pending = ledger.pending.get(slot);
            match node.accepted(*slot) {
                Some(found) if found == *acked => {}
                Some(found) => assert!(
                    pending.is_some_and(|p| p.contains(&found.0)),
                    "{at}: slot {} lost its acknowledged value",
                    slot.0
                ),
                None => assert!(
                    node.faulty_entries()
                        .iter()
                        .any(|(s, b)| s == slot && pending.is_some_and(|p| p.contains(b))),
                    "{at}: slot {} lost its acknowledged value",
                    slot.0
                ),
            }
        }
    }
    let reported = node.faulty_entries().len();
    let past_genesis = node
        .journal
        .as_ref()
        .is_some_and(|journal| journal.start_index() > super::GENESIS);
    ledger.promise = promise;
    ledger.first = node.first_slot();
    ledger.accepted = node.image.accepted.clone();
    ledger.pending.clear();
    Some((reported, past_genesis))
}

/// The writer: each step raises the promise, accepts at a slot, truncates or
/// jumps below a trim point, and syncs.
async fn write(
    provider: SimStorageProvider,
    plan: Vec<u64>,
    ledger: Shared,
) -> Result<(), StorageError> {
    let mut node = open_node(provider, "wal", small(24)).await?;
    let mut promise = node.initial_state().0.max_promised_ballot;
    for step in plan {
        let first = node.first_slot();
        match step % 8 {
            0 => {
                promise = Ballot {
                    round: promise.round + 1,
                    node: NodeId(3),
                };
                node.persist_ballot(promise).await?;
                let mut ledger = ledger.lock().expect("ledger");
                ledger.max_promise = ledger.max_promise.max(promise);
            }
            1 => {
                let to = Slot(first.0 + (step >> 8) % 4);
                node.truncate(to, JournalState::default()).await?;
            }
            2 => {
                node.trimmed_to(Slot(first.0 + 1), JournalState::default())
                    .await?;
            }
            _ => {
                let slot = Slot(first.0 + (step >> 8) % 12);
                let command = user(
                    step >> 16,
                    u8::try_from((step >> 24) % 200 + 1).expect("small"),
                );
                node.append_accepted(slot, promise, command.clone()).await?;
                let mut ledger = ledger.lock().expect("ledger");
                ledger
                    .written
                    .entry(slot)
                    .or_default()
                    .push((promise, command));
                ledger.pending.entry(slot).or_default().push(promise);
            }
        }
        node.sync(MustSync::Sync).await?;
        let mut ledger = ledger.lock().expect("ledger");
        ledger.promise = promise;
        ledger.accepted = node.image.accepted.clone();
        ledger.first = node.first_slot();
        ledger.pending.clear();
    }
    Ok(())
}
