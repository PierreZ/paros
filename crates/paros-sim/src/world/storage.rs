//! A node's handle onto the [`StorageWorld`]: the [`NodeStorage`] implementation
//! the driver runs on, with the write-path fault sites and the boot scan.
//!
//! Writes stage locally and reach the durable world only on a `sync`; a crash
//! before the fsync loses the whole un-synced batch — a faithful clean crash.
//! Two independent BUGGIFY sites (a per-record write `EIO`, a failed batch
//! fsync) and a forced torn-tail site inject the fsyncgate ambiguity: the world
//! decides, seeded and recorded as ground truth, whether the effect persisted
//! anyway, and the node only ever sees the ambiguous typed error.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use moonpool_sim::{
    TimeProvider, assert_always, assert_reachable, assert_sometimes, buggify_knob,
    buggify_with_prob, sim::sim_random,
};

use super::rot::roll_boot_rot;
use super::{
    CorruptionInjection, CorruptionKind, CorruptionOutcome, InjectedFault, InjectedFaultKind,
    NodeDisk, RecordHealth, SlotHealth, StorageWorld,
};
use crate::audit::AuditWorld;
use crate::chain::{AppliedTransition, ChainState, hash_text};
use paros::{
    Ballot, Command, Config, ConfigId, CorruptionVerdict, HardState, IntegrityFault, MemStorage,
    MetadataFault, MustSync, NodeStorage, RecoveryCase, SNAP_CHUNK_BYTES, SessionEntry, Slot,
    SlotRecord, Storage, StorageError, StorageRecord, WitnessStatus, WriteOutcome, classify_log,
    command_hash, snap_chunk_count,
};

/// **Default** per-call firing probability of the write-`EIO` BUGGIFY site (one
/// location, per-seed activation × per-call firing; the record identity travels
/// on the typed error, not on the location). The rate itself is a knob — see
/// [`WritePathRates`], which draws it per node per seed.
const P_WRITE_EIO: f64 = PCT_WRITE_EIO as f64 / 100.0;
/// [`P_WRITE_EIO`] as the integer percentage its knob draws in (see
/// [`WritePathRates`]); the two must not drift, so the probability is derived
/// from this rather than written twice.
const PCT_WRITE_EIO: u8 = 1;
/// **Default** per-call firing probability of the fsync-failure BUGGIFY site.
/// Independent from the write site — the sweep must be able to select the two
/// failure modes separately (same rule as the driver's two durability seams) —
/// and its rate is an independent knob too ([`WritePathRates`]).
const P_FSYNC_FAIL: f64 = PCT_FSYNC_FAIL as f64 / 100.0;
/// [`P_FSYNC_FAIL`] as the integer percentage its knob draws in.
const PCT_FSYNC_FAIL: u8 = 1;
/// **Default** per-call firing probability of the **forced torn tail** BUGGIFY
/// site (its rate is a knob: [`WritePathRates`]): its
/// own location, consulted on a `Sync` whose stage holds fresh appends, that
/// takes the fsync site's *lost* leg with the torn coin already decided. The
/// torn-tail shape ("storage: a crash-truncatable tail is discarded on boot")
/// is otherwise the compound of four coins — the fsync site firing at
/// [`P_FSYNC_FAIL`], its lost leg, [`P_TORN_TAIL`], and fresh appends being
/// staged at that moment — which reached only ~1–2% of raw seeds; a
/// coverage-guided schedule clustered on a few roots starved it for a
/// thousand iterations on one CI build. Per BUGGIFY doctrine the
/// rare-but-valid shape gets a location that makes it *likely* on the seeds
/// that activate it, instead of waiting for the swarm to stumble into it.
/// The fault it injects is the ordinary fsync loss (same ledger entry, same
/// budget check, same crash decision by the driver), so every downstream
/// invariant sees exactly what the unforced leg produces.
const P_FORCE_TORN_TAIL: f64 = PCT_FORCE_TORN_TAIL as f64 / 100.0;
/// [`P_FORCE_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_FORCE_TORN_TAIL: u8 = 5;
/// Coin on the fsync *lost* leg: the crash tore the batch instead of losing
/// it whole — a prefix of the staged fresh appends reaches disk without
/// identifiers (Stage 7's per-record torn durability; the `CrashTail` leg of
/// the disentanglement table). A plain seeded coin like the fsyncgate
/// `persisted` decision, NOT its own BUGGIFY location: the *location* is the
/// fsync failure; whole-loss vs torn is the world's outcome-shaping of that
/// one fault, and per-seed location activation must not suppress the torn
/// flavor (the whole-loss leg is already the clean-crash model's default).
/// Its *value* is still a knob ([`WritePathRates`]) — a knob's un-activated
/// draw is the default, so shaping the coin per seed cannot suppress a leg the
/// way gating the coin behind an activation would.
const P_TORN_TAIL: f64 = PCT_TORN_TAIL as f64 / 100.0;
/// [`P_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_TORN_TAIL: u8 = 75;
/// Cap on the crash-truncatable window at boot: the maximum concurrently
/// in-flight accept writes one torn batch can leave unwitnessed (a `Ready`
/// batch is the driver's flush unit, and its accept count is bounded by the
/// leader's recovery page size).
///
/// **Deliberately not a knob.** It is not a tunable that shapes a run but a
/// bound the classifier's correctness depends on: it must equal the driver's
/// real maximum unwitnessed in-flight window. Widen it and the boot scan may
/// discard as "crash tail" a record that *was* acknowledged — data loss the
/// scan invents. Narrow it and legitimate crash tails are classified as
/// corruption and park otherwise healthy nodes. Neither extreme is a valid
/// configuration, so this constant tracks [`paros::LEADER_RECOVERY_BATCH`]
/// instead of being drawn — and is *defined* as that constant so the two
/// cannot drift.
const MAX_TORN_TAIL: usize = paros::LEADER_RECOVERY_BATCH;

/// This node's durable evidence as of one boot, collected under the world
/// lock in `restore` and consumed by the boot scan.
#[derive(Default)]
struct BootEvidence {
    /// Every retained accepted slot in order, with its record health and its
    /// accepted ballot (the identity a recoverable classification reports).
    records: Vec<(Slot, SlotHealth, Ballot)>,
    promise: [RecordHealth; 2],
    chosen: RecordHealth,
    truncation: RecordHealth,
    snapshot: RecordHealth,
    meta: Option<MetadataFault>,
    read_eio: Option<StorageRecord>,
    /// The retained decided snapshot point's per-chunk health (#101), if any.
    snap_point: Option<(u64, Vec<RecordHealth>)>,
}

impl BootEvidence {
    fn collect(disk: &NodeDisk) -> Self {
        Self {
            records: disk
                .accepted
                .iter()
                .map(|(slot, (ballot, _command))| (*slot, disk.slot_health(*slot), *ballot))
                .collect(),
            promise: disk.promise_health,
            chosen: disk.chosen_health,
            truncation: disk.truncation_health,
            snapshot: disk.snapshot_health,
            meta: disk.meta_fault,
            read_eio: disk.read_eio,
            snap_point: disk
                .snap_point
                .map(|(at, _)| (at, disk.snap_chunk_health.clone())),
        }
    }
}

/// The BUGGIFY-side switchboard for the storage-fault sites: shares the driver
/// hooks' chaos window (per the suppression contract on [`StorageWorld`]) and
/// the quiet-mode switch, so choreographed campaigns stay fault-free.
#[derive(Clone)]
pub(crate) struct StorageFaults<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
    /// This node's write-path fault rates, part of its per-seed shape (see
    /// [`WritePathRates`] and [`crate::shape`]).
    rates: WritePathRates,
}

/// The write-path fault rates, **born workload-buggified** (AGENTS.md prong 2):
/// the defaults are this module's documented constants, and an activated seed
/// draws an extreme. Four independent knob locations, so the sweep can select
/// "this seed's disk fails writes often" separately from "this seed's disk
/// fails fsyncs often" and from either torn-tail shaping.
///
/// **The floor on all four is structural rather than numeric**: every write
/// site is gated on [`StorageFaults::active`], so all of them stop at the chaos
/// cutoff, and the accepted-record sites additionally pass through
/// [`StorageWorld::permit_and_record`]'s per-record clean-quorum budget and its
/// never-fault-every-record-of-one-node cap. A run therefore cannot be made
/// unwinnable by turning a rate up: the extremes buy a *denser* fault window,
/// never a longer one, and the recovery tail that follows (an order of
/// magnitude longer than the window) is always fault-free.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WritePathRates {
    write_eio: f64,
    fsync_fail: f64,
    force_torn_tail: f64,
    torn_tail: f64,
    /// The fsyncgate coin of the write-`EIO` site: how often the effect
    /// landed despite the error. Any value in (0, 1) keeps both quadrants
    /// reachable across the sweep; the lost leg is what the budget guards.
    eio_persisted: f64,
    /// The same coin for the fsync site, its own knob (the two sites are
    /// independent locations).
    fsync_persisted: f64,
    /// Whether the last torn record's bytes are damaged too (both `CrashTail`
    /// rows of the decision table are legal at either extreme).
    torn_entry_faulty: f64,
}

impl Default for WritePathRates {
    fn default() -> Self {
        Self {
            write_eio: P_WRITE_EIO,
            fsync_fail: P_FSYNC_FAIL,
            force_torn_tail: P_FORCE_TORN_TAIL,
            torn_tail: P_TORN_TAIL,
            eio_persisted: 0.5,
            fsync_persisted: 0.5,
            torn_entry_faulty: 0.5,
        }
    }
}

impl WritePathRates {
    /// Draw one node's rates. Called exactly once per node per seed by the
    /// shape registry ([`crate::shape`]), never per boot: a node's disk keeps
    /// its failure profile across every incarnation. The knobs are integer
    /// percentages (`buggify_knob!` draws from an integer range) converted to
    /// probabilities here.
    pub(crate) fn draw() -> Self {
        // A disk that returns `EIO` on one write in twelve rather than one in a
        // hundred. The ambiguity contract is unchanged (the world still decides
        // persisted-vs-lost per fault), so the extreme only makes the *recovery*
        // path — boot from whatever the disk actually holds — the common case
        // instead of the rare one.
        let write_eio = buggify_knob!(u64::from(PCT_WRITE_EIO), 2_u64..9_u64);
        // The batch-fsync twin, independently selectable for the same reason
        // the two sites are independent locations at all: the sweep must be
        // able to pick per-record ambiguity without whole-batch ambiguity, and
        // the other way round.
        let fsync_fail = buggify_knob!(u64::from(PCT_FSYNC_FAIL), 2_u64..9_u64);
        // Forcing the torn shape harder. It rides the ordinary fsync ledger,
        // budget and crash decision, so a high rate buys more
        // crash-truncatable tails, not a new fault.
        let force_torn_tail = buggify_knob!(u64::from(PCT_FORCE_TORN_TAIL), 10_u64..41_u64);
        // Outcome-shaping of one fault, not a fault of its own: how a lost
        // fsync leg *lands* (a torn prefix vs. a whole-batch loss). Both legs
        // stay legal at either extreme — whole-batch loss is also what every
        // seam crash before the fsync produces — so the knob only moves which
        // shape this seed's boots have to classify.
        let torn_tail = buggify_knob!(u64::from(PCT_TORN_TAIL), 25_u64..101_u64);
        let eio_persisted = buggify_knob!(50_u64, 10_u64..91_u64);
        let fsync_persisted = buggify_knob!(50_u64, 10_u64..91_u64);
        let torn_entry_faulty = buggify_knob!(50_u64, 10_u64..91_u64);
        if write_eio != u64::from(PCT_WRITE_EIO) || fsync_fail != u64::from(PCT_FSYNC_FAIL) {
            // BUGGIFY pairing: a node genuinely runs on a dense-failure disk.
            assert_reachable!("storage: a node runs with a buggified write-fault rate");
        }
        if force_torn_tail != u64::from(PCT_FORCE_TORN_TAIL)
            || torn_tail != u64::from(PCT_TORN_TAIL)
        {
            // BUGGIFY pairing: the torn-tail shaping knobs genuinely fire.
            assert_reachable!("storage: a node runs with a buggified torn-tail rate");
        }
        #[allow(clippy::cast_precision_loss)]
        let pct = |v: u64| v as f64 / 100.0;
        Self {
            write_eio: pct(write_eio),
            fsync_fail: pct(fsync_fail),
            force_torn_tail: pct(force_torn_tail),
            torn_tail: pct(torn_tail),
            eio_persisted: pct(eio_persisted),
            fsync_persisted: pct(fsync_persisted),
            torn_entry_faulty: pct(torn_entry_faulty),
        }
    }
}

impl<T: TimeProvider> StorageFaults<T> {
    /// `rates` come from the node's shape (drawn once per node per seed, so
    /// a restarted node keeps its disk's failure profile); a quiet node passes
    /// the defaults and `enabled: false`, which never consults them.
    pub(crate) fn new(time: T, cutoff: Duration, enabled: bool, rates: WritePathRates) -> Self {
        Self {
            time,
            cutoff,
            enabled,
            rates,
        }
    }

    fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }
}

/// A [`NodeStorage`] handle onto one node's slice of the shared [`StorageWorld`].
///
/// It holds a `Weak` to the world, upgraded per op (moonpool's "world held via
/// Weak, upgraded per op" convention). Reads are served from `boot` — a snapshot
/// of this node's durable records taken at construction — because the core only
/// reads storage once, at boot.
///
/// Writes stage locally and reach the durable world only on a
/// [`sync`](NodeStorage::sync): a [`MustSync::Sync`] batch flushes the stage
/// through (fsync); a [`MustSync::Relaxed`] batch leaves it staged, so it is lost
/// if the incarnation is dropped before a later sync. Because the stage lives in
/// this handle (dropped when `run_node` unwinds on a seam crash), a crash *before*
/// the fsync loses the whole un-synced batch — a faithful clean crash.
///
/// **Fault model (issue #19 B/C).** Two independent BUGGIFY sites: a per-record
/// write returns `EIO`, and the batch fsync fails. On every injected error the
/// world independently decides — seeded, recorded as ground truth — whether the
/// effect persisted anyway (fsyncgate): the *persisted* leg flushes the whole
/// current stage through to the durable disk before reporting the error, the
/// *lost* leg flushes nothing. Ambiguity is therefore at **flush granularity**,
/// the same all-or-nothing the clean-crash model already uses: both legs land
/// states an fsync boundary could legally produce, and the node — which only
/// ever sees the ambiguous typed error and then crashes — must recover
/// correctly from either. Per-record torn durability belongs to Stage 7's
/// corruption model, not here.
pub(crate) struct DurableStorage<T> {
    /// Read view: this node's durable records as of boot.
    boot: MemStorage,
    /// The shared world, upgraded per op.
    world: Weak<Mutex<StorageWorld>>,
    /// This node's IP — its key into the world.
    pub(crate) key: String,
    /// Stable numeric identity used only on application trace facts.
    pub(crate) node_id: u64,
    /// The budgeted fault switchboard (see the type-level fault model note).
    faults: StorageFaults<T>,
    /// The shared checker, fed the world's flush ground truth (see
    /// [`AuditWorld::note_flushed_ground_truth`]).
    pub(crate) checker: Arc<AuditWorld>,
    /// This incarnation's application state, including transitions staged for
    /// the next durability flush.
    application: ChainState,
    /// This boot's durable-record evidence, consumed by the boot scan.
    evidence: BootEvidence,
    /// The scan's recoverable classification, served to the core through
    /// [`Storage::faulty_entries`].
    faulty_list: Vec<(Slot, Ballot)>,
    /// The scan's rotted-chunk classification of the retained decided
    /// snapshot point (#101), served through
    /// [`NodeStorage::faulty_snap_chunks`].
    faulty_chunks: Vec<(Slot, u32)>,
    /// A decided snapshot point staged for the next durability flush (#101).
    staged_snap_point: Option<(Slot, ChainState)>,
    /// Writes staged since the last flush (lost if the incarnation is dropped).
    staged_config_id: Option<ConfigId>,
    staged_ballot: Option<Ballot>,
    staged_accepted: BTreeMap<Slot, (Ballot, Command)>,
    staged_chosen: Option<Slot>,
    staged_floor: Option<Slot>,
    staged_snapshot: Option<ChainState>,
    staged_applies: Vec<PendingApply>,
    /// Sealed ledger records staged with a truncate / snapshot install (#94),
    /// flushed to the durable world with the rest of the batch.
    staged_sealed: Vec<SessionEntry>,
}

struct PendingApply {
    slot: Slot,
    transition: AppliedTransition,
}

impl<T: TimeProvider> DurableStorage<T> {
    /// Build storage for `config`, seeding the read view from any durable records
    /// a prior boot of this node (same IP, same iteration) left in the world.
    pub(crate) fn restore(
        config: Config,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        node_id: u64,
        faults: StorageFaults<T>,
        checker: Arc<AuditWorld>,
    ) -> Self {
        let mut boot = MemStorage::new(config);
        let mut application = ChainState::default();
        let mut evidence = BootEvidence::default();
        if let Some(strong) = world.upgrade() {
            let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            application = ChainState::empty(guard.lane_count());
            // Stage 7 rot: latent faults that surfaced while the node was
            // down, rolled at the boot that immediately scans them. Gated on
            // the chaos window like every other injection.
            if faults.active() {
                roll_boot_rot(&mut guard, &key, node_id);
            }
            let guard = &*guard;
            // Coverage: the recovery paths only matter if boots genuinely
            // re-read a prior incarnation's records (attrition + seam crashes
            // make this common across the sweep).
            assert_sometimes!(
                guard.disks.contains_key(&key),
                "a node boots from a prior incarnation's durable records"
            );
            if let Some(disk) = guard.disks.get(&key) {
                application = disk.chain;
                // Read-back pair of the flush ordering `sync` claims: a floor
                // that reached the disk never outruns the chosen index that
                // reached the disk (the flush applies the floor last, and a
                // crash drops the whole stage together).
                assert_always!(
                    disk.first_slot.0 == 0
                        || disk
                            .hard_state
                            .chosen_index
                            .is_some_and(|ci| disk.first_slot.0 <= ci.0 + 1),
                    "a restored floor never outruns the restored chosen index",
                    {
                        "floor" => disk.first_slot.0,
                        "chosen" => disk.hard_state.chosen_index.map_or(0, |c| c.0)
                    }
                );
                if disk.first_slot.0 > 0 {
                    assert_reachable!("a node reboots above a non-zero compaction floor");
                }
                // Collect this boot's integrity evidence for the scan the
                // driver runs before anything reads the store.
                evidence = BootEvidence::collect(disk);
                // Seed the read view through the semantic ops (records, not a blob).
                // Set the floor first so first_slot() reads it back on boot; the
                // sealed ledger rides on the same op, exactly as a truncate
                // persisted it.
                let sealed: Vec<SessionEntry> = disk
                    .sealed
                    .iter()
                    .map(|(&(client, seq), &slot)| (client, seq, slot))
                    .collect();
                let _ = boot.truncate(disk.first_slot, &sealed);
                let _ = boot.persist_config_id(disk.hard_state.config_id);
                let _ = boot.persist_ballot(disk.hard_state.max_promised_ballot);
                for (slot, (ballot, command)) in &disk.accepted {
                    // The detector withholds bytes: only clean, witnessed
                    // records enter the read view. Anything else either
                    // crashes the scan (so the view is never consulted) or is
                    // a crash-truncatable tail the scan discards.
                    if disk.slot_health(*slot).clean() {
                        let _ = boot.append_accepted(*slot, *ballot, command.clone());
                    }
                }
                if let Some(ci) = disk.hard_state.chosen_index {
                    let _ = boot.set_chosen_index(ci);
                }
                let _ = boot.sync(MustSync::Sync);
            }
        }
        Self {
            boot,
            world,
            key,
            node_id,
            faults,
            checker,
            application,
            evidence,
            faulty_list: Vec::new(),
            faulty_chunks: Vec::new(),
            staged_snap_point: None,
            staged_config_id: None,
            staged_ballot: None,
            staged_accepted: BTreeMap::new(),
            staged_chosen: None,
            staged_floor: None,
            staged_snapshot: None,
            staged_applies: Vec::new(),
            staged_sealed: Vec::new(),
        }
    }

    /// The run's digest-lane count (the world's, or the default when the
    /// world is gone).
    fn lane_count(&self) -> u8 {
        self.with_world(|w| w.lane_count())
            .unwrap_or(crate::chain::DEFAULT_LANES)
    }

    /// Run `f` against the shared world.
    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Result<R, StorageError> {
        let strong = self.world.upgrade().ok_or(StorageError::Io {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Lost,
        })?;
        let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(f(&mut guard))
    }

    /// The record identities currently staged (what the next flush covers).
    fn staged_records(&self) -> Vec<StorageRecord> {
        let mut records = Vec::new();
        if self.staged_config_id.is_some() {
            records.push(StorageRecord::ConfigId);
        }
        if self.staged_ballot.is_some() {
            records.push(StorageRecord::Promise);
        }
        records.extend(
            self.staged_accepted
                .keys()
                .map(|s| StorageRecord::Accepted(*s)),
        );
        if self.staged_chosen.is_some() {
            records.push(StorageRecord::ChosenIndex);
        }
        if self.staged_floor.is_some() {
            records.push(StorageRecord::Truncation);
        }
        if self.staged_snapshot.is_some() {
            records.push(StorageRecord::Snapshot);
        }
        records.extend(
            self.staged_applies
                .iter()
                .map(|p| StorageRecord::Application(p.slot)),
        );
        records
    }

    /// Whether a torn flush would tear anything right now: the stage holds at
    /// least one accept above this disk's durable maximum (only fresh appends
    /// tear, see [`Self::torn_flush`]) **and** the first such record clears the
    /// per-record budget (a clean quorum of live copies survives its loss).
    /// The budget is what makes the shape rare — a leader's first write of a
    /// slot has no clean copies elsewhere yet, so only a node writing slots a
    /// quorum already holds can tear — which is why the forcing site consults
    /// this predicate rather than the bare fresh-append check.
    fn tearable_stage(&self) -> bool {
        let key = self.key.clone();
        let staged: Vec<Slot> = self.staged_accepted.keys().copied().collect();
        self.with_world(|w| {
            let durable_max = w
                .disks
                .get(&key)
                .and_then(|d| d.accepted.keys().next_back().copied());
            let Some(first) = staged
                .into_iter()
                .find(|slot| durable_max.is_none_or(|max| *slot > max))
            else {
                return false;
            };
            let already = w.marks.get(&key).is_some_and(|m| m.contains(&first.0));
            w.clean_copies(first.0)
                .saturating_sub(usize::from(!already))
                >= w.quorum()
        })
        .unwrap_or(false)
    }

    /// The torn leg of a lost fsync (Stage 7): a prefix of the batch's
    /// **fresh appends** reaches the disk without identifiers — the crash tore
    /// the batch mid-write instead of losing it whole. Only fresh appends
    /// tear: tearing an overwrite would rot a previously *witnessed* record
    /// (the rot family's job), and fresh appends are what guarantees the torn
    /// records sit at the log's very tail, where the boot scan's
    /// disentanglement classifies them crash-truncatable and discards them —
    /// the one legal discard (never acknowledged: `Accepted` only ever leaves
    /// after a clean sync, per persist-before-send). The last torn record's
    /// bytes may themselves be damaged, so both `CrashTail` rows of the
    /// decision table are visited.
    fn torn_flush(&mut self) {
        let key = self.key.clone();
        let node = self.node_id;
        let torn_faulty_rate = self.faults.rates.torn_entry_faulty;
        let staged: Vec<(Slot, (Ballot, Command))> = self
            .staged_accepted
            .iter()
            .map(|(slot, record)| (*slot, record.clone()))
            .collect();
        let _ = self.with_world(|w| {
            let durable_max = w
                .disks
                .get(&key)
                .and_then(|d| d.accepted.keys().next_back().copied());
            let fresh: Vec<(Slot, (Ballot, Command))> = staged
                .into_iter()
                .filter(|(slot, _)| durable_max.is_none_or(|max| *slot > max))
                .collect();
            if fresh.is_empty() {
                return;
            }
            let count = 1 + usize::try_from(sim_random::<u64>()).unwrap_or(0) % fresh.len();
            // The same per-record budget as every other injection: never leave
            // a record without a clean quorum of live copies.
            let quorum = w.quorum();
            let mut torn: Vec<(Slot, (Ballot, Command))> = Vec::new();
            for (slot, record) in fresh.into_iter().take(count) {
                let already = w.marks.get(&key).is_some_and(|m| m.contains(&slot.0));
                if w.clean_copies(slot.0).saturating_sub(usize::from(!already)) < quorum {
                    break;
                }
                torn.push((slot, record));
            }
            if torn.is_empty() {
                return;
            }
            let torn_entry_faulty = sim_random::<f64>() < torn_faulty_rate;
            let last = torn.len() - 1;
            let d = w.disk_mut(&key);
            for (i, (slot, record)) in torn.iter().enumerate() {
                d.accepted.insert(*slot, record.clone());
                d.entry_health.insert(
                    *slot,
                    SlotHealth {
                        entry: if i == last && torn_entry_faulty {
                            RecordHealth::Faulty
                        } else {
                            RecordHealth::Clean
                        },
                        id: WitnessStatus::Absent,
                    },
                );
            }
            // Deliberately NOT noted as flushed ground truth: a torn record is
            // not durably witnessed, and the boot scan discarding it must not
            // read as recovered-vs-persisted divergence.
            for (slot, _) in &torn {
                w.marks.entry(key.clone()).or_default().insert(slot.0);
                w.note_corruption(CorruptionInjection {
                    node,
                    record: StorageRecord::Accepted(*slot),
                    kind: CorruptionKind::TornTail,
                    block: false,
                    outcome: CorruptionOutcome::Dormant,
                });
            }
        });
    }

    /// BUGGIFY site 1: this per-record write returns `EIO`. Returns
    /// `Some(persisted)` when the fault fires and the budget permits it —
    /// `persisted` is the world's seeded ambiguity decision (item C), recorded
    /// as ground truth and never told to the node.
    fn roll_write_eio(&mut self, record: StorageRecord) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(self.faults.rates.write_eio) {
            return None;
        }
        let persisted = sim_random::<f64>() < self.faults.rates.eio_persisted;
        let accepted_slots: Vec<u64> = match record {
            StorageRecord::Accepted(slot) => vec![slot.0],
            _ => Vec::new(),
        };
        let fault = InjectedFault {
            node: self.node_id,
            record,
            kind: InjectedFaultKind::WriteEio,
            persisted,
        };
        let in_window = self.faults.active();
        let permitted = self
            .with_world(|w| {
                // Suppression is explicit: the world records no new fault
                // outside the chaos window (and never heals old ones).
                assert_always!(
                    in_window,
                    "storage: no new fault is injected after the chaos window"
                );
                w.permit_and_record(&self.key, fault, &accepted_slots)
            })
            .unwrap_or(false);
        permitted.then_some(persisted)
    }

    /// BUGGIFY site 2: the staged batch's fsync fails. Independent location
    /// from the write site; same ambiguity contract. `force_lost` is the
    /// forced-torn-tail site's verdict: it skips this site's own coin and
    /// takes the lost leg outright, through the same ledger and budget.
    fn roll_fsync_fault(&mut self, force_lost: bool) -> Option<bool> {
        if !self.faults.active() {
            return None;
        }
        if !force_lost && !buggify_with_prob!(self.faults.rates.fsync_fail) {
            return None;
        }
        let persisted = !force_lost && sim_random::<f64>() < self.faults.rates.fsync_persisted;
        let accepted_slots: Vec<u64> = self.staged_accepted.keys().map(|s| s.0).collect();
        let fault = InjectedFault {
            node: self.node_id,
            record: StorageRecord::Batch,
            kind: InjectedFaultKind::FsyncFailed,
            persisted,
        };
        let in_window = self.faults.active();
        let permitted = self
            .with_world(|w| {
                assert_always!(
                    in_window,
                    "storage: no new fault is injected after the chaos window"
                );
                w.permit_and_record(&self.key, fault, &accepted_slots)
            })
            .unwrap_or(false);
        permitted.then_some(persisted)
    }

    /// Stage one record through the write-`EIO` fault site: on the persisted
    /// leg the record is staged *and* the whole stage flushes through (the
    /// effect is durable despite the reported error); on the lost leg nothing
    /// is staged. The error is identical either way — that is the ambiguity.
    fn write_record(
        &mut self,
        record: StorageRecord,
        stage: impl FnOnce(&mut Self),
    ) -> Result<(), StorageError> {
        match self.roll_write_eio(record) {
            None => {
                stage(self);
                Ok(())
            }
            Some(persisted) => {
                if persisted {
                    stage(self);
                    self.flush_stage()?;
                }
                Err(StorageError::Io {
                    record,
                    outcome: WriteOutcome::Unknown,
                })
            }
        }
    }

    /// Flush the whole stage through to the durable world — the fsync. Also
    /// the persisted leg of an ambiguous fault: identical effect, error
    /// reported anyway. Clean re-writes clear their lost-leg fault marks.
    // One linear flush in write order; the health-healing lines belong beside
    // the writes whose durability they mirror.
    #[allow(clippy::too_many_lines)]
    fn flush_stage(&mut self) -> Result<(), StorageError> {
        let config_id = self.staged_config_id.take();
        let ballot = self.staged_ballot.take();
        let accepted = std::mem::take(&mut self.staged_accepted);
        let chosen = self.staged_chosen.take();
        let floor = self.staged_floor.take();
        let snapshot = self.staged_snapshot.take();
        let snap_point = self.staged_snap_point.take();
        let applies = std::mem::take(&mut self.staged_applies);
        let sealed = std::mem::take(&mut self.staged_sealed);
        let flushed_slots: Vec<u64> = accepted.keys().map(|s| s.0).collect();
        let flushed_hashes: Vec<(u64, u64)> = accepted
            .iter()
            .map(|(slot, (_ballot, command))| (slot.0, command_hash(command)))
            .collect();
        let key = self.key.clone();
        let node = self.node_id;
        self.with_world(|w| {
            w.clear_marks(&key, flushed_slots.iter().copied());
            let d = w.disk_mut(&key);
            if let Some(config_id) = config_id {
                d.hard_state.config_id = config_id;
            }
            // Sealed ledger records are upserts keyed by (client, seq); the
            // first-slot claim wins, matching the core's ledger semantics.
            for (client, seq, slot) in sealed {
                d.sealed.entry((client, seq)).or_insert(slot);
            }
            if let Some(b) = ballot {
                // The promise is monotonic: never let a flush lower it. A
                // SetPromise write only ever raises it, but an InstallSnapshot
                // carries the *server's* ballot, which can be below this node's own
                // promise, so take the max (matching `MemStorage::install_snapshot`).
                d.hard_state.max_promised_ballot = d.hard_state.max_promised_ballot.max(b);
                // A clean re-write of the HardState copies restores their
                // health — genuine recovery, not the world healing anything.
                d.promise_health = [RecordHealth::Clean; 2];
            }
            let mut healed: Vec<Slot> = Vec::new();
            for (slot, record) in accepted {
                // A clean flush re-writes the record and its identifier: any
                // prior torn/rotted health for the slot is genuinely replaced —
                // this is the Stage-8 in-place repair landing on disk.
                if d.entry_health.remove(&slot).is_some() {
                    healed.push(slot);
                }
                d.accepted.insert(slot, record);
            }
            if let Some(c) = chosen {
                d.hard_state.chosen_index = Some(c);
                d.chosen_health = RecordHealth::Clean;
            }
            // Apply the truncation last, after the chosen index it sits behind, so
            // a flushed floor never outruns the flushed chosen index.
            if let Some(f) = floor {
                d.first_slot = d.first_slot.max(f);
                d.accepted.retain(|s, _| *s >= d.first_slot);
                let new_floor = d.first_slot;
                // A fault mark dropped by the floor raise is superseded, not
                // repaired: the record's information migrated into the applied
                // application state (truncation is decided over the applied
                // prefix) — resolve its report as recovered-by-custodianship.
                healed.extend(
                    d.entry_health
                        .range(..new_floor)
                        .map(|(slot, _)| *slot)
                        .collect::<Vec<_>>(),
                );
                d.entry_health.retain(|s, _| *s >= new_floor);
                d.truncation_health = RecordHealth::Clean;
            }
            if let Some(installed) = snapshot {
                d.chain = installed;
                d.snapshot_health = RecordHealth::Clean;
            }
            if let Some(last) = applies.last() {
                d.chain = last.transition.next;
                d.snapshot_health = RecordHealth::Clean;
            }
            // A decided snapshot point (#101): retain the new point with a
            // fresh all-clean chunk map. Advancing the point supersedes the
            // old one's outstanding chunk reports — custody moved to the new
            // byte-identical blob.
            let mut superseded_chunks: Vec<(u64, u32)> = Vec::new();
            if let Some((at, state)) = snap_point {
                if let Some((old_at, _)) = d.snap_point
                    && old_at != at.0
                {
                    superseded_chunks = d
                        .snap_chunk_health
                        .iter()
                        .enumerate()
                        .filter(|(_, health)| **health != RecordHealth::Clean)
                        .map(|(index, _)| (old_at, u32::try_from(index).unwrap_or(u32::MAX)))
                        .collect();
                }
                let chunks = snap_chunk_count(state.encode().len());
                d.snap_point = Some((at.0, state));
                d.snap_chunk_health =
                    vec![RecordHealth::Clean; usize::try_from(chunks).unwrap_or(0)];
            }
            // Write-side pair of the `restore` read-back check: the floor is
            // applied last, behind the chosen index it sits under, so no flush
            // ever leaves a durable floor above the durable chosen index.
            assert_always!(
                d.first_slot.0 == 0
                    || d
                        .hard_state
                        .chosen_index
                        .is_some_and(|ci| d.first_slot.0 <= ci.0 + 1),
                "a flushed floor never outruns the flushed chosen index",
                {
                    "floor" => d.first_slot.0,
                    "chosen" => d.hard_state.chosen_index.map_or(0, |c| c.0)
                }
            );
            // A floor raise retires the fault marks of the records it drops:
            // truncation (or a snapshot install) is only ever decided for the
            // applied prefix, so the record's information has migrated into
            // the application snapshot — clearing the mark is custodianship
            // transfer, not the world healing a fault.
            let new_floor = d.first_slot;
            if let Some(marks) = w.marks.get_mut(&key) {
                marks.retain(|slot| *slot >= new_floor.0);
            }
            // Resolve the reports the flush genuinely healed (or truncation
            // superseded), and the snapshot report once fresh application
            // state landed (an install, or replayed applies).
            for slot in healed {
                w.note_recovered(node, StorageRecord::Accepted(slot));
            }
            if snapshot.is_some() || !applies.is_empty() {
                w.note_recovered(node, StorageRecord::Snapshot);
            }
            for (old_at, chunk) in superseded_chunks {
                w.note_recovered(node, StorageRecord::SnapChunk(Slot(old_at), chunk));
            }
        })?;

        // Feed the shared checker the flush's ground truth (see
        // [`AuditWorld::note_flushed_ground_truth`]): an ambiguous fault leg
        // flushes without the driver ever surfacing the writes, and the
        // cross-restart checks must compare against what the disk actually
        // holds. A clean flush notes the same values the driver is about to
        // report, so the double entry is idempotent.
        let now_ms = u64::try_from(self.faults.time.now().as_millis()).unwrap_or(u64::MAX);
        let landing = if snapshot.is_some() {
            chosen.map(|slot| slot.0)
        } else {
            None
        };
        self.checker.note_flushed_ground_truth(
            self.node_id,
            now_ms,
            &flushed_hashes,
            floor.map(|slot| slot.0),
            landing,
        );

        // The application-state facts, reported to the audit as they become
        // durable (and traced for humans). An install is a jump; every apply
        // is one contiguous transition.
        if let Some(installed) = snapshot {
            self.checker
                .app_snapshot(self.node_id, installed.applied_count, installed.chain_hash);
            tracing::info!(
                node = self.node_id,
                index = installed.applied_count,
                state = %hash_text(installed.chain_hash),
                "chain_snapshot_installed"
            );
        }
        for pending in applies {
            let next = pending.transition.next;
            self.checker.app_applied(
                self.node_id,
                next.applied_count,
                pending.transition.cmd_hash,
                pending.transition.kind == "user",
                pending.transition.kind == "noop",
                next.chain_hash,
            );
            tracing::info!(
                target: "chain",
                node = self.node_id,
                slot = pending.slot.0,
                index = next.applied_count,
                cmd = %hash_text(pending.transition.cmd_hash),
                state = %hash_text(next.chain_hash),
                kind = pending.transition.kind,
                "command_applied"
            );
        }
        Ok(())
    }
}

impl<T: TimeProvider> NodeStorage for DurableStorage<T> {
    /// The Stage-7 detection layer at the seam (CLStore-equivalent): verify
    /// every durable record's read-back evidence, classify each mismatch with
    /// the total decision function (`paros::classify_log`), and surface the
    /// first crash-verdict as the typed error the driver crashes on. The scan
    /// resolves exactly two things itself — a crash-truncatable tail is
    /// discarded (never acknowledged to anyone) and a single bad `HardState`
    /// copy is repaired from its twin — and **never truncates on a corruption
    /// verdict**.
    ///
    /// That last rule is CTRL Figure 2's bug class, found in both `ZooKeeper`
    /// and `LogCabin`: a scan that drops the log from the faulty entry onward
    /// and keeps running as if the log simply ended earlier silently discards
    /// possibly-chosen records (and regresses the derived chosen index with
    /// them), so the node can then win an election against lagging peers and
    /// erase committed data cluster-wide. It was proven load-bearing by
    /// mutation: wiring the scan to truncate here instead of crashing turned
    /// the audit's recovered-vs-persisted divergence leg ("storage: a
    /// recovered log omits a persisted record only after a detected corruption
    /// crash") red. Detect ⇒ crash is the baseline; a crash-truncatable tail is
    /// the only thing a scan may ever discard.
    #[allow(clippy::too_many_lines)] // one linear scan: metadata → scalars → log
    fn boot_scan(&mut self) -> Result<(), StorageError> {
        let evidence = std::mem::take(&mut self.evidence);
        let key = self.key.clone();
        let node = self.node_id;
        // FS metadata (item E): the store itself is the record; reliably
        // crash, never attempt recovery — Stage 8 either.
        if let Some(fault) = evidence.meta {
            let _ = self.with_world(|w| {
                w.s7.metadata_crashed = true;
                w.resolve_corruption(node, StorageRecord::Store, CorruptionOutcome::Crashed);
            });
            return Err(StorageError::Metadata { fault });
        }
        // A transient EIO on the read path collapses into the corruption
        // channel (zero-fill ⇒ mismatch): same detection, same crash. The
        // world clears the fault, so the retry — the next boot — reads clean.
        if let Some(record) = evidence.read_eio {
            let _ = self.with_world(|w| {
                w.s7.read_eio_detected = true;
                if let Some(disk) = w.disks.get_mut(&key) {
                    disk.read_eio = None;
                }
                w.resolve_corruption(node, record, CorruptionOutcome::Crashed);
            });
            return Err(StorageError::Corruption {
                record,
                fault: IntegrityFault::ReadError,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        // The two HardState copies (CTRL metainfo doctrine): one bad ⇒ use
        // the other and repair it; both bad ⇒ crash — the node cannot know
        // what it promised, and no peer can tell it (Stage 8's safety
        // argument, pre-stated).
        let promise_faults = [
            evidence.promise[0].integrity_fault(),
            evidence.promise[1].integrity_fault(),
        ];
        match promise_faults {
            [Some(fault), Some(_)] => {
                let _ = self.with_world(|w| {
                    w.resolve_corruption(node, StorageRecord::Promise, CorruptionOutcome::Crashed);
                    // Terminal by doctrine (detect ⇒ crash, stays down): with
                    // both copies lost the promise is unknowable and no boot
                    // will ever read past this point, so park the node here
                    // rather than crash-looping — one typed crash decision,
                    // matched 1:1 by the ledger. The injection site only
                    // builds this shape under `may_park`, so this is defense
                    // in depth for any other path assembling it.
                    w.park(&key, node);
                });
                assert_reachable!(
                    "storage: both promise copies are lost (crash: unknowable promise)"
                );
                return Err(StorageError::Corruption {
                    record: StorageRecord::Promise,
                    fault,
                    verdict: CorruptionVerdict::Corrupted,
                });
            }
            [Some(_), None] | [None, Some(_)] => {
                let _ = self.with_world(|w| {
                    w.s7.promise_repaired = true;
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.promise_health = [RecordHealth::Clean; 2];
                    }
                    w.resolve_corruption(node, StorageRecord::Promise, CorruptionOutcome::Repaired);
                });
                tracing::info!(node, "promise_copy_repaired");
            }
            [None, None] => {}
        }
        // The remaining scalars follow the atomic-rename discipline: a partial
        // write is discarded, so a mismatch is always corruption — no
        // crash-tail leg exists for them.
        for (health, record) in [
            (evidence.chosen, StorageRecord::ChosenIndex),
            (evidence.truncation, StorageRecord::Truncation),
        ] {
            if let Some(fault) = health.integrity_fault() {
                let _ = self.with_world(|w| {
                    w.park(&key, node);
                    w.resolve_corruption(node, record, CorruptionOutcome::Crashed);
                });
                return Err(StorageError::Corruption {
                    record,
                    fault,
                    verdict: CorruptionVerdict::Corrupted,
                });
            }
        }
        // Snapshot corruption: its own kind and its own gate (#71). Stage 8
        // recovers instead of crashing — a snapshot is never discardable and
        // all its data is committed by definition, so the node must never
        // install or serve the garbage, and must recover the state: with the
        // log intact from slot 0 the ordinary boot replay rebuilds it locally
        // (CTRL's cheap path, with the core's duplicate-suppression decisions
        // re-derived exactly); under a truncated log only a peer's
        // `InstallSnapshot` covers the folded prefix, so the driver opens a
        // below-floor application repair and the node *waits* on it — serving
        // consensus for every slot it can read, applying nothing.
        if evidence.snapshot.integrity_fault().is_some() {
            let floor = self.boot.first_slot();
            // #101: a fully clean decided snapshot point that covers the
            // floor restores the lost application state *locally* — the
            // CTRL payoff of consensus-decided snapshot points: no whole-blob
            // transfer, no wait, just the retained byte-identical state.
            let point_state = self
                .with_world(|w| {
                    let disk = w.disks.get(&key)?;
                    let (at, state) = disk.snap_point?;
                    let all_clean = disk
                        .snap_chunk_health
                        .iter()
                        .all(|health| *health == RecordHealth::Clean);
                    (all_clean && at + 1 >= floor.0).then_some((at, state))
                })
                .ok()
                .flatten();
            if floor.0 > 0
                && let Some((at, state)) = point_state
            {
                let _ = self.with_world(|w| {
                    w.s7.snapshot_detected = true;
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.chain = state;
                        disk.snapshot_health = RecordHealth::Clean;
                    }
                    w.resolve_corruption(
                        node,
                        StorageRecord::Snapshot,
                        CorruptionOutcome::Reported,
                    );
                    w.note_recovered(node, StorageRecord::Snapshot);
                });
                self.application = state;
                tracing::info!(node, at, "snapshot_restored_from_point");
                // For the application-agreement checker this is a reset (the
                // pre-crash prefix may sit past the point, so the jump can go
                // backward) followed by an install-shaped landing at the
                // point; the re-walk from there is contiguous again.
                self.checker.app_reset(node);
                self.checker
                    .app_snapshot(node, state.applied_count, state.chain_hash);
                tracing::info!(node, floor = floor.0, "snapshot_reset_for_recovery");
                tracing::info!(
                    node,
                    index = state.applied_count,
                    state = %hash_text(state.chain_hash),
                    "chain_snapshot_installed"
                );
                assert_reachable!(
                    "storage: a corrupted snapshot is restored from the decided snapshot point"
                );
            } else {
                let _ = self.with_world(|w| {
                    w.s7.snapshot_detected = true;
                    if floor.0 == 0 {
                        w.s7.snapshot_reset_local = true;
                    } else {
                        w.s7.snapshot_reset_remote = true;
                    }
                    let lanes = w.lane_count();
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.chain = ChainState::empty(lanes);
                        disk.snapshot_health = RecordHealth::Clean;
                    }
                    w.resolve_corruption(
                        node,
                        StorageRecord::Snapshot,
                        CorruptionOutcome::Reported,
                    );
                });
                self.application = ChainState::empty(self.lane_count());
                self.checker.app_reset(node);
                tracing::info!(node, floor = floor.0, "snapshot_reset_for_recovery");
                if floor.0 == 0 {
                    assert_reachable!(
                        "storage: a corrupted snapshot is rebuilt from the local log"
                    );
                } else {
                    assert_reachable!(
                        "storage: a corrupted snapshot awaits a peer snapshot transfer"
                    );
                }
            }
        }
        // #101: rotted chunks of the retained decided snapshot point are the
        // recoverable class by construction — the point's identity survives
        // and every peer holds the byte-identical blob — so they are
        // classified and reported for the driver's chunk-repair pull, never a
        // crash.
        if let Some((at, chunk_health)) = &evidence.snap_point {
            let rotted: Vec<(Slot, u32)> = chunk_health
                .iter()
                .enumerate()
                .filter(|(_, health)| **health != RecordHealth::Clean)
                .map(|(index, _)| (Slot(*at), u32::try_from(index).unwrap_or(u32::MAX)))
                .collect();
            if !rotted.is_empty() {
                let _ = self.with_world(|w| {
                    for (point, chunk) in &rotted {
                        w.resolve_corruption(
                            node,
                            StorageRecord::SnapChunk(*point, *chunk),
                            CorruptionOutcome::Reported,
                        );
                    }
                });
                tracing::info!(
                    node,
                    at = *at,
                    chunks = rotted.len() as u64,
                    "snap_chunks_classified"
                );
                assert_reachable!("storage: rotted snapshot chunks are classified for peer repair");
                self.faulty_chunks = rotted;
            }
        }
        // The log: reduce every retained record to its evidence booleans and
        // run the total classifier (batching rule + TigerBeetle hardening).
        let records: Vec<SlotRecord> = evidence
            .records
            .iter()
            .map(|(slot, health, _ballot)| SlotRecord {
                slot: *slot,
                entry_faulty: health.entry != RecordHealth::Clean,
                identifier: health.id,
            })
            .collect();
        let cases = classify_log(&records, MAX_TORN_TAIL);
        let mut discard: Vec<Slot> = Vec::new();
        // Stage 8's crash-relevance split (the issue-#21 table): a record whose
        // *identity* is known — the identifier survived, or the entry's own
        // checksummed identity region did — is **recoverable**: reported into
        // the tri-state, repaired in place, never a crash. A record whose
        // identity is also lost is unidentifiable (the node cannot even ask
        // peers the right question), and the abandoned-window undecidables
        // break head-certainty likewise: those stay the Stage-7 crash.
        let mut recover: Vec<(Slot, Ballot, RecoveryCase)> = Vec::new();
        let mut crashes: Vec<(Slot, SlotHealth, RecoveryCase)> = Vec::new();
        for ((slot, case), (_, health, ballot)) in cases.iter().zip(evidence.records.iter()) {
            match case.verdict() {
                None => {}
                Some(CorruptionVerdict::CrashTail) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    discard.push(*slot);
                }
                Some(CorruptionVerdict::Corrupted | CorruptionVerdict::Undecidable) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    match case {
                        RecoveryCase::CorruptionBelowTail
                        | RecoveryCase::IdentifierFaulty
                        | RecoveryCase::LastEntryAmbiguity => recover.push((*slot, *ballot, *case)),
                        _ => crashes.push((*slot, *health, *case)),
                    }
                }
            }
        }
        // Family / per-verdict coverage flags fire at *classification* — the
        // detection channels are exercised whether the reaction is a crash or
        // a report (the message strings are Stage 7's, unchanged).
        let _ = self.with_world(|w| {
            for ((_slot, case), (_, health, _ballot)) in cases.iter().zip(evidence.records.iter()) {
                if case.verdict().is_none() {
                    continue;
                }
                match case {
                    RecoveryCase::CorruptionBelowTail => w.s7.corruption_below_tail = true,
                    RecoveryCase::LastEntryAmbiguity => w.s7.last_entry_ambiguity = true,
                    RecoveryCase::IdentifierLostWithEntry => w.s7.identifier_lost = true,
                    _ => {}
                }
                match health.entry {
                    RecordHealth::Faulty => w.s7.bitflip_detected = true,
                    RecordHealth::Lost => w.s7.lost_write_detected = true,
                    RecordHealth::Misdirected => w.s7.misdirected_detected = true,
                    RecordHealth::Clean => {}
                }
            }
        });
        if let Some(&(slot, health, case)) = crashes.first() {
            let _ = self.with_world(|w| {
                // Detection is certain and terminal for an unidentifiable
                // record: restarting cannot help (rot injection already parked
                // the node; classifier-derived verdicts park it here).
                w.park(&key, node);
                for (i, (crash_slot, _crash_health, _crash_case)) in crashes.iter().enumerate() {
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*crash_slot),
                        if i == 0 {
                            CorruptionOutcome::Crashed
                        } else {
                            CorruptionOutcome::CoDetected
                        },
                    );
                }
            });
            return Err(StorageError::Corruption {
                record: StorageRecord::Accepted(slot),
                // An id-missing verdict on a clean entry means the *witness*
                // was lost, not the bytes.
                fault: health
                    .entry
                    .integrity_fault()
                    .unwrap_or(IntegrityFault::LostWrite),
                verdict: case.verdict().unwrap_or(CorruptionVerdict::Corrupted),
            });
        }
        if !recover.is_empty() {
            let _ = self.with_world(|w| {
                for (slot, _ballot, case) in &recover {
                    w.s7.faulty_reported = true;
                    tracing::info!(
                        node,
                        slot = slot.0,
                        case = case.label(),
                        "faulty_entry_reported"
                    );
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*slot),
                        CorruptionOutcome::Reported,
                    );
                }
            });
            self.faulty_list = recover
                .iter()
                .map(|(slot, ballot, _case)| (*slot, *ballot))
                .collect();
        }
        if !discard.is_empty() {
            // The one legal discard: a crash-truncatable tail was never
            // acknowledged to anyone (persist-before-send), so dropping it
            // locally is a faithful crash, not data loss.
            let _ = self.with_world(|w| {
                w.s7.torn_tail_discarded = true;
                w.clear_marks(&key, discard.iter().map(|slot| slot.0));
                if let Some(disk) = w.disks.get_mut(&key) {
                    for slot in &discard {
                        disk.accepted.remove(slot);
                        disk.entry_health.remove(slot);
                    }
                }
                for slot in &discard {
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*slot),
                        CorruptionOutcome::DiscardedTail,
                    );
                }
                for slot in &discard {
                    tracing::info!(node, slot = slot.0, "crash_tail_discarded");
                }
            });
        }
        Ok(())
    }

    fn persist_config_id(&mut self, config_id: ConfigId) -> Result<(), StorageError> {
        self.write_record(StorageRecord::ConfigId, |s| {
            s.staged_config_id = Some(config_id);
        })
    }

    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.write_record(StorageRecord::Promise, |s| {
            s.staged_ballot = Some(ballot);
        })
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.write_record(StorageRecord::Accepted(slot), |s| {
            s.staged_accepted.insert(slot, (ballot, command));
        })
    }

    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.write_record(StorageRecord::ChosenIndex, |s| {
            s.staged_chosen = Some(slot);
        })
    }

    fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        // A relaxed (chosen-index-only) batch keeps its stage un-flushed: it is
        // durable only once a later Sync flushes it, and lost on a crash before
        // then. A Sync batch flushes the whole stage through to the world.
        if must_sync != MustSync::Sync {
            // The classification contract, checked from the other side: a batch
            // allowed to skip the fsync can be holding no safety-critical write
            // (every promise-raise or accept classifies as `MustSync::Sync` and
            // was flushed by its own batch's sync).
            assert_always!(
                self.staged_ballot.is_none(),
                "a relaxed flush holds no staged promise"
            );
            assert_always!(
                self.staged_accepted.is_empty(),
                "a relaxed flush holds no staged accept"
            );
            assert_always!(
                self.staged_config_id.is_none(),
                "a relaxed flush holds no staged configuration identity"
            );
            return Ok(());
        }
        // The forced torn tail (its own BUGGIFY location): only a stage whose
        // fresh appends clear the per-record budget can tear, so the site is
        // consulted only where it can have an effect (AGENTS.md: consult a
        // hook only when the choice is observable).
        let force_torn = self.faults.active()
            && self.tearable_stage()
            && buggify_with_prob!(self.faults.rates.force_torn_tail);
        // BUGGIFY site 2: the fsync fails — only when the stage actually holds
        // something (an empty flush has nothing at stake). On the durable leg
        // the flush happens anyway before the error is reported (fsyncgate);
        // on the lost leg the stage stays un-flushed and dies with the
        // incarnation the driver's crash decision is about to unwind.
        if !self.staged_records().is_empty()
            && let Some(persisted) = self.roll_fsync_fault(force_torn)
        {
            if persisted {
                self.flush_stage()?;
            } else if self.faults.active()
                && (force_torn || sim_random::<f64>() < self.faults.rates.torn_tail)
            {
                if force_torn {
                    // BUGGIFY pairing: the forcing site genuinely fired.
                    assert_reachable!("storage: a torn tail is forced by its BUGGIFY site");
                }
                // Stage 7's per-record torn durability, paired with the crash
                // the driver is about to take on this error: a prefix of the
                // batch's fresh appends lands unwitnessed, so the next boot
                // scan walks the CrashTail rows of the disentanglement table.
                self.torn_flush();
            }
            return Err(StorageError::FsyncFailed {
                record: StorageRecord::Batch,
                outcome: WriteOutcome::Unknown,
            });
        }
        self.flush_stage()
    }

    fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        // Stage the floor like every other write: it reaches the durable world
        // only on the next Sync flush (Truncate classifies as MustSync::Sync).
        // The sealed ledger records ride in the same staged batch.
        let sealed = sealed.to_vec();
        self.write_record(StorageRecord::Truncation, |s| {
            s.staged_floor = Some(s.staged_floor.map_or(first, |f| f.max(first)));
            s.staged_sealed.extend_from_slice(&sealed);
        })
    }

    fn snapshot(&self) -> Vec<u8> {
        self.application.encode()
    }

    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        // Stage the install like every other write (InstallSnapshot is
        // MustSync::Sync): the chosen index, the adopted ballot, the floor
        // (`chosen_index + 1`), and the serving peer's session ledger reach the
        // durable world on the next Sync flush, where the floor is applied last
        // so it never outruns the chosen index.
        // A transferred snapshot that fails to decode is a mismatch on the
        // wire-to-disk path; if it ever fires, the injected⇔detected
        // correlation flags the uninjected detection as a bug.
        let installed = ChainState::decode(&snapshot).map_err(|_| StorageError::Corruption {
            record: StorageRecord::Snapshot,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        })?;
        assert_always!(
            installed.applied_slot() == Some(chosen_index),
            "chain: snapshot state matches its boundary"
        );
        assert_always!(
            installed.applied_count >= self.application.applied_count,
            "chain: snapshot install does not regress state"
        );
        let sessions = sessions.to_vec();
        self.write_record(StorageRecord::Snapshot, |s| {
            s.staged_sealed.extend_from_slice(&sessions);
            s.staged_chosen = Some(chosen_index);
            s.staged_ballot = Some(ballot);
            let first = Slot(chosen_index.0 + 1);
            s.staged_floor = Some(s.staged_floor.map_or(first, |f| f.max(first)));
            s.application = installed;
            s.staged_snapshot = Some(installed);
        })
    }

    fn apply(
        &mut self,
        chosen_index: Slot,
        slot: Slot,
        command: &Command,
    ) -> Result<(), StorageError> {
        assert_always!(
            slot <= chosen_index,
            "chain: apply does not outrun chosen prefix"
        );
        if self
            .application
            .applied_slot()
            .is_some_and(|applied| slot <= applied)
        {
            // The driver's boot replay re-walks the retained chosen prefix; a
            // node whose application state survived (clean reboot, or a crash
            // after the app fsync) skips the already-applied prefix here.
            assert_reachable!("a boot replay skips an already-applied slot");
            return Ok(());
        }
        let expected = self
            .application
            .applied_slot()
            .map_or(Slot(0), |applied| Slot(applied.0.saturating_add(1)));
        assert_always!(
            slot == expected,
            "chain: local application transition is contiguous"
        );
        let transition = self.application.apply(command);
        self.write_record(StorageRecord::Application(slot), |s| {
            s.application = transition.next;
            s.staged_applies.push(PendingApply { slot, transition });
        })
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.application.applied_slot()
    }

    fn record_snapshot(&mut self, at: Slot) -> Result<(), StorageError> {
        // Captured at the apply seam, when the staged application state IS the
        // marker's boundary state; durable with the batch's application fsync.
        self.staged_snap_point = Some((at, self.application));
        Ok(())
    }

    fn latest_snap_point(&self) -> Option<Slot> {
        if let Some((at, _)) = self.staged_snap_point {
            return Some(at);
        }
        let key = self.key.clone();
        self.with_world(|w| {
            w.disks
                .get(&key)
                .and_then(|d| d.snap_point.map(|(at, _)| Slot(at)))
        })
        .ok()
        .flatten()
    }

    fn snap_chunk_count(&self, at: Slot) -> Option<u32> {
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (point, state) = disk.snap_point?;
            (point == at.0).then(|| snap_chunk_count(state.encode().len()))
        })
        .ok()
        .flatten()
    }

    fn read_snap_chunk(&self, at: Slot, chunk: u32) -> Option<Vec<u8>> {
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (point, state) = disk.snap_point?;
            if point != at.0 {
                return None;
            }
            let index = usize::try_from(chunk).ok()?;
            // A rotted chunk answers nothing (silence, never garbage).
            if disk
                .snap_chunk_health
                .get(index)
                .is_some_and(|health| *health != RecordHealth::Clean)
            {
                return None;
            }
            let blob = state.encode();
            let start = index.checked_mul(SNAP_CHUNK_BYTES)?;
            if start >= blob.len() {
                return None;
            }
            let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
            Some(blob[start..end].to_vec())
        })
        .ok()
        .flatten()
    }

    fn write_snap_chunk(
        &mut self,
        at: Slot,
        chunk: u32,
        bytes: &[u8],
    ) -> Result<bool, StorageError> {
        let key = self.key.clone();
        let node = self.node_id;
        let bytes = bytes.to_vec();
        self.with_world(move |w| {
            let outcome = {
                let Some(disk) = w.disks.get_mut(&key) else {
                    return false;
                };
                let Some((point, state)) = disk.snap_point else {
                    return false;
                };
                if point != at.0 {
                    return false;
                }
                let Some(index) = usize::try_from(chunk).ok() else {
                    return false;
                };
                let blob = state.encode();
                let Some(start) = index.checked_mul(SNAP_CHUNK_BYTES) else {
                    return false;
                };
                if start >= blob.len() {
                    return false;
                }
                let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
                // The received chunk must be byte-identical to the decided
                // state — the identity the `Snap` marker exists to guarantee,
                // asserted against the world's ground truth.
                assert_always!(
                    bytes == blob[start..end],
                    "chain: a repaired snapshot chunk matches the decided point",
                    { "at" => at.0, "chunk" => chunk }
                );
                if bytes != blob[start..end] {
                    return false;
                }
                if disk.snap_chunk_health.len() <= index {
                    disk.snap_chunk_health.resize(
                        usize::try_from(snap_chunk_count(blob.len())).unwrap_or(0),
                        RecordHealth::Clean,
                    );
                }
                let was_faulty = disk.snap_chunk_health[index] != RecordHealth::Clean;
                // Models an atomic per-chunk file replace; the driver flushes
                // right after installing a response's chunks.
                disk.snap_chunk_health[index] = RecordHealth::Clean;
                let all_clean = disk
                    .snap_chunk_health
                    .iter()
                    .all(|health| *health == RecordHealth::Clean);
                (was_faulty, all_clean)
            };
            let (was_faulty, all_clean) = outcome;
            if was_faulty {
                w.note_recovered(node, StorageRecord::SnapChunk(at, chunk));
            }
            all_clean
        })
    }

    fn faulty_snap_chunks(&self) -> Vec<(Slot, u32)> {
        self.faulty_chunks.clone()
    }

    fn restore_from_snap_point(&mut self) -> Result<Option<Slot>, StorageError> {
        let key = self.key.clone();
        let candidate = self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (at, state) = disk.snap_point?;
            let all_clean = disk
                .snap_chunk_health
                .iter()
                .all(|health| *health == RecordHealth::Clean);
            // The point must be whole and must cover the compaction floor —
            // replay from the floor is contiguous only from `at + 1`.
            (all_clean && at + 1 >= disk.first_slot.0).then_some((at, state))
        })?;
        let Some((at, state)) = candidate else {
            return Ok(None);
        };
        if self
            .application
            .applied_slot()
            .is_some_and(|applied| applied.0 >= at)
        {
            return Ok(None);
        }
        // Stage the restored state exactly like a snapshot install: the flush
        // sets the durable application state, heals the live-snapshot health,
        // and emits the `chain_snapshot_installed` fact.
        self.application = state;
        self.staged_snapshot = Some(state);
        Ok(Some(Slot(at)))
    }
}

impl<T: TimeProvider> Storage for DurableStorage<T> {
    fn initial_state(&self) -> (HardState, Config) {
        self.boot.initial_state()
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.boot.accepted(slot)
    }
    fn first_slot(&self) -> Slot {
        self.boot.first_slot()
    }
    fn last_slot(&self) -> Slot {
        self.boot.last_slot()
    }
    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.boot.sealed_sessions()
    }
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty_list.clone()
    }
}
