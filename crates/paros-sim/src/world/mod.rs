//! The fake disk: every node's durable records, the fault ledgers, and the
//! budgets that keep a run winnable.
//!
//! The [`StorageWorld`] is **protocol-blind** — it stores records, never knowing
//! what is committed — and outlives process crashes (owned by the `StateHandle`),
//! so a write that reached it before a crash is read back on restart, exactly
//! like a real disk. Each node reaches it through a [`storage::DurableStorage`]
//! handle; the boot-rot sites live in [`rot`].

pub(crate) mod matchmaker;
pub(crate) mod rot;
pub(crate) mod storage;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};

use crate::audit::audit_world;
use crate::chain::ChainState;
use paros::{
    Ballot, ClientId, ClientSeq, Command, HardState, IntegrityFault, MetadataFault, Slot,
    StorageRecord, WitnessStatus, snap_chunk_count,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";

/// Get-or-create the singleton [`StorageWorld`] for this iteration. Get-then-
/// publish is race-free: the sim executor is single-threaded and this runs
/// synchronously (no `.await` between the get and the publish).
pub(crate) fn storage_world(state: &StateHandle) -> Arc<Mutex<StorageWorld>> {
    if let Some(world) = state.get::<Arc<Mutex<StorageWorld>>>(STORAGE_WORLD_KEY) {
        return world;
    }
    let world = Arc::new(Mutex::new(StorageWorld::default()));
    state.publish(STORAGE_WORLD_KEY, world.clone());
    world
}

/// Semantic health of one durable record — the world stores **records, not
/// bytes** (#20 fixed decision), so every corruption-family member is modeled
/// as a first-class read outcome the boot scan classifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum RecordHealth {
    /// The record verifies.
    #[default]
    Clean,
    /// The bytes fail their checksum (bit flip, latent sector error, torn
    /// write).
    Faulty,
    /// The checksum passes but the identity inside the checksummed region
    /// names a different record (misdirected write).
    Misdirected,
    /// The bytes are absent where the identifier / reserved-record contract
    /// says they must exist (lost write).
    Lost,
}

impl RecordHealth {
    fn integrity_fault(self) -> Option<IntegrityFault> {
        match self {
            RecordHealth::Clean => None,
            RecordHealth::Faulty => Some(IntegrityFault::ChecksumMismatch),
            RecordHealth::Misdirected => Some(IntegrityFault::Misdirected),
            RecordHealth::Lost => Some(IntegrityFault::LostWrite),
        }
    }
}

/// One accepted entry's record + persist-witness health.
#[derive(Clone, Copy, Debug)]
pub(super) struct SlotHealth {
    entry: RecordHealth,
    id: WitnessStatus,
}

impl Default for SlotHealth {
    fn default() -> Self {
        Self {
            entry: RecordHealth::Clean,
            id: WitnessStatus::Present,
        }
    }
}

impl SlotHealth {
    fn clean(self) -> bool {
        self.entry == RecordHealth::Clean && self.id == WitnessStatus::Present
    }
}

/// One node's durable records: the scalars, the per-slot accepted log, and the
/// compaction floor. The [`StorageWorld`] owns one of these per node IP.
#[derive(Default)]
pub(super) struct NodeDisk {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// The first slot still retained. Everything below it has been truncated.
    first_slot: Slot,
    /// Application-produced snapshot state, durable across clean reboot.
    chain: ChainState,
    /// Sealed at-most-once ledger records for truncated slots (#94): read back
    /// on boot so a restart suppresses re-chosen identities like every peer.
    sealed: BTreeMap<(ClientId, ClientSeq), Slot>,
    /// Health of the accepted-entry records; a slot absent from this map is
    /// clean and witnessed.
    entry_health: BTreeMap<Slot, SlotHealth>,
    /// The two checksummed `HardState` copies (CTRL metainfo doctrine): one
    /// bad ⇒ repair from the twin, both bad ⇒ crash.
    promise_health: [RecordHealth; 2],
    chosen_health: RecordHealth,
    truncation_health: RecordHealth,
    snapshot_health: RecordHealth,
    /// A file-granularity FS-metadata fault on the whole store.
    meta_fault: Option<MetadataFault>,
    /// One pending transient read-`EIO` target, cleared when it surfaces (the
    /// retry — the next boot — reads clean).
    read_eio: Option<StorageRecord>,
    /// The latest **decided snapshot point** (#101): the `Snap` marker's slot
    /// and the byte-identical boundary state every node captured there. Only
    /// the latest point is retained.
    snap_point: Option<(u64, ChainState)>,
    /// Per-chunk health of the retained point's blob (fixed
    /// [`SNAP_CHUNK_BYTES`] chunking of the encoded state).
    snap_chunk_health: Vec<RecordHealth>,
}

impl NodeDisk {
    /// Health of the accepted record at `slot` (clean + witnessed when
    /// untracked).
    fn slot_health(&self, slot: Slot) -> SlotHealth {
        self.entry_health.get(&slot).copied().unwrap_or_default()
    }
}

/// One injected storage fault: the **ground truth** the oracles compare
/// against. The node only ever sees the ambiguous typed error; whether the
/// effect actually persisted lives here, in the world.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InjectedFault {
    pub(crate) node: u64,
    pub(crate) record: StorageRecord,
    pub(crate) kind: InjectedFaultKind,
    /// The world's seeded, independent decision: did the effect reach the
    /// durable disk despite the reported error?
    pub(crate) persisted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InjectedFaultKind {
    /// A per-record write returned `EIO`.
    WriteEio,
    /// The staged batch's fsync failed.
    FsyncFailed,
}

/// One Stage-7 corruption injection — the family it models (issue #20 item D;
/// each kind is its own independent BUGGIFY location).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptionKind {
    /// Bit flip / latent sector error on a persisted record.
    BitFlip,
    /// A write acknowledged clean that never reached the medium.
    LostWrite,
    /// A block-aligned write landing on the wrong record.
    Misdirected,
    /// A transient `EIO` on the read path.
    ReadEio,
    /// A torn un-synced batch: fresh appends durable without identifiers.
    TornTail,
    /// One (or both) `HardState` copies rotted.
    PromiseCopy,
    /// A file-granularity FS-metadata fault.
    Metadata,
}

/// What became of one Stage-7 injection — the exercised-vs-dormant tracking
/// the injected⇔detected oracle folds (an injected fault on a never-read
/// record is not a detection failure).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptionOutcome {
    /// Injected but not read back yet (legal only on a node that parked
    /// before the read, or a torn tail the run ended before rebooting).
    Dormant,
    /// Read, detected, and surfaced as the driver's typed crash decision.
    Crashed,
    /// Detected in the same scan as a [`Crashed`](CorruptionOutcome::Crashed)
    /// record and covered by that one crash decision (a block fault's other
    /// members, the second promise copy).
    CoDetected,
    /// Read, detected, and repaired from the twin copy (`HardState` only).
    Repaired,
    /// Read, classified crash-truncatable, and discarded locally (the one
    /// legal discard: the record was never acknowledged to anyone).
    DiscardedTail,
    /// Stage 8: read, classified recoverable — identity known, value lost —
    /// and **reported** into the protocol's tri-state instead of crashing.
    /// The record stays faulty on disk until a clean re-write resolves it to
    /// [`Recovered`](CorruptionOutcome::Recovered) (or truncation supersedes
    /// it); a run may legally end with a report still standing (the WAITED
    /// leg, or a run that simply ended first).
    Reported,
    /// Stage 8: the faulty record was genuinely re-written by a clean flush
    /// (an in-place repair — a re-sent `Accept`, a learned chosen value, a
    /// decided no-op) or superseded by truncation/snapshot custodianship.
    Recovered,
}

/// Ground truth of one Stage-7 corruption injection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CorruptionInjection {
    pub(crate) node: u64,
    pub(crate) record: StorageRecord,
    pub(crate) kind: CorruptionKind,
    /// Part of a multi-record block fault (a contiguous run of entries).
    pub(crate) block: bool,
    pub(crate) outcome: CorruptionOutcome,
}

/// Sticky per-family / per-verdict facts for the Stage-7 coverage gates,
/// recorded at the detection instant and read once per run by
/// [`check_storage_gates`]. Independent bits, not a state machine (the
/// [`crate::audit`] flag-set waiver).
#[derive(Default, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct Stage7Flags {
    bitflip_detected: bool,
    lost_write_detected: bool,
    misdirected_detected: bool,
    read_eio_detected: bool,
    torn_tail_discarded: bool,
    snapshot_detected: bool,
    promise_repaired: bool,
    metadata_crashed: bool,
    corruption_below_tail: bool,
    last_entry_ambiguity: bool,
    identifier_lost: bool,
    // --- Stage 8 (issue #21) sticky facts -----------------------------------
    /// A recoverable rotted record was reported into the tri-state.
    faulty_reported: bool,
    /// A reported record was genuinely re-written by a clean flush.
    record_recovered: bool,
    /// A corrupted application snapshot was reset for local log replay
    /// (floor = 0: CTRL's cheap path).
    snapshot_reset_local: bool,
    /// A corrupted application snapshot was reset under a truncated log
    /// (recovery requires a peer's `InstallSnapshot`).
    snapshot_reset_remote: bool,
}

/// The per-iteration durable-storage world: every node's durable records, keyed
/// by IP. It is **protocol-blind** — it stores records, never knowing what is
/// committed. It outlives process crashes (owned by the `StateHandle`), so a
/// write that reached it before a crash is read back on restart, exactly like a
/// real disk.
///
/// This is also where Stage 6's **budgeted** storage faults are rolled (the
/// caller-side layer of #70/#71; moonpool's provider-level storage chaos stays
/// on as *environmental* pressure). The budget is per-record and cluster-wide:
/// for each accepted-log record, at most `quorum − 1` copies may be
/// fault-suppressed across the cluster — re-counted over **live** copies at
/// injection time (a node that truncated past a slot no longer holds a copy;
/// the `TigerBeetle` `ClusterFaultAtlas` correction) and `assert_always!`ed
/// rather than trusted to construction. Suppression semantics: once the chaos
/// window closes the world refuses **new** injections but never heals the
/// consequences of old ones (a mark clears only when the node genuinely
/// re-writes the record through a clean flush) — recovery stays genuine.
#[derive(Default)]
pub(crate) struct StorageWorld {
    disks: BTreeMap<String, NodeDisk>,
    /// The matchmakers' durable registries, keyed by IP (see
    /// [`matchmaker::DurableMatchmakerStorage`]); empty on a plain seed.
    matchmakers: BTreeMap<String, matchmaker::MatchmakerDisk>,
    /// Full cluster membership size, for the quorum bound (set once at boot;
    /// zero refuses every injection).
    cluster_size: usize,
    /// The application's digest-lane count for this run (see
    /// [`ChainState::lane_count`]): every node's blob must slice identically,
    /// so it is one per-run value, published by the first node to boot.
    lane_count: Option<u8>,
    /// Ground truth of every permitted injection, in order.
    injected: Vec<InjectedFault>,
    /// Lost-leg fault marks per node: accepted-log records whose most recent
    /// write was injected-lost, torn, or corrupted, and not yet re-written by
    /// a clean flush (or discarded as a crash tail). These are what the
    /// budget counts.
    marks: BTreeMap<String, BTreeSet<u64>>,
    /// Ground truth of every Stage-7 corruption injection, in order.
    corruptions: Vec<CorruptionInjection>,
    /// Nodes terminally crashed by a detected persistent fault (detect ⇒
    /// crash; restarting cannot help a store whose record genuinely rotted).
    /// Bounded by [`StorageWorld::dead_budget`] so a live quorum survives.
    parked: BTreeSet<String>,
    /// The same set by numeric node id, for correlating the injection ledger.
    parked_ids: BTreeSet<u64>,
    /// Sticky Stage-7 gate facts.
    s7: Stage7Flags,
    /// Unbudgeted mode (the scripted corpus): a targeted injection may take
    /// every copy of a record; each slot driven to zero readable copies is
    /// recorded in `unrecoverable`, the ground truth the corpus's analytic
    /// derivation is cross-checked against.
    unbudgeted: bool,
    /// Slots with no readable copy anywhere — no clean log record on any node
    /// and no post-truncation snapshot covering them (only ever populated in
    /// unbudgeted runs).
    unrecoverable: BTreeSet<u64>,
}

impl StorageWorld {
    /// Whether `ip` was terminally parked (see `crate::process`).
    pub(crate) fn is_parked(&self, ip: &str) -> bool {
        self.parked.contains(ip)
    }

    /// How many nodes *other than* `ip` are terminally parked — the persistent
    /// half of the "parked peer + transient process loss" overlap a restarting
    /// node reports to the audit.
    pub(crate) fn parked_count_excluding(&self, ip: &str) -> usize {
        self.parked
            .iter()
            .filter(|parked| parked.as_str() != ip)
            .count()
    }

    /// Fix this run's digest-lane count (first caller wins; the corpus pins
    /// the default, the main campaign draws a knob).
    pub(crate) fn set_lane_count(&mut self, lane_count: u8) {
        if self.lane_count.is_none() {
            self.lane_count = Some(lane_count);
        }
    }

    /// The run's digest-lane count.
    pub(crate) fn lane_count(&self) -> u8 {
        self.lane_count.unwrap_or(crate::chain::DEFAULT_LANES)
    }

    /// The disk under `key`, created on first touch with an empty application
    /// state at the run's lane count — every node's blob must slice
    /// identically, so a disk is never born with the default count.
    pub(super) fn disk_mut(&mut self, key: &str) -> &mut NodeDisk {
        let lanes = self.lane_count();
        self.disks
            .entry(key.to_string())
            .or_insert_with(|| NodeDisk {
                chain: ChainState::empty(lanes),
                ..NodeDisk::default()
            })
    }

    /// Declare the unbudgeted (corpus) mode: masks may exceed the per-record
    /// budget, and every injection records the unrecoverable ground truth.
    pub(crate) fn set_unbudgeted(&mut self) {
        self.unbudgeted = true;
    }

    pub(crate) fn set_cluster_size(&mut self, n: usize) {
        if self.cluster_size == 0 {
            self.cluster_size = n;
        }
        assert_always!(
            self.cluster_size == n,
            "storage: every node derives the same cluster size"
        );
    }

    fn quorum(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Clean live copies of the accepted-log record at `slot`: cluster members
    /// that are neither fault-marked for it, truncated past it, nor terminally
    /// parked by a detected corruption. A node the world has never seen a
    /// flush from is a clean potential copy.
    fn clean_copies(&self, slot: u64) -> usize {
        let mut unclean: BTreeSet<&String> = BTreeSet::new();
        for (node, marks) in &self.marks {
            if marks.contains(&slot) {
                unclean.insert(node);
            }
        }
        for (node, disk) in &self.disks {
            if disk.first_slot.0 > slot {
                unclean.insert(node);
            }
        }
        for node in &self.parked {
            unclean.insert(node);
        }
        self.cluster_size.saturating_sub(unclean.len())
    }

    /// How many nodes a run may terminally lose to detected corruption while
    /// keeping a live quorum.
    fn dead_budget(&self) -> usize {
        self.cluster_size.saturating_sub(self.quorum())
    }

    /// Whether terminally parking `node_key` (detect ⇒ crash, stays down) is
    /// inside the dead-node budget AND leaves every accepted record the node
    /// still holds with a clean quorum of live copies elsewhere. Checked
    /// *before* a persistent-corruption injection, so the availability cost is
    /// paid only where the cluster can absorb it.
    fn may_park(&self, node_key: &str) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.parked.contains(node_key) {
            return true;
        }
        if self.parked.len() + 1 > self.dead_budget() {
            return false;
        }
        let quorum = self.quorum();
        if let Some(disk) = self.disks.get(node_key) {
            for slot in disk.accepted.keys() {
                let already_unclean = self
                    .marks
                    .get(node_key)
                    .is_some_and(|marks| marks.contains(&slot.0));
                let hypothetical = usize::from(!already_unclean);
                if self.clean_copies(slot.0).saturating_sub(hypothetical) < quorum {
                    return false;
                }
            }
        }
        // The accepted-map walk above misses slots this node no longer holds
        // (truncated past) or never flushed — but the availability
        // re-derivation counts *every* parked node unclean for *every* marked
        // slot (a dead node serves neither the record nor its superseding
        // snapshot). Close the composition hole: for each slot marked faulty
        // anywhere in the cluster, parking this node must still leave that
        // slot its clean quorum under the re-derivation's own formula.
        for slot in self.marks.values().flatten() {
            let mut unclean: BTreeSet<&str> = self
                .marks
                .iter()
                .filter(|(_, marks)| marks.contains(slot))
                .map(|(node, _)| node.as_str())
                .chain(self.parked.iter().map(String::as_str))
                .collect();
            unclean.insert(node_key);
            if self.cluster_size.saturating_sub(unclean.len()) < quorum {
                return false;
            }
        }
        true
    }

    /// Terminally park a node: detect ⇒ crash, and it stays down.
    #[tracing::instrument(level = "debug", skip(self), fields(key = %key, node))]
    fn park(&mut self, key: &str, node: u64) {
        self.parked.insert(key.to_string());
        self.parked_ids.insert(node);
    }

    /// Whether a **recoverable** corruption of the accepted record at `slot`
    /// on `node_key` is inside the per-record budget: the record must keep a
    /// clean quorum of live copies (#70's rule, re-counted over live copies at
    /// injection time). The unbudgeted corpus lifts the bound, and
    /// [`StorageWorld::note_if_unrecoverable`] records the ground truth its
    /// analytic derivation is checked against.
    fn may_corrupt_record(&self, node_key: &str, slot: u64) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.unbudgeted {
            return true;
        }
        let already = self
            .marks
            .get(node_key)
            .is_some_and(|marks| marks.contains(&slot));
        self.clean_copies(slot)
            .saturating_sub(usize::from(!already))
            >= self.quorum()
    }

    /// Whether `slot` has **no readable copy anywhere**: every disk that holds
    /// its accepted record holds it unhealthy or sits on a parked node, no
    /// disk truncated past it with a healthy snapshot (the snapshot covers the
    /// folded prefix), no disk retains a fully clean decided snapshot point
    /// covering it (#101), and no disk holds it clean. Ground truth for the
    /// corpus.
    fn slot_unrecoverable(&self, slot: u64) -> bool {
        for (key, disk) in &self.disks {
            if self.parked.contains(key) {
                continue;
            }
            if disk.first_slot.0 > slot && disk.snapshot_health == RecordHealth::Clean {
                return false;
            }
            if disk.accepted.contains_key(&Slot(slot)) && disk.slot_health(Slot(slot)).clean() {
                return false;
            }
        }
        // #101: a decided snapshot point at or past the slot is custody too —
        // and because the point is byte-identical cluster-wide, chunk repair
        // reassembles it from *any* clean copy of each chunk, so the clause is
        // cluster-assembled, never per-disk: the point is readable unless some
        // chunk has no clean copy on any live holder.
        let points: BTreeSet<u64> = self
            .disks
            .iter()
            .filter(|(key, _)| !self.parked.contains(*key))
            .filter_map(|(_, disk)| disk.snap_point.map(|(at, _)| at))
            .filter(|at| *at >= slot)
            .collect();
        for at in points {
            let chunk_count = self
                .disks
                .values()
                .find_map(|disk| {
                    disk.snap_point
                        .filter(|(point, _)| *point == at)
                        .map(|(_, state)| snap_chunk_count(state.encode().len()))
                })
                .unwrap_or(0);
            let assemblable = (0..chunk_count).all(|chunk| {
                self.disks.iter().any(|(key, disk)| {
                    !self.parked.contains(key)
                        && disk.snap_point.is_some_and(|(point, _)| point == at)
                        && disk
                            .snap_chunk_health
                            .get(usize::try_from(chunk).unwrap_or(usize::MAX))
                            .is_none_or(|health| *health == RecordHealth::Clean)
                })
            });
            if assemblable {
                return false;
            }
        }
        true
    }

    /// After an unbudgeted injection touched `slot`, record it unrecoverable if
    /// no readable copy survives.
    fn note_if_unrecoverable(&mut self, slot: u64) {
        if self.unbudgeted && !self.unrecoverable.contains(&slot) && self.slot_unrecoverable(slot) {
            self.unrecoverable.insert(slot);
            tracing::info!(slot, "slot_unrecoverable");
        }
    }

    /// A clean flush (or truncation custodianship) genuinely resolved the
    /// standing report on `(node, record)`.
    fn note_recovered(&mut self, node: u64, record: StorageRecord) {
        for injection in &mut self.corruptions {
            if injection.node == node
                && injection.record == record
                && matches!(
                    injection.outcome,
                    CorruptionOutcome::Dormant | CorruptionOutcome::Reported
                )
            {
                injection.outcome = CorruptionOutcome::Recovered;
                self.s7.record_recovered = true;
            }
        }
    }

    /// Record one Stage-7 injection's ground truth.
    fn note_corruption(&mut self, injection: CorruptionInjection) {
        tracing::info!(
            node = injection.node,
            record = %injection.record,
            kind = ?injection.kind,
            block = injection.block,
            "corruption_injected"
        );
        self.corruptions.push(injection);
    }

    /// Resolve the dormant ledger entries matching `(node, record)` — the boot
    /// scan read them back. The first match takes `outcome`; when `outcome` is
    /// [`CorruptionOutcome::Crashed`], further matches co-resolve (one crash
    /// decision covers the whole scan).
    fn resolve_corruption(&mut self, node: u64, record: StorageRecord, outcome: CorruptionOutcome) {
        let mut first = true;
        for injection in &mut self.corruptions {
            if injection.node == node
                && injection.record == record
                && injection.outcome == CorruptionOutcome::Dormant
            {
                injection.outcome = if first {
                    outcome
                } else {
                    CorruptionOutcome::CoDetected
                };
                first = false;
                // A resolved member of a multi-record block fault proves the
                // contiguous-run family was genuinely read back and detected.
                // A reachable anchor, NOT a sometimes: the composition needs a
                // log still holding a contiguous clean run when the rot site
                // fires, and truncation-heavy runs keep logs short — CI's
                // coverage-guided schedule starved a per-sweep gate on it
                // (0/452 checks, run 33117691030) even after the sub-roll was
                // widened, while every other family gate fired. Per the
                // assertion doctrine, a leg the sweep is not *certain* to
                // reach anchors exploration when hit and never fails coverage.
                if injection.block {
                    assert_reachable!(
                        "storage: a block fault corrupts a contiguous run of entries"
                    );
                }
            }
        }
    }

    /// Targeted, budget-checked corruption of one durable record — issue
    /// #21's single-injection API (`corrupt(node, record)`): the adversarial
    /// promise-corruption test is one call. Returns whether the budget
    /// permitted it.
    #[allow(dead_code)] // armed for #21's targeted tests; the swarm sites drive it today
    #[tracing::instrument(level = "debug", skip(self), fields(node_key = %node_key, node, record = %record))]
    pub(crate) fn corrupt(&mut self, node_key: &str, node: u64, record: StorageRecord) -> bool {
        if !self.may_park(node_key) {
            return false;
        }
        let Some(disk) = self.disks.get_mut(node_key) else {
            return false;
        };
        let kind = match record {
            StorageRecord::Accepted(slot) => {
                if !disk.accepted.contains_key(&slot) {
                    return false;
                }
                disk.entry_health.insert(
                    slot,
                    SlotHealth {
                        entry: RecordHealth::Faulty,
                        id: WitnessStatus::Present,
                    },
                );
                self.marks
                    .entry(node_key.to_string())
                    .or_default()
                    .insert(slot.0);
                CorruptionKind::BitFlip
            }
            StorageRecord::Promise => {
                disk.promise_health = [RecordHealth::Faulty, RecordHealth::Faulty];
                CorruptionKind::PromiseCopy
            }
            StorageRecord::ChosenIndex => {
                disk.chosen_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            StorageRecord::Truncation => {
                disk.truncation_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            StorageRecord::Snapshot => {
                disk.snapshot_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            _ => return false,
        };
        self.park(node_key, node);
        self.note_corruption(CorruptionInjection {
            node,
            record,
            kind,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
        true
    }

    /// Decide one injection under the budget. `accepted_slots` are the
    /// accepted-log records the fault would suppress on `node` when the lost
    /// leg is taken; the check is conservative (it hypothesizes the loss even
    /// when the seeded outcome will be "persisted anyway"). Returns whether
    /// the injection is permitted, recording ground truth and marks if so.
    fn permit_and_record(
        &mut self,
        node_key: &str,
        fault: InjectedFault,
        accepted_slots: &[u64],
    ) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        let quorum = self.quorum();
        // Per-record, cluster-wide budget over live copies: refuse an injection
        // that would leave any touched record with fewer than `quorum` clean
        // copies.
        let marked = |slot: &u64| {
            self.marks
                .get(node_key)
                .is_some_and(|marks| marks.contains(slot))
        };
        for slot in accepted_slots {
            let hypothetical_loss = usize::from(!marked(slot));
            if self.clean_copies(*slot).saturating_sub(hypothetical_loss) < quorum {
                return false;
            }
        }
        // Second-order cap: never fault every accepted record of one node — a
        // fully-faulted node is permanent unavailability, not a recoverable
        // state (`TigerBeetle`'s "never corrupt all chunks of a replica").
        if let Some(disk) = self.disks.get(node_key)
            && !disk.accepted.is_empty()
        {
            let all_marked = disk.accepted.keys().all(|slot| {
                accepted_slots.contains(&slot.0)
                    || self
                        .marks
                        .get(node_key)
                        .is_some_and(|marks| marks.contains(&slot.0))
            });
            if all_marked {
                return false;
            }
        }
        if !fault.persisted {
            let marks = self.marks.entry(node_key.to_string()).or_default();
            marks.extend(accepted_slots.iter().copied());
        }
        // The injection's ground truth, surfaced for the trace (the node itself
        // only ever learns the ambiguous error).
        tracing::info!(
            node = fault.node,
            record = %fault.record,
            kind = match fault.kind {
                InjectedFaultKind::WriteEio => "write_eio",
                InjectedFaultKind::FsyncFailed => "fsync_failed",
            },
            persisted = fault.persisted,
            "storage_fault_injected"
        );
        self.injected.push(fault);
        // The budget is asserted, not assumed: after every permitted
        // injection, each touched record still has a clean quorum, and the
        // node still holds at least one clean accepted record if it holds any.
        for slot in accepted_slots {
            let clean = self.clean_copies(*slot);
            assert_always!(
                clean >= quorum,
                "storage: the per-record fault budget keeps a clean quorum of live copies",
                { "slot" => *slot, "clean" => clean as u64, "quorum" => quorum as u64 }
            );
        }
        if let Some(disk) = self.disks.get(node_key)
            && !disk.accepted.is_empty()
        {
            let any_clean = disk.accepted.keys().any(|slot| {
                !self
                    .marks
                    .get(node_key)
                    .is_some_and(|marks| marks.contains(&slot.0))
            });
            assert_always!(
                any_clean,
                "storage: a fault never blankets every record of one node"
            );
        }
        true
    }

    /// A clean flush re-wrote these accepted records on `node`: their lost-leg
    /// marks clear (the copy is durably real again). This is recovery doing
    /// its job, not the world healing anything.
    fn clear_marks(&mut self, node_key: &str, accepted_slots: impl Iterator<Item = u64>) {
        if let Some(marks) = self.marks.get_mut(node_key) {
            for slot in accepted_slots {
                marks.remove(&slot);
            }
        }
    }
}

// --- storage-fault ground truth for the oracles -------------------------------

/// Snapshot of the [`StorageWorld`]'s injected-fault ground truth, folded for
/// the workload's `check()` phase.
///
/// The bools are independent sticky quadrant facts, not a state machine (same
/// waiver as the audit's flag set).
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct StorageFaultStats {
    /// Permitted injections, in order (the ground truth ledger).
    pub(crate) injected: usize,
    /// Sticky per-quadrant facts (item C: all four ambiguity quadrants).
    pub(crate) eio_landed: bool,
    pub(crate) eio_lost: bool,
    pub(crate) fsync_durable: bool,
    pub(crate) fsync_lost: bool,
    /// Re-derived from current world state: every fault-marked accepted
    /// record still has at least a quorum of clean live copies. Under the
    /// injection-time budget this must always hold; it is re-derived
    /// independently so an unavailable run can be judged against it
    /// (`TigerBeetle`'s excuse-list doctrine — the bound is never trusted to be
    /// sufficient on its own).
    pub(crate) clean_quorum_everywhere: bool,
}

/// Fold the storage world's ground truth (empty world = no faults).
pub(crate) fn storage_fault_stats(handle: &StateHandle) -> StorageFaultStats {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut stats = StorageFaultStats {
        injected: guard.injected.len(),
        eio_landed: false,
        eio_lost: false,
        fsync_durable: false,
        fsync_lost: false,
        clean_quorum_everywhere: true,
    };
    for fault in &guard.injected {
        match (fault.kind, fault.persisted) {
            (InjectedFaultKind::WriteEio, true) => stats.eio_landed = true,
            (InjectedFaultKind::WriteEio, false) => stats.eio_lost = true,
            (InjectedFaultKind::FsyncFailed, true) => stats.fsync_durable = true,
            (InjectedFaultKind::FsyncFailed, false) => stats.fsync_lost = true,
        }
    }
    let quorum = guard.quorum();
    let marked: BTreeSet<u64> = guard
        .marks
        .values()
        .flat_map(|marks| marks.iter().copied())
        .collect();
    for slot in marked {
        // The availability re-derivation deliberately differs from the
        // injection-time budget formula: here a peer that truncated past the
        // slot counts as *clean* — its truncation was decided over the applied
        // prefix, so it can serve the superseding snapshot — while only a
        // live lost-write/corruption mark or a terminally parked node makes a
        // copy unclean. The budget stays conservative (truncated peers don't
        // count there); this independent count is what an unavailable run is
        // judged against.
        let unclean: BTreeSet<&String> = guard
            .marks
            .iter()
            .filter(|(_, marks)| marks.contains(&slot))
            .map(|(node, _)| node)
            .chain(guard.parked.iter())
            .collect();
        if guard.cluster_size.saturating_sub(unclean.len()) < quorum {
            // Red-path diagnostic: name the slot and the unclean set, so an
            // availability violation is attributable without a re-run.
            eprintln!(
                "clean-quorum lost: slot={slot} unclean={unclean:?} parked={:?} marks={:?}",
                guard.parked, guard.marks
            );
            stats.clean_quorum_everywhere = false;
        }
    }
    stats
}

/// Slots an unbudgeted (corpus) run drove to zero readable copies; empty on
/// the budgeted main campaign.
pub(crate) fn unrecoverable_slots(handle: &StateHandle) -> BTreeSet<u64> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.unrecoverable.clone()
}

/// The IPs of nodes terminally parked by a detected persistent corruption
/// (detect ⇒ crash, stays down). The workload's convergence probe skips
/// exactly these — the availability cost Stage 7's baseline deliberately pays,
/// bounded by the dead-node budget so the cluster keeps serving.
pub(crate) fn parked_nodes(handle: &StateHandle) -> BTreeSet<String> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.parked.clone()
}

// --- corpus support: world probes + targeted mask injection -------------------

/// One node's durable evidence, for the corpus workloads' deterministic waits
/// (world-truth probes: replication, floors, and the durable application
/// state — the corpus still verifies final outcomes over live RPC reads).
pub(crate) struct CorpusDiskProbe {
    /// Retained accepted slots whose record reads back clean and witnessed.
    pub(crate) clean_slots: BTreeSet<u64>,
    /// The durable compaction floor.
    pub(crate) floor: u64,
    /// The durable application state's applied count.
    pub(crate) applied_count: u64,
    /// The durable application state's chain digest.
    pub(crate) chain_hash: u64,
    /// The retained decided snapshot point, if any (#101).
    pub(crate) snap_point: Option<u64>,
    /// The retained point's rotted chunk indexes.
    pub(crate) faulty_chunks: BTreeSet<u32>,
}

pub(crate) fn corpus_disk_probe(handle: &StateHandle, ip: &str) -> Option<CorpusDiskProbe> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.disks.get(ip).map(|disk| CorpusDiskProbe {
        clean_slots: disk
            .accepted
            .keys()
            .filter(|slot| disk.slot_health(**slot).clean())
            .map(|slot| slot.0)
            .collect(),
        floor: disk.first_slot.0,
        applied_count: disk.chain.applied_count,
        chain_hash: disk.chain.chain_hash,
        snap_point: disk.snap_point.map(|(at, _)| at),
        faulty_chunks: disk
            .snap_chunk_health
            .iter()
            .enumerate()
            .filter(|(_, health)| **health != RecordHealth::Clean)
            .map(|(index, _)| u32::try_from(index).unwrap_or(u32::MAX))
            .collect(),
    })
}

/// Targeted chunk corruption of the retained decided snapshot point (#101):
/// one chunk's value lost, the point's identity — and every other chunk —
/// intact. Unbudgeted like every corpus injection; below-floor slots are
/// re-evaluated against the world's unrecoverable ground truth (the point is
/// custody, so losing its last clean copy can strand a folded prefix).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn corpus_corrupt_snap_chunk(
    handle: &StateHandle,
    ip: &str,
    node: u64,
    chunk: u32,
) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let (at, floor) = {
        let Some(disk) = guard.disks.get_mut(ip) else {
            return false;
        };
        let Some((at, state)) = disk.snap_point else {
            return false;
        };
        let chunks = usize::try_from(snap_chunk_count(state.encode().len())).unwrap_or(0);
        let Some(index) = usize::try_from(chunk).ok().filter(|index| *index < chunks) else {
            return false;
        };
        if disk.snap_chunk_health.len() < chunks {
            disk.snap_chunk_health.resize(chunks, RecordHealth::Clean);
        }
        if disk.snap_chunk_health[index] != RecordHealth::Clean {
            return false;
        }
        disk.snap_chunk_health[index] = RecordHealth::Faulty;
        (at, disk.first_slot.0)
    };
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::SnapChunk(Slot(at), chunk),
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    for slot in 0..floor {
        guard.note_if_unrecoverable(slot);
    }
    true
}

/// Targeted E1 mask corruption of one accepted record — value lost, identity
/// preserved (the recoverable class). Deliberately unbudgeted: the world
/// records the unrecoverable ground truth the analytic mask derivation
/// cross-checks. Returns whether a clean record was
/// there to corrupt.
#[tracing::instrument(level = "debug", skip(handle), fields(ip = %ip, node, slot))]
pub(crate) fn corpus_corrupt_entry(handle: &StateHandle, ip: &str, node: u64, slot: u64) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(disk) = guard.disks.get_mut(ip) else {
        return false;
    };
    if !disk.accepted.contains_key(&Slot(slot)) || !disk.slot_health(Slot(slot)).clean() {
        return false;
    }
    disk.entry_health.insert(
        Slot(slot),
        SlotHealth {
            entry: RecordHealth::Faulty,
            id: WitnessStatus::Present,
        },
    );
    guard.marks.entry(ip.to_string()).or_default().insert(slot);
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::Accepted(Slot(slot)),
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    guard.note_if_unrecoverable(slot);
    true
}

/// Targeted corruption of one node's durable application snapshot. Slots the
/// node had truncated past lose their only local custody, so each is
/// re-evaluated against the world's unrecoverable ground truth.
#[tracing::instrument(level = "debug", skip(handle), fields(ip = %ip, node))]
pub(crate) fn corpus_corrupt_snapshot(handle: &StateHandle, ip: &str, node: u64) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(disk) = guard.disks.get_mut(ip) else {
        return false;
    };
    disk.snapshot_health = RecordHealth::Faulty;
    let floor = disk.first_slot.0;
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::Snapshot,
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    for slot in 0..floor {
        guard.note_if_unrecoverable(slot);
    }
    true
}

/// Snapshot of the Stage-7 corruption ground truth, folded for `check()`.
pub(crate) struct CorruptionStats {
    /// Corruption injections recorded this run, in total.
    pub(crate) injected: usize,
    /// Ledger entries whose read surfaced as the one typed crash decision.
    pub(crate) crashed: u64,
    /// Every ledger entry is accounted for: resolved, or legitimately dormant
    /// (on a parked node the scan never re-reads, or a torn tail the run
    /// ended before rebooting).
    pub(crate) accounted: bool,
    /// Terminally parked nodes, and whether they stayed within the dead-node
    /// budget (a live quorum survives).
    pub(crate) parked: usize,
    pub(crate) parked_within_budget: bool,
    /// Sticky per-family / per-verdict gate facts.
    flags: Stage7Flags,
}

pub(crate) fn corruption_stats(handle: &StateHandle) -> CorruptionStats {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut crashed = 0_u64;
    let mut accounted = true;
    for injection in &guard.corruptions {
        match injection.outcome {
            CorruptionOutcome::Crashed => crashed += 1,
            // Exercised-vs-dormant tracking (the oracle must not overclaim):
            // an injected fault on a never-read record is not a detection
            // failure — but "never read" must be genuine: the node parked
            // before the scan could reach this record, or a torn tail the run
            // ended before rebooting through.
            CorruptionOutcome::Dormant => {
                let legal = injection.kind == CorruptionKind::TornTail
                    || guard.parked_ids.contains(&injection.node);
                if !legal {
                    accounted = false;
                }
            }
            CorruptionOutcome::CoDetected
            | CorruptionOutcome::Repaired
            | CorruptionOutcome::DiscardedTail
            // Stage 8: a standing report (a run may end mid-repair, or WAITED)
            // and a genuinely re-written record are both fully accounted.
            | CorruptionOutcome::Reported
            | CorruptionOutcome::Recovered => {}
        }
    }
    CorruptionStats {
        injected: guard.corruptions.len(),
        crashed,
        accounted,
        parked: guard.parked.len(),
        parked_within_budget: guard.parked.len() <= guard.dead_budget(),
        flags: guard.s7,
    }
}

/// The storage-fault coverage gates + the injected↔detected correlation,
/// evaluated once per run from the workload's `check()` (the shared-gate
/// doctrine in [`crate::audit`]).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn check_storage_gates(handle: &StateHandle) {
    let stats = storage_fault_stats(handle);
    let corruption = corruption_stats(handle);
    let detected = audit_world(handle).storage_faults_detected();
    // Injected fault ↔ surfaced error ↔ crash decision correlate 1:1, typed,
    // with no string parsing: a spontaneous (non-injected) storage fault or a
    // swallowed injection both break this count.
    assert_always!(
        detected == u64::try_from(stats.injected).unwrap_or(u64::MAX),
        "storage: every injected fault surfaces as exactly one typed crash decision",
        {
            "injected" => u64::try_from(stats.injected).unwrap_or(u64::MAX),
            "detected" => detected
        }
    );
    assert_always!(
        stats.clean_quorum_everywhere,
        "storage: injected faults never cost a record its clean quorum of live copies"
    );
    check_corruption_gates(handle, &corruption);
    // The #71 compound gate: corruption x partition x a lagging follower
    // reached in one run (the network swarm IS the partition; lag is the
    // audit's observed fact). It used to ride the safety-only axis, which was
    // the only one carrying swarm network turbulence; since #126 folded that
    // turbulence into the main campaign, the main campaign is where the
    // compound is reachable and where it must saturate.
    assert_sometimes!(
        corruption.injected > 0 && audit_world(handle).lag_observed(),
        "storage: corruption compounds with a partition and a lagging follower"
    );
    assert_sometimes!(
        stats.eio_landed,
        "storage: EIO was reported but the write landed"
    );
    assert_sometimes!(
        stats.eio_lost,
        "storage: EIO was reported and the write was lost"
    );
    assert_sometimes!(
        stats.fsync_durable,
        "storage: fsync failed but the batch was durable anyway"
    );
    assert_sometimes!(
        stats.fsync_lost,
        "storage: fsync failed and the batch was genuinely lost"
    );
}

/// The Stage-7 half of the storage oracle (issue #20 F): the exercised ⇔
/// detected correlation, the dead-node budget, and the per-family /
/// per-verdict coverage gates.
fn check_corruption_gates(handle: &StateHandle, corruption: &CorruptionStats) {
    // Every exercised corruption is detected as exactly one typed crash
    // decision — a spontaneous corruption detection (nothing injected) or a
    // swallowed injection both break the count — and every injection is
    // accounted for (crashed, co-detected, repaired, discarded, or genuinely
    // never read).
    let corruption_crashes = audit_world(handle).corruption_faults_detected();
    assert_always!(
        corruption_crashes == corruption.crashed,
        "storage: every exercised corruption is exactly one typed crash decision",
        {
            "ledger_crashed" => corruption.crashed,
            "detected" => corruption_crashes
        }
    );
    assert_always!(
        corruption.accounted,
        "storage: every corruption injection is detected, resolved, or genuinely unread"
    );
    // The availability cost of detect ⇒ crash is bounded a priori: a
    // corruption-parked minority never costs the cluster its live quorum.
    assert_always!(
        corruption.parked_within_budget,
        "storage: corruption never parks a quorum of nodes",
        { "parked" => u64::try_from(corruption.parked).unwrap_or(u64::MAX) }
    );
    // Stage-7 family gates (issue #20 D): one per injected fault family,
    // proving the sweep genuinely visits each detection channel.
    let s7 = corruption.flags;
    assert_sometimes!(
        s7.bitflip_detected,
        "storage: a bit-flip corruption is detected on read"
    );
    assert_sometimes!(
        s7.lost_write_detected,
        "storage: a lost write is detected by the reserved-record contract"
    );
    assert_sometimes!(
        s7.misdirected_detected,
        "storage: a misdirected write is caught by the identity check"
    );
    assert_sometimes!(
        s7.read_eio_detected,
        "storage: a read EIO degrades to a checksum mismatch"
    );
    assert_sometimes!(
        s7.torn_tail_discarded,
        "storage: a crash-truncatable tail is discarded on boot"
    );
    assert_sometimes!(
        s7.snapshot_detected,
        "storage: snapshot corruption is detected as its own record"
    );
    assert_sometimes!(
        s7.promise_repaired,
        "storage: one bad promise copy is repaired from its twin"
    );
    assert_sometimes!(
        s7.metadata_crashed,
        "storage: an fs-metadata fault reliably crashes the node"
    );
    // Per-verdict gates (issue #20 C): the disentanglement table's ambiguous
    // rows are genuinely reached, not just the happy path.
    assert_sometimes!(
        s7.corruption_below_tail,
        "storage: corruption below the tail is classified"
    );
    assert_sometimes!(
        s7.last_entry_ambiguity,
        "storage: the last-entry ambiguity is classified"
    );
    assert_sometimes!(
        s7.identifier_lost,
        "storage: an identifier lost with its entry is classified"
    );
    // Stage 8 (issue #21): the recover-or-wait flip is genuinely exercised —
    // rotted-but-identified records are *reported* (the node keeps serving)
    // and genuinely *repaired* by the cluster, not merely classified.
    assert_sometimes!(
        s7.faulty_reported,
        "storage: a rotted record is reported faulty and the node keeps serving"
    );
    assert_sometimes!(
        s7.record_recovered,
        "storage: a reported faulty record is repaired by the cluster"
    );
}
