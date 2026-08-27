//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. It
//! derives a cluster-consistent membership from the topology, wires the node to a
//! per-node handle on the shared [`StorageWorld`] (the sim's stand-in for durable
//! disk), and runs the same `run_node` a production `tokio::main` would — inside a
//! recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: `run_node` unwinds, the volatile `RawNode` is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, StateHandle, TimeProvider, assert_always,
    assert_reachable, assert_sometimes, buggify_knob, buggify_with_prob, sim::sim_random,
};

use crate::audit::{AuditWorld, GateScope, NodeAudit, audit_world};
use crate::chain::{AppliedTransition, ChainState, hash_text};
use paros::{
    Ballot, ClientId, ClientSeq, Command, Config, ConfigId, CorruptionVerdict, DriverHooks,
    HardState, IntegrityFault, MemStorage, Message, MetadataFault, MustSync, NodeId, NodeStorage,
    RecoveryCase, RunError, Seam, SessionEntry, Slot, SlotRecord, Storage, StorageError,
    StorageRecord, WitnessStatus, WriteOutcome, classify_log, command_hash, parse_addr, run_node,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";
const QUIET_HOOKS_KEY: &str = "paros-quiet-driver-hooks";
/// Flag key for the **adversarial amnesia red demo** (issue #19 item D): when
/// set, exactly one node that crashes after raising its durable promise is
/// wiped (its disk deleted from the world) and rejoins **naively** — as itself,
/// with no protocol support. This is *proven unsafe* (CTRL's `MarkNonVoting`
/// takedown: a node that lost its promise can accept from an old leader while
/// the new leader still counts that promise), so the demo's contract is to go
/// **red**: the cross-restart promise audit must catch the reneged promise.
/// Never set on a real campaign; `prob_wipe` stays 0 there (a snapshot restores
/// the log, not the promise — node replacement is #22's reconfiguration).
const NAIVE_WIPE_KEY: &str = "paros-naive-wipe-demo";

/// Flag key for the **truncate-on-mismatch red demo** (issue #20 item F): when
/// set, one persisted accepted record is corrupted at a reboot, and the boot
/// scan — instead of taking Stage 7's crash baseline — reproduces the classic
/// CTRL Figure 2 bug (found in both `ZooKeeper` and `LogCabin`): truncate from the
/// faulty entry onward and keep running as if the log simply ended earlier.
/// The demo's contract is to go **red**: the audit's recovered-vs-persisted
/// divergence leg must surface the silent loss as an `assertion_violation`.
/// Never set on a real campaign.
const TRUNCATE_ON_MISMATCH_KEY: &str = "paros-truncate-on-mismatch-demo";

/// Per-call firing probability of the write-`EIO` BUGGIFY site (one location,
/// per-seed activation × per-call firing; the record identity travels on the
/// typed error, not on the location).
const P_WRITE_EIO: f64 = 0.01;
/// Per-call firing probability of the fsync-failure BUGGIFY site. Independent
/// from the write site — the sweep must be able to select the two failure
/// modes separately (same rule as the driver's two durability seams).
const P_FSYNC_FAIL: f64 = 0.01;
/// Coin on the fsync *lost* leg: the crash tore the batch instead of losing
/// it whole — a prefix of the staged fresh appends reaches disk without
/// identifiers (Stage 7's per-record torn durability; the `CrashTail` leg of
/// the disentanglement table). A plain seeded coin like the fsyncgate
/// `persisted` decision, NOT its own BUGGIFY location: the *location* is the
/// fsync failure; whole-loss vs torn is the world's outcome-shaping of that
/// one fault, and per-seed location activation must not suppress the torn
/// flavor (the whole-loss leg is already the clean-crash model's default).
const P_TORN_TAIL: f64 = 0.75;
/// Per-boot firing probabilities of the Stage-7 rot BUGGIFY sites — each fault
/// family its own independent location (per-seed activation × per-boot
/// firing), modelling latent faults that surfaced while the node was down and
/// are read back by the boot scan that immediately follows.
const P_ENTRY_ROT: f64 = 0.06;
const P_LOST_WRITE: f64 = 0.04;
const P_MISDIRECT: f64 = 0.04;
const P_SNAPSHOT_ROT: f64 = 0.05;
const P_PROMISE_ROT: f64 = 0.04;
const P_META_FAULT: f64 = 0.03;
const P_READ_EIO: f64 = 0.05;
/// Cap on the crash-truncatable window at boot: the maximum concurrently
/// in-flight accept writes one torn batch can leave unwitnessed (a `Ready`
/// batch is the driver's flush unit, and its accept count is bounded by the
/// leader's recovery page size).
const MAX_TORN_TAIL: usize = 64;

/// A paros node in the simulation.
pub struct NodeProcess;

/// A paros node with simulation-only driver decisions disabled. This keeps the
/// dedicated lifecycle choreography's built-in graceful attrition as its sole
/// perturbation without mutating Moonpool's iteration-owned BUGGIFY state.
pub(crate) struct QuietNodeProcess;

#[async_trait]
impl Process for QuietNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(QUIET_HOOKS_KEY).is_none() {
            ctx.state().publish(QUIET_HOOKS_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

/// A paros node for the **amnesia red demo** ([`NAIVE_WIPE_KEY`]): identical to
/// [`NodeProcess`] except that one node per run loses its disk on a restart and
/// rejoins naively. The demo's deliverable is the resulting
/// `assertion_violation` — the cross-restart promise audit catching the reneged
/// promise — never a green run.
pub(crate) struct NaiveWipeNodeProcess;

#[async_trait]
impl Process for NaiveWipeNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(NAIVE_WIPE_KEY).is_none() {
            ctx.state().publish(NAIVE_WIPE_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

/// A paros node for the **truncate-on-mismatch red demo**
/// ([`TRUNCATE_ON_MISMATCH_KEY`]): identical to [`NodeProcess`] except that
/// one persisted record is corrupted at a qualifying reboot and the boot scan
/// then truncates on the mismatch instead of crashing — the CTRL Figure 2 bug
/// class Stage 7's *never truncate on a mismatch* invariant exists to forbid.
/// The deliverable is the resulting `assertion_violation`, never a green run.
pub(crate) struct TruncateOnMismatchNodeProcess;

#[async_trait]
impl Process for TruncateOnMismatchNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(TRUNCATE_ON_MISMATCH_KEY).is_none() {
            ctx.state().publish(TRUNCATE_ON_MISMATCH_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    // One recovery loop with per-exit-kind handling; splitting the arms would
    // scatter the crash/park/restart contract this function *is*.
    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Build the full cluster membership. `all_process_ips()` excludes this
        // node, so add `my_ip` and sort numerically: every node derives the
        // *same* ordered list, so `NodeId(i) <-> ips[i]` is consistent
        // cluster-wide without any coordination.
        let my_ip = ctx.my_ip().to_string();
        let mut ips: Vec<String> = ctx.topology().all_process_ips().to_vec();
        ips.push(my_ip.clone());
        ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
        ips.dedup();

        let members = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| {
                parse_addr(ip)
                    .map(|addr| (NodeId(u64::try_from(i).expect("node index fits u64")), addr))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

        let self_rank = NodeId(
            u64::try_from(
                ips.iter()
                    .position(|ip| ip == &my_ip)
                    .expect("self is a member"),
            )
            .expect("node index fits u64"),
        );
        let config = Config {
            id: self_rank,
            peers: members.iter().map(|(id, _)| *id).collect(),
            ..Config::default()
        };

        // The per-iteration durable-storage world, shared by every node and
        // surviving crash/restart (it lives in the `StateHandle`, fresh per seed
        // but stable across a process's reboots). Each node reaches it through a
        // `Weak` handle upgraded per op.
        let world = storage_world(ctx.state());
        world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_cluster_size(members.len());
        let perturb = ctx.state().get::<bool>(QUIET_HOOKS_KEY) != Some(true);
        let hooks = BuggifyHooks::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            perturb,
        );
        // The budgeted storage-fault layer (issue #19 B/C) shares the driver
        // hooks' chaos window and quiet-mode switch: after the cutoff the world
        // stops injecting **new** faults but never heals the consequences of
        // old ones — recovery through the tail must be genuine.
        let faults = StorageFaults {
            time: ctx.time().clone(),
            cutoff: Duration::from_millis(crate::CHAOS_DURATION_MS),
            enabled: perturb,
        };
        let naive_wipe_demo = ctx.state().get::<bool>(NAIVE_WIPE_KEY) == Some(true);
        let truncate_demo = ctx.state().get::<bool>(TRUNCATE_ON_MISMATCH_KEY) == Some(true);
        // The per-iteration shared audit: pure observation, published beside the
        // storage world so every node folds its transitions into one incremental
        // checker. It never influences the driver — that is `hooks`' job.
        let checker = audit_world(ctx.state());
        let audit = NodeAudit::new(ctx.time().clone(), checker.clone());

        // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
        // drop the volatile node, rebuild storage from the (surviving) world, and
        // re-run — a faithful clean crash + recovery. Attrition (process kill) is
        // handled by the harness; this covers the seams *inside* a Ready batch
        // that attrition cannot reach.
        loop {
            // A node terminally parked by a detected persistent corruption
            // stays down: attrition may revive the *process*, but the boot
            // scan would re-detect the same rotted record forever, and a
            // second crash report for one detection would break the 1:1
            // injected⇔detected correlation. Exit before touching the store.
            if world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .parked
                .contains(&my_ip)
            {
                checker.note_storage_dead(self_rank.0);
                tracing::info!(node = self_rank.0, "storage_parked");
                return Ok(());
            }
            // The amnesia red demo (item D): on a rejoin with a raised durable
            // promise, wipe the disk once and let the node come back naively.
            if naive_wipe_demo {
                maybe_naive_wipe(&world, &my_ip, self_rank.0);
            }
            if truncate_demo {
                maybe_demo_corrupt(&world, &my_ip, self_rank.0);
            }
            let storage = DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                my_ip.clone(),
                self_rank.0,
                faults.clone(),
                checker.clone(),
                DemoMode {
                    truncate_on_mismatch: truncate_demo,
                    naive_wipe: naive_wipe_demo,
                },
            );
            match run_node(
                ctx.providers().clone(),
                storage,
                parse_addr(&my_ip)?,
                members.clone(),
                ctx.shutdown().clone(),
                &hooks,
                &audit,
            )
            .await
            {
                // Simulated crash at a durability seam: fall through to recover
                // and re-run (rebuilding volatile state from the durable world).
                // The restart delay is workload-buggified config (prong 2): it
                // stretches the durability-seam crash window that process-level
                // attrition cannot reach. A node held down while the cluster
                // keeps committing and truncating returns below the compaction
                // floor and independently exercises snapshot recovery.
                Err(RunError::SeamCrash(_)) => {
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        // BUGGIFY pairing: the restart-delay knob fired — the
                        // held-down-past-the-floor generator is genuinely live.
                        assert_reachable!("a seam-crashed node restarts after a buggified delay");
                        ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                    }
                }
                // An injected storage fault surfaced as the driver's typed
                // crash decision (issue #19 A): fail-stop, so the node re-enters
                // the same Stage-4 crash/restart path — the next iteration
                // boots from whatever the disk *actually* holds, which is how
                // an ambiguous write's two possible outcomes both resolve.
                // Its restart delay is its own independent BUGGIFY location.
                Err(RunError::Storage(_)) => {
                    // Stage 7's baseline for a *persistent* detected fault —
                    // a rotted record or an FS-metadata fault — is detect ⇒
                    // crash, and restarting cannot help: the boot scan would
                    // re-detect the same record forever. The node stays down
                    // for the run (the availability disaster the CTRL paper
                    // measures; Stage 8 buys it back), bounded by the world's
                    // dead-node budget so the cluster keeps a live quorum. The
                    // audit is told so convergence excuses exactly these
                    // nodes, and only these.
                    let parked = world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .parked
                        .contains(&my_ip);
                    if parked {
                        assert_reachable!("storage: a corruption-crashed node stays down");
                        checker.note_storage_dead(self_rank.0);
                        tracing::info!(node = self_rank.0, "storage_parked");
                        return Ok(());
                    }
                    assert_reachable!("a storage-fault crash recovers through the restart path");
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                    }
                }
                // The only non-crash exit: a genuine infrastructure failure
                // propagates to the harness instead of being retried.
                Err(RunError::Infra(e)) => return Err(e),
                Ok(()) => return Ok(()),
            }
        }
    }
}

/// One-shot disk wipe for the amnesia red demo: the first time a node with a
/// raised durable promise comes back through the restart path, delete its disk
/// so it rejoins as itself with no memory of the promise. Deterministic per
/// seed (the first qualifying restart spends the single wipe budget).
fn maybe_naive_wipe(world: &Arc<Mutex<StorageWorld>>, key: &str, node: u64) {
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.wipe_spent {
        return;
    }
    let promised = guard
        .disks
        .get(key)
        .map(|disk| disk.hard_state.max_promised_ballot);
    if promised.is_some_and(|ballot| ballot > Ballot::default()) {
        guard.disks.remove(key);
        guard.marks.remove(key);
        guard.wipe_spent = true;
        // Demo-only anchor: the wipe genuinely happened before the naive rejoin.
        assert_reachable!("amnesia demo: a wiped node rejoins naively");
        tracing::info!(node, "naive_wipe");
    }
}

/// One-shot corruption for the truncate-on-mismatch red demo: the first time a
/// node reboots holding a clean, persisted accepted record, corrupt one —
/// preferring a *chosen* record, the dangerous loss — so the demo boot scan
/// has a mismatch to (buggily) truncate on. Deterministic per seed.
fn maybe_demo_corrupt(world: &Arc<Mutex<StorageWorld>>, key: &str, node: u64) {
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.demo_rot_spent {
        return;
    }
    let Some(disk) = guard.disks.get(key) else {
        return;
    };
    let chosen = disk.hard_state.chosen_index;
    let target = disk
        .accepted
        .keys()
        .rfind(|slot| disk.slot_health(**slot).clean() && chosen.is_some_and(|ci| **slot <= ci))
        .copied()
        .or_else(|| {
            disk.accepted
                .keys()
                .rfind(|slot| disk.slot_health(**slot).clean())
                .copied()
        });
    let Some(slot) = target else {
        return;
    };
    let Some(disk) = guard.disks.get_mut(key) else {
        return;
    };
    disk.entry_health.insert(
        slot,
        SlotHealth {
            entry: RecordHealth::Faulty,
            id: WitnessStatus::Present,
        },
    );
    guard
        .marks
        .entry(key.to_string())
        .or_default()
        .insert(slot.0);
    guard.demo_rot_spent = true;
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::Accepted(slot),
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    // Demo-only anchor: the corruption genuinely landed before the buggy boot.
    assert_reachable!("truncate demo: a persisted record is corrupted before a reboot");
    tracing::info!(node, slot = slot.0, "demo_corrupt");
}

/// Get-or-create the singleton [`StorageWorld`] for this iteration. Get-then-
/// publish is race-free: the sim executor is single-threaded and this runs
/// synchronously (no `.await` between the get and the publish).
fn storage_world(state: &StateHandle) -> Arc<Mutex<StorageWorld>> {
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
enum RecordHealth {
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
struct SlotHealth {
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
struct NodeDisk {
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
    /// Silently truncated by the truncate-on-mismatch red demo — never legal
    /// outside it; the audit's divergence leg is what catches it.
    DemoTruncated,
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
struct Stage7Flags {
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
    block_fault_detected: bool,
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
struct StorageWorld {
    disks: BTreeMap<String, NodeDisk>,
    /// Full cluster membership size, for the quorum bound (set once at boot;
    /// zero refuses every injection).
    cluster_size: usize,
    /// Ground truth of every permitted injection, in order.
    injected: Vec<InjectedFault>,
    /// Lost-leg fault marks per node: accepted-log records whose most recent
    /// write was injected-lost, torn, or corrupted, and not yet re-written by
    /// a clean flush (or discarded as a crash tail). These are what the
    /// budget counts.
    marks: BTreeMap<String, BTreeSet<u64>>,
    /// The amnesia demo's single wipe budget (see [`NAIVE_WIPE_KEY`]).
    wipe_spent: bool,
    /// The truncate-on-mismatch demo's single injection budget.
    demo_rot_spent: bool,
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
}

impl StorageWorld {
    fn set_cluster_size(&mut self, n: usize) {
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
        true
    }

    /// Terminally park a node: detect ⇒ crash, and it stays down.
    fn park(&mut self, key: &str, node: u64) {
        self.parked.insert(key.to_string());
        self.parked_ids.insert(node);
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
                if injection.block && outcome != CorruptionOutcome::DemoTruncated {
                    self.s7.block_fault_detected = true;
                }
            }
        }
    }

    /// Targeted, budget-checked corruption of one durable record — issue
    /// #21's single-injection API (`corrupt(node, record)`): the adversarial
    /// promise-corruption test is one call. Returns whether the budget
    /// permitted it.
    #[allow(dead_code)] // armed for #21's targeted tests; the swarm sites drive it today
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

/// Roll the Stage-7 rot sites for one booting node: latent faults that
/// surfaced while it was down, injected at the boot that will immediately read
/// them back (the boot scan runs before anything else in `run_node`, with no
/// await in between, so injection → detection is atomic per boot). Each fault
/// family is its own independent BUGGIFY location; every *persistent* family
/// terminally parks the node (detect ⇒ crash, and restarting cannot help), so
/// each is gated on [`StorageWorld::may_park`]'s dead-node budget.
#[allow(clippy::too_many_lines)] // one flat block per independent BUGGIFY location
fn roll_boot_rot(world: &mut StorageWorld, key: &str, node: u64) {
    let clean_slots = |world: &StorageWorld| -> Vec<Slot> {
        world.disks.get(key).map_or_else(Vec::new, |disk| {
            disk.accepted
                .keys()
                .filter(|slot| disk.slot_health(**slot).clean())
                .copied()
                .collect()
        })
    };
    // Pick a rot target: half the time the *last* retained slot, so the
    // proven-undecidable last-entry row of the disentanglement table is
    // genuinely visited, not just the interior corruption row.
    let pick = |slots: &[Slot]| -> Slot {
        if sim_random::<f64>() < 0.5 {
            slots[slots.len() - 1]
        } else {
            slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()]
        }
    };
    let mark_entry = |world: &mut StorageWorld, slot: Slot, health: SlotHealth, kind, block| {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.entry_health.insert(slot, health);
        }
        world
            .marks
            .entry(key.to_string())
            .or_default()
            .insert(slot.0);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Accepted(slot),
            kind,
            block,
            outcome: CorruptionOutcome::Dormant,
        });
    };

    // Bit flip / latent sector error on one persisted entry — with sub-rolls
    // for a multi-record *block* fault (CTRL injects per FS block: a
    // contiguous run mismatches at once, and recovery must not assume faults
    // are singletons) and for the identifier rotting with its entry.
    if buggify_with_prob!(P_ENTRY_ROT) && world.may_park(key) {
        let slots = clean_slots(world);
        if !slots.is_empty() {
            let primary = pick(&slots);
            // Generous coin: the identifier-lost row has its own per-verdict
            // sometimes-gate, and the entry-rot events that draw this coin
            // are budget-capped per run, so the sweep needs a fat coin to be
            // certain of the composition within a bounded seed schedule.
            let id_faulty = sim_random::<f64>() < 0.5;
            // The block sub-roll needs a contiguous clean run at the primary,
            // which short (frequently truncated) logs often lack, so it rolls
            // generously to stay reachable across a bounded sweep.
            let block = sim_random::<f64>() < 0.4;
            let members: Vec<Slot> = if block {
                slots
                    .iter()
                    .copied()
                    .filter(|s| s.0 >= primary.0.saturating_sub(2) && *s <= primary)
                    .collect()
            } else {
                vec![primary]
            };
            let is_block = members.len() > 1;
            for slot in members {
                let id = if slot == primary && id_faulty {
                    WitnessStatus::Faulty
                } else {
                    WitnessStatus::Present
                };
                mark_entry(
                    world,
                    slot,
                    SlotHealth {
                        entry: RecordHealth::Faulty,
                        id,
                    },
                    CorruptionKind::BitFlip,
                    is_block,
                );
            }
            world.park(key, node);
        }
    }
    // A lost write: the entry reads back as its reserved record where the
    // identifier exists (absence made detectable by the reserved-record
    // contract).
    if buggify_with_prob!(P_LOST_WRITE) && world.may_park(key) {
        let slots = clean_slots(world);
        if !slots.is_empty() {
            let slot = pick(&slots);
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Lost,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::LostWrite,
                false,
            );
            world.park(key, node);
        }
    }
    // A misdirected write: valid checksum, wrong identity — the identity
    // check inside the checksummed region catches it.
    if buggify_with_prob!(P_MISDIRECT) && world.may_park(key) {
        let slots = clean_slots(world);
        if !slots.is_empty() {
            let slot = pick(&slots);
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Misdirected,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::Misdirected,
                false,
            );
            world.park(key, node);
        }
    }
    // Snapshot corruption is its own kind and its own gate (#71) — a
    // first-class target, not a byproduct of log-entry coverage.
    if buggify_with_prob!(P_SNAPSHOT_ROT)
        && world
            .disks
            .get(key)
            .is_some_and(|d| d.chain.applied_count > 0)
        && world.may_park(key)
    {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.snapshot_health = RecordHealth::Faulty;
        }
        world.park(key, node);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Snapshot,
            kind: CorruptionKind::BitFlip,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
    // HardState copy rot (CTRL metainfo doctrine): usually one copy — used and
    // repaired from its twin, no availability cost — and rarely both, which is
    // the one unrecoverable scalar loss (the node cannot know what it
    // promised, and no peer can tell it).
    if buggify_with_prob!(P_PROMISE_ROT) && world.disks.contains_key(key) {
        let both = sim_random::<f64>() < 0.25;
        if both {
            if world.may_park(key) {
                if let Some(disk) = world.disks.get_mut(key) {
                    disk.promise_health = [RecordHealth::Faulty, RecordHealth::Faulty];
                }
                world.park(key, node);
                for _copy in 0..2 {
                    world.note_corruption(CorruptionInjection {
                        node,
                        record: StorageRecord::Promise,
                        kind: CorruptionKind::PromiseCopy,
                        block: false,
                        outcome: CorruptionOutcome::Dormant,
                    });
                }
            }
        } else {
            let copy = usize::from(sim_random::<f64>() < 0.5);
            if let Some(disk) = world.disks.get_mut(key) {
                disk.promise_health[copy] = RecordHealth::Faulty;
            }
            world.note_corruption(CorruptionInjection {
                node,
                record: StorageRecord::Promise,
                kind: CorruptionKind::PromiseCopy,
                block: false,
                outcome: CorruptionOutcome::Dormant,
            });
        }
    }
    // A file-granularity FS-metadata fault: reliably crash, never recover
    // (item E) — the whole store is the record.
    if buggify_with_prob!(P_META_FAULT) && world.disks.contains_key(key) && world.may_park(key) {
        let fault = match sim_random::<u64>() % 3 {
            0 => MetadataFault::Missing,
            1 => MetadataFault::WrongSize,
            _ => MetadataFault::ReadOnly,
        };
        if let Some(disk) = world.disks.get_mut(key) {
            disk.meta_fault = Some(fault);
        }
        world.park(key, node);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Store,
            kind: CorruptionKind::Metadata,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
    // A transient EIO on the read path: collapses into the corruption channel
    // (one detection path), crashes the node once, and the retry — the next
    // boot — reads clean. The only Stage-7 family with no availability cost.
    if buggify_with_prob!(P_READ_EIO) && world.disks.contains_key(key) {
        let record = world
            .disks
            .get(key)
            .and_then(|d| d.accepted.keys().next_back().copied())
            .map_or(StorageRecord::Promise, StorageRecord::Accepted);
        if let Some(disk) = world.disks.get_mut(key) {
            disk.read_eio = Some(record);
        }
        world.note_corruption(CorruptionInjection {
            node,
            record,
            kind: CorruptionKind::ReadEio,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
}

/// This node's durable evidence as of one boot, collected under the world
/// lock in `restore` and consumed by the boot scan.
#[derive(Default)]
struct BootEvidence {
    /// Every retained accepted slot in order, with its record health.
    records: Vec<(Slot, SlotHealth)>,
    promise: [RecordHealth; 2],
    chosen: RecordHealth,
    truncation: RecordHealth,
    snapshot: RecordHealth,
    meta: Option<MetadataFault>,
    read_eio: Option<StorageRecord>,
}

/// Which red demo, if any, this node runs under (both perturb the storage
/// layer's honest behavior in exactly one deliberate way each).
#[derive(Clone, Copy, Default)]
struct DemoMode {
    truncate_on_mismatch: bool,
    naive_wipe: bool,
}

impl BootEvidence {
    fn collect(disk: &NodeDisk) -> Self {
        Self {
            records: disk
                .accepted
                .keys()
                .map(|slot| (*slot, disk.slot_health(*slot)))
                .collect(),
            promise: disk.promise_health,
            chosen: disk.chosen_health,
            truncation: disk.truncation_health,
            snapshot: disk.snapshot_health,
            meta: disk.meta_fault,
            read_eio: disk.read_eio,
        }
    }
}

/// The BUGGIFY-side switchboard for the storage-fault sites: shares the driver
/// hooks' chaos window (per the suppression contract on [`StorageWorld`]) and
/// the quiet-mode switch, so choreographed campaigns stay fault-free.
#[derive(Clone)]
struct StorageFaults<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
}

impl<T: TimeProvider> StorageFaults<T> {
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
struct DurableStorage<T> {
    /// Read view: this node's durable records as of boot.
    boot: MemStorage,
    /// The shared world, upgraded per op.
    world: Weak<Mutex<StorageWorld>>,
    /// This node's IP — its key into the world.
    key: String,
    /// Stable numeric identity used only on application trace facts.
    node_id: u64,
    /// The budgeted fault switchboard (see the type-level fault model note).
    faults: StorageFaults<T>,
    /// The shared checker, fed the world's flush ground truth (see
    /// [`AuditWorld::note_flushed_ground_truth`]).
    checker: Arc<AuditWorld>,
    /// This incarnation's application state, including transitions staged for
    /// the next durability flush.
    application: ChainState,
    /// This boot's durable-record evidence, consumed by the boot scan.
    evidence: BootEvidence,
    /// Truncate-on-mismatch red-demo mode: the boot scan truncates on a
    /// corruption verdict instead of crashing (the CTRL Figure 2 bug).
    demo_truncate: bool,
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
    fn restore(
        config: Config,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        node_id: u64,
        faults: StorageFaults<T>,
        checker: Arc<AuditWorld>,
        demo: DemoMode,
    ) -> Self {
        let mut boot = MemStorage::new(config);
        let mut application = ChainState::default();
        let mut evidence = BootEvidence::default();
        if let Some(strong) = world.upgrade() {
            let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            // Stage 7 rot: latent faults that surfaced while the node was
            // down, rolled at the boot that immediately scans them. Gated on
            // the chaos window like every other injection — and never rolled
            // in either red demo, whose contracts each hinge on their own
            // single deterministic perturbation.
            if faults.active() && !demo.truncate_on_mismatch && !demo.naive_wipe {
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
            demo_truncate: demo.truncate_on_mismatch,
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
            let torn_entry_faulty = sim_random::<f64>() < 0.5;
            let last = torn.len() - 1;
            let d = w.disks.entry(key.clone()).or_default();
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

    /// The truncate-on-mismatch red demo's buggy reaction (CTRL Figure 2):
    /// drop the log from the first corrupt entry onward and keep running as if
    /// the log simply ended earlier — the derived chosen index regresses with
    /// the dropped tail, exactly as it does in an implementation that treats
    /// its log tail as the end of history. Silent by design; the audit's
    /// recovered-vs-persisted divergence leg is what must catch it.
    fn demo_truncate_from(&mut self, first_bad: Slot) -> Result<(), StorageError> {
        let key = self.key.clone();
        let node = self.node_id;
        self.with_world(|w| {
            if let Some(disk) = w.disks.get_mut(&key) {
                let dropped: Vec<Slot> = disk
                    .accepted
                    .range(first_bad..)
                    .map(|(slot, _)| *slot)
                    .collect();
                for slot in &dropped {
                    disk.accepted.remove(slot);
                    disk.entry_health.remove(slot);
                }
                let regressed = first_bad.0.checked_sub(1).map(Slot);
                disk.hard_state.chosen_index = disk.hard_state.chosen_index.min(regressed);
                for slot in dropped {
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(slot),
                        CorruptionOutcome::DemoTruncated,
                    );
                }
            }
            // The buggy node keeps running: it is not parked, it is wrong.
            w.parked.remove(&key);
            w.parked_ids.remove(&node);
        })?;
        self.reseed_boot()?;
        tracing::info!(node, slot = first_bad.0, "truncate_on_mismatch");
        Ok(())
    }

    /// Rebuild the boot read view from the world's current durable records
    /// (used after the demo's truncation mutates the disk under the view).
    fn reseed_boot(&mut self) -> Result<(), StorageError> {
        let config = self.boot.initial_state().1;
        let key = self.key.clone();
        let boot = self.with_world(|w| {
            let mut boot = MemStorage::new(config);
            if let Some(disk) = w.disks.get(&key) {
                let sealed: Vec<SessionEntry> = disk
                    .sealed
                    .iter()
                    .map(|(&(client, seq), &slot)| (client, seq, slot))
                    .collect();
                let _ = boot.truncate(disk.first_slot, &sealed);
                let _ = boot.persist_config_id(disk.hard_state.config_id);
                let _ = boot.persist_ballot(disk.hard_state.max_promised_ballot);
                for (slot, (ballot, command)) in &disk.accepted {
                    if disk.slot_health(*slot).clean() {
                        let _ = boot.append_accepted(*slot, *ballot, command.clone());
                    }
                }
                if let Some(ci) = disk.hard_state.chosen_index {
                    let _ = boot.set_chosen_index(ci);
                }
                let _ = boot.sync(MustSync::Sync);
            }
            boot
        })?;
        self.boot = boot;
        Ok(())
    }

    /// BUGGIFY site 1: this per-record write returns `EIO`. Returns
    /// `Some(persisted)` when the fault fires and the budget permits it —
    /// `persisted` is the world's seeded ambiguity decision (item C), recorded
    /// as ground truth and never told to the node.
    fn roll_write_eio(&mut self, record: StorageRecord) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(P_WRITE_EIO) {
            return None;
        }
        let persisted = sim_random::<f64>() < 0.5;
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
    /// from the write site; same ambiguity contract.
    fn roll_fsync_fault(&mut self) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(P_FSYNC_FAIL) {
            return None;
        }
        let persisted = sim_random::<f64>() < 0.5;
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
        let applies = std::mem::take(&mut self.staged_applies);
        let sealed = std::mem::take(&mut self.staged_sealed);
        let flushed_slots: Vec<u64> = accepted.keys().map(|s| s.0).collect();
        let flushed_hashes: Vec<(u64, u64)> = accepted
            .iter()
            .map(|(slot, (_ballot, command))| (slot.0, command_hash(command)))
            .collect();
        let key = self.key.clone();
        self.with_world(|w| {
            w.clear_marks(&key, flushed_slots.iter().copied());
            let d = w.disks.entry(key.clone()).or_default();
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
            for (slot, record) in accepted {
                // A clean flush re-writes the record and its identifier: any
                // prior torn/rotted health for the slot is genuinely replaced.
                d.entry_health.remove(&slot);
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

        if let Some(installed) = snapshot {
            tracing::info!(
                node = self.node_id,
                index = installed.applied_count,
                state = %hash_text(installed.chain_hash),
                "chain_snapshot_installed"
            );
        }
        for pending in applies {
            let next = pending.transition.next;
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
    /// verdict** (outside the deliberately buggy red demo).
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
        // Snapshot corruption: its own kind and its own gate (#71).
        if let Some(fault) = evidence.snapshot.integrity_fault() {
            let _ = self.with_world(|w| {
                w.s7.snapshot_detected = true;
                w.resolve_corruption(node, StorageRecord::Snapshot, CorruptionOutcome::Crashed);
            });
            return Err(StorageError::Corruption {
                record: StorageRecord::Snapshot,
                fault,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        // The log: reduce every retained record to its evidence booleans and
        // run the total classifier (batching rule + TigerBeetle hardening).
        let records: Vec<SlotRecord> = evidence
            .records
            .iter()
            .map(|(slot, health)| SlotRecord {
                slot: *slot,
                entry_faulty: health.entry != RecordHealth::Clean,
                identifier: health.id,
            })
            .collect();
        let cases = classify_log(&records, MAX_TORN_TAIL);
        let mut discard: Vec<Slot> = Vec::new();
        let mut crashes: Vec<(Slot, SlotHealth, RecoveryCase)> = Vec::new();
        for ((slot, case), (_, health)) in cases.iter().zip(evidence.records.iter()) {
            match case.verdict() {
                None => {}
                Some(CorruptionVerdict::CrashTail) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    discard.push(*slot);
                }
                Some(CorruptionVerdict::Corrupted | CorruptionVerdict::Undecidable) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    crashes.push((*slot, *health, *case));
                }
            }
        }
        if let Some(&(slot, health, case)) = crashes.first() {
            if self.demo_truncate {
                // THE BUG under demonstration: truncate on the mismatch
                // instead of crashing.
                self.demo_truncate_from(slot)?;
                return Ok(());
            }
            let _ = self.with_world(|w| {
                // Detection is certain and terminal: restarting cannot help a
                // record that genuinely rotted (rot injection already parked
                // the node; classifier-derived verdicts park it here).
                w.park(&key, node);
                for (i, (crash_slot, crash_health, crash_case)) in crashes.iter().enumerate() {
                    match crash_health.entry {
                        RecordHealth::Faulty => w.s7.bitflip_detected = true,
                        RecordHealth::Lost => w.s7.lost_write_detected = true,
                        RecordHealth::Misdirected => w.s7.misdirected_detected = true,
                        RecordHealth::Clean => {}
                    }
                    match crash_case {
                        RecoveryCase::CorruptionBelowTail => w.s7.corruption_below_tail = true,
                        RecoveryCase::LastEntryAmbiguity => w.s7.last_entry_ambiguity = true,
                        RecoveryCase::IdentifierLostWithEntry => w.s7.identifier_lost = true,
                        _ => {}
                    }
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
        // BUGGIFY site 2: the fsync fails — only when the stage actually holds
        // something (an empty flush has nothing at stake). On the durable leg
        // the flush happens anyway before the error is reported (fsyncgate);
        // on the lost leg the stage stays un-flushed and dies with the
        // incarnation the driver's crash decision is about to unwind.
        if !self.staged_records().is_empty()
            && let Some(persisted) = self.roll_fsync_fault()
        {
            if persisted {
                self.flush_stage()?;
            } else if self.faults.active() && sim_random::<f64>() < P_TORN_TAIL {
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
        if slot != expected {
            eprintln!(
                "CONTIGUITY: node={} slot={} expected={} chosen_index={} applied_count={}",
                self.node_id, slot.0, expected.0, chosen_index.0, self.application.applied_count
            );
        }
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
}

/// Simulation hooks for driver decisions that process-level attrition cannot
/// reach. Every behavior has its own `BUGGIFY` location, so activation is
/// independent and replayable. All hooks turn off with the chaos window, leaving
/// the settle tail genuinely quiet for convergence.
struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
    /// Write-window crash bias (issue #19 B, the `TigerBeetle` "×10 while writes
    /// are in flight" pressure): a workload-buggified multiplier on the
    /// durability-seam crash probability. The seams are only ever consulted
    /// with a batch in flight, so biasing them *is* biasing crashes into the
    /// write window. Drawn per seed, per node, FDB knob style.
    seam_crash_bias: f64,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    fn new(time: T, cutoff: Duration, enabled: bool) -> Self {
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = buggify_knob!(1_u64, 4_u64..11_u64) as f64;
        Self {
            time,
            cutoff,
            enabled,
            seam_crash_bias,
        }
    }

    fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn crash_at(&self, seam: Seam) -> bool {
        let prob = 0.03 * self.seam_crash_bias;
        let fired = self.active()
            && match seam {
                Seam::BeforeSync => buggify_with_prob!(prob),
                Seam::AfterSyncBeforeSend => buggify_with_prob!(prob),
                Seam::AfterApplyBeforeSync => buggify_with_prob!(prob),
            };
        if fired && self.seam_crash_bias > 1.0 {
            // BUGGIFY pairing: the biased write-window crash pressure genuinely
            // fires on some seed (no slot is created when it never does).
            assert_reachable!("a write-window-biased seam crash fires");
        }
        fired
    }

    fn skip_accept_resend(&self) -> bool {
        self.active() && buggify_with_prob!(0.95)
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }

    fn shortest_election_timeout(&self) -> bool {
        self.active() && buggify_with_prob!(0.5)
    }

    fn drop_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Three locations, selected independently per seed: an isolated
        // `Accept` loss is the interleaving behind a stranded chosen-gap wedge
        // (#80) — one earlier slot's Accept vanishes while later slots land —
        // while losing a `Promise`/`Prepare` stretches elections open, and a
        // lost `Nack` keeps a below-floor candidate's campaign alive long
        // enough for the answering snapshot to land mid-election (the
        // truncated-quorum Nack otherwise steps the candidate down before the
        // `CatchUpRequest`'s snapshot offer arrives — the #88 window).
        match msg {
            Message::Accept { .. } => buggify_with_prob!(0.05),
            Message::Prepare { .. } | Message::Promise { .. } => buggify_with_prob!(0.10),
            Message::Nack { .. } => buggify_with_prob!(0.25),
            // A dropped `Commit` delays a follower's floor-raise (truncation
            // applies lazily at its Truncate slot), widening the mixed-floor
            // window the #88 mid-election snapshot needs — and leaves the
            // follower hole commit-replay catch-up must heal (#80's terrain).
            Message::Commit { .. } => buggify_with_prob!(0.05),
            // The lost *ack*: a slot durably accepted by a quorum whose
            // proposer never learns it — the pure quorum-intersection edge
            // that forces a re-propose under a new ballot (P2c for real).
            Message::Accepted { .. } => buggify_with_prob!(0.05),
            // Starve the read fence / the catch-up push direction. Kept low:
            // these fire per tick per peer, and a high rate is just a
            // partition, which is moonpool's job.
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => buggify_with_prob!(0.02),
            // Repair traffic for a node that is already behind: a lost
            // response costs one beat of latency and re-derives on the next.
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn duplicate_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Moonpool has no message-duplication fault, so this seam is the only
        // duplicate generator. The quorum-counting kinds are the point of the
        // location: every quorum in the core is set-based today, and this
        // keeps a future "optimization" into counters from fabricating a
        // quorum out of a duplicated ack.
        match msg {
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. } => {
                buggify_with_prob!(0.05)
            }
            Message::Commit { .. } => buggify_with_prob!(0.05),
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        if !self.active() {
            return false;
        }
        // Dropped *after* the server committed/applied: the client's retry
        // must take the `(client, seq)` dedup path, the at-most-once edge the
        // truncated-dedup-window hazard lives on. One location per reply kind.
        match reply {
            paros::Reply::Propose => buggify_with_prob!(0.10),
            paros::Reply::ProposeDedup => buggify_with_prob!(0.10),
            paros::Reply::Read => buggify_with_prob!(0.10),
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
            stats.clean_quorum_everywhere = false;
        }
    }
    stats
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

/// Snapshot of the Stage-7 corruption ground truth, folded for `check()`.
pub(crate) struct CorruptionStats {
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
            | CorruptionOutcome::DemoTruncated => {}
        }
    }
    CorruptionStats {
        crashed,
        accounted,
        parked: guard.parked.len(),
        parked_within_budget: guard.parked.len() <= guard.dead_budget(),
        flags: guard.s7,
    }
}

/// The storage-fault coverage gates + the injected↔detected correlation,
/// evaluated once per run from the workload's `check()` (the shared-gate
/// doctrine in [`crate::audit`]). The correlation is safety and runs on every
/// scope; the quadrant `sometimes` gates saturate on the main campaign only.
pub(crate) fn check_storage_gates(handle: &StateHandle, scope: GateScope) {
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
    check_corruption_gates(handle, &corruption, scope);
    if scope != GateScope::Full {
        return;
    }
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
fn check_corruption_gates(handle: &StateHandle, corruption: &CorruptionStats, scope: GateScope) {
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
    if scope != GateScope::Full {
        return;
    }
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
    assert_sometimes!(
        s7.block_fault_detected,
        "storage: a block fault corrupts a contiguous run of entries"
    );
}
