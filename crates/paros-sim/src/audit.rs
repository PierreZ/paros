//! The sim-side [`Audit`] implementation: one shared incremental checker.
//!
//! This is where the paros correctness invariants live. The driver reports each
//! externally meaningful transition exactly once, at the instant it happens, so
//! every check here is **O(1) in the size of the run** — a map probe and a
//! comparison — rather than a re-scan of a growing event stream. (The
//! trace-scanning oracle tier this replaced re-copied and re-walked the whole
//! event history on every observability pump, which is quadratic in the run
//! length; profiling put ~85% of a sancov campaign inside `run_invariants`.)
//!
//! Layout mirrors `crate::node`'s storage world: one [`AuditWorld`] per
//! simulation iteration, published under a well-known [`StateHandle`] key so
//! every node process and every workload reaches the same instance, and
//! factory-created per iteration so recipe replay is exact. Each node wraps it
//! in a [`NodeAudit`], which stamps simulated time on the observations that need
//! temporal reasoning (quiescence).
//!
//! Coverage gates split by *when* they can be judged. A `reachable` gate fires
//! at the transition instant — that is what makes it an exploration anchor — and
//! is de-duplicated by a sticky flag so it costs one branch afterwards. A
//! `sometimes` gate has to be recorded once per run whether or not it held,
//! otherwise a gate that never fires anywhere would silently vanish from the
//! saturation denominator, so those are evaluated once from the workload's
//! `check()` phase through [`AuditWorld::check_gates`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::{
    Audit, Ballot, HANDOFF_BATCH, Handoff, LEADER_RECOVERY_BATCH, Message, NodeId, PROMISE_BATCH,
    SNAP_CHUNK_BYTES, Seam, Slot, StorageError, StorageFaultDecision, StorageRecord, command_hash,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`AuditWorld`] is published (shared by every node and every workload).
const AUDIT_WORLD_KEY: &str = "paros-audit-world";

/// Grace (ms) after chaos ends — and after the cluster's chosen prefix last
/// grew, and after leadership last changed hands — before a still-open chosen
/// gap is a real liveness failure rather than an ordinary transient.
/// Consecutive same-hole `chosen_gap` reports (one per node tick) a quiesced
/// cluster may show before the hole counts as a wedge. Forty ticks is several
/// election timeouts' worth of real healing opportunities on the protocol's
/// own clock, immune to wall-time dilation from buggified sleep delays.
const GAP_WEDGE_TICKS: u64 = 40;

const CONVERGENCE_GRACE_MS: u64 = 3_000;

/// Consecutive **deposed heartbeats** a leader may broadcast before the checker
/// calls it a zombie (#95). A leader whose ballot a promise-majority has moved
/// strictly past can never again assemble any quorum at that ballot — every
/// below-promise beat is ignored unacked — yet without `CheckQuorum` nothing ever
/// demotes it while it is partitioned from the very peers that could tell it.
/// `CheckQuorum` bounds the zombie window to one ack-quorum-less election-timeout
/// window (at most ~10 ticks, one beat per tick); forty beats is ~4x that on
/// the protocol's own clock, immune to wall-time dilation.
const DEPOSED_BEAT_STREAK: u64 = 40;

/// Cap on the committed-operation history the interval checker walks pairwise.
/// The current workloads stay far below it (a few dozen operations per client);
/// the cap only bounds the `O(n^2)` walk if a future workload explodes.
const LIN_HISTORY_CAP: usize = 512;

/// Get-or-create the singleton [`AuditWorld`] for this iteration. Get-then-
/// publish is race-free: the sim executor is single-threaded and this runs
/// synchronously (no `.await` between the get and the publish).
pub(crate) fn audit_world(state: &StateHandle) -> Arc<AuditWorld> {
    if let Some(world) = state.get::<Arc<AuditWorld>>(AUDIT_WORLD_KEY) {
        return world;
    }
    let world = Arc::new(AuditWorld::default());
    state.publish(AUDIT_WORLD_KEY, world.clone());
    world
}

/// Which family of coverage gates a workload's `check()` should record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GateScope {
    /// The safety axis only (the network-swarm campaign, whose provider faults
    /// outlive `chaos_duration`, so it makes no liveness or coverage claim
    /// beyond safety).
    SafetyOnly,
    /// Every protocol/driver gate the main campaign saturates on.
    Full,
    /// The budget-off campaign (issue #21, the WAITED leg): safety plus the
    /// recovered/waited pair — no full-liveness claim, since a run may
    /// correctly end unavailable.
    BudgetOff,
}

/// Fire a `reachable` gate the first time its sticky flag flips.
macro_rules! reach_once {
    ($flag:expr, $message:expr) => {
        if !$flag {
            $flag = true;
            assert_reachable!($message);
        }
    };
}

/// The per-iteration shared checker.
#[derive(Default)]
pub(crate) struct AuditWorld {
    state: Mutex<AuditState>,
}

impl AuditWorld {
    fn lock(&self) -> MutexGuard<'_, AuditState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Arm the quiescence-gated liveness checks. Off by default so a campaign
    /// whose faults outlive `chaos_duration` (the network-swarm safety axis)
    /// never claims a quiet tail it does not have.
    pub(crate) fn enable_liveness_checks(&self) {
        self.lock().liveness = true;
    }

    /// Record this run's `sometimes` coverage gates. Called once per workload
    /// from the `check()` phase; repeating it (several client workloads run
    /// concurrently) is idempotent — a `sometimes` slot only accumulates
    /// samples, and an `always` re-check of the same true fact is free.
    pub(crate) fn check_gates(&self, scope: GateScope) {
        let st = self.lock();
        // Liveness reachability: a value does get chosen.
        assert_sometimes!(st.any_chosen, "a value is eventually chosen");
        // The per-ballot proposal check is only as good as the field it reads;
        // saturation has to see it actually compare something.
        assert_sometimes!(
            st.any_proposal_checked,
            "a proposed command is checked against its ballot's other proposals"
        );
        assert_sometimes!(
            st.any_ack_checked,
            "a committed write ack is checked against the acking node's applied prefix"
        );
        match scope {
            GateScope::SafetyOnly => return,
            GateScope::BudgetOff => {
                // The #71 pair: a budget-off sweep must exercise BOTH legs of
                // the CTRL guarantee — repair where a clean copy survives, and
                // a *correct* wait where none does. Without the WAITED gate a
                // sweep can go green having only ever recovered.
                assert_sometimes!(
                    st.repaired_seen,
                    "recovery repaired a faulty record from a surviving clean copy"
                );
                assert_sometimes!(
                    st.waited,
                    "recovery correctly WAITED: no clean copy of a committed item remains"
                );
                return;
            }
            GateScope::Full => {}
        }
        st.check_protocol_gates();
        st.check_driver_hook_gates();
    }

    /// Merge one client's recorded history into the shared one and run the
    /// client-visible checks over everything merged so far. Every client
    /// workload calls this from `check()`; the merged history only grows, so a
    /// later caller sees a superset and the checks stay sound at every step.
    pub(crate) fn check_client_history(&self, history: &ClientHistory) {
        let mut st = self.lock();
        st.lin.merge(history);
        st.check_client_history();
    }

    /// How many Stage-6 write/fsync faults the drivers *detected* (one typed
    /// [`Audit::storage_fault`] crash decision each). The workload's `check()`
    /// correlates this against the storage world's injected ground truth.
    pub(crate) fn storage_faults_detected(&self) -> u64 {
        self.lock().storage_faults_detected
    }

    /// How many Stage-7 corruption/metadata detections the drivers surfaced
    /// as typed crash decisions. Correlated 1:1 against the world's
    /// corruption ledger by the workload's `check()`.
    pub(crate) fn corruption_faults_detected(&self) -> u64 {
        self.lock().corruption_crashes
    }

    /// A node was terminally parked by a detected persistent corruption
    /// (detect ⇒ crash, stays down for the run). Convergence excuses exactly
    /// these nodes — and only when the crash decision that explains the
    /// unavailability was actually observed (the asymmetric oracle:
    /// unavailable = pass, unsafe = fail — but *unexplained* unavailable is
    /// still a failure).
    pub(crate) fn note_storage_dead(&self, node: u64) {
        self.lock().storage_dead.insert(node);
    }

    /// Ground truth from the storage world (budget-off runs only): `slot` has
    /// no readable copy anywhere. The wedge and convergence oracles excuse
    /// exactly this slot — a *correct* unavailability — and the WAITED gate
    /// records that the leg was genuinely exercised.
    pub(crate) fn note_unrecoverable(&self, slot: u64) {
        self.lock().unrecoverable.insert(slot);
    }

    /// Whether any node has been observed lagging the cluster prefix (one leg
    /// of the #71 compound corruption x partition x lag gate).
    pub(crate) fn lag_observed(&self) -> bool {
        !self.lock().lagged.is_empty()
    }

    /// Red-demo side door (faulty-as-none only): record the classification the
    /// demo deliberately withholds from the protocol, so the *storage*
    /// divergence legs stay explained and the surviving red is the mutation's
    /// genuine protocol consequence — a unanimous-looking `none` no-op filling
    /// a chosen slot. Fires from the boot scan, i.e. *before* the boot's
    /// `recovered` report, so it stages exactly like the driver's own
    /// faulty report and becomes live for that incarnation at the swap.
    pub(crate) fn note_reported_faulty(&self, node: u64, slot: u64) {
        self.lock()
            .faulty_staged
            .entry(node)
            .or_default()
            .insert(slot);
    }

    /// Ground-truth feed from the storage world (issue #19 C). A record can
    /// become durable through an *ambiguous* fault leg — the flush happened,
    /// but the driver crashed on the reported error before surfacing it — so
    /// the driver's audit stream alone would go stale and the next reboot
    /// would trip the cross-restart checks as false positives. The world owns
    /// the ground truth, so every flush refreshes the **reference data** those
    /// checks compare against: the per-`(node, slot)` persisted value, the
    /// compaction floor, and the admitted snapshot landings. Reference data
    /// only — progress/liveness state (`applied_max`, quiescence clocks) stays
    /// driver-reported, so this observation cannot mask a liveness bug. This
    /// is what keeps recovered-equals-persisted checkable against *actual*
    /// durable state (the #71 weakening is for Stage 7-8, not this).
    pub(crate) fn note_flushed_ground_truth(
        &self,
        node: u64,
        now_ms: u64,
        accepted: &[(u64, u64)],
        floor: Option<u64>,
        snapshot_landing: Option<u64>,
    ) {
        let mut st = self.lock();
        for &(slot, vhash) in accepted {
            st.persisted.insert((node, slot), vhash);
        }
        if let Some(first) = floor {
            st.floor.entry(node).or_default().raise(first, now_ms);
        }
        if let Some(landing) = snapshot_landing {
            st.snap_landings.entry(node).or_default().insert(landing);
        }
    }

    /// The convergence deliverable, judged when no future leader change can
    /// invalidate a provisional quiescence decision: every node this run brought
    /// up ends on the cluster's chosen prefix.
    pub(crate) fn check_final_convergence(&self) {
        let mut st = self.lock();
        let Some(cluster_max) = st.applied_max.values().copied().max() else {
            return;
        };
        let cluster: BTreeSet<u64> = st
            .booted
            .iter()
            .copied()
            .chain(st.applied_max.keys().copied())
            .collect();
        // Stage 7's asymmetric availability oracle: a node terminally parked
        // by detect ⇒ crash is excused from convergence — but only when the
        // crash decision explaining its unavailability was actually observed,
        // and only for a minority (the world's dead-node budget, re-asserted
        // in `check_storage_gates`). Unexplained unavailability stays a
        // failure.
        for node in &st.storage_dead {
            assert_always!(
                st.corruption_crashed_nodes.contains(node),
                "storage: a node that stays down is explained by a detected corruption crash",
                { "node" => *node }
            );
        }
        for node in cluster {
            if st.storage_dead.contains(&node) {
                continue;
            }
            let prefix = st.applied_max.get(&node).copied();
            // Stage 8's WAITED excuse: a node whose next needed slot (or any
            // slot between it and the cluster maximum) has no readable copy
            // anywhere is *correctly* held below the prefix — that is the
            // guarantee, not a failure. Ground truth only (budget-off runs).
            let next_needed = prefix.map_or(0, |p| p + 1);
            let held_at_unrecoverable = st
                .unrecoverable
                .range(next_needed..=cluster_max)
                .next()
                .is_some();
            if held_at_unrecoverable && prefix != Some(cluster_max) {
                st.waited = true;
                continue;
            }
            assert_always!(
                prefix == Some(cluster_max),
                "every node converges to the cluster's chosen prefix at the end of the settle tail",
                {
                    "node" => node,
                    "prefix" => prefix.map_or(-1_i64, |p| i64::try_from(p).unwrap_or(i64::MAX)),
                    "cluster_max" => cluster_max
                }
            );
        }
        // Proof the catch-up path actually healed a hole (not merely that
        // nothing ever broke) — judged against the FINAL cluster maximum, so a
        // transient mid-run match cannot satisfy it.
        let healed = st
            .lagged
            .iter()
            .any(|n| st.applied_max.get(n).copied() == Some(cluster_max))
            && cluster_max > 0;
        if healed {
            reach_once!(st.caught_up, "a lagging node converges via catch-up");
        }
        assert_sometimes!(
            st.caught_up,
            "a lagging node catches up to the cluster's chosen prefix"
        );
    }
}

/// One leader's deposed-heartbeat streak (#95): the ballot it is beating at,
/// the last beat seq counted (one broadcast fans out to n-1 sends, so the seq
/// dedups the fan-out), and how many consecutive beats were sent while a
/// promise-majority sat strictly above the ballot.
#[derive(Clone, Copy, Default)]
struct DeposedStreak {
    round: u64,
    node: u64,
    seq: u64,
    count: u64,
}

/// Who is exercising one logical Phase-2 authority (one ballot), reconstructed
/// **from semantic events only** — the `Accept`s actually put on the wire, and
/// the relinquish/install transitions — never from any node's `role` field.
/// Reading a node's own belief about its leadership would only re-derive the
/// implementation's interpretation; this re-derives the *observable* one.
#[derive(Clone, Debug, Default)]
struct Authority {
    /// The single node currently observed exercising this ballot.
    holder: Option<u64>,
    /// Nodes that have permanently given this authority up. The `DPaxos` rule:
    /// an authority is relinquished at most once per node, and never exercised
    /// again afterwards.
    retired: BTreeSet<u64>,
    /// The highest allocator frontier this authority has been transferred with.
    /// Monotone: a successor that rewound it could propose a *different*
    /// command at a `(slot, ballot)` its predecessor already used.
    frontier: u64,
}

/// One node's compaction floor, plus what it was before the most recent raise.
///
/// The truncation checks admit a record at a slot the node compacts away *in
/// the same simulated millisecond*: the accept happened first, in-core, guarded
/// by the core's own floor check, so counting a same-instant compaction against
/// it would be a false positive. Keeping the pre-raise value is how an
/// incremental fold reproduces the "compactions strictly before this event"
/// window the trace-scanning oracle used.
///
/// Load-bearing assumption: [`Floor::strictly_before`] queries arrive in
/// non-decreasing `now_ms` (the sim clock is monotone and every caller stamps
/// its own instant); a query for a *past* instant would see too new a floor.
#[derive(Clone, Copy, Default)]
struct Floor {
    now: u64,
    before_last_raise: u64,
    raised_ms: u64,
}

impl Floor {
    /// The floor established strictly before `now_ms`.
    fn strictly_before(self, now_ms: u64) -> u64 {
        if self.raised_ms < now_ms {
            self.now
        } else {
            self.before_last_raise
        }
    }

    fn raise(&mut self, first: u64, now_ms: u64) {
        // Deliberately lenient about `first <= now`: the ground-truth flush
        // feed passes the *requested* floor through, and the storage contract
        // legally treats a lower request as a no-op (the contract suite
        // exercises exactly that). The no-regression assert lives on the
        // driver-audited truncation report instead, where the core's monotone
        // floor contract genuinely holds.
        if first <= self.now {
            return;
        }
        if self.raised_ms != now_ms {
            self.before_last_raise = self.now;
            self.raised_ms = now_ms;
        }
        self.now = first;
    }
}

/// Every incremental fact the checks need, and nothing else.
///
/// The trailing block is a *flag set*, not a state machine: one independent
/// sticky bit per `reachable` gate, each flipped once at its own transition and
/// read once at `check()`. Folding them into enums would couple gates that have
/// nothing to do with each other, so the bool-count lint is waived here.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct AuditState {
    liveness: bool,

    // --- Paxos safety -------------------------------------------------------
    /// Cluster-wide: the value chosen for each slot.
    chosen: BTreeMap<u64, u64>,
    /// Per node: the last durable promised ballot.
    promised: BTreeMap<u64, Ballot>,
    /// `(ballot round, ballot node, slot)` → the command that ballot proposed.
    proposed: BTreeMap<(u64, u64, u64), u64>,
    /// `(slot, ballot round, ballot node)` → the command durably accepted.
    accepted: BTreeMap<(u64, u64, u64), u64>,
    /// Per `(node, slot)`: the last value the node made durable.
    persisted: BTreeMap<(u64, u64), u64>,
    /// The configured cluster size (full membership), from the boot reports.
    /// One shared configuration per run, so the max across reports is it.
    cluster_size: Option<u64>,
    /// `(slot, ballot round, ballot node)` → the nodes holding a durable
    /// accept for it — the acceptor tally behind the quorum-decided oracle.
    /// Fed by both the live accept fold and the boot re-reports (idempotent).
    accept_sets: BTreeMap<(u64, u64, u64), BTreeSet<u64>>,
    /// Per slot: the first quorum-decided `(ballot round, ballot node,
    /// vhash)`. Records a decision the moment a majority of the *configured*
    /// cluster holds the durable accept — even if no node ever applies the
    /// slot, which is exactly the blind spot the apply-fed `chosen` map has.
    decided: BTreeMap<u64, (u64, u64, u64)>,
    /// The highest slot ever quorum-decided — a monotone scalar the
    /// below-floor pruning of `decided` never lowers, so the cross-restart
    /// frontier check stays sound after the whole prefix compacts away.
    decided_max: Option<u64>,
    /// Per node: the last durably reported chosen index, reset each boot
    /// (`SetChosenIndex` flushes relaxed, so a crash may legally rewind it
    /// across incarnations — within one it only advances).
    chosen_watermark: BTreeMap<u64, u64>,
    /// Per node: the highest confirmed read index served, reset each boot.
    read_watermark: BTreeMap<u64, Option<u64>>,

    // --- truncation ---------------------------------------------------------
    floor: BTreeMap<u64, Floor>,
    /// Per node: the highest floor its *driver-audited truncations* have
    /// reported — the monotonicity watermark for those reports alone (the
    /// folded [`Floor`] also absorbs installs and ground-truth flushes, which
    /// can legally outrun a reordered stale truncate; see
    /// [`NodeAudit::truncated`]).
    truncate_watermark: BTreeMap<u64, u64>,

    // --- applied prefix -----------------------------------------------------
    /// Per node: the next slot expected to be newly applied.
    frontier: BTreeMap<u64, u64>,
    /// Per node: every index it jumped to through a snapshot install.
    snap_landings: BTreeMap<u64, BTreeSet<u64>>,
    /// Per node: its applied high-water mark (absent = applied nothing).
    applied_max: BTreeMap<u64, u64>,
    cluster_applied_max: Option<u64>,
    cluster_applied_max_ms: u64,
    lagged: BTreeSet<u64>,
    booted: BTreeSet<u64>,

    // --- cooperative leader handoff -----------------------------------------
    /// `(ballot round, ballot node)` → who is exercising that logical authority
    /// (see [`Authority`]). The uniqueness oracle's whole state.
    authorities: BTreeMap<(u64, u64), Authority>,
    /// Refusal totals folded from [`Audit::handoff_refused`].
    handoff_refused: (u64, u64, u64),
    /// `(node, authority)` pairs the core decided to relinquish — the
    /// "at most once" ledger, keyed on the decision rather than the wire (one
    /// decision can be re-transmitted many times).
    relinquish_calls: BTreeSet<(u64, (u64, u64))>,
    /// How many authorities have been installed in this run.
    handoff_installs: u64,

    // --- leadership ---------------------------------------------------------
    /// Per node: its deposed-heartbeat streak (#95, see [`DeposedStreak`]).
    deposed_streaks: BTreeMap<u64, DeposedStreak>,
    leader_round: BTreeMap<u64, u64>,
    leader_rounds: BTreeSet<u64>,
    first_leader_round: Option<u64>,
    leader_change_ms: Option<u64>,
    last_leader_ms: u64,

    // --- client history -----------------------------------------------------
    lin: LinHistory,

    // --- sticky coverage flags ---------------------------------------------
    any_chosen: bool,
    any_proposal_checked: bool,
    any_ack_checked: bool,
    config_tagged_protocol_message: bool,
    any_leader: bool,
    leader_promise_checked: bool,
    compacted: bool,
    prepare_below_floor: bool,
    gap_filled: bool,
    snapshot_installed: bool,
    snapshot_offered: bool,
    snapshot_mid_election: bool,
    caught_up: bool,
    below_all_floors: bool,
    /// Per-node `(hole, consecutive quiesced-tick reports)`. The driver emits
    /// `chosen_gap` once per *node tick* while a gap lasts, so the streak counts
    /// the protocol's own clock: under moonpool's buggified sleep delay, wall
    /// sim time dilates during the chaos window but a tick is still one real
    /// opportunity for an election timeout / re-send to heal the hole. A wedge
    /// is a hole that survives
    /// [`GAP_WEDGE_TICKS`] such opportunities after quiescence; a merely
    /// slowed cluster never accumulates the streak (seed 8057455177754870256).
    gap_streaks: BTreeMap<u64, (u64, u64)>,
    /// At-most-once ledger for the oracle: each applied user command's
    /// `(client, seq)` and the single log index it applied at. A second apply
    /// of the same identity at a *different* index is the double-apply the
    /// core review flagged (mandatory P2c re-proposal of a stale suffix after
    /// a healed partition) — every node applies it, so per-index agreement is
    /// blind to it by construction.
    applied_identity: BTreeMap<(u64, u64), u64>,
    /// The #94 suppression fired: a re-chosen `(client, seq)` executed as a
    /// no-op. Reachable-only (no `sometimes` counterpart): the interleaving
    /// needs a partition-shaped seed and would starve saturation as a per-run
    /// gate, but when a seed does reach it, the sweep records it.
    duplicate_suppressed: bool,
    /// `CheckQuorum` fired (#95): a leader without an ack quorum for a full
    /// election-timeout window demoted itself. The n=2 regime plus attrition
    /// generates it reliably (killing the only peer starves the window).
    quorum_lost: bool,
    /// A parked proposal reply was superseded by a different decided command
    /// and answered with a redirect instead of a false commit. Reachable-only:
    /// needs a stale leader learning a foreign decision for a slot it admitted.
    waiter_superseded: bool,
    multi_slot_applied: bool,
    several_slots_applied: bool,
    leadership_turnover: bool,
    crashed_before_sync: bool,
    crashed_after_sync: bool,
    /// Typed Stage-6 write/fsync crash decisions folded in
    /// ([`Audit::storage_fault`] with `Io`/`FsyncFailed`).
    storage_faults_detected: u64,
    storage_fault_crashed: bool,
    /// Typed Stage-7 corruption/metadata crash decisions folded in.
    corruption_crashes: u64,
    corruption_crashed: bool,
    /// Explanation state for the recovered-vs-persisted divergence leg (#71,
    /// first leg): the accepted records — and the nodes — whose corruption
    /// crash was actually observed. A boot missing a persisted record is
    /// legal iff explained here (the peer-heal leg arrives in Stage 8).
    corruption_crashed_records: BTreeSet<(u64, u64)>,
    corruption_crashed_nodes: BTreeSet<u64>,
    /// Nodes terminally parked by detect ⇒ crash (fed by the sim node loop).
    storage_dead: BTreeSet<u64>,
    /// Stage 8: per node, the slots its boot scan classified recoverable and
    /// reported into the tri-state — the second explanation the divergence
    /// and no-gaps checks accept (#71's explained-only rule). Scoped to the
    /// node's **current incarnation**: each boot re-runs the scan and
    /// re-reports what is still faulty, so a stale excuse from a previous
    /// boot must not keep explaining gaps forever.
    reported_faulty: BTreeMap<u64, BTreeSet<u64>>,
    /// Faulty reports staged since the node's last boot report: the scan (and
    /// the red-demo side door) speak *before* [`Audit::recovered`] fires, so
    /// the swap-in happens there — the boot report is the incarnation edge.
    faulty_staged: BTreeMap<u64, BTreeSet<u64>>,
    /// Stage 8 ground truth (budget-off only): slots with no readable copy
    /// anywhere. The wedge/convergence excuse — and the WAITED witness.
    unrecoverable: BTreeSet<u64>,
    /// The WAITED leg fired: the cluster correctly held position at an
    /// unrecoverable committed item instead of fabricating or losing data.
    waited: bool,
    /// Repair progress observed (from [`Audit::repair_progress`]): in-place
    /// repairs, straggler Case-1 re-proposals, Case-2 no-op fills, and
    /// recovery-timeout resignations.
    repaired_seen: bool,
    case1_seen: bool,
    case2_seen: bool,
    repair_stepdown_seen: bool,
    app_repair_seen: bool,
    app_repair_below_floor_seen: bool,
    /// #101: decided snapshot points each node has durably recorded — the
    /// per-node custody facts the truncation-coupling check reads.
    snap_points: BTreeMap<u64, BTreeSet<u64>>,
    snap_recorded_seen: bool,
    snap_chunks_reported_seen: bool,
    snap_chunk_repaired_seen: bool,
    snap_fallback_seen: bool,
    snap_restore_seen: bool,
    resend_skipped: bool,
    resigned: bool,
    /// Cooperative-handoff coverage: one sticky bit per distinct fact.
    handoff_relinquished: bool,
    handoff_installed: bool,
    /// The payoff: an installed authority streamed Phase 2 without any Phase 1
    /// of its own — the whole point of the `DPaxos` technique.
    handoff_streamed_without_phase1: bool,
    /// A transfer carried unfinished business (an accepted-but-unchosen tail).
    handoff_carried_tail: bool,
    /// Leadership was handed over more than once in this run. Distinct
    /// authorities: one authority is handed on at most once (see
    /// `RawNode::can_relinquish`'s *One hop only*).
    handoff_repeated: bool,
    /// A refusal path fired: wrong addressee/non-member, stale authority, or a
    /// malformed tail.
    handoff_refused_target: bool,
    handoff_refused_stale: bool,
    handoff_refused_shape: bool,
    /// A handoff-installed leadership resigned on its uncovered inherited
    /// fence — the deliberate fallback to ordinary Phase 1.
    handoff_fence_expired: bool,
    /// A relinquishment was lost at the send seam (the availability-only
    /// failure mode a handoff deliberately accepts).
    dropped_relinquish: bool,
    duplicated_relinquish: bool,
    compact_ack_accepted: bool,
    compact_ack_refused: bool,
    mailbox_dropped: bool,
    offer_skipped: bool,
    shortest_timeout: bool,
    dropped_accept: bool,
    dropped_election: bool,
    dropped_commit: bool,
    dropped_accepted: bool,
    dropped_heartbeat: bool,
    dropped_repair: bool,
    dropped_catchup_request: bool,
    dropped_snap_ack: bool,
    dropped_snap_chunk_request: bool,
    dropped_snap_chunk_response: bool,
    dropped_check_leader: bool,
    crashed_after_apply: bool,
    crashed_before_chunk_sync: bool,
    crashed_after_chunk_restore: bool,
    duplicated_any: bool,
    duplicated_quorum_kind: bool,
    duplicated_commit: bool,
    duplicated_repair: bool,
    duplicated_catchup_request: bool,
    duplicated_snap_ack: bool,
    duplicated_snap_chunk_request: bool,
    duplicated_snap_chunk_response: bool,
    reply_dropped: bool,
    propose_reply_dropped: bool,
    read_reply_dropped: bool,
    dedup_after_dropped_reply: bool,
}

impl AuditState {
    /// Whether the run has genuinely settled: chaos is over, the cluster's
    /// chosen prefix has not grown for [`CONVERGENCE_GRACE_MS`], and leadership
    /// has not changed hands for just as long. Before this gate, holes are
    /// legitimate transients and nothing is asserted.
    fn quiesced(&self, now_ms: u64) -> bool {
        now_ms > crate::CHAOS_DURATION_MS + CONVERGENCE_GRACE_MS
            && now_ms.saturating_sub(self.cluster_applied_max_ms) > CONVERGENCE_GRACE_MS
            && now_ms.saturating_sub(self.last_leader_ms) > CONVERGENCE_GRACE_MS
    }

    /// The protocol-level `sometimes` gates: progress, truncation, snapshot and
    /// the multi-slot log. Their `reachable` counterparts already fired at their
    /// transition instants.
    fn check_protocol_gates(&self) {
        let max_applied = self.cluster_applied_max.unwrap_or(0);
        // The log is multi-slot (a stable leader streamed past slot 0).
        assert_sometimes!(max_applied >= 2, "a multi-slot prefix is applied");
        assert_sometimes!(max_applied >= 3, "a stable leader streams several slots");
        assert_sometimes!(
            self.leader_rounds.len() >= 2,
            "leadership turns over and the cluster recovers"
        );
        assert_sometimes!(self.any_leader, "a leader is elected");
        assert_sometimes!(
            self.config_tagged_protocol_message,
            "a protocol message carries a configuration identity"
        );
        // The #67 check reads a promise and a won ballot; saturation has to see
        // it actually compare something.
        assert_sometimes!(
            self.leader_promise_checked,
            "a fresh leader's promise is checked against the ballot it won"
        );
        // #61's cluster-size regimes are actually visited: the quorum edge
        // cases (a singleton that decides alone; a pair where any attrition
        // freezes progress until recovery) and the n>=5 shape whose accept
        // quorums can avoid a two-node pin. Every node boots at start, so the
        // booted set is the drawn topology.
        let n = self.booted.len();
        assert_sometimes!(n == 1, "a run drives a single-node cluster");
        assert_sometimes!(
            n == 2,
            "a run drives a two-node cluster (any attrition freezes it)"
        );
        assert_sometimes!(n >= 5, "a run drives a five-node cluster");
        // Compaction actually happens (the workload drives it every run).
        assert_sometimes!(self.compacted, "the log is compacted (truncation happens)");
        // The #101 coupling's other half: compaction implies a decided
        // snapshot point was recorded first, so this saturates wherever the
        // compaction gate does.
        assert_sometimes!(
            self.snap_recorded_seen,
            "storage: a decided snapshot point is recorded"
        );
        assert_sometimes!(
            self.snapshot_installed,
            "a below-floor node recovers via snapshot transfer"
        );
        // The #88 mid-election install window is anchored by the `reach_once!`
        // in [`AuditWorld::snapshot_mid_election`], not demanded per sweep:
        // #101 made whole-blob installs structurally rare (a below-floor node
        // with a clean covering point restores locally, and rotted chunks
        // repair chunk-wise), so the install x live-election coincidence is a
        // leg the swarm is no longer *certain* to visit. Per the assertion
        // doctrine such a leg anchors exploration when hit and never fails
        // coverage (same shape as the block-fault family gate).
        // CheckQuorum (#95) is actually exercised: some seed isolates a leader
        // from its ack quorum long enough that it demotes itself (the n=2
        // regime plus attrition is the reliable generator).
        assert_sometimes!(
            self.quorum_lost,
            "a leader without an ack quorum steps down (CheckQuorum)"
        );
    }

    /// The driver's durability seams and rare-but-valid policy decisions are
    /// actually taken on some seeds. Asserts no new safety property; it proves
    /// the hooks are still connected, since perturbations that stopped firing
    /// would leave a sweep looking green while quietly testing less.
    fn check_driver_hook_gates(&self) {
        assert_sometimes!(
            self.crashed_after_sync,
            "the driver crashes after sync and before sending a batch"
        );
        assert_sometimes!(
            self.crashed_before_sync,
            "the driver crashes before syncing a staged batch"
        );
        assert_sometimes!(
            self.snapshot_offered,
            "a snapshot offer enters the driver's common outbound path"
        );
        assert_sometimes!(
            self.shortest_timeout,
            "the driver selects the shortest valid election timeout"
        );
        assert_sometimes!(
            self.resend_skipped,
            "the driver skips a pending accept re-send"
        );
        assert_sometimes!(self.resigned, "the driver voluntarily resigns leadership");
        assert_sometimes!(
            self.dropped_accept,
            "the driver drops one isolated accept at the send seam"
        );
        assert_sometimes!(
            self.dropped_election,
            "the driver drops an election message at the send seam"
        );
        assert_sometimes!(
            self.dropped_commit,
            "the driver drops a commit at the send seam"
        );
        assert_sometimes!(
            self.dropped_accepted,
            "the driver drops an accepted ack at the send seam"
        );
        assert_sometimes!(
            self.dropped_heartbeat,
            "the driver drops a heartbeat at the send seam"
        );
        assert_sometimes!(
            self.dropped_repair,
            "the driver drops a repair message at the send seam"
        );
        assert_sometimes!(
            self.crashed_after_apply,
            "the driver crashes after applying a batch and before its application fsync"
        );
        assert_sometimes!(
            self.duplicated_any,
            "the driver duplicates a message at the send seam"
        );
        assert_sometimes!(
            self.duplicated_quorum_kind,
            "the driver duplicates a quorum-counting message at the send seam"
        );
        assert_sometimes!(
            self.duplicated_commit,
            "the driver duplicates a commit at the send seam"
        );
        assert_sometimes!(
            self.duplicated_repair,
            "the driver duplicates a repair message at the send seam"
        );
        assert_sometimes!(
            self.reply_dropped,
            "a committed client reply is dropped at the reply seam"
        );
        assert_sometimes!(
            self.dedup_after_dropped_reply,
            "a committed proposal ack is lost and the retry takes the dedup path"
        );
        self.check_handoff_gates();
    }

    /// The cooperative-handoff coverage gates.
    ///
    /// Split by *what each one proves* rather than lumped into one "handoff
    /// happened" bit: a campaign that only ever transfers settled leaderships,
    /// or only ever completes them, would saturate a single gate while leaving
    /// the interesting halves of the design — the inherited tail, the refusal
    /// paths, the fallback to Phase 1 — entirely unexercised.
    ///
    /// The two ends of the spectrum are deliberately `reachable`-only rather
    /// than `sometimes`: a refused *shape* needs a payload damaged in flight,
    /// and an expired inherited fence needs a decision that departed with its
    /// only holder. Both are genuine, both are rare, and gating every run on
    /// them would starve saturation.
    fn check_handoff_gates(&self) {
        assert_sometimes!(
            self.handoff_relinquished,
            "a leader cooperatively hands its authority on"
        );
        assert_sometimes!(
            self.handoff_installed,
            "a successor installs a transferred authority"
        );
        assert_sometimes!(
            self.handoff_streamed_without_phase1,
            "a handed-over authority continues Phase 2 without another Phase 1"
        );
        assert_sometimes!(
            self.handoff_carried_tail,
            "a handoff carries accepted-but-unchosen work"
        );
        assert_sometimes!(
            self.handoff_repeated,
            "leadership is handed over more than once in a run"
        );
        assert_sometimes!(
            self.dropped_relinquish,
            "a relinquishment is lost and the cluster falls back to an election"
        );
        assert_sometimes!(
            self.handoff_refused_stale,
            "a superseded handoff is refused by its target"
        );
    }

    /// A node's promised ballot is monotonic — it never decreases, including
    /// across a restart (the boot re-reports the recovered promise).
    fn observe_promise(&mut self, node: u64, ballot: Ballot) {
        if let Some(prev) = self.promised.insert(node, ballot) {
            assert_always!(ballot >= prev, "a node's promised ballot never decreases");
        }
    }

    /// Fold one durable accept into the acceptor tally and run the
    /// quorum-decided oracle. Fed by the live accept fold *and* the boot
    /// re-reports (a `BTreeSet` makes the re-fold idempotent), so a value a
    /// majority durably accepted is **decided** here even when no node ever
    /// applies it — the case a buggy leader's later no-op fill would
    /// otherwise hide from the apply-fed `chosen` map. Quorum arithmetic uses
    /// the *configured* cluster size from the boot reports, never the booted
    /// subset (which under-counts while nodes are still coming up).
    fn observe_durable_accept(&mut self, node: u64, slot: u64, ballot: Ballot, vhash: u64) {
        let key = (slot, ballot.round, ballot.node.0);
        if let Some(prev) = self.accepted.insert(key, vhash) {
            assert_always!(
                prev == vhash,
                "at most one command is ever accepted for one (slot, ballot)"
            );
        }
        // P2, observed on durable state: once a slot is decided at some
        // ballot, every accept at or above that ballot carries the decided
        // value (a proposer above it must have adopted it via P2c).
        if let Some(&(round, bnode, decided_vhash)) = self.decided.get(&slot)
            && (ballot.round, ballot.node.0) >= (round, bnode)
        {
            assert_always!(
                vhash == decided_vhash,
                "an accept at or above a decided ballot carries the decided value",
                {
                    "node" => node,
                    "slot" => slot,
                    "round" => ballot.round,
                    "decided_round" => round
                }
            );
        }
        let holders = self.accept_sets.entry(key).or_default();
        holders.insert(node);
        let quorum = self.cluster_size.map(|n| n / 2 + 1);
        if quorum.is_some_and(|q| u64::try_from(holders.len()).unwrap_or(u64::MAX) >= q) {
            match self.decided.get(&slot) {
                None => {
                    self.decided
                        .insert(slot, (ballot.round, ballot.node.0, vhash));
                    self.decided_max = Some(self.decided_max.map_or(slot, |m| m.max(slot)));
                }
                // Two quorums (at any two ballots) must agree — the crown
                // jewel judged on durable accepts alone, with no apply in the
                // loop. The first decision wins the recorded ballot.
                Some(&(_, _, decided_vhash)) => {
                    assert_always!(
                        vhash == decided_vhash,
                        "a durable accept quorum never decides two values for a slot",
                        { "node" => node, "slot" => slot, "round" => ballot.round }
                    );
                }
            }
        }
    }

    /// The lowest compaction floor across the cluster: everything below it is
    /// truncated *everywhere*, so the per-slot safety tallies can be pruned.
    fn cluster_min_floor(&self) -> u64 {
        self.booted
            .iter()
            .map(|node| self.floor.get(node).map_or(0, |f| f.now))
            .min()
            .unwrap_or(0)
    }

    /// Fold one broadcast leader beat (#95). A leader beating at a ballot that
    /// a **promise-majority** has durably promised strictly past is deposed for
    /// good: an acceptor only acks a beat at or above its promise, so at most a
    /// minority can ever ack this ballot again, and no round it starts can
    /// decide. Zombie-ness is a *bounded-liveness* claim — `CheckQuorum` demotes
    /// a leader that spends a full election-timeout window without an ack
    /// quorum, partition or not — so the streak needs no quiescence gate: it
    /// counts beats (one per tick per leader; the seq dedups the per-peer
    /// fan-out) and must reset well inside [`DEPOSED_BEAT_STREAK`].
    ///
    /// Below n=3 the condition is unreachable honestly: a singleton has no
    /// peers to depose it, and an n=2 majority (both nodes) includes the leader
    /// itself, whose own promise cannot sit above the ballot it is beating.
    fn observe_beat(&mut self, node: u64, ballot: Ballot, seq: u64) {
        let n = self.booted.len();
        if n < 3 {
            return;
        }
        let majority = n / 2 + 1;
        let above = self.promised.values().filter(|p| **p > ballot).count();
        let entry = self.deposed_streaks.entry(node).or_default();
        if entry.round != ballot.round || entry.node != ballot.node.0 {
            *entry = DeposedStreak {
                round: ballot.round,
                node: ballot.node.0,
                seq,
                count: 0,
            };
        } else if entry.seq == seq {
            return;
        }
        entry.seq = seq;
        if above >= majority {
            entry.count += 1;
        } else {
            entry.count = 0;
        }
        let streak = entry.count;
        assert_always!(
            streak < DEPOSED_BEAT_STREAK,
            "a leader deposed by a promise-majority stops beating within an election timeout (CheckQuorum)",
            {
                "node" => node,
                "round" => ballot.round,
                "streak_beats" => streak
            }
        );
    }

    /// Fold one authority actually changing hands, at the **transmit** instant.
    ///
    /// Deliberately not at the core call that decided it: the abdicating batch
    /// may still have `Accept`s queued ahead of this message, and those were
    /// proposed while the node genuinely held the authority. Here the ordering
    /// is exact — every earlier message of the batch has already been reported,
    /// and no successor can install a message it has not yet received.
    ///
    /// Idempotent, because the send seam deliberately duplicates messages: a
    /// re-transmit simply re-applies the same retirement.
    fn observe_authority_release(&mut self, from: u64, ballot: Ballot, next_slot: Slot) {
        let entry = self
            .authorities
            .entry((ballot.round, ballot.node.0))
            .or_default();
        assert_always!(
            entry.holder.is_none_or(|held| held == from),
            "only the node exercising an authority relinquishes it",
            { "node" => from, "holder" => entry.holder.unwrap_or(u64::MAX) }
        );
        // The allocator frontier only ever moves forward: a rewind is how one
        // `(slot, ballot)` ends up carrying two different commands.
        assert_always!(
            next_slot.0 >= entry.frontier,
            "a transferred allocator frontier never rewinds",
            {
                "node" => from,
                "frontier" => next_slot.0,
                "previous" => entry.frontier
            }
        );
        entry.frontier = next_slot.0;
        entry.retired.insert(from);
        entry.holder = None;
    }

    /// Fold one observed exercise of a logical authority: `node` put an
    /// `Accept` at `ballot` on the wire.
    ///
    /// This is where **authority uniqueness** — the `DPaxos` handoff's central
    /// safety rule — is checked, and it is checked against what the
    /// cluster can actually observe (a proposal on the wire), never against a
    /// node's own `role` flag. Two nodes exercising one ballot for overlapping
    /// slots is exactly how two different values get chosen for one slot, and
    /// the sibling check in [`NodeAudit::sent`] ("one ballot proposes at most
    /// one command for a slot") is the consequence this exists to prevent
    /// upstream of.
    fn observe_authority_use(&mut self, node: u64, ballot: Ballot) {
        let key = (ballot.round, ballot.node.0);
        // A node proposing under a ballot that names *someone else* can only
        // have got there through a handoff: a `Prepare` is honored solely when
        // the ballot names its sender, so no Phase 1 at this ballot is even
        // expressible here. Read straight off the wire, with no bookkeeping to
        // race against.
        let inherited = node != ballot.node.0;
        let entry = self.authorities.entry(key).or_default();
        assert_always!(
            !entry.retired.contains(&node),
            "a relinquished authority is never exercised again",
            { "node" => node, "round" => ballot.round, "bnode" => ballot.node.0 }
        );
        let previous = entry.holder;
        entry.holder = Some(node);
        assert_always!(
            previous.is_none_or(|held| held == node),
            "one physical node at a time exercises a logical Paxos authority",
            {
                "node" => node,
                "previous" => previous.unwrap_or(u64::MAX),
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        if inherited {
            // The payoff, observed rather than assumed: this node acquired the
            // ballot from a predecessor and is now streaming Phase 2 under it.
            reach_once!(
                self.handoff_streamed_without_phase1,
                "an inherited authority streams Phase 2 with no Phase 1 of its own"
            );
        }
    }

    /// Fold one applied index into the per-node prefix, the no-gaps frontier and
    /// the cluster high-water mark.
    fn observe_applied_index(&mut self, node: u64, idx: u64, now_ms: u64) {
        self.check_no_gaps(node, idx);
        if idx >= 2 {
            reach_once!(
                self.multi_slot_applied,
                "a multi-slot log prefix is applied"
            );
        }
        if idx >= 3 {
            reach_once!(
                self.several_slots_applied,
                "the chosen prefix advances under a stable leader"
            );
        }
        if self.cluster_applied_max.is_none_or(|m| idx > m) {
            self.cluster_applied_max = Some(idx);
            self.cluster_applied_max_ms = now_ms;
        }
        let prefix = self.applied_max.entry(node).or_insert(0);
        *prefix = (*prefix).max(idx);
        for (&n, &nm) in &self.applied_max {
            if Some(nm) < self.cluster_applied_max {
                self.lagged.insert(n);
            }
        }
        // `caught_up` is judged in `check_final_convergence` against the FINAL
        // cluster maximum: a node that transiently matched a max the cluster
        // immediately moved past is not evidence the catch-up path healed it.
        self.check_below_all_floors(now_ms);
    }

    /// A node's applied (contiguous chosen) prefix advances one slot at a time.
    /// A *replay* of an already-applied slot after a restart is idempotent and
    /// allowed; only a forward skip past the frontier is a real gap, and that is
    /// legal only at the node's compaction floor (a truncated log's boot replay
    /// resumes there) or at a snapshot install.
    fn check_no_gaps(&mut self, node: u64, idx: u64) {
        let at_floor = idx == self.floor.get(&node).map_or(0, |f| f.now);
        let at_snapshot = self
            .snap_landings
            .get(&node)
            .is_some_and(|landings| landings.contains(&idx));
        let next_now = self.frontier.get(&node).copied().unwrap_or(0);
        // Stage 8: a boot replay may step over a rotted record whose effect is
        // already durable in the application state — legal only when every
        // skipped slot was reported faulty by this node (the explained jump).
        let over_reported = idx > next_now
            && self
                .reported_faulty
                .get(&node)
                .is_some_and(|slots| (next_now..idx).all(|s| slots.contains(&s)));
        let next = self.frontier.entry(node).or_insert(0);
        if idx == *next {
            *next += 1;
        } else if idx > *next {
            *next = idx + 1;
            assert_always!(
                at_floor || at_snapshot || over_reported,
                "a node's applied prefix advances one slot at a time (a forward jump only at the compaction floor or a snapshot install)",
                { "node" => node, "index" => idx }
            );
        }
    }

    /// The hard below-floor case: a node whose next needed slot has been
    /// truncated on *every* peer, so commit-replay catch-up can no longer serve
    /// it and only snapshot transfer can. A reachability gate, not an escape
    /// hatch — convergence is still demanded of it.
    fn check_below_all_floors(&mut self, now_ms: u64) {
        if self.below_all_floors {
            return;
        }
        // Gated on quiescence like the trace-scanning oracle was: firing on a
        // transient mid-chaos lag would satisfy the gate without the settled
        // below-floor state it exists to witness.
        if !self.quiesced(now_ms) {
            return;
        }
        let Some(cluster_max) = self.cluster_applied_max else {
            return;
        };
        // Candidates include booted nodes with an *empty* applied prefix
        // (`next_needed = 0`) — the node most likely to sit below every floor.
        let cluster: BTreeSet<u64> = self
            .booted
            .iter()
            .copied()
            .chain(self.applied_max.keys().copied())
            .collect();
        for &node in &cluster {
            let prefix = self.applied_max.get(&node).copied();
            if prefix == Some(cluster_max) {
                continue;
            }
            let next_needed = prefix.map_or(0, |m| m + 1);
            let below = cluster.iter().any(|&p| p != node)
                && cluster
                    .iter()
                    .filter(|&&p| p != node)
                    .all(|p| next_needed < self.floor.get(p).map_or(0, |f| f.now));
            if below {
                reach_once!(
                    self.below_all_floors,
                    "a node fell below every peer's compaction floor (recovers via snapshot transfer)"
                );
                return;
            }
        }
    }

    /// The client-visible checks over the merged history (see [`LinHistory`]).
    fn check_client_history(&self) {
        let h = &self.lin;
        // A terminal event is only ever recorded for an op that was issued.
        assert_always!(
            h.acked + h.failed <= h.issued,
            "no proposal is acked/failed before it is issued"
        );
        assert_always!(
            h.read_acked + h.read_failed <= h.read_issued,
            "no read is acked/failed before it is issued"
        );
        // With no chaos a proposal does come back — a "sometimes" + "reachable".
        assert_sometimes!(h.acked > 0, "at least one proposal is acknowledged");
        if h.acked > 0 {
            assert_reachable!("a client proposal is acknowledged");
        }
        check_disclosed_order(h);
        // The sequential fast path, per client: program order is real-time order
        // within a non-pipelined client even where timestamps tie, so C1-C3 stay
        // strictly stronger than L1-L4 for those clients.
        let committed_clients: BTreeSet<u64> = h
            .write_slot
            .keys()
            .chain(h.read_wm.keys())
            .map(|&(c, _)| c)
            .collect();
        for &client in &committed_clients {
            if !h.pipelined_clients.contains(&client) {
                check_sequential_client(client, h);
            }
        }
        h.check_mode_gates();
        h.check_coverage_gates(&committed_clients, self.leader_change_ms);
    }
}

/// One node's view of the shared checker. Constructed beside the node's
/// `BuggifyHooks` and handed to `paros::run_node`; it stamps simulated time on
/// the observations that need it and forwards everything else unchanged.
pub(crate) struct NodeAudit<T> {
    time: T,
    world: Arc<AuditWorld>,
}

/// The driver hands each peer-delivery task its own handle to the audit (the
/// bounded-mailbox drops happen inside those tasks); every clone shares the
/// one per-iteration [`AuditWorld`].
impl<T: Clone> Clone for NodeAudit<T> {
    fn clone(&self) -> Self {
        Self {
            time: self.time.clone(),
            world: self.world.clone(),
        }
    }
}

impl<T: TimeProvider> NodeAudit<T> {
    pub(crate) fn new(time: T, world: Arc<AuditWorld>) -> Self {
        Self { time, world }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.time.now().as_millis()).unwrap_or(u64::MAX)
    }

    fn state(&self) -> MutexGuard<'_, AuditState> {
        self.world.lock()
    }
}

impl<T: TimeProvider> Audit for NodeAudit<T> {
    fn promised(&self, node: NodeId, ballot: Ballot) {
        self.state().observe_promise(node.0, ballot);
    }

    fn accepted(&self, node: NodeId, slot: Slot, ballot: Ballot, promised: Ballot, vhash: u64) {
        let now = self.now_ms();
        let mut st = self.state();
        // A node never persists an accept above the ballot it has promised.
        assert_always!(
            ballot <= promised,
            "a node's accepted ballot never exceeds its promised ballot"
        );
        // The independent half of the same claim: `promised` above is the
        // driver's own word — cross-check the accept against the promise the
        // audit *folded* from durable reports. Sound because a promise raise
        // in the same batch is surfaced before its accepts, and every earlier
        // raise (or the boot report) already fed the fold.
        let folded = st.promised.get(&node.0).copied();
        assert_always!(
            folded.is_some_and(|p| ballot <= p),
            "a node's accepted ballot never exceeds its last reported promise",
            {
                "node" => node.0,
                "slot" => slot.0,
                "round" => ballot.round,
                "folded_round" => folded.map_or(0, |p| p.round)
            }
        );
        // The durable mirror of the on-the-wire per-ballot proposal check
        // lives in the fold (two different commands under one `(slot,
        // ballot)` would be a ratified double-allocation), together with the
        // acceptor tally behind the quorum-decided oracle.
        st.observe_durable_accept(node.0, slot.0, ballot, vhash);
        // The truncated prefix is genuinely gone: nothing below the durable
        // floor is ever written again.
        let floor = st.floor.get(&node.0).copied().unwrap_or_default();
        assert_always!(
            slot.0 >= floor.strictly_before(now),
            "a node never persists an accept below its compaction floor"
        );
        st.persisted.insert((node.0, slot.0), vhash);
    }

    fn truncated(&self, node: NodeId, first: Slot) {
        let now = self.now_ms();
        let mut st = self.state();
        // The core stages `WriteOp::Truncate` only when it raises its floor,
        // batches flush in order, and a report only ever follows a successful
        // fsync — so the *truncated reports themselves* are monotone per
        // node, within and across incarnations. Judged against their own
        // watermark, never the folded floor: a same-batch snapshot install
        // jumps the folded floor higher while the driver's split flushes the
        // (now stale-lower, no-op-on-disk) truncate after it, and the
        // ground-truth feed likewise forwards raw requests the storage
        // contract treats as no-ops. Equality is an idempotent re-raise.
        let was = st.truncate_watermark.get(&node.0).copied().unwrap_or(0);
        assert_always!(
            first.0 >= was,
            "a compaction floor never regresses",
            { "node" => node.0, "was" => was, "reported" => first.0 }
        );
        st.truncate_watermark.insert(node.0, first.0.max(was));
        st.floor.entry(node.0).or_default().raise(first.0, now);
        if first.0 > 0 {
            reach_once!(
                st.compacted,
                "a node truncates its log prefix behind the chosen index"
            );
            // The #101 coupling, checked at the cluster level: a `Truncate`
            // is only ever proposed once a quorum has durably recorded the
            // covering decided snapshot point, so by the time ANY node
            // applies it, that point is already in the shared audit's custody
            // map. Deliberately not per-node: a node with an open application
            // repair advances consensus (and its floor) while its own marker
            // emission is deferred to the repair pump, and an
            // install-recovered node never applies the folded marker at all —
            // both legitimately truncate on points *others* recorded. A
            // truncation no node's recorded point covers evaded the coupling
            // policy.
            let covered = st
                .snap_points
                .values()
                .flatten()
                .any(|point| point + 1 >= first.0);
            assert_always!(
                covered,
                "storage: a truncation is covered by a recorded snapshot point",
                { "node" => node.0, "first" => first.0 }
            );
            // Once a truncation at `first` is validated, a point too low to
            // cover it can never cover any later (higher) truncation either —
            // coverage is `point + 1 >= first` and floors only rise — so drop
            // it. Only after a *passed* check: pruning on a red run could
            // cascade the one root cause into noise. The surviving covering
            // point keeps every lagging node's smaller `first` covered too.
            if covered {
                for points in st.snap_points.values_mut() {
                    *points = points.split_off(&(first.0.saturating_sub(1)));
                }
            }
        }
        // Below the *cluster-wide* minimum floor every node has truncated, so
        // the per-slot safety tallies can never be consulted again: reclaim
        // them (an O(log n) split, on the rare truncation path).
        let min_floor = st.cluster_min_floor();
        if min_floor > 0 {
            st.decided = st.decided.split_off(&min_floor);
            st.accept_sets = st.accept_sets.split_off(&(min_floor, 0, 0));
        }
        st.check_below_all_floors(now);
    }

    fn snapshot_installed(&self, node: NodeId, chosen_index: Slot, ballot: Ballot) {
        let now = self.now_ms();
        let mut st = self.state();
        reach_once!(
            st.snapshot_installed,
            "a snapshot was installed to recover a below-floor node"
        );
        // The core adopts `max(promise, ballot)` on install and any raise is
        // surfaced (as the batch's `SetPromise`) before this write's report,
        // so by now the folded promise must already cover the snapshot's
        // ballot — a lower fold would mean the adoption was lost.
        let folded = st.promised.get(&node.0).copied();
        assert_always!(
            folded.is_some_and(|p| p >= ballot),
            "an installed snapshot's ballot is covered by the node's promise",
            {
                "node" => node.0,
                "round" => ballot.round,
                "folded_round" => folded.map_or(0, |p| p.round)
            }
        );
        // An offer is only ever materialized from state the serving peer had
        // durably applied (the driver skips a mismatched offer), and that
        // apply was folded before the offer left — so a landing past the
        // cluster's applied frontier is a fabricated prefix.
        assert_always!(
            st.cluster_applied_max
                .is_some_and(|max| chosen_index.0 <= max),
            "an installed snapshot lands within the cluster's applied frontier",
            {
                "node" => node.0,
                "landing" => chosen_index.0,
                "cluster_max" => st.cluster_applied_max.unwrap_or(0)
            }
        );
        // (Deliberately NOT asserted: `landing >= this node's own applied
        // fold`. The applied fold is reported before the application fsync,
        // so a crash at the after-apply seam legally leaves the fold above
        // the durable state a rebooted node then heals from — a lower
        // landing from a lagging-but-sufficient peer is legitimate there.)
        //
        // The install also jumps the durable chosen index to the landing;
        // keep the per-incarnation watermark in step so a later
        // `SetChosenIndex` report is judged against it.
        let watermark = st.chosen_watermark.entry(node.0).or_insert(0);
        *watermark = (*watermark).max(chosen_index.0);
        // A node can install more than one snapshot in a single drain (two peers
        // each serve it), so the admitted landings are a set.
        st.snap_landings
            .entry(node.0)
            .or_default()
            .insert(chosen_index.0);
        // The install jumps the applied prefix straight to the snapshot's
        // boundary without replaying entries.
        st.observe_applied_index(node.0, chosen_index.0, now);
    }

    fn snapshot_mid_election(&self, _node: NodeId) {
        let mut st = self.state();
        reach_once!(
            st.snapshot_mid_election,
            "a snapshot lands during a live election"
        );
    }

    fn applied(&self, node: NodeId, slot: Slot, vhash: u64, identity: Option<(u64, u64)>) {
        let now = self.now_ms();
        let mut st = self.state();
        // The crown jewel: at most one value is ever chosen per slot, cluster-wide.
        if let Some(prev) = st.chosen.insert(slot.0, vhash) {
            assert_always!(prev == vhash, "at most one value is ever chosen for a slot");
        }
        // The at-most-once half: one (client, seq) applies at exactly one
        // index, cluster-wide. Keyed on identity, not payload bytes (distinct
        // requests legitimately share bytes); a boot replay of the same slot
        // is idempotent and passes.
        if let Some(id) = identity {
            let first = *st.applied_identity.entry(id).or_insert(slot.0);
            assert_always!(
                first == slot.0,
                "a (client, seq) command is applied at exactly one log index"
            );
        }
        // The quorum-decided oracle's apply leg: a user command applied where
        // the durable-accept tally already decided the slot must apply the
        // decided value. Control applies are exempt on purpose — the #94
        // suppression legitimately executes a re-chosen identity as a `Noop`
        // (identity `None`) while the quorum durably accepted the user
        // command, and control-slot agreement is already covered by the
        // per-slot `chosen` check above.
        if identity.is_some()
            && let Some(&(_, _, decided_vhash)) = st.decided.get(&slot.0)
        {
            assert_always!(
                vhash == decided_vhash,
                "an applied value matches the decided value",
                { "node" => node.0, "slot" => slot.0 }
            );
        }
        reach_once!(st.any_chosen, "a value is chosen");
        st.observe_applied_index(node.0, slot.0, now);
    }

    fn sent(&self, node: NodeId, _to: NodeId, msg: &Message) {
        if msg.config_id().is_some() {
            let mut st = self.state();
            reach_once!(
                st.config_tagged_protocol_message,
                "a protocol message carries a configuration identity"
            );
        }
        if let Message::Relinquish {
            from,
            ballot,
            next_slot,
            ..
        } = msg
        {
            self.state()
                .observe_authority_release(from.0, *ballot, *next_slot);
            return;
        }
        // #95: every broadcast leader beat feeds the zombie-leader streak.
        if let Message::Heartbeat { ballot, seq, .. } = msg {
            self.state().observe_beat(node.0, *ballot, *seq);
            return;
        }
        if let Message::Promise {
            ballot, accepted, ..
        } = msg
        {
            assert_always!(
                accepted.len() <= PROMISE_BATCH,
                "a Promise carries at most one bounded suffix chunk",
                { "entries" => accepted.len() }
            );
            // Persist-before-send at the promise seam: the batch that raised
            // the promise flushed and reported it before this send, so a
            // Promise above the folded durable promise left before its fsync.
            let st = self.state();
            let folded = st.promised.get(&node.0).copied();
            assert_always!(
                folded.is_some_and(|p| p >= *ballot),
                "a sent Promise carries a durably promised ballot",
                {
                    "node" => node.0,
                    "round" => ballot.round,
                    "folded_round" => folded.map_or(0, |p| p.round)
                }
            );
        }
        // Persist-before-send at the accept seam: an `Accepted` claims "I hold
        // this durably", so the matching record must already be in this
        // node's folded durable-accept tally (the same-batch write is flushed
        // and reported before the send; a re-answer names an older record
        // that was folded when it was first written or re-read at boot).
        if let Message::Accepted { ballot, slot, .. } = msg {
            let st = self.state();
            let holds = st
                .accept_sets
                .get(&(slot.0, ballot.round, ballot.node.0))
                .is_some_and(|holders| holders.contains(&node.0));
            assert_always!(
                holds,
                "an outgoing Accepted names a durably accepted record",
                { "node" => node.0, "slot" => slot.0, "round" => ballot.round }
            );
        }
        // A `Commit` names a decided slot; where the durable-accept tally
        // already knows the decision, the commit must carry that value.
        // Checked against `decided`, never the apply-fed `chosen` map: a #94
        // re-chosen identity applies as a `Noop` everywhere while its commit
        // honestly carries the decided user command.
        if let Message::Commit { slot, command, .. } = msg {
            let vhash = command_hash(command);
            let st = self.state();
            if let Some(&(_, _, decided_vhash)) = st.decided.get(&slot.0) {
                assert_always!(
                    vhash == decided_vhash,
                    "a commit carries the chosen value",
                    { "node" => node.0, "slot" => slot.0 }
                );
            }
        }
        // The Phase-2 half of P2b, checked *on the wire*: a ballot names its
        // own proposer, so exactly one node ever sends `Accept`s at it, and
        // two different commands under one `(ballot, slot)` mean the
        // proposer allocated a slot it already had in flight. Reading the
        // send rather than the receive is deliberate — it indicts the
        // proposer, not the network — and it is the only place the anomaly
        // is visible: an accept quorum may reject it, leaving no durable
        // trace at all.
        if let Message::Accept {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            let vhash = command_hash(command);
            let mut st = self.state();
            // Authority uniqueness first: *who* may propose under this ballot,
            // checked before *what* they proposed. A violation of the first
            // explains a violation of the second, so ordering them this way
            // makes the root cause the one that fires.
            st.observe_authority_use(node.0, *ballot);
            reach_once!(
                st.any_proposal_checked,
                "a proposed command is checked against its ballot's other proposals"
            );
            if let Some(prev) = st
                .proposed
                .insert((ballot.round, ballot.node.0, slot.0), vhash)
            {
                assert_always!(
                    prev == vhash,
                    "one ballot proposes at most one command for a slot"
                );
            }
        }
    }

    fn elected(&self, node: NodeId, won: Ballot, promised: Ballot, _gap_fills: u64) {
        let now = self.now_ms();
        let mut st = self.state();
        if let Some(prev) = st.leader_round.insert(node.0, won.round) {
            assert_always!(
                won.round > prev,
                "a node's leadership ballots strictly increase"
            );
        }
        // Placed at the *instant of victory*: winning means having promised your
        // own campaign ballot and heard nothing higher, so this is an identity
        // there. A tick later the same state is indistinguishable from a sitting
        // leader legitimately learning a higher-ballot commit.
        assert_always!(
            won >= promised,
            "a fresh leader has not promised a ballot above the one it won"
        );
        reach_once!(
            st.leader_promise_checked,
            "a fresh leader's promise is checked against the ballot it won"
        );
        reach_once!(st.any_leader, "a leader is elected");
        st.leader_rounds.insert(won.round);
        if st.leader_rounds.len() >= 2 {
            reach_once!(
                st.leadership_turnover,
                "leadership turns over (re-election)"
            );
        }
        st.last_leader_ms = now;
        match st.first_leader_round {
            None => st.first_leader_round = Some(won.round),
            Some(r) if r != won.round && st.leader_change_ms.is_none() => {
                st.leader_change_ms = Some(now);
            }
            Some(_) => {}
        }
    }

    fn stepped_down(&self, _node: NodeId) {
        let mut st = self.state();
        reach_once!(st.resigned, "the driver voluntarily resigns leadership");
    }

    fn authority_relinquished(&self, node: NodeId, handoff: Handoff) {
        let now = self.now_ms();
        let mut st = self.state();
        // Shape and coverage only. The *bookkeeping* — who holds the authority
        // now — is folded from the `Relinquish` on the wire (see
        // [`NodeAudit::sent`]), because that is the instant with the right
        // causal order: it lands after every message the abdicating batch had
        // already queued, and before any successor can possibly install.
        assert_always!(
            u64::try_from(handoff.decided + handoff.pending).unwrap_or(u64::MAX)
                == handoff.next_slot.0.saturating_sub(handoff.from_slot.0),
            "a relinquished tail exactly tiles the transferred range",
            {
                "node" => node.0,
                "decided" => handoff.decided,
                "pending" => handoff.pending
            }
        );
        assert_always!(
            handoff.decided + handoff.pending <= HANDOFF_BATCH,
            "a relinquished tail stays within one bounded page",
            { "node" => node.0, "slots" => handoff.decided + handoff.pending }
        );
        assert_always!(
            handoff.to != node,
            "an authority is handed to another node, never to its own holder",
            { "node" => node.0 }
        );
        let key = (handoff.ballot.round, handoff.ballot.node.0);
        // The `DPaxos` "at most once" rule, checked on the decision itself: the
        // core demotes in the very call that decides, so it can never decide to
        // relinquish one authority twice.
        assert_always!(
            st.relinquish_calls.insert((node.0, key)),
            "an authority is relinquished at most once by a node",
            { "node" => node.0, "round" => handoff.ballot.round }
        );
        // One hop only: the node that mints a ballot by winning Phase 1 at it is
        // the only one that may hand it on (see `RawNode::can_relinquish`).
        // Without that rule a replayed payload can re-install an authority at a
        // node that already gave it up while its own successor is still
        // exercising it — the hole this sweep found.
        assert_always!(
            handoff.ballot.node == node,
            "only a ballot's own minter relinquishes it",
            { "node" => node.0, "bnode" => handoff.ballot.node.0 }
        );
        reach_once!(
            st.handoff_relinquished,
            "a leader cooperatively relinquishes its authority"
        );
        if handoff.pending > 0 {
            reach_once!(
                st.handoff_carried_tail,
                "a handoff carries accepted-but-unchosen work across"
            );
        }
        // Leadership changed hands: the quiescence gate must not read the
        // resulting transient as a settled cluster.
        st.last_leader_ms = now;
    }

    fn authority_installed(
        &self,
        node: NodeId,
        from: NodeId,
        ballot: Ballot,
        next_slot: Slot,
        _tail: u64,
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        assert_always!(
            from != node,
            "an installed authority came from another node",
            { "node" => node.0 }
        );
        let key = (ballot.round, ballot.node.0);
        let entry = st.authorities.entry(key).or_default();
        assert_always!(
            !entry.retired.contains(&node.0),
            "a node never re-installs an authority it relinquished",
            { "node" => node.0, "round" => ballot.round }
        );
        assert_always!(
            entry.holder.is_none_or(|held| held == node.0),
            "at most one node installs a relinquished authority",
            { "node" => node.0, "holder" => entry.holder.unwrap_or(u64::MAX) }
        );
        assert_always!(
            next_slot.0 >= entry.frontier,
            "an inherited allocator frontier never rewinds",
            {
                "node" => node.0,
                "frontier" => next_slot.0,
                "previous" => entry.frontier
            }
        );
        entry.frontier = next_slot.0;
        entry.holder = Some(node.0);
        st.handoff_installs = st.handoff_installs.saturating_add(1);
        reach_once!(
            st.handoff_installed,
            "a node installs a predecessor's transferred authority"
        );
        if st.handoff_installs >= 2 {
            reach_once!(
                st.handoff_repeated,
                "leadership is handed over more than once in a run"
            );
        }
        reach_once!(st.any_leader, "a leader is elected");
        st.last_leader_ms = now;
    }

    fn handoff_refused(&self, _node: NodeId, target: u64, stale: u64, shape: u64) {
        let mut st = self.state();
        let (last_target, last_stale, last_shape) = st.handoff_refused;
        st.handoff_refused = (
            target.max(last_target),
            stale.max(last_stale),
            shape.max(last_shape),
        );
        if target > 0 {
            reach_once!(
                st.handoff_refused_target,
                "a handoff addressed elsewhere is refused"
            );
        }
        if stale > 0 {
            reach_once!(
                st.handoff_refused_stale,
                "a stale or superseded handoff is refused"
            );
        }
        if shape > 0 {
            reach_once!(
                st.handoff_refused_shape,
                "a malformed handoff tail is refused"
            );
        }
    }

    fn handoff_fence_expired(&self, _node: NodeId, _count: u64) {
        let now = self.now_ms();
        let mut st = self.state();
        st.last_leader_ms = now;
        reach_once!(
            st.handoff_fence_expired,
            "an uncovered inherited fence resigns back to an ordinary election"
        );
    }

    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {
        let now = self.now_ms();
        let mut st = self.state();
        if !st.liveness {
            return;
        }
        // A gap is perfectly ordinary — pipelining leaves several slots
        // undecided, and a follower that missed one `Commit` holds one until
        // catch-up runs. The wedge claim therefore requires all of:
        //  - quiescence (chaos over, prefix and leadership stable),
        //  - the hole sitting *above* the cluster's applied maximum (below it
        //    the slot exists on some peer and catch-up can serve it),
        //  - and the same hole persisting for GAP_WEDGE_TICKS consecutive
        //    reports of the same node — the drift-immune part: the driver
        //    emits this once per node tick, so the streak counts genuine
        //    election-timeout/re-send opportunities even when moonpool's
        //    buggified sleep delay stretches ticks during the chaos window.
        // `None` is the empty applied prefix (conceptually slot -1), so every
        // real hole sits above it and must remain observable.
        // The WAITED excuse (Stage 8, budget-off ground truth): a hole at a
        // slot with no readable copy anywhere is *correct* unavailability —
        // the CTRL guarantee explicitly demands the cluster hold position
        // there. Excused by the world's ground truth only, never by the
        // node's own claim; the wedge stays armed everywhere else, so an
        // unhealed hole WITH a surviving clean copy is still a red run.
        if st.unrecoverable.contains(&hole.0) {
            st.waited = true;
            return;
        }
        let wedged = st.quiesced(now)
            && st
                .cluster_applied_max
                .is_none_or(|cluster_max| hole.0 > cluster_max);
        let streak = {
            let entry = st.gap_streaks.entry(node.0).or_insert((hole.0, 0));
            if entry.0 == hole.0 && wedged {
                entry.1 += 1;
            } else {
                *entry = (hole.0, u64::from(wedged));
            }
            entry.1
        };
        assert_always!(
            streak < GAP_WEDGE_TICKS,
            "a quiesced cluster holds no chosen slot above its applied prefix (an election left an undecided hole)",
            {
                "node" => node.0,
                "hole" => hole.0,
                "above" => above.0,
                "cluster_max" => st.cluster_applied_max.unwrap_or(0),
                "streak_ticks" => streak,
                "now_ms" => now
            }
        );
    }

    fn client_acked(
        &self,
        node: NodeId,
        client: u64,
        seq: u64,
        slot: Slot,
        applied: Option<Slot>,
        dedup: bool,
    ) {
        let mut st = self.state();
        reach_once!(
            st.any_ack_checked,
            "a committed write ack is checked against the acking node's applied prefix"
        );
        // A committed ack is a claim about a specific applied command: on both
        // ack paths (ack-on-commit and the dedup fast path) the apply of this
        // `(client, seq)` was folded before the ack fired — on this node, or,
        // for a session fact adopted from a snapshot, on the peer that served
        // it. The ack must name exactly the index the identity applied at; an
        // ack for a never-applied identity fails the same check.
        let applied_at = st.applied_identity.get(&(client, seq)).copied();
        assert_always!(
            applied_at == Some(slot.0),
            "a committed ack names the slot its command applied at",
            {
                "node" => node.0,
                "client" => client,
                "seq" => seq,
                "acked_slot" => slot.0,
                "applied_at" => applied_at.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX))
            }
        );
        // The dedup-window edge the reply-drop location exists for: a reply
        // was dropped after commit, and a retry then took the dedup path.
        if dedup && st.propose_reply_dropped {
            reach_once!(
                st.dedup_after_dropped_reply,
                "a committed proposal ack is lost and the retry takes the dedup path"
            );
        }
        // `committed = true` is the promise that the write is in the register
        // this project defines — the *applied* log prefix — so an ack that
        // outruns the acking node's own apply is a client-visible
        // linearizability violation on its own.
        assert_always!(
            applied.is_some_and(|a| a >= slot),
            "a committed write ack names a slot the acking node had already applied"
        );
    }

    fn chosen_index(&self, node: NodeId, index: Slot) {
        let mut st = self.state();
        // Within one incarnation the core's chosen index only ever advances
        // (the ordering-chain invariant), and its durable reports arrive in
        // batch order — so a regression here is a driver/storage reordering
        // bug. Across a restart the scalar is flushed *relaxed*, so a crash
        // may legally rewind it (the boot recomputes it from what the disk
        // actually holds); `recovered` therefore resets this watermark to the
        // recovered index instead of asserting continuity across boots.
        let watermark = st.chosen_watermark.entry(node.0).or_insert(0);
        assert_always!(
            index.0 >= *watermark,
            "a chosen index never regresses within a boot",
            { "node" => node.0, "index" => index.0, "watermark" => *watermark }
        );
        *watermark = (*watermark).max(index.0);
    }

    fn read_confirmed(&self, node: NodeId, index: Option<Slot>) {
        let mut st = self.state();
        // The confirmed index is the serve-time chosen index, which is
        // monotone within an incarnation — so confirmed reads on one node
        // never step backwards between boots' resets. (Deliberately NOT
        // asserted against the audit's applied fold: that fold is reported
        // before the application fsync, so after an after-apply-seam crash it
        // can legitimately sit above what this incarnation has applied.)
        let confirmed = index.map(|s| s.0);
        let watermark = st.read_watermark.entry(node.0).or_insert(None);
        assert_always!(
            confirmed >= *watermark,
            "a confirmed read index never regresses within a boot",
            {
                "node" => node.0,
                "confirmed" => confirmed.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX)),
                "watermark" => watermark.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX))
            }
        );
        *watermark = (*watermark).max(confirmed);
    }

    fn recovered(
        &self,
        node: NodeId,
        promised: Ballot,
        chosen_index: Option<Slot>,
        cluster_size: u64,
        accepted: &[(Slot, Ballot, u64)],
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        st.booted.insert(node.0);
        st.cluster_size = Some(
            st.cluster_size
                .map_or(cluster_size, |n| n.max(cluster_size)),
        );
        st.observe_promise(node.0, promised);
        // The boot report is the incarnation edge: swap in the faulty
        // classifications staged by *this* boot's scan (driver report and
        // red-demo side door alike) and drop the previous incarnation's — a
        // stale excuse must not keep explaining divergence forever.
        let fresh = st.faulty_staged.remove(&node.0).unwrap_or_default();
        st.reported_faulty.insert(node.0, fresh);
        // Fresh incarnation: the durable chosen index legally rewinds across
        // a crash (its writes flush relaxed), so restart the within-boot
        // watermarks from what this boot actually recovered.
        st.chosen_watermark
            .insert(node.0, chosen_index.map_or(0, |s| s.0));
        st.read_watermark.remove(&node.0);
        let boot_floor = st
            .floor
            .get(&node.0)
            .copied()
            .unwrap_or_default()
            .strictly_before(now);
        for &(slot, ballot, vhash) in accepted {
            // A synced accept is never lost or altered by a crash.
            if let Some(&prev) = st.persisted.get(&(node.0, slot.0)) {
                assert_always!(
                    prev == vhash,
                    "a restart never changes a pre-crash accepted value for a slot"
                );
            }
            assert_always!(
                slot.0 >= boot_floor,
                "a truncated record is never recovered on boot (the log stays bounded)"
            );
            // Re-fold the durable record into the acceptor tally: a record
            // that became durable through an ambiguous fault leg (flushed,
            // but the driver crashed before reporting it) enters the
            // quorum-decided oracle here; a re-fold of an already-counted
            // record is idempotent.
            st.observe_durable_accept(node.0, slot.0, ballot, vhash);
        }
        // A chosen index is only ever set once the commits below it were
        // learned, every one of which needed a durable accept quorum — one
        // the tally has folded from live reports, or one this very boot
        // report just re-supplied (an ambiguous fsync can land a decided
        // batch durably with the driver crashing before surfacing it, which
        // is why this check runs *after* the record fold above). The one
        // evidence a fold cannot recover is a torn record whose value rotted:
        // its `(slot, ballot)` identity survives as this boot's faulty
        // report, so those slots extend the admissible frontier. Anything
        // past all three is a fabricated prefix.
        if let Some(ci) = chosen_index {
            let faulty_max = st
                .reported_faulty
                .get(&node.0)
                .and_then(|slots| slots.iter().next_back().copied());
            let frontier = st.decided_max.max(faulty_max);
            assert_always!(
                frontier.is_some_and(|max| ci.0 <= max),
                "a recovered chosen index stays within the cluster's decided frontier",
                {
                    "node" => node.0,
                    "recovered" => ci.0,
                    "frontier" => frontier.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX))
                }
            );
        }
        // The #71 explained-divergence form, first leg (Stage 7): a recovered
        // log missing a record this node durably persisted is legal iff a
        // detected-corruption crash explains it. The one honest reaction that
        // drops records without a crash — the truncate-on-mismatch bug class
        // (CTRL Figure 2) — is exactly what this catches: a node that
        // silently truncated on a mismatch reports a recovered log with an
        // unexplained hole. The current floor (not the boot-instant one) is
        // deliberate: a same-instant truncate+reboot only ever *excludes*
        // legally-dropped records, and the divergence this leg hunts never
        // raises the floor. Never weaken for unexplained divergence.
        let reported: BTreeSet<u64> = accepted.iter().map(|&(slot, _, _)| slot.0).collect();
        let floor_now = st.floor.get(&node.0).map_or(0, |f| f.now);
        let missing: Vec<u64> = st
            .persisted
            .range((node.0, 0)..=(node.0, u64::MAX))
            .map(|(&(_, slot), _)| slot)
            .filter(|slot| *slot >= floor_now && !reported.contains(slot))
            .collect();
        for slot in missing {
            let explained = st.corruption_crashed_records.contains(&(node.0, slot))
                || st.corruption_crashed_nodes.contains(&node.0)
                // Stage 8's second explanation: the record was classified
                // recoverable and reported into the tri-state this boot —
                // the peer-recovery path owns it now (#71: explained
                // divergence only, never a blanket weakening).
                || st
                    .reported_faulty
                    .get(&node.0)
                    .is_some_and(|slots| slots.contains(&slot));
            assert_always!(
                explained,
                "storage: a recovered log omits a persisted record only after a detected corruption crash",
                { "node" => node.0, "slot" => slot }
            );
        }
    }

    fn storage_fault(&self, node: NodeId, error: &StorageError, decision: StorageFaultDecision) {
        let mut st = self.state();
        // Stages 6/7 have exactly one honest reaction; a different decision
        // here is a driver bug until Stage 8's protocol-aware choices exist.
        assert_always!(
            decision == StorageFaultDecision::Crash,
            "a storage fault is decided as a fail-stop crash"
        );
        match error {
            StorageError::Io { .. } | StorageError::FsyncFailed { .. } => {
                st.storage_faults_detected += 1;
                reach_once!(
                    st.storage_fault_crashed,
                    "a storage fault crashes the node (fail-stop)"
                );
            }
            // Stage 7: a classified detection — detect ⇒ crash, and the crash
            // is the explanation the divergence/convergence excuses key on.
            StorageError::Corruption { record, .. } => {
                st.corruption_crashes += 1;
                if let StorageRecord::Accepted(slot) = record {
                    st.corruption_crashed_records.insert((node.0, slot.0));
                }
                st.corruption_crashed_nodes.insert(node.0);
                reach_once!(
                    st.corruption_crashed,
                    "storage: a detected corruption crashes the node"
                );
            }
            StorageError::Metadata { .. } => {
                st.corruption_crashes += 1;
                st.corruption_crashed_nodes.insert(node.0);
                reach_once!(
                    st.corruption_crashed,
                    "storage: a detected corruption crashes the node"
                );
            }
        }
    }

    fn crashed(&self, _node: NodeId, seam: Seam) {
        let mut st = self.state();
        match seam {
            Seam::BeforeSync => st.crashed_before_sync = true,
            Seam::AfterSyncBeforeSend => {
                reach_once!(
                    st.crashed_after_sync,
                    "the driver crashes after sync and before sending a batch"
                );
            }
            Seam::AfterApplyBeforeSync => {
                reach_once!(
                    st.crashed_after_apply,
                    "the driver crashes after applying a batch and before its application fsync"
                );
            }
            Seam::BeforeChunkSync => {
                reach_once!(
                    st.crashed_before_chunk_sync,
                    "the driver crashes before syncing repaired snapshot chunks"
                );
            }
            Seam::AfterChunkRestoreBeforeSync => {
                reach_once!(
                    st.crashed_after_chunk_restore,
                    "the driver crashes after a snap-point restore and before its sync"
                );
            }
        }
    }

    fn dropped_at_send(&self, _node: NodeId, _to: NodeId, msg: &Message) {
        let mut st = self.state();
        match msg {
            Message::Accept { .. } => {
                reach_once!(
                    st.dropped_accept,
                    "the driver drops one isolated accept at the send seam"
                );
            }
            Message::Prepare { .. } | Message::Promise { .. } | Message::Nack { .. } => {
                reach_once!(
                    st.dropped_election,
                    "the driver drops an election message at the send seam"
                );
            }
            Message::Commit { .. } => {
                reach_once!(
                    st.dropped_commit,
                    "the driver drops a commit at the send seam"
                );
            }
            Message::Accepted { .. } => {
                reach_once!(
                    st.dropped_accepted,
                    "the driver drops an accepted ack at the send seam"
                );
            }
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => {
                reach_once!(
                    st.dropped_heartbeat,
                    "the driver drops a heartbeat at the send seam"
                );
            }
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                reach_once!(
                    st.dropped_repair,
                    "the driver drops a repair message at the send seam"
                );
            }
            Message::CatchUpRequest { .. } => {
                reach_once!(
                    st.dropped_catchup_request,
                    "the driver drops a catch-up request at the send seam"
                );
            }
            Message::SnapAck { .. } => {
                reach_once!(
                    st.dropped_snap_ack,
                    "the driver drops a snap custody ack at the send seam"
                );
            }
            Message::SnapChunkRequest { .. } => {
                reach_once!(
                    st.dropped_snap_chunk_request,
                    "the driver drops a snap chunk request at the send seam"
                );
            }
            Message::SnapChunkResponse { .. } => {
                reach_once!(
                    st.dropped_snap_chunk_response,
                    "the driver drops a snap chunk response at the send seam"
                );
            }
            // The whole cooperative handoff, lost in one message: the outgoing
            // leader has already stepped down and the successor never starts,
            // so this must cost availability only — an ordinary Phase 1 is the
            // documented fallback, and the liveness checks are what prove it.
            Message::Relinquish { .. } => {
                reach_once!(
                    st.dropped_relinquish,
                    "the driver drops a relinquishment at the send seam"
                );
            }
            // Inert today: `CheckLeader` never crosses the transport (it is a
            // tick-injected self-event), so this reach gate creates no slot
            // until a future remote probe makes the drop arm live.
            Message::CheckLeader { .. } => {
                reach_once!(
                    st.dropped_check_leader,
                    "the driver drops a check-leader at the send seam"
                );
            }
            _ => {}
        }
    }

    fn duplicated_at_send(&self, _node: NodeId, _to: NodeId, msg: &Message) {
        let mut st = self.state();
        reach_once!(
            st.duplicated_any,
            "the driver duplicates a message at the send seam"
        );
        // The quorum-counting kinds are the point of the location: a
        // duplicate of one of these must never fabricate a quorum.
        if matches!(
            msg,
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. }
        ) {
            reach_once!(
                st.duplicated_quorum_kind,
                "the driver duplicates a quorum-counting message at the send seam"
            );
        }
        match msg {
            Message::Commit { .. } => {
                reach_once!(
                    st.duplicated_commit,
                    "the driver duplicates a commit at the send seam"
                );
            }
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                reach_once!(
                    st.duplicated_repair,
                    "the driver duplicates a repair message at the send seam"
                );
            }
            Message::CatchUpRequest { .. } => {
                reach_once!(
                    st.duplicated_catchup_request,
                    "the driver duplicates a catch-up request at the send seam"
                );
            }
            // The snap plane's idempotency witnesses: a duplicated custody ack
            // meets the leader's set-based tally; a duplicated chunk response
            // finds its chunks no longer pending.
            Message::SnapAck { .. } => {
                reach_once!(
                    st.duplicated_snap_ack,
                    "the driver duplicates a snap custody ack at the send seam"
                );
            }
            Message::SnapChunkRequest { .. } => {
                reach_once!(
                    st.duplicated_snap_chunk_request,
                    "the driver duplicates a snap chunk request at the send seam"
                );
            }
            Message::SnapChunkResponse { .. } => {
                reach_once!(
                    st.duplicated_snap_chunk_response,
                    "the driver duplicates a snap chunk response at the send seam"
                );
            }
            // A re-delivered handoff must be a no-op at its addressee — never
            // an allocator rewind — and refused everywhere else. The uniqueness
            // oracle above is what keeps that honest.
            Message::Relinquish { .. } => {
                reach_once!(
                    st.duplicated_relinquish,
                    "the driver duplicates a relinquishment at the send seam"
                );
            }
            _ => {}
        }
    }

    fn client_reply_dropped(&self, _node: NodeId, reply: paros::Reply) {
        let mut st = self.state();
        reach_once!(
            st.reply_dropped,
            "a committed client reply is dropped at the reply seam"
        );
        if matches!(reply, paros::Reply::Propose | paros::Reply::ProposeDedup) {
            st.propose_reply_dropped = true;
        }
        if matches!(reply, paros::Reply::Read) {
            reach_once!(
                st.read_reply_dropped,
                "a confirmed read reply is dropped at the reply seam"
            );
        }
    }
    fn snapshot_offered(&self, _node: NodeId, _offers: u64) {
        let mut st = self.state();
        reach_once!(
            st.snapshot_offered,
            "the driver queues a snapshot offer before the send seam"
        );
    }

    fn compact_acked(&self, _node: NodeId, accepted: bool) {
        let mut st = self.state();
        if accepted {
            reach_once!(
                st.compact_ack_accepted,
                "a compact request is acked as accepted"
            );
        } else {
            reach_once!(
                st.compact_ack_refused,
                "a compact request is acked as refused"
            );
        }
    }

    fn dropped_at_mailbox(&self, _node: NodeId, _to: NodeId, _kind: &'static str) {
        let mut st = self.state();
        reach_once!(st.mailbox_dropped, "mailbox overflow dropped a message");
    }

    fn snapshot_offer_skipped(&self, _node: NodeId, _offered: Slot) {
        let mut st = self.state();
        reach_once!(
            st.offer_skipped,
            "the driver skips a mismatched snapshot offer"
        );
    }

    fn resend_skipped(&self, _node: NodeId) {
        let mut st = self.state();
        reach_once!(
            st.resend_skipped,
            "the driver skips a pending accept re-send"
        );
    }

    fn election_timeout_extreme(&self, _node: NodeId, _ticks: u64) {
        let mut st = self.state();
        reach_once!(
            st.shortest_timeout,
            "the driver selects the shortest valid election timeout"
        );
    }

    fn waiter_superseded(&self, _node: NodeId, _slot: Slot) {
        let mut st = self.state();
        reach_once!(
            st.waiter_superseded,
            "a parked proposal reply is superseded by a different decided command"
        );
    }

    fn quorum_lost(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.quorum_lost,
            "a leader without an ack quorum steps down (CheckQuorum)"
        );
    }

    fn duplicate_suppressed(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        // Reachable-only: the double-choose needs a partition-era retry plus a
        // later election's mandatory P2c re-proposal — a per-run `sometimes`
        // would starve saturation on seeds that never partition a leader.
        reach_once!(
            st.duplicate_suppressed,
            "a re-chosen (client, seq) is suppressed at the apply seam (at-most-once)"
        );
    }

    fn faulty_reported(&self, node: NodeId, entries: &[(Slot, Ballot)]) {
        let mut st = self.state();
        // Staged, not live: this fires from the boot path *before* the boot's
        // `recovered` report, which swaps the staged set in as this
        // incarnation's classification (and drops the previous boot's).
        let staged = st.faulty_staged.entry(node.0).or_default();
        for &(slot, _ballot) in entries {
            staged.insert(slot.0);
        }
    }

    fn app_repair_started(&self, _node: NodeId, _from: Slot, below_floor: bool) {
        let mut st = self.state();
        if below_floor {
            reach_once!(
                st.app_repair_below_floor_seen,
                "a node with a lost snapshot waits on a peer InstallSnapshot below its floor"
            );
        } else {
            reach_once!(
                st.app_repair_seen,
                "a faulty chosen record stalls the apply seam and opens a repair"
            );
        }
    }

    fn repair_progress(
        &self,
        _node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
        _bytes: u64,
    ) {
        let mut st = self.state();
        if repaired > 0 {
            reach_once!(
                st.repaired_seen,
                "a faulty record is repaired in place from the cluster"
            );
        }
        if case1 > 0 {
            reach_once!(
                st.case1_seen,
                "a blocked slot resolves as Case 1 from a straggler's clean copy"
            );
        }
        if case2 > 0 {
            reach_once!(
                st.case2_seen,
                "a blocked slot resolves as Case 2 with a full quorum of none"
            );
        }
        if step_downs > 0 {
            reach_once!(
                st.repair_stepdown_seen,
                "a leader that cannot finish recovery resigns (recovery timeout)"
            );
        }
    }

    fn recovery_batch(&self, _node: NodeId, started: u64, gap_fills: u64, remaining: u64) {
        assert_always!(
            started <= LEADER_RECOVERY_BATCH as u64,
            "a leader starts at most one bounded recovery chunk per Ready",
            { "started" => started, "remaining" => remaining }
        );
        assert_always!(
            gap_fills <= started,
            "a recovery batch reports only gap fills it actually started",
            { "started" => started, "gap_fills" => gap_fills }
        );
        if gap_fills > 0 {
            let mut st = self.state();
            reach_once!(
                st.gap_filled,
                "a new leader gap-fills a hole its promise quorum never reported"
            );
        }
    }

    fn snap_recorded(&self, node: NodeId, at: Slot) {
        let mut st = self.state();
        st.snap_points.entry(node.0).or_default().insert(at.0);
        reach_once!(
            st.snap_recorded_seen,
            "storage: a decided snapshot point is recorded at its marker slot"
        );
    }

    fn snap_chunks_reported(&self, _node: NodeId, _at: Slot, _chunks: u64) {
        let mut st = self.state();
        reach_once!(
            st.snap_chunks_reported_seen,
            "storage: rotted snapshot chunks are reported for peer repair"
        );
    }

    fn snap_chunk_repaired(
        &self,
        _node: NodeId,
        _at: Slot,
        chunks: u64,
        bytes: u64,
        blob_bytes: u64,
    ) {
        let mut st = self.state();
        // The CTRL §5.2 chunk-repair cost metric: an install ships at most the
        // chunks it names — never the whole blob riding along.
        assert_always!(
            bytes <= chunks.saturating_mul(SNAP_CHUNK_BYTES as u64),
            "storage: a chunk repair ships at most the chunks it installs",
            { "chunks" => chunks, "bytes" => bytes, "blob_bytes" => blob_bytes }
        );
        reach_once!(
            st.snap_chunk_repaired_seen,
            "storage: a rotted snapshot chunk is repaired from a peer"
        );
    }

    fn snap_advanced_fallback(&self, _node: NodeId, _to: NodeId) {
        let mut st = self.state();
        reach_once!(
            st.snap_fallback_seen,
            "storage: a chunk request is answered with the advanced whole snapshot"
        );
    }

    fn snap_point_restored(&self, _node: NodeId, _at: Slot) {
        let mut st = self.state();
        reach_once!(
            st.snap_restore_seen,
            "storage: a lost application state is restored from the decided snapshot point"
        );
    }

    fn prepare_below_floor(&self, _node: NodeId, _from_slot: Slot, _floor: Slot) {
        let mut st = self.state();
        // Rare (only a lagging node below a compacted peer's floor triggers it),
        // so reachable-only: it must be hit at least once across exploration,
        // not on every seed.
        reach_once!(
            st.prepare_below_floor,
            "a candidate prepares below a peer's compaction floor"
        );
    }
}

// --- the client-history checker ---------------------------------------------

/// One committed operation's real-time span: first issue to first committed
/// ack, in simulated milliseconds. Two spans sharing a boundary millisecond are
/// treated as *concurrent* (no precedence edge), which can only drop — never
/// fabricate — a real-time constraint, so the checker stays sound at
/// millisecond granularity.
#[derive(Clone, Copy)]
struct OpSpan {
    inv: u64,
    resp: u64,
}

impl OpSpan {
    fn before(self, other: OpSpan) -> bool {
        self.resp < other.inv
    }
}

/// This run's client workload mode, per client instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientMode {
    /// One outstanding op at a time: `W0 R0 W1 R1 …`.
    Sequential,
    /// Several fresh-seq proposals in flight at once.
    Pipelined,
    /// One decision, then silence.
    Quiet,
}

/// One client's own record of what it asked for and what came back. Owned by
/// the workload — the client is the only party that knows its own program order
/// — and merged into the shared [`LinHistory`] at `check()` time.
#[derive(Default)]
pub(crate) struct ClientHistory {
    client: u64,
    mode: Option<ClientMode>,
    issued: usize,
    acked: usize,
    failed: usize,
    read_issued: usize,
    read_acked: usize,
    read_failed: usize,
    /// First issue time per write seq.
    write_inv: BTreeMap<u64, u64>,
    /// First committed ack per write seq: `(time, slot)`.
    write_resp: BTreeMap<u64, (u64, Option<u64>)>,
    read_inv: BTreeMap<u64, u64>,
    /// First committed ack per read seq: `(time, watermark)`.
    read_resp: BTreeMap<u64, (u64, Option<u64>)>,
    read_retried: bool,
}

impl ClientHistory {
    pub(crate) fn set_client(&mut self, client: u64) {
        self.client = client;
    }

    pub(crate) fn set_mode(&mut self, mode: ClientMode) {
        self.mode = Some(mode);
    }

    pub(crate) fn record_write_issued(&mut self, seq: u64, now_ms: u64) {
        self.issued += 1;
        self.write_inv.entry(seq).or_insert(now_ms);
    }

    pub(crate) fn record_write_ack(&mut self, seq: u64, slot: Option<u64>, now_ms: u64) {
        self.acked += 1;
        self.write_resp.entry(seq).or_insert((now_ms, slot));
    }

    pub(crate) fn record_write_failed(&mut self) {
        self.failed += 1;
    }

    pub(crate) fn record_read_issued(&mut self, seq: u64, now_ms: u64) {
        self.read_issued += 1;
        self.read_inv.entry(seq).or_insert(now_ms);
    }

    pub(crate) fn record_read_ack(
        &mut self,
        seq: u64,
        watermark: Option<u64>,
        attempts: u64,
        now_ms: u64,
    ) {
        self.read_acked += 1;
        self.read_resp.entry(seq).or_insert((now_ms, watermark));
        self.read_retried |= attempts > 1;
    }

    pub(crate) fn record_read_failed(&mut self) {
        self.read_failed += 1;
    }
}

/// The committed client history of the whole run, keyed by `(client_id, seq)`.
/// A watermark is `Option<u64>`: an absent `read_index` is the *empty* applied
/// prefix, and `None < Some(0)` is exactly the watermark order.
///
/// The register under check is the **applied log prefix**: an acked write is a
/// state transition at its committed `slot`, and a committed read observes the
/// watermark. Failed / timed-out operations enter no constraint — a timed-out
/// write may still commit later, so it is deliberately unconstrained.
///
/// Its bools are independent per-run coverage flags (see [`AuditState`]).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct LinHistory {
    /// Acked writes with a known slot (program order within one client).
    write_slot: BTreeMap<(u64, u64), u64>,
    /// Committed reads and their observed watermark.
    read_wm: BTreeMap<(u64, u64), Option<u64>>,
    /// Committed writes as real-time spans with their slot (`None` for a
    /// defensive slotless ack, which still forbids the empty prefix later).
    writes: Vec<(OpSpan, Option<u64>)>,
    /// Committed reads as real-time spans with their watermark.
    reads: Vec<(OpSpan, Option<u64>)>,
    /// Clients whose mode gives no per-client program order to linearize.
    pipelined_clients: BTreeSet<u64>,
    sequential_seen: bool,
    pipelined_seen: bool,
    quiet_seen: bool,
    issued: usize,
    acked: usize,
    failed: usize,
    read_issued: usize,
    read_acked: usize,
    read_failed: usize,
    read_ack_ms: Vec<u64>,
    read_retried: bool,
}

impl LinHistory {
    /// Fold one client's record in. Called once per client, from its `check()`.
    fn merge(&mut self, h: &ClientHistory) {
        let c = h.client;
        match h.mode {
            Some(ClientMode::Sequential) => self.sequential_seen = true,
            Some(ClientMode::Pipelined) => {
                self.pipelined_seen = true;
                self.pipelined_clients.insert(c);
            }
            Some(ClientMode::Quiet) => self.quiet_seen = true,
            None => {}
        }
        self.issued += h.issued;
        self.acked += h.acked;
        self.failed += h.failed;
        self.read_issued += h.read_issued;
        self.read_acked += h.read_acked;
        self.read_failed += h.read_failed;
        self.read_retried |= h.read_retried;
        for (&seq, &(resp, slot)) in &h.write_resp {
            if let Some(s) = slot {
                self.write_slot.insert((c, seq), s);
            }
            if let Some(&inv) = h.write_inv.get(&seq) {
                self.writes.push((OpSpan { inv, resp }, slot));
            }
        }
        for (&seq, &(resp, wm)) in &h.read_resp {
            self.read_wm.insert((c, seq), wm);
            self.read_ack_ms.push(resp);
            if let Some(&inv) = h.read_inv.get(&seq) {
                self.reads.push((OpSpan { inv, resp }, wm));
            }
        }
    }

    /// The per-run workload modes rotate across the seed sweep; gated so
    /// saturation proves every mode is reached.
    fn check_mode_gates(&self) {
        assert_sometimes!(
            self.sequential_seen,
            "a run uses the sequential client workload mode"
        );
        if self.sequential_seen {
            assert_reachable!("a run uses the sequential client workload mode");
        }
        assert_sometimes!(
            self.pipelined_seen,
            "a run uses the pipelined client workload mode"
        );
        if self.pipelined_seen {
            assert_reachable!("a run uses the pipelined client workload mode");
        }
        // The one-decision-then-idle mode: the only run shape whose chosen
        // prefix stops at slot 0, and therefore the only one in which an empty
        // prefix can be told apart from a prefix of exactly slot 0.
        assert_sometimes!(
            self.quiet_seen,
            "a run uses the quiet single-decision workload mode"
        );
        if self.quiet_seen {
            assert_reachable!("a cluster decides one slot and then idles");
        }
    }

    /// Coverage gates on the client-visible register (`UntilCoverageStable`
    /// only saturates once these fire).
    fn check_coverage_gates(
        &self,
        committed_clients: &BTreeSet<u64>,
        leader_change_ms: Option<u64>,
    ) {
        let multi_client = committed_clients.len() > 1;
        assert_sometimes!(
            multi_client,
            "a run drives concurrent clients against one register"
        );
        if multi_client {
            assert_reachable!("a run drives concurrent clients against one register");
        }
        let concurrent_read_write = self.reads.iter().any(|&(r, _)| {
            self.writes
                .iter()
                .any(|&(w, _)| !w.before(r) && !r.before(w))
        });
        assert_sometimes!(
            concurrent_read_write,
            "a linearizable read commits concurrently with a conflicting write"
        );
        if concurrent_read_write {
            assert_reachable!("a linearizable read commits concurrently with a conflicting write");
        }
        assert_sometimes!(!self.read_wm.is_empty(), "a linearizable read commits");
        if !self.read_wm.is_empty() {
            assert_reachable!("a linearizable read commits");
        }
        let multi_slot = self.read_wm.values().any(|wm| *wm >= Some(1));
        assert_sometimes!(multi_slot, "a committed read observes a multi-slot prefix");
        if multi_slot {
            assert_reachable!("a committed read observes a multi-slot prefix");
        }
        // A read served after leadership changed hands — the window where a
        // naive local read goes stale.
        let read_after_change =
            leader_change_ms.is_some_and(|t| self.read_ack_ms.iter().any(|&ms| ms > t));
        assert_sometimes!(read_after_change, "a read commits after a leader change");
        if read_after_change {
            assert_reachable!("a read commits after a leader change");
        }
        assert_sometimes!(
            self.read_retried,
            "a read is retried across nodes before committing"
        );
        if self.read_retried {
            assert_reachable!("a read is retried across nodes before committing");
        }
    }
}

/// The full checker: disclosed-order linearizability over real time. Committed
/// writes pin to their slot, committed reads to their watermark; the induced
/// order is a valid linearization iff it agrees with every real-time precedence
/// edge. A Wing & Gong / Porcupine search backtracks over candidate
/// linearization orders; here the consensus log *discloses* every linearization
/// point, so the search collapses to its verification half — four pairwise
/// interval checks over committed operations, valid for any number of
/// concurrent clients and any per-client mode, bounded by [`LIN_HISTORY_CAP`].
fn check_disclosed_order(h: &LinHistory) {
    if h.writes.len() + h.reads.len() > LIN_HISTORY_CAP {
        return;
    }
    // L1 — the log order of two committed writes agrees with their real-time
    // order.
    for (i, &(w1, s1)) in h.writes.iter().enumerate() {
        for &(w2, s2) in &h.writes[i + 1..] {
            let (Some(s1), Some(s2)) = (s1, s2) else {
                continue;
            };
            if w1.before(w2) {
                assert_always!(
                    s1 < s2,
                    "two real-time-ordered committed writes land in log order"
                );
            } else if w2.before(w1) {
                assert_always!(
                    s2 < s1,
                    "two real-time-ordered committed writes land in log order"
                );
            }
        }
    }
    // L2 — a committed read observes every write that completed before it
    // began (a slotless committed ack still forbids the empty prefix). L3 — a
    // write invoked after a committed read lands above that read's watermark.
    for &(r, wm) in &h.reads {
        for &(w, slot) in &h.writes {
            if w.before(r) {
                let observed = match slot {
                    Some(s) => wm >= Some(s),
                    None => wm.is_some(),
                };
                assert_always!(
                    observed,
                    "a committed read observes every write completed before it began"
                );
            } else if r.before(w)
                && let Some(s) = slot
            {
                assert_always!(
                    Some(s) > wm,
                    "a write invoked after a committed read lands above its watermark"
                );
            }
        }
    }
    // L4 — watermarks of real-time-ordered committed reads never move
    // backwards.
    for (i, &(r1, wm1)) in h.reads.iter().enumerate() {
        for &(r2, wm2) in &h.reads[i + 1..] {
            if r1.before(r2) {
                assert_always!(
                    wm2 >= wm1,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            } else if r2.before(r1) {
                assert_always!(
                    wm1 >= wm2,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            }
        }
    }
}

/// The sequential fast path for one non-pipelined client: program order (seq)
/// is real-time order within the client even where timestamps tie, so C1-C3
/// are strictly stronger than the interval checks for its operations.
fn check_sequential_client(client: u64, h: &LinHistory) {
    let span = (client, 0)..=(client, u64::MAX);
    // C1 — a committed read observes every write acked before it began: read
    // `k` starts after write `j`'s ack for every `j <= k`, so its watermark
    // covers the running max acked slot (two-pointer over seq).
    let mut max_acked_slot: Option<u64> = None;
    let mut writes = h.write_slot.range(span.clone()).peekable();
    for (&(_, rk), &wm) in h.read_wm.range(span.clone()) {
        while let Some(&(&(_, wj), &slot)) = writes.peek() {
            if wj > rk {
                break;
            }
            max_acked_slot = max_acked_slot.max(Some(slot));
            writes.next();
        }
        assert_always!(
            wm >= max_acked_slot,
            "a committed read's watermark covers every write acked before it began"
        );
    }

    // C2 — this client's reads do not overlap, so their watermarks never move
    // backwards.
    let mut prev: Option<u64> = None;
    for (_, &wm) in h.read_wm.range(span.clone()) {
        assert_always!(wm >= prev, "committed-read watermarks never move backwards");
        prev = prev.max(wm);
    }

    // C3 — a write issued after a committed read must land above that read's
    // watermark (a slot at or below it would place the write inside the prefix
    // the read already observed). Guards against an inflated / speculative
    // watermark.
    let mut max_read_wm: Option<u64> = None;
    let mut reads = h.read_wm.range(span.clone()).peekable();
    for (&(_, wj), &slot) in h.write_slot.range(span) {
        while let Some(&(&(_, rk), &wm)) = reads.peek() {
            if rk >= wj {
                break;
            }
            max_read_wm = max_read_wm.max(wm);
            reads.next();
        }
        if let Some(i) = max_read_wm {
            assert_always!(
                slot > i,
                "a write issued after a committed read lands above its watermark"
            );
        }
    }
}
