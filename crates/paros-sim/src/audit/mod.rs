//! The sim-side [`Audit`] implementation: one shared incremental checker.
//!
//! This is where the paros correctness invariants live. The driver reports each
//! externally meaningful transition exactly once, at the instant it happens, so
//! every check here is **O(1) in the size of the run** — a map probe and a
//! comparison — rather than a re-scan of a growing event stream.
//!
//! Layout mirrors `crate::world`: one [`AuditWorld`] per simulation iteration,
//! published under a well-known [`StateHandle`] key so every node process and
//! every workload reaches the same instance, and factory-created per iteration
//! so recipe replay is exact. Each node wraps it in a [`NodeAudit`], which
//! stamps simulated time on the observations that need it.
//!
//! - [`state`] holds the folded facts and the per-transition safety checks;
//! - [`client`] holds the client's own history and the linearizability checks;
//! - [`check_run`] is the one entry point a workload's `check()` calls.
//!
//! Coverage gates split by *when* they can be judged. A `reachable` gate fires
//! at the transition instant — that is what makes it an exploration anchor — and
//! is de-duplicated by a sticky flag so it costs one branch afterwards. A
//! `sometimes` gate has to be recorded once per run whether or not it held,
//! otherwise a gate that never fires anywhere would silently vanish from the
//! saturation denominator, so those are evaluated once from `check_run`.

/// Fire a `reachable` gate the first time its sticky flag flips.
macro_rules! reach_once {
    ($flag:expr, $message:expr) => {
        if !$flag {
            $flag = true;
            assert_reachable!($message);
        }
    };
}

mod client;
mod matchmaker;
mod state;

/// A stable label per message kind, for the failure print's send tally.
fn message_kind(msg: &Message) -> &'static str {
    match msg {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Commit { .. } => "commit",
        Message::Nack { .. } => "nack",
        Message::Heartbeat { .. } => "heartbeat",
        Message::HeartbeatAck { .. } => "heartbeat_ack",
        Message::CatchUpRequest { .. } => "catch_up_request",
        Message::CatchUpResponse { .. } => "catch_up_response",
        Message::InstallSnapshot { .. } => "install_snapshot",
        Message::Relinquish { .. } => "relinquish",
        Message::SnapAck { .. } => "snap_ack",
        Message::SnapChunkRequest { .. } => "snap_chunk_request",
        Message::SnapChunkResponse { .. } => "snap_chunk_response",
        _ => "other",
    }
}

pub(crate) use client::ClientHistory;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use moonpool_sim::{StateHandle, TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::{
    AcceptorConfig, Audit, Ballot, Deployment, EdgeRejection, HANDOFF_BATCH, Handoff,
    LEADER_RECOVERY_BATCH, MatchRefusal, MatchmakerId, Message, NodeId, PROMISE_BATCH,
    ReconfigureResult, SNAP_CHUNK_BYTES, Seam, Slot, StorageError, StorageFaultDecision,
    StorageRecord, command_hash,
};

use self::state::AuditState;

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`AuditWorld`] is published (shared by every node and every workload).
const AUDIT_WORLD_KEY: &str = "paros-audit-world";

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

/// The per-iteration shared checker.
#[derive(Default)]
pub(crate) struct AuditWorld {
    state: Mutex<AuditState>,
}

impl AuditWorld {
    /// A private checker for a run with **no client** at all (the storage
    /// contract suite drives the world-backed storage directly): every
    /// per-transition check still runs, except the "applied command was
    /// proposed" claim, which has no client to be proposed by.
    pub(crate) fn client_free() -> Self {
        let world = Self::default();
        world.lock().client_free = true;
        world
    }

    fn lock(&self) -> MutexGuard<'_, AuditState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The workload registered a user command it is about to propose. Fed
    /// before the RPC leaves, so an applied user command that was never
    /// registered is one the cluster invented.
    pub(crate) fn note_submitted(&self, cmd_hash: u64) {
        self.lock().submitted.insert(cmd_hash);
    }

    /// The application applied one command at `index` (its 1-based applied
    /// count), reaching `state`. Reported by the storage layer as the
    /// transition is made durable. Contiguous per node, one command and one
    /// state per index cluster-wide, and a user command traces to a submission.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) fn app_applied(
        &self,
        node: u64,
        index: u64,
        cmd_hash: u64,
        user: bool,
        noop: bool,
        state: u64,
    ) {
        let mut st = self.lock();
        if noop {
            reach_once!(st.noop_applied, "chain: noop gap fill is applied");
        }
        let expected = st
            .app_index
            .get(&node)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        assert_always!(
            index == expected,
            "chain: applies are contiguous per node",
            { "node" => node, "index" => index, "expected" => expected }
        );
        st.app_index.insert(node, index);
        let prior_command = *st.app_command.entry(index).or_insert(cmd_hash);
        let prior_state = *st.app_state.entry(index).or_insert(state);
        assert_always!(
            prior_command == cmd_hash && prior_state == state,
            "chain: one state per applied index",
            {
                "node" => node,
                "index" => index,
                "expected_command" => prior_command,
                "observed_command" => cmd_hash,
                "expected_state" => prior_state,
                "observed_state" => state
            }
        );
        // The was-proposed claim guards *client* commands: a control command
        // is minted inside the system (a leader's `Noop` gap fill, a `Snap`
        // marker, a `Truncate`), so only a user entry must trace back to a
        // submission.
        assert_always!(
            !user || st.client_free || st.submitted.contains(&cmd_hash),
            "chain: applied command was proposed",
            { "node" => node, "index" => index, "command" => cmd_hash }
        );
    }

    /// The application jumped to `state` at `index` through a snapshot install
    /// or a decided-point restore. Never backward per node, and agreeing at its
    /// index with every apply and install that reached it.
    #[tracing::instrument(level = "debug", skip(self), fields(node, index, state))]
    pub(crate) fn app_snapshot(&self, node: u64, index: u64, state: u64) {
        let mut st = self.lock();
        let previous = st.app_index.get(&node).copied();
        assert_always!(
            previous.is_none_or(|previous| index >= previous),
            "chain: a snapshot jump never moves the applied index backward",
            {
                "node" => node,
                "from" => previous.map_or(-1_i64, |p| i64::try_from(p).unwrap_or(i64::MAX)),
                "to" => index
            }
        );
        st.app_index.insert(node, index);
        let prior_state = *st.app_state.entry(index).or_insert(state);
        assert_always!(
            prior_state == state,
            "chain: one state per applied index",
            {
                "node" => node,
                "index" => index,
                "expected_state" => prior_state,
                "observed_state" => state
            }
        );
    }

    /// A corrupted application snapshot was reset for recovery: the node's
    /// applied index legally restarts from zero, and the replay that follows
    /// re-derives the same per-index states.
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn app_reset(&self, node: u64) {
        self.lock().app_index.remove(&node);
    }

    /// How many below-floor `Prepare`s each acceptor has refused so far.
    pub(crate) fn below_floor_refusals(&self) -> BTreeMap<u64, u64> {
        self.lock().below_floor_refusals.clone()
    }

    /// Whether `node` installed a snapshot landing at or past `index`.
    pub(crate) fn snapshot_landed_at_least(&self, node: u64, index: u64) -> bool {
        self.lock()
            .snap_landings
            .get(&node)
            .is_some_and(|landings| landings.iter().any(|landing| *landing >= index))
    }

    /// The cluster's applied high-water mark so far (`None` before any apply).
    pub(crate) fn cluster_applied_max(&self) -> Option<u64> {
        self.lock().cluster_applied_max
    }

    /// A one-line picture of the run for the red path: per-node applied
    /// prefixes, the leader rounds, and the last chosen gap each node reported.
    pub(crate) fn diagnostics(&self) -> String {
        let st = self.lock();
        format!(
            "applied_max={:?} cluster_max={:?} booted={:?} storage_dead={:?} leader_rounds={:?} last_gap={:?} promised={:?} sent={:?} delivery_failures={} edge_rejections={} matchmakers=[{}]",
            st.applied_max,
            st.cluster_applied_max,
            st.booted,
            st.storage_dead,
            st.leader_rounds,
            st.last_gap,
            st.promised,
            st.sent_kinds,
            st.delivery_failures,
            st.edge_rejections,
            st.matchmaker.diagnostics()
        )
    }

    /// Record this run's `sometimes` coverage gates. Called once per workload
    /// from the `check()` phase; repeating it (several client workloads run
    /// concurrently) is idempotent — a `sometimes` slot only accumulates
    /// samples, and an `always` re-check of the same true fact is free.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn check_gates(&self) {
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
        st.check_protocol_gates();
        st.check_driver_hook_gates();
        st.matchmaker.check_gates();
    }

    /// Merge one client's recorded history into the shared one and run the
    /// client-visible checks over everything merged so far. Every client
    /// workload calls this from `check()`; the merged history only grows, so a
    /// later caller sees a superset and the checks stay sound at every step.
    #[tracing::instrument(level = "debug", skip_all)]
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
    #[tracing::instrument(level = "debug", skip(self), fields(node))]
    pub(crate) fn note_storage_dead(&self, node: u64) {
        self.lock().storage_dead.insert(node);
    }

    /// A node booted again after a process-level kill (moonpool attrition on
    /// the main campaign, the script on the corpus) while `parked_peers` other
    /// nodes sat terminally parked. Until this very boot the node was down, so
    /// the two loss kinds — persistent (a parked disk that never comes back)
    /// and transient (a process that does) — overlapped for the whole hold-down.
    /// On a small cluster that overlap is the interesting one: `n = 3` with one
    /// parked node and one killed node has **no quorum** until the killed node
    /// returns, and the run is still required to converge afterwards.
    ///
    /// Recorded here as coverage, never as a verdict: whether a seed draws both
    /// an attrition kill and a parking corruption is the swarm's business.
    #[tracing::instrument(level = "debug", skip(self), fields(node, parked_peers, cluster_size))]
    pub(crate) fn note_process_restart(&self, node: u64, parked_peers: usize, cluster_size: usize) {
        let mut st = self.lock();
        if parked_peers == 0 {
            return;
        }
        reach_once!(
            st.parked_overlap,
            "storage: a transient process loss overlaps a corruption-parked node"
        );
        let quorum = cluster_size / 2 + 1;
        // The node reporting is the one that was down; anything else down at
        // the same time only makes the loss deeper, so this is the *at least*
        // side of the count.
        let live_during_hold_down = cluster_size.saturating_sub(parked_peers + 1);
        if live_during_hold_down < quorum {
            reach_once!(
                st.parked_overlap_quorum_returned,
                "storage: quorum returns after a parked node and a transient process loss overlapped"
            );
        }
        tracing::info!(node, parked_peers, cluster_size, "restart_over_parked_peer");
    }

    /// Whether any node has been observed lagging the cluster prefix (one leg
    /// of the #71 compound corruption x partition x lag gate).
    pub(crate) fn lag_observed(&self) -> bool {
        !self.lock().lagged.is_empty()
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

    /// A fold of the run's end state for the determinism proof: the chosen
    /// log, every node's applied prefix, and the leadership history. Two runs
    /// of one seed must agree on it bit for bit.
    pub(crate) fn digest(&self) -> u64 {
        let st = self.lock();
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut fold = |v: u64| {
            for byte in v.to_le_bytes() {
                h ^= u64::from(byte);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (&slot, &vhash) in &st.chosen {
            fold(slot);
            fold(vhash);
        }
        for (&node, &max) in &st.applied_max {
            fold(node);
            fold(max);
        }
        for &round in &st.leader_rounds {
            fold(round);
        }
        for (&node, &round) in &st.leader_round {
            fold(node);
            fold(round);
        }
        h
    }

    /// The convergence deliverable, judged at the end of the recovery tail
    /// when no future leader change can invalidate a provisional quiescence
    /// decision. It ties the run's four frontiers together:
    ///
    /// ```text
    /// decided frontier == applied frontier == every live node's applied prefix >= every acked slot
    /// ```
    ///
    /// - the **decided frontier** (`decided_max`) is the highest slot a
    ///   majority of the configured cluster durably accepted at one ballot
    ///   for one value — the quorum-decided oracle
    ///   ([`AuditState::decided`]: keyed by `(slot, ballot)`, value-checked
    ///   per key, so it is Paxos "chosen" and never a cross-ballot count),
    ///   fed by durable accepts alone, so it sees a slot that is chosen even
    ///   if no node ever applied it (the blind spot of the apply-fed `chosen`
    ///   map);
    /// - the **applied frontier** (`cluster_max`) is the highest slot any node
    ///   applied; a node's applied prefix is contiguous (`check_no_gaps`), so
    ///   the frontier names a prefix, not a sparse set;
    /// - every node this run brought up that is not terminally parked ends
    ///   exactly on that frontier;
    /// - every slot a client was told was committed is inside it (the
    ///   per-identity presence check lives in the client history fold).
    ///
    /// `decided == applied` is two liveness claims in one. `decided <= applied`
    /// says every quorum-decided slot was eventually applied: a slot durably
    /// accepted by a majority whose proposer never learnt it (lost `Accepted`
    /// acks, a crashed proposer) must still be chosen — by the `Accept`
    /// re-send, by a successor's P2c re-proposal, or as a gap-filled `Noop`
    /// (the applied value's agreement with the decided value is asserted per
    /// apply, so a `Noop` here means the decided command was itself a
    /// control command or a #94-suppressed identity). `decided >= applied`
    /// says nothing was applied without a durable majority behind it — the
    /// persist-before-send ordering seen from the outside. That ordering is
    /// not assumed here; it is asserted at each transition it rests on, and
    /// this end-of-run leg is their corollary: an outgoing `Accepted` names
    /// a durably recorded accept, an outgoing `Commit` names a slot the
    /// tally already decided with that value, and every `applied` report
    /// finds its slot decided (all three in the `sent`/`applied` callbacks
    /// below). The driver folds each accept at its fsync
    /// (`surface_persisted`), before the ack that could count toward a
    /// quorum leaves the node.
    ///
    /// Sparse states are excused where they are legal: a run in which nothing
    /// was ever applied has no frontier (then nothing may have been decided or
    /// acked either), and a parked node is excused from the per-node leg only
    /// when its parking was observed as a corruption crash.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn check_final_convergence(&self, acked_max: Option<u64>) {
        let mut st = self.lock();
        let Some(cluster_max) = st.applied_max.values().copied().max() else {
            assert_always!(
                acked_max.is_none(),
                "every acked slot is inside the cluster's applied prefix at the end of the tail"
            );
            assert_always!(
                st.decided_max.is_none(),
                "every quorum-decided slot is applied by the end of the tail",
                { "decided_max" => st.decided_max.unwrap_or(0), "cluster_max" => -1_i64 }
            );
            return;
        };
        // The prefix every node must reach covers everything any client was
        // told was committed.
        assert_always!(
            acked_max.is_none_or(|acked| acked <= cluster_max),
            "every acked slot is inside the cluster's applied prefix at the end of the tail",
            { "acked_max" => acked_max.unwrap_or(0), "cluster_max" => cluster_max }
        );
        // The decided frontier and the applied frontier coincide (see above).
        assert_always!(
            st.decided_max.is_none_or(|decided| decided <= cluster_max),
            "every quorum-decided slot is applied by the end of the tail",
            { "decided_max" => st.decided_max.unwrap_or(0), "cluster_max" => cluster_max }
        );
        assert_always!(
            st.decided_max.is_some_and(|decided| decided >= cluster_max),
            "the applied frontier never runs ahead of the quorum-decided frontier",
            {
                "decided_max" => st.decided_max.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX)),
                "cluster_max" => cluster_max
            }
        );
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
            assert_always!(
                prefix == Some(cluster_max),
                "every node converges to the cluster's chosen prefix at the end of the settle tail",
                {
                    "node" => node,
                    "prefix" => prefix.map_or(-1_i64, |p| i64::try_from(p).unwrap_or(i64::MAX)),
                    "cluster_max" => cluster_max,
                    "decided_max" => st.decided_max.map_or(-1_i64, |s| i64::try_from(s).unwrap_or(i64::MAX))
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

/// The whole check, in one place: the two perspectives a run is judged from.
///
/// **Client side** — the workload's own history, merged into the shared one:
/// disclosed-order linearizability over real time (L1–L4), the sequential
/// per-client checks (C1–C3), and every acked identity present in the audit's
/// applied map. **Audit side** — the coverage gates recorded once per run, the
/// storage world's injected⇔detected correlation, and the one liveness claim:
/// every live node ends on the cluster's applied prefix, which covers every
/// acked slot. Returns the run's digest for the determinism proof.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn check_run(state: &StateHandle, history: &ClientHistory) -> u64 {
    let audit = audit_world(state);
    audit.check_client_history(history);
    audit.check_gates();
    crate::world::check_storage_gates(state);
    crate::shape::check_shape_gates(state);
    let acked_max = audit.lock().lin.acked_max();
    audit.check_final_convergence(acked_max);
    audit.digest()
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
    /// Matchmaking invariant 1 (#120): on a deployment with matchmakers, no
    /// `Prepare` leaves a node for a ballot whose matchmaking this fold has
    /// not seen close with a quorum, and it carries exactly the registered
    /// configuration. The re-sent probe `Prepare`s of a leader's repair probe
    /// run at the leadership ballot, which was licensed the same way. On plain
    /// Multi-Paxos a `Prepare` carries no configuration at all.
    fn check_prepare_licence(
        &self,
        node: NodeId,
        to: NodeId,
        ballot: Ballot,
        config: Option<&AcceptorConfig>,
    ) {
        let st = self.state();
        if st.matchmaker.has_matchmakers() {
            assert_always!(
                st.matchmaker.phase1_licensed(node.0, ballot),
                "matchmaking: no Prepare leaves before a matchmaker quorum registered its ballot",
                { "node" => node.0, "round" => ballot.round, "to" => to.0 }
            );
            let registered = st.matchmaker.registered_config(ballot);
            assert_always!(
                config.is_some_and(|c| registered == Some(c)),
                "matchmaking: a Prepare carries the configuration registered for its ballot",
                { "node" => node.0, "round" => ballot.round }
            );
        } else {
            assert_always!(
                config.is_none(),
                "plain: a Prepare on a deployment without matchmakers carries no configuration",
                { "node" => node.0, "round" => ballot.round }
            );
        }
    }
    pub(crate) fn new(time: T, world: Arc<AuditWorld>) -> Self {
        Self { time, world }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.time.now().as_millis()).unwrap_or(u64::MAX)
    }

    fn state(&self) -> MutexGuard<'_, AuditState> {
        self.world.lock()
    }

    /// Persist-before-send, observed at the send seam: the two replies whose
    /// meaning is a durable fact must find that fact already folded — an
    /// `Accepted` its sender's durable accept, a `Commit` a quorum decision
    /// on the tally (see [`AuditWorld::check_final_convergence`]).
    fn observe_durable_send(&self, node: NodeId, msg: &Message) {
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
        if let Message::Commit {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            // The leader-side half of persist-before-send: a `Commit` is the
            // core's decision on a quorum of `Accepted`s, each preceded (above)
            // by its durable accept, so the tally has already decided the slot
            // — and with this value, at this or a lower ballot (P2: a later
            // ballot re-decides only the same value).
            let st = self.state();
            let decided = st.decided.get(&slot.0).copied();
            assert_always!(
                decided.is_some(),
                "an outgoing Commit names a slot a durable accept quorum already decided",
                { "node" => node.0, "slot" => slot.0, "round" => ballot.round }
            );
            assert_always!(
                decided.is_none_or(|(_, _, decided_vhash)| decided_vhash == command_hash(command)),
                "an outgoing Commit carries the quorum-decided value",
                { "node" => node.0, "slot" => slot.0, "round" => ballot.round }
            );
        }
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

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, first = first.0))]
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
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, chosen_index = chosen_index.0))]
    fn snapshot_installed(&self, node: NodeId, chosen_index: Slot, ballot: Ballot) {
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
        st.observe_applied_index(node.0, chosen_index.0);
    }

    fn snapshot_mid_election(&self, _node: NodeId) {
        let mut st = self.state();
        reach_once!(
            st.snapshot_mid_election,
            "a snapshot lands during a live election"
        );
    }

    fn applied(&self, node: NodeId, slot: Slot, vhash: u64, identity: Option<(u64, u64)>) {
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
        // Persist-before-send, observed at the apply seam: a slot is applied
        // only once chosen, chosen only on a quorum of `Accepted`s, and each
        // of those left its node after the audit folded the durable accept —
        // so by the time any node applies a slot, the tally has decided it.
        // The end-of-run `decided >= applied` leg is this, per slot.
        assert_always!(
            st.decided.contains_key(&slot.0),
            "an applied slot was decided by a durable accept quorum before any node applied it",
            { "node" => node.0, "slot" => slot.0 }
        );
        reach_once!(st.any_chosen, "a value is chosen");
        st.observe_applied_index(node.0, slot.0);
    }

    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {
        *self
            .state()
            .sent_kinds
            .entry(message_kind(msg))
            .or_default() += 1;
        if let Message::Prepare { ballot, config, .. } = msg {
            self.check_prepare_licence(node, to, *ballot, config.as_ref());
            self.state().observe_prepare_send(node.0, to.0, *ballot);
        }
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
        if let Message::Promise { ballot, .. } = msg {
            self.state().observe_promise_send(node.0, *ballot);
        }
        // Persist-before-send at the accept seam: an `Accepted` claims "I hold
        // this durably", so the matching record must already be in this
        // node's folded durable-accept tally (the same-batch write is flushed
        // and reported before the send; a re-answer names an older record
        // that was folded when it was first written or re-read at boot).
        self.observe_durable_send(node, msg);
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
            // Then *whom* it addresses and *on what Phase-1 licence* (#121,
            // #122): the ballot's own acceptors, once every prior
            // configuration promised a quorum.
            st.observe_accept_send(node.0, to.0, *ballot);
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

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, round = won.round))]
    fn elected(
        &self,
        node: NodeId,
        won: Ballot,
        promised: Ballot,
        _gap_fills: u64,
        config: &AcceptorConfig,
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        // Matchmaking invariants 4 and 5 (#120): a leadership on a matchmaker
        // deployment stands on a campaign that closed with a quorum and was
        // never refused, and runs Phase 2 under exactly the configuration
        // some matchmaker durably registered for the ballot. On plain
        // Multi-Paxos the configuration is the bootstrap membership, always.
        if st.matchmaker.has_matchmakers() {
            assert_always!(
                st.matchmaker.phase1_licensed(node.0, won),
                "matchmaking: a refused or unregistered ballot never becomes a leadership",
                { "node" => node.0, "round" => won.round }
            );
            let registered = st.matchmaker.registered_config(won);
            assert_always!(
                registered == Some(config),
                "matchmaking: a leader runs Phase 2 under the configuration registered for its ballot",
                { "node" => node.0, "round" => won.round }
            );
        } else {
            assert_always!(
                st.bootstrap.as_ref() == Some(config),
                "plain: a leader on a deployment without matchmakers keeps the bootstrap configuration",
                { "node" => node.0, "round" => won.round }
            );
        }
        st.bind_config(won, config);
        if st.bootstrap.as_ref().is_some_and(|b| b != config) {
            st.reconfiguration_completed = true;
        }
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

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0))]
    fn authority_relinquished(&self, node: NodeId, handoff: Handoff) {
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
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn authority_installed(
        &self,
        node: NodeId,
        from: NodeId,
        ballot: Ballot,
        next_slot: Slot,
        _tail: u64,
    ) {
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
    }

    fn handoff_refused(&self, _node: NodeId, target: u64, stale: u64, shape: u64, unfit: u64) {
        let mut st = self.state();
        let (last_target, last_stale, last_shape, last_unfit) = st.handoff_refused;
        st.handoff_refused = (
            target.max(last_target),
            stale.max(last_stale),
            shape.max(last_shape),
            unfit.max(last_unfit),
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
        if unfit > 0 {
            reach_once!(
                st.handoff_refused_unfit,
                "a handoff onto a node needing Phase-1 repair is refused"
            );
        }
    }

    fn handoff_fence_expired(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.handoff_fence_expired,
            "an uncovered inherited fence resigns back to an ordinary election"
        );
    }

    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {
        // A gap is perfectly ordinary — pipelining leaves several slots
        // undecided, and a follower that missed one `Commit` holds one until
        // catch-up runs — so nothing is asserted here. A gap that never heals
        // shows up where every liveness failure does: the end-of-run
        // convergence claim, and this record says where the node was stuck.
        self.state().last_gap.insert(node.0, (hole.0, above.0));
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
        if st.leader_change_ms.is_some() {
            st.ack_after_leader_change = true;
        }
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

    #[tracing::instrument(level = "trace", skip_all)]
    fn recovered(
        &self,
        node: NodeId,
        promised: Ballot,
        chosen_index: Option<Slot>,
        deployment: &Deployment,
        accepted: &[(Slot, Ballot, u64)],
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        st.booted.insert(node.0);
        // One shared deployment per run: every node's durable configuration
        // names the same bootstrap membership, pool and matchmaker set.
        let bootstrap = st
            .bootstrap
            .get_or_insert_with(|| deployment.bootstrap.clone())
            .clone();
        assert_always!(
            bootstrap == deployment.bootstrap,
            "every node derives the same bootstrap configuration",
            { "node" => node.0, "members" => deployment.bootstrap.members.len() }
        );
        let pool: BTreeSet<u64> = deployment.pool.iter().map(|n| n.0).collect();
        let known = st.pool.get_or_insert_with(|| pool.clone()).clone();
        assert_always!(
            known == pool,
            "every node derives the same node pool",
            { "node" => node.0, "pool" => pool.len() }
        );
        st.matchmaker.note_deployment(deployment.matchmakers.len());
        st.matchmaker.note_bootstrap(&deployment.bootstrap);
        st.matchmaker.node_booted(node);
        st.observe_promise(node.0, promised);
        // The boot report is the incarnation edge: swap in the faulty
        // classifications staged by *this* boot's scan and drop the previous
        // incarnation's — a stale excuse must not keep explaining divergence
        // forever.
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

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, decision = ?decision))]
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

    #[tracing::instrument(level = "trace", skip_all, fields(seam = ?seam))]
    fn crashed(&self, _node: NodeId, seam: Seam) {
        let mut st = self.state();
        st.crashed_any = true;
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
            Seam::AfterBootReplayBeforeSync => {
                reach_once!(
                    st.crashed_after_boot_replay,
                    "the driver crashes after the boot replay and before its sync"
                );
            }
            // The matchmaker's seams are reported through
            // `matchmaker_crashed`, in their own namespace.
            Seam::MatchBeforeSync | Seam::MatchAfterSyncBeforeReply => {}
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
        if matches!(reply, paros::Reply::Compact) {
            // The compaction client's re-ask loop is its own recovery path
            // (a lost ack must not double-seed a snapshot point), so the
            // kind keeps a gate beside the redirect family's.
            reach_once!(
                st.compact_reply_dropped,
                "a compaction reply is dropped at the reply seam"
            );
        }
        if matches!(
            reply,
            paros::Reply::ProposeRedirect
                | paros::Reply::ReadRedirect
                | paros::Reply::Compact
                | paros::Reply::Reconfigure
        ) {
            // Nothing committed behind these: the client sees a deadline and
            // retries blind. No dedup edge to track, only the reach.
            reach_once!(
                st.redirect_dropped,
                "a redirect or compaction reply is dropped at the reply seam"
            );
            return;
        }
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

    fn snap_chunk_withheld(&self, _node: NodeId, to: NodeId) {
        let mut st = self.state();
        // BUGGIFY pairing for `withhold_snap_chunk`: the fired half. The
        // recovery half fires in `snap_chunk_repaired` for a requester that
        // was withheld from.
        reach_once!(
            st.chunk_withheld,
            "a custodian withholds a requested snapshot chunk"
        );
        st.withheld_from.insert(to.0);
    }

    fn read_expired(&self, _node: NodeId, early: bool) {
        let mut st = self.state();
        if early {
            // BUGGIFY pairing for `expire_parked_read_early`. The recovery
            // half is the client's own: "a read is retried across nodes
            // before committing" in the client history fold.
            reach_once!(
                st.read_expired_early,
                "the driver redirects a parked read before its confirmation deadline"
            );
        } else {
            reach_once!(
                st.read_expired_overdue,
                "a parked read outlives its confirmation deadline and is redirected"
            );
        }
    }

    fn snapshot_offer_skipped(&self, _node: NodeId, _offered: Slot) {
        let mut st = self.state();
        reach_once!(
            st.offer_skipped,
            "the driver skips a mismatched snapshot offer"
        );
    }

    fn delivery_failed(&self, _node: NodeId, _to: NodeId) {
        let mut st = self.state();
        st.delivery_failures += 1;
        reach_once!(st.delivery_failed, "a peer delivery RPC fails or times out");
    }

    fn waiters_cleared(&self, _node: NodeId, _writes: u64, _reads: u64) {
        let mut st = self.state();
        reach_once!(
            st.waiters_cleared,
            "a deposed leader clears client replies it still held"
        );
    }

    fn edge_rejected(&self, _node: NodeId, _kind: EdgeRejection) {
        let mut st = self.state();
        st.edge_rejections += 1;
        reach_once!(
            st.edge_rejected,
            "the gRPC edge rejects a corrupted request"
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
        node: NodeId,
        _at: Slot,
        chunks: u64,
        bytes: u64,
        blob_bytes: u64,
    ) {
        let mut st = self.state();
        if st.withheld_from.contains(&node.0) {
            // The `withhold_snap_chunk` recovery half: the silence cost this
            // requester beats or a second custodian, and it still repaired.
            reach_once!(
                st.repaired_after_withhold,
                "a requester repairs its snapshot chunks after a custodian withheld one"
            );
        }
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

    fn prepare_below_floor(&self, node: NodeId, _from_slot: Slot, _floor: Slot) {
        let mut st = self.state();
        *st.below_floor_refusals.entry(node.0).or_insert(0) += 1;
        // Rare (only a lagging node below a compacted peer's floor triggers it),
        // so reachable-only: it must be hit at least once across exploration,
        // not on every seed.
        reach_once!(
            st.prepare_below_floor,
            "a candidate prepares below a peer's compaction floor"
        );
    }

    // ---- the matchmaker registry (see `matchmaker`) -------------------------

    #[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.0))]
    fn matchmaker_recovered(
        &self,
        matchmaker: MatchmakerId,
        registry: &[(Ballot, AcceptorConfig)],
        gc_watermark: Ballot,
    ) {
        self.state()
            .matchmaker
            .recovered(matchmaker, registry, gc_watermark);
    }

    fn match_registered(&self, matchmaker: MatchmakerId, ballot: Ballot, config: &AcceptorConfig) {
        let mut st = self.state();
        st.matchmaker.registered(matchmaker, ballot, config);
        // The per-ballot configuration the quorum oracles count over: bound
        // at its durable registration, before any leader could exercise it.
        st.bind_config(ballot, config);
    }

    fn gc_watermark_raised(&self, matchmaker: MatchmakerId, watermark: Ballot) {
        self.state()
            .matchmaker
            .watermark_raised(matchmaker, watermark);
    }

    fn match_replied(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        history: &[(Ballot, AcceptorConfig)],
        gc_watermark: Ballot,
    ) {
        self.state()
            .matchmaker
            .replied(matchmaker, to, ballot, history, gc_watermark);
    }

    // ---- the leader-side matchmaking phase (#120) and reconfiguration (#122) ----

    fn matchmaking_started(
        &self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        reconfiguration: bool,
    ) {
        self.state()
            .matchmaker
            .campaign_started(node, ballot, config, reconfiguration);
    }

    fn match_request_sent(&self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {
        self.state()
            .matchmaker
            .request_sent(node, matchmaker, ballot);
    }

    fn matchmaking_timeout(&self, node: NodeId, ballot: Ballot, _count: u64) {
        self.state().matchmaker.clock_reasked(node, ballot);
    }

    fn matchmaking_resend_skipped(&self, _node: NodeId) {
        self.state().matchmaker.resend_skipped();
    }

    fn match_registered_by(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
    ) {
        self.state()
            .matchmaker
            .registered_by(node, matchmaker, ballot, remaining);
    }

    fn matchmaking_completed(
        &self,
        node: NodeId,
        ballot: Ballot,
        prior: &[AcceptorConfig],
        watermark: Ballot,
        registered_by: usize,
        disagreements: u64,
    ) {
        let mut st = self.state();
        st.matchmaker
            .completed(node, ballot, prior, watermark, registered_by, disagreements);
        st.note_prior(node.0, ballot, prior);
    }

    fn matchmaking_refused(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        self.state()
            .matchmaker
            .campaign_refused(node, ballot, refusal);
    }

    fn campaign_skipped_non_member(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.non_member_campaign_skipped,
            "reconfiguration: a node outside the acceptor set declines to campaign"
        );
    }

    fn non_member_leader_resigned(&self, _node: NodeId, _count: u64) {
        let mut st = self.state();
        reach_once!(
            st.non_member_leader_resigned,
            "reconfiguration: a leader its own reconfiguration removed resigns"
        );
    }

    fn reconfigure_acked(&self, node: NodeId, _members: &[NodeId], result: ReconfigureResult) {
        let mut st = self.state();
        match result {
            ReconfigureResult::Started(_) => {
                assert_always!(
                    st.matchmaker.has_matchmakers(),
                    "reconfiguration: a deployment without matchmakers never starts one",
                    { "node" => node.0 }
                );
                reach_once!(
                    st.reconfigure_started,
                    "reconfiguration: a reconfiguration request is started"
                );
            }
            ReconfigureResult::Refused(_) | ReconfigureResult::NotLeader(_) => {
                reach_once!(
                    st.reconfigure_refused,
                    "reconfiguration: a reconfiguration request is refused or redirected"
                );
            }
        }
    }

    fn match_refused(
        &self,
        matchmaker: MatchmakerId,
        _to: NodeId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        self.state().matchmaker.refused(matchmaker, ballot, refusal);
    }

    fn matchmaker_crashed(&self, _matchmaker: MatchmakerId, seam: Seam) {
        self.state().matchmaker.crashed(seam);
    }

    fn match_reply_dropped(&self, _matchmaker: MatchmakerId) {
        self.state().matchmaker.reply_dropped();
    }

    fn matchmaker_storage_fault(
        &self,
        matchmaker: MatchmakerId,
        _error: &StorageError,
        decision: StorageFaultDecision,
    ) {
        // The sim's matchmaker store injects no faults (#119: the registry
        // rides the generic record contract, with no fault story of its
        // own), so a fault here is an uninjected detection — a bug.
        assert_always!(
            false,
            "matchmaker: no storage fault is ever surfaced by the sim's registry store",
            { "matchmaker" => matchmaker.0, "decision" => format!("{decision:?}") }
        );
    }
}
