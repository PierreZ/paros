//! The sim-side [`Audit`] implementation: one shared incremental checker.
//!
//! This is where the paros correctness invariants live. The driver reports each
//! externally meaningful transition exactly once, at the instant it happens, so
//! every check here is **O(1) in the size of the run** — a map probe and a
//! comparison — rather than a re-scan of a growing event stream.
//!
//! Layout mirrors `crate::world`: one [`AuditWorld`] per simulation iteration,
//! published under a well-known [`StateHandle`](moonpool_sim::StateHandle)
//! key so every node process and every workload reaches the same instance,
//! and factory-created per iteration so recipe replay is exact. Each node
//! wraps it in a [`NodeAudit`], which stamps simulated time on the
//! observations that need it.
//!
//! - [`world`] holds the [`AuditWorld`] API every node and workload reaches
//!   it through, and the run's final judgement;
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

/// How long an unserved read's floor is kept, in simulated ms (#260): far
/// past any read's TTL, so a dropped floor is a read that can no longer be
/// served, never one the check skips.
const READ_FLOOR_SPAN_MS: u64 = 120_000;

mod client;
pub(crate) mod installs;
mod journal_model;
pub(crate) mod journals;
mod linearizability;
pub(crate) mod liveness;
mod losses;
mod machine;
mod matchmaker;
mod matchmaking;
mod net;
mod node;
mod proxy_leader;
mod replica;
mod state;
pub(crate) mod system;
pub(crate) mod tenants;
mod world;

pub(crate) use client::{ClientHistory, check_control_history};
pub(crate) use journal_model::AckedWrite;
pub(crate) use linearizability::{Attempt, Call, Seen};
pub(crate) use world::{
    AuditWorld, audit_world, audit_world_for, audit_world_named, check_every_world, check_run,
};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use moonpool_sim::{TimeProvider, assert_always, assert_reachable};
use paros::client::{MatchmakersRefusal, RetireRefusal};
use paros::core::collector::GcStep;
use paros::core::contract::Party;
use paros::core::journal::{Command, JournalIdentifier};
use paros::core::matchmaker::{
    GcAck, MatchRefusal, MatchmakerHardState, MatchmakerPhase, PendingBootstrap, ReconfigureReply,
    ReconfigureRequest, ReconfigurerStep, Registration, RegistrationKind,
};
use paros::core::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet, ProxyId};
use paros::core::node::{Handoff, ReconfigureResult};
use paros::core::{Ballot, Message, NodeId, Slot};
use paros::net::EdgeRejection;
use paros::observe::audit::{Audit, Deployment, HistoryPage, LogReadReport, StorageFaultDecision};
use paros::roles::node::{BootRefusal, command_hash, message_kind};
use paros::storage::StorageError;

use self::state::AuditState;

/// One node's view of the shared checker, handed to `paros::roles::node::run_node`; it stamps simulated time on
/// the observations that need it and forwards everything else unchanged.
///
/// The driver hands each peer-delivery task its own handle to the audit (the
/// bounded-mailbox drops happen inside those tasks); every clone shares the
/// one per-iteration [`AuditWorld`].
#[derive(Clone)]
pub(crate) struct NodeAudit<T> {
    time: T,
    world: Arc<AuditWorld>,
    /// The journal this port reports for and the run's cross-journal board
    /// (#188); `None` for a port outside the journal plane (a matchmaker).
    journal: Option<(JournalIdentifier, Arc<Mutex<journals::JournalBoard>>)>,
    /// The run's system-journal board (#189), on a node that follows the
    /// system journals, and the registry it reports (#650).
    system: Option<(Arc<Mutex<system::SystemBoard>>, system::Plane)>,
    /// The run's tenant board (#210), on a machine that follows the tenant
    /// control journals.
    tenants: Option<Arc<Mutex<tenants::TenantBoard>>>,
    /// The run's machine board (#246), on a machine's own port.
    machines: Option<(
        Arc<Mutex<crate::machine::MachineBoard>>,
        paros::model::address::Address,
    )>,
    /// The run's state, on a machine's own port: where a wiped machine is
    /// signaled healed (moonpool #344 (rolling wipes), `crate::machine::heal`).
    wipes: Option<moonpool_sim::StateHandle>,
    /// The observer of the node's own client's calls (the cell
    /// coordinator's, #240), handed to `paros` through
    /// [`Audit::call_observer`].
    calls: Option<Arc<dyn paros::client::CallObserver>>,
    /// The run's state and the machine's process IP, on a machine's journal
    /// port: a commit of a `Batched` journal registers there for the cut
    /// budget (#599 (durability mode per journal), `crate::world::cut`).
    cut: Option<(moonpool_sim::StateHandle, String)>,
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
        Self {
            time,
            world,
            journal: None,
            system: None,
            tenants: None,
            machines: None,
            wipes: None,
            calls: None,
            cut: None,
        }
    }

    /// This port registers each commit of a `Batched` journal its machine
    /// at `ip` runs in `state`'s cut budget (#599 (durability mode per
    /// journal)).
    pub(crate) fn with_cut_budget(mut self, state: moonpool_sim::StateHandle, ip: String) -> Self {
        self.cut = Some((state, ip));
        self
    }

    /// This port also reports the tenant control journals' folds and the
    /// appends they judge to `board` (#210).
    pub(crate) fn with_tenants(mut self, board: Arc<Mutex<tenants::TenantBoard>>) -> Self {
        self.tenants = Some(board);
        self
    }

    /// This port also hands `observer` to the node's own client (#240).
    pub(crate) fn with_calls(mut self, observer: Arc<dyn paros::client::CallObserver>) -> Self {
        self.calls = Some(observer);
        self
    }

    /// This port also reports a machine's lifecycle to the machine board
    /// of `state` (#246), and signals there when a wiped machine heals
    /// (moonpool #344 (rolling wipes)).
    pub(crate) fn on_machines(
        mut self,
        state: &moonpool_sim::StateHandle,
        addr: paros::model::address::Address,
    ) -> Self {
        self.machines = Some((crate::machine::machine_board(state), addr));
        self.wipes = Some(state.clone());
        self
    }

    /// This port also reports the system journals' folds and their effects
    /// to `board` (#189).
    pub(crate) fn with_system(self, board: Arc<Mutex<system::SystemBoard>>) -> Self {
        self.on_registry(board, system::Plane::Harness)
    }

    /// This port reports the registry `plane` names to `board`: a machine's
    /// port reports its cell control journal's folds, its record and its
    /// cache (#650).
    pub(crate) fn on_registry(
        mut self,
        board: Arc<Mutex<system::SystemBoard>>,
        plane: system::Plane,
    ) -> Self {
        self.system = Some((board, plane));
        self
    }

    /// The board, on a port that reports to it, with the port's plane.
    fn registry_board(&self) -> Option<(MutexGuard<'_, system::SystemBoard>, system::Plane)> {
        self.system
            .as_ref()
            .map(|(board, plane)| (system::lock(board), *plane))
    }

    /// The board, on a port that reports the harness registry: the joiner
    /// facts are its alone (#650).
    fn system_board(&self) -> Option<MutexGuard<'_, system::SystemBoard>> {
        self.registry_board()
            .and_then(|(board, plane)| matches!(plane, system::Plane::Harness).then_some(board))
    }

    /// This port reports for `journal` (#188): the non-interference oracles
    /// on `board` see its applies, its sends and its quarantines.
    pub(crate) fn in_journal(
        mut self,
        journal: JournalIdentifier,
        board: Arc<Mutex<journals::JournalBoard>>,
    ) -> Self {
        journals::lock(&board).serves(journal);
        self.journal = Some((journal, board));
        self
    }

    /// Whether this port reports a journal the machines host (#652): the
    /// registry, the universe, election and tenant control journals and
    /// every created journal.
    fn on_machine(&self) -> bool {
        self.system
            .as_ref()
            .is_some_and(|(_, plane)| matches!(plane, system::Plane::Machine(..)))
    }

    /// The non-interference half of an apply (#188): once the run serves
    /// more than one journal, a `Write`, `Truncate` or `SetLeader` (hashing
    /// to `command`; `None` for a `Noop`) must be one a client sent to this
    /// journal (#652: the machines host many journals in one process on
    /// every seed), and the board learns which journal committed while a
    /// sibling was held or quarantined.
    fn journal_applied(&self, node: NodeId, command: Option<u64>) {
        let Some((journal, board)) = &self.journal else {
            return;
        };
        let mut board = journals::lock(board);
        board.applied_under_parent(node.0, *journal);
        if !board.judges_slots() {
            return;
        }
        if let Some(vhash) = command {
            assert_always!(
                self.state().appended.contains(&vhash),
                "journal: a slot holds only a command appended to its own journal",
                { "node" => node.0, "journal" => journal.to_string(), "command" => vhash }
            );
            if self.on_machine() {
                assert_reachable!(
                    "journal: a machine-hosted journal's slot is judged by its appends"
                );
            }
        }
        if !board.is_multi() {
            return;
        }
        let in_chaos = crate::in_chaos();
        board.applied(*journal, in_chaos);
    }

    /// The chaos window must hold a working cell, or every write,
    /// reconfiguration and crash-hint site stays silent (#495).
    fn elected_in_chaos(&self) {
        if crate::in_chaos() {
            let mut st = self.state();
            reach_once!(
                st.elected_in_chaos,
                "a leader is elected inside the chaos window"
            );
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.time.now().as_millis()).unwrap_or(u64::MAX)
    }

    fn state(&self) -> MutexGuard<'_, AuditState> {
        self.world.lock()
    }

    /// Count one message leaving a node or a proxy, by kind.
    fn count_sent(&self, msg: &Message) {
        *self
            .state()
            .sent_kinds
            .entry(message_kind(msg))
            .or_default() += 1;
    }

    /// Persist-before-send, observed at the send seam: the two replies whose
    /// meaning is a durable fact must find that fact already folded — an
    /// `Accepted` its sender's durable accept, a `Commit` a quorum decision
    /// on the tally (see [`AuditWorld::check_final_convergence`]). `from` is
    /// a node, or the proxy leader that decided the `Commit` (#142); only a
    /// node ever sends an `Accepted`.
    ///
    /// A `Commit` on the wire, whoever sends it, names a decided slot, and
    /// where the durable-accept tally already knows the decision it carries
    /// that value. Checked against `decided`, never the apply-fed `chosen`
    /// map: a #94 re-chosen identity applies as a `Noop` everywhere while its
    /// commit honestly carries the decided user command.
    ///
    /// Below the cluster-wide compaction floor the per-node tally is pruned
    /// (`AuditState::prune_below_floor`), and the witness there is a
    /// durable accept of that `(slot, ballot)` (`AuditState::accepted`,
    /// never pruned): the slot is decided on every node by then. One batch
    /// can carry an acceptor's re-answered `Accept`, the `Commit` that
    /// decides the slot and the `Truncate` its walk then reaches: its durable
    /// report raises the last floor past the slot and prunes the tally just
    /// before its `Accepted` leaves. Group commit (#613 (settle off the node
    /// loop)) makes that batch common: an `Accept` and its `Commit` held
    /// back during one flush are stepped together. Witness
    /// 5484246127051667688: node 0 sent `Accepted` for slot 60 at round 14
    /// from a batch `[AppendAccepted(60), SetChosenIndex(60), Truncate(61)]`
    /// whose accept was reported durable first.
    fn observe_durable_send(&self, from: Party, msg: &Message) {
        if let Message::Accepted { ballot, slot, .. } = msg {
            let st = self.state();
            let node = from.node().map(|n| n.0);
            let holds = st
                .accept_sets
                .get(&(slot.0, ballot.round, ballot.node.0))
                .is_some_and(|holders| node.is_some_and(|n| holders.contains(&n)));
            // Below the cluster-wide floor the tally is pruned
            // (`AuditState::prune_below_floor`), so it cannot name the
            // record. A batch can accept a slot, walk its chosen index past
            // it and compact it, all before its `Accepted` leaves: the batch
            // reports the accept, then the truncation that prunes it (seed
            // 6657126035707192045 on 25d00a7: a heal-path accept of slot 3
            // and a floor raised to 5 in one batch). The record was durable;
            // the witnesses left are the ones pruning keeps: a decided slot
            // the sender's own floor passed, a durable accept reported at
            // exactly this ballot, and a durable copy on the sender.
            let compacted = !holds
                && slot.0 < st.cluster_min_floor()
                && st.decided_vhash(slot.0).is_some()
                && st
                    .accepted
                    .contains_key(&(slot.0, ballot.round, ballot.node.0))
                && node.is_some_and(|n| {
                    st.floor.get(&n).is_some_and(|f| f.now > slot.0)
                        && st.persisted.contains_key(&(n, slot.0))
                });
            if compacted {
                assert_reachable!("an Accepted leaves for a slot its own batch compacted");
            }
            assert_always!(
                holds || compacted,
                "an outgoing Accepted names a durably accepted record",
                { "from" => from.to_string(), "slot" => slot.0, "round" => ballot.round }
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
            // ballot re-decides only the same value). Below the cluster-wide
            // compaction floor the per-slot tally is pruned, and the witness
            // there is the decided vhash the pruning kept
            // (`AuditState::decided_vhash`) — the consensus decision, never
            // the apply-fed `chosen` map, whose entry for a #94 re-chosen
            // identity is the `Noop` it applied as while its `Commit`
            // honestly carries the decided user command. A proxy leader has
            // no floor and never learns a slot chosen, so its `Commit` for a
            // round whose votes arrived late can trail the whole cluster's
            // truncation (seed 17112434982126988317: slot 34 committed by a
            // proxy at a cluster floor of 38 — harmless, every learner
            // ignores it).
            let st = self.state();
            let vhash = command_hash(command);
            let decided = st.decided_vhash(slot.0);
            // The cluster floor is an O(nodes) walk, so it is taken only on
            // failure: `assert_always!` evaluates its detail map lazily.
            assert_always!(
                decided.is_some(),
                "an outgoing Commit names a slot a durable accept quorum already decided",
                {
                    "from" => from.to_string(),
                    "slot" => slot.0,
                    "round" => ballot.round,
                    "min_floor" => st.cluster_min_floor()
                }
            );
            assert_always!(
                decided.is_none_or(|decided_vhash| decided_vhash == vhash),
                "an outgoing Commit carries the quorum-decided value",
                { "from" => from.to_string(), "slot" => slot.0, "round" => ballot.round }
            );
            if let Some(&(_, _, decided_vhash)) = st.decided.get(&slot.0) {
                assert_always!(
                    vhash == decided_vhash,
                    "a commit carries the chosen value",
                    { "from" => from.to_string(), "slot" => slot.0 }
                );
            }
        }
    }
}

impl<T: TimeProvider> Audit for NodeAudit<T> {
    fn call_observer(&self) -> Option<Arc<dyn paros::client::CallObserver>> {
        self.calls.clone()
    }

    fn promised(&self, node: NodeId, ballot: Ballot) {
        NodeAudit::promised(self, node, ballot);
    }

    fn accepted(&self, node: NodeId, slot: Slot, ballot: Ballot, promised: Ballot, vhash: u64) {
        NodeAudit::accepted(self, node, slot, ballot, promised, vhash);
    }

    fn floor_requested(&self, node: NodeId, first: Slot) {
        NodeAudit::floor_requested(self, node, first);
    }

    fn commit_started(&self, node: NodeId, writes: &[paros::core::WriteOp]) {
        NodeAudit::commit_started(self, node, writes);
    }

    fn commit_ended(&self, node: NodeId) {
        NodeAudit::commit_ended(self, node);
    }

    fn truncated(&self, node: NodeId, first: Slot) {
        NodeAudit::truncated(self, node, first);
    }

    fn trimmed_to(&self, node: NodeId, point: Slot) {
        NodeAudit::trimmed_to(self, node, point);
    }

    fn applied(
        &self,
        node: NodeId,
        slot: Slot,
        vhash: u64,
        command: &Command,
        outcome: Option<&paros::core::journal::Outcome>,
    ) {
        NodeAudit::applied(self, node, slot, vhash, command, outcome);
    }

    fn journal_quarantined(&self, node: NodeId) {
        NodeAudit::journal_quarantined(self, node);
    }

    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {
        NodeAudit::sent(self, node, to, msg);
    }

    fn sent_to_proxy(&self, node: NodeId, proxy: ProxyId, msg: &Message) {
        NodeAudit::sent_to_proxy(self, node, proxy, msg);
    }

    fn proxy_sent(&self, proxy: ProxyId, to: NodeId, msg: &Message) {
        NodeAudit::proxy_sent(self, proxy, to, msg);
    }

    fn replica_booted(&self, replica: NodeId, chosen_index: Option<Slot>, floor: Slot) {
        NodeAudit::replica_booted(self, replica, chosen_index, floor);
    }

    fn delegation_taken_back(&self, node: NodeId, slot: Slot, proxy: ProxyId) {
        NodeAudit::delegation_taken_back(self, node, slot, proxy);
    }

    fn proxy_fanned_out(
        &self,
        proxy: ProxyId,
        leader: NodeId,
        slot: Slot,
        ballot: Ballot,
        vhash: u64,
        column: Option<usize>,
        addressees: usize,
    ) {
        NodeAudit::proxy_fanned_out(self, proxy, leader, slot, ballot, vhash, column, addressees);
    }

    fn proxy_decided(&self, proxy: ProxyId, slot: Slot, ballot: Ballot, vhash: u64) {
        NodeAudit::proxy_decided(self, proxy, slot, ballot, vhash);
    }

    fn proxy_round_expired(&self, proxy: ProxyId, slot: Slot) {
        NodeAudit::proxy_round_expired(self, proxy, slot);
    }

    fn elected(
        &self,
        node: NodeId,
        won: Ballot,
        promised: Ballot,
        gap_fills: u64,
        config: &AcceptorConfig,
    ) {
        NodeAudit::elected(self, node, won, promised, gap_fills, config);
    }

    fn stepped_down(&self, node: NodeId) {
        NodeAudit::stepped_down(self, node);
    }

    fn authority_relinquished(&self, node: NodeId, handoff: Handoff) {
        NodeAudit::authority_relinquished(self, node, handoff);
    }

    fn authority_installed(
        &self,
        node: NodeId,
        from: NodeId,
        ballot: Ballot,
        next_slot: Slot,
        tail: u64,
    ) {
        NodeAudit::authority_installed(self, node, from, ballot, next_slot, tail);
    }

    fn handoff_refused(&self, node: NodeId, target: u64, stale: u64, shape: u64, unfit: u64) {
        NodeAudit::handoff_refused(self, node, target, stale, shape, unfit);
    }

    fn handoff_fence_expired(&self, node: NodeId, count: u64) {
        NodeAudit::handoff_fence_expired(self, node, count);
    }

    fn chosen_gap(&self, node: NodeId, hole: Slot, above: Slot) {
        NodeAudit::chosen_gap(self, node, hole, above);
    }

    fn answered(
        &self,
        node: NodeId,
        slot: Slot,
        command: &Command,
        outcome: &paros::core::journal::Outcome,
    ) {
        NodeAudit::answered(self, node, slot, command, outcome);
    }

    fn chosen_index(&self, node: NodeId, index: Slot) {
        NodeAudit::chosen_index(self, node, index);
    }

    fn log_read_served(&self, node: NodeId, report: &LogReadReport<'_>) {
        NodeAudit::log_read_served(self, node, report);
    }

    fn log_read_not_held(&self, node: NodeId, from: paros::core::journal::Seq) {
        NodeAudit::log_read_not_held(self, node, from);
    }

    fn journal_refused(&self, node: NodeId, journal: JournalIdentifier, call: &'static str) {
        NodeAudit::journal_refused(self, node, journal, call);
    }

    fn system_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        event: &paros::model::registry::SystemEvent,
    ) {
        NodeAudit::system_folded(self, node, journal, seq, event);
    }

    fn checkpoint_folded(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        seq: u64,
        verified: Option<bool>,
        state: &paros::model::registry::Registry,
    ) {
        NodeAudit::checkpoint_folded(self, node, journal, seq, verified, state);
    }

    fn journal_held(&self, node: NodeId) {
        NodeAudit::journal_held(self, node);
    }

    fn journal_started(&self, node: NodeId, journal: JournalIdentifier) {
        NodeAudit::journal_started(self, node, journal);
    }

    fn journal_stopped(&self, node: NodeId, journal: JournalIdentifier) {
        NodeAudit::journal_stopped(self, node, journal);
    }

    fn unpooled_message(&self, node: NodeId, journal: JournalIdentifier, from: NodeId) {
        NodeAudit::unpooled_message(self, node, journal, from);
    }

    fn pool_admitted(&self, node: NodeId, admitted: NodeId) {
        NodeAudit::pool_admitted(self, node, admitted);
    }

    fn quorum_read_opened(&self, node: NodeId, ctx: u64) {
        NodeAudit::quorum_read_opened(self, node, ctx);
    }

    fn quorum_read_served(
        &self,
        node: NodeId,
        ctx: u64,
        row: Option<usize>,
        watermark: Option<Slot>,
        served: Option<Slot>,
        opened: Option<Slot>,
        leader: bool,
    ) {
        NodeAudit::quorum_read_served(self, node, ctx, row, watermark, served, opened, leader);
    }

    fn recovered(
        &self,
        node: NodeId,
        promised: Ballot,
        chosen_index: Option<Slot>,
        floor: Slot,
        deployment: &Deployment,
        accepted: &[(Slot, Ballot, u64)],
    ) {
        NodeAudit::recovered(
            self,
            node,
            promised,
            chosen_index,
            floor,
            deployment,
            accepted,
        );
    }

    fn storage_fault(&self, node: NodeId, error: &StorageError, decision: StorageFaultDecision) {
        NodeAudit::storage_fault(self, node, error, decision);
    }

    fn boot_refused(&self, node: NodeId, refusal: BootRefusal) {
        NodeAudit::boot_refused(self, node, refusal);
    }

    fn machine_booted(
        &self,
        addr: &paros::model::address::Address,
        record: Option<&paros::machine::MachineRecord>,
    ) {
        NodeAudit::machine_booted(self, addr, record);
    }

    fn call_received(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        call: paros::observe::metrics::CallKind,
    ) {
        NodeAudit::call_received(self, node, journal, call);
    }

    fn call_answered(
        &self,
        node: NodeId,
        journal: JournalIdentifier,
        call: (
            paros::observe::metrics::CallKind,
            paros::observe::metrics::CallOutcome,
        ),
        written: u64,
        read: u64,
    ) {
        NodeAudit::call_answered(self, node, journal, call, written, read);
    }

    fn machine_recorded(&self, record: &paros::machine::MachineRecord) {
        NodeAudit::machine_recorded(self, record);
    }

    fn registry_cached(&self, node: NodeId, cache: &paros::machine::CachedRegistry, durable: bool) {
        NodeAudit::registry_cached(self, node, cache, durable);
    }

    fn registry_cache_read(&self, node: NodeId, cache: Option<&paros::machine::CachedRegistry>) {
        NodeAudit::registry_cache_read(self, node, cache);
    }

    fn cell_formatting(
        &self,
        addr: &paros::model::address::Address,
        node: NodeId,
        plan: &paros::machine::CellPlan,
        leftovers: bool,
    ) {
        NodeAudit::cell_formatting(self, addr, node, plan, leftovers);
    }

    fn dropped_at_send(&self, from: Party, to: Party, msg: &Message) {
        NodeAudit::dropped_at_send(self, from, to, msg);
    }

    fn duplicated_at_send(&self, from: Party, to: Party, msg: &Message) {
        NodeAudit::duplicated_at_send(self, from, to, msg);
    }

    fn client_reply_dropped(&self, node: NodeId, reply: paros::observe::reply::Reply) {
        NodeAudit::client_reply_dropped(self, node, reply);
    }

    fn dropped_at_mailbox(&self, from: Party, to: Party, kind: &'static str) {
        NodeAudit::dropped_at_mailbox(self, from, to, kind);
    }

    fn read_expired(&self, node: NodeId, early: bool) {
        NodeAudit::read_expired(self, node, early);
    }

    fn delivery_failed(&self, from: Party, to: Party) {
        NodeAudit::delivery_failed(self, from, to);
    }

    fn waiters_cleared(&self, node: NodeId, calls: u64) {
        NodeAudit::waiters_cleared(self, node, calls);
    }

    fn edge_rejected(&self, at: Party, kind: EdgeRejection) {
        NodeAudit::edge_rejected(self, at, kind);
    }

    fn election_timeout_set(&self, node: NodeId, ticks: u64) {
        NodeAudit::election_timeout_set(self, node, ticks);
    }

    fn heartbeat_ack_received(&self, node: NodeId, from: NodeId, ballot: Ballot) {
        NodeAudit::heartbeat_ack_received(self, node, from, ballot);
    }

    fn ticked(&self, node: NodeId) {
        NodeAudit::ticked(self, node);
    }

    fn election_backoff(&self, node: NodeId, doublings: u32) {
        NodeAudit::election_backoff(self, node, doublings);
    }

    fn waiter_superseded(&self, node: NodeId, slot: Slot) {
        NodeAudit::waiter_superseded(self, node, slot);
    }

    fn quorum_lost(&self, node: NodeId, count: u64) {
        NodeAudit::quorum_lost(self, node, count);
    }

    fn watermark_filled(&self, node: NodeId, count: u64) {
        NodeAudit::watermark_filled(self, node, count);
    }

    fn faulty_reported(&self, node: NodeId, entries: &[(Slot, Ballot)]) {
        NodeAudit::faulty_reported(self, node, entries);
    }

    fn repair_progress(
        &self,
        node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
    ) {
        NodeAudit::repair_progress(self, node, repaired, case1, case2, step_downs);
    }

    fn recovery_batch(
        &self,
        node: NodeId,
        started: u64,
        gap_fills: u64,
        remaining: u64,
        page: u64,
    ) {
        NodeAudit::recovery_batch(self, node, started, gap_fills, remaining, page);
    }

    fn prepare_below_floor(&self, node: NodeId, from_slot: Slot, floor: Slot) {
        NodeAudit::prepare_below_floor(self, node, from_slot, floor);
    }

    fn matchmaker_recovered(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        scalars: &paros::core::matchmaker::MatchmakerHardState,
        registry: &BTreeMap<Ballot, Registration>,
        gc_watermark: Ballot,
    ) {
        NodeAudit::matchmaker_recovered(
            self,
            matchmaker,
            set,
            phase,
            scalars,
            registry,
            gc_watermark,
        );
    }

    fn matchmaker_in_flight(
        &self,
        matchmaker: MatchmakerId,
        writes: Option<&[paros::core::matchmaker::MatchmakerWriteOp]>,
    ) {
        NodeAudit::matchmaker_in_flight(self, matchmaker, writes);
    }

    fn matchmaker_scalars_persisted(
        &self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
        NodeAudit::matchmaker_scalars_persisted(self, matchmaker, scalars);
    }

    fn reconfigurer_reconstructed(
        &self,
        node: NodeId,
        generation: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
        NodeAudit::reconfigurer_reconstructed(self, node, generation, bootstrap, disagreements);
    }

    fn matchmaker_activated(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        effective: Option<&(Ballot, AcceptorConfig)>,
        registry: &BTreeMap<Ballot, Registration>,
    ) {
        NodeAudit::matchmaker_activated(self, matchmaker, set, gc_watermark, effective, registry);
    }

    fn matchmaker_reconfigure_replied(
        &self,
        matchmaker: MatchmakerId,
        request: &ReconfigureRequest,
        reply: &ReconfigureReply,
    ) {
        NodeAudit::matchmaker_reconfigure_replied(self, matchmaker, request, reply);
    }

    fn matchmaker_gc_replied(&self, matchmaker: MatchmakerId, ack: &GcAck) {
        NodeAudit::matchmaker_gc_replied(self, matchmaker, ack);
    }

    fn match_registered(
        &self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        NodeAudit::match_registered(self, matchmaker, ballot, registration);
    }

    fn gc_watermark_raised(&self, matchmaker: MatchmakerId, watermark: Ballot) {
        NodeAudit::gc_watermark_raised(self, matchmaker, watermark);
    }

    fn match_replied(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        generation: u64,
        page: &HistoryPage<'_>,
    ) {
        NodeAudit::match_replied(self, matchmaker, to, ballot, generation, page);
    }

    fn matchmaking_started(
        &self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        kind: RegistrationKind,
        generation: u64,
    ) {
        NodeAudit::matchmaking_started(self, node, ballot, config, kind, generation);
    }

    fn gc_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        generation: u64,
        watermark: Ballot,
        fence: Option<Slot>,
    ) {
        NodeAudit::gc_request_sent(self, node, matchmaker, generation, watermark, fence);
    }

    fn gc_step(&self, node: NodeId, matchmaker: MatchmakerId, ack: &GcAck, step: &GcStep) {
        NodeAudit::gc_step(self, node, matchmaker, ack, step);
    }

    fn matchmakers_learned(&self, node: NodeId, set: &MatchmakerSet) {
        NodeAudit::matchmakers_learned(self, node, set);
    }

    fn reconfigurer_started(&self, node: NodeId, old: &MatchmakerSet, target: &[MatchmakerId]) {
        NodeAudit::reconfigurer_started(self, node, old, target);
    }

    fn reconfigurer_aborted(&self, node: NodeId) {
        NodeAudit::reconfigurer_aborted(self, node);
    }

    fn reconfigurer_backoff(&self, node: NodeId, ticks: u64) {
        NodeAudit::reconfigurer_backoff(self, node, ticks);
    }

    fn reconfigure_matchmakers_acked(&self, node: NodeId, refusal: Option<MatchmakersRefusal>) {
        NodeAudit::reconfigure_matchmakers_acked(self, node, refusal);
    }

    fn reconfigurer_step(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
        NodeAudit::reconfigurer_step(self, node, matchmaker, reply, step);
    }

    fn successor_republished(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        successor: &MatchmakerSet,
    ) {
        NodeAudit::successor_republished(self, node, matchmaker, successor);
    }

    fn retire_acked(&self, node: NodeId, refusal: Option<RetireRefusal>) {
        NodeAudit::retire_acked(self, node, refusal);
    }

    fn retired(&self, node: NodeId) {
        NodeAudit::retired(self, node);
    }

    fn membership_probe_opened(
        &self,
        node: NodeId,
        ballot: Ballot,
        believed: &AcceptorConfig,
        generation: u64,
    ) {
        NodeAudit::membership_probe_opened(self, node, ballot, believed, generation);
    }

    fn membership_probe_closed(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
        answered_by: usize,
    ) {
        NodeAudit::membership_probe_closed(self, node, ballot, effective, member, answered_by);
    }

    fn membership_probe_late(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
    ) {
        NodeAudit::membership_probe_late(self, node, ballot, effective, member);
    }

    fn match_request_sent(&self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {
        NodeAudit::match_request_sent(self, node, matchmaker, ballot);
    }

    fn matchmaking_stale_configuration(&self, node: NodeId, ballot: Ballot, newest: Ballot) {
        NodeAudit::matchmaking_stale_configuration(self, node, ballot, newest);
    }

    fn matchmaking_timeout(&self, node: NodeId, ballot: Ballot, count: u64) {
        NodeAudit::matchmaking_timeout(self, node, ballot, count);
    }

    fn match_registered_by(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        remaining: usize,
        watermark: Ballot,
        history_hash: u64,
    ) {
        NodeAudit::match_registered_by(
            self,
            node,
            matchmaker,
            ballot,
            remaining,
            watermark,
            history_hash,
        );
    }

    fn match_paged(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        next: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
        NodeAudit::match_paged(
            self,
            node,
            matchmaker,
            ballot,
            next,
            watermark,
            history_hash,
        );
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
        NodeAudit::matchmaking_completed(
            self,
            node,
            ballot,
            prior,
            watermark,
            registered_by,
            disagreements,
        );
    }

    fn matchmaking_refused(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        NodeAudit::matchmaking_refused(self, node, matchmaker, ballot, refusal);
    }

    fn reconfigure_acked(&self, node: NodeId, members: &[NodeId], result: ReconfigureResult) {
        NodeAudit::reconfigure_acked(self, node, members, result);
    }

    fn match_refused(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        NodeAudit::match_refused(self, matchmaker, to, ballot, refusal);
    }

    fn matchmaker_boot_refused(&self, matchmaker: MatchmakerId, refusal: BootRefusal) {
        NodeAudit::matchmaker_boot_refused(self, matchmaker, refusal);
    }

    fn matchmaker_storage_fault(
        &self,
        matchmaker: MatchmakerId,
        error: &StorageError,
        decision: StorageFaultDecision,
    ) {
        NodeAudit::matchmaker_storage_fault(self, matchmaker, error, decision);
    }
}

/// Fold the compaction floor `node`'s boot read back (#496). A batch that
/// raised the floor can land durably and the node crash before it reported
/// the batch (an ambiguous sync): the boot then holds a floor the audit
/// never saw, and every oracle that reads `floor` (the GC licence among
/// them: a slot below a member's floor is held as chosen) must see it. The
/// floor is one the node staged, and it lies inside the chosen prefix the
/// boot recovered. Witness 4373183430197841510: a leader whose own floor
/// had passed the fence asked for GC, and the audit, still at the floor
/// before the crash, found no member holding the fence slot.
fn recover_floor(st: &mut AuditState, node: u64, floor: u64, chosen: Option<Slot>, now: u64) {
    let staged = st.requested_floor.get(&node).copied().unwrap_or(0);
    let reported = st.truncate_watermark.get(&node).copied().unwrap_or(0);
    assert_always!(
        floor <= staged.max(reported),
        "storage: a recovered floor is one its node staged",
        { "node" => node, "floor" => floor, "staged" => staged, "reported" => reported }
    );
    assert_always!(
        floor <= chosen.map_or(0, |c| c.0 + 1),
        "storage: a recovered floor lies inside the recovered chosen prefix",
        { "node" => node, "floor" => floor, "chosen" => crate::signed_watermark(chosen.map(|c| c.0)) }
    );
    let known = st.floor.get(&node).map_or(0, |f| f.now);
    if floor <= known {
        return;
    }
    reach_once!(
        st.boot_floor_unreported,
        "storage: a boot recovers a floor its node never reported"
    );
    st.floor.entry(node).or_default().raise(floor, now);
    st.truncate_watermark.insert(node, floor.max(reported));
    // The fold now knows the higher floor: what every node truncated may
    // be reclaimed, as at a reported truncation.
    st.prune_below_floor();
    assert_always!(
        st.floor.get(&node).is_some_and(|f| f.now == floor),
        "storage: a recovered floor is folded"
    );
}

/// Judge `node`'s pending compaction floor once its walk reported `slot`
/// (#448): every record batch in a slot below the floor ends at or below
/// the highest `first_seq` a decided `Truncate` revealed. A floor that drops
/// a record no truncation released loses acknowledged data, and the client
/// sees it only if a read asks for exactly those records.
fn judge_floor(st: &mut AuditState, node: u64, slot: u64) {
    let Some(&(first, due)) = st.floor_claims.get(&node) else {
        return;
    };
    if slot + 1 < due {
        return;
    }
    st.floor_claims.remove(&node);
    let released = st.journal.released();
    let end = st.journal.end_below(first);
    assert_always!(
        end.is_none_or(|end| end <= released),
        "journal: compaction drops only records a Truncate released",
        {
            "node" => node,
            "first" => first,
            "end" => end.unwrap_or(0),
            "released" => released
        }
    );
    if end.is_some() {
        reach_once!(
            st.floor_judged,
            "journal: a compaction floor is judged against the released records"
        );
    }
}

#[cfg(test)]
mod tests {
    //! Mechanism pins for two oracle rules a review of #142 part B found
    //! missing (their scenarios — a proxy's delayed `Commit` for a compacted
    //! slot, an `Accepted` replied to a proxy — are the campaign's to reach;
    //! these pin the checks themselves, on the state and on the real
    //! node-to-proxy route).

    use std::sync::Arc;
    use std::time::Duration;

    use moonpool_sim::{TimeError, TimeProvider, has_always_violations, reset_always_violations};
    use paros::core::Ballot;
    use paros::core::Message;
    use paros::core::NodeId;
    use paros::core::Slot;
    use paros::core::membership::ProxyId;

    use super::state::{AuditState, Floor};
    use super::{AuditWorld, NodeAudit};

    /// A clock that never moves: the audit reads it only to stamp floors.
    #[derive(Clone)]
    struct FrozenClock;

    impl TimeProvider for FrozenClock {
        async fn sleep(&self, _duration: Duration) -> Result<(), TimeError> {
            Ok(())
        }

        fn now(&self) -> Duration {
            Duration::ZERO
        }

        async fn timeout<F, T>(&self, _duration: Duration, future: F) -> Result<T, TimeError>
        where
            F: std::future::Future<Output = T> + Send,
            T: Send,
        {
            Ok(future.await)
        }
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    /// Below the cluster-wide floor a `Commit` is judged against the
    /// command consensus decided, kept when the tally was pruned — never
    /// against the applied command, which a #94 re-chosen identity turns
    /// into a `Noop` while its `Commit` honestly carries the user command.
    #[test]
    fn a_below_floor_commit_is_judged_against_the_decided_command_not_the_applied_one() {
        let user = 11;
        let noop = 22;
        let b = ballot(3, 0);
        let mut st = AuditState::default();
        st.decided.insert(5, (b.round, b.node.0, user));
        // The slot applied as a `Noop` everywhere (the at-most-once
        // suppression), then every node truncated past it.
        st.chosen.insert(5, noop);
        st.booted.insert(0);
        st.floor.insert(
            0,
            Floor {
                now: 8,
                ..Floor::default()
            },
        );
        st.prune_below_floor();
        assert!(!st.decided.contains_key(&5), "the tally is reclaimed");
        assert_eq!(st.decided_below_floor.get(&5), Some(&user));
        assert_eq!(st.decided_vhash(5), Some(user));

        reset_always_violations();
        st.observe_proxy_decision(0, 5, b, user);
        assert!(
            !has_always_violations(),
            "the delayed Commit carries the decided user command and is valid"
        );
        st.observe_proxy_decision(0, 5, b, noop);
        assert!(
            has_always_violations(),
            "a Commit carrying anything but the decided command is red"
        );
        reset_always_violations();
    }

    /// The persist-before-send check on an `Accepted` runs on the real
    /// node-to-proxy route: a vote replied to a proxy before its durable
    /// accept was reported is red exactly as one replied to a leader is.
    #[test]
    fn an_accepted_replied_to_a_proxy_needs_its_durable_accept_first() {
        let world = Arc::new(AuditWorld::default());
        let audit = NodeAudit::new(FrozenClock, world);
        let b = ballot(3, 0);
        let vote = Message::Accepted {
            from: NodeId(1),
            ballot: b,
            slot: Slot(4),
            vhash: 9,
        };
        reset_always_violations();
        audit.sent_to_proxy(NodeId(1), ProxyId(0), &vote);
        assert!(
            has_always_violations(),
            "an Accepted to a proxy without its durable accept is red"
        );
        reset_always_violations();
        audit.promised(NodeId(1), b);
        audit.accepted(NodeId(1), Slot(4), b, b, 9);
        audit.sent_to_proxy(NodeId(1), ProxyId(0), &vote);
        assert!(
            !has_always_violations(),
            "once the durable accept is folded the same vote is clean"
        );
    }

    /// A trim-point jump that raises the cluster's minimum floor prunes
    /// below it, and a proxy's late `Commit` for a slot under the floor is
    /// judged against its decided vhash, pruned or not (witness
    /// 15198787338826444353 of PR #551 (operator calls name their journal)
    /// at a65d9cc: a decided command flagged as unbacked).
    #[test]
    fn a_trim_jump_prunes_and_a_late_commit_below_the_floor_is_judged_decided() {
        let user = 11;
        let b = ballot(3, 0);
        let world = Arc::new(AuditWorld::default());
        let audit = NodeAudit::new(FrozenClock, world.clone());
        {
            let mut st = world.lock();
            st.decided.insert(5, (b.round, b.node.0, user));
            st.booted.insert(0);
            st.booted.insert(1);
            st.floor.insert(
                0,
                Floor {
                    now: 8,
                    ..Floor::default()
                },
            );
            st.cluster_applied_max = Some(10);
            st.prune_below_floor();
            assert!(
                st.decided.contains_key(&5),
                "node 1 still at floor 0: nothing pruned"
            );
        }
        reset_always_violations();
        audit.trimmed_to(NodeId(1), Slot(8));
        assert!(!has_always_violations());
        {
            let st = world.lock();
            assert_eq!(st.cluster_min_floor(), 8);
            assert!(
                !st.decided.contains_key(&5),
                "the trim jump prunes below the floor"
            );
            assert_eq!(st.decided_below_floor.get(&5), Some(&user));
        }
        audit.proxy_decided(ProxyId(0), Slot(5), b, user);
        assert!(!has_always_violations(), "a decided command is backed");
        reset_always_violations();
    }
}
