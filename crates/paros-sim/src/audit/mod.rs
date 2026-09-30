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

mod client;
pub(crate) mod journals;
mod matchmaker;
mod state;
mod world;

pub(crate) use client::ClientHistory;
pub(crate) use world::{AuditWorld, audit_world, audit_world_for, check_run};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use moonpool_sim::{TimeProvider, assert_always, assert_reachable};
use paros::{
    AcceptorConfig, Audit, Ballot, BootRefusal, Command, Control, Deployment, EdgeRejection, GcAck,
    GcStep, HANDOFF_BATCH, Handoff, HistoryPage, JournalId, LEADER_RECOVERY_BATCH, LogReadAnswer,
    LogReadReport, MatchRefusal, MatchmakerHardState, MatchmakerId, MatchmakerPhase, MatchmakerSet,
    Message, NodeId, PROMISE_BATCH, Party, PendingBootstrap, ProxyId, QuorumSystem,
    ReconfigureReply, ReconfigureRequest, ReconfigureResult, ReconfigurerStep, Registration,
    RegistrationKind, Seam, Slot, StorageError, StorageFaultDecision, StorageRecord, command_hash,
    message_kind,
};

use self::state::AuditState;

/// One node's view of the shared checker. Constructed beside the node's
/// `BuggifyHooks` and handed to `paros::run_node`; it stamps simulated time on
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
    journal: Option<(JournalId, Arc<Mutex<journals::JournalBoard>>)>,
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
        }
    }

    /// This port reports for `journal` (#188): the non-interference oracles
    /// on `board` see its applies, its sends and its quarantines.
    pub(crate) fn in_journal(
        mut self,
        journal: JournalId,
        board: Arc<Mutex<journals::JournalBoard>>,
    ) -> Self {
        self.journal = Some((journal, board));
        self
    }

    /// The non-interference half of an apply (#188): on a multi-journal run
    /// a user command's identity must be one appended to this journal, and
    /// the board learns which journal committed while a sibling was held or
    /// quarantined.
    fn journal_applied(&self, node: NodeId, identity: Option<(u64, u64)>) {
        let Some((journal, board)) = &self.journal else {
            return;
        };
        let mut board = journals::lock(board);
        if !board.is_multi() {
            return;
        }
        if let Some((client, seq)) = identity {
            assert_always!(
                self.state().appended.contains(&(client, seq)),
                "journal: a slot holds only a command appended to its own journal",
                { "node" => node.0, "journal" => journal.0, "client" => client, "seq" => seq }
            );
        }
        let in_chaos = self.time.now() < crate::CHAOS_DURATION;
        board.applied(*journal, in_chaos);
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
    fn observe_durable_send(&self, from: Party, msg: &Message) {
        if let Message::Accepted { ballot, slot, .. } = msg {
            let st = self.state();
            let holds = st
                .accept_sets
                .get(&(slot.0, ballot.round, ballot.node.0))
                .is_some_and(|holders| from.node().is_some_and(|n| holders.contains(&n.0)));
            assert_always!(
                holds,
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
        // watermark, never the folded floor: a same-batch trim-point jump
        // raises the folded floor higher than a truncate in the batch, and
        // the ground-truth feed likewise forwards raw requests the storage
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
        }
        // Below the *cluster-wide* minimum floor every node has truncated, so
        // the per-slot safety tallies can never be consulted again: reclaim
        // them, keeping the decided vhash per pruned slot as the witness a
        // late `Commit` there is judged against.
        st.prune_below_floor();
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, point = point.0))]
    fn trimmed_to(&self, node: NodeId, point: Slot) {
        let now = self.now_ms();
        let mut st = self.state();
        st.trim_jumped = true;
        if st.replicas.contains(&node.0) {
            st.replica_jumped = true;
        }
        let landing = point.0.saturating_sub(1);
        // A trim point is a floor some peer holds, and a floor only ever
        // moves inside a walked chosen prefix — so the landing sits inside
        // the cluster's applied frontier, or it names slots nobody chose.
        // The message keeps its pre-#186 wording — the same claim the
        // snapshot install made, and an assertion's slot is its hash.
        assert_always!(
            st.cluster_applied_max.is_some_and(|max| landing <= max),
            "an installed snapshot lands within the cluster's applied frontier",
            {
                "node" => node.0,
                "landing" => landing,
                "cluster_max" => st.cluster_applied_max.unwrap_or(0)
            }
        );
        // The jump raises the durable floor and the chosen index with it:
        // keep the per-incarnation watermarks in step so a later
        // `SetChosenIndex` or `Truncate` report is judged against them.
        let watermark = st.chosen_watermark.entry(node.0).or_insert(0);
        *watermark = (*watermark).max(landing);
        let was = st.truncate_watermark.get(&node.0).copied().unwrap_or(0);
        st.truncate_watermark.insert(node.0, point.0.max(was));
        st.floor.entry(node.0).or_default().raise(point.0, now);
        // The jump moves the walked prefix straight to the landing without
        // walking the slots below it: an admitted forward jump.
        st.landings.entry(node.0).or_default().insert(landing);
        st.observe_applied_index(node.0, landing);
    }

    fn applied(&self, node: NodeId, slot: Slot, vhash: u64, identity: Option<(u64, u64)>) {
        let mut st = self.state();
        if st.replicas.contains(&node.0) {
            st.applied_on_replica = true;
        }
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
            && let Some(decided_vhash) = st.decided_vhash(slot.0)
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
        // The end-of-run `decided >= applied` leg is this, per slot. A slot
        // the tally pruned below the acceptors' floor stays decided through
        // its witness: a replica (#144) may still apply it — it was down
        // while the acceptors truncated, and it holds its own log below
        // their floor — and its decision is exactly what the witness keeps
        // (seeds 14697535725710265276, 12166376049160003182 and
        // 2038247294279376366 on the first replica-tier hunt).
        assert_always!(
            st.decided_vhash(slot.0).is_some(),
            "an applied slot was decided by a durable accept quorum before any node applied it",
            { "node" => node.0, "slot" => slot.0 }
        );
        st.any_chosen = true;
        st.observe_applied_index(node.0, slot.0);
        drop(st);
        self.journal_applied(node, identity);
    }

    fn journal_quarantined(&self, node: NodeId) {
        if let Some((journal, board)) = &self.journal {
            journals::lock(board).quarantine(node.0, *journal);
            // A cause: the storage fault that took one journal down on a
            // node; the outcome is the board's "serves its other journals".
            assert_reachable!("journal: a storage fault quarantines one journal of a node");
        }
    }

    fn sent(&self, node: NodeId, to: NodeId, msg: &Message) {
        if let Some((journal, board)) = &self.journal {
            let mut board = journals::lock(board);
            assert_always!(
                !board.is_quarantined(node.0, *journal),
                "journal: a quarantined journal sends nothing",
                { "node" => node.0, "journal" => journal.0 }
            );
            board.sent(node.0, *journal);
        }
        self.count_sent(msg);
        if let Message::Prepare { ballot, config, .. } = msg {
            self.check_prepare_licence(node, to, *ballot, config.as_ref());
            self.state().observe_prepare_send(node.0, to.0, *ballot);
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
        if let Message::CatchUpResponse { entries, .. } = msg
            && !entries.is_empty()
        {
            self.state().observe_catch_up_serve(node.0);
        }
        // Persist-before-send at the accept seam: an `Accepted` claims "I hold
        // this durably", so the matching record must already be in this
        // node's folded durable-accept tally (the same-batch write is flushed
        // and reported before the send; a re-answer names an older record
        // that was folded when it was first written or re-read at boot). A
        // `Commit` names a decided slot and carries the decided value.
        self.observe_durable_send(Party::Node(node), msg);
        // The Phase-2 half of P2b, checked *on the wire*, and the two claims
        // around it: *who* may propose under this ballot (authority
        // uniqueness — checked first, since a violation of it explains a
        // violation of the rest), *whom* it addresses and *on what Phase-1
        // licence* (#121, #122), then *what* was proposed.
        if let Message::Accept {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            let vhash = command_hash(command);
            let mut st = self.state();
            st.observe_authority_use(node.0, *ballot);
            st.observe_accept_send(Party::Node(node), to.0, *ballot);
            st.observe_proposal(Party::Node(node), *ballot, slot.0, vhash);
        }
    }

    fn sent_to_proxy(&self, node: NodeId, _proxy: ProxyId, msg: &Message) {
        self.count_sent(msg);
        // Persist-before-send at the accept seam, whoever the vote goes to:
        // an acceptor's `Accepted` to a proxy claims "I hold this durably"
        // exactly as one to a leader does, so the check `sent` runs is run
        // here on the same message (a node never sends a `Commit` to a
        // proxy; the call is the shared seam). Routing Phase 2 through a
        // proxy removes no check — the proxy's later quorum check judges
        // the decision, not the order of each vote and its fsync.
        self.observe_durable_send(Party::Node(node), msg);
        // A delegation is the leader exercising its Phase-2 authority for
        // the slot — the same two claims a colocated `Accept` makes about
        // *who* and *what*; *whom* it addresses is a proxy, which is judged
        // at the proxy's own fan-out.
        if let Message::Accept {
            ballot,
            slot,
            command,
            ..
        } = msg
        {
            let vhash = command_hash(command);
            let mut st = self.state();
            st.observe_authority_use(node.0, *ballot);
            st.observe_proposal(Party::Node(node), *ballot, slot.0, vhash);
        }
    }

    fn proxy_sent(&self, proxy: ProxyId, to: NodeId, msg: &Message) {
        self.count_sent(msg);
        let from = Party::Proxy(proxy);
        match msg {
            // The fan-out carries the leader's command to the ballot's own
            // acceptors: *whom* and *what* are judged exactly as the leader's
            // colocated `Accept` is; *who* is the leader the hint names, whose
            // authority the delegation already exercised.
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => {
                let vhash = command_hash(command);
                let mut st = self.state();
                st.observe_accept_send(from, to.0, *ballot);
                st.observe_proposal(from, *ballot, slot.0, vhash);
            }
            Message::Commit { .. } => self.observe_durable_send(from, msg),
            _ => {}
        }
    }

    fn replica_booted(&self, replica: NodeId, chosen_index: Option<Slot>, _floor: Slot) {
        let mut st = self.state();
        // A replica's id is outside the pool by construction; a collision
        // would fold a replica's reports into an acceptor's state.
        assert_always!(
            st.pool.as_ref().is_none_or(|pool| !pool.contains(&replica.0)),
            "replica: a replica's id is outside the node pool",
            { "replica" => replica.0 }
        );
        st.replicas.insert(replica.0);
        // A replica's read frontier is per boot, as a node's.
        st.read_watermark.remove(&replica.0);
        // The recovered prefix is walked, exactly as a node's boot report
        // says in `recovered`: with no application to replay (#186) the
        // walk resumes one past the durable chosen index, so a crash that
        // made the index durable before the walk over it was reported
        // (the `AfterSyncBeforeSend` seam) lands the next incarnation past
        // slots the audit never saw walked. Admitted as a landing — before
        // this, a replica's boot was not, and its first walked slot tripped
        // the no-gaps check (seeds 6838332052396296126,
        // 13879836091973256863, 3245387034260967674).
        if let Some(ci) = chosen_index {
            st.landings.entry(replica.0).or_default().insert(ci.0);
            st.observe_applied_index(replica.0, ci.0);
        }
    }

    fn delegation_taken_back(&self, _node: NodeId, _slot: Slot, _proxy: ProxyId) {
        self.state().delegation_taken_back = true;
    }

    // The proxy's own paths — a reboot, a re-fan-out, an ignored or
    // superseded delegation, a relayed `Nack` — are reported through the
    // port but not gated here: the model checker proves each in-core, and
    // the slot budget (512 per campaign process) is spent on outcomes.

    fn proxy_fanned_out(
        &self,
        _proxy: ProxyId,
        leader: NodeId,
        slot: Slot,
        ballot: Ballot,
        _vhash: u64,
        _column: Option<usize>,
        _addressees: usize,
    ) {
        // Every leader hint a round's fan-outs ever named: a second one is
        // a handoff successor's re-delegation refreshing it.
        self.state()
            .fanout_leaders
            .entry((slot.0, ballot.round, ballot.node.0))
            .or_default()
            .insert(leader.0);
    }

    fn proxy_decided(&self, proxy: ProxyId, slot: Slot, ballot: Ballot, vhash: u64) {
        self.state()
            .observe_proxy_decision(proxy.0, slot.0, ballot, vhash);
    }

    fn proxy_resend_skipped(&self, _proxy: ProxyId) {
        let mut st = self.state();
        reach_once!(
            st.proxy_resend_skipped,
            "proxy: a proxy skips a re-fan-out beat"
        );
    }

    fn proxy_round_expired(&self, _proxy: ProxyId, _slot: Slot) {
        let mut st = self.state();
        reach_once!(
            st.proxy_round_expired,
            "proxy: a proxy evicts a round nobody answers"
        );
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
        // A grid tolerates no member lost for good: each slot is decided by
        // its own column, a column with a dead member never decides again,
        // and the leader's recovery — hence every later reconfiguration —
        // waits on it forever. The harness keeps every identity it gave up
        // for good out of a grid in force (the copy budget parks nobody on
        // a grid seed, the composer asks every column to stay live, the
        // operators' ledger withholds a retirement a registered
        // reconfiguration still names); a leadership under a grid naming a
        // retired identity is one of those promises broken (#198).
        if matches!(config.quorum_system(), QuorumSystem::Grid { .. }) {
            let retired = config
                .members()
                .iter()
                .find(|member| st.retired.contains(&member.0))
                .map(|member| member.0);
            assert_always!(
                retired.is_none(),
                "gc: a leader never runs a grid configuration naming a retired identity",
                {
                    "node" => node.0,
                    "round" => won.round,
                    "retired" => retired.unwrap_or(u64::MAX)
                }
            );
        }
        st.bind_config(won, config);
        if st.bootstrap.as_ref().is_some_and(|b| b != config) {
            st.reconfiguration_completed = true;
        }
        // The #140 outcome: a leadership genuinely ran under a flexible
        // split (the draw is a `reachable` in `shape::quorum_policy`).
        if matches!(config.quorum_system(), QuorumSystem::Flexible { .. }) {
            st.elected_flexible = true;
        }
        // The #141 outcomes: a leadership ran under a grid (its Phase 1 was
        // covered by a full row); its matchmaking closed with a *different*
        // grid among the prior configurations (Phase 1 needed a row of that
        // one too — a row across a reconfiguration); and the configuration
        // moved between a grid and a majority (a prior of the other kind).
        let is_grid = |c: &AcceptorConfig| matches!(c.quorum_system(), QuorumSystem::Grid { .. });
        if is_grid(config) {
            st.elected_grid = true;
        }
        let prior: Vec<AcceptorConfig> = st.prior_of(won).map(<[_]>::to_vec).unwrap_or_default();
        if prior.iter().any(|c| c != config && is_grid(c)) {
            st.elected_across_grid = true;
        }
        if prior
            .iter()
            .any(|c| c != config && is_grid(c) != is_grid(config))
        {
            st.reconfigured_across_grid_boundary = true;
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
        st.leader_promise_checked = true;
        st.any_leader = true;
        st.leader_rounds.insert(won.round);
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
        // the only one that may hand it on (see `ColocatedNode::can_relinquish`).
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
        st.any_leader = true;
    }

    fn handoff_refused(&self, _node: NodeId, target: u64, stale: u64, shape: u64, unfit: u64) {
        let mut st = self.state();
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
        st.any_ack_checked = true;
        // Decision 1 of #144: the node asked acks, and the slot's reply
        // owner — a replica, a different process by construction — is noted
        // so the gate can prove it applied what it would have answered.
        if let Some(owner) = paros::ReplicaId::of(slot, st.replica_count) {
            let owner = crate::roles::replica_node_id(owner).0;
            if owner != node.0 {
                let first = st.acked_by_other.entry(owner).or_insert(slot.0);
                *first = (*first).min(slot.0);
            }
        }
        // A committed ack is a claim about a specific applied command: on both
        // ack paths (ack-on-commit and the dedup fast path) the apply of this
        // `(client, seq)` was folded before the ack fired — on this node, or,
        // for a session fact adopted from a trim-point jump, on the peer that
        // served it. The ack must name exactly the index the identity applied at; an
        // ack for a never-applied identity fails the same check. ("Applied"
        // is the walk over the chosen prefix: paros runs no application,
        // #186.)
        let applied_at = st.applied_identity.get(&(client, seq)).copied();
        assert_always!(
            applied_at == Some(slot.0),
            "a committed ack names the slot its command applied at",
            {
                "node" => node.0,
                "client" => client,
                "seq" => seq,
                "acked_slot" => slot.0,
                "applied_at" => crate::signed_watermark(applied_at)
            }
        );
        if st.leader_change_ms.is_some() {
            st.ack_after_leader_change = true;
        }
        // The dedup-window edge the reply-drop location exists for: a reply
        // was dropped after commit, and a retry then took the dedup path.
        if dedup && st.propose_reply_dropped {
            st.dedup_after_dropped_reply = true;
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
        st.check_read_frontier(node, index);
        // The #141 outcome: the confirming ack set was a full column of a
        // grid (the leader's configuration at its current ballot).
        if let Some(round) = st.leader_round.get(&node.0).copied()
            && st
                .config_of(Ballot { round, node })
                .is_some_and(|c| matches!(c.quorum_system(), QuorumSystem::Grid { .. }))
        {
            st.read_confirmed_on_column = true;
        }
    }

    fn log_read_served(&self, node: NodeId, report: &LogReadReport) {
        let mut st = self.state();
        // A page is served from the serving process's contiguous chosen
        // prefix and never above it: an empty long-poll answer names its own
        // start, everything else ends inside the prefix.
        assert_always!(
            report.next <= report.committed_end
                || (report.entries == 0 && report.next == report.from),
            "journal read: a page never passes the serving prefix",
            {
                "node" => node.0,
                "from" => report.from.0,
                "next" => report.next.0,
                "committed_end" => report.committed_end.0
            }
        );
        if let Some(trim) = report.trimmed_to {
            // Trimmed only below the trim point, and a trim point only ever
            // inside what the cluster decided (a floor moves inside the
            // chosen prefix).
            assert_always!(
                report.from < trim,
                "journal read: a trim point refuses only reads below it",
                { "node" => node.0, "from" => report.from.0, "trim" => trim.0 }
            );
            assert_always!(
                st.decided_max.is_some_and(|d| trim.0 <= d + 1),
                "journal read: a trim point never passes the decided prefix",
                {
                    "node" => node.0,
                    "trim" => trim.0,
                    "decided_max" => crate::signed_watermark(st.decided_max)
                }
            );
            st.journal_read_trimmed = true;
        }
        st.journal_read_skipped_hole |= report.skipped > 0;
        st.journal_read_woke |= report.answer == LogReadAnswer::Woke && report.entries > 0;
        st.journal_read_on_replica |= st.replicas.contains(&node.0);
    }

    fn journal_refused(&self, _node: NodeId, _journal: JournalId, _call: &'static str) {
        assert_reachable!("journal: a call naming an unserved journal is refused");
    }

    fn quorum_read_served(
        &self,
        node: NodeId,
        row: Option<usize>,
        watermark: Option<Slot>,
        served: Option<Slot>,
        opened: Option<Slot>,
        leader: bool,
    ) {
        let now = self.now_ms();
        let mut st = self.state();
        // The replica half of §3.4: a node answers only once its own chosen
        // prefix covers the maximum watermark its row reported — the step
        // that makes every write acked before the read visible to it.
        assert_always!(
            served >= watermark,
            "quorum read: a node serves only once its prefix covers the row's watermark",
            {
                "node" => node.0,
                "served" => crate::signed_watermark(served.map(|s| s.0)),
                "watermark" => crate::signed_watermark(watermark.map(|s| s.0))
            }
        );
        // Both read paths answer from one node's chosen prefix, so the
        // per-boot frontier is one fold over both.
        st.check_read_frontier(node, served);
        st.quorum_read_by_follower |= !leader;
        // The read a local answer would have got wrong: the row knew of a
        // vote past what this node had chosen when the read opened.
        st.quorum_read_past_opened |= watermark > opened;
        st.quorum_read_after_leader_change |= st.leader_change_ms.is_some_and(|t| now > t);
        st.quorum_read_on_row |= row.is_some();
        // §3.4's own shape (#144): the read answered from a replica's state.
        st.quorum_read_on_replica |= st.replicas.contains(&node.0);
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
        if let Some((journal, board)) = &self.journal {
            journals::lock(board).reopened(node.0, *journal);
        }
        let mut st = self.state();
        st.booted.insert(node.0);
        // One shared deployment per run: every node's durable configuration
        // names the same bootstrap membership, pool and matchmaker set.
        let bootstrap = st
            .bootstrap
            .get_or_insert_with(|| deployment.bootstrap.clone());
        assert_always!(
            *bootstrap == deployment.bootstrap,
            "every node derives the same bootstrap configuration",
            { "node" => node.0, "members" => deployment.bootstrap.members().len() }
        );
        let pool: BTreeSet<u64> = deployment.pool.iter().map(|n| n.0).collect();
        let known = st.pool.get_or_insert_with(|| pool.clone());
        assert_always!(
            *known == pool,
            "every node derives the same node pool",
            { "node" => node.0, "pool" => pool.len() }
        );
        st.replica_count = deployment.replica_count;
        st.matchmaker.note_deployment(&deployment.matchmakers);
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
                    "frontier" => crate::signed_watermark(frontier)
                }
            );
            // The recovered prefix is walked: with no application to replay
            // (#186), a boot resumes its walk one past the durable chosen
            // index — an admitted forward jump when the previous incarnation
            // made the index durable before reporting the walk over it.
            st.landings.entry(node.0).or_default().insert(ci.0);
            st.observe_applied_index(node.0, ci.0);
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

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, refusal = ?refusal))]
    fn boot_refused(&self, node: NodeId, refusal: BootRefusal) {
        let mut st = self.state();
        match refusal {
            // #147: the library, not the harness, keeps a wiped identity
            // down. The identity is gone for good: convergence excuses it,
            // a reconfiguration replaces it.
            BootRefusal::Amnesia => {
                st.wiped.insert(node.0);
                reach_once!(
                    st.wiped_any,
                    "storage: a wiped identity stays down and is replaced by reconfiguration"
                );
                reach_once!(
                    st.amnesia_refused,
                    "storage: the library refuses to boot an amnesiac member"
                );
            }
            BootRefusal::AlreadyFormatted => {
                assert_always!(
                    false,
                    "storage: a first boot never meets a formatted store",
                    { "node" => node.0 }
                );
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(seam = ?seam))]
    fn crashed(&self, _node: NodeId, seam: Seam) {
        let mut st = self.state();
        st.crashed_any = true;
        match seam {
            Seam::BeforeSync => {
                reach_once!(
                    st.crashed_before_sync,
                    "the driver crashes before syncing a staged batch"
                );
            }
            Seam::AfterSyncBeforeSend => {
                reach_once!(
                    st.crashed_after_sync,
                    "the driver crashes after sync and before sending a batch"
                );
            }
            // The matchmaker's seams are reported through
            // `matchmaker_crashed`, in their own namespace.
            Seam::MatchBeforeSync | Seam::MatchAfterSyncBeforeReply => {}
        }
    }

    fn dropped_at_send(&self, _from: Party, _to: Party, msg: &Message) {
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
            Message::TrimmedTo { .. } | Message::CatchUpResponse { .. } => {
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
            _ => {}
        }
    }

    fn duplicated_at_send(&self, _from: Party, _to: Party, msg: &Message) {
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
        // The other kinds (a commit, the repair and snap-repair planes, a
        // catch-up request) share the family gate above: every one of them
        // is an idempotency claim the `always` checks judge, and the hook
        // keeps one location per kind regardless. A re-delivered handoff
        // keeps its own — it must be a no-op at its addressee, never an
        // allocator rewind, and refused everywhere else; the uniqueness
        // oracle above is what keeps that honest.
        if matches!(msg, Message::Relinquish { .. }) {
            reach_once!(
                st.duplicated_relinquish,
                "the driver duplicates a relinquishment at the send seam"
            );
        }
    }

    fn client_reply_dropped(&self, _node: NodeId, reply: paros::Reply) {
        let mut st = self.state();
        if matches!(
            reply,
            paros::Reply::ProposeRedirect
                | paros::Reply::ReadRedirect
                | paros::Reply::Compact
                | paros::Reply::Reconfigure
                | paros::Reply::ReconfigureMatchmakers
                | paros::Reply::Retire
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

    fn compact_acked(&self, _node: NodeId, accepted: bool) {
        let mut st = self.state();
        if accepted {
            // Feeds the "chain: compact takes effect" outcome gate.
            st.compact_ack_accepted = true;
        } else {
            reach_once!(
                st.compact_ack_refused,
                "a compact request is acked as refused"
            );
        }
    }

    fn dropped_at_mailbox(&self, _from: Party, _to: Party, _kind: &'static str) {
        let mut st = self.state();
        reach_once!(st.mailbox_dropped, "mailbox overflow dropped a message");
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

    fn delivery_failed(&self, _from: Party, _to: Party) {
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

    fn client_reply_duplicated(&self, _node: NodeId, reply: paros::Reply) {
        let mut st = self.state();
        match reply {
            paros::Reply::Match => reach_once!(
                st.reply_duplicated[0],
                "a matchmaker's registration reply is folded twice"
            ),
            paros::Reply::GcAck => reach_once!(
                st.reply_duplicated[1],
                "a matchmaker's GC ack is folded twice"
            ),
            paros::Reply::MatchmakerReconfigure => reach_once!(
                st.reply_duplicated[2],
                "a matchmaker's handover reply is folded twice"
            ),
            _ => {}
        }
    }

    fn edge_rejected(&self, _at: Party, _kind: EdgeRejection) {
        let mut st = self.state();
        st.edge_rejections += 1;
        // The message predates the move to moonpool-rpc; it stays verbatim
        // because a message's hash is its assertion slot.
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

    fn election_timeout_set(&self, node: NodeId, ticks: u64) {
        self.state().election_timeouts.insert(node.0, ticks);
    }

    fn heartbeat_ack_received(&self, node: NodeId, from: NodeId, ballot: Ballot, _seq: u64) {
        self.state().observe_ack_received(node.0, from.0, ballot);
    }

    fn ticked(&self, node: NodeId) {
        self.state().observe_tick(node.0);
    }

    fn election_timeout_extreme(&self, _node: NodeId, _ticks: u64) {
        let mut st = self.state();
        reach_once!(
            st.shortest_timeout,
            "the driver selects the shortest valid election timeout"
        );
    }

    fn election_backoff(&self, _node: NodeId, _doublings: u32) {
        // A cause, not an outcome: failed campaigns are the swarm's business;
        // what the backoff buys is the convergence claim.
        let mut st = self.state();
        reach_once!(
            st.election_backoff,
            "the driver backs off its election timeout after a failed campaign"
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
        self.state().quorum_lost = true;
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

    fn repair_progress(
        &self,
        _node: NodeId,
        repaired: u64,
        case1: u64,
        case2: u64,
        step_downs: u64,
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

    // ---- the matchmaker registry (see `matchmaker`) -------------------------

    #[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.0))]
    fn matchmaker_recovered(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        registry: &BTreeMap<Ballot, Registration>,
        gc_watermark: Ballot,
    ) {
        self.state()
            .matchmaker
            .recovered(matchmaker, set, phase, registry, gc_watermark);
    }

    fn matchmaker_scalars_persisted(
        &self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
        self.state()
            .matchmaker
            .scalars_persisted(matchmaker, scalars);
    }

    fn reconfigurer_reconstructed(
        &self,
        node: NodeId,
        generation: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
        self.state()
            .matchmaker
            .reconstructed(node, generation, bootstrap, disagreements);
    }

    fn matchmaker_activated(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        effective: Option<&(Ballot, AcceptorConfig)>,
        registry: &BTreeMap<Ballot, Registration>,
    ) {
        self.state()
            .matchmaker
            .activated(matchmaker, set, gc_watermark, effective, registry);
    }

    fn matchmaker_reconfigure_replied(
        &self,
        matchmaker: MatchmakerId,
        _request: &ReconfigureRequest,
        reply: &ReconfigureReply,
    ) {
        self.state()
            .matchmaker
            .reconfigure_replied(matchmaker, reply);
    }

    fn matchmaker_gc_replied(&self, matchmaker: MatchmakerId, ack: &GcAck) {
        self.state().matchmaker.gc_replied(matchmaker, ack);
    }

    fn match_registered(
        &self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        let mut st = self.state();
        st.matchmaker.registered(matchmaker, ballot, registration);
        let config = &registration.config;
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
        generation: u64,
        page: &HistoryPage<'_>,
    ) {
        self.state()
            .matchmaker
            .replied(matchmaker, to, ballot, generation, page);
    }

    // ---- the leader-side matchmaking phase (#120) and reconfiguration (#122) ----

    fn matchmaking_started(
        &self,
        node: NodeId,
        ballot: Ballot,
        config: &AcceptorConfig,
        kind: RegistrationKind,
        generation: u64,
    ) {
        self.state()
            .matchmaker
            .campaign_started(node, ballot, config, kind, generation);
    }

    // ---- garbage collection (#123) ------------------------------------------

    fn gc_request_sent(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        _generation: u64,
        watermark: Ballot,
        fence: Option<Slot>,
    ) {
        let mut st = self.state();
        // The leader re-sends its request every beat and the licence is judged
        // once per `(node, watermark)`, so ask before deriving: everything
        // below is an O(fence x members) walk whose answer would be dropped.
        if !st.matchmaker.gc_needs_licence(node, watermark) {
            st.matchmaker.gc_requested(node, watermark, fence, None, "");
            return;
        }
        // GC invariant 1, re-derived from the audit's own durable fold: a
        // Phase-2 quorum of the configuration bound to the leader's ballot
        // holds every slot up to the fence — a durable record carrying the
        // decided value where the audit knows one (any record otherwise), or
        // a compaction floor above the slot (the below-floor `Nack` is the
        // acceptor's "already chosen"). `None` when the configuration is not
        // known yet (then nothing is judged).
        // A slot applied as a `Noop` over a *different* durable record is the
        // #94 duplicate substitution (a repeated `(client, seq)` executes as a
        // no-op while its record keeps the user command): the record is the
        // chosen value, and what every Phase 1 would find.
        let noop = command_hash(&Command::Control(Control::Noop));
        let mut uncovered: Vec<String> = Vec::new();
        let covered = st.config_of(watermark).cloned().map(|config| {
            let holders: BTreeSet<NodeId> = config
                .members()
                .iter()
                .filter(|m| {
                    let m = m.0;
                    match fence {
                        None => true,
                        Some(fence) => {
                            let gap = (0..=fence.0).find(|slot| {
                                let floor = st.floor.get(&m).map_or(0, |f| f.now);
                                if floor > *slot {
                                    return false;
                                }
                                match (st.persisted.get(&(m, *slot)), st.chosen.get(slot)) {
                                    (Some(held), Some(decided)) => {
                                        held != decided && *decided != noop
                                    }
                                    (Some(_), None) => false,
                                    (None, _) => true,
                                }
                            });
                            if let Some(slot) = gap {
                                let why = match st.persisted.get(&(m, slot)) {
                                    Some(_) => "mismatch",
                                    None => "missing",
                                };
                                uncovered.push(format!("{m}@{slot}:{why}"));
                            }
                            gap.is_none()
                        }
                    }
                })
                .copied()
                .collect();
            // Judged by the configuration's own quorum system, never by a
            // count: the custody claim the leader's GC rests on is the same
            // Phase-2 quorum question `Collector::covered` asks in the core.
            config.has_phase2_quorum(&holders)
        });
        st.matchmaker
            .gc_requested(node, watermark, fence, covered, &uncovered.join(","));
    }

    fn gc_resend_skipped(&self, _node: NodeId) {
        self.state().matchmaker.gc_resend_skipped();
    }

    fn gc_step(&self, node: NodeId, _matchmaker: MatchmakerId, ack: &GcAck, step: &GcStep) {
        let mut st = self.state();
        let config = st.config_of(ack.watermark).cloned();
        st.matchmaker.gc_step(node, ack, step, config.as_ref());
    }

    // ---- the matchmaker set and its reconfiguration (#125) ------------------

    fn matchmakers_learned(&self, node: NodeId, set: &MatchmakerSet) {
        self.state().matchmaker.set_learned(node, set);
    }

    fn reconfigurer_started(&self, node: NodeId, old: &MatchmakerSet, target: &[MatchmakerId]) {
        self.state()
            .matchmaker
            .reconfigurer_started(node, old, target);
    }

    fn reconfigurer_aborted(&self, node: NodeId) {
        self.state().matchmaker.reconfigurer_aborted(node);
    }

    fn reconfigurer_backoff(&self, _node: NodeId, _ticks: u64) {
        self.state().matchmaker.reconfigurer_backoff();
    }

    fn reconfigure_matchmakers_acked(&self, _node: NodeId, refusal: &'static str) {
        let mut st = self.state();
        if refusal.is_empty() {
            reach_once!(
                st.reconfigure_matchmakers_started,
                "generation: a client's matchmaker reconfiguration is started"
            );
        } else {
            reach_once!(
                st.reconfigure_matchmakers_refused,
                "generation: a client's matchmaker reconfiguration is refused"
            );
        }
    }

    fn reconfigurer_resend_skipped(&self, _node: NodeId) {
        self.state().matchmaker.reconfigurer_resend_skipped();
    }

    fn reconfigurer_step(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
        self.state()
            .matchmaker
            .reconfigurer_step(node, matchmaker, reply, step);
    }

    fn successor_republished(
        &self,
        node: NodeId,
        _matchmaker: MatchmakerId,
        successor: &MatchmakerSet,
    ) {
        self.state()
            .matchmaker
            .successor_republished(node, successor);
    }

    fn retire_acked(&self, _node: NodeId, accepted: bool, refusal: &str) {
        let mut st = self.state();
        // The refusal legs the shared gate cannot tell apart. `not_collected`
        // is the one #123's rule turns from a workload discipline into a
        // protocol answer: the node is outside the configuration it believes
        // in force, and still refuses, because nothing proves the cluster is
        // done with the configurations it *was* in.
        if !accepted {
            match refusal {
                "not_collected" => reach_once!(
                    st.retire_not_collected,
                    "gc: a retirement is refused for want of an effective floor"
                ),
                // The freshness leg (#165): outside the configuration it
                // believes in force, but that belief predates the floor, so
                // the node cannot tell whether the configuration the floor
                // kept names it.
                "stale" => reach_once!(
                    st.retire_stale,
                    "gc: a retirement is refused on a belief older than the floor"
                ),
                "leader" => reach_once!(
                    st.retire_leader,
                    "gc: a retirement is refused by the sitting leader"
                ),
                _ => {}
            }
        }
        st.matchmaker.retire_acked(accepted);
    }

    fn retired(&self, node: NodeId) {
        let mut st = self.state();
        st.matchmaker.retired(node);
        st.retired.insert(node.0);
    }

    fn membership_probe_opened(
        &self,
        node: NodeId,
        ballot: Ballot,
        _believed: &AcceptorConfig,
        generation: u64,
    ) {
        self.state()
            .matchmaker
            .probe_opened(node, ballot, generation);
    }

    fn membership_probe_closed(
        &self,
        node: NodeId,
        ballot: Ballot,
        effective: Option<Ballot>,
        member: bool,
    ) {
        self.state()
            .matchmaker
            .probe_closed(node, ballot, effective, member);
    }

    fn match_request_sent(&self, node: NodeId, matchmaker: MatchmakerId, ballot: Ballot) {
        self.state()
            .matchmaker
            .request_sent(node, matchmaker, ballot);
    }

    fn matchmaking_stale_configuration(&self, node: NodeId, ballot: Ballot, newest: Ballot) {
        self.state().matchmaker.campaign_stale(node, ballot, newest);
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
        watermark: Ballot,
        history_hash: u64,
    ) {
        self.state().matchmaker.registered_by(
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
        _next: Ballot,
        watermark: Ballot,
        history_hash: u64,
    ) {
        self.state()
            .matchmaker
            .paged(node, matchmaker, ballot, watermark, history_hash);
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
            .campaign_refused(node, ballot, &refusal);
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

    #[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.0, refusal = ?refusal))]
    fn matchmaker_boot_refused(&self, matchmaker: MatchmakerId, refusal: BootRefusal) {
        match refusal {
            // #183: the library, not the harness, keeps a wiped registry out.
            BootRefusal::Amnesia => self.state().matchmaker.boot_refused(matchmaker.0),
            BootRefusal::AlreadyFormatted => {
                assert_always!(
                    false,
                    "matchmaker: a first boot never meets a formatted registry",
                    { "matchmaker" => matchmaker.0 }
                );
            }
        }
    }

    fn match_reply_dropped(&self, _matchmaker: MatchmakerId, reply: paros::Reply) {
        self.state().matchmaker.reply_dropped(reply);
    }

    fn matchmaker_storage_fault(
        &self,
        matchmaker: MatchmakerId,
        _error: &StorageError,
        decision: StorageFaultDecision,
    ) {
        self.state().matchmaker.storage_fault(matchmaker, decision);
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
    use paros::{Ballot, Message, NodeId, ProxyId, Slot};

    use super::state::{AuditState, Floor};
    use super::{Audit, AuditWorld, NodeAudit};

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
}
