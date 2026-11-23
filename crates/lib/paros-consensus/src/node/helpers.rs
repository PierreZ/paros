use super::{
    Audience, Ballot, BeliefSource, ColocatedNode, Command, LeadershipOrigin, Message, NodeId,
    NodeRole, Party, Slot,
};
use crate::membership::{AcceptorConfig, MAX_ROLE_COUNT};
use crate::quorum_read::ReadBasis;

impl ColocatedNode {
    // ---- helpers ----------------------------------------------------------

    /// Record `(ballot, command)` in the acceptor's log. The one path the
    /// wiring takes to [`crate::acceptor::Acceptor::record_accepted`] (which
    /// tallies an in-place repair of a faulty entry itself).
    pub(super) fn record_accepted(&mut self, slot: Slot, ballot: Ballot, command: Command) {
        let writes = self.pending_writes.len();
        self.acceptor
            .record_accepted(slot, ballot, command, &mut self.pending_writes);
        // The record and its durable op land in this node's one batch.
        assert!(
            self.pending_writes.len() == writes + 1,
            "a record emits one write"
        );
        assert!(
            self.acceptor
                .record(slot)
                .is_some_and(|(b, _)| *b == ballot),
            "a record lands at its ballot"
        );
    }

    /// Whether `node` is in the addressable pool — the wire-hygiene boundary
    /// every handler draws around a sender: a misrouted or misconfigured id
    /// is never followed, counted, or replied to. Membership of a
    /// *configuration* is a separate, per-configuration question.
    pub(super) fn in_pool(&self, node: NodeId) -> bool {
        let pooled = self.pool.binary_search(&node).is_ok();
        // Every configuration this node runs is drawn from the pool.
        if self.acceptors.contains(node) {
            assert!(pooled, "a member of the configuration in force is pooled");
        }
        pooled
    }

    // ---- the wire-configuration coin ----------------------------------------
    //
    // **A plain deployment carries no configuration on the wire.** Every
    // message that can carry one (`Prepare`, a delegated `Accept`,
    // `Heartbeat`, `Relinquish`, the read fence of a `PreReadAck`) carries
    // it only on a matchmaker deployment (a colocated `Accept` never does), where a peer must learn the configuration in
    // force off the wire; plain Multi-Paxos exchanges byte-for-byte today's
    // messages and never moves its static membership. The three helpers
    // below are that one coin, minted once.

    /// `config` on the wire: itself on a matchmaker deployment, nothing on
    /// plain Multi-Paxos (see the note above).
    pub(super) fn wire_config_of(&self, config: &AcceptorConfig) -> Option<AcceptorConfig> {
        let wire = self.config.has_matchmakers().then(|| config.clone());
        // The plain path carries no configuration, ever.
        assert!(
            wire.is_some() == self.config.has_matchmakers(),
            "only a matchmaker deployment carries a configuration on the wire"
        );
        wire
    }

    /// The configuration in force on the wire — what a beat, a colocated
    /// leader's delegated `Accept` and a handoff carry
    /// ([`ColocatedNode::wire_config_of`] over `acceptors`).
    pub(super) fn wire_config(&self) -> Option<AcceptorConfig> {
        self.wire_config_of(&self.acceptors)
    }

    /// The read fence on the wire: what a `PreReadAck` carries on a
    /// matchmaker deployment, nothing on plain Multi-Paxos (see the note
    /// above). It is the higher of this node's durable promise and the
    /// ballot its belief is bound to. The promise is the rule of the reads
    /// model (`specs/tla/reads/Reads.tla`, `Fence`): a node that promised
    /// a ballot above a read's basis may have helped a newer leadership win,
    /// whether or not it adopted that campaign's configuration. A node whose
    /// pool lags ignores a configuration that names nodes outside it
    /// ([`ColocatedNode::learn_config`]) but still raises its promise, so the
    /// belief ballot alone let a read complete below a write the newer
    /// configuration chose (finding 1 of the paros-consensus review of
    /// 2026-11-16).
    pub(super) fn wire_read_fence(&self) -> Option<Ballot> {
        let fence = self
            .config
            .has_matchmakers()
            .then(|| self.acceptor.promised().max(self.acceptors_since));
        assert!(
            fence.is_some() == self.config.has_matchmakers(),
            "only a matchmaker deployment carries a read fence"
        );
        if let Some(fence) = fence {
            assert!(
                fence >= self.acceptor.promised(),
                "the read fence covers the promise"
            );
            assert!(
                fence >= self.acceptors_since,
                "the read fence covers the belief ballot"
            );
        }
        fence
    }

    /// Whether this node's promise is its own campaign that sent no
    /// `Prepare` and is no longer open (#890 (learner above the leader)):
    /// the matchmakers refused it, so no acceptor ever heard of its ballot.
    pub(super) fn promise_is_own_unprepared(&self) -> bool {
        let own = self.matchmaking.is_none()
            && self.unprepared_campaign == Some(self.acceptor.promised());
        if own {
            assert!(
                self.acceptor.promised().node == self.config.id,
                "an unprepared campaign is this node's own ballot"
            );
            assert!(
                self.proposer.election().is_none(),
                "an unprepared campaign opened no Phase 1"
            );
        }
        own
    }

    /// The fence that gates this node's **own** reads over `config`: to
    /// take a basis, to open a read on it and to keep it open. It is
    /// [`ColocatedNode::wire_read_fence`], with one exception, option (b) of
    /// #890 (learner above the leader): a node outside `config` whose only
    /// promise above the belief ballot is its own campaign that sent no
    /// `Prepare` is gated on the belief ballot alone. That campaign chose
    /// nothing, and the node does not answer its own read, so the quorum of
    /// `config` that answers carries every fence. Rule `Below` with
    /// `Learner = "own"` in `specs/tla/reads/` (checks `learner_own`, and
    /// the control `learner_own_serves` of `learner_strict`). The fence that
    /// a `PreReadAck` carries to other readers never takes the exception.
    pub(super) fn reader_fence(&self, config: &AcceptorConfig) -> Option<Ballot> {
        let fence = self.wire_read_fence()?;
        if self.promise_is_own_unprepared() && !config.contains(self.config.id) {
            assert!(
                self.acceptors_since <= fence,
                "the exception only lowers the fence"
            );
            return Some(self.acceptors_since);
        }
        Some(fence)
    }

    /// The configuration a `Prepare` at this node's ballot carries: the
    /// registered `C_b` on a matchmaker deployment (so every acceptor it
    /// reaches learns it), nothing on plain Multi-Paxos (whose `Prepare` is
    /// byte-for-byte today's). Differs from [`ColocatedNode::wire_config`]
    /// in what it carries — the open election's `C_b` when there is one —
    /// not in when.
    pub(super) fn phase1_wire_config(&self) -> Option<AcceptorConfig> {
        if !self.config.has_matchmakers() {
            return None;
        }
        let config = self
            .proposer
            .election()
            .map(|e| e.config().clone())
            .or_else(|| Some(self.acceptors.clone()));
        assert!(
            config.is_some(),
            "a matchmaker deployment's Prepare carries C_b"
        );
        config
    }

    /// Adopt `config` as the latest known configuration when `ballot` is
    /// above the ballot the current belief was registered under. Only a
    /// deployment with matchmakers ever learns a configuration off the wire;
    /// a plain node ignores the field (a mixed deployment is a
    /// misconfiguration, never a reason to move a static membership).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round)))]
    pub(super) fn learn_config(&mut self, ballot: Ballot, config: Option<AcceptorConfig>) {
        let Some(config) = config else {
            return;
        };
        if !self.config.has_matchmakers() || ballot <= self.acceptors_since {
            return;
        }
        // Wire hygiene: a configuration naming a node outside the pool is not
        // one this deployment can run; ignore it whole.
        if !config.is_drawn_from(&self.pool) {
            probe!(
                reachable,
                "learning: a configuration outside the pool is ignored"
            );
            return;
        }
        let since = self.acceptors_since;
        self.adopt_configuration(config, ballot);
        // A learned configuration is strictly newer than the belief it
        // replaces: the binding only moves forward off the wire.
        assert!(
            self.acceptors_since == ballot,
            "a learned configuration binds its ballot"
        );
        assert!(
            self.acceptors_since > since,
            "a learned configuration is newer"
        );
    }

    /// Learn the read basis `(config, since, fence)` of a leadership that
    /// **won** `since` (#260): this node's own election or handoff install,
    /// or a beat at or above its promise. Only a matchmaker deployment keeps
    /// one; the basis only moves forward, and a configuration naming a node
    /// outside the pool is ignored whole, as [`ColocatedNode::learn_config`]
    /// ignores it.
    pub(super) fn learn_read_basis(
        &mut self,
        config: AcceptorConfig,
        since: Ballot,
        fence: Option<Slot>,
    ) {
        if !self.config.has_matchmakers()
            || self.read_basis.as_ref().is_some_and(|b| b.since > since)
            || !config.is_drawn_from(&self.pool)
        {
            return;
        }
        let before = self.read_basis.as_ref().map(|b| b.since);
        self.read_basis = Some(ReadBasis {
            config,
            since,
            fence,
        });
        // The basis only moves forward, and lands exactly on what was learned.
        assert!(
            before.is_none_or(|b| b <= since),
            "a read basis never moves back"
        );
        assert!(
            self.read_basis
                .as_ref()
                .is_some_and(|b| b.since == since && b.fence == fence),
            "a learned read basis binds its ballot and fence"
        );
    }

    /// Follow a ballot this node accepted leader contact under — a
    /// `Prepare` it promised, an `Accept` it admitted, a beat at or above
    /// its promise: the operating ballot rises to it (never falls), and the
    /// configuration it carries is learned ([`ColocatedNode::learn_config`]).
    pub(super) fn follow_ballot(&mut self, ballot: Ballot, config: Option<AcceptorConfig>) {
        let before = self.ballot;
        self.ballot = self.ballot.max(ballot);
        assert!(self.ballot >= before, "the operating ballot never falls");
        assert!(
            self.ballot >= ballot,
            "the operating ballot reaches a followed ballot"
        );
        self.learn_config(ballot, config);
    }

    /// **The one way the configuration in force moves**: bind `config` to
    /// `since` and record what that does to this node's membership. Every
    /// site that moves it — [`ColocatedNode::learn_config`], a won election,
    /// a handoff install, the adoption of an effective configuration — comes
    /// through here, so none can forget the membership record that
    /// [`ColocatedNode::may_retire`] and the quorum reads depend on.
    ///
    /// Whatever moved it, the belief is now one this incarnation **heard**
    /// ([`BeliefSource::Heard`]), and a membership probe still open is
    /// answered by it: a probe only ever asks about the bootstrap default
    /// (#173). A closed probe's late answers stop counting (#278), and a
    /// *different* configuration (or a binding below the fact) drops the
    /// reconfiguration fact the old one matched: the caller that adopted a fact names it afterwards
    /// ([`ColocatedNode::bind_fact`]).
    pub(super) fn adopt_configuration(&mut self, config: AcceptorConfig, since: Ballot) {
        // Plain Multi-Paxos never moves its static membership.
        assert!(
            self.config.has_matchmakers(),
            "only a matchmaker deployment adopts a configuration"
        );
        // A binding below the fact cannot carry it either: the fact is
        // always at most the ballot the belief is bound to.
        if config != self.acceptors || since < self.belief_fact {
            self.belief_fact = Ballot::zero();
        }
        self.acceptors = config;
        self.acceptors_since = since;
        self.belief_source = BeliefSource::Heard;
        self.probe = None;
        self.closed_probe = None;
        self.retire_check = None;
        self.record_membership();
        assert!(
            self.acceptors_since == since,
            "the configuration binds its ballot"
        );
        assert!(
            self.probe.is_none(),
            "an adopted belief answers any open probe"
        );
        assert!(
            self.closed_probe.is_none(),
            "an adopted belief retires a closed probe's late answers"
        );
    }

    /// Name the reconfiguration fact the belief now matches (#278): the
    /// effective configuration a probe or a `StaleConfiguration` just
    /// adopted, registered under `fact`. Called right after
    /// [`ColocatedNode::adopt_configuration`] bound that configuration.
    pub(super) fn bind_fact(&mut self, fact: Ballot) {
        assert!(
            fact != Ballot::zero(),
            "a reconfiguration fact is registered under a ballot"
        );
        assert!(
            fact <= self.acceptors_since,
            "a belief is bound at or above the fact it matches"
        );
        self.belief_fact = fact;
        assert!(
            self.belief_fact == fact,
            "the belief names the fact it matches"
        );
    }

    /// Record that `acceptors`/`acceptors_since` just moved: if the new
    /// configuration names this node, the ballot it is bound to is the newest
    /// at which this node was a member. Called from every assignment to
    /// `acceptors_since` ([`ColocatedNode::adopt_configuration`]), so
    /// [`ColocatedNode::may_retire`] never under-reports the membership it must
    /// outlive.
    fn record_membership(&mut self) {
        let fence = self.last_member_ballot;
        if self.is_acceptor() {
            self.last_member_ballot = self.last_member_ballot.max(self.acceptors_since);
            assert!(
                self.last_member_ballot >= self.acceptors_since,
                "a member's fence covers its configuration's ballot"
            );
        }
        assert!(
            self.last_member_ballot >= fence,
            "the membership fence never falls"
        );
        // The second thing every configuration move does (#143): a quorum
        // read opened against the superseded configuration asked a row that
        // need not intersect the successor's columns, so it may never
        // complete — the driver times it out and the client retries under
        // the configuration now believed.
        self.quorum_reads.abandon_superseded(self.acceptors_since);
    }

    /// Queue `msg` to every node of the pool except this one — the learner
    /// fan-out (commits, beats, catch-up), which reaches spares and removed
    /// members so every replica keeps the chosen log.
    pub(super) fn broadcast(&mut self, msg: Message) {
        let queued = self.pending_messages.len();
        self.pending_messages.push((Audience::Learners, msg));
        assert!(
            self.pending_messages.len() == queued + 1,
            "a broadcast queues one message"
        );
    }

    /// Queue `msg` to the one node `to`.
    pub(super) fn send(&mut self, to: NodeId, msg: Message) {
        let queued = self.pending_messages.len();
        self.pending_messages.push((Audience::Node(to), msg));
        assert!(
            self.pending_messages.len() == queued + 1,
            "a send queues one message"
        );
    }

    /// Queue one `Prepare` at `ballot` from `from_slot`, carrying `config`,
    /// to each of `targets` — the opening fan-out of a campaign, a Promise
    /// page request and a repair probe's straggler re-query alike.
    pub(super) fn send_prepare(
        &mut self,
        targets: impl IntoIterator<Item = NodeId>,
        ballot: Ballot,
        from_slot: Slot,
        config: Option<AcceptorConfig>,
    ) {
        // A Prepare runs this node's own campaign or probe, at a ballot its
        // own promise covers (it promised the ballot when it minted it).
        assert!(
            ballot <= self.acceptor.promised(),
            "a Prepare's ballot is one this node promised"
        );
        // The owner testimony covers this Prepare before it leaves.
        self.phase1_opened = self.phase1_opened.max(ballot);
        assert!(
            self.phase1_opened >= ballot,
            "the owner testimony covers every Prepare this node sends"
        );
        let prepare = Message::Prepare {
            reply_to: self.config.id,
            ballot,
            from_slot,
            config,
        };
        let queued = self.pending_messages.len();
        let mut sent = 0_usize;
        for to in targets {
            assert!(
                to != self.config.id,
                "a node never prepares itself over the wire"
            );
            self.send(to, prepare.clone());
            sent += 1;
        }
        assert!(
            self.pending_messages.len() == queued + sent,
            "one Prepare per target"
        );
    }

    /// The lowest slot this node still misses: the first faulty slot of the
    /// fold's hole, or else the first unchosen slot. A catch-up request asked
    /// below it is answered (the node holds that range now) and no longer
    /// opens the trim-point gate.
    pub(super) fn catch_up_need(&self) -> Slot {
        let first_unchosen = self.first_unchosen();
        let need = self
            .replica
            .fold_hole()
            .map_or(first_unchosen, |hole| hole.min(first_unchosen));
        assert!(
            need <= first_unchosen,
            "the need is at or below the first unchosen slot"
        );
        assert!(
            need >= self.acceptor.first_slot(),
            "a node never misses a slot below its floor"
        );
        need
    }

    /// The open catch-up request of this node (`catchup_asked`), if it still
    /// asks for a slot the node misses. A request below
    /// [`Self::catch_up_need`] is answered and dropped here.
    pub(super) fn open_catch_up(&mut self) -> Option<Slot> {
        let need = self.catch_up_need();
        if self.catchup_asked.is_some_and(|asked| asked < need) {
            self.catchup_asked = None;
        }
        assert!(
            self.catchup_asked.is_none_or(|asked| asked >= need),
            "an open request asks for a slot the node misses"
        );
        self.catchup_asked
    }

    /// A `CatchUpRequest` from this node for the decided range from
    /// `from_slot`. It records the lowest open slot asked (`catchup_asked`):
    /// a `TrimmedTo` is taken only as an answer to an open request.
    pub(super) fn catch_up_request(&mut self, from_slot: Slot) -> Message {
        let open = self.open_catch_up();
        self.catchup_asked = Some(open.map_or(from_slot, |a| a.min(from_slot)));
        assert!(
            self.catchup_asked.is_some_and(|a| a <= from_slot),
            "a request is recorded as asked"
        );
        Message::CatchUpRequest {
            from: self.config.id,
            from_slot,
        }
    }

    /// Whether `msg` is in this node's own name: a transport echo. The core
    /// never addresses itself ([`Audience::resolve`] leaves the sender
    /// out), so following one would let a node depose its own leadership
    /// with its own beat; `step` drops it, for every driver.
    ///
    /// [`Audience::resolve`]: crate::contract::message::Audience::resolve
    pub(super) fn is_own_echo(&self, msg: &Message) -> bool {
        msg.sender() == Party::Node(self.config.id)
    }

    /// Whether this node answers a request whose reply goes to `node`: a
    /// node of the pool, or, on a deployment with a replica tier, a replica
    /// (its id is outside the pool by construction; the driver routes it
    /// through the deployment map and drops an address it does not know).
    pub(super) fn is_reply_addressable(&self, node: NodeId) -> bool {
        let addressable = self.in_pool(node) || self.acceptors.has_replicas();
        if !self.acceptors.has_replicas() {
            assert!(
                addressable == self.in_pool(node),
                "without a replica tier only the pool is answered"
            );
        }
        addressable
    }

    /// Whether `party` is one this deployment can answer: a node of the
    /// pool ([`ColocatedNode::in_pool`]) or a proxy rank under the cap
    /// (`ProxyId(0..MAX_ROLE_COUNT)`) — the wire-hygiene boundary an
    /// `Accept`'s reply address is checked against (#142). The cap, not the
    /// count: the count is the configuration's, bound to the ballot (#754
    /// (counts off the format marker)), and an acceptor may not know the
    /// configuration of the ballot it votes in yet.
    pub(super) fn is_party_addressable(&self, party: Party) -> bool {
        match party {
            Party::Node(node) => self.in_pool(node),
            Party::Proxy(proxy) => proxy.is_in(MAX_ROLE_COUNT),
        }
    }

    /// Drop every volatile leadership and campaign state: the open campaign
    /// phases, the in-flight rounds, recovery, repair, the standing authority
    /// and the inherited origin. Shared by [`ColocatedNode::become_follower`] and a fresh
    /// campaign, so a leader that reconfigures abandons exactly what a
    /// deposed one does.
    ///
    /// The fence and the ack window die with the leadership (inside
    /// [`Proposer::abandon`]); quorum reads and already-served
    /// `pending_read_states` stay — a quorum read touches no leader state,
    /// and the driver drains the served ones this same batch.
    pub(super) fn clear_leadership_state(&mut self) {
        // Leadership state dies whole, the inherited origin included: a
        // demoted node holds no authority, so it can neither be a handoff
        // leader nor be counted as one by the invariants.
        self.leadership_origin = LeadershipOrigin::Elected;
        self.handoff_fence_elapsed = 0;
        self.proposer.abandon();
        self.matchmaking = None;
        self.gc = None;
        assert!(
            self.proposer.election().is_none(),
            "no campaign survives the clear"
        );
        assert!(
            self.proposer.rounds().is_empty(),
            "no round survives the clear"
        );
        assert!(
            self.proposer.recovery().is_none(),
            "no recovery survives the clear"
        );
    }

    /// Step down to Follower, abandoning any campaign or in-flight rounds, and
    /// ask the driver for a fresh randomized election timeout.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, leader = ?leader)))]
    pub(super) fn become_follower(&mut self, leader: Option<NodeId>) {
        self.role = NodeRole::Follower;
        self.leader = leader;
        self.clear_leadership_state();
        self.election_elapsed = 0;
        self.needs_election_timeout = true;
        assert!(
            self.proposer.probe().is_none(),
            "a follower holds no repair probe"
        );
        assert!(self.gc.is_none(), "a follower holds no GC campaign");
    }

    /// First slot not in the contiguous chosen prefix.
    pub(super) fn first_unchosen(&self) -> Slot {
        self.replica.first_unchosen()
    }

    // ---- settledness --------------------------------------------------------
    //
    // Two predicates over the Phase-1-shaped work a leadership may still
    // hold open, and the difference between them is deliberate.
    //
    // `phase1_work_open` is the *proposer's* half alone — an election
    // recovery or a CTRL repair probe, the two tallies that were reported by
    // a Phase-1 quorum and re-propose or decide on its strength. It is what
    // delegation asks (`may_delegate`: a proxy never runs the rounds a fresh
    // leadership's recovery depends on) and what the non-member step-down in
    // `tick` asks (a removed leader resigns once its recovery and repair
    // closed and its rounds decided).
    //
    // `leadership_settled` is the same question asked where the leadership
    // is moved or built on — a GC floor (`gc_covered`), a reconfiguration
    // (`reconfigure`) and a handoff (`can_relinquish`, which further
    // requires no local `faulty` record, a condition of its own). It used
    // to add the replica's application repair, which #186 deleted with the
    // application.

    /// Whether Phase-1-shaped work is open on the proposer: an election
    /// recovery or a repair probe (see the note above).
    pub(super) fn phase1_work_open(&self) -> bool {
        let open = self.proposer.recovery().is_some() || self.proposer.probe().is_some();
        // Phase-1-shaped work is a leadership's: it dies with it.
        if open {
            assert!(
                self.role == NodeRole::Leader,
                "only a leader holds Phase-1-shaped work"
            );
        }
        open
    }

    /// Whether the leadership is **settled**: no Phase-1-shaped work open
    /// on the proposer ([`ColocatedNode::phase1_work_open`]).
    pub(super) fn leadership_settled(&self) -> bool {
        !self.phase1_work_open()
    }
}
