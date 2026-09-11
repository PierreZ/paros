use super::{
    Audience, Ballot, ColocatedNode, Command, LeadershipOrigin, Message, NodeId, NodeRole, Party,
    Slot,
};
use crate::membership::AcceptorConfig;

impl ColocatedNode {
    // ---- helpers ----------------------------------------------------------

    /// Record `(ballot, command)` in the acceptor's log. The one path the
    /// wiring takes to [`crate::acceptor::Acceptor::record_accepted`] (which
    /// tallies an in-place repair of a faulty entry itself).
    pub(super) fn record_accepted(&mut self, slot: Slot, ballot: Ballot, command: Command) {
        self.acceptor
            .record_accepted(slot, ballot, command, &mut self.pending_writes);
    }

    /// Whether `node` is in the addressable pool — the wire-hygiene boundary
    /// every handler draws around a sender: a misrouted or misconfigured id
    /// is never followed, counted, or replied to. Membership of a
    /// *configuration* is a separate, per-configuration question.
    pub(super) fn in_pool(&self, node: NodeId) -> bool {
        self.config.pool().binary_search(&node).is_ok()
    }

    /// The configuration a `Prepare` at this node's ballot carries: the
    /// registered `C_b` on a matchmaker deployment (so every acceptor it
    /// reaches learns it), nothing on plain Multi-Paxos (whose `Prepare` is
    /// byte-for-byte today's).
    pub(super) fn phase1_wire_config(&self) -> Option<AcceptorConfig> {
        if !self.config.has_matchmakers() {
            return None;
        }
        self.proposer
            .election()
            .map(|e| e.config().clone())
            .or_else(|| Some(self.acceptors.clone()))
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
        if !config.members().iter().all(|m| self.in_pool(*m)) {
            return;
        }
        self.acceptors = config;
        self.acceptors_since = ballot;
        self.record_membership();
    }

    /// Record that `acceptors`/`acceptors_since` just moved: if the new
    /// configuration names this node, the ballot it is bound to is the newest
    /// at which this node was a member. Called from every assignment to
    /// `acceptors_since` — [`ColocatedNode::learn_config`], `try_become_leader`, a
    /// handoff install, and the adoption of an effective configuration — so
    /// [`ColocatedNode::may_retire`] never under-reports the membership it must
    /// outlive.
    pub(super) fn record_membership(&mut self) {
        if self.is_acceptor() {
            self.last_member_ballot = self.last_member_ballot.max(self.acceptors_since);
        }
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
    pub(super) fn broadcast(&mut self, msg: &Message) {
        self.pending_messages
            .push((Audience::Learners, msg.clone()));
    }

    /// Whether `party` is one this deployment can answer: a node of the
    /// pool ([`ColocatedNode::in_pool`]) or a proxy of the deployment
    /// (`ProxyId(0..proxy_count)`) — the wire-hygiene boundary an `Accept`'s
    /// reply address is checked against (#142).
    pub(super) fn is_party_addressable(&self, party: Party) -> bool {
        match party {
            Party::Node(node) => self.in_pool(node),
            Party::Proxy(proxy) => proxy.is_in(self.config.proxy_count),
        }
    }

    /// Drop every volatile leadership and campaign state: the open campaign
    /// phases, the in-flight rounds, recovery, repair, read rounds and the
    /// inherited origin. Shared by [`ColocatedNode::become_follower`] and a fresh
    /// campaign, so a leader that reconfigures abandons exactly what a
    /// deposed one does.
    ///
    /// Unconfirmed read rounds die with the leadership (inside
    /// [`Proposer::abandon`], with the fence and the ack window they belong
    /// to); already-confirmed `pending_read_states` stay — they were valid at
    /// their linearization point and the driver drains them this same batch.
    pub(super) fn clear_leadership_state(&mut self) {
        // Leadership state dies whole, the inherited origin included: a
        // demoted node holds no authority, so it can neither be a handoff
        // leader nor be counted as one by the invariants.
        self.leadership_origin = LeadershipOrigin::Elected;
        self.handoff_fence_elapsed = 0;
        self.proposer.abandon();
        self.matchmaking = None;
        self.gc = None;
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
    }

    /// First slot not in the contiguous chosen prefix.
    pub(super) fn first_unchosen(&self) -> Slot {
        self.replica.first_unchosen()
    }
}
