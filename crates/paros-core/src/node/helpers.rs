use super::{Ballot, LeadershipOrigin, Message, NodeId, NodeRole, RawNode, Slot};
use crate::matchmaker::AcceptorConfig;

impl RawNode {
    // ---- helpers ----------------------------------------------------------

    /// The Phase-2 quorum of the active configuration ([`RawNode::acceptors`]):
    /// what accept rounds, read rounds and `CheckQuorum` are counted against.
    /// Intersection is asserted per configuration inside
    /// [`AcceptorConfig::quorum_size`].
    pub(super) fn phase2_quorum(&self) -> usize {
        self.acceptors.quorum_size()
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
        self.election
            .as_ref()
            .map(|e| e.config.clone())
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
        if !config.members.iter().all(|m| self.in_pool(*m)) {
            return;
        }
        self.acceptors = config;
        self.acceptors_since = ballot;
    }

    /// Queue `msg` to every node of the pool except this one — the learner
    /// fan-out (commits, beats, catch-up), which reaches spares and removed
    /// members so every replica keeps the chosen log.
    pub(super) fn broadcast(&mut self, msg: &Message) {
        let me = self.config.id;
        let targets: Vec<NodeId> = self
            .config
            .pool()
            .iter()
            .copied()
            .filter(|&p| p != me)
            .collect();
        for to in targets {
            self.pending_messages.push((to, msg.clone()));
        }
    }

    /// Queue `msg` to every member of the active configuration except this
    /// node — the Phase-2 fan-out: a removed node is never contacted for a new
    /// ballot's accepts.
    pub(super) fn broadcast_acceptors(&mut self, msg: &Message) {
        let me = self.config.id;
        let targets: Vec<NodeId> = self
            .acceptors
            .members
            .iter()
            .copied()
            .filter(|&p| p != me)
            .collect();
        for to in targets {
            self.pending_messages.push((to, msg.clone()));
        }
    }

    /// Drop every volatile leadership and campaign state: the open campaign
    /// phases, the in-flight rounds, recovery, repair, read rounds and the
    /// inherited origin. Shared by [`RawNode::become_follower`] and a fresh
    /// campaign, so a leader that reconfigures abandons exactly what a
    /// deposed one does.
    pub(super) fn clear_leadership_state(&mut self) {
        // Leadership state dies whole, the inherited origin included: a
        // demoted node holds no authority, so it can neither be a handoff
        // leader nor be counted as one by the invariants.
        self.leadership_origin = LeadershipOrigin::Elected;
        self.handoff_fence_elapsed = 0;
        self.election = None;
        self.matchmaking = None;
        self.leader_recovery = None;
        self.repair_probe = None;
        self.repair_elapsed = 0;
        self.resend_cursor = None;
        self.proposer.clear();
        // Unconfirmed read rounds die with the leadership; already-confirmed
        // `pending_read_states` stay — they were valid at their linearization
        // point and the driver drains them this same batch.
        self.read_rounds.clear();
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
        match self.hard_state.chosen_index {
            Some(s) => Slot(s.0 + 1),
            None => Slot(0),
        }
    }
}
