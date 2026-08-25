use super::{Message, NodeId, NodeRole, RawNode, Slot};

impl RawNode {
    // ---- helpers ----------------------------------------------------------

    /// Quorum size of the cluster, per the configured [`crate::QuorumSystem`]
    /// (membership includes self).
    pub(super) fn quorum(&self) -> usize {
        let n = self.config.peers.len();
        let q = self.config.quorum_system.quorum_size(n);
        // Paxos safety rests on any two quorums intersecting; for the majority
        // system that is `2q > n`. A future quorum system that breaks this must
        // fail loudly here, not silently choose two values for one slot.
        assert!(q >= 1, "a quorum requires at least one acceptor");
        assert!(2 * q > n, "any two quorums must intersect");
        q
    }
    /// Queue `msg` to every member except this node.
    pub(super) fn broadcast(&mut self, msg: &Message) {
        let me = self.config.id;
        let targets: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&p| p != me)
            .collect();
        for to in targets {
            self.pending_messages.push((to, msg.clone()));
        }
    }

    /// Step down to Follower, abandoning any campaign or in-flight rounds, and
    /// ask the driver for a fresh randomized election timeout.
    pub(super) fn become_follower(&mut self, leader: Option<NodeId>) {
        self.role = NodeRole::Follower;
        self.leader = leader;
        self.election = None;
        self.leader_recovery = None;
        self.resend_cursor = None;
        self.proposer.clear();
        // Unconfirmed read rounds die with the leadership; already-confirmed
        // `pending_read_states` stay — they were valid at their linearization
        // point and the driver drains them this same batch.
        self.read_rounds.clear();
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
