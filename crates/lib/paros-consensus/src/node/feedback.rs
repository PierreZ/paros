//! The node's **feedback** ([`NodeFeedback`]): one snapshot of what it
//! believes, what it counted and what it concluded, for a driver or a
//! journal controller (#1564 (journal controller, version 1)). Read-only:
//! every field comes from an accessor of the node.

use crate::contract::feedback::{LeaderFeedback, NodeFeedback, NodeInsight};

use super::{BeliefSource, ColocatedNode, NodeRole};

impl ColocatedNode {
    /// What this node reports about itself, as plain data
    /// ([`crate::contract::feedback`]). Not durable, and in no order with
    /// the messages of a `Ready`: a driver reads it at its own pace.
    ///
    /// ```
    /// use paros_consensus::contract::MemStorage;
    /// use paros_consensus::journal::JournalIdentifier;
    /// use paros_consensus::{ColocatedNode, Config, Limits, NodeId};
    ///
    /// let ids = [NodeId(1), NodeId(2), NodeId(3)];
    /// let disk = MemStorage::new(Config::new(ids[0], JournalIdentifier::UNSET).with_peers(ids));
    /// let node = ColocatedNode::new(&disk, Limits::DEFAULT);
    /// let feedback = node.feedback();
    /// assert_eq!(feedback.id, NodeId(1));
    /// assert!(feedback.leading.is_none(), "a fresh node does not lead");
    /// ```
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn feedback(&self) -> NodeFeedback {
        let counters = self.counters();
        let leading = self.leader_feedback();
        let mut insights = Vec::new();
        if self.quorum_window_lost() {
            insights.push(NodeInsight::QuorumWindowLost);
        }
        if let Some(leading) = &leading {
            for &(node, windows) in &leading.missed {
                insights.push(NodeInsight::MemberSilent { node, windows });
            }
        }
        if !self.is_acceptor() {
            insights.push(NodeInsight::Outside);
        }
        if let Some(floor) = self.gc_floor_heard() {
            if self.may_retire(floor) {
                insights.push(NodeInsight::MayRetire { floor });
            } else if self.retirement_unchecked(floor) {
                insights.push(NodeInsight::RetirementUnchecked { floor });
            }
        }
        let feedback = NodeFeedback {
            id: self.config.id,
            ballot: self.ballot(),
            leader: self.leader(),
            acceptors: self.acceptors().clone(),
            since: self.acceptors_since(),
            heard: self.belief_source() == BeliefSource::Heard,
            pool: self.pool().to_vec(),
            retire_mark: self.retire_mark(),
            gc_floor_heard: self.gc_floor_heard(),
            leading,
            tick: self.ticks(),
            quorum_windows_held: counters.quorum_windows_held,
            quorum_lost_step_downs: counters.quorum_lost_step_downs,
            matchmaking_timeouts: counters.matchmaking_timeouts,
            retained_bytes: self.retained_bytes(),
            insights,
        };
        assert!(
            feedback.leading.is_some() == (self.role == NodeRole::Leader),
            "only a leader reports a leadership"
        );
        assert!(
            feedback.acceptors.is_drawn_from(&feedback.pool),
            "a reported configuration is drawn from the reported pool"
        );
        feedback
    }

    /// The part of the feedback that only a leader has.
    fn leader_feedback(&self) -> Option<LeaderFeedback> {
        let (prior, current) = self.configs_in_force()?;
        let prior: Vec<_> = prior.into_iter().cloned().collect();
        assert!(
            prior.iter().all(|c| c != current),
            "the configuration in force is never its own prior"
        );
        let missed = self.missed_windows();
        assert!(
            missed.iter().all(|(id, _)| current.contains(*id)),
            "a leader counts missed windows of its members only"
        );
        Some(LeaderFeedback {
            settled: self.is_settled(),
            prior,
            gc_effective: self
                .gc_effective()
                .map(|(floor, retired)| (floor, retired.to_vec())),
            missed,
        })
    }
}
