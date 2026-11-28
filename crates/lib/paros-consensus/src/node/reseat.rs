//! **The `SetReplicas`** (D10 of `docs/architecture/04-7-segments.md`
//! §4.7): a driver call that gives the open segment new replicas.
//!
//! [`ColocatedNode::set_replicas`] proposes a [`SetReplicas`] of the tail
//! segment as this node knows the log ([`ColocatedNode::tail_segment`]).
//! The judgement is the apply path's: the fold finds it valid only when it
//! names the tail, so a proposal that a `Roll` overtook changes nothing and
//! costs one slot. From the slot after a valid one, the leader routes each
//! `Commit` to the new set (`node/route.rs`), a new member pulls the slots
//! of the segment it lacks, and a removed member stops at it and answers a
//! read above it with "moved" (`specs/tla/segment-reseat/`).
//!
//! Like a roll, it needs matchmakers (D5): on a plain deployment it is
//! refused, so a plain log never decides one.

use super::{ColocatedNode, Delegation, NodeRole, ProposeResult};
use crate::journal::{Command, ReplicaSet, SegmentId, SetReplicas};
use crate::types::{NodeId, Slot};

/// The answer to [`ColocatedNode::set_replicas`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetReplicasResult {
    /// This node is not the leader: ask the hinted node (`None` when the
    /// leader is unknown).
    NotLeader(Option<NodeId>),
    /// The `SetReplicas` of `segment` is admitted at `slot`. It takes
    /// effect when the fold of the chosen prefix reaches the slot and finds
    /// it valid (the slot's outcome is `Reseated`, else `ReseatIgnored` or
    /// `ReseatRemoved`).
    Accepted {
        /// The slot of the `SetReplicas`.
        slot: Slot,
        /// The segment it names.
        segment: SegmentId,
    },
    /// The call was refused before anything was proposed.
    Refused(SetReplicasRefusal),
}

/// Why [`ColocatedNode::set_replicas`] proposed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetReplicasRefusal {
    /// The deployment has no matchmakers (D5): segments need the matchmakers
    /// that hold their chain.
    NoMatchmakers,
}

impl ColocatedNode {
    /// Give the open segment new replicas: propose a `SetReplicas` of the
    /// tail segment as this node knows the log.
    ///
    /// A plain deployment refuses ([`SetReplicasRefusal::NoMatchmakers`]),
    /// and so does a node that is not the leader. Nothing waits on it: the
    /// leader goes on proposing above it.
    ///
    /// ```
    /// use paros_consensus::contract::MemStorage;
    /// use paros_consensus::journal::{JournalIdentifier, ReplicaSet};
    /// use paros_consensus::node::{SetReplicasRefusal, SetReplicasResult};
    /// use paros_consensus::{ColocatedNode, Config, Limits, NodeId};
    ///
    /// // Plain Multi-Paxos: no matchmakers, so no replica set changes.
    /// let config = Config::new(NodeId(1), JournalIdentifier::UNSET).with_peers([NodeId(1)]);
    /// let mut node = ColocatedNode::new(&MemStorage::new(config), Limits::DEFAULT);
    /// node.campaign();
    /// assert!(node.is_leader());
    /// assert_eq!(
    ///     node.set_replicas(ReplicaSet::new([NodeId(7)])),
    ///     SetReplicasResult::Refused(SetReplicasRefusal::NoMatchmakers),
    /// );
    /// ```
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn set_replicas(&mut self, replicas: ReplicaSet) -> SetReplicasResult {
        self.assert_landed();
        if !self.config.has_matchmakers() {
            return SetReplicasResult::Refused(SetReplicasRefusal::NoMatchmakers);
        }
        let segment = self.tail_segment();
        assert!(
            segment >= self.replica.journal().segment.id,
            "a SetReplicas names a segment at or past the folded tail"
        );
        let command = Command::SetReplicas(SetReplicas { segment, replicas });
        let answer = match self.propose_command_at(command, Delegation::Auto) {
            ProposeResult::NotLeader(hint) => SetReplicasResult::NotLeader(hint),
            ProposeResult::Accepted(slot) => {
                assert!(
                    self.role == NodeRole::Leader,
                    "only a leader admits a SetReplicas"
                );
                // A `SetReplicas` moves no segment, so the known tail stays.
                assert!(
                    self.tail_segment() == segment,
                    "an admitted SetReplicas keeps the known tail"
                );
                SetReplicasResult::Accepted { slot, segment }
            }
        };
        self.assert_invariants();
        answer
    }
}
