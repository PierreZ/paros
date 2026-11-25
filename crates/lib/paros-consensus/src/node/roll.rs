//! **The `Roll`** (`docs/architecture/04-7-segments.md` §4.7): a driver call
//! that cuts the log into a new segment.
//!
//! [`ColocatedNode::roll`] proposes a [`Roll`] to the tail segment
//! plus one. The judgement is the apply path's (R2, R3): the `Roll` takes
//! effect only when the fold of the chosen prefix finds it valid, so a
//! proposal that guessed the tail wrong changes nothing and costs one slot.
//! The leader takes the tail from what it knows of the log (R4c): its own
//! fold, the chosen slots above that fold, the `Roll`s of its open rounds,
//! and the values its Phase 1 recovered that it has not proposed again. It never takes it from the
//! chain at the matchmakers, which is behind the log.
//!
//! A roll needs matchmakers (D5): on a plain deployment it is refused, so a
//! plain log never decides a `Roll` and stays in its first segment (R10,
//! R11). There is no pause (R4a, dropped on 2026-11-25): the leader pipelines
//! across a `Roll` like across any other command.

use std::collections::BTreeMap;

use super::{ColocatedNode, Delegation, NodeRole, ProposeResult};
use crate::journal::{Command, ReplicaSet, Roll, SegmentId};
use crate::types::{NodeId, Slot};

/// The answer to [`ColocatedNode::roll`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RollResult {
    /// This node is not the leader: ask the hinted node (`None` when the
    /// leader is unknown).
    NotLeader(Option<NodeId>),
    /// The `Roll` to `next` is admitted at `slot`. It takes effect when the
    /// fold of the chosen prefix reaches the slot and finds it valid (the
    /// slot's outcome is `Rolled` or `RollIgnored`).
    Accepted {
        /// The slot of the `Roll`.
        slot: Slot,
        /// The segment the `Roll` opens.
        next: SegmentId,
    },
    /// The roll was refused before anything was proposed.
    Refused(RollRefusal),
}

/// Why [`ColocatedNode::roll`] proposed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollRefusal {
    /// The deployment has no matchmakers (D5): a roll needs the matchmakers
    /// that will hold the chain of segments.
    NoMatchmakers,
}

impl ColocatedNode {
    /// Cut the log: propose a `Roll` to the tail segment plus one, with the
    /// replicas of the next segment (`None` keeps the replicas of the segment
    /// it ends, D4).
    ///
    /// A plain deployment refuses ([`RollRefusal::NoMatchmakers`]), and so
    /// does a node that is not the leader. Nothing waits on the `Roll`: the
    /// leader goes on proposing above it (no pause).
    ///
    /// ```
    /// use paros_consensus::contract::MemStorage;
    /// use paros_consensus::journal::JournalIdentifier;
    /// use paros_consensus::node::{RollRefusal, RollResult};
    /// use paros_consensus::{ColocatedNode, Config, Limits, NodeId};
    ///
    /// // Plain Multi-Paxos: no matchmakers, so no segment but the first.
    /// let config = Config::new(NodeId(1), JournalIdentifier::UNSET).with_peers([NodeId(1)]);
    /// let mut node = ColocatedNode::new(&MemStorage::new(config), Limits::DEFAULT);
    /// node.campaign();
    /// assert!(node.is_leader());
    /// assert_eq!(node.roll(None), RollResult::Refused(RollRefusal::NoMatchmakers));
    /// ```
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn roll(&mut self, replicas: Option<ReplicaSet>) -> RollResult {
        self.assert_landed();
        if !self.config.has_matchmakers() {
            return RollResult::Refused(RollRefusal::NoMatchmakers);
        }
        let next = self.tail_segment().successor();
        let tail = self.replica.journal().segment.id;
        assert!(next > tail, "a roll names a segment past the folded tail");
        let result =
            self.propose_command_at(Command::Roll(Roll { next, replicas }), Delegation::Auto);
        let answer = match result {
            ProposeResult::NotLeader(hint) => RollResult::NotLeader(hint),
            ProposeResult::Accepted(slot) => {
                assert!(self.role == NodeRole::Leader, "only a leader admits a Roll");
                assert!(
                    self.tail_segment() == next,
                    "an admitted Roll is the tail a later roll builds on"
                );
                RollResult::Accepted { slot, next }
            }
        };
        self.assert_invariants();
        answer
    }

    /// The tail segment as this node knows the log (R4c): the fold of its
    /// chosen prefix, then each later `Roll` it knows, in slot order, when it
    /// names the tail plus one ([`SegmentId::after`]). It knows a `Roll`
    /// chosen, in an open round, or recovered by its Phase 1 and not yet
    /// proposed again (a bounded recovery opens its rounds a page at a
    /// time). A guess that is wrong costs a `Roll` the fold ignores, never
    /// safety.
    pub(super) fn tail_segment(&self) -> SegmentId {
        let folded = self.replica.journal().segment.id;
        let after = self.replica.folded();
        let chosen = self.replica.chosen().range(after..).filter_map(|slot| {
            self.replica
                .chosen_at(&self.acceptor, *slot)
                .map(|c| (*slot, c))
        });
        let open = self
            .proposer
            .rounds()
            .iter()
            .filter(|(slot, _)| **slot >= after)
            .map(|(slot, round)| (*slot, round.command()));
        let recovered = self
            .proposer
            .recovery()
            .into_iter()
            .flat_map(|recovery| recovery.recovered().range(after..))
            .map(|(slot, command)| (*slot, command));
        // A chosen value wins over an open round at the same slot, and an
        // open round over a recovered value it has not proposed yet.
        let mut later: BTreeMap<Slot, &Command> = recovered.collect();
        later.extend(open);
        later.extend(chosen);
        let known = later.len();
        let mut tail = folded;
        for command in later.into_values() {
            if let Command::Roll(roll) = command {
                tail = tail.after(roll);
            }
        }
        assert!(
            tail >= folded,
            "the known tail is at or past the folded one"
        );
        assert!(
            usize::try_from(tail.0 - folded.0).is_ok_and(|rolls| rolls <= known),
            "each step of the known tail is one Roll the node knows above its fold"
        );
        tail
    }
}
