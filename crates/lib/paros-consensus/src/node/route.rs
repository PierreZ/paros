//! The **route of a `Commit` to the replicas of its segment** (R6 and R4d
//! of `docs/architecture/04-7-segments.md` §4.7).
//!
//! A `Commit` goes to the learners as it always did (D2), from the decision
//! itself. A leader also sends the `Commit` of each slot to the replicas of
//! the segment of that slot ([`Audience::ReplicasOf`]), but only once its own
//! fold applied every slot below it (R4d): the segment and its replicas are
//! then read from the fold of a chosen prefix, so they are the true ones. A
//! `Commit` routed on a proposal can reach the replicas of a segment that a
//! later ballot never chose (the check `route_propose` of
//! `specs/tla/segment-roll/`). The walk is the only source: a slot this node
//! folded while it was not the leader is not routed, and its replicas pull
//! it. The plain deployment names no replicas, and routes nothing.

use super::ColocatedNode;
use crate::contract::{Audience, Message, Party};
use crate::replica::ChosenLog;
use crate::types::Slot;

impl ColocatedNode {
    /// Route the `Commit` of each slot the fold applied since the last
    /// route to the replicas of its segment, when this node leads, and
    /// move the mark to the fold.
    pub(super) fn route_to_segment_replicas(&mut self) {
        let folded = self.replica.folded();
        let from = self.segment_routed.max(self.acceptor.first_slot());
        assert!(
            self.segment_routed <= folded,
            "the route mark never passes the fold"
        );
        self.segment_routed = folded;
        if !self.is_leader() {
            return;
        }
        let me = self.config.id;
        let mut slot = from;
        while slot < folded {
            let state = self.replica.journal_at(slot);
            let replicas = state
                .replicas
                .clone()
                .or_else(|| self.config.segment_replicas.clone());
            if let Some(replicas) = replicas {
                let Some((ballot, command)) = ChosenLog::record(&self.acceptor, slot) else {
                    panic!("a folded slot holds its record in the chosen log");
                };
                let command = command.clone();
                self.pending_messages.push((
                    Audience::ReplicasOf {
                        segment: state.segment.id,
                        replicas,
                    },
                    Message::Commit {
                        from: Party::Node(me),
                        ballot,
                        slot,
                        command,
                    },
                ));
            }
            slot = Slot(slot.0 + 1);
        }
        assert!(
            self.segment_routed == self.replica.folded(),
            "the route mark stands at the fold"
        );
    }
}
