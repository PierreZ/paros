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
//! it. Segments exist only with matchmakers (R10, D5), so a deployment
//! without them routes nothing, whatever its configuration names.
//!
//! The routed `Commit` carries a ballot that chose its slot: the ballot of
//! this node's own decision, or of a `Commit` it learned. The record of the
//! slot in the acceptor can stand at a higher ballot that chose nothing (a
//! later leader's recovery vote for the chosen command, with no quorum
//! behind it), so the record's ballot is never sent as a choosing one. A
//! slot this node learned with no choosing ballot (a catch-up replay, a
//! handoff's decided tail, a Phase-1 heal) is not routed, and its replicas
//! pull it.
//!
//! A slot that a follower folded and the leader of its time never routed is
//! not routed by the next leader either: today the `Commit` to the learners
//! (D2) reaches every replica, and a replica pulls what it missed. With pure
//! voters (step 8) that `Commit` goes away; **Open** (step 8): who routes such
//! a slot.

use std::collections::BTreeMap;

use super::ColocatedNode;
use crate::contract::{Audience, Message, Party};
use crate::replica::ChosenLog;
use crate::types::{Ballot, Slot};

impl ColocatedNode {
    /// Keep `ballot` as a ballot that chose `slot`, for the route. Only
    /// with matchmakers (R10), and only at or above the route mark: a slot
    /// below it is already past the route.
    pub(super) fn note_choosing_ballot(&mut self, slot: Slot, ballot: Ballot) {
        if !self.config.has_matchmakers() || slot < self.segment_routed {
            return;
        }
        self.choosing.entry(slot).or_insert(ballot);
        assert!(
            self.choosing.contains_key(&slot),
            "a noted slot holds a choosing ballot"
        );
    }

    /// Route the `Commit` of each slot the fold applied since the last
    /// route to the replicas of its segment, when this node leads, and
    /// move the mark to the fold.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn route_to_segment_replicas(&mut self) {
        let folded = self.replica.folded();
        let from = self.segment_routed.max(self.acceptor.first_slot());
        assert!(
            self.segment_routed <= folded,
            "the route mark never passes the fold"
        );
        self.segment_routed = folded;
        let below = std::mem::take(&mut self.choosing);
        let (routable, above): (BTreeMap<_, _>, BTreeMap<_, _>) =
            below.into_iter().partition(|(slot, _)| *slot < folded);
        self.choosing = above;
        assert!(
            self.choosing.keys().all(|slot| *slot >= folded),
            "the choosing ballots start at the route mark"
        );
        if !self.config.has_matchmakers() || !self.is_leader() {
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
            if let (Some(replicas), Some(&ballot)) = (replicas, routable.get(&slot)) {
                let Some((record, command)) = ChosenLog::record(&self.acceptor, slot) else {
                    panic!("a folded slot holds its record in the chosen log");
                };
                // A record never stands below a ballot that chose its slot
                // (`Acceptor::learn_chosen` raises it to the choosing one).
                assert!(
                    record >= ballot,
                    "a folded record stands at or above its choosing ballot"
                );
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
