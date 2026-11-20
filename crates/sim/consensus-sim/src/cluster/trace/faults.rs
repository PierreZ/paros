//! The faulty-entry events of a cluster walk, for
//! `specs/tla/node/FaultsTrace.tla`: the events of `TruncationTrace` and
//! the faulty entries of CTRL of `specs/tla/node/Node.tla` (#1231 (traced
//! walks reach the rules)).
//!
//! Two seams. A boot that reads a faulty record from its disk (`Rot`): the
//! slot keeps its ballot and loses its value. And each page of a promise
//! (`Page`): a promise is paged (`Limits::promise_page`), and it reports
//! the faulty records beside the readable ones. A page is recorded once:
//! the first page of an answer, each continuation, and an answer again at
//! the ballot the acceptor promised already (the repair probe asks the
//! acceptors that did not answer, and a dropped answer is asked again).
//! And the hole a node's fold stops at (`Hole`), where its campaign starts,
//! recorded when it moves: at a boot and before a batch.

use std::collections::BTreeSet;

use paros_consensus::{Ballot, Message, NodeId, Slot};

use super::{ClusterEvent, Cmd, Tracer};

/// The steps of `FaultsTrace` beside those of `TruncationTrace`: it reads
/// the pages of a promise in place of the promise that raised it.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Page { .. } | ClusterEvent::Rot { .. } | ClusterEvent::Hole { .. }
    )
}

/// One page of a promise, as the tracer recorded it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Seen {
    acceptor: NodeId,
    ballot: Ballot,
    from: Slot,
    next: Option<Slot>,
    votes: Vec<(Slot, Ballot, Cmd)>,
    faulty: Vec<(Slot, Ballot)>,
}

impl Tracer {
    /// `promise` (paged): the page `message` is, when it is a `Promise`
    /// that the tracer did not record before.
    pub(super) fn page(&mut self, message: &Message) -> Option<ClusterEvent> {
        let Message::Promise {
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
            ..
        } = message
        else {
            return None;
        };
        let seen = Seen {
            acceptor: *from,
            ballot: *ballot,
            from: *from_slot,
            next: *next_from_slot,
            votes: accepted
                .iter()
                .map(|(slot, (at, command))| (*slot, *at, Cmd::of(command)))
                .collect(),
            faulty: faulty.iter().map(|(slot, at)| (*slot, *at)).collect(),
        };
        if !self.pages.insert(seen.clone()) {
            return None;
        }
        Some(ClusterEvent::Page {
            acceptor: seen.acceptor,
            ballot: seen.ballot,
            from: seen.from,
            next: seen.next,
            votes: seen.votes,
            faulty: seen.faulty,
        })
    }

    /// `rot`: the boot of node `id` read its record of `slot` faulty, with
    /// `ballot` known. A later vote of the same value heals the record: the
    /// tracer forgets the vote it mirrored, so that vote is a step again.
    pub(super) fn rot(&mut self, id: NodeId, slot: Slot, ballot: Ballot) -> ClusterEvent {
        self.votes.remove(&(id, slot));
        ClusterEvent::Rot {
            node: id,
            slot,
            ballot,
        }
    }
}

impl Tracer {
    /// `hole`: the fold of node `id` stops at `hole` now, when the tracer
    /// recorded another hole before. A restart forgets the hole.
    pub(in crate::cluster) fn hole(
        &mut self,
        id: NodeId,
        hole: Option<Slot>,
    ) -> Option<ClusterEvent> {
        let before = self.holes.get(&id).copied();
        if before == hole {
            return None;
        }
        match hole {
            Some(slot) => self.holes.insert(id, slot),
            None => self.holes.remove(&id),
        };
        Some(ClusterEvent::Hole {
            node: id,
            slot: hole,
        })
    }
}

/// The pages a tracer recorded.
pub(super) type Pages = BTreeSet<Seen>;
