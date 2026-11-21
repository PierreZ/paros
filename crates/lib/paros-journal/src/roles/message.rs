//! The messages between the journal roles (§J4.1). Every message names the
//! ballot or the segment it belongs to, so a late or repeated copy is
//! judged against the state of its receiver.

use std::collections::BTreeMap;

use paros_consensus::{Ballot, Slot};

use crate::command::Command;
use crate::fold::Answer;
use crate::segment::SegmentId;

/// One page of an acceptor's promise: its votes and its faulty entries at or
/// after the slot that the `Prepare` asked from, and where the next page
/// starts when the suffix did not fit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromisePage {
    /// The readable votes, by slot.
    pub votes: BTreeMap<Slot, (Ballot, Command)>,
    /// The faulty entries (the ballot kept, the value lost), by slot. A
    /// faulty entry counts as a vote for the seal end (#1313).
    pub faulty: BTreeMap<Slot, Ballot>,
    /// The first slot of the next page, when this one was full.
    pub next: Option<Slot>,
}

/// A message between two roles of the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Leader to acceptor: Phase 1 at `ballot`, for the slots from `from`.
    Prepare {
        /// The leader's ballot.
        ballot: Ballot,
        /// The first slot the page reports.
        from: Slot,
    },
    /// Acceptor to leader: the promise at `ballot`, with one page.
    Promise {
        /// The ballot promised.
        ballot: Ballot,
        /// The slot the page starts at.
        from: Slot,
        /// The page.
        page: PromisePage,
    },
    /// Acceptor to leader: the acceptor holds a promise above `ballot`.
    Nack {
        /// The ballot refused.
        ballot: Ballot,
        /// The acceptor's promise.
        promised: Ballot,
    },
    /// Leader to acceptor: Phase 2 of `slot` at `ballot`.
    Accept {
        /// The leader's ballot.
        ballot: Ballot,
        /// The slot.
        slot: Slot,
        /// The command proposed.
        command: Command,
    },
    /// Acceptor to leader: the vote of `slot` at `ballot` is durable.
    Accepted {
        /// The ballot.
        ballot: Ballot,
        /// The slot.
        slot: Slot,
    },
    /// Leader or replica to replica: `slot` of `segment` chose `command`.
    Commit {
        /// The segment of the slot.
        segment: SegmentId,
        /// The slot.
        slot: Slot,
        /// The chosen command.
        command: Command,
    },
    /// Replica to leader: the replica applied `slot` (the command chosen
    /// there) with `answer`, and its fold is at `front`.
    Applied {
        /// The segment of the slot.
        segment: SegmentId,
        /// The slot.
        slot: Slot,
        /// The command the slot chose.
        command: Command,
        /// The answer of the fold.
        answer: Answer,
    },
    /// Replica to a peer of its set: send the chosen slots of `segment`
    /// from `from`.
    Pull {
        /// The segment.
        segment: SegmentId,
        /// The first slot the asker misses.
        from: Slot,
    },
}
