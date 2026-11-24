//! The messages between the journal roles (§J4.1). Every message names the
//! ballot or the segment it belongs to, so a late or repeated copy is
//! judged against the state of its receiver.

use std::collections::BTreeMap;

use paros_consensus::journal::Seq;
use paros_consensus::{Ballot, NodeId, Slot};

use crate::command::Command;
use crate::fold::{Answer, JournalState};
use crate::segment::SegmentId;

use super::PersistId;

/// One page of an acceptor's promise: its votes and its faulty entries at or
/// after the slot that the `Prepare` asked from, and where the next page
/// starts when the suffix did not fit.
///
/// The leader folds a page by the core's paging rule
/// (`paros_consensus::proposer::PromiseTally`): the page starts at the
/// acceptor's cursor, holds at most `PROMISE_BATCH` votes and faulty entries
/// at different slots, and a page with a `next` holds at least one entry,
/// all below `next`. The dropped ranges do not count as entries. So a page of
/// dropped ranges only can never carry a `next`: the leader refuses it, and
/// asks the acceptor again from the same cursor at every tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromisePage {
    /// The readable votes, by slot.
    pub votes: BTreeMap<Slot, (Ballot, Command)>,
    /// The faulty entries (the ballot kept, the value lost), by slot. A
    /// faulty entry counts as a vote for the seal end (#1313).
    pub faulty: BTreeMap<Slot, Ballot>,
    /// The first slot of the next page, when this one was full.
    pub next: Option<Slot>,
    /// The slots whose votes the acceptor dropped (§J3.2): each range
    /// `start .. below` of one segment, by its start. Every page carries
    /// them all. A dropped slot is chosen and applied at `F + 1` replicas
    /// of its set: the leader proposes nothing there, and the seal end
    /// counts it as a vote.
    pub dropped: BTreeMap<Slot, Slot>,
}

/// The replica's tag of one leaderless read (§J2.5): the acceptors echo it
/// in their `PreReadAck`, and the replica counts an answer only for the
/// read that carries the same tag.
///
/// The tag is unique across the restarts of a replica. `boot` is the id of
/// the first batch of the replica's incarnation (written empty by its first
/// read when it has written none), and every `PreRead` waits on it. A later incarnation
/// starts above every durable batch, so its `boot` is above the `boot` of
/// every earlier incarnation that sent a `PreRead`. `number` counts the
/// reads of one incarnation. So an answer to a read of an earlier
/// incarnation, still in flight at a restart, never counts for a new read:
/// it reports a watermark from before the restart, and a write
/// acknowledged since then would be missing from the page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadTag {
    /// The first batch of the replica's incarnation.
    pub boot: PersistId,
    /// The read's number in that incarnation, from 1.
    pub number: u64,
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
        /// The segment `ballot` is bound to. The acceptor keeps it as its
        /// belief: a leaderless read of an older segment that meets it ends
        /// (§J3.3, the belief half of the read fence).
        segment: SegmentId,
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
    /// Leader to proxy leader: run the Phase 2 of `slot` at `ballot` with
    /// `command` on `column`, and commit it to `replicas`, the replica set
    /// of `segment`.
    Delegate {
        /// The leader's ballot.
        ballot: Ballot,
        /// The slot.
        slot: Slot,
        /// The command proposed.
        command: Command,
        /// The acceptors of the column of the leader's grid that writes the
        /// slot: `2F + 1`, a majority of them decides.
        column: Vec<NodeId>,
        /// The segment of the slot.
        segment: SegmentId,
        /// The replica set of the segment.
        replicas: Vec<NodeId>,
    },
    /// Proxy leader to leader: `slot` is chosen at `ballot`.
    Chosen {
        /// The ballot.
        ballot: Ballot,
        /// The slot.
        slot: Slot,
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
    /// Replica to acceptor: the leaderless read `read` of `segment` asks for
    /// the acceptor's vote watermark (§J2.5, §J3.3).
    PreRead {
        /// The segment of the reader.
        segment: SegmentId,
        /// The replica's tag of the read.
        read: ReadTag,
    },
    /// Acceptor to replica: the answer to a `PreRead`. It waits on the
    /// acceptor's last batch, so the votes it reports are durable.
    PreReadAck {
        /// The segment of the reader.
        segment: SegmentId,
        /// The replica's tag of the read, echoed.
        read: ReadTag,
        /// The highest slot the acceptor voted in, if any.
        watermark: Option<Slot>,
        /// The newest segment a ballot that the acceptor promised since its
        /// start is bound to: the fence of the read. An acceptor that knows
        /// none since its start does not answer.
        bound: SegmentId,
    },
    /// Leader or new replica to a replica of `segment`: does it hold every
    /// slot of the sealed segment? The leader counts the holders for the
    /// retire (§J3.1); a new replica of a heal looks for a source that holds
    /// the whole segment (#1314).
    Holding {
        /// The sealed segment.
        segment: SegmentId,
    },
    /// Replica to the asker of `Holding`: whether its disk holds every slot
    /// of `segment` up to the end of the done seal. It waits on the
    /// replica's last batch, so a "full" answer is durable.
    Holds {
        /// The segment.
        segment: SegmentId,
        /// Whether the replica holds every slot of the sealed segment.
        full: bool,
    },
    /// Leader to replica: report the compaction point of `segment`
    /// (§J3.2). The leader asks at each tick.
    Compaction {
        /// The segment.
        segment: SegmentId,
    },
    /// Replica to leader: the replica dropped the slots of `segment` below
    /// `compacted`, and keeps the answers of the slots from `answers`. It
    /// waits on the replica's last batch, so the compaction is durable.
    CompactedTo {
        /// The segment.
        segment: SegmentId,
        /// The compaction point: the replica applied every slot below it
        /// and dropped it from its log.
        compacted: Slot,
        /// The first slot whose answer the replica can give: at or above
        /// `compacted`.
        answers: Slot,
    },
    /// Leader to acceptor: drop the votes of the slots `start .. below` of
    /// `segment` (§J3.2, #1318). The leader sends it once `F + 1` replicas
    /// of the segment's set reported a compaction point at or above
    /// `below`.
    Compact {
        /// The segment.
        segment: SegmentId,
        /// The first slot of the segment.
        start: Slot,
        /// The slot below which the votes go.
        below: Slot,
    },
    /// Leader to replica: send the command that `slot` of `segment` chose,
    /// if the replica applied it (CTRL, §J5.3). The leader asks for each
    /// slot that its Phase 1 left blocked.
    Learn {
        /// The segment of the slot.
        segment: SegmentId,
        /// The blocked slot.
        slot: Slot,
    },
    /// Replica to the asker of `Learn`: `slot` of `segment` chose
    /// `command`.
    Learned {
        /// The segment of the slot.
        segment: SegmentId,
        /// The slot.
        slot: Slot,
        /// The chosen command.
        command: Command,
    },
    /// Leader to acceptor: `slot` chose `command` at `ballot` (CTRL repair,
    /// §J5.3). An acceptor whose vote at `slot` is faulty writes `command`
    /// at the higher of `ballot` and the ballot of the faulty vote, in one
    /// write; any other acceptor ignores it.
    Repair {
        /// The ballot whose Phase 2 chose the command.
        ballot: Ballot,
        /// The slot.
        slot: Slot,
        /// The chosen command.
        command: Command,
    },
    /// Replica to a peer of its set: the answer to a `Pull` below the
    /// replica's compaction point (the trim-point jump). It carries the
    /// compaction point, the fold there and the trim floor of the fold that
    /// chose the point: no bytes and no ballot.
    TrimmedTo {
        /// The segment.
        segment: SegmentId,
        /// The compaction point.
        slot: Slot,
        /// The fold at the compaction point.
        state: JournalState,
        /// The trim floor that chose the point. Every write that a slot
        /// below the point holds lies below it, so the fold that starts at
        /// the point judges a position at or above it as the whole log
        /// does.
        floor: Seq,
    },
}
