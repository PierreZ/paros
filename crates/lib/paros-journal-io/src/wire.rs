//! The names that cross the boundary between the [`Host`](crate::Host) and
//! its driver: the address of a role, a message between two roles, and the
//! time of a step.
//!
//! A role instance is named by its journal and its node: one process serves
//! many journals, and in each journal one node runs one kind of role (an
//! acceptor, a segment replica or a leader). A replica node holds one
//! replica for each segment whose replica set names it, as an acceptor node
//! serves every segment whose grid names it: a heal keeps the live members
//! of a replica set (§J6.2), and the kept node starts the new segment beside
//! its copy of the old one. So the address of a replica also names its
//! segment. The transport maps a node to a process; the journal roles name
//! nodes only.

use paros_consensus::journal::JournalId;
use paros_journal::roles::Message;
use paros_journal::{NodeId, SegmentId};

/// One role instance: a node of one journal, and for a segment replica its
/// segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address {
    /// The journal.
    pub journal: JournalId,
    /// The node of the role in that journal.
    pub node: NodeId,
    /// The segment of a replica; `None` for an acceptor and a leader, and
    /// for "the newest replica of the node" in a lookup.
    pub segment: Option<SegmentId>,
}

impl Address {
    /// The role `node` of `journal`: an acceptor or a leader, or the newest
    /// replica of the node in a lookup.
    #[must_use]
    pub fn new(journal: JournalId, node: NodeId) -> Self {
        Self {
            journal,
            node,
            segment: None,
        }
    }

    /// The replica of `segment` at `node` of `journal`.
    ///
    /// # Panics
    ///
    /// When `segment` is 0: segment ids start at 1, and 0 is the key of a
    /// role with no segment ([`crate::layout::Key`]).
    #[must_use]
    pub fn replica(journal: JournalId, node: NodeId, segment: SegmentId) -> Self {
        assert!(segment.0 > 0, "segment ids start at 1");
        Self {
            journal,
            node,
            segment: Some(segment),
        }
    }

    /// The node of this role, with no segment.
    #[must_use]
    pub fn node_address(self) -> Self {
        Self::new(self.journal, self.node)
    }
}

/// A message from one role to another role of the same journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The journal of both roles.
    pub journal: JournalId,
    /// The sender.
    pub from: NodeId,
    /// The receiver.
    pub to: NodeId,
    /// The message.
    pub message: Message,
}

impl Envelope {
    /// The node of the sender, with no segment: a `Commit` may come from a
    /// leader or from a replica, so the message does not say which role
    /// sent it ([`crate::Effect::Send`] does).
    #[must_use]
    pub fn sender(&self) -> Address {
        Address::new(self.journal, self.from)
    }

    /// The role of the receiver. A `Commit`, a `Pull`, a `Holding`, a
    /// `PreReadAck`, a `Compaction`, a `TrimmedTo` and a `Learn` go to the replica of
    /// the segment that they name; every other message goes
    /// to an acceptor or a leader. A `Holds` answers the asker of `Holding`,
    /// a leader or a new replica, so its receiver is the node: the `Host`
    /// also gives it to the replica of its segment on that node.
    #[must_use]
    pub fn receiver(&self) -> Address {
        match &self.message {
            Message::Commit { segment, .. }
            | Message::Pull { segment, .. }
            | Message::Holding { segment }
            | Message::PreReadAck { segment, .. }
            | Message::Compaction { segment }
            | Message::TrimmedTo { segment, .. }
            | Message::Learn { segment, .. } => Address::replica(self.journal, self.to, *segment),
            // No wildcard: a new message kind chooses its receiver here.
            Message::Prepare { .. }
            | Message::Promise { .. }
            | Message::Nack { .. }
            | Message::Accept { .. }
            | Message::Accepted { .. }
            | Message::Applied { .. }
            | Message::Holds { .. }
            | Message::Delegate { .. }
            | Message::Chosen { .. }
            | Message::PreRead { .. }
            | Message::CompactedTo { .. }
            | Message::Compact { .. }
            | Message::Learned { .. }
            | Message::Repair { .. } => Address::new(self.journal, self.to),
        }
    }
}

/// A time, in nanoseconds from an origin that the clock chooses: the time
/// of the generic host. The [`Host`](crate::Host) reads no clock: the driver
/// gives the time with every input (§J4.2, rule 2), so a recorded run
/// replays.
pub use paros_host::Time;
