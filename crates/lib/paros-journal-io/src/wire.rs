//! The names that cross the boundary between the [`Host`](crate::Host) and
//! its driver: the address of a role, a message between two roles, and the
//! time of a step.
//!
//! A role instance is named by its journal and its node: one process serves
//! many journals, and in each journal one node runs one role (an acceptor,
//! a segment replica or a leader). The transport maps a node to a process;
//! the journal roles name nodes only.

use paros_consensus::journal::JournalId;
use paros_journal::NodeId;
use paros_journal::roles::Message;

/// One role instance: a node of one journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address {
    /// The journal.
    pub journal: JournalId,
    /// The node of the role in that journal.
    pub node: NodeId,
}

impl Address {
    /// The role `node` of `journal`.
    #[must_use]
    pub fn new(journal: JournalId, node: NodeId) -> Self {
        Self { journal, node }
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
    /// The address of the sender.
    #[must_use]
    pub fn sender(&self) -> Address {
        Address::new(self.journal, self.from)
    }

    /// The address of the receiver.
    #[must_use]
    pub fn receiver(&self) -> Address {
        Address::new(self.journal, self.to)
    }
}

/// A time, in nanoseconds from an origin that the clock chooses. The
/// [`Host`](crate::Host) reads no clock: the driver gives the time with
/// every input (§J4.2, rule 2), so a recorded run replays.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);

impl Time {
    /// The time `nanos` nanoseconds after this one, saturated at the
    /// largest time.
    #[must_use]
    pub fn after(self, nanos: u64) -> Self {
        Self(self.0.saturating_add(nanos))
    }
}
