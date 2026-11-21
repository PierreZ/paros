//! The names that cross the boundary between the [`Host`](crate::Host) and
//! its driver: the address of a role, a message between two roles, and the
//! time of a step.
//!
//! A role instance is named by its journal and its node: one process serves
//! many journals, and in each journal one node runs one role (an acceptor,
//! a segment replica or a leader). The transport maps a node to a process;
//! the journal roles name nodes only.
//!
//! **The bytes of an envelope** ([`Envelope::encode`]) are what a transport
//! between processes sends: the bytes of the message
//! ([`paros_journal::encode_message`], which start with the release of the
//! journal's formats), then the route, 24 bytes: the journal, the sender and
//! the receiver, each a little-endian `u64`. The release leads every
//! encoding of the journal, so the route goes last; the message encoding
//! ends where the route starts, since the route has a fixed size.

use paros_consensus::journal::JournalId;
use paros_journal::roles::Message;
use paros_journal::{DecodeError, NodeId, decode_message, encode_message};

/// The bytes of the route of an envelope: three `u64`.
const ROUTE_LEN: usize = 3 * size_of::<u64>();

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

    /// The bytes of this envelope (the module doc gives the format).
    ///
    /// # Panics
    ///
    /// When the envelope names the unset journal: a role never sends one.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        assert!(self.journal.is_set(), "an envelope names its journal");
        let mut bytes = encode_message(&self.message);
        let message = bytes.len();
        for word in [self.journal.0, self.from.0, self.to.0] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        assert!(bytes.len() == message + ROUTE_LEN, "the route is 24 bytes");
        bytes
    }

    /// The envelope in `bytes`. A peer's bytes are not trusted: bytes that
    /// are not one envelope give an error, never a panic.
    ///
    /// # Errors
    ///
    /// When the bytes are shorter than a route, name the unset journal, or
    /// do not hold one message of a known release before the route.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let Some(at) = bytes.len().checked_sub(ROUTE_LEN) else {
            return Err(DecodeError::Short);
        };
        let (message, route) = bytes.split_at(at);
        let word = |i: usize| {
            let mut word = [0; 8];
            word.copy_from_slice(&route[i * 8..(i + 1) * 8]);
            u64::from_le_bytes(word)
        };
        let journal = JournalId(word(0));
        if !journal.is_set() {
            return Err(DecodeError::Invalid("an envelope of the unset journal"));
        }
        Ok(Self {
            journal,
            from: NodeId(word(1)),
            to: NodeId(word(2)),
            message: decode_message(message)?,
        })
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

#[cfg(test)]
mod tests;
