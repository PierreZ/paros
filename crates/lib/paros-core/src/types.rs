//! Core domain types for Multi-Paxos. Pure data, no logic.

use core::cmp::Ordering;

/// Stable identity of a node in the cluster. An id has no default: it is
/// minted (random, at format, in a deployment) or named, never assumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub u64);

/// A replicated-log slot index. Multi-Paxos chooses one [`Value`] per slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Slot(pub u64);

/// An opaque value proposed into / chosen for a slot. The core never interprets
/// the bytes; the application owns their meaning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Value(pub Vec<u8>);

/// A consensus value that can name itself in one word.
///
/// Phase 2 carries the fingerprint, not the value: an `Accepted` reports
/// *which* value the acceptor took, so a leader never credits an ack for a
/// different value at the same `(slot, ballot)`. That is the only thing the
/// [`crate::proposer::Proposer`] role ever needs to know about a value, which
/// is why it is a trait rather than a `Command`-shaped function call: a
/// deployment over some other value type implements this and reuses the role
/// unchanged.
pub trait Fingerprint {
    /// A stable identity for this value. Two values that compare equal must
    /// fingerprint equal; distinct values should collide only by accident.
    fn fingerprint(&self) -> u64;
}

/// The FNV-1a offset basis every paros fingerprint folds from.
pub(crate) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

// A zero basis would fingerprint every all-zero prefix to zero.
const _: () = assert!(FNV_OFFSET != 0);

/// Folds `bytes` into an FNV-1a `hash`: the one mixing step behind
/// [`command_fingerprint`] and a matchmaker set's decree identity.
pub(crate) fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A Paxos ballot (a.k.a. proposal / round number), forming a **total order**.
///
/// Ordering is keyed on `(round, node)`: a strictly higher `round` always wins;
/// equal rounds are broken deterministically by [`NodeId`]. This total order is
/// the backbone of Paxos safety — every two ballots are comparable, so an
/// acceptor can always decide whether an incoming ballot is `>=` the one it has
/// promised.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Ballot {
    /// The round number. Higher rounds dominate.
    pub round: u64,
    /// The proposer's identity, used only to break ties between equal rounds.
    pub node: NodeId,
}

impl Ballot {
    /// The smallest possible ballot (round 0 from node 0). Doubles as the
    /// "nothing promised / nothing accepted yet" sentinel in [`crate::HardState`].
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            round: 0,
            node: NodeId(0),
        }
    }
}

// The sentinel is the order's minimum: round zero, node zero.
const _: () = assert!(Ballot::zero().round == 0);
const _: () = assert!(Ballot::zero().node.0 == 0);

impl Default for Ballot {
    /// [`Ballot::zero`]: the smallest ballot, the sentinel of nothing
    /// promised — an order's minimum, not an identity.
    fn default() -> Self {
        Self::zero()
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher round wins; ties broken by NodeId. Written out (rather than
        // derived) so the total-order contract is local to this impl and
        // survives any future field reordering.
        let order = self
            .round
            .cmp(&other.round)
            .then_with(|| self.node.cmp(&other.node));
        // The total order agrees with equality: two ballots tie exactly when
        // they are the same ballot.
        assert!(
            (order == Ordering::Equal) == (self == other),
            "ballot order agrees with ballot equality"
        );
        order
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{Ballot, NodeId};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    #[test]
    fn higher_round_dominates_regardless_of_node() {
        assert!(ballot(2, 0) > ballot(1, 9));
    }

    #[test]
    fn equal_round_is_broken_by_node_id() {
        assert!(ballot(1, 2) > ballot(1, 1));
        assert_eq!(ballot(1, 1), ballot(1, 1));
    }

    #[test]
    fn zero_is_the_minimum_and_equals_default() {
        assert!(Ballot::zero() < ballot(0, 1));
        assert_eq!(Ballot::zero(), Ballot::default());
    }
}
