//! Core domain types for Multi-Paxos. Pure data, no logic.

use core::cmp::Ordering;

/// Stable identity of a node in the cluster.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub u64);

/// A replicated-log slot index. Multi-Paxos chooses one [`Value`] per slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Slot(pub u64);

/// Opaque client-supplied identity, used to dedupe requests for at-most-once
/// execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClientId(pub u64);

/// Per-client monotonically increasing request sequence number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClientSeq(pub u64);

/// An opaque value proposed into / chosen for a slot. The core never interprets
/// the bytes; the application owns their meaning.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Value(pub Vec<u8>);

/// An opaque client command: a proposal payload before it is assigned a slot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Command(pub Vec<u8>);

/// A Paxos ballot (a.k.a. proposal / round number), forming a **total order**.
///
/// Ordering is keyed on `(round, node)`: a strictly higher `round` always wins;
/// equal rounds are broken deterministically by [`NodeId`]. This total order is
/// the backbone of Paxos safety — every two ballots are comparable, so an
/// acceptor can always decide whether an incoming ballot is `>=` the one it has
/// promised.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
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

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher round wins; ties broken by NodeId. Written out (rather than
        // derived) so the total-order contract is local to this impl and
        // survives any future field reordering.
        self.round
            .cmp(&other.round)
            .then_with(|| self.node.cmp(&other.node))
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
