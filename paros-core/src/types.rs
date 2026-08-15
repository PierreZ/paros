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

/// A log entry: the [`Value`] chosen for a slot, tagged with the client request
/// that produced it. Carrying `(client, seq)` in the entry is what lets the core
/// deduplicate client retries for at-most-once execution, even for an in-flight
/// command a recovering leader inherits during Phase 1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    /// The client that issued the command.
    pub client: ClientId,
    /// The client's per-request sequence number.
    pub seq: ClientSeq,
    /// The opaque command payload chosen for the slot.
    pub value: Value,
}

/// A paros-interpreted **control command**: cluster metadata decided into a log
/// slot by ordinary consensus, rather than an opaque client value.
///
/// Unlike a [`Command::User`] payload (whose bytes the core never interprets), a
/// control command *is* interpreted — by the replica/apply path only, when the
/// slot it occupies enters the contiguous chosen prefix. The acceptor/consensus
/// paths (`Prepare`/`Accept`/`Promise`/catch-up) treat a whole [`Command`]
/// opaquely, exactly as Compartmentalized Paxos treats a `Noop`, so promoting
/// truncation to a decided fact does not leak into the vote machinery.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Control {
    /// A slot that decides *nothing*. A new leader fills the holes inside the
    /// range its Phase-1 quorum reported with these, so the log it inherits is
    /// contiguous and its applied prefix can advance past them (see
    /// `RawNode::try_become_leader`). Applying one is a no-op by construction —
    /// which is the point: the slot must be **decided**, not skipped, because a
    /// slot nobody ever decides blocks the contiguous prefix forever.
    Noop,
    /// Truncate the log: every node drops its retained prefix up to `up_to`
    /// (clamped to its own chosen index) when it applies this slot. The
    /// leader-decided, cluster-wide analogue of a local
    /// [`crate::RawNode::compact`] call, forwarded by normal replication +
    /// catch-up.
    Truncate {
        /// The last slot the application permits dropping (inclusive).
        up_to: Slot,
    },
}

/// What a single log slot decides: either an opaque client [`Entry`] or a
/// paros-interpreted [`Control`] command.
///
/// This is the per-slot value the whole protocol carries (in `Accept`,
/// `Promise`, `Commit`, catch-up, and the durable accepted log). Only the
/// replica/apply path distinguishes the two variants; every acceptor/consensus
/// path stores and relays a `Command` without inspecting it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    /// An opaque client command (the core never interprets its bytes).
    User(Entry),
    /// A paros-interpreted control command (interpreted only at apply time).
    Control(Control),
}

impl Command {
    /// The client [`Entry`] if this is a [`Command::User`], else `None`.
    #[must_use]
    pub fn user(&self) -> Option<&Entry> {
        match self {
            Command::User(entry) => Some(entry),
            Command::Control(_) => None,
        }
    }
}

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
