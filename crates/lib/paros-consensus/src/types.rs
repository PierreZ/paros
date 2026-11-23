//! Core domain types for Multi-Paxos. Pure data, no logic.

use core::cmp::Ordering;
use std::sync::Arc;

/// Stable identity of a node in the cluster. An id has no default: it is
/// minted (random, at format, in a deployment) or named, never assumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub u64);

impl core::fmt::Display for NodeId {
    /// The bare number: `NodeId(3)` prints as `3`.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A replicated-log slot index. Multi-Paxos chooses one [`Value`] per slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Slot(pub u64);

impl core::fmt::Display for Slot {
    /// The bare number: `Slot(4)` prints as `4`.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An opaque value proposed into / chosen for a slot. The core never interprets
/// the bytes; the application owns their meaning.
///
/// The bytes are shared: a clone of a value costs one reference count, never
/// a copy of the bytes. A command is cloned on each hop of a write (into the
/// leader's round, the `Accept` of each acceptor, the acceptor's record, the
/// `Commit`, the replica's apply), so a shared value keeps one copy of each
/// record in memory, whatever the number of holders (#1101 (one copy of the
/// retained log)). A value is never changed in place, so the sharing is
/// invisible: two values are equal when their bytes are equal.
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Value(Arc<[u8]>);

impl Value {
    /// A value that holds `bytes`.
    #[must_use]
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    /// The bytes of the value.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether `self` and `other` share one copy of their bytes: a clone
    /// does, a value built again from the same bytes does not.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn shares_bytes_with(&self, other: &Self) -> bool {
        let shared = Arc::ptr_eq(&self.0, &other.0);
        assert!(
            !shared || self == other,
            "values that share bytes are equal"
        );
        shared
    }
}

impl core::ops::Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        let value = Self(bytes.into());
        assert_eq!(
            value.len(),
            len,
            "a value holds every byte it was built from"
        );
        value
    }
}

impl From<&[u8]> for Value {
    fn from(bytes: &[u8]) -> Self {
        let len = bytes.len();
        let value = Self(bytes.into());
        assert_eq!(
            value.len(),
            len,
            "a value holds every byte it was built from"
        );
        value
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    fn from(bytes: &[u8; N]) -> Self {
        let value = Self(bytes.as_slice().into());
        let len = N;
        assert_eq!(
            value.len(),
            len,
            "a value holds every byte it was built from"
        );
        value
    }
}

impl core::fmt::Debug for Value {
    /// `Value([1, 2])`: the bytes as a list, as an owned vector prints them.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Value").field(&&*self.0).finish()
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Value {
    /// The bytes as a sequence, as an owned vector serializes them.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&*self.0, serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Vec<u8> as serde::Deserialize>::deserialize(deserializer).map(Self::from)
    }
}

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
pub const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

// A zero basis would fingerprint every all-zero prefix to zero.
const _: () = assert!(FNV_OFFSET != 0);

/// Folds `bytes` into an FNV-1a `hash`: the one mixing step behind
/// [`crate::journal::command_fingerprint`] and the decree identity of a
/// matchmaker set.
#[must_use]
pub const fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

// The fold is the published FNV-1a: no byte keeps the basis, and the test
// vector of "a" matches the reference implementation.
const _: () = assert!(fnv1a(FNV_OFFSET, b"") == FNV_OFFSET);
const _: () = assert!(fnv1a(FNV_OFFSET, b"a") == 0xaf63_dc4c_8601_ec8c);
// Each byte moves the hash: two prefixes of one input never collide here.
const _: () = assert!(fnv1a(FNV_OFFSET, b"ab") != fnv1a(FNV_OFFSET, b"a"));

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

impl core::fmt::Display for Ballot {
    /// `round.node`, the form of the traces and the examples: round 7 of
    /// node 2 prints as `7.2`.
    ///
    /// ```
    /// use paros_consensus::{Ballot, NodeId};
    ///
    /// let ballot = Ballot { round: 7, node: NodeId(2) };
    /// assert_eq!(ballot.to_string(), "7.2");
    /// // A higher round wins, and the node breaks a tie.
    /// assert!(ballot > Ballot { round: 6, node: NodeId(9) });
    /// assert!(ballot > Ballot { round: 7, node: NodeId(1) });
    /// ```
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}.{}", self.round, self.node)
    }
}

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
    fn the_scalars_print_as_the_traces_write_them() {
        use super::Slot;
        assert_eq!(NodeId(3).to_string(), "3");
        assert_eq!(Slot(4).to_string(), "4");
        assert_eq!(ballot(7, 2).to_string(), "7.2");
        assert_eq!(Ballot::zero().to_string(), "0.0");
    }

    #[test]
    fn zero_is_the_minimum_and_equals_default() {
        assert!(Ballot::zero() < ballot(0, 1));
        assert_eq!(Ballot::zero(), Ballot::default());
    }

    #[test]
    fn a_cloned_value_shares_its_bytes() {
        use super::Value;
        let value = Value::from(b"abc");
        let copy = value.clone();
        assert!(copy.shares_bytes_with(&value), "a clone costs a count");
        let again = Value::from(b"abc".to_vec());
        assert_eq!(again, value, "equal bytes are an equal value");
        assert!(
            !again.shares_bytes_with(&value),
            "a new value has its own bytes"
        );
        assert_eq!(value.as_bytes(), b"abc");
        assert_eq!(value.len(), 3);
        let bytes: &[u8] = value.as_ref();
        assert_eq!(bytes, b"abc", "`AsRef` reads the value's own bytes");
        let from_slice = Value::from(&b"abc"[..]);
        assert_eq!(from_slice.as_bytes(), b"abc", "a slice builds its bytes");
        assert_eq!(from_slice, value, "the three builders agree");
    }

    /// A value reads back from the wire with the bytes it was written with:
    /// a sequence of bytes, as an owned vector deserializes.
    #[cfg(feature = "serde")]
    #[test]
    fn a_value_deserializes_from_its_bytes() {
        use super::Value;
        use serde::de::value::{Error, SeqDeserializer};
        let wire = SeqDeserializer::<_, Error>::new(vec![1u8, 2, 3].into_iter());
        let value: Value =
            serde::Deserialize::deserialize(wire).expect("a byte sequence is a value");
        assert_eq!(value, Value::from(vec![1, 2, 3]), "the bytes read back");
    }

    #[test]
    fn a_value_prints_as_an_owned_vector_did() {
        use super::Value;
        assert_eq!(format!("{:?}", Value::from(vec![1, 2])), "Value([1, 2])");
        assert_eq!(format!("{:?}", Value::default()), "Value([])");
        assert!(
            Value::from(b"a") < Value::from(b"b"),
            "values order by bytes"
        );
    }
}
