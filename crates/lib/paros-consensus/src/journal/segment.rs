//! **Segments** (`docs/architecture/04-7-segments.md` §4.7): a segment is a
//! range of slots of the log and the replicas that keep it, and it ends at a
//! decided command, the [`Control::Roll`](crate::journal::Control::Roll).
//!
//! The log starts in segment [`SegmentId::FIRST`] at slot 0. A `Roll` chosen
//! at slot `s` whose `next` is the tail segment plus one ends the tail at
//! `s + 1` and opens `next` there (R2). Any other `Roll` changes nothing. The
//! judgement is the apply path's (R3): every node folds the same chosen prefix,
//! so every node sees the same first valid `Roll` and the same end
//! (`SegsAgree` of `specs/tla/multi-paxos-roll/`, `OneEnd` of
//! `specs/tla/segment-roll/`).
//!
//! A deployment with no matchmakers never chooses a `Roll` (R10, D5), so its
//! log stays in the first segment for ever (R11).

use crate::types::{NodeId, Slot};

/// The id of one segment of the log: `0` for the first, and one more at each
/// valid `Roll`. Ids are dense, so the segment after `k` is `k + 1`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// The segment that holds slot 0.
    pub const FIRST: Self = Self(0);

    /// The segment after this one: the only `next` a `Roll` from this
    /// segment may name.
    ///
    /// # Panics
    ///
    /// If the id is `u64::MAX`: the log cannot roll that many times.
    #[must_use]
    pub fn successor(self) -> Self {
        let next = Self(self.0.checked_add(1).expect("a segment id has a successor"));
        assert!(next > self, "the next segment follows this one");
        assert_eq!(next.0 - self.0, 1, "segment ids are dense");
        next
    }
}

/// Where the tail segment of the log stands, as the fold of the chosen prefix
/// gives it: its id and its first slot. Part of the journal state
/// ([`crate::journal::JournalState::segment`]), so it is sealed at the floor
/// and carried by a trim-point jump with the rest of that state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Segment {
    /// The id of the tail segment.
    pub id: SegmentId,
    /// The first slot of the tail segment: 0 for the first, and the slot after
    /// the `Roll` that opened it for every other.
    pub start: Slot,
}

impl Segment {
    /// The tail after a valid `Roll` chosen at `slot`: the next id, opened at
    /// `slot + 1`.
    ///
    /// # Panics
    ///
    /// If `slot` lies below the start of this segment (a fold never goes
    /// back), or the next slot or id overflows.
    #[must_use]
    pub fn rolled_at(self, slot: Slot) -> Self {
        assert!(slot >= self.start, "a Roll lies inside the segment it ends");
        let start = Slot(slot.0.checked_add(1).expect("the slot after a Roll exists"));
        let next = Self {
            id: self.id.successor(),
            start,
        };
        assert!(
            next.start > self.start,
            "a new segment starts past the old one"
        );
        assert!(next.id > self.id, "a new segment has a higher id");
        next
    }
}

/// The replicas of a segment: the nodes that keep its range (R1, R6). Sorted,
/// with no repeat and never empty; [`ReplicaSet::new`] and
/// [`ReplicaSet::try_new`] are its only constructors (deserialisation goes
/// through `try_new` too).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "Vec<NodeId>", into = "Vec<NodeId>")
)]
pub struct ReplicaSet {
    members: Vec<NodeId>,
}

/// Why a list of nodes is not a [`ReplicaSet`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaSetError {
    /// The list names no node: a segment kept by nobody is lost.
    Empty,
    /// The list names a node twice.
    Duplicate(NodeId),
}

impl core::fmt::Display for ReplicaSetError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => f.write_str("a replica set names at least one node"),
            Self::Duplicate(node) => write!(f, "a replica set names node {} twice", node.0),
        }
    }
}

impl ReplicaSet {
    /// The set of `members`, sorted.
    ///
    /// # Errors
    ///
    /// [`ReplicaSetError::Empty`] for an empty list, and
    /// [`ReplicaSetError::Duplicate`] when a node is named twice.
    ///
    /// # Panics
    ///
    /// If the sorted list it keeps is not strictly increasing (a programmer
    /// error).
    pub fn try_new(members: impl IntoIterator<Item = NodeId>) -> Result<Self, ReplicaSetError> {
        let mut members: Vec<NodeId> = members.into_iter().collect();
        members.sort_unstable();
        if let Some(pair) = members.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(ReplicaSetError::Duplicate(pair[0]));
        }
        if members.is_empty() {
            return Err(ReplicaSetError::Empty);
        }
        assert!(
            members.windows(2).all(|pair| pair[0] < pair[1]),
            "a replica set is sorted with no repeat"
        );
        assert!(!members.is_empty(), "a replica set is never empty");
        Ok(Self { members })
    }

    /// The set of `members`, for operator input known to be well shaped.
    ///
    /// # Panics
    ///
    /// If [`ReplicaSet::try_new`] refuses the list; the panic text is the
    /// [`ReplicaSetError`].
    #[must_use]
    pub fn new(members: impl IntoIterator<Item = NodeId>) -> Self {
        Self::try_new(members).unwrap_or_else(|error| panic!("{error}"))
    }

    /// The members, sorted.
    ///
    /// # Panics
    ///
    /// If the set is empty, which no constructor allows.
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        assert!(!self.members.is_empty(), "a replica set is never empty");
        &self.members
    }

    /// Whether `node` keeps this segment.
    ///
    /// # Panics
    ///
    /// If the set is empty, which no constructor allows.
    #[must_use]
    pub fn contains(&self, node: NodeId) -> bool {
        assert!(!self.members.is_empty(), "a replica set is never empty");
        self.members.binary_search(&node).is_ok()
    }
}

impl TryFrom<Vec<NodeId>> for ReplicaSet {
    type Error = ReplicaSetError;

    fn try_from(members: Vec<NodeId>) -> Result<Self, ReplicaSetError> {
        Self::try_new(members)
    }
}

impl From<ReplicaSet> for Vec<NodeId> {
    fn from(set: ReplicaSet) -> Self {
        assert!(!set.members.is_empty(), "a replica set is never empty");
        set.members
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplicaSet, ReplicaSetError, Segment, SegmentId};
    use crate::types::{NodeId, Slot};

    #[test]
    fn a_roll_opens_the_next_segment_at_the_next_slot() {
        let first = Segment::default();
        assert_eq!(first.id, SegmentId::FIRST);
        assert_eq!(first.start, Slot(0));
        let second = first.rolled_at(Slot(4));
        assert_eq!(second.id, SegmentId(1));
        assert_eq!(second.start, Slot(5));
        assert_eq!(second.rolled_at(Slot(5)).start, Slot(6));
    }

    #[test]
    #[should_panic(expected = "a Roll lies inside the segment it ends")]
    fn a_roll_below_the_start_is_a_bug() {
        let _ = Segment::default().rolled_at(Slot(4)).rolled_at(Slot(2));
    }

    #[test]
    fn a_replica_set_is_sorted_and_refuses_an_empty_list_or_a_repeat() {
        let set = ReplicaSet::new([NodeId(3), NodeId(1)]);
        assert_eq!(set.members(), &[NodeId(1), NodeId(3)]);
        assert!(set.contains(NodeId(3)));
        assert!(!set.contains(NodeId(2)));
        assert_eq!(ReplicaSet::try_new([]), Err(ReplicaSetError::Empty));
        assert_eq!(
            ReplicaSet::try_new([NodeId(2), NodeId(2)]),
            Err(ReplicaSetError::Duplicate(NodeId(2)))
        );
    }
}
