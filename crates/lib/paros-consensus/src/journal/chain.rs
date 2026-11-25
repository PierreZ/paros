//! The **chain of segments** (R5 of `docs/architecture/04-7-segments.md`
//! §4.7): what the matchmakers know of each segment of a journal's log.
//!
//! The chain is an add-only map from a [`SegmentId`] to a [`SegmentRecord`].
//! Each field of a record is a function of the chosen log: the start, the end
//! and the replicas that the `Roll` named. So each field is written once, and
//! every writer writes the same value: a node publishes the end of a segment
//! only after its own walk applied the `Roll` that ends it (R3). Two records of
//! one segment therefore merge field by field ([`SegmentRecord::merge`]), and
//! two chains merge segment by segment ([`merge_chain`]), with no
//! compare-and-set. A handover carries the chain by this merge, and the read of
//! the chain over a matchmaker quorum is the same merge (its reader, the
//! matchmaking and probe answers, comes in step 5 of the plan). The chain has
//! no size limit yet: it grows by one record for each `Roll`, until the states
//! past `Sealed` let it forget old segments (§4.7, **Open**). Its TLA+ model is the chain of
//! `specs/tla/handover-scalars/` (`ChainTrue`, `ChainCarried`, `ChainGrows`).
//!
//! A record that a minority of matchmakers took is already true: it is never
//! taken back, and no later write can disagree with it.

use std::collections::BTreeMap;

use super::segment::{ReplicaSet, SegmentId};
use crate::types::Slot;

/// A journal's chain: each segment the matchmakers heard of, by id.
pub type Chain = BTreeMap<SegmentId, SegmentRecord>;

/// How a segment opened: what the `Roll` that opened it says. The first
/// segment opens at the bootstrap of the journal and has none.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Opening {
    /// The replicas that the `Roll` named, or `None` when it kept the replicas
    /// of the segment before (D4).
    pub replicas: Option<ReplicaSet>,
}

/// What the chain holds of one segment. The start is known from the first
/// write; the opening and the end each come with a write that knows them,
/// and each is written once.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SegmentRecord {
    /// The first slot of the segment.
    pub start: Slot,
    /// How the segment opened, once a write that opened it arrived. Never
    /// set on the first segment.
    pub opening: Option<Opening>,
    /// The slot after the `Roll` that ends the segment, once the segment is
    /// sealed. A sealed segment holds the slots `start..end`.
    pub end: Option<Slot>,
}

impl SegmentRecord {
    /// Whether the segment is sealed: its end is known.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.end.is_some()
    }

    /// Merge `other`, a record of the same segment, into this one: a field
    /// that this record lacks takes the value of `other`, and a field that
    /// both hold must agree. A field never goes away, so the merge only grows.
    ///
    /// # Panics
    ///
    /// If the two records disagree on a field. Each field is a function of
    /// the chosen log (R3), so two values of one field are two values chosen
    /// for one slot: a detected safety violation, never an operating
    /// condition.
    pub fn merge(&mut self, other: &Self) {
        assert!(
            self.start == other.start,
            "R5: two records of one segment agree on its start"
        );
        match (&self.opening, &other.opening) {
            (Some(held), Some(theirs)) => assert!(
                held == theirs,
                "R5: two records of one segment agree on how it opened"
            ),
            (None, Some(theirs)) => self.opening = Some(theirs.clone()),
            (_, None) => {}
        }
        match (self.end, other.end) {
            (Some(held), Some(theirs)) => assert!(
                held == theirs,
                "R5: two records of one segment agree on its end"
            ),
            (None, Some(theirs)) => self.end = Some(theirs),
            (_, None) => {}
        }
        assert!(
            other.opening.is_none() || self.opening.is_some(),
            "a merged record keeps every opening it heard"
        );
        assert!(
            other.end.is_none() || self.end.is_some(),
            "a merged record keeps every end it heard"
        );
    }

    /// Check the shape of the record of segment `id`: only the first segment
    /// starts at slot 0 and has no opening to wait for, a later one starts at
    /// slot `id` or later, and an end lies past the start.
    ///
    /// # Panics
    ///
    /// If the record breaks one of these rules.
    pub fn assert_shape(&self, id: SegmentId) {
        assert!(
            (id == SegmentId::FIRST) == (self.start == Slot(0)),
            "only the first segment starts at slot 0"
        );
        assert!(self.start.0 >= id.0, "segment k starts at slot k or later");
        assert!(
            id != SegmentId::FIRST || self.opening.is_none(),
            "the first segment opens at the bootstrap, not at a Roll"
        );
        if let Some(end) = self.end {
            assert!(end > self.start, "a sealed segment holds its Roll");
        }
    }
}

/// Merge the chain `from` into `into`, segment by segment
/// ([`SegmentRecord::merge`]): the chain only grows.
///
/// # Panics
///
/// If two records of one segment disagree (a detected safety violation, see
/// [`SegmentRecord::merge`]), or the merged chain is not contiguous where it
/// knows both sides of a boundary.
pub fn merge_chain(into: &mut Chain, from: &Chain) {
    let before = into.len();
    for (id, record) in from {
        match into.get_mut(id) {
            Some(held) => held.merge(record),
            None => {
                into.insert(*id, record.clone());
            }
        }
    }
    assert!(into.len() >= before, "a merged chain only grows");
    assert!(
        from.keys().all(|id| into.contains_key(id)),
        "a merged chain holds every segment it heard"
    );
    assert_chain(into);
}

/// Check a chain: each record has its shape ([`SegmentRecord::assert_shape`]),
/// and where the chain holds a sealed segment and the one after it, the next
/// starts at the end of the first.
///
/// # Panics
///
/// If a record breaks its shape or two neighbours do not meet.
pub fn assert_chain(chain: &Chain) {
    for (id, record) in chain {
        record.assert_shape(*id);
        if let Some(end) = record.end
            && let Some(next) = chain.get(&id.successor())
        {
            assert!(
                next.start == end,
                "the next segment starts at the end of the one it follows"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Chain, Opening, SegmentRecord, merge_chain};
    use crate::journal::segment::{ReplicaSet, SegmentId};
    use crate::types::{NodeId, Slot};

    fn sealed(start: u64, end: u64) -> SegmentRecord {
        SegmentRecord {
            start: Slot(start),
            opening: None,
            end: Some(Slot(end)),
        }
    }

    fn opened(start: u64, replicas: Option<&[u64]>) -> SegmentRecord {
        SegmentRecord {
            start: Slot(start),
            opening: Some(Opening {
                replicas: replicas.map(|r| ReplicaSet::new(r.iter().map(|n| NodeId(*n)))),
            }),
            end: None,
        }
    }

    #[test]
    fn a_seal_and_an_opening_merge_into_one_record() {
        let mut record = opened(5, Some(&[1, 2]));
        record.merge(&sealed(5, 9));
        assert_eq!(record.end, Some(Slot(9)));
        assert!(record.is_sealed());
        assert_eq!(record.opening, opened(5, Some(&[1, 2])).opening);
        // The same in the other order gives the same record.
        let mut other = sealed(5, 9);
        other.merge(&opened(5, Some(&[1, 2])));
        assert_eq!(other, record);
        // A merge with a record that knows less changes nothing.
        let before = record.clone();
        record.merge(&opened(5, Some(&[1, 2])));
        assert_eq!(record, before);
    }

    #[test]
    #[should_panic(expected = "R5: two records of one segment agree on its end")]
    fn two_ends_of_one_segment_are_a_safety_violation() {
        let mut record = sealed(5, 9);
        record.merge(&sealed(5, 10));
    }

    #[test]
    #[should_panic(expected = "R5: two records of one segment agree on how it opened")]
    fn two_openings_of_one_segment_are_a_safety_violation() {
        let mut record = opened(5, Some(&[1]));
        record.merge(&opened(5, None));
    }

    #[test]
    #[should_panic(expected = "R5: two records of one segment agree on its start")]
    fn two_starts_of_one_segment_are_a_safety_violation() {
        let mut record = sealed(5, 9);
        record.merge(&sealed(6, 9));
    }

    #[test]
    fn chains_merge_segment_by_segment() {
        let mut held: Chain = [
            (SegmentId(0), sealed(0, 5)),
            (SegmentId(1), opened(5, None)),
        ]
        .into_iter()
        .collect();
        let theirs: Chain = [
            (SegmentId(1), sealed(5, 9)),
            (SegmentId(2), opened(9, Some(&[3]))),
        ]
        .into_iter()
        .collect();
        merge_chain(&mut held, &theirs);
        assert_eq!(held.len(), 3);
        assert_eq!(held[&SegmentId(1)].end, Some(Slot(9)));
        assert!(held[&SegmentId(1)].opening.is_some());
        assert!(!held[&SegmentId(2)].is_sealed());
    }

    #[test]
    #[should_panic(expected = "the next segment starts at the end of the one it follows")]
    fn a_chain_whose_neighbours_do_not_meet_is_a_bug() {
        let mut held: Chain = [(SegmentId(0), sealed(0, 5))].into_iter().collect();
        let theirs: Chain = [(SegmentId(1), opened(6, None))].into_iter().collect();
        merge_chain(&mut held, &theirs);
    }

    #[test]
    #[should_panic(expected = "only the first segment starts at slot 0")]
    fn a_later_segment_at_slot_zero_is_a_bug() {
        sealed(0, 4).assert_shape(SegmentId(1));
    }
}
