//! The **held tally** (R7 of `docs/architecture/04-7-segments.md` §4.7): the
//! leader-side count of the replicas that hold a whole sealed segment.
//!
//! A sealed replica whose floor is the start of its segment answers each beat
//! with [`crate::Message::SegmentHeld`]. The tally keeps a sender only when
//! the leader's own fold names it as a replica of that segment and gives the
//! segment the same end: the fold of a chosen prefix is the only source of a
//! segment's end and its replicas (R3). A segment is `Held` once more than
//! `F` of its replicas reported it ([`AcceptorConfig::is_held_by`], D8) and
//! its end is in the chain (the control `held_log` of
//! `specs/tla/held-floor/`: a floor past an end that the chain does not
//! publish). `Held` only grows, and a report counts forever: a replica that
//! reported a segment held it durably, and at most `F` replicas are down at
//! one time, a replica being down from a lost disk until it copies the
//! segment again (`held_ever` of `specs/tla/held-floor-retry/`, `MarkHeld` of
//! `specs/tla/held-floor/HeldFloor.tla`).
//!
//! What it does not know: who leads, or whether the chain holds the end. The
//! wiring (`node/held.rs`) hands them in, and notes the end and the replicas
//! of each segment from the fold ([`HeldTally::note`]).

use std::collections::{BTreeMap, BTreeSet};

use crate::journal::{ReplicaSet, SegmentId, Seq};
use crate::membership::AcceptorConfig;
use crate::types::{NodeId, Slot};

/// What the leader's fold says of a sealed segment: its end and its
/// replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSegment {
    /// The first slot of the segment.
    pub start: Slot,
    /// The first slot past its end (the slot after its `Roll`).
    pub end: Slot,
    /// The position that the next write took at its end: the fold's
    /// `next_seq` at its `Roll`, which a `Hold` carries.
    pub next_seq: Seq,
    /// The replicas that keep it: the set the fold gives at its `Roll`,
    /// else the deployment's `Config::segment_replicas`. `None` when
    /// neither names one: such a segment is never held.
    pub replicas: Option<ReplicaSet>,
}

/// The count of the replicas that hold each sealed segment, and the
/// segments it marked `Held` (the module doc).
#[derive(Clone, Debug, Default)]
pub struct HeldTally {
    /// What the fold said of each sealed segment, as the wiring noted it.
    sealed: BTreeMap<SegmentId, SealedSegment>,
    /// The replicas that reported each segment, kept only when the fold
    /// names them and gives the same end.
    reports: BTreeMap<SegmentId, BTreeSet<NodeId>>,
    /// The segments marked `Held`.
    held: BTreeSet<SegmentId>,
}

impl HeldTally {
    /// Note what the fold says of the sealed `segment`. A second note of
    /// one segment is the same (every field is a function of the chosen
    /// log).
    ///
    /// # Panics
    ///
    /// If two notes of one segment differ (R3), or the segment holds no
    /// `Roll`.
    pub fn note(&mut self, segment: SegmentId, fold: SealedSegment) {
        assert!(fold.start < fold.end, "a sealed segment holds its `Roll`");
        if let Some(noted) = self.sealed.get(&segment) {
            assert!(*noted == fold, "R3: one segment is sealed one way");
            return;
        }
        self.sealed.insert(segment, fold);
        assert!(
            self.sealed.contains_key(&segment),
            "a noted segment is kept"
        );
    }

    /// What the fold said of `segment`, once noted.
    ///
    /// # Panics
    ///
    /// If a noted segment holds no `Roll`: a programmer error.
    #[must_use]
    pub fn sealed(&self, segment: SegmentId) -> Option<&SealedSegment> {
        let fold = self.sealed.get(&segment);
        assert!(
            fold.is_none_or(|f| f.start < f.end),
            "a sealed segment holds its `Roll`"
        );
        assert!(
            fold.is_some() || !self.held.contains(&segment),
            "a held segment is noted"
        );
        fold
    }

    /// Fold the report of `from` that it holds `segment` up to `end`.
    /// Returns whether the report counts: the segment is noted and not held
    /// yet, `from` is one of its replicas, and the ends agree. A report that
    /// does not count changes nothing (wire input, never asserted): a held
    /// segment needs no report, and a replica reports on every beat.
    ///
    /// # Panics
    ///
    /// If a counted report names a sender outside the replicas: a
    /// programmer error.
    pub fn report(&mut self, from: NodeId, segment: SegmentId, end: Slot) -> bool {
        if self.held.contains(&segment) {
            assert!(
                !self.reports.contains_key(&segment),
                "a held segment keeps no report"
            );
            return false;
        }
        let Some(fold) = self.sealed.get(&segment) else {
            return false;
        };
        let Some(replicas) = &fold.replicas else {
            return false;
        };
        if fold.end != end || !replicas.contains(from) {
            return false;
        }
        assert!(fold.start < fold.end, "a sealed segment holds its `Roll`");
        let reporters = self.reports.entry(segment).or_default();
        reporters.insert(from);
        assert!(
            reporters.iter().all(|r| replicas.contains(*r)),
            "a tally counts only the replicas of the segment"
        );
        true
    }

    /// Mark `segment` `Held` when it is noted, more than `F` of its
    /// replicas reported it and `published` says that its end is in the
    /// chain. Returns
    /// whether this call marked it.
    ///
    /// # Panics
    ///
    /// If a held segment loses its mark: a programmer error.
    pub fn judge(&mut self, segment: SegmentId, config: &AcceptorConfig, published: bool) -> bool {
        if self.held.contains(&segment) || !published {
            return false;
        }
        let Some(replicas) = self.sealed.get(&segment).and_then(|f| f.replicas.as_ref()) else {
            return false;
        };
        let copies = self.reports.get(&segment).map_or(0, |reporters| {
            reporters.iter().filter(|r| replicas.contains(**r)).count()
        });
        if !config.is_held_by(copies) {
            return false;
        }
        self.held.insert(segment);
        // The reports are no longer needed: `Held` only grows.
        self.reports.remove(&segment);
        assert!(self.held.contains(&segment), "a judged segment is held");
        assert!(
            !self.reports.contains_key(&segment),
            "a held segment keeps no report"
        );
        true
    }

    /// Whether `segment` is marked `Held`.
    ///
    /// # Panics
    ///
    /// If a held segment is not noted, or keeps its reports: a programmer
    /// error.
    #[must_use]
    pub fn is_held(&self, segment: SegmentId) -> bool {
        let held = self.held.contains(&segment);
        assert!(
            !held || self.sealed.contains_key(&segment),
            "a held segment is noted"
        );
        assert!(
            !held || !self.reports.contains_key(&segment),
            "a held segment keeps no report"
        );
        held
    }

    /// The segments marked `Held`, in order.
    ///
    /// # Panics
    ///
    /// If a held segment is not noted, or names no replicas: a programmer
    /// error.
    pub fn held(&self) -> impl Iterator<Item = SegmentId> + '_ {
        assert!(
            self.held.iter().all(|k| self.sealed.contains_key(k)),
            "every held segment is noted"
        );
        assert!(
            self.held
                .iter()
                .all(|k| self.sealed.get(k).is_some_and(|f| f.replicas.is_some())),
            "every held segment names its replicas"
        );
        self.held.iter().copied()
    }

    /// The replicas whose report of `segment` counts so far, until it is
    /// held.
    ///
    /// # Panics
    ///
    /// If a held segment still keeps its reports: a programmer error.
    #[must_use]
    pub fn reporters(&self, segment: SegmentId) -> usize {
        let count = self.reports.get(&segment).map_or(0, BTreeSet::len);
        assert!(
            count == 0 || !self.held.contains(&segment),
            "a held segment keeps no report"
        );
        assert!(
            count == 0 || self.sealed.contains_key(&segment),
            "only a noted segment has reports"
        );
        count
    }
}

#[cfg(test)]
mod tests;
