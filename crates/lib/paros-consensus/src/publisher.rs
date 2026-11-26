//! The **publisher** of the chain of segments (R5 of
//! `docs/architecture/04-7-segments.md` §4.7): the node-side tally of each
//! [`SegmentPublish`] until a majority of the matchmakers of one generation
//! acked it.
//!
//! A node publishes the end of a segment only after its own walk applied the
//! chosen `Roll` that ends it (R3); the deployment hands the publish in
//! ([`Publisher::note`]). The publisher knows nothing of the log, the walk or
//! a ballot: it holds the publishes, says which matchmaker has not acked each
//! one ([`Publisher::requests`]), folds the acks ([`Publisher::fold_ack`]),
//! and drops a publish once a majority of its generation holds it
//! (`ChainCarried` of `specs/tla/handover-scalars/`: such a publish is in
//! every later read and crosses every handover). A publish that never
//! completes costs a chain behind the log, never safety: what a minority took
//! is already true.
//!
//! The tally is volatile. The deployment notes again, at boot, every `Roll`
//! its retained fold holds, so a publish that no node completed before it
//! stopped is asked again; a publish the matchmakers already hold merges to
//! the same chain.

use std::collections::{BTreeMap, BTreeSet};

use crate::journal::SegmentId;
use crate::matchmaker::{SegmentAck, SegmentPublish};
use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};

/// One publish and the matchmakers of the current generation that acked it.
#[derive(Clone, Debug)]
struct Pending {
    publish: SegmentPublish,
    acked: BTreeSet<MatchmakerId>,
}

/// The node-side tally of the chain's publishes (the module doc).
#[derive(Clone, Debug)]
pub struct Publisher {
    /// The matchmaker generation the acks are counted for.
    generation: MatchmakerGeneration,
    /// The publishes no majority acked yet, by the segment they seal.
    pending: BTreeMap<SegmentId, Pending>,
    /// The segments whose publish a majority of one generation acked: their
    /// end is in every later read of the chain (`ChainCarried`).
    completed: BTreeSet<SegmentId>,
}

impl Publisher {
    /// An empty tally for `generation`.
    ///
    /// # Panics
    ///
    /// Never: the assertions check the postconditions of the constructor.
    #[must_use]
    pub fn new(generation: MatchmakerGeneration) -> Self {
        let publisher = Self {
            generation,
            pending: BTreeMap::new(),
            completed: BTreeSet::new(),
        };
        assert!(!publisher.is_pending(), "a new tally holds no publish");
        assert!(
            publisher.generation == generation,
            "a new tally counts for its generation"
        );
        publisher
    }

    /// The tally of a deployment that believes `set` in force, `None` on a
    /// deployment with no matchmakers (the plain arm publishes nothing).
    ///
    /// # Panics
    ///
    /// Never: the assertion checks that the tally counts for `set`.
    #[must_use]
    pub fn for_set(set: Option<&MatchmakerSet>) -> Option<Self> {
        let publisher = set.map(|set| Self::new(set.generation));
        assert!(
            publisher.as_ref().map(Self::generation) == set.map(|set| set.generation),
            "a deployment's tally counts for the set it believes"
        );
        publisher
    }

    /// The generation the acks are counted for.
    #[must_use]
    pub fn generation(&self) -> MatchmakerGeneration {
        self.generation
    }

    /// Whether a publish waits for its acks.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// The segments whose publish waits for its acks.
    pub fn pending(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.pending.keys().copied()
    }

    /// Whether a majority of one generation acked the publish of the end
    /// of `segment` since this tally started: the end is in the chain.
    /// Volatile, like the tally: after a restart the node publishes again.
    ///
    /// # Panics
    ///
    /// If a completed publish still waits: a programmer error.
    #[must_use]
    pub fn is_published(&self, segment: SegmentId) -> bool {
        let published = self.completed.contains(&segment);
        assert!(
            !published || !self.pending.contains_key(&segment),
            "a completed publish waits no more"
        );
        assert!(
            self.completed.iter().all(|k| !self.pending.contains_key(k)),
            "no publish is both pending and completed"
        );
        published
    }

    /// Hold a publish that the node's walk made. A second publish of the same
    /// segment is the same publish (every field is a function of the chosen
    /// log) and changes nothing, and so is one that already completed.
    ///
    /// # Panics
    ///
    /// If a publish of the same segment with other values is held: the walk
    /// applied two different `Roll`s at one segment's end.
    pub fn note(&mut self, mut publish: SegmentPublish) {
        publish.generation = self.generation;
        let sealed = publish.sealed;
        if self.completed.contains(&sealed) {
            return;
        }
        if let Some(held) = self.pending.get(&sealed) {
            assert!(
                held.publish == publish,
                "R3: one segment's end is published with one value"
            );
            return;
        }
        self.pending.insert(
            sealed,
            Pending {
                publish,
                acked: BTreeSet::new(),
            },
        );
        assert!(self.pending.contains_key(&sealed), "a noted publish waits");
    }

    /// Each publish that a member of `set` has not acked, with its target.
    ///
    /// # Panics
    ///
    /// If `set` is not the generation the acks are counted for: the
    /// deployment resets the tally before it asks another set.
    pub fn requests<'a>(
        &'a self,
        set: &'a MatchmakerSet,
    ) -> impl Iterator<Item = (MatchmakerId, SegmentPublish)> + 'a {
        assert!(
            set.generation == self.generation,
            "a publish addresses the generation its acks are counted for"
        );
        self.pending.values().flat_map(move |pending| {
            set.members()
                .iter()
                .filter(|m| !pending.acked.contains(m))
                .map(|m| (*m, pending.publish.clone()))
        })
    }

    /// Fold one ack. An ack of another generation, of a matchmaker outside
    /// `set`, of a refusal, or of a publish that is not pending is ignored
    /// (wire input, never asserted). Returns the segment whose publish a
    /// majority of `set` now holds; the tally drops it.
    ///
    /// # Panics
    ///
    /// If a completed publish stays in the tally: a programmer error.
    pub fn fold_ack(&mut self, ack: &SegmentAck, set: &MatchmakerSet) -> Option<SegmentId> {
        if !ack.applied
            || ack.generation != self.generation
            || set.generation != self.generation
            || !set.contains(ack.matchmaker)
        {
            return None;
        }
        let pending = self.pending.get_mut(&ack.sealed)?;
        pending.acked.insert(ack.matchmaker);
        assert!(
            pending.acked.iter().all(|m| set.contains(*m)),
            "a tally counts only the members of its set"
        );
        if !set.has_quorum(&pending.acked) {
            return None;
        }
        self.pending.remove(&ack.sealed);
        self.completed.insert(ack.sealed);
        assert!(
            !self.pending.contains_key(&ack.sealed),
            "a completed publish waits no more"
        );
        Some(ack.sealed)
    }

    /// Count the acks for `generation` from now on: the acks of a replaced
    /// generation say nothing about the quorum of the new one. Every publish
    /// stays, to be asked of the new set.
    ///
    /// # Panics
    ///
    /// If a publish is lost on the way: a programmer error.
    pub fn reset_for_generation(&mut self, generation: MatchmakerGeneration) {
        let held = self.pending.len();
        assert!(
            generation > self.generation,
            "a tally moves only to a later generation"
        );
        self.generation = generation;
        for pending in self.pending.values_mut() {
            pending.acked.clear();
            pending.publish.generation = generation;
        }
        assert!(
            self.pending.len() == held,
            "a new generation keeps every publish"
        );
        assert!(
            self.pending.values().all(|p| p.acked.is_empty()),
            "a new generation counts no old ack"
        );
    }
}

#[cfg(test)]
mod tests;
