//! The **publish of the chain of segments** (R5 of
//! `docs/architecture/04-7-segments.md` §4.7): the wiring between the walk,
//! which applies a chosen `Roll`, and the [`Publisher`] tally.
//!
//! A node publishes the end of a segment only after its own walk applied the
//! `Roll` that ends it (R3): [`ColocatedNode::note_rolls`] reads the fold of
//! the chosen prefix and nothing else, never a vote or a proposal. Every path
//! that folds a slot (the walk, the refold past a healed hole, a trim-point
//! jump, the fold at boot) notes the `Roll`s it folded, so an end is
//! published again after a restart even when no node kept its tally. Every node of a
//! matchmaker deployment publishes, leader or not, to every matchmaker of the
//! set it believes in force; the publish completes at a majority
//! ([`ColocatedNode::on_segment_ack`]). A lost request or ack only stalls the
//! chain, so the driver re-sends on its own cadence
//! ([`ColocatedNode::resend_segment_publishes`]). The plain deployment has no
//! publisher, and publishes nothing.

use super::ColocatedNode;
use crate::journal::{Command, Opening, SegmentId};
use crate::matchmaker::{SegmentAck, SegmentPublish};
use crate::membership::MatchmakerId;
use crate::publisher::Publisher;
use crate::replica::ChosenLog;
use crate::types::Slot;

impl ColocatedNode {
    /// Note with the publisher each `Roll` that the fold applied since the
    /// last note, up to [`crate::replica::Replica::folded`]. The fold is the
    /// only source: a `Roll` reached by the walk, by a refold past a healed
    /// hole, at boot or by a trim-point jump is noted alike, with the start
    /// and the end that the fold gives it, so each published field is a
    /// function of the chosen log (R3). Returns whether a `Roll` was noted.
    fn note_folded_rolls(&mut self) -> bool {
        let from = self.rolls_noted;
        let folded = self.replica.folded();
        assert!(
            folded >= from,
            "the fold never retreats below the note mark"
        );
        self.rolls_noted = folded;
        if self.publisher.is_none() {
            return false;
        }
        assert!(
            self.config.has_matchmakers(),
            "only a matchmaker deployment publishes the chain"
        );
        let mut publishes = Vec::new();
        for (slot, start, tail) in self.replica.rolls_from(from) {
            assert!(slot < folded, "only a folded Roll is noted");
            let Some((_, Command::Roll(roll))) = ChosenLog::record(&self.acceptor, slot) else {
                panic!("a folded Roll is a Roll in the chosen log");
            };
            assert!(tail.id > SegmentId::FIRST, "a Roll opens a later segment");
            publishes.push(SegmentPublish {
                from: self.config.id,
                journal: self.config.journal.journal,
                generation: self.deployment_matchmakers().generation,
                sealed: SegmentId(tail.id.0 - 1),
                start,
                end: tail.start,
                opening: Opening {
                    replicas: roll.replicas.clone(),
                    boundary: self.replica.journal_at(tail.start),
                },
            });
        }
        if publishes.is_empty() {
            return false;
        }
        probe!(reachable, "chain: a node publishes the end of a segment");
        if let Some(publisher) = self.publisher.as_mut() {
            for publish in publishes {
                publisher.note(publish);
            }
        }
        true
    }

    /// Publish each `Roll` the fold applied since the last note
    /// ([`ColocatedNode::note_folded_rolls`]), and queue the requests.
    pub(super) fn note_rolls(&mut self) {
        if self.note_folded_rolls() {
            self.queue_segment_publishes();
        }
        assert!(
            self.rolls_noted >= self.replica.folded(),
            "every folded Roll is noted"
        );
    }

    /// At boot, note every `Roll` the retained fold holds. The tally is
    /// volatile, so a `Roll` that this node walked before it stopped may
    /// still lack a majority: noting it again makes the end reach the
    /// matchmakers even when every node that walked it restarted. A publish
    /// the matchmakers already hold merges to the same chain. Nothing is
    /// queued: the driver's next [`ColocatedNode::resend_segment_publishes`]
    /// sends it.
    pub(super) fn note_rolls_at_boot(&mut self) {
        assert!(
            self.rolls_noted == Slot(0),
            "the boot notes from the first slot"
        );
        let _ = self.note_folded_rolls();
    }

    /// Queue each pending publish toward every matchmaker of the believed set
    /// that has not acked it.
    fn queue_segment_publishes(&mut self) {
        let Some(publisher) = self.publisher.as_ref() else {
            return;
        };
        let set = self.deployment_matchmakers();
        assert!(
            publisher.generation() == set.generation,
            "a publish is asked of the set its acks are counted for"
        );
        let requests: Vec<(MatchmakerId, SegmentPublish)> = publisher.requests(set).collect();
        assert!(
            requests.iter().all(|(m, r)| set.contains(*m)
                && r.generation == set.generation
                && r.from == self.config.id),
            "a publish addresses the believed set in this node's name"
        );
        self.pending_segment_publishes.extend(requests);
    }

    /// Re-queue every pending publish toward every matchmaker that has not
    /// acked it. A no-op when nothing is pending, and on a plain deployment.
    ///
    /// **The driver is expected to call this on a steady cadence** while
    /// [`ColocatedNode::segment_publish_pending`] reports one, and
    /// **skipping a call is always safe**: a publish that never completes
    /// leaves the chain behind the log, never wrong.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_segment_publishes(&mut self) {
        self.assert_landed();
        if !self.segment_publish_pending() {
            return;
        }
        let queued = self.pending_segment_publishes.len();
        self.queue_segment_publishes();
        assert!(
            self.pending_segment_publishes.len() > queued,
            "a re-send of a pending publish queues a request"
        );
        self.assert_invariants();
    }

    /// Whether a publish waits for a majority of its acks: the driver's cue
    /// to pace [`ColocatedNode::resend_segment_publishes`].
    #[must_use]
    pub fn segment_publish_pending(&self) -> bool {
        self.publisher.as_ref().is_some_and(Publisher::is_pending)
    }

    /// The segments whose publish waits for a majority of its acks.
    pub fn pending_segment_publishes(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.publisher.iter().flat_map(Publisher::pending)
    }

    /// Fold one matchmaker's ack of a publish. A refusal that names a later
    /// set teaches it first (the publishes are asked of the new set). An ack
    /// of another journal, generation or publish, or from a matchmaker
    /// outside the believed set, is ignored (wire input, never asserted).
    /// Returns the segment whose publish a majority now holds.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmaker = ack.matchmaker.0)))]
    pub fn on_segment_ack(&mut self, ack: &SegmentAck) -> Option<SegmentId> {
        self.assert_landed();
        let believed = self.matchmakers.as_ref()?;
        if !believed.contains(ack.matchmaker) || ack.journal != self.config.journal.journal {
            return None;
        }
        // A matchmaker names only a set later than the generation it
        // refused (`Matchmaker::newer_set`), and `learn_matchmakers` takes
        // only a set later than the believed one: it alone decides.
        if !ack.applied
            && let Some(newer) = &ack.newer
            && self.learn_matchmakers(newer)
        {
            assert!(
                self.publisher
                    .as_ref()
                    .is_some_and(|p| p.generation() == newer.generation),
                "the publisher counts acks for the taught successor"
            );
            self.assert_invariants();
            return None;
        }
        let set = self.matchmakers.as_ref()?;
        let completed = self.publisher.as_mut()?.fold_ack(ack, set);
        if let Some(sealed) = completed {
            probe!(reachable, "chain: a majority holds a published end");
            assert!(
                self.pending_segment_publishes().all(|s| s != sealed),
                "a completed publish waits no more"
            );
        }
        self.assert_invariants();
        completed
    }

    /// The publishes are asked of a newer matchmaker generation: the acks of
    /// a replaced one say nothing about the new quorum.
    pub(super) fn reset_publishes_for_generation(&mut self) {
        let generation = self.deployment_matchmakers().generation;
        let Some(publisher) = self.publisher.as_mut() else {
            return;
        };
        publisher.reset_for_generation(generation);
        assert!(
            publisher.generation() == generation,
            "the publisher counts acks for the believed set"
        );
        let queued = self.pending_segment_publishes.len();
        self.queue_segment_publishes();
        assert!(
            self.pending_segment_publishes.len() >= queued,
            "a new generation retracts no queued publish"
        );
    }

    pub(crate) fn queued_segment_publishes(&self) -> &[(MatchmakerId, SegmentPublish)] {
        if !self.pending_segment_publishes.is_empty() {
            assert!(
                self.config.has_matchmakers(),
                "only a matchmaker deployment publishes the chain"
            );
        }
        assert!(
            self.pending_segment_publishes
                .iter()
                .all(|(_, r)| r.from == self.config.id),
            "a publish leaves in this node's name"
        );
        &self.pending_segment_publishes
    }
}
