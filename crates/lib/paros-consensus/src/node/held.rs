//! The wiring of the **held tally** (R7 of
//! `docs/architecture/04-7-segments.md` §4.7) between the replicas'
//! [`Message::SegmentHeld`](crate::Message::SegmentHeld) reports, the fold
//! (which gives each sealed segment its end and its replicas,
//! `note_folded_rolls`) and the publisher (which says when the end is in the
//! chain). The count itself is [`HeldTally`](crate::collector::HeldTally).
//!
//! Segments exist only with matchmakers (R10, D5): a deployment without them
//! ignores every report, and its tally stays empty.

use super::ColocatedNode;
use crate::journal::SegmentId;
use crate::publisher::Publisher;
use crate::types::{NodeId, Slot};

impl ColocatedNode {
    /// Fold the report of the replica `from` that it holds `segment` up to
    /// `end`, and judge the segment. A report on a plain deployment, or of a
    /// segment whose `Roll` this node has not folded, changes nothing (wire
    /// input, never asserted).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, segment = segment.0)))]
    pub(super) fn on_segment_held(&mut self, from: NodeId, segment: SegmentId, end: Slot) {
        let Some(held) = self.held.as_mut() else {
            assert!(
                !self.config.has_matchmakers(),
                "a matchmaker deployment keeps a held tally"
            );
            return;
        };
        let counted = held.report(from, segment, end);
        assert!(
            !counted || held.sealed(segment).is_some(),
            "a counted report names a noted segment"
        );
        if counted {
            self.judge_held(segment);
        }
    }

    /// Mark `segment` `Held` when its reports and the chain allow it: the
    /// end is in the chain once this node's publish of it completed.
    pub(super) fn judge_held(&mut self, segment: SegmentId) {
        let published = self
            .publisher
            .as_ref()
            .is_some_and(|p| Publisher::is_published(p, segment));
        let config = self.belief.config();
        let Some(held) = self.held.as_mut() else {
            return;
        };
        if held.judge(segment, config, published) {
            assert!(published, "a held segment's end is in the chain");
            assert!(held.is_held(segment), "a judged segment is marked held");
        }
    }

    /// The segments this node's tally marked `Held` (R7): more than `F`
    /// of the replicas that the fold names hold the whole segment, and its
    /// end is in the chain. Empty on a deployment without matchmakers.
    /// Volatile: a restart forgets it, and the replicas report again.
    ///
    /// # Panics
    ///
    /// If the tally and the deployment disagree on matchmakers: a
    /// programmer error.
    pub fn held_segments(&self) -> impl Iterator<Item = SegmentId> + '_ {
        assert!(
            self.held.is_some() == self.config.has_matchmakers(),
            "a matchmaker deployment, and it alone, keeps a held tally"
        );
        assert!(
            self.held.is_some() == self.publisher.is_some(),
            "the held tally lives beside the publisher"
        );
        self.held.iter().flat_map(|held| held.held())
    }
}
