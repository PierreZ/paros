//! The wiring of the **held tally** (R7 of
//! `docs/architecture/04-7-segments.md` §4.7) between the replicas'
//! [`Message::SegmentHeld`](crate::Message::SegmentHeld) reports, the fold
//! (which gives each sealed segment its end and its replicas,
//! `note_folded_rolls`) and the publisher (which says when the end is in the
//! chain). The count itself is [`HeldTally`](crate::collector::HeldTally).
//!
//! A leader proposes a [`Hold`] for each segment its tally marks, in order,
//! so that the held segments of the log are a prefix (D9: the held floor is a
//! decided command, judged at apply by [`crate::journal::JournalState`]).
//!
//! Segments exist only with matchmakers (R10, D5): a deployment without them
//! ignores every report, and its tally stays empty.

use super::{ColocatedNode, Delegation, NodeRole, ProposeResult};
use crate::journal::{Command, Hold, SegmentId};
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
        self.propose_holds();
    }

    /// Mark `segment` `Held` when its reports and the chain allow it: the
    /// end is in the chain once this node's publish of it completed. `F` is
    /// the `f` of the configuration that this node believes in force when it
    /// judges (D8).
    pub(super) fn judge_held(&mut self, segment: SegmentId) {
        let published = self
            .publisher
            .as_ref()
            .is_some_and(|p| Publisher::is_published(p, segment));
        let config = self.belief.config();
        let first_seq = self.replica.journal().first_seq;
        let Some(held) = self.held.as_mut() else {
            return;
        };
        if held.judge(segment, config, published, first_seq) {
            assert!(published, "a held segment's end is in the chain");
            assert!(held.is_held(segment), "a judged segment is marked held");
        }
    }

    /// Judge each noted segment that the fold's `first_seq` released, after
    /// a walk: a `Truncate` can release a segment whose end the chain
    /// already holds, and no report or publish comes after it. Then propose
    /// the `Hold`s it allows.
    pub(super) fn judge_released(&mut self) {
        let first_seq = self.replica.journal().first_seq;
        let Some(held) = self.held.as_ref() else {
            return;
        };
        assert!(
            self.config.has_matchmakers(),
            "only a matchmaker deployment keeps a held tally"
        );
        let released: Vec<SegmentId> = held.released(first_seq).collect();
        if released.is_empty() {
            return;
        }
        for segment in released {
            self.judge_held(segment);
            // The released rule: a released segment needs no report, only
            // its end in the chain.
            let published = self
                .publisher
                .as_ref()
                .is_some_and(|p| Publisher::is_published(p, segment));
            assert!(
                !published || self.held.as_ref().is_some_and(|h| h.is_held(segment)),
                "a released segment whose end is in the chain is held"
            );
        }
        self.propose_holds();
    }

    /// The held prefix as this node knows the log: the fold's, then each
    /// later `Hold` it knows ([`ColocatedNode::known_past_fold`]), in slot
    /// order, when it names the segment right after the prefix (the rule of
    /// the fold). `None` while no segment is held.
    pub(super) fn known_held(&self) -> Option<SegmentId> {
        let folded = self.replica.journal().held.map(|hold| hold.segment);
        let mut prefix = folded;
        for command in self.known_past_fold().into_values() {
            if let Command::Hold(hold) = command
                && hold.segment == prefix.map_or(SegmentId::FIRST, SegmentId::successor)
            {
                prefix = Some(hold.segment);
            }
        }
        assert!(
            prefix >= folded,
            "the known held prefix is at or past the folded one"
        );
        prefix
    }

    /// Propose a `Hold` for each segment past the known held prefix that the
    /// tally marked `Held`, in order, until one is not: the fold accepts a
    /// `Hold` only for the segment right after the prefix. A node that is
    /// not the leader proposes nothing, and a new leader proposes the
    /// segments its own tally marks again from the reports.
    pub(super) fn propose_holds(&mut self) {
        if self.role != NodeRole::Leader {
            return;
        }
        let Some(tally) = self.held.as_ref() else {
            return;
        };
        let mut next = self
            .known_held()
            .map_or(SegmentId::FIRST, SegmentId::successor);
        let mut holds = Vec::new();
        while tally.is_held(next) {
            let sealed = tally.sealed(next).expect("a held segment is noted");
            holds.push(Hold {
                segment: next,
                end: sealed.end,
                next_seq: sealed.next_seq,
            });
            next = next.successor();
        }
        for hold in holds {
            assert!(
                hold.end.0 > hold.segment.0,
                "a held segment ends past the Roll of each segment before it"
            );
            if let ProposeResult::NotLeader(_) =
                self.propose_command_at(Command::Hold(hold), Delegation::Auto)
            {
                return;
            }
            assert!(
                self.known_held() == Some(hold.segment),
                "an admitted Hold extends the known held prefix"
            );
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
