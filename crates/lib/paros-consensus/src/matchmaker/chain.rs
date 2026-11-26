//! The **chain of segments** at a matchmaker (R5 of §4.7): a node's
//! [`SegmentPublish`] merged into the journal's chain, and its ack.
//!
//! The chain is an add-only map ([`crate::journal::chain`]), so a publish is a
//! merge and never a compare-and-set: every writer of a field writes the same
//! value, because each field is a function of the chosen log. A publish that
//! only a minority took is already true. A publish completes at the node when
//! a majority of one generation acked it, and the handover carries the chain
//! from a majority of reports to the next generation
//! (`specs/tla/handover-scalars/`, the `chain` checks).

use super::{Matchmaker, SegmentAck, SegmentPublish};
use crate::journal::chain::{Chain, merge_chain};

/// What a [`SegmentPublish`] did at this matchmaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The chain grew (a durable write is staged).
    Merged,
    /// The chain held the publish already: nothing changed.
    Unchanged,
    /// Not active for the addressed generation: refused.
    Refused,
}

impl Matchmaker {
    /// The journal's chain of segments, as it stands (`None` when no
    /// publish reached this matchmaker).
    #[must_use]
    pub fn chain(&self) -> Option<&Chain> {
        Some(&self.hard_state.journal.segments).filter(|chain| !chain.is_empty())
    }

    /// Merge a node's publish into the journal's chain and stage the durable
    /// write. A publish that addresses a generation this matchmaker is not
    /// active for is refused (a frozen member takes nothing after its report
    /// left: the control `chain_frozen` of `specs/tla/handover-scalars/`).
    ///
    /// # Panics
    ///
    /// If the publish disagrees with the chain held here: two ends of one
    /// segment are two values chosen for one slot, a detected safety
    /// violation (R3, R5).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, segment = request.sealed.0)))]
    pub fn publish_segment(&mut self, request: &SegmentPublish) -> PublishOutcome {
        self.assert_invariants();
        if self.generation_refusal(request.generation).is_some() {
            return PublishOutcome::Refused;
        }
        let records = request.records();
        let held = self.chain().cloned().unwrap_or_default();
        let mut merged = held.clone();
        merge_chain(&mut merged, &records);
        if merged == held {
            return PublishOutcome::Unchanged;
        }
        probe!(reachable, "matchmaker: a publish grows the chain");
        let writes = self.pending_writes.len();
        self.hard_state.journal.segments = merged;
        self.stage_scalars();
        assert!(
            self.pending_writes.len() == writes + 1,
            "a publish stages exactly one write"
        );
        assert!(
            self.chain().is_some_and(|chain| {
                records
                    .iter()
                    .all(|(id, record)| chain.get(id).is_some_and(|h| h.end >= record.end))
            }),
            "the chain holds every record the publish wrote"
        );
        self.assert_invariants();
        PublishOutcome::Merged
    }

    /// The answer to `request`, once [`Matchmaker::publish_segment`] returned
    /// `outcome` for it and its write is durable. A refusal names the later
    /// set to ask instead ([`Matchmaker::newer_set`]).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn segment_ack(&self, request: &SegmentPublish, outcome: PublishOutcome) -> SegmentAck {
        let refused = outcome == PublishOutcome::Refused;
        let generation = request.generation;
        let ack = SegmentAck {
            matchmaker: self.config.id,
            generation,
            sealed: request.sealed,
            applied: !refused,
            newer: refused.then(|| self.newer_set(generation)).flatten(),
        };
        assert!(
            ack.applied || self.generation_refusal(generation).is_some(),
            "only a matchmaker not active for the generation refuses"
        );
        assert!(
            !ack.applied
                || self
                    .chain()
                    .and_then(|chain| chain.get(&request.sealed))
                    .is_some_and(|record| record.end == Some(request.end)),
            "an applied publish acks a chain that holds its end"
        );
        ack
    }
}
