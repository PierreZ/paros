//! The floor events of a cluster walk, for
//! `specs/tla/node/TruncationTrace.tla`: the log of `MultiPaxosTrace` and
//! the compaction floor of `specs/tla/node/Node.tla` (#1231 (traced walks
//! reach the rules)).
//!
//! The floor is recorded at two seams: a batch reached the disk with a
//! `WriteOp::Truncate` (`Compact`) or a `WriteOp::TrimmedTo` (`Trim`), and
//! the first party that applied a slot folded it (`Fold`): the end of its
//! records and `first_seq` after it. The fold is a function of the chosen
//! log, so every party folds a slot the same way; the tracer asserts it.

use paros_consensus::journal::Outcome;
use paros_consensus::{Slot, WriteOp};

use super::{Batch, ClusterEvent, Tracer};

/// The floor steps of `TruncationTrace`.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Fold { .. } | ClusterEvent::Compact { .. } | ClusterEvent::Trim { .. }
    )
}

impl Tracer {
    /// `compact` and `trim`: each floor that the batch wrote.
    pub(super) fn floors(batch: &Batch<'_>, events: &mut Vec<ClusterEvent>) {
        for op in batch.writes {
            match op {
                WriteOp::Truncate { first, .. } => events.push(ClusterEvent::Compact {
                    node: batch.id,
                    floor: *first,
                }),
                WriteOp::TrimmedTo { point, .. } => events.push(ClusterEvent::Trim {
                    node: batch.id,
                    floor: *point,
                }),
                _ => {}
            }
        }
    }

    /// `fold`: a party applied `slot` with `outcome`. The first party to
    /// apply a slot records its fold; a later one must fold it the same.
    ///
    /// # Panics
    ///
    /// When a party applies a slot above the next one no party folded, or
    /// folds a slot otherwise than the first party did.
    pub(in crate::cluster) fn fold(
        &mut self,
        slot: Slot,
        outcome: &Outcome,
    ) -> Option<ClusterEvent> {
        let index = usize::try_from(slot.0).expect("a slot");
        let before = self.folds.last().map_or(0, |(_, first)| *first);
        let (high, first) = match outcome {
            Outcome::Accepted { seq, count } => (seq.0 + count, before),
            Outcome::Trimmed(view) => (0, view.first_seq.0),
            _ => (0, before),
        };
        if let Some(folded) = self.folds.get(index) {
            assert_eq!(
                folded.0, high,
                "every party folds the records of slot {slot:?} the same"
            );
            return None;
        }
        assert_eq!(index, self.folds.len(), "the slots are folded in order");
        assert!(first >= before, "first_seq never goes down");
        self.folds.push((high, first));
        Some(ClusterEvent::Fold { slot, high, first })
    }
}
