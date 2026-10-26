//! The tenant scrubber's progress record (#362 (background scrubbing),
//! `docs/architecture/05-failure-model-and-zones.md` §5): where the elected scrubber stands in its pass
//! and what it found, flushed to the tenant's control journal so a later term
//! resumes from it.
//!
//! A pass reads every live journal in id order, each from its first slot. A
//! record is a cursor (`pass`, `done`, `journal`, `next_slot`) and the counts
//! it reached; the fold keeps the newest one and refuses a record behind it,
//! so a stale term's flush never moves a pass back.

use paros_core::journal::JournalId;

use paros_wire::tenant as wire;

/// Where the scrubber stands and what it found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScrubProgress {
    /// The pass, from 1 (0: none started).
    pub pass: u64,
    /// The pass read every live journal.
    pub done: bool,
    /// The journal the pass reads now (unset before the first one, or when
    /// the pass is done).
    pub journal: JournalId,
    /// That journal's next slot.
    pub next_slot: u64,
    /// This pass's counts.
    pub counts: ScrubCounts,
    /// Damaged records found over every pass.
    pub damaged_total: u64,
    /// Damaged records written again over every pass.
    pub repaired_total: u64,
    /// Value mismatches over every pass.
    pub mismatched_total: u64,
    /// The pass outlived the full-pass bound.
    pub late: bool,
}

/// One pass's counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScrubCounts {
    /// Records read, summed over the acceptors.
    pub checked: u64,
    /// Records an acceptor found damaged.
    pub damaged: u64,
    /// Damaged records the acceptor wrote again from its own copy.
    pub repaired: u64,
    /// Slots under the chosen prefix where two acceptors hold different
    /// values: never expected (the simulation asserts it never happens).
    pub mismatched: u64,
}

impl ScrubCounts {
    /// `self` plus `other`, saturating.
    #[must_use]
    pub fn plus(self, other: ScrubCounts) -> ScrubCounts {
        ScrubCounts {
            checked: self.checked.saturating_add(other.checked),
            damaged: self.damaged.saturating_add(other.damaged),
            repaired: self.repaired.saturating_add(other.repaired),
            mismatched: self.mismatched.saturating_add(other.mismatched),
        }
    }
}

impl Default for ScrubProgress {
    /// No pass started: the progress a tenant holds before its first record.
    fn default() -> Self {
        Self {
            pass: 0,
            done: true,
            journal: JournalId(0),
            next_slot: 0,
            counts: ScrubCounts::default(),
            damaged_total: 0,
            repaired_total: 0,
            mismatched_total: 0,
            late: false,
        }
    }
}

impl ScrubProgress {
    /// The cursor the fold orders records by.
    #[must_use]
    pub fn cursor(&self) -> (u64, bool, u64, u64) {
        (self.pass, self.done, self.journal.0, self.next_slot)
    }

    /// The progress after `found` more, at a new cursor of the same pass.
    ///
    /// # Panics
    ///
    /// Never: the counts only add up, saturating.
    #[must_use]
    pub fn advanced(&self, found: ScrubCounts, journal: JournalId, next_slot: u64) -> Self {
        let advanced = Self {
            journal,
            next_slot,
            counts: self.counts.plus(found),
            damaged_total: self.damaged_total.saturating_add(found.damaged),
            repaired_total: self.repaired_total.saturating_add(found.repaired),
            mismatched_total: self.mismatched_total.saturating_add(found.mismatched),
            ..*self
        };
        assert!(
            advanced.counts.checked >= self.counts.checked,
            "a pass's checked count never goes back"
        );
        assert!(
            advanced.damaged_total >= self.damaged_total,
            "a damaged total never goes back"
        );
        advanced
    }

    /// The next pass, from its start, its counts reset.
    ///
    /// # Panics
    ///
    /// If the pass number overflows.
    #[must_use]
    pub fn next_pass(&self) -> Self {
        let next = Self {
            pass: self.pass + 1,
            done: false,
            journal: JournalId(0),
            next_slot: 0,
            counts: ScrubCounts::default(),
            late: false,
            ..*self
        };
        assert!(next.cursor() > self.cursor(), "a new pass is ahead");
        next
    }

    /// This pass, done.
    ///
    /// # Panics
    ///
    /// Never for a cursor the fold accepted: a cursor that is not done is
    /// behind its own pass's end.
    #[must_use]
    pub fn finished(&self) -> Self {
        let done = Self {
            done: true,
            journal: JournalId(0),
            next_slot: 0,
            ..*self
        };
        assert!(
            self.done || done.cursor() > self.cursor(),
            "a finished pass is ahead of its last cursor"
        );
        done
    }

    pub(crate) fn to_wire(self) -> wire::Scrubbed {
        wire::Scrubbed {
            pass: self.pass,
            done: self.done,
            journal: self.journal.0,
            next_slot: self.next_slot,
            checked: self.counts.checked,
            damaged: self.counts.damaged,
            repaired: self.counts.repaired,
            mismatched: self.counts.mismatched,
            damaged_total: self.damaged_total,
            repaired_total: self.repaired_total,
            mismatched_total: self.mismatched_total,
            late: self.late,
        }
    }

    pub(crate) fn from_wire(scrubbed: &wire::Scrubbed) -> Result<Self, &'static str> {
        if scrubbed.pass == 0 {
            return Err("a scrub record names its pass");
        }
        if scrubbed.done && (scrubbed.journal != 0 || scrubbed.next_slot != 0) {
            return Err("a finished pass names no journal");
        }
        if scrubbed.repaired > scrubbed.damaged
            || scrubbed.damaged_total < scrubbed.damaged
            || scrubbed.repaired_total < scrubbed.repaired
            || scrubbed.mismatched_total < scrubbed.mismatched
        {
            return Err("a scrub record's counts do not add up");
        }
        Ok(Self {
            pass: scrubbed.pass,
            done: scrubbed.done,
            journal: JournalId(scrubbed.journal),
            next_slot: scrubbed.next_slot,
            counts: ScrubCounts {
                checked: scrubbed.checked,
                damaged: scrubbed.damaged,
                repaired: scrubbed.repaired,
                mismatched: scrubbed.mismatched,
            },
            damaged_total: scrubbed.damaged_total,
            repaired_total: scrubbed.repaired_total,
            mismatched_total: scrubbed.mismatched_total,
            late: scrubbed.late,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_moves_forward_and_its_counts_add_up() {
        let first = ScrubProgress::default().next_pass();
        assert_eq!(first.pass, 1);
        let found = ScrubCounts {
            checked: 9,
            damaged: 2,
            repaired: 1,
            mismatched: 0,
        };
        let moved = first.advanced(found, JournalId(5), 3);
        assert!(moved.cursor() > first.cursor());
        assert_eq!(moved.damaged_total, 2);
        let done = moved.finished();
        assert!(done.cursor() > moved.cursor());
        let second = done.next_pass();
        assert_eq!(second.counts, ScrubCounts::default());
        assert_eq!(second.repaired_total, 1);
        assert_eq!(ScrubProgress::from_wire(&second.to_wire()), Ok(second));
        assert_eq!(ScrubProgress::from_wire(&done.to_wire()), Ok(done));
    }

    #[test]
    fn a_malformed_scrub_record_is_refused() {
        let unset = wire::Scrubbed::default();
        assert!(ScrubProgress::from_wire(&unset).is_err());
        let too_many = wire::Scrubbed {
            pass: 1,
            damaged: 1,
            repaired: 2,
            damaged_total: 1,
            repaired_total: 2,
            ..wire::Scrubbed::default()
        };
        assert!(ScrubProgress::from_wire(&too_many).is_err());
        let done_with_cursor = wire::Scrubbed {
            pass: 1,
            done: true,
            journal: 4,
            ..wire::Scrubbed::default()
        };
        assert!(ScrubProgress::from_wire(&done_with_cursor).is_err());
    }
}
