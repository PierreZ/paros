//! The order of register values, written again from §J3.4 and
//! `specs/tla/segment-records/` (`AtOrAfter`), with no code of
//! `paros_journal::register`: it reads the records as data only.
//!
//! A value `r` is at or after `s` when nothing of `s` moved back in `r`: the
//! term never falls, a record is never removed, a segment's state never
//! goes back, and a start, a seal end or a boundary state, once written,
//! stays the same.

use paros_journal::register::RegisterValue;
use paros_journal::segment::{SegmentId, SegmentState};

/// Whether `r` is at or after `s`.
#[must_use]
pub fn at_or_after(r: &RegisterValue, s: &RegisterValue) -> bool {
    r.term().number >= s.term().number
        && s.segments().iter().all(|(id, old)| {
            r.segments().get(id).is_some_and(|new| {
                rank(new.state()) >= rank(old.state())
                    && (old.start().is_none() || new.start() == old.start())
                    && (old.end().is_none() || new.end() == old.end())
                    && (old.boundary().is_none() || new.boundary() == old.boundary())
                    && new.opening() == old.opening()
            })
        })
}

/// Whether `segment` of `value` is at `least` or later.
#[must_use]
pub fn holds(value: &RegisterValue, segment: SegmentId, least: SegmentState) -> bool {
    value
        .segments()
        .get(&segment)
        .is_some_and(|record| rank(record.state()) >= rank(least))
}

/// The place of a state in the life of a segment (§J3.4), written out so
/// that the oracle does not lean on the order of the code's enum.
fn rank(state: SegmentState) -> u8 {
    match state {
        SegmentState::Pending => 0,
        SegmentState::InForce => 1,
        SegmentState::Sealed => 2,
        SegmentState::Retired => 3,
        SegmentState::Offloaded => 4,
        SegmentState::Trimmed => 5,
    }
}
