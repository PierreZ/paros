//! **`Resolve`**: the read-only call that settles a `Below` answer
//! (`docs/journal/02-data-plane.md` §J2.2; decided by Pierre on 2026-11-21,
//! #1320 (the wire shape that resolves a `Below` answer), option (a)).
//!
//! A fenced write resent across a seal is answered `Below`: its position is
//! in an older segment, whose records the fold of the segment in force does
//! not hold. The client then sends `Resolve(uuid, expected_seq, write)` to a
//! replica of the segment that holds `expected_seq`. The replica judges the
//! write against the write that took that position, with the compare of the
//! fold ([`Write::same_write`]): the same write is written, anything else
//! is not written. Below the replica's `first_seq` it is `Truncated`.
//!
//! The replica answers only once its fold covers the position. A sealed
//! segment does not change, so the answer is then final (except
//! `Truncated`). `Resolve` only reads: it never writes the sealed segment,
//! never moves its end and never places the write there. After "not
//! written", the writer reads the tail and sends the write again to the
//! segment in force with a new `expected_seq`.
//!
//! TLC checks the call in `specs/tla/journal-state/` (`ResolveTrue`,
//! `ResolveReadOnly`; controls `resolve_uuid` and `resolve_early`).

use paros_consensus::journal::{LeaderUuid, Seq};

use crate::command::{Fence, Record, Write};
use crate::fold::JournalState;

/// The answer of a `Resolve`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResolveAnswer {
    /// The write took its position: its records hold `[seq, seq + count)`.
    /// Final.
    Written {
        /// The position of the first record.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// Another write took the position: this write is not in this
    /// segment, now and for ever. Final.
    NotWritten,
    /// The position is below the replica's `first_seq`: the fate is
    /// unknown. Not final.
    Truncated,
    /// This replica cannot judge yet: its fold does not cover the position
    /// (it did not apply that far, or the position is not in its segment).
    /// The client asks again later, or asks another replica of the segment.
    NotCovered,
}

/// Judge `Resolve(uuid, expected_seq, records)` at a replica whose fold is
/// `state`. `written_at(seq)` answers "which write took position `seq` in
/// this segment?", as for [`JournalState::apply`]; it is asked only for a
/// position that the fold [`holds`](JournalState::holds).
///
/// The call takes the state by shared reference: it changes nothing.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error, never
/// an operating condition.
#[must_use]
pub fn resolve<'a>(
    state: &JournalState,
    uuid: LeaderUuid,
    expected_seq: Seq,
    records: &[Record],
    written_at: impl Fn(Seq) -> Option<&'a Write>,
) -> ResolveAnswer {
    state.assert_invariants();
    if expected_seq < state.first_seq() {
        return ResolveAnswer::Truncated;
    }
    if !state.holds(expected_seq) {
        return ResolveAnswer::NotCovered;
    }
    let write = Write {
        fence: Fence::Leader { uuid, expected_seq },
        records: records.to_vec(),
    };
    let answer = match written_at(expected_seq) {
        Some(original) if original.same_write(&write) => ResolveAnswer::Written {
            seq: expected_seq,
            count: write.count(),
        },
        _ => ResolveAnswer::NotWritten,
    };
    if let ResolveAnswer::Written { seq, count } = answer {
        assert!(
            count > 0 || records.is_empty(),
            "a written write counts its records"
        );
        assert!(
            seq.0 + count <= state.next_seq().0,
            "a written write lies below the fold's next position"
        );
    }
    answer
}

#[cfg(test)]
mod tests;
