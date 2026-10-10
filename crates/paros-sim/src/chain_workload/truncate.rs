//! The chain client's truncations: the `TRUNCATE` and `TRUNCATE_STORM`
//! operations, and the writer fence they are sent under (#228).

use paros::LeaderUuid;
use paros::client::{TruncateOutcome, Writer};

/// The writer fence an owner truncates under (#228): the uuid it leads
/// with, or `None` when it leads no term (it sends nothing).
pub(super) fn fence(writer: &Writer) -> Option<LeaderUuid> {
    writer.owned()
}

/// Fold a truncation's verdict back into the writer: a refusal names the
/// writer in force, so a superseded owner stops.
pub(super) fn absorb_truncate(writer: &mut Writer, outcome: Option<&TruncateOutcome>) {
    if let Some(outcome) = outcome {
        writer.absorb_truncate(outcome);
    }
}
