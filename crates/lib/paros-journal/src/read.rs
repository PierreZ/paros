//! The answer of a `Read` (§J2.5, §J3.3): one type for the segment replica
//! that serves it ([`crate::roles::SegmentReplica::read`]) and the client
//! that takes it ([`crate::client`]).

use paros_consensus::journal::Seq;

use crate::command::Record;
use crate::fold::JournalView;
use crate::segment::SegmentId;

/// The answer to one `Read(from, limit)` (§J2.5): what a replica of a
/// segment serves, and what the client takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadAnswer {
    /// The records from the asked position, in order, at most the limit,
    /// and the view of the fold that served them. `sealed` is true when the
    /// segment is sealed and the page reaches its last record: the next
    /// records are in a later segment.
    Page {
        /// Each record with its position.
        records: Vec<(Seq, Record)>,
        /// The view of the fold.
        view: JournalView,
        /// Whether the segment is sealed and the page reaches its end.
        sealed: bool,
    },
    /// The position is below the trim floor.
    Truncated(JournalView),
    /// The position is in an older segment: ask a replica of that segment.
    /// The view is the fold of the segment asked.
    Below(JournalView),
    /// An acceptor of the grid knows a later segment: read the register
    /// again and ask a replica of the segment in force.
    Superseded(SegmentId),
    /// This replica cannot serve the read now: ask again, or ask another.
    Unserved,
}
