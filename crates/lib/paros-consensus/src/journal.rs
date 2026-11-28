//! A **journal**: the identifiers that name one, the command it decides into
//! a slot, the segments of its log and their chain, and the state machine that judges each
//! command at apply.

pub mod chain;
pub mod command;
pub mod ids;
pub mod segment;
pub mod state;

pub use chain::{Chain, Opening, SegmentRecord};
pub use command::{Command, Control, Entry, command_fingerprint};
pub use ids::{JournalId, JournalIdentifier, LeaderUuid, Seq, TenantId};
pub use segment::{Hold, ReplicaSet, ReplicaSetError, Roll, Segment, SegmentId, SetReplicas};
pub use state::{JournalState, JournalView, Outcome, WriterMode};
