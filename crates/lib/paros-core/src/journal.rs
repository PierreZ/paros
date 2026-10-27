//! A **journal**: the identifiers that name one, the command it decides into
//! a slot, and the state machine that judges each command at apply.

pub mod command;
pub mod ids;
pub mod state;

pub use command::{Command, Control, Entry, command_fingerprint};
pub use ids::{JournalId, JournalIdentifier, LeaderUuid, Seq, TenantId};
pub use state::{JournalState, JournalView, Outcome, WriterMode};
