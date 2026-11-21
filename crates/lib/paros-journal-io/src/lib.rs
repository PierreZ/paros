//! **paros-journal-io**: the traits that a backend of the paros journal
//! implements, the pure [`Host`] that serves many journals in one process,
//! and a generic [`Driver`] under the flight rule (#1329,
//! `docs/journal/04-roles.md` §J4.2).
//!
//! The journal roles of `paros_journal::roles` are pure state machines. A
//! program runs them by bringing four things, each a trait of this crate:
//!
//! - [`Storage`]: one atomic `commit` of typed ops, and a range read of one
//!   role's records. The format is this crate's ([`layout`]); the backend
//!   stores keys and bytes.
//! - [`Transport`]: send a message, receive the next one.
//! - [`Clock`]: the time now.
//! - [`Register`]: the journal's compare-and-set register (§J3.4).
//!
//! Then [`Driver::poll`] in a loop. The driver steps the [`Host`], groups
//! the persist batches of a round into one commit, and transmits each send
//! only once the batch that it names is durable. The [`Events`] port
//! reports each step of the `Host`, for the trace check and the grid
//! controller.
//!
//! **No runtime** (§J4.3). This crate depends on the workspace crates only.
//! No trait requires `Send`, an executor or a runtime type, so a backend
//! may run on compio, tokio, a thread, or nothing. An adapter that does
//! I/O is a crate of its own (`paros-journal-moonpool`, #1330).
//!
//! [`mem`] has backends in memory, and [`contract`] the suites that check a
//! backend. The examples (`examples/`) run a whole journal on them, and on
//! a storage written by hand.

pub mod clock;
pub mod contract;
pub mod driver;
pub mod events;
pub mod host;
pub mod layout;
pub mod mem;
pub mod register;
pub mod storage;
pub mod transport;
pub mod wire;

pub use clock::Clock;
pub use driver::{CallReply, Driver};
pub use events::{Event, Events};
pub use host::{Assignment, CallId, CallOutcome, Effect, Host, HostConfig, Input, Stop};
pub use layout::{Damage, Entry, Key, Kind, Op};
pub use register::Register;
pub use storage::{Storage, StorageError};
pub use transport::Transport;
pub use wire::{Address, Envelope, Time};

pub use paros_consensus::journal::JournalId;
