//! **paros-host**: a pure host of actors (§4.4 of
//! `docs/architecture/04-roles.md`, #1410 (split the generic host of actors);
//! the flight rule (§J4.2) and the rule of no runtime (§J4.3) are in
//! `docs/journal/04-roles.md` at the git tag `journal-design-a`).
//!
//! A role is an actor: a deterministic step under the flight rule. It reads
//! no clock, draws no random number and does no I/O; a send waits for the
//! persist batch that it names, and a refusal is data. One host serves every
//! role of a process: the roles of the core, a journal, the election, the
//! coordinators and the frontend. This crate knows none of them:
//!
//! - [`Family`]: the types that every role of one host shares (an opaque
//!   address, a message, an op of the disk format, a batch id, ...).
//! - [`Role`]: one actor, with [`Start`].
//! - [`Host`]: the pure state machine that maps each address to its role,
//!   starts a role from its records, routes each input, and keeps the
//!   deadline of each role's timer.
//! - [`Machine`]: what a driver drives; a `Host` is one, and a family that
//!   routes some inputs itself wraps a `Host` in its own.
//! - [`Driver`]: the generic driver, with group commit and held sends and
//!   outputs (the flight rule), over a [`Storage`], a [`Transport`], a
//!   [`Clock`], an [`Entropy`] source, the output [`Ports`] and the
//!   [`Events`] port.
//!
//! **No runtime** (§J4.3). This crate depends on no crate. No trait
//! requires `Send`, an executor or a runtime type.
//!
//! The example `coordinator` runs a role that is not a journal role on it:
//! a toy coordinator with a disk and a watcher with none.

pub mod driver;
pub mod family;
pub mod host;
pub mod io;
pub mod machine;
pub mod ports;
pub mod role;
pub mod time;

#[cfg(test)]
mod toy;

pub use driver::Driver;
pub use family::Family;
pub use host::{Effect, Host, Input};
pub use io::{Clock, Entropy, NoDraws, Storage, StorageError, Transport, Verdict};
pub use machine::Machine;
pub use ports::{Event, Events, Ports};
pub use role::{Role, Start};
pub use time::Time;
