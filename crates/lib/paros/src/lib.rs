//! `paros` — Paxos in Rust, the library facade.
//!
//! Each layer of the library is a crate of its own, and this crate
//! re-exports each one as a module:
//!
//! - [`consensus`]: the sans-IO consensus state machine (no I/O, no clock, no
//!   randomness).
//! - [`journal`]: the pure core of the journal v1 (`docs/journal/`): the fold
//!   of the data plane, the segment record, the `Resolve` call and the codec.
//! - [`journal_io`]: the traits that a backend of the journal implements
//!   (storage, transport, clock, register), the pure `Host` and the generic
//!   driver under the flight rule. No runtime: a program brings the backends.
//!
//! The journal layers of the old design (wire, model, storage, net, client,
//! roles, coordinator, machine) and the `parosd` service are at the git tag
//! `v0.2.0-alpha.1`, and `docs/archive/v0.2.0-alpha.1/` has one recap per
//! removed crate.

pub use paros_consensus as consensus;
pub use paros_journal as journal;
pub use paros_journal_io as journal_io;
