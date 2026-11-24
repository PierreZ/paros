//! `paros` — Paxos in Rust, the library facade.
//!
//! Each layer of the library is a crate of its own, and this crate
//! re-exports each one as a module:
//!
//! - [`consensus`]: the sans-IO consensus state machine (no I/O, no clock, no
//!   randomness).
//! - [`host`]: the pure host of actors: the `Role` trait, the `Host` that
//!   serves many roles in one process, and the generic driver with group
//!   commit and held sends (the flight rule). It knows no journal.
//!
//! The journal is designed again on the consensus core. The journal layers
//! of the earlier designs are at the git tags `v0.2.0-alpha.1` and
//! `journal-design-a`, and `docs/archive/` has one recap per removed crate.

pub use paros_consensus as consensus;
pub use paros_host as host;
