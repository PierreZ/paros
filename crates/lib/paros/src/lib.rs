//! `paros` — Paxos in Rust, the library facade.
//!
//! Each layer of the library is a crate of its own, and this crate
//! re-exports each one as a module:
//!
//! - [`consensus`]: the sans-IO consensus state machine (no I/O, no clock, no
//!   randomness).
//!
//! The journal is designed again on the consensus core. The journal layers
//! of the earlier designs are at the git tags `v0.2.0-alpha.1` and
//! `journal-design-a`, and `docs/archive/` has one recap per removed crate.

pub use paros_consensus as consensus;
