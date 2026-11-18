//! `paros` — Paxos in Rust, the library facade.
//!
//! Each layer of the library is a crate of its own, and this crate
//! re-exports each one as a module. Today there is one layer: [`core`], the
//! sans-IO consensus state machine (no I/O, no clock, no randomness).
//!
//! The journal layers that sat above the core (wire, model, storage, net,
//! client, roles, coordinator, machine) and the `parosd` service were removed
//! on 2026-11-19. They are at the git tag `v0.2.0-alpha.1`, and
//! `docs/archive/v0.2.0-alpha.1/` has one recap per removed crate.

pub use paros_core as core;
