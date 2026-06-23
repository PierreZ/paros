//! `paros` — the user-facing entry point for this Paxos implementation.
//!
//! This is the umbrella/facade crate (mirroring moonpool's `moonpool` crate). It
//! re-exports the sans-IO [`paros_core`] so downstream users can depend on a
//! single `paros` crate. The production **client** API and the **binary** (a
//! tokio-driven runner — a trivial provider swap over the simulation driver)
//! will live here once the core stabilizes.

pub use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Config, HardState, Message, NodeId, RawNode, Ready, Slot,
    Storage, Value,
};
