//! `paros-sim` — the deterministic-simulation driver, Paxos workloads, and
//! oracles, backed by moonpool.
//!
//! This crate wraps the sans-IO [`paros_core::RawNode`] in moonpool's
//! deterministic providers and transport (the async driver: step/tick/Ready/
//! advance ↔ I/O, durable-before-send), and defines the `Workload`s and
//! `Invariant`s once so both the native runner and the wasm demo reuse them.
//! It is kept wasm-safe (`default-features = false`).
//!
//! The moonpool integration arrives in Stage 1 (#14); Stage 0 is an empty
//! scaffold.
