//! `parosd` — the paros server: every role's provider-generic driver run
//! over moonpool's **Tokio providers** (#206), the journal stores on a real
//! filesystem, and the plumbing a process needs around them — a topology
//! read from flags or `PAROS_*` variables, a tracing subscriber, `SIGTERM`
//! to the driver's cancellation token, and an exit code per
//! [`RunError`](paros::RunError) variant.
//!
//! This is the one crate of the workspace that links `TokioProviders`: the
//! `paros` library stays free of production providers and wasm-checkable,
//! and runs here unchanged — the code the simulation tests is the code
//! that ships.

pub mod exit;
pub mod stores;
pub mod topology;
