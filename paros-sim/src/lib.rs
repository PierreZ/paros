//! `paros-sim` — the deterministic-simulation harness for paros: the moonpool
//! `Process` adapter, the client workloads, and the oracles.
//!
//! The node driver itself lives in `paros` (provider-generic, runs in production
//! *or* simulation). This crate adapts it to a moonpool [`Process`] under
//! `SimProviders` and adds the workloads + `Invariant`s — defined once so both
//! the native runner and the wasm demo reuse them. It is kept wasm-safe
//! (`default-features = false` drops moonpool's native providers + fork explorer).
//!
//! Stage 1 stands up the harness with **no protocol yet**: an empty cluster
//! advances logical time, acknowledges client proposals, and replays
//! bit-identically from a seed. [`run_seed`] is the single entry point both the
//! native runner and the browser demo call.

mod node;
mod oracle;
mod workload;

pub use node::NodeProcess;
pub use oracle::{Outcome, RunResult, Shot};

use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{SimulationBuilder, WorkloadCount};

use crate::oracle::{ClientLivenessOracle, RecorderData, TimelineRecorder, build_result};
use crate::workload::ProposeClient;

// --- Tuning knobs ------------------------------------------------------------

/// Number of proposals the client sends.
pub(crate) const REQUESTS: u32 = 12;
/// Per-proposal client deadline, in simulated milliseconds.
pub(crate) const TIMEOUT_MS: u64 = 700;
/// Gap between proposals, in simulated milliseconds, so node ticks interleave.
pub(crate) const GAP_MS: u64 = 20;
/// Number of paros nodes in the cluster.
pub(crate) const CLUSTER_SIZE: usize = 3;

/// Run one deterministic seed of the Stage-1 simulation and return its timeline.
/// The same seed always produces the same [`RunResult`].
#[must_use]
pub fn run_seed(seed: u64) -> RunResult {
    let data = Arc::new(Mutex::new(RecorderData::default()));
    let _report = SimulationBuilder::new()
        .processes(ProcessCount::Fixed(CLUSTER_SIZE), || Box::new(NodeProcess))
        .workloads(WorkloadCount::Fixed(1), |_| Box::new(ProposeClient))
        .invariant(TimelineRecorder::new(data.clone()))
        .invariant(ClientLivenessOracle)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run();

    let data = data.lock().unwrap_or_else(PoisonError::into_inner);
    build_result(seed, &data)
}

/// Run one seed and serialize the [`RunResult`] to JSON. Serializing a plain data
/// struct cannot fail, but on the off chance it does the error is returned as a
/// small JSON object instead of panicking.
#[must_use]
pub fn run_seed_json(seed: u64) -> String {
    serde_json::to_string(&run_seed(seed))
        .unwrap_or_else(|e| format!("{{\"error\":\"serialize failed: {e}\"}}"))
}
