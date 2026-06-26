//! `paros-sim` — the deterministic-simulation harness for paros: the moonpool
//! `Process` adapter, the client workloads, and the oracles.
//!
//! The node driver itself lives in `paros` (provider-generic, runs in production
//! *or* simulation). This crate adapts it to a moonpool [`Process`] under
//! `SimProviders` and adds the workloads + `Invariant`s — defined once so both
//! the native runner and the wasm demo reuse them. It is kept wasm-safe
//! (`default-features = false` drops moonpool's native providers + fork explorer).
//!
//! [`run_seed`] is the single entry point both the native runner and the browser
//! demo call: it runs one seeded multi-slot Paxos cluster under network chaos and
//! returns its timeline, replaying bit-identically from a seed. [`explore`] is the
//! DST sweep that asserts safety + progress across the seed space.

mod node;
mod oracle;
mod workload;

pub use node::NodeProcess;
pub use oracle::{ChosenShot, NodeStateShot, Outcome, ProtocolShot, RunResult, Shot};

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, SimulationBuilder, SimulationReport, WorkloadCount,
};

use crate::oracle::{
    ClientLivenessOracle, LeadershipOracle, NoGapsOracle, ProgressOracle, ProtocolData,
    ProtocolRecorder, RecorderData, SafetyOracle, TimelineRecorder, build_result,
};
use crate::workload::ProposeClient;

// --- Tuning knobs ------------------------------------------------------------

/// Number of proposals the client sends. Enough to exercise multi-slot streaming
/// under a stable leader without bloating the per-run trace.
pub(crate) const REQUESTS: u32 = 12;
/// Per-proposal client deadline, in simulated milliseconds. Wide enough to
/// survive a leader loss + re-election (election timeout is `[250, 500)` ms).
pub(crate) const TIMEOUT_MS: u64 = 1000;
/// Gap between proposals, in simulated milliseconds, so node ticks interleave.
pub(crate) const GAP_MS: u64 = 20;
/// Number of paros nodes in the cluster.
pub(crate) const CLUSTER_SIZE: usize = 3;
/// Adaptive-sweep plateau window: stop once coverage has been stable for this
/// many consecutive seeds (and every `sometimes`/`reachable` has fired).
pub(crate) const PLATEAU_SEEDS: usize = 64;
/// Cap on the full sweep used by the nextest safety+progress test (no sancov):
/// high enough for `AssertionCoverage` to saturate (all gates fire, then a plateau).
pub const SWEEP_ITERATIONS: usize = 5000;
/// Cap on the sancov coverage run (`cargo xtask sim`): bounded so the instrumented
/// sweep stays a few minutes instead of grinding `CodeCoverage` edges toward the cap.
pub const COVERAGE_ITERATIONS: usize = 64;
/// Simulated window over which chaos (network faults + attrition reboots) fires.
/// Wide enough to span a run's proposal phase so crashes land mid-protocol.
const CHAOS_DURATION: Duration = Duration::from_secs(30);

/// The chaos surfaces every run exercises: swarm network faults plus single-node
/// crash/restart attrition. `prob_wipe = 0`, so durable state (the per-node
/// `HardState` mirrored into the per-iteration `StateHandle`) survives a restart,
/// modelling a clean process crash with intact disk. Shared by [`run_seed`] and
/// [`explore`] so a failing seed replays identically.
fn chaos_surfaces() -> [Chaos; 2] {
    [
        Chaos::Network(ChaosMode::Swarm),
        Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 0.0,
                prob_crash: 1.0,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(50..200),
                grace_period_ms: None,
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Swarm,
        },
    ]
}

/// Run one deterministic seed and return its timeline. Network chaos (swarm) is
/// always on, so a run exercises the real protocol under faults; the same seed
/// always produces the same [`RunResult`].
///
/// # Panics
///
/// Panics if the safety oracle (or any other `always`-assertion) was violated on
/// this seed: a safety bug must blow up, in tests and in the wasm demo alike.
#[must_use]
pub fn run_seed(seed: u64) -> RunResult {
    let data = Arc::new(Mutex::new(RecorderData::default()));
    let proto = Arc::new(Mutex::new(ProtocolData::default()));
    let report = SimulationBuilder::new()
        .processes(ProcessCount::Fixed(CLUSTER_SIZE), || Box::new(NodeProcess))
        .workloads(WorkloadCount::Fixed(1), |_| Box::new(ProposeClient))
        .invariant(TimelineRecorder::new(data.clone()))
        .invariant(ProtocolRecorder::new(proto.clone()))
        .invariant(ClientLivenessOracle)
        .invariant(SafetyOracle)
        .invariant(NoGapsOracle)
        .invariant(LeadershipOracle)
        .invariant(ProgressOracle)
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run();

    assert!(
        report.assertion_violations.is_empty(),
        "safety violation on seed {seed}: {:?}",
        report.assertion_violations
    );

    let data = data.lock().unwrap_or_else(PoisonError::into_inner);
    let proto = proto.lock().unwrap_or_else(PoisonError::into_inner);
    build_result(seed, &data, &proto)
}

/// Run the DST bug-finding sweep: swarm network chaos + the safety oracle under
/// `UntilCoverageStable` (stop once every `sometimes`/`reachable` has fired and
/// coverage plateaus, capped at `max_iterations`). The cap is a parameter because
/// the two modes saturate differently: the nextest test passes [`SWEEP_ITERATIONS`]
/// (`AssertionCoverage`), the sancov runner passes [`COVERAGE_ITERATIONS`]
/// (`CodeCoverage`). Returns the report so the caller can assert no
/// `assertion_violations` and inspect progress.
#[must_use]
pub fn explore(max_iterations: usize) -> SimulationReport {
    SimulationBuilder::new()
        .processes(ProcessCount::Fixed(CLUSTER_SIZE), || Box::new(NodeProcess))
        .workloads(WorkloadCount::Fixed(1), |_| Box::new(ProposeClient))
        .invariant(ClientLivenessOracle)
        .invariant(SafetyOracle)
        .invariant(NoGapsOracle)
        .invariant(LeadershipOracle)
        .invariant(ProgressOracle)
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run()
}

/// Run one seed and serialize the [`RunResult`] to JSON. Serializing a plain data
/// struct cannot fail, but on the off chance it does the error is returned as a
/// small JSON object instead of panicking.
#[must_use]
pub fn run_seed_json(seed: u64) -> String {
    serde_json::to_string(&run_seed(seed))
        .unwrap_or_else(|e| format!("{{\"error\":\"serialize failed: {e}\"}}"))
}
