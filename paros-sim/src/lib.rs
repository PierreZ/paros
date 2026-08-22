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

mod nemesis;
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
    ClientLivenessOracle, ConvergenceOracle, LeadershipOracle, LinearizabilityOracle,
    NemesisOracle, NoGapsOracle, ProgressOracle, ProtocolData, ProtocolRecorder, RecorderData,
    RecoveryData, RecoveryOracle, RecoveryRecorder, SafetyOracle, SnapshotOracle, TimelineRecorder,
    TruncationOracle, build_result,
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
/// Quiescence window the client holds open *after* its last proposal before it
/// returns (and thereby triggers the harness shutdown). Chaos has ended by now,
/// so this is a quiet tail in which the leader keeps heartbeating and any lagging
/// follower runs commit-replay catch-up to converge. The [`oracle::ConvergenceOracle`]
/// asserts over exactly this tail; it must comfortably exceed a few heartbeat /
/// election-timeout intervals plus a catch-up round trip.
pub(crate) const SETTLE_MS: u64 = 5_000;
/// Number of paros nodes in the cluster.
pub(crate) const CLUSTER_SIZE: usize = 3;
/// Adaptive-sweep plateau window: stop once coverage has been stable for this
/// many consecutive seeds (and every `sometimes`/`reachable` has fired).
pub(crate) const PLATEAU_SEEDS: usize = 64;
/// Cap on the full coverage-guided sweep. The **sancov runner** (`cargo xtask
/// sim`) drives this so `AssertionCoverage`/`CodeCoverage` can saturate; the
/// nextest tests deliberately do *not* (they use [`SMOKE_ITERATIONS`]), so the
/// heavy, coverage-instrumented sweep stays in xtask where it belongs.
pub const SWEEP_ITERATIONS: usize = 5000;
/// Cap on the fast smoke sweep the nextest suite runs: a handful of random seeds
/// through the safety oracles, enough to catch an obvious regression quickly.
/// Saturation/coverage is **not** asserted here (that is `cargo xtask sim`'s job).
pub const SMOKE_ITERATIONS: usize = 50;
/// Cap on the sancov coverage run (`cargo xtask sim`): bounded so the instrumented
/// sweep stays a few minutes instead of grinding `CodeCoverage` edges toward the cap.
pub const COVERAGE_ITERATIONS: usize = 64;

/// Pinned seeds that exercise durability + convergence edge cases (crash/restart,
/// the persist/send seam crashes, and a follower left with a permanent decided-slot
/// hole), replayed on every CI run so a regression in the storage, recovery, or
/// catch-up path is caught immediately — not left to the adaptive sweep to
/// rediscover. Anchored by the seed on which the `buggify` seam crash first went
/// red (the `log_applied` gap the recovery path now fills); grows as new bugs are
/// found. Seed 5 is where the [`oracle::ConvergenceOracle`] first went red: a
/// follower that missed both the `Accept` and the `Commit` for a decided slot kept
/// a permanent hole until commit-replay catch-up landed. Seed
/// `18153519926117387038` is where the [`oracle::TruncationOracle`] scenario first
/// went red: a quorum that truncated a chosen slot answered a lagging candidate's
/// below-floor `Prepare` with an empty-looking `Promise`, so the blind candidate
/// won and re-proposed a different value into the already-chosen slot (two values
/// chosen for one slot), until the acceptor floor guards landed. Seed
/// `11316277997507784505` exercises **snapshot restore**: a node that fell behind
/// while the cluster kept committing and truncating (leader-driven) comes back
/// below a peer's compaction floor, and instead of stalling it recovers through
/// paros via an `InstallSnapshot` (the [`oracle::SnapshotOracle`] coverage gate
/// fires and the [`oracle::ConvergenceOracle`] — now with no below-floor exemption
/// — confirms it converges). Seed `286172402316494352` is where the
/// [`oracle::LinearizabilityOracle`] first went red: a *naive* leader read (serve
/// the local `chosen_index` whenever `role == Leader`, no confirmation round)
/// returned a watermark below a write the client had already seen acknowledged —
/// a stale leader belief served as a committed read — until the read-index
/// protocol (heartbeat-ack quorum round + the fresh-leader read floor) landed.
/// Each replays clean via [`run_seed`].
///
/// All of the above predate the moonpool deterministic-executor bump (rev
/// `f7a6d52`, #65): that change replaced tokio's FIFO task scheduling with
/// seeded-random scheduling, so a seed's *meaning* (the exact task interleaving
/// it drives) shifted. These seeds still replay clean (no safety oracle trips),
/// but they no longer reproduce the original bug interleavings described above
/// — they are historical markers of what each seed once caught, not live
/// reproductions of it.
pub const REGRESSION_SEEDS: &[u64] = &[
    99,
    42,
    7,
    12_345,
    5,
    18_153_519_926_117_387_038,
    11_316_277_997_507_784_505,
    286_172_402_316_494_352,
];
/// Simulated window (ms) over which chaos (network faults + attrition reboots)
/// fires — wide enough to span the proposal phase so crashes land mid-protocol
/// (creating the follower holes convergence must heal), but ending *before* the
/// client's [`SETTLE_MS`] tail so that tail is quiet. The [`oracle::ConvergenceOracle`]
/// only asserts once `sim_time_ms` is past this window, so it never trips on the
/// legitimate transient lag while chaos is still firing.
pub(crate) const CHAOS_DURATION_MS: u64 = 4_000;
/// Simulated window over which chaos fires (see [`CHAOS_DURATION_MS`]).
const CHAOS_DURATION: Duration = Duration::from_millis(CHAOS_DURATION_MS);

/// The chaos surfaces every run exercises: swarm network faults plus single-node
/// crash/restart attrition. `prob_wipe = 0`, so durable state (the per-node
/// records in the per-iteration `StorageWorld`) survives a restart, modelling a
/// clean process crash with intact disk (a **wiped** disk, which loses the
/// promise, is the amnesia case deferred to a later stage). The recovery window
/// is deliberately *wide* (`200..900` ms): a node kept down that long while the
/// cluster keeps committing and truncating (leader-driven, per
/// [`crate::workload`]) comes back **below every peer's compaction floor**, so
/// commit-replay catch-up can no longer heal it and only snapshot transfer can.
/// That is the scenario the [`oracle::ConvergenceOracle`] now demands convergence
/// for. Shared by [`run_seed`] and [`explore`] so a failing seed replays
/// identically.
fn chaos_surfaces() -> [Chaos; 2] {
    [
        Chaos::Network(ChaosMode::Swarm),
        Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 0.0,
                prob_crash: 1.0,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(200..900),
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
    let recovery = Arc::new(Mutex::new(RecoveryData::default()));
    let report = SimulationBuilder::new()
        .processes(ProcessCount::Fixed(CLUSTER_SIZE), || Box::new(NodeProcess))
        .workloads(WorkloadCount::Fixed(1), |_| Box::new(ProposeClient))
        .invariant(TimelineRecorder::new(data.clone()))
        .invariant(ProtocolRecorder::new(proto.clone()))
        .invariant(RecoveryRecorder::new(recovery.clone()))
        .invariant(ClientLivenessOracle)
        .invariant(SafetyOracle)
        .invariant(LinearizabilityOracle)
        .invariant(RecoveryOracle)
        .invariant(NoGapsOracle)
        .invariant(LeadershipOracle)
        .invariant(ProgressOracle)
        .invariant(ConvergenceOracle)
        .invariant(TruncationOracle)
        .invariant(SnapshotOracle)
        .invariant(NemesisOracle)
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
    let recovery = recovery.lock().unwrap_or_else(PoisonError::into_inner);
    build_result(seed, &data, &proto, &recovery)
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
        .invariant(LinearizabilityOracle)
        .invariant(RecoveryOracle)
        .invariant(NoGapsOracle)
        .invariant(LeadershipOracle)
        .invariant(ProgressOracle)
        // `ConvergenceOracle` is deliberately *not* in the adaptive sweep. The
        // sweep draws a fresh wall-clock base seed each run, and convergence is a
        // *liveness* property ("every live node eventually catches up"), not a hard
        // safety invariant: under the harshest interleavings a lagging node can take
        // many seconds to converge (slow leader-election stabilization after a crash
        // reverts a relaxed chosen-index), longer than any bounded settle window. As
        // an `assert_always` over random seeds that reads as a flaky failure. The
        // oracle instead runs on the *deterministic* [`run_seed`] path (the pinned
        // `REGRESSION_SEEDS`, incl. the seed on which it first went red), where the
        // red→green result is reproducible; the deterministic core unit test
        // `follower_fills_a_hole_via_commit_replay_catch_up` pins the mechanism.
        .invariant(TruncationOracle)
        .invariant(SnapshotOracle)
        .invariant(NemesisOracle)
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
