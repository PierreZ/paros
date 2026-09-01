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
//! demo call: it runs one seeded multi-slot Paxos cluster under network/attrition/driver chaos and
//! returns its timeline, replaying bit-identically from a seed. [`explore`] is the
//! DST sweep that asserts safety + progress across the seed space.

mod audit;
mod chain;
mod chain_workload;
mod choreography;
mod corpus;
mod node;
mod oracle;
mod protocol_bounds;
mod workload;

pub use moonpool_sim::{AssertKind, SimulationReport};
pub use node::NodeProcess;
pub use oracle::{ChosenShot, NodeStateShot, Outcome, ProtocolShot, RunResult, Shot};

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, LinkLatencyConfig, LocalityConfig, NetworkFault,
    NetworkFaultMask, SimulationBuilder, WorkloadCount,
};

use crate::chain_workload::ChainWorkload;
use crate::choreography::SnapshotRecoveryWorkload;
use crate::node::{BudgetOffNodeProcess, QuietNodeProcess};
use crate::oracle::{
    ChainAgreement, ProtocolData, ProtocolRecorder, RecorderData, RecoveryData, RecoveryRecorder,
    TimelineRecorder, build_result,
};
use crate::protocol_bounds::{ProtocolBoundsIdleProcess, ProtocolBoundsWorkload};
use crate::workload::ProposeClient;

/// Client-side gRPC channel config for the sim workloads. Mirrors the driver's
/// peer channels: h2 PING keep-alive so a connection left half-open by a node
/// restart is detected and replaced deterministically instead of swallowing
/// requests forever. Without it, a workload probe channel established before an
/// attrition restart can stay dead for the entire recovery tail (the seed
/// 6442591786636745658 convergence false-negative: every node had applied and
/// agreed by t=12.7s, and the probe to one restarted node then timed out for 74
/// simulated seconds).
pub(crate) fn client_channel_config() -> moonpool_hyper::ChannelConfig {
    moonpool_hyper::ChannelConfig {
        connection_timeout: Duration::from_secs(1),
        keep_alive: Some(moonpool_hyper::KeepAlive {
            interval: Duration::from_secs(2),
            timeout: Duration::from_secs(1),
            while_idle: false,
        }),
        ..moonpool_hyper::ChannelConfig::default()
    }
}

#[cfg(feature = "native")]
use moonpool_sim::ExplorationConfig;

#[cfg(feature = "native")]
fn exploration_config(max_runs_per_seed: u64) -> ExplorationConfig {
    ExplorationConfig {
        workers: 0,
        max_runs_per_seed,
        branching_factor: 4,
        max_frontier: 256,
        max_recipe_len: 64,
    }
}

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
/// returns (and thereby triggers the harness shutdown). This is
/// [`ProposeClient`]'s half of the recovery tail drawn in [`CHAOS_DURATION_MS`]:
/// every fault source — paros' own and, since Moonpool `43304d8`, the
/// simulator's network/storage families and the partitions in force — has
/// stopped by now, while the damage they did has not been undone, so the
/// cluster spends this window recovering on live nodes (the leader keeps
/// heartbeating, a lagging follower runs commit-replay catch-up, a below-floor
/// one takes a snapshot). The final convergence check runs after the tail, not
/// at the cutoff. Eight seconds covers issue #86's buggified seed 0, where a
/// delayed leadership turnover made the final follower catch up 6.05 seconds
/// into the tail.
///
/// **Never buggified.** It is the convergence budget the end-of-run assertion
/// is judged against, not a shape the run takes: drawing it short would fail a
/// cluster that was converging, and drawing it long would only buy wall clock.
/// Oracle thresholds set the verdict; knobs explore the state (AGENTS.md,
/// prong 2).
pub(crate) const SETTLE_MS: u64 = 8_000;
/// Per-seed cluster-size draw (inclusive), AGENTS.md prong 2: the cluster size
/// is workload-buggified config, resolved from the seeded RNG at topology-build
/// time so every seed replays its own shape. The full #61 set: n=1 and n=2 are
/// the quorum edge cases (a singleton decides alone; a pair needs both nodes,
/// so any attrition freezes it until recovery), and n=5 (quorum 3) is the
/// shape issue #88's stale-ballot scenario needs — at n=3 the two nodes pinned
/// above the stale ballot always intersect the accept quorum, so only n>=5
/// leaves a full quorum below the minted promise. Per-regime sometimes-gates
/// in the audit prove the sweep actually visits the edges.
pub(crate) const CLUSTER_SIZE_RANGE: std::ops::RangeInclusive<usize> = 1..=5;
/// Per-seed concurrent-client draw (half-open: 1–3 clients). Multi-client runs
/// are what give the real linearizability checker (#60) conflicting concurrent
/// histories to reject; single-client runs keep the cheap sequential fast path.
pub(crate) const CLIENT_COUNT_RANGE: std::ops::Range<usize> = 1..4;
/// Adaptive-sweep plateau window: stop once coverage has been stable for this
/// many consecutive seeds (and every `sometimes`/`reachable` has fired).
///
/// **Never buggified**, like every `*_ITERATIONS` ceiling below: this is the
/// sweep's own stopping rule, so it decides *which seeds run* rather than what
/// happens inside one. A knob here would randomize the schedule the guided
/// search depends on, not the run under test.
pub(crate) const PLATEAU_SEEDS: usize = 8;
/// Cap on the full coverage-guided sweep. The **sancov runner** (`cargo xtask
/// sim`) drives this so `AssertionCoverage`/`CodeCoverage` can saturate; the
/// nextest tests deliberately do *not* (they use [`SMOKE_ITERATIONS`]), so the
/// heavy, coverage-instrumented sweep stays in xtask where it belongs.
pub const SWEEP_ITERATIONS: usize = 5000;
/// Cap on the fast smoke sweep the nextest suite runs: a handful of random seeds
/// through the safety oracles, enough to catch an obvious regression quickly.
/// Saturation/coverage is **not** asserted here (that is `cargo xtask sim`'s job).
pub const SMOKE_ITERATIONS: usize = 50;
/// Cap on the sancov coverage run (`cargo xtask sim`). Re-armed provider timing
/// needs headroom to reach the rare gates before the eight-root quiet window;
/// the adaptive sweep still stops as soon as it saturates.
///
/// Raised from 256 when swarm network turbulence folded into this campaign
/// (the separate network axis, which carried its own 512-seed ceiling, is
/// gone): partitions, clogs and random closes widen the timing surface the
/// guided schedule has to cover before it can plateau. Raised again to 1024
/// when the per-kind peer mailbox landed: the sancov sweep saturated in 207
/// seeds on the mpsc mailbox and in 478 on the per-kind one, too close to a
/// 512 ceiling for comfort. Like every other ceiling here it is a schedule
/// parameter, not a safety margin — a saturating run still stops early, so
/// the raise costs nothing in wall clock.
pub const COVERAGE_ITERATIONS: usize = 1024;
/// Small cap for the dedicated graceful lifecycle choreography. Its workload
/// forces one ordered scenario per root, so it needs plateau headroom rather
/// than the broad seed volume of the main axis.
pub const SNAPSHOT_RECOVERY_COVERAGE_ITERATIONS: usize = 32;
/// Tiny cap for the deterministic protocol-bounds choreography. Every root
/// drives the complete three-page suffix and 64/64/2 Ready sequence.
pub const PROTOCOL_BOUNDS_COVERAGE_ITERATIONS: usize = 8;
/// Cap for the budget-off (WAITED-leg) exploration axis registered in the CI
/// campaign. Its `GateScope::BudgetOff` pair — a repair from a surviving clean
/// copy AND a correct WAIT at an unrecoverable committed item — must both
/// fire; the adaptive sweep stops as soon as coverage plateaus.
///
/// Raised from 128 when cooperative leader handoff added its instrumented
/// surface to `paros-core`/`paros`, which starved this axis' WAITED gate.
///
/// **This value is not a safety margin — it is a schedule parameter.** It is
/// passed as `max_iterations` to `until_coverage_stable`, which feeds the
/// guided seed schedule, so changing it changes *which* seeds are drawn rather
/// than only how many are allowed. Measured on one build, standalone
/// (`sim-paros-hunt budget-off-coverage`, cold coverage map): a ceiling of 128
/// saturates in 53 seeds, a ceiling of 384 in 95. Inside the campaign — where
/// the sancov map is **per process** and this axis runs fifth, so guidance
/// reaches it already warm — 128 exhausted the budget without ever firing the
/// WAITED gate, while 384 saturates in 75 (104 on CI's build).
///
/// So the honest reading is: the new instrumented surface shifted the guided
/// schedule off the corruption-deep seeds the WAITED leg needs, and this value
/// shifts it back. It was chosen empirically, and it will need choosing again
/// the next time the instrumented surface moves — the number carries no margin
/// you can reason about in advance. The plateau contract ([`PLATEAU_SEEDS`]) is
/// unchanged, and a saturating run still stops early, so the raise costs
/// nothing in wall clock. (Same remedy, and the same empirical character, as
/// the #102 bump.)
///
/// Raised again to 1024 when the per-kind peer mailbox moved the instrumented
/// surface once more: on CI's build the WAITED gate never fired inside 384
/// guided seeds (a local sancov run saturated in 56 — the schedule is
/// build-dependent, which is the empirical character above). A more reliable
/// catch-up delivery also makes a bare-quorum commit rarer, and the WAITED
/// leg needs exactly that shape, so the leg is genuinely rarer per seed than
/// it was.
pub const BUDGET_OFF_COVERAGE_ITERATIONS: usize = 1024;
/// Seeded-mask volume for the #113 E1 evaluation corpus in the CI campaign.
/// Scripted and fast per seed; enough draws from the 512-mask space that both
/// corpus gates (recoverable-converges and unrecoverable-waits) fire.
pub const CORPUS_CI_ITERATIONS: usize = 64;
/// Seeded-mask volume for the #101 per-chunk corpus in the CI campaign.
pub const CHUNK_CORPUS_CI_ITERATIONS: usize = 32;
/// Maximum root-plus-continuation timelines explored for each adaptive seed.
/// Eight is enough to drive real branches while keeping the sancov gate suitable
/// for CI; Moonpool stops earlier when a root discovers no new frontier.
pub const EXPLORATION_TIMELINES_PER_SEED: u64 = 8;

/// Simulated window (ms) over which chaos (network faults + attrition reboots +
/// the paros-side driver/storage perturbations) fires — wide enough to span the
/// proposal phase so faults land mid-protocol (creating the follower holes
/// convergence must heal), and ending well *before* the workloads do, so what
/// follows is a recovery tail.
///
/// The run's shape, and the one place convergence is judged:
///
/// ```text
/// t = 0 .. CHAOS_DURATION_MS      workload + chaos (network, attrition, BUGGIFY)
/// t = CHAOS_DURATION_MS           chaos_duration expires
///                                   → Moonpool enters recovery mode:
///                                       no new simulator faults,
///                                       partitions in force are healed,
///                                       persistent damage is kept,
///                                       replicas stay alive
///                                   → paros stops its own driver-hook and
///                                     storage-fault injection at the same cutoff
/// t = CHAOS_DURATION_MS ..        the quiet tail: leader election, `Accept`
///     + recovery budget             re-sends, gap fill, catch-up, snapshot
///                                   transfer, chunk repair — real protocol
///                                   recovery, on live nodes
/// end of the tail                 convergence + the audit/oracle checks
/// ```
///
/// The tail is the client workloads' own lifetime, not a separate framework:
/// [`SETTLE_MS`] for [`workload::ProposeClient`] and the buggified
/// `recovery_budget_ms` (45–90 s, default 60 s) for
/// [`chain_workload::ChainWorkload`]. Both are an order of magnitude longer
/// than this window. Convergence is asserted only at the end of that tail, and
/// the audit's quiescence gate additionally waits out a grace window past this
/// cutoff, so legitimate lag while chaos is active — or immediately after it —
/// is never mistaken for a stall.
pub(crate) const CHAOS_DURATION_MS: u64 = 4_000;
/// Simulated window over which chaos fires (see [`CHAOS_DURATION_MS`]).
const CHAOS_DURATION: Duration = Duration::from_millis(CHAOS_DURATION_MS);

/// The main campaign's chaos surfaces: swarm network turbulence, single-node
/// crash/restart attrition, and buggified provider knobs — one combined axis.
///
/// Network chaos used to live on its own safety-only axis because Moonpool's
/// environmental faults outlived `chaos_duration`, which made the quiet tail
/// unclaimable and the liveness/convergence gates unassertable. Moonpool
/// `43304d8` ends chaos properly: at the cutoff the runner enters recovery
/// mode, which stops every configuration-driven network/storage/block fault
/// family and heals the partitions in force, while leaving *persistent* damage
/// (closed connections, degraded pair latency, accumulated clock skew, killed
/// processes, rotted records) exactly as chaos left it. The tail after the
/// cutoff is therefore a real protocol-recovery window, so network turbulence
/// belongs on the main campaign with everything else.
///
/// `prob_wipe = 0`, so durable state (the per-node
/// records in the per-iteration `StorageWorld`) survives a restart, modelling a
/// clean process crash with intact disk (a **wiped** disk, which loses the
/// promise, is the amnesia case deferred to a later stage). The recovery window
/// is deliberately *wide* (`1_200..2_500` ms): a node kept down that long while the
/// cluster keeps committing and truncating (leader-driven, per
/// [`crate::workload`]) comes back **below every peer's compaction floor**, so
/// commit-replay catch-up can no longer heal it and only snapshot transfer can.
/// That is the scenario the [`oracle::ConvergenceOracle`] now demands convergence
/// for. Shared by [`run_seed`] and [`explore`] so a failing seed replays
/// identically.
fn chaos_surfaces() -> [Chaos; 3] {
    [
        Chaos::Network(ChaosMode::Swarm),
        Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 0.0,
                prob_crash: 1.0,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(1_200..2_500),
                grace_period_ms: None,
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Swarm,
        },
        Chaos::BuggifyKnobs,
    ]
}

/// Fresh main-campaign builder. Keeping all state behind process/workload
/// factories is what makes fork-free exploration and recipe replay trustworthy.
///
/// `BuggifiedDelay` stays enabled: the pinned moonpool gates sleep inflation to
/// `chaos_duration`, so setup and the quiet recovery tail remain fault-free.
/// `BitFlip` stays masked off for the reason every axis masks it (moonpool#183
/// terrain): the un-checksummed public replies carry no per-message integrity
/// protection, so a provider-level flip fabricates a *client observation*
/// rather than cluster state.
fn chain_cluster_builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .cluster(LocalityConfig::new(CLUSTER_SIZE_RANGE, 1, 1, 1), || {
            Box::new(NodeProcess)
        })
        .link_latency(LinkLatencyConfig::default())
}

fn chain_logic_builder() -> SimulationBuilder {
    chain_cluster_builder()
        .workload_factory(|| Box::new(ChainWorkload::default()))
        .invariant(ChainAgreement::new())
}

fn chain_builder() -> SimulationBuilder {
    chain_logic_builder()
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
        .swarm_operations()
}

/// Fixed-shape lifecycle axis: one graceful Moonpool reboot, no network or
/// storage chaos, no operation swarm, and no buggified provider knobs.
fn snapshot_recovery_builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .processes(3, || Box::new(QuietNodeProcess))
        .workload_factory(|| Box::new(SnapshotRecoveryWorkload::default()))
        // Reuse ChainAgreement's continuously pumped application-safety checks,
        // but not the main campaign's unrelated driver-hook/liveness gates. The
        // choreography workload owns the stronger ordered lifecycle gate.
        .invariant(ChainAgreement::safety_only())
        .enable_chaos([Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 1.0,
                prob_crash: 0.0,
                prob_wipe: 0.0,
                // The victim's downtime is the choreography's whole stage: the
                // survivors must commit twelve proposals (a budget of up to
                // 20s), get a Truncate decided, AND both apply it before this
                // clock restarts the victim. 45s keeps real slack past the
                // survivor budget — at 30s an explorer-perturbed timeline
                // could race the restart past the survivors' compaction apply
                // ("both survivors compact past the victim before restart",
                // 4/33 timelines red on one root), a scenario-establishment
                // failure, not a protocol one.
                recovery_delay_ms: Some(45_000..45_001),
                grace_period_ms: Some(1..2),
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Random,
        }])
        .chaos_duration(Duration::from_secs(6))
}

fn protocol_bounds_builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .processes(1, || Box::new(ProtocolBoundsIdleProcess))
        .workload_factory(|| Box::new(ProtocolBoundsWorkload))
}

/// Run the shared `NodeStorage` behavioral contract suite (issue #21 item F)
/// against the simulation's world-backed storage, inside one quiet iteration.
/// `MemStorage` runs the identical suite as a `paros` unit test; together they
/// keep the fake and the trait contract from drifting apart.
#[must_use]
pub fn run_storage_contract_suite() -> SimulationReport {
    SimulationBuilder::new()
        .processes(1, || Box::new(ProtocolBoundsIdleProcess))
        .workload_factory(|| Box::new(crate::node::ContractSuiteWorkload))
        .set_iterations(1)
        .run()
}

/// Run one deterministic seed and return its timeline. The main campaign's
/// combined surfaces are active — swarm network turbulence, clean-crash
/// attrition, and the driver's buggified decisions — and the same seed always
/// produces the same [`RunResult`].
///
/// # Panics
///
/// Panics if an in-run invariant is violated or the nodes have not converged by
/// the end of the settle tail.
#[must_use]
pub fn run_seed(seed: u64) -> RunResult {
    let data = Arc::new(Mutex::new(RecorderData::default()));
    let proto = Arc::new(Mutex::new(ProtocolData::default()));
    let recovery = Arc::new(Mutex::new(RecoveryData::default()));
    let report = SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(ProcessCount::Range(CLUSTER_SIZE_RANGE), || {
            Box::new(NodeProcess)
        })
        .workloads(WorkloadCount::Random(CLIENT_COUNT_RANGE), |_| {
            Box::new(ProposeClient::default())
        })
        .invariant(TimelineRecorder::new(data.clone()))
        .invariant(ProtocolRecorder::new(proto.clone()))
        .invariant(RecoveryRecorder::new(recovery.clone()))
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

/// Run the DST bug-finding sweep: regional latency, swarm network turbulence,
/// attrition, driver hooks, operation swarm, and the safety/recovery oracles under
/// `UntilCoverageStable` (stop once every `sometimes`/`reachable` has fired and
/// coverage plateaus, capped at `max_iterations`). The cap is a parameter because
/// the two modes saturate differently: the nextest test passes [`SWEEP_ITERATIONS`]
/// (`AssertionCoverage`), the sancov runner passes [`COVERAGE_ITERATIONS`]
/// (`CodeCoverage`). Returns the report so the caller can assert no
/// `assertion_violations` and inspect progress.
#[must_use]
pub fn explore(max_iterations: usize) -> SimulationReport {
    let builder = chain_builder();
    #[cfg(feature = "native")]
    let builder = builder.enable_exploration(exploration_config(EXPLORATION_TIMELINES_PER_SEED));
    builder
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run()
}

/// Coverage-stable roots for the dedicated graceful kill → truncate → restart
/// → snapshot-install choreography.
#[must_use]
pub fn explore_snapshot_recovery(max_iterations: usize) -> SimulationReport {
    let builder = snapshot_recovery_builder();
    #[cfg(feature = "native")]
    let builder = builder.enable_exploration(exploration_config(EXPLORATION_TIMELINES_PER_SEED));
    builder.until_coverage_stable(4, max_iterations).run()
}

/// Coverage-stable deterministic choreography for Promise paging, bounded
/// leader recovery, Accepted fingerprints, and Nack round isolation.
#[must_use]
pub fn explore_protocol_bounds(max_iterations: usize) -> SimulationReport {
    protocol_bounds_builder()
        .until_coverage_stable(2, max_iterations)
        .run()
}

/// Raw iterations through the deterministic protocol-bounds choreography.
#[must_use]
pub fn protocol_bounds_hunt(iterations: usize) -> SimulationReport {
    protocol_bounds_builder().set_iterations(iterations).run()
}

/// Replay one deterministic protocol-bounds root seed.
#[must_use]
pub fn run_protocol_bounds_seed(seed: u64) -> SimulationReport {
    protocol_bounds_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Replay one dedicated graceful snapshot-recovery choreography seed.
#[must_use]
pub fn run_snapshot_recovery_seed(seed: u64) -> SimulationReport {
    snapshot_recovery_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// The **budget-off** campaign (issue #21, the WAITED leg): the main
/// campaign's chaos surfaces with the per-record corruption budget lifted, so
/// every copy of a committed item can rot in one run. The CTRL guarantee then
/// demands a *correct* wait: the safety oracles stay fully armed, the
/// wedge/convergence gates are excused only by the world's ground truth ("no
/// readable copy of this slot exists anywhere"), and the WAITED gate proves
/// the leg is genuinely exercised — a sweep cannot go green having only ever
/// recovered.
fn budget_off_builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .cluster(LocalityConfig::new(CLUSTER_SIZE_RANGE, 1, 1, 1), || {
            Box::new(BudgetOffNodeProcess)
        })
        .link_latency(LinkLatencyConfig::default())
        .workload_factory(|| Box::new(ChainWorkload::budget_off()))
        .invariant(ChainAgreement::safety_only())
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
        .swarm_operations()
}

/// Raw-seed budget-off sweep (no saturation gate): the WAITED-leg hunting and
/// evidence entry point.
#[must_use]
pub fn budget_off_hunt(iterations: usize) -> SimulationReport {
    budget_off_builder().set_iterations(iterations).run()
}

/// Coverage-stable budget-off roots: saturates the WAITED/recovered pair.
#[must_use]
pub fn explore_budget_off(max_iterations: usize) -> SimulationReport {
    budget_off_builder()
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run()
}

/// Replay one budget-off seed deterministically.
#[must_use]
pub fn run_budget_off_seed(seed: u64) -> SimulationReport {
    budget_off_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

// --- the #113 CTRL evaluation corpus -----------------------------------------

/// The corpus cluster: three scripted-lifecycle nodes, no swarm chaos (every
/// fault is a targeted injection from the workload), the application-safety
/// invariant continuously pumped. See `crate::corpus`.
fn corpus_builder(source: corpus::MaskSource) -> SimulationBuilder {
    SimulationBuilder::new()
        // Like every other axis: the un-checksummed public replies (inspect,
        // acks) have no per-message integrity protection, so a provider-level
        // bit flip fabricates a *client observation*, not cluster state
        // (moonpool#183 terrain). The corpus judges real states only.
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(corpus::CORPUS_NODES, || {
            Box::new(crate::node::CorpusNodeProcess)
        })
        .workload_factory(move || Box::new(corpus::E1MaskWorkload::new(source)))
        .invariant(ChainAgreement::safety_only())
}

/// The canonical E1 mask cases the nextest corpus runner enumerates: the
/// exhaustive 2-slot × 3-node sub-grid (bits `node * 3 + slot`, slot ∈ {0, 1} —
/// 64 masks, every recoverable/unrecoverable boundary shape over two slots),
/// plus the full-grid corner cases: each slot lost on every node, each node
/// fully rotted, and the everything-lost mask.
#[must_use]
pub fn corpus_canonical_masks() -> Vec<u16> {
    let mut masks: Vec<u16> = Vec::new();
    // Exhaustive over slots 0 and 1 on all three nodes (slot-2 bits clear):
    // per node, bits {0, 1} of its 3-bit group.
    for low in 0_u16..64 {
        let mut mask = 0_u16;
        for node in 0..3_u16 {
            mask |= (low >> (node * 2) & 0b11) << (node * 3);
        }
        masks.push(mask);
    }
    // Full-grid corners.
    for extra in [
        0b001_001_001, // slot 0 lost everywhere
        0b010_010_010, // slot 1 lost everywhere
        0b100_100_100, // slot 2 lost everywhere
        0b000_000_111, // node 0 fully rotted
        0b111_000_000, // node 2 fully rotted
        0b111_111_111, // everything lost
        0b011_101_110, // mixed: every slot down to exactly one clean copy
    ] {
        if !masks.contains(&extra) {
            masks.push(extra);
        }
    }
    masks
}

/// Run one explicit E1 mask case deterministically (seeded by the mask itself,
/// so a failing case names its own replay).
#[must_use]
pub fn run_corpus_mask(mask: u16) -> SimulationReport {
    corpus_builder(corpus::MaskSource::Fixed(mask))
        .set_iterations(1)
        .set_debug_seeds(vec![u64::from(mask)])
        .run()
}

/// Raw-volume E1 sampling: each seed draws its mask from the seeded RNG, so a
/// hunt densely samples the full 512-case space. Replay with
/// [`run_corpus_seed`].
#[must_use]
pub fn corpus_hunt(iterations: usize) -> SimulationReport {
    corpus_builder(corpus::MaskSource::Seeded)
        .set_iterations(iterations)
        .run()
}

/// Replay one seeded E1 corpus case deterministically.
#[must_use]
pub fn run_corpus_seed(seed: u64) -> SimulationReport {
    corpus_builder(corpus::MaskSource::Seeded)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Run the bare-quorum lost-slot case (see `crate::corpus`): one slot decided
/// by a bare quorum, then every copy of it rotted — the `faulty, faulty, none`
/// Phase-1 tally that must WAIT, and the deterministic red target of CTRL
/// §5.1.1's mutation (b) (a sub-Q1 `none` count no-op-filling a chosen slot).
#[must_use]
pub fn run_bare_quorum_case(seed: u64) -> SimulationReport {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(corpus::CORPUS_NODES, || {
            Box::new(crate::node::CorpusNodeProcess)
        })
        .workload_factory(|| Box::new(corpus::BareQuorumWorkload::new()))
        .invariant(ChainAgreement::safety_only())
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Run the §5.1.2 snapshot-lifecycle compound (see `crate::corpus`): log-only,
/// snapshotted, and snapshotted-and-truncated nodes in one scripted run,
/// reaching all four snapshot-recovery paths.
#[must_use]
pub fn run_snapshot_lifecycle_case(seed: u64) -> SimulationReport {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(corpus::CORPUS_NODES, || {
            Box::new(crate::node::CorpusNodeProcess)
        })
        .workload_factory(|| Box::new(corpus::SnapshotLifecycleWorkload::new()))
        .invariant(ChainAgreement::safety_only())
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// The #101 per-chunk mask corpus builder (see `crate::corpus`).
fn chunk_corpus_builder(
    source: corpus::ChunkMaskSource,
    rot_live_node0: bool,
) -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(corpus::CORPUS_NODES, || {
            Box::new(crate::node::CorpusNodeProcess)
        })
        .workload_factory(move || Box::new(corpus::ChunkMaskWorkload::new(source, rot_live_node0)))
        .invariant(ChainAgreement::safety_only())
}

/// The canonical #101 chunk-mask cases (bit index `node * 5 + chunk` over the
/// five-chunk decided-point blob): the no-rot sanity case, single-copy and
/// two-copy losses (repair from the survivors), a per-node cross pattern, a
/// whole node's point lost, a chunk lost everywhere (must stay faulty, never
/// fabricated), and everything lost.
#[must_use]
pub fn chunk_corpus_canonical_masks() -> Vec<u32> {
    vec![
        0,
        1 << 0,
        (1 << 0) | (1 << 5),
        (1 << 0) | (1 << 5) | (1 << 10),
        0b11111,
        (1 << 0) | (1 << 6) | (1 << 12),
        (1 << 2) | (1 << 7) | (1 << 12) | (1 << 3) | (1 << 9),
        0x7FFF,
    ]
}

/// Run one explicit #101 chunk-mask case deterministically (seeded by the
/// mask). `rot_live_node0` additionally rots node 0's live snapshot, driving
/// the point-restore / whole-blob race on top of the chunk repair.
#[must_use]
pub fn run_chunk_mask(mask: u32, rot_live_node0: bool) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Fixed(mask), rot_live_node0)
        .set_iterations(1)
        .set_debug_seeds(vec![u64::from(mask)])
        .run()
}

/// Raw-volume chunk-mask sampling: each seed draws its mask from the seeded
/// RNG. Replay with [`run_chunk_corpus_seed`].
#[must_use]
pub fn chunk_corpus_hunt(iterations: usize) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Seeded, false)
        .set_iterations(iterations)
        .run()
}

/// Replay one seeded chunk-mask corpus case deterministically.
#[must_use]
pub fn run_chunk_corpus_seed(seed: u64) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Seeded, false)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Run one fresh Chain timeline without requiring coverage saturation. Used for
/// smoke, planted-oracle proof, and deterministic seed replay.
#[must_use]
pub fn run_chain_seed(seed: u64) -> SimulationReport {
    chain_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// One no-chaos timeline for validating the workload/application boundary before
/// fault axes are layered on.
#[must_use]
pub fn run_chain_baseline_seed(seed: u64) -> SimulationReport {
    chain_logic_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Fast random-seed Chain smoke with no adaptive saturation or branch
/// exploration. This is the only Chain sweep used by nextest.
#[must_use]
pub fn chain_smoke(iterations: usize) -> SimulationReport {
    chain_builder().set_iterations(iterations).run()
}

/// Replay an exploration recipe from a newly constructed campaign builder.
#[cfg(feature = "native")]
#[must_use]
pub fn replay_chain(seed: u64, recipe: Vec<(u64, u64)>) -> SimulationReport {
    chain_builder().replay_timeline(seed, recipe).run()
}

/// Explore one known root seed. This is the focused recipe-discovery command;
/// the registered campaign still explores every adaptive root seed.
#[cfg(feature = "native")]
#[must_use]
pub fn explore_chain_seed(seed: u64, max_runs: u64) -> SimulationReport {
    chain_builder()
        .set_debug_seeds(vec![seed])
        .enable_exploration(exploration_config(max_runs))
        .until_coverage_stable(1, 1)
        .run()
}

/// Explore one known snapshot-recovery choreography root seed with the same
/// timeline branching the campaign uses — the replay command for failures that
/// live on explorer *continuation* timelines, which a bare
/// [`run_snapshot_recovery_seed`] root replay never reaches.
#[cfg(feature = "native")]
#[must_use]
pub fn explore_snapshot_recovery_seed(seed: u64, max_runs: u64) -> SimulationReport {
    snapshot_recovery_builder()
        .set_debug_seeds(vec![seed])
        .enable_exploration(exploration_config(max_runs))
        .until_coverage_stable(1, 1)
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
