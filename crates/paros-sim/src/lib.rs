//! `paros-sim` — the deterministic-simulation harness for paros: the moonpool
//! `Process` adapter, the client workload, the fault world, and the audit.
//!
//! The node driver itself lives in `paros` (provider-generic, runs in production
//! *or* simulation). This crate adapts it to a moonpool [`Process`] under
//! `SimProviders`, drives it with one randomized client workload, perturbs it
//! through the driver's hooks and a budgeted fake disk, and judges it from two
//! perspectives only: the client's own history and the audit's fold of every
//! driver transition.
//!
//! Two axes, one check:
//!
//! - the **main campaign** ([`explore`], [`chain_smoke`], [`run_chain_seed`]):
//!   a 3–5 node cluster under swarm network turbulence, crash/restart attrition,
//!   buggified provider knobs, the driver's BUGGIFY hooks and the disk's fault
//!   sites, driven by the Chain-of-Blocks workload;
//! - the **corpus** ([`corpus_hunt`], [`run_corpus_mask`], …): scripted,
//!   analytically-judged recovery cases on a fixed three-node cluster.
//!
//! [`Process`]: moonpool_sim::Process

mod audit;
mod chain;
mod chain_workload;
mod corpus;
mod node;
mod oracle;

pub use moonpool_sim::{AssertKind, SimulationReport};
pub use node::NodeProcess;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, ExplorationConfig, LinkLatencyConfig,
    LocalityConfig, NetworkFault, NetworkFaultMask, SimulationBuilder,
};

use crate::chain_workload::ChainWorkload;
use crate::oracle::ChainAgreement;

/// Client-side gRPC channel config for the sim workloads: h2 PING keep-alive so
/// a connection left half-open by a node restart is detected and replaced
/// deterministically instead of swallowing requests forever.
///
/// **`while_idle` must be `true` here.** A workload channel is idle by
/// construction — it sends one probe, waits for the answer, sleeps, repeats —
/// and each probe is raced against its request timeout and cancelled when that
/// fires. With `while_idle: false` an idle connection is left alone until it is
/// used again, so the only chance to ping is while a stream is open; but the
/// probe timeout is always shorter than the ping interval, so the stream is
/// always gone before a ping is due and the keep-alive can never fire.
pub(crate) fn client_channel_config() -> moonpool_hyper::ChannelConfig {
    moonpool_hyper::ChannelConfig {
        connection_timeout: Duration::from_secs(1),
        keep_alive: Some(moonpool_hyper::KeepAlive {
            interval: Duration::from_secs(2),
            timeout: Duration::from_secs(1),
            while_idle: true,
        }),
        ..moonpool_hyper::ChannelConfig::default()
    }
}

fn exploration_config(max_runs_per_seed: u64) -> ExplorationConfig {
    ExplorationConfig {
        workers: 0,
        max_runs_per_seed,
        branching_factor: 4,
        max_frontier: 256,
        max_recipe_len: 64,
    }
}

// --- Schedule parameters and oracle clocks ------------------------------------

/// Per-seed cluster-size draw (inclusive), resolved from the seeded RNG at
/// topology-build time so every seed replays its own shape. Three is the
/// smallest cluster that tolerates a failure; five (quorum 3) is the shape whose
/// accept quorums can avoid any two pinned nodes (the #88 stale-ballot window).
/// Four sits in between as three with an extra vote. Singletons and pairs are
/// deliberately out: a pair loses quorum on every kill and a singleton cannot
/// lose one, so neither exercises a regime a 3–5 node cluster under attrition
/// does not, and each needed its own special cases in the checks.
pub(crate) const CLUSTER_SIZE_RANGE: std::ops::RangeInclusive<usize> = 3..=5;
/// Adaptive-sweep plateau window: stop once coverage has been stable for this
/// many consecutive seeds (and every `sometimes`/`reachable` has fired).
///
/// **Never buggified**, like every `*_ITERATIONS` ceiling below: this is the
/// sweep's own stopping rule, so it decides *which seeds run* rather than what
/// happens inside one.
pub(crate) const PLATEAU_SEEDS: usize = 8;
/// Cap on the full coverage-guided sweep when driven by `AssertionCoverage`.
pub const SWEEP_ITERATIONS: usize = 5000;
/// Cap on the fast smoke sweep the nextest suite runs: a handful of random seeds
/// through the safety checks, enough to catch an obvious regression quickly.
/// Saturation is **not** asserted here (that is `cargo xtask sim`'s job).
pub const SMOKE_ITERATIONS: usize = 50;
/// Cap on the sancov coverage run (`cargo xtask sim`). A schedule parameter,
/// not a safety margin: a saturating run stops early.
pub const COVERAGE_ITERATIONS: usize = 1024;
/// Seeded-mask volume for the E1 evaluation corpus in the CI campaign.
pub const CORPUS_CI_ITERATIONS: usize = 64;
/// Seeded-mask volume for the per-chunk corpus in the CI campaign.
pub const CHUNK_CORPUS_CI_ITERATIONS: usize = 32;
/// Maximum root-plus-continuation timelines explored for each adaptive seed.
pub const EXPLORATION_TIMELINES_PER_SEED: u64 = 8;

/// Simulated window (ms) over which chaos fires — network faults, attrition
/// reboots, and the paros-side driver/storage perturbations. It ends well
/// before the workload does, so what follows is a recovery tail:
///
/// ```text
/// t = 0 .. CHAOS_DURATION_MS      workload + chaos (network, attrition, BUGGIFY)
/// t = CHAOS_DURATION_MS           chaos_duration expires
///                                   → Moonpool enters recovery mode:
///                                       no new simulator faults,
///                                       partitions in force are healed,
///                                       persistent damage is kept
///                                   → paros stops its own driver-hook and
///                                     storage-fault injection at the same cutoff
/// t = CHAOS_DURATION_MS ..        the quiet tail: election, `Accept` re-sends,
///     recovery budget               gap fill, catch-up, snapshot transfer,
///                                   chunk repair — real protocol recovery
/// end of the tail                 the client-side and audit-side checks
/// ```
///
/// The tail is the workload's own lifetime (its buggified `recovery_budget_ms`,
/// an order of magnitude longer than this window). Convergence is judged only
/// at the end of that tail. **Never buggified**: it is the clock the verdict is
/// measured against, not a shape the run takes.
pub(crate) const CHAOS_DURATION_MS: u64 = 4_000;
const CHAOS_DURATION: Duration = Duration::from_millis(CHAOS_DURATION_MS);

/// The main campaign's chaos surfaces: swarm network turbulence, single-node
/// crash/restart attrition, and buggified provider knobs — one combined axis.
///
/// Moonpool re-samples the attrition base per seed under `ChaosMode::Swarm`
/// (about half the seeds run with no attrition, and the restart window is
/// rescaled to 50–200% of the range below), so the values here are a base, not
/// a fixed shape. `prob_wipe = 0`: durable state survives a restart, modelling a
/// clean process crash with intact disk (a wiped disk loses the promise, which
/// is the amnesia case deferred to reconfiguration). The recovery window is
/// deliberately wide: a node kept down that long while the cluster keeps
/// committing and truncating comes back below every peer's compaction floor,
/// where only snapshot transfer can heal it.
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
/// `BitFlip` is masked off: the un-checksummed public replies carry no
/// per-message integrity protection, so a provider-level flip fabricates a
/// *client observation* rather than cluster state (moonpool#183 terrain).
fn chain_builder(digest: Option<DigestSink>) -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .cluster(LocalityConfig::new(CLUSTER_SIZE_RANGE, 1, 1, 1), || {
            Box::new(NodeProcess)
        })
        .link_latency(LinkLatencyConfig::default())
        .workload_factory(move || Box::new(ChainWorkload::new(digest.clone())))
        .invariant(ChainAgreement::new())
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
        .swarm_operations()
}

/// Where a run publishes its end-of-run audit digest (see
/// [`chain_seed_digest`]). Shared by the workload factory's clones.
pub(crate) type DigestSink = Arc<Mutex<Option<u64>>>;

/// Run the DST bug-finding sweep: regional latency, swarm network turbulence,
/// attrition, driver hooks, operation swarm, and the safety/recovery checks under
/// `UntilCoverageStable` (stop once every `sometimes`/`reachable` has fired and
/// coverage plateaus, capped at `max_iterations`). The cap is a parameter because
/// the two modes saturate differently: the nextest test passes [`SWEEP_ITERATIONS`]
/// (`AssertionCoverage`), the sancov runner passes [`COVERAGE_ITERATIONS`]
/// (`CodeCoverage`). Returns the report so the caller can assert no
/// `assertion_violations` and inspect progress.
#[must_use]
pub fn explore(max_iterations: usize) -> SimulationReport {
    chain_builder(None)
        .enable_exploration(exploration_config(EXPLORATION_TIMELINES_PER_SEED))
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run()
}

/// Run one fresh Chain timeline without requiring coverage saturation. Used for
/// smoke and deterministic seed replay.
#[must_use]
pub fn run_chain_seed(seed: u64) -> SimulationReport {
    chain_builder(None)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Run one seed and return the audit's end-of-run digest: a fold of the chosen
/// log, every node's applied prefix, and the leadership history. Two runs of the
/// same seed must return the same digest — the determinism proof.
///
/// # Panics
///
/// Panics if the run violated an assertion or produced no digest (the workload
/// never reached its `check()` phase).
#[must_use]
pub fn chain_seed_digest(seed: u64) -> u64 {
    let sink: DigestSink = Arc::new(Mutex::new(None));
    let report = chain_builder(Some(sink.clone()))
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run();
    assert!(
        report.assertion_violations.is_empty(),
        "safety violation on seed {seed}: {:?}",
        report.assertion_violations
    );
    let digest = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    digest.expect("the chain workload published its audit digest")
}

/// Fast random-seed Chain smoke with no adaptive saturation or branch
/// exploration. This is the only Chain sweep used by nextest.
#[must_use]
pub fn chain_smoke(iterations: usize) -> SimulationReport {
    chain_builder(None).set_iterations(iterations).run()
}

/// Replay an exploration recipe from a newly constructed campaign builder.
#[must_use]
pub fn replay_chain(seed: u64, recipe: Vec<(u64, u64)>) -> SimulationReport {
    chain_builder(None).replay_timeline(seed, recipe).run()
}

/// Explore one known root seed. This is the focused recipe-discovery command;
/// the registered campaign still explores every adaptive root seed.
#[must_use]
pub fn explore_chain_seed(seed: u64, max_runs: u64) -> SimulationReport {
    chain_builder(None)
        .set_debug_seeds(vec![seed])
        .enable_exploration(exploration_config(max_runs))
        .until_coverage_stable(1, 1)
        .run()
}

/// Run the shared `NodeStorage` behavioral contract suite against the
/// simulation's world-backed storage, inside one quiet iteration. `MemStorage`
/// runs the identical suite as a `paros` unit test; together they keep the fake
/// and the trait contract from drifting apart.
#[must_use]
pub fn run_storage_contract_suite() -> SimulationReport {
    SimulationBuilder::new()
        .processes(1, || Box::new(crate::node::IdleProcess))
        .workload_factory(|| Box::new(crate::node::ContractSuiteWorkload))
        .set_iterations(1)
        .run()
}

// --- the CTRL evaluation corpus ----------------------------------------------

/// The corpus cluster: three scripted-lifecycle nodes, no swarm chaos (every
/// fault is a targeted injection from the workload), the application-safety
/// invariant continuously pumped. See `crate::corpus`.
fn corpus_builder(source: corpus::MaskSource) -> SimulationBuilder {
    SimulationBuilder::new()
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
    for low in 0_u16..64 {
        let mut mask = 0_u16;
        for node in 0..3_u16 {
            mask |= (low >> (node * 2) & 0b11) << (node * 3);
        }
        masks.push(mask);
    }
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

/// The per-chunk mask corpus builder (see `crate::corpus`).
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

/// The canonical chunk-mask cases (bit index `node * 5 + chunk` over the
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

/// Run one explicit chunk-mask case deterministically (seeded by the mask).
/// `rot_live_node0` additionally rots node 0's live snapshot, driving the
/// point-restore / whole-blob race on top of the chunk repair.
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
