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
//!   a 3–6 node acceptor pool plus a 0–3 matchmaker pool, each its own
//!   moonpool process group with its own per-seed count, under swarm network
//!   turbulence, crash/restart attrition scoped per group, buggified provider
//!   knobs, the driver's BUGGIFY hooks and the disk's fault sites, driven by
//!   the Chain-of-Blocks workload;
//! - the **corpus** ([`corpus_hunt`], [`run_corpus_mask`], …): scripted,
//!   analytically-judged recovery cases on a fixed three-node cluster.
//!
//! [`Process`]: moonpool_sim::Process

mod audit;
mod chain;
mod chain_workload;
mod corpus;
mod hooks;
mod lifecycle;
mod process;
mod roles;
mod shape;
mod world;

pub use moonpool_sim::{AssertKind, SimulationReport};

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    Attrition, AttritionScope, AttritionVictims, Chaos, ChaosMode, ExplorationConfig,
    LinkLatencyConfig, LocalityConfig, NetworkFault, NetworkFaultMask, SimulationBuilder,
    WorkloadCount,
};

use crate::chain_workload::ChainWorkload;
use crate::lifecycle::ScriptedLifecycle;
use crate::process::{MatchmakerProcess, NodeProcess};
use crate::roles::{ACCEPTOR_GROUP, MATCHMAKER_GROUP};

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
///
/// The three durations are the chain workload's knobs (the corpus passes the
/// production defaults through [`default_client_channel_config`]).
pub(crate) fn client_channel_config(
    connection_timeout: Duration,
    keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
) -> moonpool_hyper::ChannelConfig {
    moonpool_hyper::ChannelConfig {
        connection_timeout,
        keep_alive: Some(moonpool_hyper::KeepAlive {
            interval: keep_alive_interval,
            timeout: keep_alive_timeout,
            while_idle: true,
        }),
        ..moonpool_hyper::ChannelConfig::default()
    }
}

/// [`client_channel_config`] at the production defaults.
pub(crate) fn default_client_channel_config() -> moonpool_hyper::ChannelConfig {
    client_channel_config(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
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

/// Per-seed **acceptor pool** draw (inclusive), resolved from the seeded RNG at
/// topology-build time so every seed replays its own shape. The pool is the
/// acceptor process group (`crate::roles::ACCEPTOR_GROUP`), ranked into
/// `NodeId`s in IP order.
///
/// Three is the smallest cluster that tolerates a failure; five (quorum 3) is
/// the shape whose accept quorums can avoid any two pinned nodes (the #88
/// stale-ballot window); four and six sit beside them as three and five with
/// an extra vote (six is the first even shape with a quorum of four).
/// Singletons and pairs are deliberately out: a pair loses quorum on every
/// kill and a singleton cannot lose one, so neither exercises a regime a 3–6
/// node cluster under attrition does not, and each needed its own special
/// cases in the checks.
pub(crate) const PROCESS_POOL_RANGE: std::ops::RangeInclusive<usize> = 3..=6;
/// Per-seed **matchmaker pool** draw (inclusive): the matchmaker process group
/// (`crate::roles::MATCHMAKER_GROUP`), drawn independently of the acceptor
/// pool. Zero is the plain Multi-Paxos deployment (AGENTS.md, *Plain
/// Multi-Paxos is first-class*): no matchmakers, no matchmaking phase, every
/// campaign straight to `Prepare`. One and three are the `2f + 1` sets for
/// `f ∈ {0, 1}`; two is a valid set whose quorum is both members — it
/// tolerates no matchmaker loss, which is exactly the shape under which a
/// matchmaker crash must cost a campaign and never safety. The pool is the
/// address book; the **bootstrap matchmaker set** (generation 0, #125) is
/// drawn from it per seed (`crate::shape::matchmaker_bootstrap_ranks`) and
/// may be a subset that leaves matchmaker spares for a
/// `ReconfigureMatchmakers` to pull in; four and five leave room for a
/// replacement after a matchmaker's registry is lost for good.
pub(crate) const MATCHMAKER_POOL_RANGE: std::ops::RangeInclusive<usize> = 0..=5;
/// Per-seed concurrent-client draw (half-open: 1–3 clients). Multi-client runs
/// are what give the linearizability checker conflicting concurrent histories
/// to reject; single-client runs keep the cheap sequential fast path. Each
/// client is its own identity (`ctx.client_id()`), so their histories merge
/// without aliasing.
pub(crate) const CLIENT_COUNT_RANGE: std::ops::Range<usize> = 1..4;
/// Adaptive-sweep plateau window: stop once coverage has been stable for this
/// many consecutive seeds (and every `sometimes`/`reachable` has fired).
///
/// **Never buggified**, like every `*_ITERATIONS` ceiling below: this is the
/// sweep's own stopping rule, so it decides *which seeds run* rather than what
/// happens inside one.
pub(crate) const PLATEAU_SEEDS: usize = 8;
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
/// The corpus keeps its "chaos window" open for the whole scripted run: its
/// only fault injector is the scripted lifecycle, which must be able to crash
/// and restart nodes at every phase of the script.
const CORPUS_CHAOS: Duration = Duration::from_mins(10);

/// The main campaign's chaos surfaces: swarm network turbulence, one
/// crash/restart attrition regime **per process group**, and buggified
/// provider knobs — one combined axis.
///
/// Moonpool re-samples each attrition base per seed under `ChaosMode::Swarm`
/// (about half the seeds run a regime with no attrition, and the restart
/// window is rescaled to 50–200% of the range below), so the values here are
/// a base, not a fixed shape. The two regimes are independent: the acceptor
/// pool's `max_dead` budget is spent only by dead acceptors and the
/// matchmakers' only by dead matchmakers, so a killed matchmaker never keeps
/// the cluster's own quorum whole by proxy, and both roles can be down at
/// once. `prob_wipe = 0` **stays** zero: moonpool's `CrashAndWipe`
/// wipes its own storage provider, which paros does not use (the fake disk is
/// the `StorageWorld`), so the amnesia fault is the world's own coin, drawn at
/// a restart in `crate::process` (#124) and answered by replacement through
/// reconfiguration, never by a rejoin. The recovery window is
/// deliberately wide: a node kept down that long while the cluster keeps
/// committing and truncating comes back below every peer's compaction floor,
/// where only snapshot transfer can heal it.
fn chaos_surfaces() -> [Chaos; 4] {
    let regime = |victims: AttritionVictims| Attrition {
        max_dead: 1,
        prob_graceful: 0.0,
        prob_crash: 1.0,
        prob_wipe: 0.0,
        recovery_delay_ms: Some(1_200..2_500),
        grace_period_ms: None,
        scope: AttritionScope::PerProcess,
        victims,
    };
    [
        Chaos::Network(ChaosMode::Swarm),
        Chaos::Attrition {
            config: regime(AttritionVictims::group(ACCEPTOR_GROUP)),
            mode: ChaosMode::Swarm,
        },
        Chaos::Attrition {
            config: regime(AttritionVictims::group(MATCHMAKER_GROUP)),
            mode: ChaosMode::Swarm,
        },
        Chaos::BuggifyKnobs,
    ]
}

/// Fresh main-campaign builder. Keeping all state behind process/workload
/// factories is what makes fork-free exploration and recipe replay trustworthy.
///
/// `BitFlip` is masked off: wire integrity is the transport's job — TCP's own
/// checks today, a TLS layer later — not paros's. A provider-level flip below
/// an intact transport models damage no deployed link delivers, and would
/// fabricate a *client observation* rather than cluster state (moonpool#183
/// terrain).
fn chain_builder(digest: Option<DigestSink>) -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .cluster(LocalityConfig::new(PROCESS_POOL_RANGE, 1, 1, 1), || {
            Box::new(NodeProcess::chaotic())
        })
        .processes(MATCHMAKER_POOL_RANGE, || {
            Box::new(MatchmakerProcess::chaotic())
        })
        .link_latency(LinkLatencyConfig::default())
        .workloads(WorkloadCount::Random(CLIENT_COUNT_RANGE), move |_| {
            Box::new(ChainWorkload::new(digest.clone()))
        })
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
/// coverage plateaus, capped at `max_iterations`). The cap is a parameter so the
/// caller owns the schedule: the sancov runner (`cargo xtask sim`) passes
/// [`COVERAGE_ITERATIONS`] and saturates on `CodeCoverage`; the nextest suite
/// never calls this (its smoke is [`chain_smoke`]). Returns the report so the
/// caller can assert no `assertion_violations` and inspect progress.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn explore(max_iterations: usize) -> SimulationReport {
    chain_builder(None)
        .enable_exploration(exploration_config(EXPLORATION_TIMELINES_PER_SEED))
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run()
}

/// Run one fresh Chain timeline without requiring coverage saturation. Used for
/// smoke and deterministic seed replay.
#[must_use]
#[tracing::instrument(level = "debug")]
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
#[tracing::instrument(level = "debug")]
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
#[tracing::instrument(level = "debug")]
pub fn chain_smoke(iterations: usize) -> SimulationReport {
    chain_builder(None).set_iterations(iterations).run()
}

/// Explore one known root seed. This is the focused recipe-discovery command;
/// the registered campaign still explores every adaptive root seed.
#[must_use]
#[tracing::instrument(level = "debug")]
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
#[tracing::instrument(level = "debug")]
pub fn run_storage_contract_suite() -> SimulationReport {
    SimulationBuilder::new()
        .processes(1, || Box::new(crate::process::IdleProcess))
        .workload_factory(|| Box::new(crate::process::ContractSuiteWorkload))
        .set_iterations(1)
        .run()
}

// --- the CTRL evaluation corpus ----------------------------------------------

/// The scripted corpus cluster, the one shape every corpus case is built
/// from: `nodes` scripted-lifecycle nodes bootstrapped on `bootstrap` of them
/// (`None` — the usual case — bootstraps on all), `matchmakers` scripted
/// matchmakers (zero on every case but the departed straggler, which needs a
/// prior configuration), and no swarm chaos at all — every fault is a
/// targeted injection from the workload. The caller adds its
/// `workload_factory`, iterations and seeds. See `crate::corpus`.
fn scripted_builder(
    nodes: usize,
    bootstrap: Option<usize>,
    matchmakers: usize,
) -> SimulationBuilder {
    let mut builder = SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(nodes, move || match bootstrap {
            Some(ranks) => Box::new(NodeProcess::scripted_with_bootstrap(ranks)),
            None => Box::new(NodeProcess::scripted()),
        });
    if matchmakers > 0 {
        builder = builder.processes(matchmakers, || Box::new(MatchmakerProcess::scripted()));
    }
    builder
        .fault_factory(|| Box::new(ScriptedLifecycle))
        .chaos_duration(CORPUS_CHAOS)
}

/// The E1 mask corpus builder (see `crate::corpus`). `non_vacuous` is where
/// the workload publishes whether its run judged its analytic outcome (see
/// [`corpus_mask_case`]); the hunt axes pass `None`.
fn corpus_builder(
    source: corpus::MaskSource,
    non_vacuous: Option<NonVacuousSink>,
) -> SimulationBuilder {
    scripted_builder(corpus::CORPUS_NODES, None, 0).workload_factory(move || {
        Box::new(corpus::E1MaskWorkload::new(source, non_vacuous.clone()))
    })
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
#[tracing::instrument(level = "debug")]
pub fn run_corpus_mask(mask: u16) -> SimulationReport {
    corpus_mask_case(mask).0
}

/// The same run, reporting whether it was **non-vacuous**: `true` means the
/// workload judged its analytically derived outcome (a recoverable mask
/// converged intact, or an unrecoverable one waited without fabricating);
/// `false` means a late write healed a masked record before the cluster died,
/// so the run was released unjudged and observed nothing (see
/// `E1MaskWorkload`). Its `sometimes` gates are recorded, never asserted, by
/// nextest, so a caller that enumerates masks must require a minimum number of
/// `true`s or the corpus passes on vacuous runs alone.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn corpus_mask_case(mask: u16) -> (SimulationReport, bool) {
    let sink: NonVacuousSink = Arc::new(Mutex::new(false));
    let report = corpus_builder(corpus::MaskSource::Fixed(mask), Some(sink.clone()))
        .set_iterations(1)
        .set_debug_seeds(vec![u64::from(mask)])
        .run();
    let non_vacuous = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    (report, non_vacuous)
}

/// Raw-volume E1 sampling: each seed draws its mask from the seeded RNG, so a
/// hunt densely samples the full 512-case space. Replay with
/// [`run_corpus_seed`].
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn corpus_hunt(iterations: usize) -> SimulationReport {
    corpus_builder(corpus::MaskSource::Seeded, None)
        .set_iterations(iterations)
        .run()
}

/// Replay one seeded E1 corpus case deterministically.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_corpus_seed(seed: u64) -> SimulationReport {
    corpus_builder(corpus::MaskSource::Seeded, None)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Run the bare-quorum lost-slot case (see `crate::corpus`): one slot decided
/// by a bare quorum, then every copy of it rotted — the `faulty, faulty, none`
/// Phase-1 tally that must WAIT, and the deterministic red target of CTRL
/// §5.1.1's mutation (b) (a sub-Q1 `none` count no-op-filling a chosen slot).
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_bare_quorum_case(seed: u64) -> SimulationReport {
    scripted_builder(corpus::CORPUS_NODES, None, 0)
        .workload_factory(|| Box::new(corpus::BareQuorumWorkload::new()))
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Where the departed-straggler case publishes whether its run genuinely
/// reached its injection (see [`departed_straggler_case`]). Shared by the
/// workload factory's clones.
pub(crate) type NonVacuousSink = Arc<Mutex<bool>>;

/// Run the departed-straggler case (see `crate::corpus`, #124): a four-node
/// pool bootstrapped on three, reconfigured onto the spare, then the only
/// clean copy of a slot left on the node the reconfiguration removed — CTRL
/// Case 3 across a configuration boundary. The cluster must WAIT while the
/// straggler is down (its leader resigning under `REPAIR_TIMEOUT_ELECTIONS`)
/// and recover the slot through the prior configuration once it returns.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_departed_straggler_case(seed: u64) -> SimulationReport {
    departed_straggler_case(seed).0
}

/// The same run, reporting whether it was **non-vacuous**: `false` means the
/// case was superseded before its injection (GC forgot the prior
/// configuration, or a late write healed the mask) and observed nothing. A
/// caller that enumerates seeds must require at least one `true`.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn departed_straggler_case(seed: u64) -> (SimulationReport, bool) {
    let sink: NonVacuousSink = Arc::new(Mutex::new(false));
    let workload_sink = sink.clone();
    let report = scripted_builder(corpus::DEPARTED_POOL, Some(corpus::DEPARTED_BOOTSTRAP), 1)
        .workload_factory(move || {
            Box::new(corpus::DepartedStragglerWorkload::new(Some(
                workload_sink.clone(),
            )))
        })
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run();
    let non_vacuous = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    (report, non_vacuous)
}

/// Run the §5.1.2 snapshot-lifecycle compound (see `crate::corpus`): log-only,
/// snapshotted, and snapshotted-and-truncated nodes in one scripted run,
/// reaching all four snapshot-recovery paths.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_snapshot_lifecycle_case(seed: u64) -> SimulationReport {
    scripted_builder(corpus::CORPUS_NODES, None, 0)
        .workload_factory(|| Box::new(corpus::SnapshotLifecycleWorkload::new()))
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// The per-chunk mask corpus builder (see `crate::corpus`).
fn chunk_corpus_builder(
    source: corpus::ChunkMaskSource,
    rot_live_node0: bool,
) -> SimulationBuilder {
    scripted_builder(corpus::CORPUS_NODES, None, 0)
        .workload_factory(move || Box::new(corpus::ChunkMaskWorkload::new(source, rot_live_node0)))
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
#[tracing::instrument(level = "debug")]
pub fn run_chunk_mask(mask: u32, rot_live_node0: bool) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Fixed(mask), rot_live_node0)
        .set_iterations(1)
        .set_debug_seeds(vec![u64::from(mask)])
        .run()
}

/// Raw-volume chunk-mask sampling: each seed draws its mask from the seeded
/// RNG. Replay with [`run_chunk_corpus_seed`].
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chunk_corpus_hunt(iterations: usize) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Seeded, false)
        .set_iterations(iterations)
        .run()
}

/// Replay one seeded chunk-mask corpus case deterministically.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_chunk_corpus_seed(seed: u64) -> SimulationReport {
    chunk_corpus_builder(corpus::ChunkMaskSource::Seeded, false)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}
