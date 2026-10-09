//! `paros-sim` — the deterministic-simulation harness for paros: the moonpool
//! `Process` adapter, the client workload, the fault world, and the audit.
//!
//! The node driver itself lives in `paros` (provider-generic, runs in production
//! *or* simulation). This crate adapts it to a moonpool [`Process`] under
//! `SimProviders`, drives it with one randomized client workload, perturbs it
//! through the driver's hooks and the ledgered storage faults on moonpool's
//! simulated disk, and judges it from two
//! perspectives only: the client's own history and the audit's fold of every
//! driver transition.
//!
//! One campaign ([`explore`], [`chain_smoke`], [`run_chain_seed`]): a 3–6
//! node acceptor pool plus a 0–5 matchmaker pool plus a 0–3 proxy leader
//! pool, each its own moonpool process group with its own per-seed count,
//! under swarm network turbulence, crash/restart attrition scoped per group,
//! buggified provider knobs, the driver's BUGGIFY hooks and the journal
//! stores' damage sites, driven by the Chain-of-Blocks workload. Every shape
//! is provoked through BUGGIFY and swarm, never scripted (#263).
//!
//! [`Process`]: moonpool_sim::Process

mod audit;
mod chain;
mod chain_workload;
mod client;
mod hooks;
mod lifecycle;
mod process;
mod roles;
mod shape;
mod state;
mod world;

pub use moonpool_sim::{AssertKind, SimulationReport};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    Attrition, AttritionScope, AttritionVictims, Chaos, ChaosMode, ExplorationConfig,
    LinkLatencyConfig, LocalityConfig, NetworkFault, NetworkFaultMask, SimulationBuilder,
    StorageFault, StorageFaultMask, WorkloadCount,
};

use crate::chain_workload::ChainWorkload;
use crate::lifecycle::ScriptedLifecycle;
use crate::process::{JoinerProcess, MatchmakerProcess, NodeProcess, ProxyProcess, ReplicaProcess};
use crate::roles::{ACCEPTOR_GROUP, MATCHMAKER_GROUP, PROXY_GROUP, REPLICA_GROUP};

/// An optional slot or watermark as a signed trace/detail value: `None`
/// (the empty prefix, nothing seen yet) is `-1`, and a value too large for
/// an `i64` saturates at `i64::MAX`.
pub(crate) fn signed_watermark(watermark: Option<u64>) -> i64 {
    watermark.map_or(-1_i64, |wm| i64::try_from(wm).unwrap_or(i64::MAX))
}

/// [`SimulationBuilder::run`](moonpool_sim::SimulationBuilder::run) for a
/// builder this crate assembled: every one registers its fault injectors with
/// a chaos window and never pairs an instance workload with exploration or
/// the determinism canary, so the runner cannot refuse it, and an `Err` is a
/// harness bug rather than a seed's outcome. That is why it panics instead of
/// using a moonpool assertion macro: it is a setup failure before any seed
/// ran, not a check on a run — do not copy it as a pattern for checks.
trait RunConfigured {
    fn run_configured(self) -> SimulationReport;
}

impl RunConfigured for SimulationBuilder {
    fn run_configured(self) -> SimulationReport {
        self.run()
            .unwrap_or_else(|error| panic!("a paros simulation builder is misconfigured: {error}"))
    }
}

fn exploration_config(max_runs_per_seed: u64, workers: usize) -> ExplorationConfig {
    ExplorationConfig {
        workers,
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
/// Per-seed **proxy leader pool** draw (inclusive, #142): the proxy process
/// group (`crate::roles::PROXY_GROUP`), drawn independently of the other two
/// pools. Zero is the plain deployment — `Config::proxy_count = 0`, every
/// Phase 2 colocated on the leader, the wire byte-for-byte today's. One proxy
/// is the deployment where a single dead proxy stalls every delegated slot
/// until the leader takes them back (the take-back's whole reason); two and
/// three spread `slot % proxy_count` so consecutive slots run on different
/// proxies and a handoff successor's re-delegation can land on a proxy other
/// than the one holding the round. The count is protocol data every node's
/// `Config` carries; which process answers to a `ProxyId` is the deployment
/// map's.
pub(crate) const PROXY_POOL_RANGE: std::ops::RangeInclusive<usize> = 0..=3;
/// Per-seed **replica tier** draw (inclusive, #144): the replica process
/// group (`crate::roles::REPLICA_GROUP`), drawn independently of the other
/// pools. Zero is the plain deployment — `Config::replica_count = 0`, the
/// learner traffic reaching the pool alone, the wire byte-for-byte today's.
/// One replica is the tier whose single member must heal itself from the
/// acceptors after every attrition kill; two make `reply_owner` alternate
/// and let one replica sit below the floor while the other stays current.
/// The count is protocol data every node's `Config` carries; which process
/// answers to a `ReplicaId` is the deployment map's.
pub(crate) const REPLICA_POOL_RANGE: std::ops::RangeInclusive<usize> = 0..=2;
/// Per-seed **joiner pool** draw (inclusive, #189): the joiner process group
/// (`crate::roles::JOINER_GROUP`), nodes outside the genesis pool that the
/// chain client registers, drains and retires through the node registry at
/// runtime. Zero is a seed without joiners; a joiner on a seed without system
/// journals idles. Two let one joiner race another's registration and a
/// created journal name both.
pub(crate) const JOINER_POOL_RANGE: std::ops::RangeInclusive<usize> = 0..=2;
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
/// [`CHAOS_DURATION_MS`] as a `Duration`: the cutoff every role's hooks and
/// fault coins share (`crate::process`).
pub(crate) const CHAOS_DURATION: Duration = Duration::from_millis(CHAOS_DURATION_MS);

/// The main campaign's chaos surfaces: swarm network turbulence, one
/// crash/restart attrition regime **per process group**, and buggified
/// provider knobs — one combined axis.
///
/// Moonpool re-samples each attrition base per seed under `ChaosMode::Swarm`
/// (about half the seeds run a regime with no attrition, and the restart
/// window is rescaled to 50–200% of the range below), so the values here are
/// a base, not a fixed shape. The four regimes are independent: the acceptor
/// pool's `max_dead` budget is spent only by dead acceptors, the
/// matchmakers' only by dead matchmakers, the proxies' only by dead proxies
/// and the replicas' only by dead replicas, so a killed matchmaker, proxy or
/// replica never keeps the cluster's own quorum whole by proxy, and every
/// role can be down at once. A killed replica (#144) comes back to a log the
/// acceptors kept deciding and truncating without it — the catch-up and the
/// below-floor snapshot install a replica exists to survive. A killed proxy
/// leader (#142) is the fault the leader's take-back exists for: every slot
/// delegated to it stalls until the leader runs it colocated, and a proxy
/// killed at the chaos cutoff stays down for the whole recovery tail.
/// `prob_wipe = 0` **stays** zero: moonpool's `CrashAndWipe` now reaches
/// the disk paros stores on, but it wipes a whole machine without asking the
/// copy budget, so it could lose a quorum's copies at once. The amnesia fault
/// is the storage ledger's own coin instead, drawn at a restart in
/// `crate::process` (#124) under the dead-node budget and the provisioning
/// ledger, and executed on the simulated disk (`world::wipe`) and answered by replacement through
/// reconfiguration, never by a rejoin. The recovery window is
/// deliberately wide: a node kept down that long while the cluster keeps
/// committing and truncating comes back below every peer's compaction floor,
/// where only snapshot transfer can heal it.
///
/// `Chaos::Storage` runs moonpool's disk faults under every journal store
/// (#176): the families a node's own store must survive alone (crash damage
/// to unsynced sectors: lost, latent and shorn; failed syncs; short
/// transfers; lost unsynced directory entries), swarm-masked per seed by
/// moonpool. A world-store seed has no files and is untouched. See
/// [`storage_fault_mask`] for what is masked.
fn chaos_surfaces() -> [Chaos; 8] {
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
        Chaos::Attrition {
            config: regime(AttritionVictims::group(PROXY_GROUP)),
            mode: ChaosMode::Swarm,
        },
        Chaos::Attrition {
            config: regime(AttritionVictims::group(REPLICA_GROUP)),
            mode: ChaosMode::Swarm,
        },
        crate::world::outage::regime(),
        Chaos::BuggifyKnobs,
        Chaos::Storage(ChaosMode::Swarm),
    ]
}

/// The storage families the campaign leaves out (architecture §5, #176).
/// Rot (corruption, EIO, misdirection) damages a record on whichever disks
/// it lands, and random rot eventually hits every copy of one vote, an
/// unwinnable run: it enters with replicated fault patterns, which keep a
/// quorum's copies clean. Phantom writes lose an acknowledged vote no quorum
/// survives; a failed disk needs a storage watchdog in the driver;
/// degradation episodes are a performance knob. Each has an issue to lift
/// its mask.
fn storage_fault_mask() -> StorageFaultMask {
    StorageFaultMask::all()
        .without(StorageFault::Corruption)
        .without(StorageFault::Eio)
        .without(StorageFault::Misdirect)
        .without(StorageFault::PhantomWrite)
        .without(StorageFault::Degradation)
        .without(StorageFault::DiskFailure)
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
        .storage_fault_mask(storage_fault_mask())
        .cluster(LocalityConfig::new(PROCESS_POOL_RANGE, 1, 1, 1), || {
            Box::new(NodeProcess::chaotic())
        })
        .processes(MATCHMAKER_POOL_RANGE, || {
            Box::new(MatchmakerProcess::chaotic())
        })
        .processes(PROXY_POOL_RANGE, || Box::new(ProxyProcess::chaotic()))
        .processes(REPLICA_POOL_RANGE, || Box::new(ReplicaProcess::chaotic()))
        .processes(JOINER_POOL_RANGE, || Box::new(JoinerProcess::chaotic()))
        .link_latency(LinkLatencyConfig::default())
        .workloads(WorkloadCount::Random(CLIENT_COUNT_RANGE), move |_| {
            Box::new(ChainWorkload::new(digest.clone()))
        })
        .enable_chaos(chaos_surfaces())
        .fault_factory(|| Box::new(ScriptedLifecycle))
        .fault_factory(|| Box::new(crate::world::outage::OutageLosses))
        .fault_factory(|| Box::new(crate::world::late_outage::LateOutage))
        .fault_factory(|| Box::new(crate::world::bare_outage::BareOutage))
        .chaos_duration(CHAOS_DURATION)
        .swarm_operations()
}

/// Where a run publishes its end-of-run audit digest (see
/// [`chain_seed_digest`]). Shared by the workload factory's clones.
pub(crate) type DigestSink = Arc<Mutex<Option<u64>>>;

/// The forked exploration workers the sweep runs with: one per core but the
/// controller's (decided on 2026-10-08). Each worker replays one explored
/// timeline and exits, merging its assertion and sancov counts into the
/// controller's tables, so the sweep reaches the same coverage as in-process
/// exploration, faster (1.4x on 4 cores over 240 pinned seeds); only the
/// order of the search depends on which worker finishes first, and every
/// timeline still replays from its seed and recipe. A replay or a focused
/// exploration stays in-process (`workers: 0`): fully deterministic.
fn sweep_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .saturating_sub(1)
}

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
        .enable_exploration(exploration_config(
            EXPLORATION_TIMELINES_PER_SEED,
            sweep_workers(),
        ))
        .until_coverage_stable(PLATEAU_SEEDS, max_iterations)
        .run_configured()
}

/// Run one fresh Chain timeline without requiring coverage saturation. Used for
/// smoke and deterministic seed replay.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_chain_seed(seed: u64) -> SimulationReport {
    chain_builder(None)
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run_configured()
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
        .run_configured();
    assert!(
        report.assertion_violations.is_empty(),
        "safety violation on seed {seed}: {:?}",
        report.assertion_violations
    );
    let digest = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    digest.expect("the chain workload published its audit digest")
}

/// Run one seed of the main campaign under moonpool's determinism canary:
/// the seed runs twice, the second run's every draw on the simulation stream
/// is fingerprinted against the first's (generator state after the draw
/// mixed with the logical clock), and the whole record must be consumed. A
/// process-wide static, a wall-clock read, a `HashMap` iterated in its
/// randomized order — anything paros or its harness keeps outside the seed —
/// fails the seed with the always-assertion the canary evaluates, naming the
/// first diverging draw. Stronger than [`chain_seed_digest`], which compares
/// end states: this compares the whole run, draw by draw.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chain_seed_canary(seed: u64) -> SimulationReport {
    chain_builder(None)
        .check_determinism()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run_configured()
}

/// The canary at volume: `iterations` random seeds of the main campaign, each
/// run twice under moonpool's determinism canary (see [`chain_seed_canary`]).
/// The hunt axis for entropy leaks — a seed that fails here names the first
/// draw at which paros or its harness stopped being a function of the seed.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chain_canary_hunt(iterations: usize) -> SimulationReport {
    chain_builder(None)
        .check_determinism()
        .set_iterations(iterations)
        .run_configured()
}

/// Fast random-seed Chain smoke with no adaptive saturation or branch
/// exploration. This is the only Chain sweep used by nextest.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chain_smoke(iterations: usize) -> SimulationReport {
    chain_builder(None)
        .set_iterations(iterations)
        .run_configured()
}

/// Explore one known root seed. This is the focused recipe-discovery command;
/// the registered campaign still explores every adaptive root seed.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn explore_chain_seed(seed: u64, max_runs: u64) -> SimulationReport {
    chain_builder(None)
        .set_debug_seeds(vec![seed])
        .enable_exploration(exploration_config(max_runs, 0))
        .until_coverage_stable(1, 1)
        .run_configured()
}

/// Run the shared `LogStorage` and `MatchmakerStorage` behavioral contract
/// suites against the library's journal stores (`paros::journal`) on the
/// simulation's own disk, inside one quiet iteration. `MemStorage` runs the
/// identical suites as a `paros` unit test.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn run_storage_contract_suite() -> SimulationReport {
    SimulationBuilder::new()
        .processes(1, || Box::new(crate::process::IdleProcess))
        .workload_factory(|| Box::new(crate::process::ContractSuiteWorkload))
        .set_iterations(1)
        .run_configured()
}
