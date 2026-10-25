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
mod frontend;
mod lifecycle;
mod machine;
mod process;
mod roles;
mod shape;
mod state;
mod tls;
mod world;

pub use moonpool_sim::{AssertKind, AssertionStats, SimulationReport};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tracing::level_filters::LevelFilter;

use moonpool_sim::{
    Attrition, AttritionScope, AttritionVictims, Chaos, ChaosMode, ExplorationConfig, GrayFailure,
    LinkLatencyConfig, LocalityConfig, NetworkFault, NetworkFaultMask, SimulationBuilder,
    StorageFault, StorageFaultMask, WipeHeal, WorkloadCount,
};

use crate::chain_workload::ChainWorkload;
use crate::lifecycle::ScriptedLifecycle;
use crate::machine::MachineProcess;
use crate::process::{JoinerProcess, MatchmakerProcess, NodeProcess, ProxyProcess, ReplicaProcess};
use crate::roles::{
    ACCEPTOR_GROUP, FRONTEND_GROUP, MACHINE_GROUP, MATCHMAKER_GROUP, PROXY_GROUP, REPLICA_GROUP,
};

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
/// Per-seed **machine pool** draw (inclusive, #246): the machine process
/// group (`crate::roles::MACHINE_GROUP`), each a `parosd` running the shipped
/// lifecycle from an empty disk until the workload's `init` forms the cell
/// over the founding members the layout draws
/// (`crate::shape::machine_layout`). One is the one-founder cell; two is a
/// cell whose majority is both founders, or a founder and a waiting machine;
/// three a cell that keeps a quorum through one founder's loss (#213 (three
/// founding members), `crate::world::downed_founder`); four leaves room for
/// admitted storage machines a journal moves between (#474 (journals on
/// admitted machines)).
pub(crate) const MACHINE_POOL_RANGE: std::ops::RangeInclusive<usize> = 1..=4;
/// Per-seed **frontend pool** draw (inclusive, #192 (the frontend)): the
/// frontend process group (`crate::roles::FRONTEND_GROUP`), each the shipped
/// `run_frontend` in front of the machines' founding members. Zero is a seed
/// whose clients reach the machines alone; two let a client move on to
/// another frontend when one is down.
pub(crate) const FRONTEND_POOL_RANGE: std::ops::RangeInclusive<usize> = 0..=2;
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
/// Seeds the mutation hunt runs per mutant (#269), when `cargo xtask mutants`
/// is given no `--seeds`: about 2.5 minutes in release for a survivor. A
/// schedule parameter like the `*_ITERATIONS` caps, never buggified.
pub const MUTANT_SEEDS: u64 = 300;

/// Seeds per batch of the mutation hunt (#269): the hunt stops at the first
/// batch with a violation, so a caught mutant costs a fraction of the range.
/// A schedule parameter, never buggified.
pub const MUTANT_BATCH: u64 = 20;

/// The chaos phase lasts the clients' whole run (#454 (the control plane
/// meets chaos), moonpool's `chaos_until_run_ends`), as `FoundationDB`'s
/// fault workloads last a test's whole duration:
///
/// ```text
/// t = 0 .. run end                 setup, `init`, every client operation
///                                    under chaos (network, storage, attrition,
///                                    gray failure, BUGGIFY, `hint!` strikes)
/// the last client's run() returns  moonpool enters recovery mode:
///                                    no new simulator fault, partitions in
///                                    force healed, persistent damage kept;
///                                    paros's own fault coins stop with it
///                                    (`in_chaos`)
/// check()                          the quiet tail on the live cluster:
///                                    election, re-sends, catch-up, the
///                                    recovery batch, the control plane's
///                                    settle, the full read-back and every
///                                    convergence oracle
/// ```
///
/// The run's end is a per-seed knob (`shape::run_end`, 30–120 s); the tail
/// is the workload's `recovery_budget_ms`. This cap only bounds a run whose
/// last step overruns its end. It is also the reference length of a gray
/// failure's window ([`chaos_surfaces`]). **Never buggified.**
pub(crate) const CHAOS_CAP: Duration = Duration::from_secs(150);

/// A gray failure's slow window, as a fraction of [`CHAOS_CAP`].
const GRAY_WINDOW: std::ops::RangeInclusive<f64> = 0.05..=0.4;

/// The shortest run `shape::run_end` draws: a fault timed from the start
/// of the run strikes before it (`world::outage`).
pub(crate) const RUN_FLOOR: Duration = Duration::from_secs(30);

/// Whether the run is still in its chaos phase: until the last client's
/// `run()` returned. Every paros fault coin and scenario window asks it.
pub(crate) fn in_chaos() -> bool {
    moonpool_sim::in_chaos()
}

/// Whether `now` is at least `after` past the chaos cutoff; false while
/// chaos lasts. A scenario that may strike a bounded time into the quiet
/// tail (`world::late_outage::LATE_WINDOW`) times itself with it.
pub(crate) fn past_cutoff(now: Duration, after: Duration) -> bool {
    moonpool_sim::chaos_ended_at().is_some_and(|at| now >= at + after)
}

/// The main campaign's chaos surfaces: swarm network turbulence, one
/// crash/restart attrition regime **per process group**, and buggified
/// provider knobs — one combined axis.
///
/// Moonpool re-samples each attrition base per seed under `ChaosMode::Swarm`
/// (about half the seeds run a regime with no attrition, and the restart
/// window is rescaled to 50–200% of the range below), so the values here are
/// a base, not a fixed shape. The five regimes are independent: the acceptor
/// pool's `max_dead` budget is spent only by dead acceptors, the
/// matchmakers' only by dead matchmakers, the proxies' only by dead proxies,
/// the replicas' only by dead replicas and the machines' only by dead
/// machines (#246), so a killed matchmaker, proxy or replica never keeps the
/// cluster's own quorum whole by proxy, and every role can be down at once.
/// A killed machine reboots from its record: before `init` it waits again,
/// in the middle of one it resumes, after one it serves its cell. A killed replica (#144) comes back to a log the
/// acceptors kept deciding and truncating without it — the catch-up and the
/// below-floor snapshot install a replica exists to survive. A killed proxy
/// leader (#142) is the fault the leader's take-back exists for: every slot
/// delegated to it stalls until the leader runs it colocated, and a proxy
/// killed at the chaos cutoff stays down for the whole recovery tail.
/// `prob_wipe = 0` **stays** zero but on the machines
/// ([`MACHINE_WIPE_WEIGHT`]): moonpool's `CrashAndWipe` now reaches
/// the disk paros stores on, but it wipes a whole machine without asking the
/// copy budget, so it could lose a quorum's copies at once. On the machines
/// the wipes roll (decided on 2026-10-18, moonpool #344 (rolling wipes)):
/// the regime's wipe budget allows one unhealed wiped machine, and moonpool
/// turns a later drawn wipe into a plain crash until the harness signals
/// the first one healed, once the cell holds nothing on its old id
/// (`crate::machine::heal_wiped`). `max_dead: 1` alone did not do this: a
/// wiped machine restarts within seconds, so three machines wiped at about
/// 9 s, 21 s and 27 s took a tenant's three-member matchmaker set. The amnesia fault
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
///
/// `Chaos::Cpu` is moonpool's CPU model (#424 (busyness metrics)), drawn on
/// about half the seeds: every poll of a process's task costs time on the
/// process's one core, and so does every byte it sends or writes. A busy
/// process answers late, and its `Load` reports real CPU numbers.
///
/// `Chaos::GrayFailure` keeps a process alive and slow for 5 % to 40 % of
/// [`CHAOS_CAP`] (7.5 to 60 s, [`GRAY_WINDOW`]), starting anywhere in it, so
/// on a short run part of it falls in the run and the rest is cleared by the
/// cutoff: its CPU (on a CPU-model seed), its disk and its network
/// are each slowed or not by the seed's swarm mask. Two regimes, each with a
/// budget of one slow process: the acceptor group, so a quorum of healthy
/// acceptors stays possible beside the one acceptor the attrition may kill,
/// and the machine group, so `LOAD` meets a slow machine. A slow process is
/// not a dead one: no liveness budget changes. Recovery mode makes every
/// process healthy again, so the tail is still a genuine recovery.
fn chaos_surfaces() -> [Chaos; 13] {
    let regime = |victims: AttritionVictims| Attrition {
        max_dead: 1,
        prob_graceful: 0.0,
        prob_crash: 1.0,
        prob_wipe: 0.0,
        recovery_delay_ms: Some(1_200..2_500),
        grace_period_ms: None,
        scope: AttritionScope::PerProcess,
        victims,
        // No regime but the machines' draws a wipe.
        wipe_budget: None,
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
        // Machine wipes roll (decided on 2026-10-18, moonpool #344
        // (rolling wipes)): one unhealed wipe at a time, healed by the
        // harness once the cell holds nothing on the old id
        // (`crate::machine::heal_wiped`).
        Chaos::Attrition {
            config: Attrition {
                prob_graceful: MACHINE_GRACEFUL_WEIGHT,
                prob_wipe: MACHINE_WIPE_WEIGHT,
                grace_period_ms: Some(MACHINE_GRACE_MS),
                ..regime(AttritionVictims::group(MACHINE_GROUP))
            }
            .wipe_budget(1, WipeHeal::Signal),
            mode: ChaosMode::Swarm,
        },
        Chaos::Attrition {
            config: Attrition {
                prob_graceful: MACHINE_GRACEFUL_WEIGHT,
                grace_period_ms: Some(MACHINE_GRACE_MS),
                ..regime(AttritionVictims::group(FRONTEND_GROUP))
            },
            mode: ChaosMode::Swarm,
        },
        crate::world::outage::regime(),
        Chaos::BuggifyKnobs,
        Chaos::Storage(ChaosMode::Swarm),
        Chaos::Cpu(ChaosMode::Swarm),
        Chaos::GrayFailure {
            config: GrayFailure {
                window: GRAY_WINDOW,
                ..GrayFailure::new(1).victims(AttritionVictims::group(ACCEPTOR_GROUP))
            },
            mode: ChaosMode::Swarm,
        },
        Chaos::GrayFailure {
            config: GrayFailure {
                window: GRAY_WINDOW,
                ..GrayFailure::new(1).victims(AttritionVictims::group(MACHINE_GROUP))
            },
            mode: ChaosMode::Swarm,
        },
    ]
}

/// The weight of moonpool's `CrashAndWipe` in the machine group's
/// attrition, against a crash's 1.0 (#246): about one machine reboot in
/// eleven, timed or at a lifecycle `hint!`, replaces the machine's disk. A
/// wiped machine is a new one that never rejoins as the old one
/// (`crate::machine`): a founding member wiped during `init` is a dead
/// member of the cell once a vote names it, and a wiped member of a formed
/// cell is a member lost for good; a cell that lost a majority is lost
/// (`cell_lost`). Only the machines draw it: an
/// acceptor's wipe is the storage ledger's coin (see [`chaos_surfaces`]).
const MACHINE_WIPE_WEIGHT: f64 = 0.1;

/// The weight of moonpool's graceful reboot in the machine and frontend
/// groups' attrition, against a crash's 1.0 (#560 (clean Ctrl-C shutdown)): about
/// one machine reboot in three is an operator's `SIGTERM`. moonpool cancels
/// the machine's shutdown token, the token `parosd`'s signal cancels, and
/// kills it once [`MACHINE_GRACE_MS`] ran out. The shipped clean stop runs:
/// it refuses new calls, answers the held ones, hands its leaderships on
/// and lets its flushes land. The swarm masks the weight off on half the
/// seeds, so the crash-only regime stays.
const MACHINE_GRACEFUL_WEIGHT: f64 = 0.5;

/// The grace a stopping machine or frontend gets before moonpool kills it,
/// in milliseconds (#560): around the drawn `shutdown_within` (0 to 4 s), so
/// a stop ends on its own on some seeds and is cut by the kill on others.
/// Short, like a crash's recovery delay: moonpool lets a grace started just
/// before the chaos ends run on into the recovery tail, and a founder held
/// down there for up to 10 s left `cell init` unfinished past
/// `FLEET_SETTLE` (hunt seed 10118493541056837987 on f8e629f).
const MACHINE_GRACE_MS: std::ops::Range<usize> = 1_000..3_000;

/// The storage families the campaign leaves out (architecture §5, #176).
/// Rot (corruption, EIO, misdirection) damages a record on whichever disks
/// it lands, and random rot eventually hits every copy of one vote, an
/// unwinnable run: it enters with replicated fault patterns, which keep a
/// quorum's copies clean. Phantom writes lose an acknowledged vote no quorum
/// survives; a failed disk needs a storage watchdog in the driver;
/// degradation episodes are a performance knob; a failed writeback
/// (fsyncgate, moonpool#355) leaves bytes a re-opened store reads as durable
/// (#720 (fsyncgate)). Each has an issue to lift its mask.
fn storage_fault_mask() -> StorageFaultMask {
    StorageFaultMask::all()
        .without(StorageFault::Corruption)
        .without(StorageFault::Eio)
        .without(StorageFault::Misdirect)
        .without(StorageFault::PhantomWrite)
        .without(StorageFault::Degradation)
        .without(StorageFault::DiskFailure)
        .without(StorageFault::WritebackError)
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
        .processes(MACHINE_POOL_RANGE, || Box::new(MachineProcess::chaotic()))
        .processes(FRONTEND_POOL_RANGE, || {
            Box::new(crate::frontend::FrontendProcess::chaotic())
        })
        .link_latency(LinkLatencyConfig::default())
        .workloads(WorkloadCount::Random(CLIENT_COUNT_RANGE), move |_| {
            Box::new(ChainWorkload::new(digest.clone()))
        })
        .enable_chaos(chaos_surfaces())
        .fault_factory(|| Box::new(ScriptedLifecycle))
        .fault_factory(|| Box::new(crate::world::outage::OutageLosses))
        .fault_factory(|| Box::new(crate::world::late_outage::LateOutage))
        .fault_factory(|| Box::new(crate::world::bare_outage::BareOutage))
        .fault_factory(|| Box::new(crate::world::wiped_founder::WipedFounder))
        .fault_factory(|| Box::new(crate::world::silent_machine::SilentMachine))
        .fault_factory(|| Box::new(crate::world::gone_machine::GoneMachine))
        .fault_factory(|| Box::new(crate::world::slow_machine::SlowMachine))
        .fault_factory(|| Box::new(crate::world::disk_full::DiskFull))
        .fault_factory(|| Box::new(crate::world::downed_founder::DownedFounder))
        .fault_factory(|| Box::new(crate::world::zone_outage::ZoneOutage))
        .fault_factory(|| Box::new(crate::world::moved_founder::MovedFounder))
        .fault_factory(|| Box::new(crate::world::replaced_founder::ReplacedFounder))
        .fault_factory(|| Box::new(crate::world::lagging_acceptor::LaggingAcceptor))
        .fault_factory(|| Box::new(crate::world::split_floor::SplitFloor))
        .fault_factory(|| Box::new(crate::world::lagging_replica::LaggingReplica))
        .chaos_duration(CHAOS_CAP)
        .chaos_until_run_ends()
        .swarm_operations()
        // No invariant reads the trace timeline and no subscriber prints it,
        // so every span and event is off: about a fifth of a seed's time went
        // to tracing. The level never changes scheduling or randomness, so a
        // seed replays the same at any level.
        .trace_level(LevelFilter::OFF)
}

/// The index in `0..len` a 64-bit draw names: `draw % len`, reduced in
/// `u64` before it narrows to `usize`. Narrowing first would turn most draws
/// into `0` on a 32-bit target (`wasm32`), so a seed would run differently in
/// the browser than natively (#307 (paros-sim in wasm)). `len == 0` gives `0`.
pub(crate) fn draw_index(draw: u64, len: usize) -> usize {
    let Ok(len64) = u64::try_from(len) else {
        return 0;
    };
    if len64 == 0 {
        return 0;
    }
    let index = usize::try_from(draw % len64).unwrap_or(0);
    assert!(index < len, "a drawn index is inside its range");
    index
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
/// attrition, the driver's BUGGIFY sites, operation swarm, and the safety/recovery checks under
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

/// Run `iterations` more seeds of the sweep's campaign, with the same
/// exploration, and no stop rule: the sancov runner's top-up when the
/// coverage plateau stopped [`explore`] before the declared gates had seeds
/// enough to be judged (`paros-sim-runner`'s `GATE_FLOOR_SEEDS`).
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn explore_more(iterations: usize) -> SimulationReport {
    chain_builder(None)
        .enable_exploration(exploration_config(
            EXPLORATION_TIMELINES_PER_SEED,
            sweep_workers(),
        ))
        .set_iterations(iterations)
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

/// Replay one explored timeline: `seed` with an exploration bug recipe's
/// RNG breakpoints (`[(count, reseed), ..]`, as the sweep prints it), so a
/// failure the sweep found on an explorer branch replays alone.
#[must_use]
#[tracing::instrument(level = "debug", skip(recipe))]
pub fn replay_chain_timeline(seed: u64, recipe: Vec<(u64, u64)>) -> SimulationReport {
    chain_builder(None)
        .replay_timeline(seed, recipe)
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

/// What one seed's run looks like from outside, for comparing two builds of
/// the same campaign: native and `wasm32` (#307 (paros-sim in wasm)). Every
/// field is a function of the seed alone; none reads the host clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedFingerprint {
    /// The seed that ran.
    pub seed: u64,
    /// The audit's end-of-run digest ([`chain_seed_digest`]); `None` when the
    /// workload never reached `check()`.
    pub digest: Option<u64>,
    /// The simulated time the run took, in milliseconds.
    pub simulated_ms: u128,
    /// The scheduler events the run processed.
    pub events: u64,
    /// The always-assertions the run violated.
    pub violations: Vec<String>,
    /// Whether the run failed.
    pub failed: bool,
}

impl std::fmt::Display for SeedFingerprint {
    /// One line, the same bytes on every target for the same run.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let digest = self
            .digest
            .map_or_else(|| "none".to_owned(), |digest| format!("{digest:016x}"));
        write!(
            f,
            "seed={} digest={digest} simulated_ms={} events={} failed={} violations={:?}",
            self.seed, self.simulated_ms, self.events, self.failed, self.violations
        )
    }
}

/// Run one seed and return its [`SeedFingerprint`]. Two builds of paros-sim
/// that run the same seed must return the same fingerprint: CI compares the
/// native build with the `wasm32-unknown-unknown` one under node
/// (`scripts/check-sim-wasm.sh`).
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chain_seed_fingerprint(seed: u64) -> SeedFingerprint {
    let sink: DigestSink = Arc::new(Mutex::new(None));
    let report = chain_builder(Some(sink.clone()))
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run_configured();
    let digest = *sink.lock().unwrap_or_else(PoisonError::into_inner);
    SeedFingerprint {
        seed,
        digest,
        simulated_ms: report.metrics.simulated_time.as_millis(),
        events: report.metrics.events_processed,
        violations: report.assertion_violations.clone(),
        failed: report.failed_runs > 0,
    }
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

/// The mutation hunt (#269): one batch of the seeds `1..=MUTANT_SEEDS` of the
/// main campaign, the same ones for every mutant, so survivors compare between
/// runs. These are not witnesses (AGENTS.md, *No pinned seeds*): the range is
/// a fixed sample, and a draw added to the harness simply shifts which states
/// it covers. `paros-sim-runner`'s `mutants` test target runs the range in
/// batches of [`MUTANT_BATCH`] under `cargo mutants` and stops at the first
/// batch whose report is not clean: the mutant is caught.
///
/// # Panics
///
/// Panics if `seeds` is empty, starts at zero or does not fit a `usize`.
#[must_use]
#[tracing::instrument(level = "debug")]
pub fn chain_mutants(seeds: std::ops::RangeInclusive<u64>) -> SimulationReport {
    assert!(
        !seeds.is_empty(),
        "the mutation hunt runs at least one seed"
    );
    assert!(*seeds.start() > 0, "seed 0 is not a hunt seed");
    let count = seeds.end() - seeds.start() + 1;
    let iterations = usize::try_from(count).expect("the seed count fits a usize");
    chain_builder(None)
        .set_iterations(iterations)
        .set_debug_seeds(seeds.collect())
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
