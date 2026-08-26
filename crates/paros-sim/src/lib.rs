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
//! demo call: it runs one seeded multi-slot Paxos cluster under driver/attrition chaos and
//! returns its timeline, replaying bit-identically from a seed. [`explore`] is the
//! DST sweep that asserts safety + progress across the seed space.

mod audit;
mod chain;
mod chain_workload;
mod choreography;
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
use crate::node::{NaiveWipeNodeProcess, QuietNodeProcess};
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
/// returns (and thereby triggers the harness shutdown). Chaos has ended by now,
/// so this is a quiet tail in which the leader keeps heartbeating and any lagging
/// follower runs commit-replay catch-up to converge. The final convergence check
/// runs after this tail. Eight seconds covers issue #86's buggified seed 0, where
/// a delayed leadership turnover made the final follower catch up 6.05 seconds
/// into the tail.
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
pub const COVERAGE_ITERATIONS: usize = 256;
/// Cap for the network-swarm safety axis. This is likewise only a ceiling; its
/// wider timing surface needs room to establish the unchanged 32-root plateau.
pub const NETWORK_COVERAGE_ITERATIONS: usize = 512;
/// Small cap for the dedicated graceful lifecycle choreography. Its workload
/// forces one ordered scenario per root, so it needs plateau headroom rather
/// than the broad seed volume of the main and network axes.
pub const SNAPSHOT_RECOVERY_COVERAGE_ITERATIONS: usize = 32;
/// Tiny cap for the deterministic protocol-bounds choreography. Every root
/// drives the complete three-page suffix and 64/64/2 Ready sequence.
pub const PROTOCOL_BOUNDS_COVERAGE_ITERATIONS: usize = 8;
/// Maximum root-plus-continuation timelines explored for each adaptive seed.
/// Eight is enough to drive real branches while keeping the sancov gate suitable
/// for CI; Moonpool stops earlier when a root discovers no new frontier.
pub const EXPLORATION_TIMELINES_PER_SEED: u64 = 8;

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
/// Seed 53 is where the [`oracle::GapFillOracle`] first went red: a slot reached
/// the leader alone below a later slot that reached the promise quorum, the
/// leader lost it to a crash, and the election that followed stepped clean over
/// it — freezing every node's chosen prefix one below the hole for the rest of
/// the run, with reads fenced above it, until the `Control::Noop` gap fill
/// landed. Seed 11 is where the
/// [`oracle::AppliedAckOracle`] first went red: a slot chosen above the applied
/// prefix (the leader streams slots concurrently, so a later slot's accept quorum
/// completes first) marked its command *applied* the moment it was learned
/// chosen, so a client retry took the `propose` dedup fast path and was told
/// `committed: true` for a write no node had applied yet — until the
/// `applied_seq`/`inflight` hand-off moved into the contiguous walk. Seed 283 is
/// where the [`oracle::ConvergenceOracle`]'s empty-prefix arm first went red: a
/// quiet-mode run (see [`crate::workload`]) decided exactly one slot — slot 0 — and
/// then went silent, leaving one follower that had learned nothing at all. The
/// leader's beat advertised a bare `Slot(0)` a follower could not tell from "the
/// leader has chosen nothing", so it read its own empty prefix as caught up and
/// never pulled, and every other repair path is shut in that state; the oracle
/// reported it on all 2 039 checks of the settle tail, until
/// `Message::Heartbeat.commit` became `Option<Slot>`. Each replays clean via
/// [`run_seed`].
///
/// **Every seed above is a historical marker, not a live reproduction.** Two
/// changes moved what a seed *means*, and neither is reversible:
///
/// - the moonpool deterministic-executor bump (rev `f7a6d52`, #65) replaced
///   tokio's FIFO task scheduling with seeded-random scheduling, shifting the
///   exact interleaving every seed drives;
/// - #81 removed the message-class nemesis and replaced its one non-redundant
///   capability with driver-level skip/resign hooks, moving the RNG stream;
/// - the direct `DriverHooks` BUGGIFY refactor later moved those draws again by
///   evaluating each independent location only when its action can matter;
/// - #56 added the quiet workload mode, whose draw sits *ahead* of the
///   sequential/pipelined coin on the config stream, so which script a seed runs
///   (and at what pipeline depth) moved again;
/// - the #88/#61 swarm arc made cluster size (`3..=5`, later widened to the
///   full `1..=5` with the quorum edge cases) and client count (`1..4`)
///   per-seed draws resolved at topology-build time — *before* every BUGGIFY
///   activation and workload draw on the counted stream — and added the
///   send-seam drop locations, so every seed's script moved once more. The
///   whole corpus was re-verified green against the shifted stream;
/// - the Stage-6 storage-fault layer (#19) added the write-`EIO` and
///   fsync-failure BUGGIFY sites, the seam-crash bias knob, and the
///   storage-crash restart-delay knob, moving the stream again. The corpus
///   was re-verified green against this shift too.
///
/// Seed 53 was farmed under the nemesis's slot starvation, which no longer
/// exists. Seed 11 was found after the executor bump and was a live reproduction
/// until #81; it no longer is — replaying it against a build with the
/// `applied_seq`/`inflight` hand-off reverted comes back **clean**. Both are kept
/// for what they once caught, and for what the whole set is worth as a cheap
/// always-green replay corpus across the storage, recovery, catch-up, truncation,
/// snapshot and read paths. Any other arc's red→green witness has to be re-hunted
/// against the current tree (#80's seed 1364 included).
///
/// Seed 6156 was the live witness #81 re-derived for the
/// [`oracle::GapFillOracle`] wedge under a leader that skipped pending `Accept`
/// re-sends and then resigned. The direct `DriverHooks` refactor moved those
/// BUGGIFY draws, so it is now retained as a historical regression seed until a
/// witness is re-hunted against the current hook locations.
///
/// **Seed 283 is live, and it is #56's witness.** Restore the sentinel — make
/// `Message::Heartbeat.commit` a bare `Slot` again, `chosen_index.unwrap_or(Slot(0))`
/// at the two producers, with `lags_behind`'s `None => commit > Slot(0)` arm — and
/// this seed goes red (`a stable live node's chosen prefix is never empty once the
/// cluster has chosen a slot`, on every one of 2 039 checks); restore the
/// `Option<Slot>` and it is clean. Seeds 180 and 550 are the same shape, kept
/// unpinned: 283 is the one that fails on *every* check rather than on the last
/// one or two, which is what makes it a witness rather than a near-miss. All three
/// were the only quiet-mode failures in the 1 200 seeds swept, and all three went
/// green on the fix.
///
/// **Seed 0 is #86's live convergence-timing witness.** The shortest-timeout
/// driver BUGGIFY location shifts the leader-turnover pattern into the first seed:
/// the old mid-run `assert_always!` failed on all 467 checks even though the final
/// follower applied slot 1 about 6.05 seconds into the settle tail. It pins both
/// the eight-second recovery budget and the end-of-run convergence assertion.
pub const REGRESSION_SEEDS: &[u64] = &[
    0,
    99,
    42,
    7,
    12_345,
    5,
    18_153_519_926_117_387_038,
    11_316_277_997_507_784_505,
    286_172_402_316_494_352,
    53,
    11,
    6_156,
    283,
];
/// Chain-workload witnesses discovered by the application-state oracle.
pub const CHAIN_REGRESSION_SEEDS: &[u64] = &[
    9_708_989_754_240_691_684,
    11_811_656_051_295_404_958,
    // The half-open probe-channel witness (2026-08-25 swarm, round 11 of 15):
    // every node had applied and agreed by t=12.7s, then the workload's
    // convergence probe to one attrition-restarted node timed out for 74
    // simulated seconds — the probe channel predated the restart and had no
    // keep-alive to detect the dead connection. Red on `ChannelConfig::default`
    // for the workload clients; green with [`client_channel_config`]'s h2 PING
    // keep-alive. Likely the root cause of the #93-era bounded-recovery
    // false positive that widening `recovery_budget_ms` papered over.
    6_442_591_786_636_745_658,
    // The time-dilation false-positive witness (2026-08-25, first sweep with the
    // widened BUGGIFY surface): moonpool's buggified sleep delay — enabled by
    // the per-seed network chaos config and NOT gated on the chaos window —
    // stretched every node's 50ms tick sleep through the tail, collapsing the
    // cluster to ~2 ticks/second. No elections could fire for 4.5 wall-sim
    // seconds, the wall-time quiescence heuristic called that "settled", and
    // the gap assert flagged a chosen slot that healed the instant a stretched
    // election timer finally fired. Red on the wall-time wedge gate; green with
    // the GAP_WEDGE_TICKS streak (the wedge claim now counts the protocol's
    // own tick clock, immune to time dilation).
    8_057_455_177_754_870_256,
    // The tick-starvation witness (2026-08-25, the n=1 edge case's first
    // catch): `run_node`'s tick arm was a fresh relative sleep re-created on
    // every `select!` pass, so a singleton absorbing every client retry (plus
    // reconnect/keep-alive traffic) at sub-interval cadence never ticked —
    // twice in 81 simulated seconds — never elected itself, never acked, and
    // the clients retried harder: a self-sustaining starvation loop. Red on
    // the relative sleep; green with the absolute tick deadline. Seeds
    // 8838873099546465481 and 1481936964395890271 are the same shape.
    3_847_608_256_092_482_294,
    8_838_873_099_546_465_481,
    1_481_936_964_395_890_271,
];

/// Network-swarm-axis witnesses, replayed via [`run_network_seed`].
///
/// Both pin the **false-commit dedup bug**: `propose`'s old `seq <= applied`
/// shortcut assumed a client's seqs execute in order, but an early seq can die
/// without entering the log (a `NotLeader` window on a fresh singleton) while a
/// later seq applies — and the retry of the dead command was then acked
/// `committed: true` at another command's slot. Red on the latest-only
/// `applied_seq`; green with the per-client executed-seq ledger (exact-seq
/// `Chosen`, honest fall-through re-execution otherwise).
pub const NETWORK_REGRESSION_SEEDS: &[u64] = &[
    2_791_878_389_799_639_169,
    8_872_503_201_755_490_526,
    // --- the #94/#95 arc (2026-08-25): four bugs pinned by the 300k-seed raw
    // --- hunt (`sim-paros-hunt network`), all red on the pre-arc tree and
    // --- green after; each seed names the assertion that caught it.
    //
    // #94 at-most-once double-apply ("a (client, seq) command is applied at
    // exactly one log index"): a retry crosses a partition and is served by
    // the majority while the deposed leader's lone accept survives above the
    // cluster prefix; a later election's mandatory P2c re-proposal decides the
    // same identity at a second slot. 16 of the 24 hunt reds were this shape;
    // two representatives pinned. Green with the session-ledger fix
    // (apply-seam suppression + sealed sessions + InstallSnapshot transport).
    17_924_630_138_148_251_668,
    9_504_961_290_707_644_556,
    // Truncation durability ordering ("chain: local application transition is
    // contiguous" / "chain: one state per applied index"): a Truncate flushed
    // in step 1 dropped the accepted records before the application fsync; an
    // AfterApplyBeforeSync crash then left the application permanently behind
    // its own floor, its apply stream shifted forever. Green with the driver
    // flushing truncates only after the application fsync.
    8_398_193_358_524_544_360,
    7_767_531_023_511_969_805,
    // False commit ack ("chain: every acknowledged command was applied"): a
    // stale leader acked a parked waiter when its slot committed a *different*
    // command (it learned the majority's decision via Commit while still
    // role=Leader). Green with the ack-on-commit identity check.
    12_491_191_414_293_127_136,
    // #95 zombie leader ("a leader deposed by a promise-majority stops beating
    // within an election timeout (CheckQuorum)"): 23/2000 seeds red pre-fix —
    // an idle partitioned leader never demotes itself. Green with CheckQuorum
    // (ack-quorum window = election timeout).
    901_969_623_722_906_706,
    // Moonpool #183 established-stream witness ("chain: applied command was
    // proposed"): a directional partition silently removed an interior TCP
    // chunk while later HTTP/2 bytes kept flowing, changing bytes 20..64 of a
    // proposal with BitFlip disabled. Green with the client-proposal checksum:
    // the public gRPC boundary rejects the altered request before consensus.
    11_666_517_603_030_887_004,
];
/// Simulated window (ms) over which chaos (network faults + attrition reboots)
/// fires — wide enough to span the proposal phase so crashes land mid-protocol
/// (creating the follower holes convergence must heal), but ending *before* the
/// client's [`SETTLE_MS`] tail so that tail is quiet. Convergence is asserted
/// only after that tail, so legitimate lag while chaos is active is ignored.
pub(crate) const CHAOS_DURATION_MS: u64 = 4_000;
/// Simulated window over which chaos fires (see [`CHAOS_DURATION_MS`]).
const CHAOS_DURATION: Duration = Duration::from_millis(CHAOS_DURATION_MS);

/// The main liveness campaign's chaos surfaces: single-node crash/restart
/// attrition plus buggified provider knobs. Network swarm is a separate safety
/// axis because its faults persist past Moonpool's cutoff. `prob_wipe = 0`, so durable state (the per-node
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
fn chaos_surfaces() -> [Chaos; 2] {
    [
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

fn chain_network_builder() -> SimulationBuilder {
    chain_cluster_builder()
        .workload_factory(|| Box::new(ChainWorkload::network_safety()))
        .invariant(ChainAgreement::network())
        .enable_chaos([Chaos::Network(ChaosMode::Swarm)])
        .chaos_duration(CHAOS_DURATION)
        .swarm_operations()
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
        .invariant(ChainAgreement::network())
        .enable_chaos([Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 1.0,
                prob_crash: 0.0,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(30_000..30_001),
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

/// Run one deterministic seed and return its timeline. Driver decisions and
/// clean-crash attrition are active; the same seed
/// always produces the same [`RunResult`].
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

/// Run the DST bug-finding sweep: regional latency, attrition, driver hooks,
/// operation swarm, and the safety/recovery oracles under
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

/// Coverage-guided network-turbulence safety axis. Provider network faults do
/// not stop at `chaos_duration` in the pinned Moonpool revision, so this axis
/// deliberately checks Chain and Paxos safety without claiming a quiet recovery
/// tail. Gap-fill Noop application is recorded opportunistically; the pinned
/// network model lacks independent message loss/reorder, so it is not a
/// saturation gate.
#[must_use]
pub fn explore_network_safety(max_iterations: usize) -> SimulationReport {
    chain_network_builder()
        .until_coverage_stable(32, max_iterations)
        .run()
}

/// Raw-iteration network-swarm sweep: `iterations` fresh seeds through the
/// safety oracles with **no** saturation gate and no plateau stop. This is the
/// red-seed *hunting* entry point for partition-shaped interleavings (the #94
/// double-apply lives on this axis): [`explore_network_safety`] stops once
/// coverage plateaus, which is exactly wrong for a hunt that needs raw seed
/// volume past the plateau.
#[must_use]
pub fn network_hunt(iterations: usize) -> SimulationReport {
    chain_network_builder().set_iterations(iterations).run()
}

/// Replay one network-swarm safety-axis seed deterministically (the axis
/// [`explore_network_safety`] sweeps): same builder, one iteration.
#[must_use]
pub fn run_network_seed(seed: u64) -> SimulationReport {
    chain_network_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// The **amnesia red demo** (issue #19 item D): a fixed three-node cluster
/// under the main campaign's attrition + driver chaos, where the first node
/// that comes back through the restart path holding a raised durable promise
/// is *wiped* and rejoins **naively** — as itself, with no protocol support.
///
/// This is proven unsafe (CTRL's takedown of Google's `MarkNonVoting`: a node
/// that lost its promise can accept from an old leader while the new leader
/// still counts that promise, letting a chosen value be overwritten), so the
/// demo's contract is to go **red**: the cross-restart promise audit — the
/// wipe evades the *storage* record, so `set_promise`'s in-core assert never
/// sees it — must catch the reneged promise as an `assertion_violation`.
/// [`AMNESIA_DEMO_SEED`] pins a witness; the nextest suite asserts it *stays*
/// red. On every real campaign the wipe stays off (`prob_wipe = 0`): a
/// snapshot restores the log, not the promise, and restoring redundancy is
/// node replacement — #22's reconfiguration, not a rejoin.
fn amnesia_demo_builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .network_fault_mask(NetworkFaultMask::all().without(NetworkFault::BitFlip))
        .processes(3, || Box::new(NaiveWipeNodeProcess))
        .link_latency(LinkLatencyConfig::default())
        .workload_factory(|| Box::new(ChainWorkload::network_safety()))
        .invariant(ChainAgreement::network())
        .enable_chaos(chaos_surfaces())
        .chaos_duration(CHAOS_DURATION)
}

/// Deterministic witness seed for the amnesia red demo: replaying it through
/// [`run_amnesia_demo_seed`] surfaces the reneged promise ("a node's promised
/// ballot never decreases") as an `assertion_violation`. Recorded per the
/// issue-#19 D contract — this is the book-material citation for why
/// `prob_wipe` stays 0 outside targeted runs.
pub const AMNESIA_DEMO_SEED: u64 = 0;

/// Replay one amnesia red-demo seed (see [`amnesia_demo_builder`]'s contract:
/// the interesting result is the violation, not a green run).
#[must_use]
pub fn run_amnesia_demo_seed(seed: u64) -> SimulationReport {
    amnesia_demo_builder()
        .set_iterations(1)
        .set_debug_seeds(vec![seed])
        .run()
}

/// Raw-seed hunt over the amnesia red demo, for re-deriving a witness seed
/// after a harness change shifts seed meaning.
#[must_use]
pub fn amnesia_demo_hunt(iterations: usize) -> SimulationReport {
    amnesia_demo_builder().set_iterations(iterations).run()
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

/// Run one seed and serialize the [`RunResult`] to JSON. Serializing a plain data
/// struct cannot fail, but on the off chance it does the error is returned as a
/// small JSON object instead of panicking.
#[must_use]
pub fn run_seed_json(seed: u64) -> String {
    serde_json::to_string(&run_seed(seed))
        .unwrap_or_else(|e| format!("{{\"error\":\"serialize failed: {e}\"}}"))
}
