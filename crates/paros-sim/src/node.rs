//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. It
//! derives a cluster-consistent membership from the topology, wires the node to a
//! per-node handle on the shared [`StorageWorld`] (the sim's stand-in for durable
//! disk), and runs the same `run_node` a production `tokio::main` would — inside a
//! recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: `run_node` unwinds, the volatile `RawNode` is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, StateHandle, TimeProvider, assert_always,
    assert_reachable, assert_sometimes, buggify_knob, buggify_with_prob, sim::sim_random,
};

use crate::audit::{AuditWorld, GateScope, NodeAudit, audit_world};
use crate::chain::{AppliedTransition, ChainState, hash_text};
use paros::{
    Ballot, BootReport, ClientId, ClientSeq, Command, Config, ConfigId, CorruptionKind,
    CorruptionVerdict, DriverHooks, HardState, IdentState, LogRecord, LogVerdict, MemStorage,
    Message, MetadataFault, MetainfoVerdict, MustSync, NodeId, NodeStorage, RecordState,
    RecoveryCase, RunError, Seam, SessionEntry, Slot, Storage, StorageError, StorageRecord,
    WriteOutcome, classify_log, command_hash, decide_metainfo, parse_addr, run_node,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";
const QUIET_HOOKS_KEY: &str = "paros-quiet-driver-hooks";
/// Flag key for the **adversarial amnesia red demo** (issue #19 item D): when
/// set, exactly one node that crashes after raising its durable promise is
/// wiped (its disk deleted from the world) and rejoins **naively** — as itself,
/// with no protocol support. This is *proven unsafe* (CTRL's `MarkNonVoting`
/// takedown: a node that lost its promise can accept from an old leader while
/// the new leader still counts that promise), so the demo's contract is to go
/// **red**: the cross-restart promise audit must catch the reneged promise.
/// Never set on a real campaign; `prob_wipe` stays 0 there (a snapshot restores
/// the log, not the promise — node replacement is #22's reconfiguration).
const NAIVE_WIPE_KEY: &str = "paros-naive-wipe-demo";

/// Per-call firing probability of the write-`EIO` BUGGIFY site (one location,
/// per-seed activation × per-call firing; the record identity travels on the
/// typed error, not on the location).
const P_WRITE_EIO: f64 = 0.01;
/// Per-call firing probability of the fsync-failure BUGGIFY site. Independent
/// from the write site — the sweep must be able to select the two failure
/// modes separately (same rule as the driver's two durability seams).
const P_FSYNC_FAIL: f64 = 0.01;

/// Flag key for the **corruption fault surface** (issue #20 item D). Published
/// by [`CorruptionNodeProcess`]: the world's per-record corruption sites
/// (bit-flip, lost write, misdirected write, read-`EIO`, block runs, snapshot,
/// metainfo-both, sealed ledger, fs-metadata) arm only under it, because their
/// Stage-7 reaction — detect ⇒ crash, with recovery deferred to Stage 8 —
/// permanently downs the node, which the main campaign's convergence contract
/// cannot absorb. The dedicated corruption axis makes safety + detection
/// claims only (asymmetric oracle: unavailable = pass, unsafe = fail). The
/// *recoverable* corruption legs — a torn tail discarded at boot, a single
/// metainfo copy repaired from its twin — stay on the main campaign, where
/// recovery through them is part of the convergence deliverable.
const CORRUPTION_FAULTS_KEY: &str = "paros-corruption-faults";

/// Flag key for the **truncate-on-mismatch red demo** (issue #20 item F).
/// When set, a node whose boot scan reaches a *fatal* corruption verdict on a
/// log entry truncates from the faulty entry onward and keeps running instead
/// of crashing — the exact bug CTRL Figure 2 found in both `ZooKeeper` and
/// `LogCabin`. Proven unsafe (the truncating node can win an election with
/// lagging peers and silently erase committed data cluster-wide), so the
/// demo's contract is to go **red**: the tail-discard audit — a discarded
/// record must never have been a reported durable accept, and must sit
/// strictly above the certain head — catches the illegal truncation. Never
/// set on a real campaign.
const TRUNCATE_DEMO_KEY: &str = "paros-truncate-on-mismatch-demo";

/// Per-`sync` firing probability of the **torn-tail** BUGGIFY site: the flush
/// lands but the update's last record(s) are torn — entry or identifier
/// partially written — and the reported fsync failure crashes the node
/// through the Stage-6 path, pairing the crash-biasing with a world fault on
/// the just-synced batch so the disentanglement table's `CrashTail` rows are
/// reachable. Recovery (discard at the next boot scan) is genuine, so this
/// site runs on the main campaign.
const P_TORN_TAIL: f64 = 0.01;
/// Per-**boot** firing probability of the single-metainfo-copy fault site
/// (the repairable leg of the CTRL metainfo doctrine; main campaign). Boot is
/// the metainfo's only read, so rot-since-last-write is drawn there — and a
/// boot is a rare event, so the per-call rate is high enough for the repair
/// path to saturate.
const P_METAINFO_COPY: f64 = 0.05;
/// Per-`sync` firing probabilities of the corruption-axis fault sites — one
/// independent BUGGIFY location per family so per-seed activation composes.
const P_ENTRY_BITFLIP: f64 = 0.012;
const P_ENTRY_READ_EIO: f64 = 0.008;
const P_ENTRY_LOST_WRITE: f64 = 0.008;
const P_ENTRY_MISDIRECT: f64 = 0.008;
const P_IDENT_FAULT: f64 = 0.006;
const P_BLOCK_FAULT: f64 = 0.006;
const P_SNAPSHOT_FAULT: f64 = 0.006;
const P_LEDGER_FAULT: f64 = 0.004;
/// Per-boot (like [`P_METAINFO_COPY`]), corruption axis only.
const P_METAINFO_BOTH: f64 = 0.02;
const P_METADATA_FAULT: f64 = 0.004;

/// Maximum concurrently in-flight accept writes one fsync can cover — the
/// `TigerBeetle` hardening cap on the crash-truncatable window
/// ([`classify_log`]'s `max_inflight`). The torn-tail site tears at most two
/// records, well inside it.
const MAX_TORN_WINDOW: usize = 4;

/// A paros node in the simulation.
pub struct NodeProcess;

/// A paros node with simulation-only driver decisions disabled. This keeps the
/// dedicated lifecycle choreography's built-in graceful attrition as its sole
/// perturbation without mutating Moonpool's iteration-owned BUGGIFY state.
pub(crate) struct QuietNodeProcess;

#[async_trait]
impl Process for QuietNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(QUIET_HOOKS_KEY).is_none() {
            ctx.state().publish(QUIET_HOOKS_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

/// A paros node for the **amnesia red demo** ([`NAIVE_WIPE_KEY`]): identical to
/// [`NodeProcess`] except that one node per run loses its disk on a restart and
/// rejoins naively. The demo's deliverable is the resulting
/// `assertion_violation` — the cross-restart promise audit catching the reneged
/// promise — never a green run.
pub(crate) struct NaiveWipeNodeProcess;

#[async_trait]
impl Process for NaiveWipeNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(NAIVE_WIPE_KEY).is_none() {
            ctx.state().publish(NAIVE_WIPE_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

/// A paros node with the full corruption fault surface armed (issue #20 item
/// D; see [`CORRUPTION_FAULTS_KEY`]). Used only by the dedicated corruption
/// axis, whose oracle is asymmetric: detection and safety are asserted,
/// availability is not (Stage 7's baseline is detect ⇒ crash).
pub(crate) struct CorruptionNodeProcess;

#[async_trait]
impl Process for CorruptionNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(CORRUPTION_FAULTS_KEY).is_none() {
            ctx.state().publish(CORRUPTION_FAULTS_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

/// A paros node running the **truncate-on-mismatch red demo** (issue #20 item
/// F; see [`TRUNCATE_DEMO_KEY`]): the corruption surface is armed AND the boot
/// scan truncates on a fatal mismatch instead of crashing. The deliverable is
/// the resulting `assertion_violation`, never a green run.
pub(crate) struct TruncateDemoNodeProcess;

#[async_trait]
impl Process for TruncateDemoNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        if ctx.state().get::<bool>(CORRUPTION_FAULTS_KEY).is_none() {
            ctx.state().publish(CORRUPTION_FAULTS_KEY, true);
        }
        if ctx.state().get::<bool>(TRUNCATE_DEMO_KEY).is_none() {
            ctx.state().publish(TRUNCATE_DEMO_KEY, true);
        }
        NodeProcess.run(ctx).await
    }
}

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Build the full cluster membership. `all_process_ips()` excludes this
        // node, so add `my_ip` and sort numerically: every node derives the
        // *same* ordered list, so `NodeId(i) <-> ips[i]` is consistent
        // cluster-wide without any coordination.
        let my_ip = ctx.my_ip().to_string();
        let mut ips: Vec<String> = ctx.topology().all_process_ips().to_vec();
        ips.push(my_ip.clone());
        ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
        ips.dedup();

        let members = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| {
                parse_addr(ip)
                    .map(|addr| (NodeId(u64::try_from(i).expect("node index fits u64")), addr))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

        let self_rank = NodeId(
            u64::try_from(
                ips.iter()
                    .position(|ip| ip == &my_ip)
                    .expect("self is a member"),
            )
            .expect("node index fits u64"),
        );
        let config = Config {
            id: self_rank,
            peers: members.iter().map(|(id, _)| *id).collect(),
            ..Config::default()
        };

        // The per-iteration durable-storage world, shared by every node and
        // surviving crash/restart (it lives in the `StateHandle`, fresh per seed
        // but stable across a process's reboots). Each node reaches it through a
        // `Weak` handle upgraded per op.
        let world = storage_world(ctx.state());
        world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_cluster_size(members.len());
        let perturb = ctx.state().get::<bool>(QUIET_HOOKS_KEY) != Some(true);
        let hooks = BuggifyHooks::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            perturb,
        );
        // The budgeted storage-fault layer (issue #19 B/C) shares the driver
        // hooks' chaos window and quiet-mode switch: after the cutoff the world
        // stops injecting **new** faults but never heals the consequences of
        // old ones — recovery through the tail must be genuine.
        let faults = StorageFaults {
            time: ctx.time().clone(),
            cutoff: Duration::from_millis(crate::CHAOS_DURATION_MS),
            enabled: perturb,
            // The corruption surface arms only on its dedicated axis (see
            // CORRUPTION_FAULTS_KEY): its faults permanently down a node in
            // Stage 7, which the main campaign's convergence contract cannot
            // absorb. The recoverable legs (torn tail, metainfo repair) run
            // wherever `enabled` is.
            corruption: ctx.state().get::<bool>(CORRUPTION_FAULTS_KEY) == Some(true),
            truncate_demo: ctx.state().get::<bool>(TRUNCATE_DEMO_KEY) == Some(true),
        };
        let naive_wipe_demo = ctx.state().get::<bool>(NAIVE_WIPE_KEY) == Some(true);
        // The per-iteration shared audit: pure observation, published beside the
        // storage world so every node folds its transitions into one incremental
        // checker. It never influences the driver — that is `hooks`' job.
        let checker = audit_world(ctx.state());
        let audit = NodeAudit::new(ctx.time().clone(), checker.clone());

        // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
        // drop the volatile node, rebuild storage from the (surviving) world, and
        // re-run — a faithful clean crash + recovery. Attrition (process kill) is
        // handled by the harness; this covers the seams *inside* a Ready batch
        // that attrition cannot reach.
        loop {
            // The amnesia red demo (item D): on a rejoin with a raised durable
            // promise, wipe the disk once and let the node come back naively.
            if naive_wipe_demo {
                maybe_naive_wipe(&world, &my_ip, self_rank.0);
            }
            let storage = DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                my_ip.clone(),
                self_rank.0,
                faults.clone(),
                checker.clone(),
            );
            match run_node(
                ctx.providers().clone(),
                storage,
                parse_addr(&my_ip)?,
                members.clone(),
                ctx.shutdown().clone(),
                &hooks,
                &audit,
            )
            .await
            {
                // Simulated crash at a durability seam: fall through to recover
                // and re-run (rebuilding volatile state from the durable world).
                // The restart delay is workload-buggified config (prong 2): it
                // stretches the durability-seam crash window that process-level
                // attrition cannot reach. A node held down while the cluster
                // keeps committing and truncating returns below the compaction
                // floor and independently exercises snapshot recovery.
                Err(RunError::SeamCrash(_)) => {
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        // BUGGIFY pairing: the restart-delay knob fired — the
                        // held-down-past-the-floor generator is genuinely live.
                        assert_reachable!("a seam-crashed node restarts after a buggified delay");
                        ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                    }
                }
                // An injected storage fault surfaced as the driver's typed
                // crash decision (issue #19 A): fail-stop, so the node re-enters
                // the same Stage-4 crash/restart path — the next iteration
                // boots from whatever the disk *actually* holds, which is how
                // an ambiguous write's two possible outcomes both resolve.
                // Its restart delay is its own independent BUGGIFY location.
                //
                // Stage 7 (issue #20): a *persistent* detected fault — a
                // corrupted durable record or an fs-metadata fault, still on
                // the disk — makes a retry futile: every reboot re-detects it
                // and re-crashes. The honest baseline is fail-stop until
                // operator attention (Stage 8 recovers instead), so the node
                // parks until shutdown. The oracle for this leg is
                // asymmetric: unavailable = pass, unsafe = fail.
                Err(RunError::Storage(_)) => {
                    let parked = {
                        let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                        guard.has_persistent_fault(&my_ip)
                    };
                    if parked {
                        assert_reachable!(
                            "storage: a corruption-downed node stays down (fail-stop)"
                        );
                        ctx.shutdown().cancelled().await;
                        return Ok(());
                    }
                    assert_reachable!("a storage-fault crash recovers through the restart path");
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                    }
                }
                // The only non-crash exit: a genuine infrastructure failure
                // propagates to the harness instead of being retried.
                Err(RunError::Infra(e)) => return Err(e),
                Ok(()) => return Ok(()),
            }
        }
    }
}

/// Seeded uniform choice in `0..n` (for small `n`), from the iteration's RNG.
fn roll_choice(n: u32) -> u32 {
    assert!(n > 0, "a choice needs at least one option");
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let i = (sim_random::<f64>() * f64::from(n)) as u32;
    i.min(n - 1)
}

/// One-shot disk wipe for the amnesia red demo: the first time a node with a
/// raised durable promise comes back through the restart path, delete its disk
/// so it rejoins as itself with no memory of the promise. Deterministic per
/// seed (the first qualifying restart spends the single wipe budget).
fn maybe_naive_wipe(world: &Arc<Mutex<StorageWorld>>, key: &str, node: u64) {
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.wipe_spent {
        return;
    }
    let promised = guard
        .disks
        .get(key)
        .map(|disk| disk.hard_state.max_promised_ballot);
    if promised.is_some_and(|ballot| ballot > Ballot::default()) {
        guard.disks.remove(key);
        guard.marks.remove(key);
        guard.overlays.remove(key);
        guard.wipe_spent = true;
        // Demo-only anchor: the wipe genuinely happened before the naive rejoin.
        assert_reachable!("amnesia demo: a wiped node rejoins naively");
        tracing::info!(node, "naive_wipe");
    }
}

/// Get-or-create the singleton [`StorageWorld`] for this iteration. Get-then-
/// publish is race-free: the sim executor is single-threaded and this runs
/// synchronously (no `.await` between the get and the publish).
fn storage_world(state: &StateHandle) -> Arc<Mutex<StorageWorld>> {
    if let Some(world) = state.get::<Arc<Mutex<StorageWorld>>>(STORAGE_WORLD_KEY) {
        return world;
    }
    let world = Arc::new(Mutex::new(StorageWorld::default()));
    state.publish(STORAGE_WORLD_KEY, world.clone());
    world
}

/// One node's durable records: the scalars, the per-slot accepted log, and the
/// compaction floor. The [`StorageWorld`] owns one of these per node IP.
#[derive(Default)]
struct NodeDisk {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// The first slot still retained. Everything below it has been truncated.
    first_slot: Slot,
    /// Application-produced snapshot state, durable across clean reboot.
    chain: ChainState,
    /// Sealed at-most-once ledger records for truncated slots (#94): read back
    /// on boot so a restart suppresses re-chosen identities like every peer.
    sealed: BTreeMap<(ClientId, ClientSeq), Slot>,
}

/// One injected storage fault: the **ground truth** the oracles compare
/// against. The node only ever sees the ambiguous typed error; whether the
/// effect actually persisted lives here, in the world.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InjectedFault {
    pub(crate) node: u64,
    pub(crate) record: StorageRecord,
    pub(crate) kind: InjectedFaultKind,
    /// The world's seeded, independent decision: did the effect reach the
    /// durable disk despite the reported error?
    pub(crate) persisted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InjectedFaultKind {
    /// A per-record write returned `EIO`.
    WriteEio,
    /// The staged batch's fsync failed.
    FsyncFailed,
}

/// The corruption fault family an injection belongs to — one gate per family
/// (issue #20 item D), correlated injection ↔ detection without string
/// parsing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptionFamily {
    /// Bit flip / latent sector error on a persisted entry.
    BitFlip,
    /// `EIO` on read, degraded to zero-fill ⇒ mismatch.
    ReadEio,
    /// A lost entry write: absence where the identifier exists.
    LostWrite,
    /// A misdirected write: wrong-but-valid record, caught by identity.
    Misdirected,
    /// A fault on an entry's separate identifier record.
    Identifier,
    /// A single block fault hitting a contiguous run of entries.
    BlockRun,
    /// A torn tail paired with the Stage-6 fsync crash (the `CrashTail` leg).
    TornTail,
    /// The snapshot record — its own kind and its own gate (#71).
    Snapshot,
    /// The sealed-sessions ledger record.
    Ledger,
    /// One metainfo copy (repairable from its twin).
    MetainfoCopy,
    /// Both metainfo copies (fatal: the node cannot know what it promised).
    MetainfoBoth,
    /// A filesystem-metadata fault at file granularity (item E).
    Metadata,
}

impl CorruptionFamily {
    fn label(self) -> &'static str {
        match self {
            CorruptionFamily::BitFlip => "bit_flip",
            CorruptionFamily::ReadEio => "read_eio",
            CorruptionFamily::LostWrite => "lost_write",
            CorruptionFamily::Misdirected => "misdirected",
            CorruptionFamily::Identifier => "identifier",
            CorruptionFamily::BlockRun => "block_run",
            CorruptionFamily::TornTail => "torn_tail",
            CorruptionFamily::Snapshot => "snapshot",
            CorruptionFamily::Ledger => "ledger",
            CorruptionFamily::MetainfoCopy => "metainfo_copy",
            CorruptionFamily::MetainfoBoth => "metainfo_both",
            CorruptionFamily::Metadata => "metadata",
        }
    }
}

/// One corruption injection's ground truth.
///
/// The injected ⇔ detected oracle (issue #20 item F) distinguishes
/// *exercised* faults from *dormant* ones so it never overclaims: `detected`
/// flips when the fault is read back and classified; an undetected injection
/// is either dormant (its overlay part still planted, never read) or was
/// retired by a clean re-write before any read (recovery re-writing a record
/// makes the copy durably real again). The failure the oracle exists to catch
/// — read back but NOT detected — is asserted impossible at the read site
/// itself: no faulty record ever reaches the boot view.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CorruptionInjection {
    pub(crate) node: u64,
    pub(crate) family: CorruptionFamily,
    pub(crate) detected: bool,
}

/// One faulted part of a log entry's on-disk pair, with the injection it
/// belongs to (for the ledger's state transitions).
#[derive(Clone, Copy, Debug)]
struct PartFault {
    state: RecordState,
    kind: CorruptionKind,
    inj: usize,
}

/// The read-back fault overlay for one accepted-log slot: what the entry and
/// its identifier will *look like* at the next read. `None` reads back Valid.
#[derive(Clone, Copy, Debug, Default)]
struct EntryFault {
    entry: Option<PartFault>,
    ident: Option<(IdentState, usize)>,
}

/// One node's fault overlay: the semantic read outcomes the world will serve
/// for each durable record class (#70/#71 — records, not bytes; every
/// corruption-family member is a first-class read outcome at the seam).
#[derive(Debug, Default)]
struct FaultOverlay {
    /// Per-slot entry/identifier faults.
    entries: BTreeMap<u64, EntryFault>,
    /// Per-copy metainfo (`HardState`) faults.
    metainfo: [Option<(CorruptionKind, usize)>; 2],
    /// Snapshot-record fault (its own corruption target, #71).
    snapshot: Option<(CorruptionKind, usize)>,
    /// Sealed-sessions ledger fault.
    ledger: Option<(CorruptionKind, usize)>,
    /// File-granularity fs-metadata fault (item E).
    file: Option<(MetadataFault, usize)>,
}

impl FaultOverlay {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.metainfo.iter().all(Option::is_none)
            && self.snapshot.is_none()
            && self.ledger.is_none()
            && self.file.is_none()
    }
}

/// One targetable corruption, injectable in a single
/// [`StorageWorld::corrupt`] call (issue #20 item D: `corrupt(node, record)`
/// — #21's adversarial promise test is a single injection).
#[derive(Clone, Copy, Debug)]
pub(crate) enum CorruptTarget {
    /// The entry record at `slot` reads back as `state` (never `Valid`).
    Entry {
        slot: u64,
        state: RecordState,
        kind: CorruptionKind,
    },
    /// The identifier record at `slot` reads back as `state` (never `Valid`).
    Ident { slot: u64, state: IdentState },
    /// A single block fault: a contiguous run of `len` present entries
    /// starting at `from` all read back mismatched.
    EntryRun { from: u64, len: usize },
    /// One metainfo copy fails its checksum (repairable from its twin).
    MetainfoCopy { copy: u8, kind: CorruptionKind },
    /// Both metainfo copies fail (fatal).
    MetainfoBoth,
    /// The snapshot record fails its checksum.
    Snapshot { kind: CorruptionKind },
    /// The sealed-sessions ledger record fails its checksum.
    Ledger,
    /// A file-granularity fs-metadata fault.
    File { fault: MetadataFault },
}

/// The per-iteration durable-storage world: every node's durable records, keyed
/// by IP. It is **protocol-blind** — it stores records, never knowing what is
/// committed. It outlives process crashes (owned by the `StateHandle`), so a
/// write that reached it before a crash is read back on restart, exactly like a
/// real disk.
///
/// This is also where Stage 6's **budgeted** storage faults are rolled (the
/// caller-side layer of #70/#71; moonpool's provider-level storage chaos stays
/// on as *environmental* pressure). The budget is per-record and cluster-wide:
/// for each accepted-log record, at most `quorum − 1` copies may be
/// fault-suppressed across the cluster — re-counted over **live** copies at
/// injection time (a node that truncated past a slot no longer holds a copy;
/// the `TigerBeetle` `ClusterFaultAtlas` correction) and `assert_always!`ed
/// rather than trusted to construction. Suppression semantics: once the chaos
/// window closes the world refuses **new** injections but never heals the
/// consequences of old ones (a mark clears only when the node genuinely
/// re-writes the record through a clean flush) — recovery stays genuine.
#[derive(Default)]
struct StorageWorld {
    disks: BTreeMap<String, NodeDisk>,
    /// Full cluster membership size, for the quorum bound (set once at boot;
    /// zero refuses every injection).
    cluster_size: usize,
    /// Ground truth of every permitted injection, in order.
    injected: Vec<InjectedFault>,
    /// Lost-leg fault marks per node: accepted-log records whose most recent
    /// write was injected-lost and not yet re-written by a clean flush. These
    /// are what the budget counts.
    marks: BTreeMap<String, BTreeSet<u64>>,
    /// Per-node corruption fault overlays (Stage 7): the semantic read
    /// outcomes the next read of each record will serve.
    overlays: BTreeMap<String, FaultOverlay>,
    /// Ground truth of every corruption injection, in order (the injected ⇔
    /// detected ledger, issue #20 item F).
    corruptions: Vec<CorruptionInjection>,
    /// Nodes carrying (or having carried) a *persistent* fault — one whose
    /// Stage-7 detection permanently downs the node. Counted by the
    /// corruption budget (at most `cluster_size − quorum` nodes, n ≥ 3) and
    /// treated as unclean copies of every record they hold. Sticky:
    /// conservatively never un-counted, even if a clean re-write later
    /// retires the specific fault.
    corruption_downed: BTreeSet<String>,
    /// The amnesia demo's single wipe budget (see [`NAIVE_WIPE_KEY`]).
    wipe_spent: bool,
}

impl StorageWorld {
    fn set_cluster_size(&mut self, n: usize) {
        if self.cluster_size == 0 {
            self.cluster_size = n;
        }
        assert_always!(
            self.cluster_size == n,
            "storage: every node derives the same cluster size"
        );
    }

    fn quorum(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Cluster members whose copy of the accepted-log record at `slot` cannot
    /// be counted clean: fault-marked for it, truncated past it, holding a
    /// corruption overlay on it, or persistently corruption-downed (their
    /// whole disk is off the table at the next boot). A node the world has
    /// never seen a flush from is a clean potential copy.
    fn unclean_nodes(&self, slot: u64) -> BTreeSet<String> {
        let mut unclean: BTreeSet<String> = BTreeSet::new();
        for (node, marks) in &self.marks {
            if marks.contains(&slot) {
                unclean.insert(node.clone());
            }
        }
        for (node, disk) in &self.disks {
            if disk.first_slot.0 > slot {
                unclean.insert(node.clone());
            }
        }
        for (node, overlay) in &self.overlays {
            if overlay.entries.contains_key(&slot) {
                unclean.insert(node.clone());
            }
        }
        unclean.extend(self.corruption_downed.iter().cloned());
        unclean
    }

    /// Clean live copies of the accepted-log record at `slot` (both fault
    /// layers — the Stage-6 lost-write marks and the Stage-7 corruption
    /// overlays — feed the same budget arithmetic).
    fn clean_copies(&self, slot: u64) -> usize {
        self.cluster_size
            .saturating_sub(self.unclean_nodes(slot).len())
    }

    /// Decide one injection under the budget. `accepted_slots` are the
    /// accepted-log records the fault would suppress on `node` when the lost
    /// leg is taken; the check is conservative (it hypothesizes the loss even
    /// when the seeded outcome will be "persisted anyway"). Returns whether
    /// the injection is permitted, recording ground truth and marks if so.
    fn permit_and_record(
        &mut self,
        node_key: &str,
        fault: InjectedFault,
        accepted_slots: &[u64],
    ) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        let quorum = self.quorum();
        // Per-record, cluster-wide budget over live copies: refuse an injection
        // that would leave any touched record with fewer than `quorum` clean
        // copies.
        let marked = |slot: &u64| {
            self.marks
                .get(node_key)
                .is_some_and(|marks| marks.contains(slot))
        };
        for slot in accepted_slots {
            let hypothetical_loss = usize::from(!marked(slot));
            if self.clean_copies(*slot).saturating_sub(hypothetical_loss) < quorum {
                return false;
            }
        }
        // Second-order cap: never fault every accepted record of one node — a
        // fully-faulted node is permanent unavailability, not a recoverable
        // state (`TigerBeetle`'s "never corrupt all chunks of a replica").
        if let Some(disk) = self.disks.get(node_key)
            && !disk.accepted.is_empty()
        {
            let all_marked = disk.accepted.keys().all(|slot| {
                accepted_slots.contains(&slot.0)
                    || self
                        .marks
                        .get(node_key)
                        .is_some_and(|marks| marks.contains(&slot.0))
            });
            if all_marked {
                return false;
            }
        }
        if !fault.persisted {
            let marks = self.marks.entry(node_key.to_string()).or_default();
            marks.extend(accepted_slots.iter().copied());
        }
        // The injection's ground truth, surfaced for the trace (the node itself
        // only ever learns the ambiguous error).
        tracing::info!(
            node = fault.node,
            record = %fault.record,
            kind = match fault.kind {
                InjectedFaultKind::WriteEio => "write_eio",
                InjectedFaultKind::FsyncFailed => "fsync_failed",
            },
            persisted = fault.persisted,
            "storage_fault_injected"
        );
        self.injected.push(fault);
        // The budget is asserted, not assumed: after every permitted
        // injection, each touched record still has a clean quorum, and the
        // node still holds at least one clean accepted record if it holds any.
        for slot in accepted_slots {
            let clean = self.clean_copies(*slot);
            assert_always!(
                clean >= quorum,
                "storage: the per-record fault budget keeps a clean quorum of live copies",
                { "slot" => *slot, "clean" => clean as u64, "quorum" => quorum as u64 }
            );
        }
        if let Some(disk) = self.disks.get(node_key)
            && !disk.accepted.is_empty()
        {
            let any_clean = disk.accepted.keys().any(|slot| {
                !self
                    .marks
                    .get(node_key)
                    .is_some_and(|marks| marks.contains(&slot.0))
            });
            assert_always!(
                any_clean,
                "storage: a fault never blankets every record of one node"
            );
        }
        true
    }

    /// A clean flush re-wrote these accepted records on `node`: their lost-leg
    /// marks clear (the copy is durably real again). This is recovery doing
    /// its job, not the world healing anything.
    fn clear_marks(&mut self, node_key: &str, accepted_slots: impl Iterator<Item = u64>) {
        if let Some(marks) = self.marks.get_mut(node_key) {
            for slot in accepted_slots {
                marks.remove(&slot);
            }
        }
    }

    /// Whether `node` still carries a fault whose Stage-7 detection is
    /// permanent — every future boot re-detects it and re-crashes, so the sim
    /// node loop parks the node instead of hot-looping (fail-stop until
    /// Stage 8's recovery exists).
    fn has_persistent_fault(&self, node_key: &str) -> bool {
        self.corruption_downed.contains(node_key)
            && self.overlays.get(node_key).is_some_and(|o| !o.is_empty())
    }

    /// One corruption injection was read back and classified: transition its
    /// ledger entry to detected (idempotent) and fire its family's coverage
    /// gate — each family is its own reachability anchor (issue #20 item D).
    fn mark_detected(&mut self, inj: usize) {
        let Some(rec) = self.corruptions.get_mut(inj) else {
            return;
        };
        if rec.detected {
            return;
        }
        rec.detected = true;
        tracing::info!(
            node = rec.node,
            family = rec.family.label(),
            "storage_corruption_detected"
        );
        match rec.family {
            CorruptionFamily::BitFlip => {
                assert_reachable!("storage: a bit-flip surfaces as a detected checksum mismatch");
            }
            CorruptionFamily::ReadEio => {
                assert_reachable!("storage: a read EIO is degraded to a checksum mismatch");
            }
            CorruptionFamily::LostWrite => {
                assert_reachable!("storage: a lost write is detected by its surviving identifier");
            }
            CorruptionFamily::Misdirected => {
                assert_reachable!("storage: a misdirected write is caught by the identity check");
            }
            CorruptionFamily::Identifier => {
                assert_reachable!("storage: an identifier fault is detected apart from its entry");
            }
            CorruptionFamily::BlockRun => {
                assert_reachable!("storage: a block fault on a contiguous entry run is detected");
            }
            CorruptionFamily::TornTail => {
                assert_reachable!("storage: a torn tail is discarded as a crash artifact");
            }
            CorruptionFamily::Snapshot => {
                assert_reachable!("storage: a corrupt snapshot is detected at its own checksum");
            }
            CorruptionFamily::Ledger => {
                assert_reachable!("storage: a corrupt sealed-sessions ledger crashes the node");
            }
            CorruptionFamily::MetainfoCopy => {
                assert_reachable!("storage: a lone bad metainfo copy is repaired from its twin");
            }
            CorruptionFamily::MetainfoBoth => {
                assert_reachable!("storage: both metainfo copies faulty crashes the node");
            }
            CorruptionFamily::Metadata => {
                assert_reachable!("storage: an fs-metadata fault reliably crashes the node");
            }
        }
    }

    /// Budget check for a fault whose Stage-7 detection permanently downs the
    /// node. Node-granular (any such fault takes the whole disk off the table
    /// at the next boot): permitted only in clusters of three or more, for at
    /// most `cluster_size − quorum` distinct nodes, and only while every
    /// accepted record the node holds keeps a clean quorum of live copies
    /// without it. Registers the node in the sticky downed set when
    /// permitted.
    fn permit_permanent(&mut self, node_key: &str) -> bool {
        if self.cluster_size < 3 {
            return false;
        }
        let quorum = self.quorum();
        let already = self.corruption_downed.contains(node_key);
        let downs = self.corruption_downed.len() + usize::from(!already);
        if downs > self.cluster_size - quorum {
            return false;
        }
        if let Some(disk) = self.disks.get(node_key) {
            for slot in disk.accepted.keys() {
                let mut unclean = self.unclean_nodes(slot.0);
                unclean.insert(node_key.to_string());
                if self.cluster_size.saturating_sub(unclean.len()) < quorum {
                    return false;
                }
            }
        }
        self.corruption_downed.insert(node_key.to_string());
        true
    }

    /// Inject one targetable corruption — the single-call per-record API
    /// (issue #20 item D: `corrupt(node, record)`; #21's adversarial promise
    /// test is one call). Returns whether the budget and the disk's current
    /// shape permitted it; ground truth is recorded on success.
    fn corrupt(&mut self, node_key: &str, node: u64, target: CorruptTarget) -> bool {
        if self.cluster_size == 0 || !self.disks.contains_key(node_key) {
            return false;
        }
        // Feasibility against the disk's current shape, then the budget.
        let (family, permanent) = match target {
            CorruptTarget::Entry { slot, state, kind } => {
                if state == RecordState::Valid || !self.has_slot(node_key, slot) {
                    return false;
                }
                let family = match (state, kind) {
                    (_, CorruptionKind::ReadIo) => CorruptionFamily::ReadEio,
                    (RecordState::Absent, _) => CorruptionFamily::LostWrite,
                    (RecordState::WrongIdentity, _) => CorruptionFamily::Misdirected,
                    _ => CorruptionFamily::BitFlip,
                };
                (family, true)
            }
            CorruptTarget::Ident { slot, state } => {
                if state == IdentState::Valid || !self.has_slot(node_key, slot) {
                    return false;
                }
                (CorruptionFamily::Identifier, true)
            }
            CorruptTarget::EntryRun { from, len } => {
                if len < 2 || !self.has_run(node_key, from, len) {
                    return false;
                }
                (CorruptionFamily::BlockRun, true)
            }
            CorruptTarget::MetainfoCopy { copy, .. } => {
                if copy > 1 || self.metainfo_copy_faulty(node_key, 1 - copy) {
                    // The twin is already bad: repairing is impossible, and
                    // that shape is MetainfoBoth's to inject.
                    return false;
                }
                (CorruptionFamily::MetainfoCopy, false)
            }
            CorruptTarget::MetainfoBoth => (CorruptionFamily::MetainfoBoth, true),
            CorruptTarget::Snapshot { .. } => (CorruptionFamily::Snapshot, true),
            // The sealed-sessions ledger record exists (formatted) whether or
            // not it holds records yet, so it is corruptible on any disk.
            CorruptTarget::Ledger => (CorruptionFamily::Ledger, true),
            CorruptTarget::File { .. } => (CorruptionFamily::Metadata, true),
        };
        if permanent && !self.permit_permanent(node_key) {
            return false;
        }
        let inj = self.corruptions.len();
        let overlay = self.overlays.entry(node_key.to_string()).or_default();
        match target {
            CorruptTarget::Entry { slot, state, kind } => {
                overlay.entries.entry(slot).or_default().entry =
                    Some(PartFault { state, kind, inj });
            }
            CorruptTarget::Ident { slot, state } => {
                overlay.entries.entry(slot).or_default().ident = Some((state, inj));
            }
            CorruptTarget::EntryRun { from, len } => {
                for slot in Self::run_slots(&self.disks[node_key], from, len) {
                    overlay.entries.entry(slot).or_default().entry = Some(PartFault {
                        state: RecordState::Mismatch,
                        kind: CorruptionKind::ChecksumMismatch,
                        inj,
                    });
                }
            }
            CorruptTarget::MetainfoCopy { copy, kind } => {
                overlay.metainfo[usize::from(copy)] = Some((kind, inj));
            }
            CorruptTarget::MetainfoBoth => {
                overlay.metainfo[0] = Some((CorruptionKind::ChecksumMismatch, inj));
                overlay.metainfo[1] = Some((CorruptionKind::ChecksumMismatch, inj));
            }
            CorruptTarget::Snapshot { kind } => {
                overlay.snapshot = Some((kind, inj));
            }
            CorruptTarget::Ledger => {
                overlay.ledger = Some((CorruptionKind::ChecksumMismatch, inj));
            }
            CorruptTarget::File { fault } => {
                overlay.file = Some((fault, inj));
            }
        }
        tracing::info!(
            node,
            family = family.label(),
            target = ?target,
            "storage_corruption_injected"
        );
        self.corruptions.push(CorruptionInjection {
            node,
            family,
            detected: false,
        });
        true
    }

    /// Tear the just-synced tail (the torn-tail generator): overlay the given
    /// per-record states and record the ground truth. Always within budget —
    /// the torn records are provably unacknowledged (the reported fsync
    /// failure crashes the node before any message predicated on them is
    /// sent) — but their copies still count as unclean via the overlay until
    /// the boot scan discards them.
    fn tear(&mut self, node_key: &str, node: u64, torn: &[(u64, RecordState, IdentState)]) {
        let inj = self.corruptions.len();
        let overlay = self.overlays.entry(node_key.to_string()).or_default();
        for &(slot, entry, ident) in torn {
            let fault = overlay.entries.entry(slot).or_default();
            if entry != RecordState::Valid {
                fault.entry = Some(PartFault {
                    state: entry,
                    kind: CorruptionKind::ChecksumMismatch,
                    inj,
                });
            }
            if ident != IdentState::Valid {
                fault.ident = Some((ident, inj));
            }
        }
        tracing::info!(node, torn = torn.len(), "storage_tail_torn");
        self.corruptions.push(CorruptionInjection {
            node,
            family: CorruptionFamily::TornTail,
            detected: false,
        });
    }

    fn has_slot(&self, node_key: &str, slot: u64) -> bool {
        self.disks
            .get(node_key)
            .is_some_and(|d| d.accepted.contains_key(&Slot(slot)))
    }

    /// The next `len` present slots starting at `from` — the "block" a single
    /// block fault clobbers (CTRL injects per FS block and observes several
    /// entries mismatch at once).
    fn run_slots(disk: &NodeDisk, from: u64, len: usize) -> Vec<u64> {
        disk.accepted
            .range(Slot(from)..)
            .take(len)
            .map(|(slot, _)| slot.0)
            .collect()
    }

    fn has_run(&self, node_key: &str, from: u64, len: usize) -> bool {
        self.disks
            .get(node_key)
            .is_some_and(|d| Self::run_slots(d, from, len).len() == len)
    }

    fn metainfo_copy_faulty(&self, node_key: &str, copy: u8) -> bool {
        self.overlays
            .get(node_key)
            .is_some_and(|o| o.metainfo[usize::from(copy)].is_some())
    }
}

/// The BUGGIFY-side switchboard for the storage-fault sites: shares the driver
/// hooks' chaos window (per the suppression contract on [`StorageWorld`]) and
/// the quiet-mode switch, so choreographed campaigns stay fault-free.
#[derive(Clone)]
struct StorageFaults<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
    /// The Stage-7 corruption surface is armed (the dedicated corruption
    /// axis; see [`CORRUPTION_FAULTS_KEY`]).
    corruption: bool,
    /// The truncate-on-mismatch red demo is armed (see
    /// [`TRUNCATE_DEMO_KEY`]).
    truncate_demo: bool,
}

impl<T: TimeProvider> StorageFaults<T> {
    fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }
}

/// A [`NodeStorage`] handle onto one node's slice of the shared [`StorageWorld`].
///
/// It holds a `Weak` to the world, upgraded per op (moonpool's "world held via
/// Weak, upgraded per op" convention). Reads are served from `boot` — a snapshot
/// of this node's durable records taken at construction — because the core only
/// reads storage once, at boot.
///
/// Writes stage locally and reach the durable world only on a
/// [`sync`](NodeStorage::sync): a [`MustSync::Sync`] batch flushes the stage
/// through (fsync); a [`MustSync::Relaxed`] batch leaves it staged, so it is lost
/// if the incarnation is dropped before a later sync. Because the stage lives in
/// this handle (dropped when `run_node` unwinds on a seam crash), a crash *before*
/// the fsync loses the whole un-synced batch — a faithful clean crash.
///
/// **Fault model (issue #19 B/C).** Two independent BUGGIFY sites: a per-record
/// write returns `EIO`, and the batch fsync fails. On every injected error the
/// world independently decides — seeded, recorded as ground truth — whether the
/// effect persisted anyway (fsyncgate): the *persisted* leg flushes the whole
/// current stage through to the durable disk before reporting the error, the
/// *lost* leg flushes nothing. Ambiguity is therefore at **flush granularity**,
/// the same all-or-nothing the clean-crash model already uses: both legs land
/// states an fsync boundary could legally produce, and the node — which only
/// ever sees the ambiguous typed error and then crashes — must recover
/// correctly from either. Per-record torn durability belongs to Stage 7's
/// corruption model, not here.
struct DurableStorage<T> {
    /// Read view: this node's durable records as of boot.
    boot: MemStorage,
    /// The shared world, upgraded per op.
    world: Weak<Mutex<StorageWorld>>,
    /// This node's IP — its key into the world.
    key: String,
    /// Stable numeric identity used only on application trace facts.
    node_id: u64,
    /// The budgeted fault switchboard (see the type-level fault model note).
    faults: StorageFaults<T>,
    /// The shared checker, fed the world's flush ground truth (see
    /// [`AuditWorld::note_flushed_ground_truth`]).
    checker: Arc<AuditWorld>,
    /// The boot-time integrity scan's outcome, computed by `restore` (the
    /// only durable read) and handed to the driver through
    /// [`NodeStorage::boot_scan`] before the core reads a byte.
    pending_boot: Result<BootReport, StorageError>,
    /// This incarnation's application state, including transitions staged for
    /// the next durability flush.
    application: ChainState,
    /// Writes staged since the last flush (lost if the incarnation is dropped).
    staged_config_id: Option<ConfigId>,
    staged_ballot: Option<Ballot>,
    staged_accepted: BTreeMap<Slot, (Ballot, Command)>,
    staged_chosen: Option<Slot>,
    staged_floor: Option<Slot>,
    staged_snapshot: Option<ChainState>,
    staged_applies: Vec<PendingApply>,
    /// Sealed ledger records staged with a truncate / snapshot install (#94),
    /// flushed to the durable world with the rest of the batch.
    staged_sealed: Vec<SessionEntry>,
}

struct PendingApply {
    slot: Slot,
    transition: AppliedTransition,
}

/// The boot-time integrity scan + verified seeding (issue #20 items B/C/E):
/// read every durable record of `key` through its fault overlay, classify the
/// evidence with the pure classifier, act on the verdicts — discard a
/// crash-truncatable tail, repair a lone metainfo copy, surface anything else
/// as the classified fatal error — and seed the boot view from **verified
/// records only**. Also the injected ⇔ detected ledger's read site: every
/// fault this scan reads back is marked detected in the world's ground truth.
#[allow(clippy::too_many_lines)] // One linear scan: file → metainfo → ledger →
// snapshot → log → seed, each step ordered before the seeding it protects.
fn scan_and_seed(
    guard: &mut StorageWorld,
    key: &str,
    node_id: u64,
    boot: &mut MemStorage,
    application: &mut ChainState,
    truncate_demo: bool,
    checker: &Arc<AuditWorld>,
) -> Result<BootReport, StorageError> {
    let mut report = BootReport::default();

    // --- File-granularity fs-metadata faults (item E): no per-record witness
    // exists to disentangle, so the verdict is reliably crash. A read-only
    // store serves reads and fails at the write path instead.
    if let Some((fault, inj)) = guard.overlays.get(key).and_then(|o| o.file) {
        match fault {
            MetadataFault::Missing | MetadataFault::WrongSize => {
                guard.mark_detected(inj);
                return Err(StorageError::Metadata { fault });
            }
            MetadataFault::ReadOnly => {}
        }
    }

    // --- Metainfo copies (CTRL doctrine): one bad ⇒ use the other and repair
    // it; both bad ⇒ crash — the node cannot know what it promised.
    let copies = guard.overlays.get(key).map_or([None, None], |o| o.metainfo);
    let copy_state = |c: Option<(CorruptionKind, usize)>| {
        if c.is_some() {
            RecordState::Mismatch
        } else {
            RecordState::Valid
        }
    };
    match decide_metainfo(copy_state(copies[0]), copy_state(copies[1])) {
        MetainfoVerdict::Clean => {}
        MetainfoVerdict::RepairCopy(copy) => {
            if let Some((_, inj)) = copies[usize::from(copy)] {
                guard.mark_detected(inj);
            }
            if let Some(overlay) = guard.overlays.get_mut(key) {
                overlay.metainfo[usize::from(copy)] = None;
            }
            report.metainfo_repaired = Some(copy);
        }
        MetainfoVerdict::Fatal => {
            let mut kind = CorruptionKind::ChecksumMismatch;
            for (copy_index, copy) in copies.iter().enumerate() {
                if let Some((k, inj)) = *copy {
                    if copy_index == 0 {
                        kind = k;
                    }
                    guard.mark_detected(inj);
                }
            }
            return Err(StorageError::Corruption {
                record: StorageRecord::Metainfo(0),
                kind,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
    }

    // --- The sealed-sessions ledger: atomic-rename discipline means a partial
    // update was discarded on read, so a mismatch is always corruption.
    if let Some((kind, inj)) = guard.overlays.get(key).and_then(|o| o.ledger) {
        guard.mark_detected(inj);
        return Err(StorageError::Corruption {
            record: StorageRecord::Truncation,
            kind,
            verdict: CorruptionVerdict::Corrupted,
        });
    }

    // --- The snapshot: a first-class corruption target with its own checksum
    // (#71), likewise exempt from crash entanglement.
    if let Some((kind, inj)) = guard.overlays.get(key).and_then(|o| o.snapshot) {
        guard.mark_detected(inj);
        return Err(StorageError::Corruption {
            record: StorageRecord::Snapshot,
            kind,
            verdict: CorruptionVerdict::Corrupted,
        });
    }

    // --- The accepted log: per-record evidence through the fault overlay,
    // classified by the pure batching + hardening classifier.
    let overlay_entries = guard
        .overlays
        .get(key)
        .map(|o| o.entries.clone())
        .unwrap_or_default();
    let disk = guard
        .disks
        .get(key)
        .expect("caller checked the disk exists");
    let chosen_index = disk.hard_state.chosen_index;
    let records: Vec<LogRecord> = disk
        .accepted
        .keys()
        .map(|slot| {
            let fault = overlay_entries.get(&slot.0).copied().unwrap_or_default();
            LogRecord {
                slot: *slot,
                entry: fault.entry.map_or(RecordState::Valid, |p| p.state),
                ident: fault.ident.map_or(IdentState::Valid, |(s, _)| s),
            }
        })
        .collect();
    // The scan read the whole log: every overlaid fault is now exercised and
    // classified — mark its injection detected in the ground-truth ledger.
    for fault in overlay_entries.values() {
        if let Some(part) = fault.entry {
            guard.mark_detected(part.inj);
        }
        if let Some((_, inj)) = fault.ident {
            guard.mark_detected(inj);
        }
    }
    match classify_log(&records, chosen_index, MAX_TORN_WINDOW) {
        LogVerdict::Clean => {}
        LogVerdict::DiscardTail { discard } => {
            let slots: Vec<u64> = discard.iter().map(|(slot, _)| slot.0).collect();
            discard_records(guard, key, &slots);
            checker.note_tail_discard(node_id, &slots);
            report.tail_discarded = discard;
            report.certain_head = chosen_index;
        }
        LogVerdict::Fatal {
            slot,
            case,
            verdict,
        } => {
            if truncate_demo {
                // THE BUG (red demo, CTRL Figure 2): truncate from the first
                // faulty entry onward and keep running, exactly as ZooKeeper
                // and LogCabin did. The tail-discard audit must go red on it.
                let first_faulty = overlay_entries.keys().next().copied().unwrap_or(slot.0);
                let case_by_slot: BTreeMap<u64, RecoveryCase> = records
                    .iter()
                    .enumerate()
                    .map(|(i, record)| {
                        let successor = i + 1 < records.len();
                        (
                            record.slot.0,
                            paros::decide(record.entry, record.ident, successor),
                        )
                    })
                    .collect();
                let slots: Vec<u64> = guard.disks[key]
                    .accepted
                    .range(Slot(first_faulty)..)
                    .map(|(s, _)| s.0)
                    .collect();
                let demo_discard: Vec<(Slot, RecoveryCase)> = slots
                    .iter()
                    .map(|s| {
                        (
                            Slot(*s),
                            case_by_slot.get(s).copied().unwrap_or(RecoveryCase::Clean),
                        )
                    })
                    .collect();
                discard_records(guard, key, &slots);
                checker.note_tail_discard(node_id, &slots);
                tracing::warn!(
                    node = node_id,
                    from = first_faulty,
                    count = slots.len(),
                    "truncate_on_mismatch_demo"
                );
                assert_reachable!("demo: a node truncates its log on a mismatch");
                report.tail_discarded = demo_discard;
                report.certain_head = chosen_index;
            } else {
                // The classified fatal verdict, typed for Stage 8: prefer the
                // injected fault family recorded on the fatal record (it
                // preserves the read-EIO distinction the evidence alone
                // cannot).
                match verdict {
                    CorruptionVerdict::Undecidable => {
                        assert_reachable!(
                            "storage: a last-entry ambiguity is treated as corruption"
                        );
                    }
                    CorruptionVerdict::Corrupted | CorruptionVerdict::CrashTail => {
                        assert_reachable!("storage: a corruption below the tail crashes the node");
                    }
                }
                if case == RecoveryCase::UnidentifiableEntry {
                    assert_reachable!("storage: an entry and its identifier are both faulty");
                }
                let kind = overlay_entries
                    .get(&slot.0)
                    .and_then(|f| f.entry.map(|p| p.kind))
                    .or_else(|| case.kind())
                    .unwrap_or(CorruptionKind::ChecksumMismatch);
                return Err(StorageError::Corruption {
                    record: StorageRecord::Accepted(slot),
                    kind,
                    verdict,
                });
            }
        }
    }

    // --- Seed the boot view from the surviving, verified records. Zero
    // silent bad reads: nothing faulty may remain on any record class the
    // boot view is about to serve (a read-only file fault is write-side and
    // legitimately survives the scan).
    if let Some(overlay) = guard.overlays.get(key) {
        assert_always!(
            overlay.entries.is_empty()
                && overlay.snapshot.is_none()
                && overlay.ledger.is_none()
                && overlay.metainfo.iter().all(Option::is_none),
            "storage: no faulty record ever reaches the boot view"
        );
    }
    let disk = guard
        .disks
        .get(key)
        .expect("caller checked the disk exists");
    *application = disk.chain;
    // Read-back pair of the flush ordering `sync` claims: a floor that
    // reached the disk never outruns the chosen index that reached the disk
    // (the flush applies the floor last, and a crash drops the whole stage
    // together).
    assert_always!(
        disk.first_slot.0 == 0
            || disk
                .hard_state
                .chosen_index
                .is_some_and(|ci| disk.first_slot.0 <= ci.0 + 1),
        "a restored floor never outruns the restored chosen index",
        {
            "floor" => disk.first_slot.0,
            "chosen" => disk.hard_state.chosen_index.map_or(0, |c| c.0)
        }
    );
    if disk.first_slot.0 > 0 {
        assert_reachable!("a node reboots above a non-zero compaction floor");
    }
    // Seed the read view through the semantic ops (records, not a blob).
    // Set the floor first so first_slot() reads it back on boot; the
    // sealed ledger rides on the same op, exactly as a truncate
    // persisted it.
    let sealed: Vec<SessionEntry> = disk
        .sealed
        .iter()
        .map(|(&(client, seq), &slot)| (client, seq, slot))
        .collect();
    let _ = boot.truncate(disk.first_slot, &sealed);
    let _ = boot.persist_config_id(disk.hard_state.config_id);
    let _ = boot.persist_ballot(disk.hard_state.max_promised_ballot);
    for (slot, (ballot, command)) in &disk.accepted {
        let _ = boot.append_accepted(*slot, *ballot, command.clone());
    }
    if let Some(ci) = disk.hard_state.chosen_index {
        let _ = boot.set_chosen_index(ci);
    }
    let _ = boot.sync(MustSync::Sync);
    Ok(report)
}

/// Remove discarded records from the durable world: the disk drops them, the
/// overlay parts covering them are consumed, and their lost-leg marks stay —
/// the node no longer holds a copy, so the budget keeps counting it unclean
/// until a genuine re-write (catch-up) lands one.
fn discard_records(guard: &mut StorageWorld, key: &str, slots: &[u64]) {
    if let Some(disk) = guard.disks.get_mut(key) {
        for slot in slots {
            disk.accepted.remove(&Slot(*slot));
        }
    }
    if let Some(overlay) = guard.overlays.get_mut(key) {
        for slot in slots {
            overlay.entries.remove(slot);
        }
    }
    guard
        .marks
        .entry(key.to_string())
        .or_default()
        .extend(slots.iter().copied());
}

impl<T: TimeProvider> DurableStorage<T> {
    /// Build storage for `config`, seeding the read view from any durable records
    /// a prior boot of this node (same IP, same iteration) left in the world —
    /// after the Stage-7 boot-time integrity scan verified them
    /// (`scan_and_seed`): no faulty record ever reaches the boot view.
    fn restore(
        config: Config,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        node_id: u64,
        faults: StorageFaults<T>,
        checker: Arc<AuditWorld>,
    ) -> Self {
        let mut boot = MemStorage::new(config);
        let mut application = ChainState::default();
        let mut pending_boot: Result<BootReport, StorageError> = Ok(BootReport::default());
        if let Some(strong) = world.upgrade() {
            let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            // Coverage: the recovery paths only matter if boots genuinely
            // re-read a prior incarnation's records (attrition + seam crashes
            // make this common across the sweep).
            assert_sometimes!(
                guard.disks.contains_key(&key),
                "a node boots from a prior incarnation's durable records"
            );
            if guard.disks.contains_key(&key) {
                // Latent metainfo rot is drawn at read time — the metainfo is
                // only ever *read* here, at boot, and every flush rewrites
                // both copies, so boot is where rot since the last write
                // surfaces. A lone bad copy (repairable) runs on any
                // campaign; both-copies (fatal) is corruption-axis only.
                // Each is its own independent BUGGIFY location.
                if faults.active() {
                    if buggify_with_prob!(P_METAINFO_COPY) {
                        let copy = u8::from(sim_random::<f64>() < 0.5);
                        guard.corrupt(
                            &key,
                            node_id,
                            CorruptTarget::MetainfoCopy {
                                copy,
                                kind: CorruptionKind::ChecksumMismatch,
                            },
                        );
                    }
                    if faults.corruption && buggify_with_prob!(P_METAINFO_BOTH) {
                        guard.corrupt(&key, node_id, CorruptTarget::MetainfoBoth);
                    }
                }
                pending_boot = scan_and_seed(
                    &mut guard,
                    &key,
                    node_id,
                    &mut boot,
                    &mut application,
                    faults.truncate_demo,
                    &checker,
                );
            }
        }
        Self {
            boot,
            world,
            key,
            node_id,
            faults,
            checker,
            application,
            pending_boot,
            staged_config_id: None,
            staged_ballot: None,
            staged_accepted: BTreeMap::new(),
            staged_chosen: None,
            staged_floor: None,
            staged_snapshot: None,
            staged_applies: Vec::new(),
            staged_sealed: Vec::new(),
        }
    }

    /// Run `f` against the shared world.
    fn with_world<R>(&self, f: impl FnOnce(&mut StorageWorld) -> R) -> Result<R, StorageError> {
        let strong = self.world.upgrade().ok_or(StorageError::Io {
            record: StorageRecord::Batch,
            outcome: WriteOutcome::Lost,
        })?;
        let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(f(&mut guard))
    }

    /// The record identities currently staged (what the next flush covers).
    fn staged_records(&self) -> Vec<StorageRecord> {
        let mut records = Vec::new();
        if self.staged_config_id.is_some() {
            records.push(StorageRecord::ConfigId);
        }
        if self.staged_ballot.is_some() {
            records.push(StorageRecord::Promise);
        }
        records.extend(
            self.staged_accepted
                .keys()
                .map(|s| StorageRecord::Accepted(*s)),
        );
        if self.staged_chosen.is_some() {
            records.push(StorageRecord::ChosenIndex);
        }
        if self.staged_floor.is_some() {
            records.push(StorageRecord::Truncation);
        }
        if self.staged_snapshot.is_some() {
            records.push(StorageRecord::Snapshot);
        }
        records.extend(
            self.staged_applies
                .iter()
                .map(|p| StorageRecord::Application(p.slot)),
        );
        records
    }

    /// BUGGIFY site 1: this per-record write returns `EIO`. Returns
    /// `Some(persisted)` when the fault fires and the budget permits it —
    /// `persisted` is the world's seeded ambiguity decision (item C), recorded
    /// as ground truth and never told to the node.
    fn roll_write_eio(&mut self, record: StorageRecord) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(P_WRITE_EIO) {
            return None;
        }
        let persisted = sim_random::<f64>() < 0.5;
        let accepted_slots: Vec<u64> = match record {
            StorageRecord::Accepted(slot) => vec![slot.0],
            _ => Vec::new(),
        };
        let fault = InjectedFault {
            node: self.node_id,
            record,
            kind: InjectedFaultKind::WriteEio,
            persisted,
        };
        let in_window = self.faults.active();
        let permitted = self
            .with_world(|w| {
                // Suppression is explicit: the world records no new fault
                // outside the chaos window (and never heals old ones).
                assert_always!(
                    in_window,
                    "storage: no new fault is injected after the chaos window"
                );
                w.permit_and_record(&self.key, fault, &accepted_slots)
            })
            .unwrap_or(false);
        permitted.then_some(persisted)
    }

    /// BUGGIFY site 2: the staged batch's fsync fails. Independent location
    /// from the write site; same ambiguity contract.
    fn roll_fsync_fault(&mut self) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(P_FSYNC_FAIL) {
            return None;
        }
        let persisted = sim_random::<f64>() < 0.5;
        let accepted_slots: Vec<u64> = self.staged_accepted.keys().map(|s| s.0).collect();
        let fault = InjectedFault {
            node: self.node_id,
            record: StorageRecord::Batch,
            kind: InjectedFaultKind::FsyncFailed,
            persisted,
        };
        let in_window = self.faults.active();
        let permitted = self
            .with_world(|w| {
                assert_always!(
                    in_window,
                    "storage: no new fault is injected after the chaos window"
                );
                w.permit_and_record(&self.key, fault, &accepted_slots)
            })
            .unwrap_or(false);
        permitted.then_some(persisted)
    }

    /// BUGGIFY site 3 (Stage 7): the fsync **tears the tail** — the batch
    /// reaches the disk but the update's last record(s) are only partially
    /// written, and the reported fsync failure crashes the node through the
    /// Stage-6 path. This pairs the crash-biasing with a world fault on the
    /// just-synced batch, making every `CrashTail` row of the disentanglement
    /// table reachable; the next boot's scan discards the torn suffix and the
    /// node recovers, so the site runs on the main campaign.
    ///
    /// Returns `None` when the site did not fire (nothing flushed);
    /// `Some(true)` when the stage flushed and a tail was torn (the caller
    /// reports the fsync failure); `Some(false)` when the stage flushed but
    /// no staged record qualified as a tearable tail (fired inert).
    fn roll_torn_tail(&mut self) -> Result<Option<bool>, StorageError> {
        if !self.faults.active() || !buggify_with_prob!(P_TORN_TAIL) {
            return Ok(None);
        }
        let staged: Vec<u64> = self.staged_accepted.keys().map(|s| s.0).collect();
        if staged.is_empty() {
            return Ok(None);
        }
        let key = self.key.clone();
        let node_id = self.node_id;
        // Only a FIRST write of a slot can tear into a discardable artifact: a
        // re-accept (P2c upsert) of a slot already reported durable leaves the
        // prior record recoverable in a real append-only log — and the record
        // was acknowledged, so discarding it is exactly the illegal truncation
        // the audit rejects.
        let pre_existing: BTreeSet<u64> = self
            .with_world(|w| {
                w.disks
                    .get(&key)
                    .map_or_else(BTreeSet::new, |d| d.accepted.keys().map(|s| s.0).collect())
            })
            .unwrap_or_default();
        // The torn write reached the device: flush first, then tear the tail
        // of what just landed.
        self.flush_stage()?;
        let torn = self.with_world(|w| {
            let Some(disk) = w.disks.get(&key) else {
                return false;
            };
            // A discardable tear must sit strictly above the durable chosen
            // index (the classifier's certain head) and form the physical log
            // suffix — anything else would classify as corruption and
            // permanently down the node, which is not this site's contract.
            let min_slot = disk.hard_state.chosen_index.map_or(0, |c| c.0 + 1);
            let max_other = disk
                .accepted
                .keys()
                .map(|s| s.0)
                .filter(|s| !staged.contains(s))
                .max();
            let mut qualifying: Vec<u64> = staged
                .iter()
                .copied()
                .filter(|s| *s >= min_slot && max_other.is_none_or(|m| *s > m))
                .filter(|s| !pre_existing.contains(s))
                .filter(|s| disk.accepted.contains_key(&Slot(*s)))
                .collect();
            qualifying.sort_unstable();
            let take = 1 + usize::from(qualifying.len() >= 2 && sim_random::<f64>() < 0.35);
            let torn_slots: Vec<u64> = qualifying.split_off(qualifying.len().saturating_sub(take));
            if torn_slots.is_empty() {
                return false;
            }
            // Budget + Stage-6 ground truth: the tear rides the fsync-failure
            // ledger (the surfaced error IS an fsync failure, durable leg), so
            // injected ↔ detected stays 1:1, and the per-record quorum budget
            // hypothesizes the torn copies lost.
            let fault = InjectedFault {
                node: node_id,
                record: StorageRecord::Batch,
                kind: InjectedFaultKind::FsyncFailed,
                persisted: true,
            };
            if !w.permit_and_record(&key, fault, &torn_slots) {
                return false;
            }
            let mut torn: Vec<(u64, RecordState, IdentState)> = Vec::new();
            for (i, slot) in torn_slots.iter().enumerate() {
                let last = i + 1 == torn_slots.len();
                // The three tail shapes a crash mid-update can leave; an
                // earlier record of a two-record tear is the window opener.
                let shape = if last {
                    match roll_choice(3) {
                        0 => (RecordState::Mismatch, IdentState::Absent),
                        1 => (RecordState::Valid, IdentState::Absent),
                        _ => (RecordState::Valid, IdentState::Mismatch),
                    }
                } else {
                    (RecordState::Mismatch, IdentState::Absent)
                };
                torn.push((*slot, shape.0, shape.1));
            }
            w.tear(&key, node_id, &torn);
            true
        })?;
        Ok(Some(torn))
    }

    /// The corruption-at-rest fault sites (issue #20 item D), rolled once per
    /// durable flush — one independent BUGGIFY location per family, all armed
    /// only on the corruption axis (their Stage-7 detection permanently downs
    /// the node) and all riding the world's corruption budget.
    // One roll per family, kept together so the per-flush injection pass is a
    // single readable list of independent locations.
    #[allow(clippy::too_many_lines)]
    fn roll_corruption_sites(&mut self) {
        if !self.faults.corruption || !self.faults.active() {
            return;
        }
        let key = self.key.clone();
        let node_id = self.node_id;
        let _ = self.with_world(|w| {
            if !w.disks.contains_key(&key) {
                return;
            }
            let slots: Vec<u64> = w.disks[&key].accepted.keys().map(|s| s.0).collect();
            let pick = |slots: &[u64], bias_tail: bool| -> Option<u64> {
                if slots.is_empty() {
                    return None;
                }
                if bias_tail && sim_random::<f64>() < 0.5 {
                    return slots.last().copied();
                }
                let i = roll_choice(u32::try_from(slots.len()).unwrap_or(u32::MAX)) as usize;
                slots.get(i.min(slots.len() - 1)).copied()
            };
            // Bit flip / latent sector error — biased toward the tail so the
            // proven-fundamental last-entry ambiguity (Undecidable) is
            // routinely reachable, not a lottery.
            if buggify_with_prob!(P_ENTRY_BITFLIP)
                && let Some(slot) = pick(&slots, true)
            {
                w.corrupt(
                    &key,
                    node_id,
                    CorruptTarget::Entry {
                        slot,
                        state: RecordState::Mismatch,
                        kind: CorruptionKind::ChecksumMismatch,
                    },
                );
            }
            // EIO on read: same mismatch evidence, its own fault family
            // (zero-fill ⇒ mismatch — one detection path, one classification
            // path).
            if buggify_with_prob!(P_ENTRY_READ_EIO)
                && let Some(slot) = pick(&slots, false)
            {
                w.corrupt(
                    &key,
                    node_id,
                    CorruptTarget::Entry {
                        slot,
                        state: RecordState::Mismatch,
                        kind: CorruptionKind::ReadIo,
                    },
                );
            }
            // Lost write: the entry reads back reserved where its identifier
            // stands witness.
            if buggify_with_prob!(P_ENTRY_LOST_WRITE)
                && let Some(slot) = pick(&slots, false)
            {
                w.corrupt(
                    &key,
                    node_id,
                    CorruptTarget::Entry {
                        slot,
                        state: RecordState::Absent,
                        kind: CorruptionKind::LostWrite,
                    },
                );
            }
            // Misdirected write: wrong-but-valid record — the checksum
            // passes, the identity check catches it.
            if buggify_with_prob!(P_ENTRY_MISDIRECT)
                && let Some(slot) = pick(&slots, false)
            {
                w.corrupt(
                    &key,
                    node_id,
                    CorruptTarget::Entry {
                        slot,
                        state: RecordState::WrongIdentity,
                        kind: CorruptionKind::Misdirected,
                    },
                );
            }
            // A fault on the physically separate identifier record —
            // interior slots only: at the tail an identifier fault is
            // indistinguishable from a crash artifact (that is the point of
            // the persist-witness rule), so planting one there on a record
            // that WAS acknowledged would make the honest classifier discard
            // acknowledged data — a model lie, not a detector bug. Rot on the
            // very newest identifier is the torn-tail site's shape instead,
            // where the pairing with the fsync crash keeps the ledger honest.
            if buggify_with_prob!(P_IDENT_FAULT)
                && slots.len() >= 2
                && let Some(slot) = pick(&slots[..slots.len() - 1], false)
            {
                let state = if sim_random::<f64>() < 0.5 {
                    IdentState::Mismatch
                } else {
                    IdentState::Absent
                };
                w.corrupt(&key, node_id, CorruptTarget::Ident { slot, state });
            }
            // A single block fault clobbering a contiguous run of entries
            // (Stage 8 must never assume faults are singletons). The run is
            // clamped to what the log actually holds past the start — the
            // chaos-window log is often only a handful of records deep.
            if buggify_with_prob!(P_BLOCK_FAULT) && slots.len() >= 2 {
                let start =
                    roll_choice(u32::try_from(slots.len() - 1).unwrap_or(u32::MAX)) as usize;
                let from = slots[start];
                let len = (2 + roll_choice(3) as usize).min(slots.len() - start);
                w.corrupt(&key, node_id, CorruptTarget::EntryRun { from, len });
            }
            // The snapshot record — its own kind and its own gate (#71).
            if buggify_with_prob!(P_SNAPSHOT_FAULT) {
                let kind = if sim_random::<f64>() < 0.5 {
                    CorruptionKind::ChecksumMismatch
                } else {
                    CorruptionKind::ReadIo
                };
                w.corrupt(&key, node_id, CorruptTarget::Snapshot { kind });
            }
            // The sealed-sessions ledger record.
            if buggify_with_prob!(P_LEDGER_FAULT) {
                w.corrupt(&key, node_id, CorruptTarget::Ledger);
            }
            // File-granularity fs-metadata faults (item E).
            if buggify_with_prob!(P_METADATA_FAULT) {
                let fault = match roll_choice(3) {
                    0 => MetadataFault::Missing,
                    1 => MetadataFault::WrongSize,
                    _ => MetadataFault::ReadOnly,
                };
                w.corrupt(&key, node_id, CorruptTarget::File { fault });
            }
        });
    }

    /// Stage one record through the write-`EIO` fault site: on the persisted
    /// leg the record is staged *and* the whole stage flushes through (the
    /// effect is durable despite the reported error); on the lost leg nothing
    /// is staged. The error is identical either way — that is the ambiguity.
    fn write_record(
        &mut self,
        record: StorageRecord,
        stage: impl FnOnce(&mut Self),
    ) -> Result<(), StorageError> {
        match self.roll_write_eio(record) {
            None => {
                stage(self);
                Ok(())
            }
            Some(persisted) => {
                if persisted {
                    stage(self);
                    self.flush_stage()?;
                }
                Err(StorageError::Io {
                    record,
                    outcome: WriteOutcome::Unknown,
                })
            }
        }
    }

    /// Flush the whole stage through to the durable world — the fsync. Also
    /// the persisted leg of an ambiguous fault: identical effect, error
    /// reported anyway. Clean re-writes clear their lost-leg fault marks and
    /// retire the corruption overlay parts they cover.
    // One linear flush: every step is ordered against its neighbors, and the
    // overlay retirements sit beside the write that justifies each of them.
    #[allow(clippy::too_many_lines)]
    fn flush_stage(&mut self) -> Result<(), StorageError> {
        let config_id = self.staged_config_id.take();
        let ballot = self.staged_ballot.take();
        let accepted = std::mem::take(&mut self.staged_accepted);
        let chosen = self.staged_chosen.take();
        let floor = self.staged_floor.take();
        let snapshot = self.staged_snapshot.take();
        let applies = std::mem::take(&mut self.staged_applies);
        let sealed = std::mem::take(&mut self.staged_sealed);
        let flushed_slots: Vec<u64> = accepted.keys().map(|s| s.0).collect();
        let flushed_hashes: Vec<(u64, u64)> = accepted
            .iter()
            .map(|(slot, (_ballot, command))| (slot.0, command_hash(command)))
            .collect();
        let metainfo_rewritten = config_id.is_some() || ballot.is_some() || chosen.is_some();
        let sealed_rewritten = !sealed.is_empty();
        let snapshot_rewritten = snapshot.is_some() || !applies.is_empty();
        let key = self.key.clone();
        self.with_world(|w| {
            w.clear_marks(&key, flushed_slots.iter().copied());
            // A clean re-write retires the corruption overlay it covers (the
            // record is durably real again) — the injection ledger derives
            // "cleared" from the part disappearing before any read. The
            // metainfo copies are rewritten whole on every scalar update; the
            // snapshot record is rewritten by installs and by application
            // fsyncs; the ledger by new sealed records.
            if let Some(overlay) = w.overlays.get_mut(&key) {
                for slot in &flushed_slots {
                    overlay.entries.remove(slot);
                }
                if metainfo_rewritten {
                    overlay.metainfo = [None, None];
                }
                if sealed_rewritten {
                    overlay.ledger = None;
                }
                if snapshot_rewritten {
                    overlay.snapshot = None;
                }
            }
            let d = w.disks.entry(key.clone()).or_default();
            if let Some(config_id) = config_id {
                d.hard_state.config_id = config_id;
            }
            // Sealed ledger records are upserts keyed by (client, seq); the
            // first-slot claim wins, matching the core's ledger semantics.
            for (client, seq, slot) in sealed {
                d.sealed.entry((client, seq)).or_insert(slot);
            }
            if let Some(b) = ballot {
                // The promise is monotonic: never let a flush lower it. A
                // SetPromise write only ever raises it, but an InstallSnapshot
                // carries the *server's* ballot, which can be below this node's own
                // promise, so take the max (matching `MemStorage::install_snapshot`).
                d.hard_state.max_promised_ballot = d.hard_state.max_promised_ballot.max(b);
            }
            for (slot, record) in accepted {
                d.accepted.insert(slot, record);
            }
            if let Some(c) = chosen {
                d.hard_state.chosen_index = Some(c);
            }
            // Apply the truncation last, after the chosen index it sits behind, so
            // a flushed floor never outruns the flushed chosen index.
            if let Some(f) = floor {
                d.first_slot = d.first_slot.max(f);
                d.accepted.retain(|s, _| *s >= d.first_slot);
            }
            if let Some(installed) = snapshot {
                d.chain = installed;
            }
            if let Some(last) = applies.last() {
                d.chain = last.transition.next;
            }
            // Write-side pair of the `restore` read-back check: the floor is
            // applied last, behind the chosen index it sits under, so no flush
            // ever leaves a durable floor above the durable chosen index.
            assert_always!(
                d.first_slot.0 == 0
                    || d
                        .hard_state
                        .chosen_index
                        .is_some_and(|ci| d.first_slot.0 <= ci.0 + 1),
                "a flushed floor never outruns the flushed chosen index",
                {
                    "floor" => d.first_slot.0,
                    "chosen" => d.hard_state.chosen_index.map_or(0, |c| c.0)
                }
            );
            // A floor raise retires the fault marks of the records it drops:
            // truncation (or a snapshot install) is only ever decided for the
            // applied prefix, so the record's information has migrated into
            // the application snapshot — clearing the mark is custodianship
            // transfer, not the world healing a fault.
            let new_floor = d.first_slot;
            if let Some(marks) = w.marks.get_mut(&key) {
                marks.retain(|slot| *slot >= new_floor.0);
            }
            // A floor raise drops the records it covers, faults and all: the
            // information migrated into the application snapshot, so the
            // overlay parts below the floor are retired with the records.
            if let Some(overlay) = w.overlays.get_mut(&key) {
                overlay.entries.retain(|slot, _| *slot >= new_floor.0);
            }
        })?;

        // Feed the shared checker the flush's ground truth (see
        // [`AuditWorld::note_flushed_ground_truth`]): an ambiguous fault leg
        // flushes without the driver ever surfacing the writes, and the
        // cross-restart checks must compare against what the disk actually
        // holds. A clean flush notes the same values the driver is about to
        // report, so the double entry is idempotent.
        let now_ms = u64::try_from(self.faults.time.now().as_millis()).unwrap_or(u64::MAX);
        let landing = if snapshot.is_some() {
            chosen.map(|slot| slot.0)
        } else {
            None
        };
        self.checker.note_flushed_ground_truth(
            self.node_id,
            now_ms,
            &flushed_hashes,
            floor.map(|slot| slot.0),
            landing,
        );

        if let Some(installed) = snapshot {
            tracing::info!(
                node = self.node_id,
                index = installed.applied_count,
                state = %hash_text(installed.chain_hash),
                "chain_snapshot_installed"
            );
        }
        for pending in applies {
            let next = pending.transition.next;
            tracing::info!(
                target: "chain",
                node = self.node_id,
                slot = pending.slot.0,
                index = next.applied_count,
                cmd = %hash_text(pending.transition.cmd_hash),
                state = %hash_text(next.chain_hash),
                kind = pending.transition.kind,
                "command_applied"
            );
        }
        Ok(())
    }
}

impl<T: TimeProvider> NodeStorage for DurableStorage<T> {
    fn persist_config_id(&mut self, config_id: ConfigId) -> Result<(), StorageError> {
        self.write_record(StorageRecord::ConfigId, |s| {
            s.staged_config_id = Some(config_id);
        })
    }

    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.write_record(StorageRecord::Promise, |s| {
            s.staged_ballot = Some(ballot);
        })
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.write_record(StorageRecord::Accepted(slot), |s| {
            s.staged_accepted.insert(slot, (ballot, command));
        })
    }

    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.write_record(StorageRecord::ChosenIndex, |s| {
            s.staged_chosen = Some(slot);
        })
    }

    fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        // A relaxed (chosen-index-only) batch keeps its stage un-flushed: it is
        // durable only once a later Sync flushes it, and lost on a crash before
        // then. A Sync batch flushes the whole stage through to the world.
        if must_sync != MustSync::Sync {
            // The classification contract, checked from the other side: a batch
            // allowed to skip the fsync can be holding no safety-critical write
            // (every promise-raise or accept classifies as `MustSync::Sync` and
            // was flushed by its own batch's sync).
            assert_always!(
                self.staged_ballot.is_none(),
                "a relaxed flush holds no staged promise"
            );
            assert_always!(
                self.staged_accepted.is_empty(),
                "a relaxed flush holds no staged accept"
            );
            assert_always!(
                self.staged_config_id.is_none(),
                "a relaxed flush holds no staged configuration identity"
            );
            return Ok(());
        }
        // Item E: a read-only store serves reads and fails every fsync — a
        // file-granularity metadata fault with no record witness, so the
        // verdict is reliably crash (never attempted recovery).
        let readonly = self.with_world(|w| {
            w.overlays
                .get(&self.key)
                .and_then(|o| o.file)
                .filter(|(fault, _)| *fault == MetadataFault::ReadOnly)
        })?;
        if let Some((fault, inj)) = readonly {
            self.with_world(|w| w.mark_detected(inj))?;
            return Err(StorageError::Metadata { fault });
        }
        // BUGGIFY site 2: the fsync fails — only when the stage actually holds
        // something (an empty flush has nothing at stake). On the durable leg
        // the flush happens anyway before the error is reported (fsyncgate);
        // on the lost leg the stage stays un-flushed and dies with the
        // incarnation the driver's crash decision is about to unwind.
        if !self.staged_records().is_empty()
            && let Some(persisted) = self.roll_fsync_fault()
        {
            if persisted {
                self.flush_stage()?;
            }
            return Err(StorageError::FsyncFailed {
                record: StorageRecord::Batch,
                outcome: WriteOutcome::Unknown,
            });
        }
        // BUGGIFY site 3 (Stage 7): the fsync lands the batch but tears the
        // tail record(s); the reported failure crashes the node and the next
        // boot scan disentangles the torn suffix.
        match self.roll_torn_tail()? {
            Some(true) => {
                return Err(StorageError::FsyncFailed {
                    record: StorageRecord::Batch,
                    outcome: WriteOutcome::Unknown,
                });
            }
            Some(false) => {
                self.roll_corruption_sites();
                return Ok(());
            }
            None => {}
        }
        self.flush_stage()?;
        // Corruption at rest, drawn per durable flush (corruption axis only).
        self.roll_corruption_sites();
        Ok(())
    }

    fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        // Stage the floor like every other write: it reaches the durable world
        // only on the next Sync flush (Truncate classifies as MustSync::Sync).
        // The sealed ledger records ride in the same staged batch.
        let sealed = sealed.to_vec();
        self.write_record(StorageRecord::Truncation, |s| {
            s.staged_floor = Some(s.staged_floor.map_or(first, |f| f.max(first)));
            s.staged_sealed.extend_from_slice(&sealed);
        })
    }

    fn boot_scan(&mut self) -> Result<BootReport, StorageError> {
        self.pending_boot.clone()
    }

    fn snapshot(&self) -> Result<Vec<u8>, StorageError> {
        // Serving a snapshot is a durable-record read: verify through the
        // fault overlay first (a corrupt snapshot is detected here, at the
        // would-be server, and never shipped to a peer).
        let fault = self
            .with_world(|w| {
                let fault = w.overlays.get(&self.key).and_then(|o| o.snapshot);
                if let Some((_, inj)) = fault {
                    w.mark_detected(inj);
                }
                fault
            })
            .unwrap_or(None);
        if let Some((kind, _)) = fault {
            assert_reachable!("storage: a corrupt snapshot is caught before being served");
            return Err(StorageError::Corruption {
                record: StorageRecord::Snapshot,
                kind,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        Ok(self.application.encode())
    }

    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        // Stage the install like every other write (InstallSnapshot is
        // MustSync::Sync): the chosen index, the adopted ballot, the floor
        // (`chosen_index + 1`), and the serving peer's session ledger reach the
        // durable world on the next Sync flush, where the floor is applied last
        // so it never outruns the chosen index.
        let installed = ChainState::decode(&snapshot).map_err(|detail| {
            // A received snapshot that fails validation is a detected
            // corruption of transferred bytes: classified, never installed.
            tracing::warn!(node = self.node_id, detail, "snapshot_decode_rejected");
            StorageError::Corruption {
                record: StorageRecord::Snapshot,
                kind: CorruptionKind::ChecksumMismatch,
                verdict: CorruptionVerdict::Corrupted,
            }
        })?;
        assert_always!(
            installed.applied_slot() == Some(chosen_index),
            "chain: snapshot state matches its boundary"
        );
        assert_always!(
            installed.applied_count >= self.application.applied_count,
            "chain: snapshot install does not regress state"
        );
        let sessions = sessions.to_vec();
        self.write_record(StorageRecord::Snapshot, |s| {
            s.staged_sealed.extend_from_slice(&sessions);
            s.staged_chosen = Some(chosen_index);
            s.staged_ballot = Some(ballot);
            let first = Slot(chosen_index.0 + 1);
            s.staged_floor = Some(s.staged_floor.map_or(first, |f| f.max(first)));
            s.application = installed;
            s.staged_snapshot = Some(installed);
        })
    }

    fn apply(
        &mut self,
        chosen_index: Slot,
        slot: Slot,
        command: &Command,
    ) -> Result<(), StorageError> {
        assert_always!(
            slot <= chosen_index,
            "chain: apply does not outrun chosen prefix"
        );
        if self
            .application
            .applied_slot()
            .is_some_and(|applied| slot <= applied)
        {
            // The driver's boot replay re-walks the retained chosen prefix; a
            // node whose application state survived (clean reboot, or a crash
            // after the app fsync) skips the already-applied prefix here.
            assert_reachable!("a boot replay skips an already-applied slot");
            return Ok(());
        }
        let expected = self
            .application
            .applied_slot()
            .map_or(Slot(0), |applied| Slot(applied.0.saturating_add(1)));
        if slot != expected {
            eprintln!(
                "CONTIGUITY: node={} slot={} expected={} chosen_index={} applied_count={}",
                self.node_id, slot.0, expected.0, chosen_index.0, self.application.applied_count
            );
        }
        assert_always!(
            slot == expected,
            "chain: local application transition is contiguous"
        );
        let transition = self.application.apply(command);
        self.write_record(StorageRecord::Application(slot), |s| {
            s.application = transition.next;
            s.staged_applies.push(PendingApply { slot, transition });
        })
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.application.applied_slot()
    }
}

impl<T: TimeProvider> Storage for DurableStorage<T> {
    fn initial_state(&self) -> (HardState, Config) {
        self.boot.initial_state()
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.boot.accepted(slot)
    }
    fn first_slot(&self) -> Slot {
        self.boot.first_slot()
    }
    fn last_slot(&self) -> Slot {
        self.boot.last_slot()
    }
    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.boot.sealed_sessions()
    }
}

/// Simulation hooks for driver decisions that process-level attrition cannot
/// reach. Every behavior has its own `BUGGIFY` location, so activation is
/// independent and replayable. All hooks turn off with the chaos window, leaving
/// the settle tail genuinely quiet for convergence.
struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
    /// Write-window crash bias (issue #19 B, the `TigerBeetle` "×10 while writes
    /// are in flight" pressure): a workload-buggified multiplier on the
    /// durability-seam crash probability. The seams are only ever consulted
    /// with a batch in flight, so biasing them *is* biasing crashes into the
    /// write window. Drawn per seed, per node, FDB knob style.
    seam_crash_bias: f64,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    fn new(time: T, cutoff: Duration, enabled: bool) -> Self {
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = buggify_knob!(1_u64, 4_u64..11_u64) as f64;
        Self {
            time,
            cutoff,
            enabled,
            seam_crash_bias,
        }
    }

    fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn crash_at(&self, seam: Seam) -> bool {
        let prob = 0.03 * self.seam_crash_bias;
        let fired = self.active()
            && match seam {
                Seam::BeforeSync => buggify_with_prob!(prob),
                Seam::AfterSyncBeforeSend => buggify_with_prob!(prob),
                Seam::AfterApplyBeforeSync => buggify_with_prob!(prob),
            };
        if fired && self.seam_crash_bias > 1.0 {
            // BUGGIFY pairing: the biased write-window crash pressure genuinely
            // fires on some seed (no slot is created when it never does).
            assert_reachable!("a write-window-biased seam crash fires");
        }
        fired
    }

    fn skip_accept_resend(&self) -> bool {
        self.active() && buggify_with_prob!(0.95)
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }

    fn shortest_election_timeout(&self) -> bool {
        self.active() && buggify_with_prob!(0.5)
    }

    fn drop_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Three locations, selected independently per seed: an isolated
        // `Accept` loss is the interleaving behind a stranded chosen-gap wedge
        // (#80) — one earlier slot's Accept vanishes while later slots land —
        // while losing a `Promise`/`Prepare` stretches elections open, and a
        // lost `Nack` keeps a below-floor candidate's campaign alive long
        // enough for the answering snapshot to land mid-election (the
        // truncated-quorum Nack otherwise steps the candidate down before the
        // `CatchUpRequest`'s snapshot offer arrives — the #88 window).
        match msg {
            Message::Accept { .. } => buggify_with_prob!(0.05),
            Message::Prepare { .. } | Message::Promise { .. } => buggify_with_prob!(0.10),
            Message::Nack { .. } => buggify_with_prob!(0.25),
            // A dropped `Commit` delays a follower's floor-raise (truncation
            // applies lazily at its Truncate slot), widening the mixed-floor
            // window the #88 mid-election snapshot needs — and leaves the
            // follower hole commit-replay catch-up must heal (#80's terrain).
            Message::Commit { .. } => buggify_with_prob!(0.05),
            // The lost *ack*: a slot durably accepted by a quorum whose
            // proposer never learns it — the pure quorum-intersection edge
            // that forces a re-propose under a new ballot (P2c for real).
            Message::Accepted { .. } => buggify_with_prob!(0.05),
            // Starve the read fence / the catch-up push direction. Kept low:
            // these fire per tick per peer, and a high rate is just a
            // partition, which is moonpool's job.
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => buggify_with_prob!(0.02),
            // Repair traffic for a node that is already behind: a lost
            // response costs one beat of latency and re-derives on the next.
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn duplicate_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Moonpool has no message-duplication fault, so this seam is the only
        // duplicate generator. The quorum-counting kinds are the point of the
        // location: every quorum in the core is set-based today, and this
        // keeps a future "optimization" into counters from fabricating a
        // quorum out of a duplicated ack.
        match msg {
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. } => {
                buggify_with_prob!(0.05)
            }
            Message::Commit { .. } => buggify_with_prob!(0.05),
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        if !self.active() {
            return false;
        }
        // Dropped *after* the server committed/applied: the client's retry
        // must take the `(client, seq)` dedup path, the at-most-once edge the
        // truncated-dedup-window hazard lives on. One location per reply kind.
        match reply {
            paros::Reply::Propose => buggify_with_prob!(0.10),
            paros::Reply::ProposeDedup => buggify_with_prob!(0.10),
            paros::Reply::Read => buggify_with_prob!(0.10),
        }
    }
}

// --- storage-fault ground truth for the oracles -------------------------------

/// Snapshot of the [`StorageWorld`]'s injected-fault ground truth, folded for
/// the workload's `check()` phase.
///
/// The bools are independent sticky quadrant facts, not a state machine (same
/// waiver as the audit's flag set).
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct StorageFaultStats {
    /// Permitted injections, in order (the ground truth ledger).
    pub(crate) injected: usize,
    /// Sticky per-quadrant facts (item C: all four ambiguity quadrants).
    pub(crate) eio_landed: bool,
    pub(crate) eio_lost: bool,
    pub(crate) fsync_durable: bool,
    pub(crate) fsync_lost: bool,
    /// Re-derived from current world state: every fault-marked accepted
    /// record still has at least a quorum of clean live copies. Under the
    /// injection-time budget this must always hold; it is re-derived
    /// independently so an unavailable run can be judged against it
    /// (`TigerBeetle`'s excuse-list doctrine — the bound is never trusted to be
    /// sufficient on its own).
    pub(crate) clean_quorum_everywhere: bool,
}

/// Fold the storage world's ground truth (empty world = no faults).
pub(crate) fn storage_fault_stats(handle: &StateHandle) -> StorageFaultStats {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut stats = StorageFaultStats {
        injected: guard.injected.len(),
        eio_landed: false,
        eio_lost: false,
        fsync_durable: false,
        fsync_lost: false,
        clean_quorum_everywhere: true,
    };
    for fault in &guard.injected {
        match (fault.kind, fault.persisted) {
            (InjectedFaultKind::WriteEio, true) => stats.eio_landed = true,
            (InjectedFaultKind::WriteEio, false) => stats.eio_lost = true,
            (InjectedFaultKind::FsyncFailed, true) => stats.fsync_durable = true,
            (InjectedFaultKind::FsyncFailed, false) => stats.fsync_lost = true,
        }
    }
    let quorum = guard.quorum();
    let marked: BTreeSet<u64> = guard
        .marks
        .values()
        .flat_map(|marks| marks.iter().copied())
        .collect();
    for slot in marked {
        // The availability re-derivation deliberately differs from the
        // injection-time budget formula: here a peer that truncated past the
        // slot counts as *clean* — its truncation was decided over the applied
        // prefix, so it can serve the superseding snapshot — while only a
        // live lost-write mark makes a copy unclean. The budget stays
        // conservative (truncated peers don't count there); this independent
        // count is what an unavailable run is judged against.
        let marked_nodes = guard
            .marks
            .values()
            .filter(|marks| marks.contains(&slot))
            .count();
        if guard.cluster_size.saturating_sub(marked_nodes) < quorum {
            stats.clean_quorum_everywhere = false;
        }
    }
    stats
}

/// The storage-fault coverage gates + the injected↔detected correlation,
/// evaluated once per run from the workload's `check()` (the shared-gate
/// doctrine in [`crate::audit`]). The correlation is safety and runs on every
/// scope; the quadrant `sometimes` gates saturate on the main campaign only.
pub(crate) fn check_storage_gates(handle: &StateHandle, scope: GateScope) {
    let stats = storage_fault_stats(handle);
    let detected = audit_world(handle).storage_faults_detected();
    // Injected fault ↔ surfaced error ↔ crash decision correlate 1:1, typed,
    // with no string parsing: a spontaneous (non-injected) storage fault or a
    // swallowed injection both break this count.
    assert_always!(
        detected == u64::try_from(stats.injected).unwrap_or(u64::MAX),
        "storage: every injected fault surfaces as exactly one typed crash decision",
        {
            "injected" => u64::try_from(stats.injected).unwrap_or(u64::MAX),
            "detected" => detected
        }
    );
    assert_always!(
        stats.clean_quorum_everywhere,
        "storage: injected faults never cost a record its clean quorum of live copies"
    );
    if scope != GateScope::Full {
        return;
    }
    assert_sometimes!(
        stats.eio_landed,
        "storage: EIO was reported but the write landed"
    );
    assert_sometimes!(
        stats.eio_lost,
        "storage: EIO was reported and the write was lost"
    );
    assert_sometimes!(
        stats.fsync_durable,
        "storage: fsync failed but the batch was durable anyway"
    );
    assert_sometimes!(
        stats.fsync_lost,
        "storage: fsync failed and the batch was genuinely lost"
    );
    // The recoverable Stage-7 legs live on the main campaign: a torn tail is
    // discarded at the next boot and the node converges anyway; a lone bad
    // metainfo copy is repaired from its twin.
    let audit = audit_world(handle);
    let facts = audit.corruption_facts();
    assert_sometimes!(
        facts.tail_discarded,
        "storage: a torn tail is discarded at boot and the node recovers"
    );
    assert_sometimes!(
        facts.metainfo_repaired,
        "storage: a metainfo copy is repaired at boot"
    );
}

/// Per-family injected/detected facts folded from the world's corruption
/// ledger (issue #20 item F). `detected` distinguishes exercised faults from
/// dormant ones, so the oracle never overclaims.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // Independent sticky per-family facts.
pub(crate) struct CorruptionStats {
    pub(crate) injected: usize,
    pub(crate) detected: usize,
    pub(crate) bitflip: bool,
    pub(crate) read_eio: bool,
    pub(crate) lost_write: bool,
    pub(crate) misdirected: bool,
    pub(crate) identifier: bool,
    pub(crate) block_run: bool,
    pub(crate) torn_tail: bool,
    pub(crate) snapshot: bool,
    pub(crate) ledger: bool,
    pub(crate) metainfo_copy: bool,
    pub(crate) metainfo_both: bool,
    pub(crate) metadata: bool,
}

/// Fold the corruption ledger's ground truth (empty world = no injections).
pub(crate) fn corruption_stats(handle: &StateHandle) -> CorruptionStats {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut stats = CorruptionStats {
        injected: guard.corruptions.len(),
        ..CorruptionStats::default()
    };
    for injection in &guard.corruptions {
        if !injection.detected {
            continue;
        }
        stats.detected += 1;
        match injection.family {
            CorruptionFamily::BitFlip => stats.bitflip = true,
            CorruptionFamily::ReadEio => stats.read_eio = true,
            CorruptionFamily::LostWrite => stats.lost_write = true,
            CorruptionFamily::Misdirected => stats.misdirected = true,
            CorruptionFamily::Identifier => stats.identifier = true,
            CorruptionFamily::BlockRun => stats.block_run = true,
            CorruptionFamily::TornTail => stats.torn_tail = true,
            CorruptionFamily::Snapshot => stats.snapshot = true,
            CorruptionFamily::Ledger => stats.ledger = true,
            CorruptionFamily::MetainfoCopy => stats.metainfo_copy = true,
            CorruptionFamily::MetainfoBoth => stats.metainfo_both = true,
            CorruptionFamily::Metadata => stats.metadata = true,
        }
    }
    stats
}

/// The corruption axis's coverage gates (issue #20 items D/F), recorded once
/// per run from that axis's workload `check()`: one gate per injected fault
/// family (each must be injected AND read back — detection totality itself is
/// asserted at the read sites), one per disentanglement verdict, and the
/// fail-stop outcome. The "zero silent bad reads" safety half is an `always`
/// at the boot-view seed point, not here.
pub(crate) fn check_corruption_gates(handle: &StateHandle) {
    let stats = corruption_stats(handle);
    let facts = audit_world(handle).corruption_facts();
    assert_sometimes!(
        stats.injected > 0,
        "storage: a corruption fault is injected"
    );
    assert_sometimes!(
        stats.detected > 0,
        "storage: an injected corruption is read back and detected"
    );
    assert_sometimes!(
        stats.bitflip,
        "storage: a bit-flip is injected and detected"
    );
    assert_sometimes!(
        stats.read_eio,
        "storage: a read EIO is injected and detected"
    );
    assert_sometimes!(
        stats.lost_write,
        "storage: a lost write is injected and detected"
    );
    assert_sometimes!(
        stats.misdirected,
        "storage: a misdirected write is injected and detected"
    );
    assert_sometimes!(
        stats.identifier,
        "storage: an identifier fault is injected and detected"
    );
    assert_sometimes!(
        stats.block_run,
        "storage: a block fault is injected and detected"
    );
    assert_sometimes!(
        stats.torn_tail,
        "storage: a torn tail is injected and detected"
    );
    assert_sometimes!(
        stats.snapshot,
        "storage: a snapshot corruption is injected and detected"
    );
    assert_sometimes!(
        stats.ledger,
        "storage: a ledger corruption is injected and detected"
    );
    assert_sometimes!(
        stats.metainfo_copy,
        "storage: a metainfo copy fault is injected and detected"
    );
    assert_sometimes!(
        stats.metainfo_both,
        "storage: a metainfo double fault is injected and detected"
    );
    assert_sometimes!(
        stats.metadata,
        "storage: an fs-metadata fault is injected and detected"
    );
    // Per-verdict outcomes, folded from the audit's typed stream.
    assert_sometimes!(
        facts.tail_discarded,
        "storage: the crash-truncatable-tail verdict is exercised"
    );
    assert_sometimes!(
        facts.corrupted_crash,
        "storage: the corrupted verdict crashes a node"
    );
    assert_sometimes!(
        facts.undecidable_crash,
        "storage: the undecidable verdict crashes a node"
    );
    assert_sometimes!(
        facts.metadata_crash,
        "storage: an fs-metadata fault crashed a node"
    );
    assert_sometimes!(
        facts.metainfo_repaired,
        "storage: a metainfo repair avoids a crash"
    );
}
