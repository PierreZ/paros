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

use crate::audit::{AuditWorld, NodeAudit, audit_world};
use crate::chain::{AppliedTransition, ChainState, hash_text};
use paros::{
    Ballot, ClientId, ClientSeq, Command, Config, ConfigId, CorruptionVerdict, DriverHooks,
    DriverTunables, HandoffContext, HardState, IntegrityFault, MemStorage, Message, MetadataFault,
    MustSync, NodeId, NodeStorage, RecoveryCase, RunError, SNAP_CHUNK_BYTES, Seam, SessionEntry,
    Slot, SlotRecord, Storage, StorageError, StorageRecord, WitnessStatus, WriteOutcome,
    classify_log, command_hash, parse_addr, run_node, snap_chunk_count,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";

/// **Default** per-call firing probability of the write-`EIO` BUGGIFY site (one
/// location, per-seed activation × per-call firing; the record identity travels
/// on the typed error, not on the location). The rate itself is a knob — see
/// [`WritePathRates`], which draws it per node per seed.
const P_WRITE_EIO: f64 = PCT_WRITE_EIO as f64 / 100.0;
/// [`P_WRITE_EIO`] as the integer percentage its knob draws in (see
/// [`WritePathRates`]); the two must not drift, so the probability is derived
/// from this rather than written twice.
const PCT_WRITE_EIO: u8 = 1;
/// **Default** per-call firing probability of the fsync-failure BUGGIFY site.
/// Independent from the write site — the sweep must be able to select the two
/// failure modes separately (same rule as the driver's two durability seams) —
/// and its rate is an independent knob too ([`WritePathRates`]).
const P_FSYNC_FAIL: f64 = PCT_FSYNC_FAIL as f64 / 100.0;
/// [`P_FSYNC_FAIL`] as the integer percentage its knob draws in.
const PCT_FSYNC_FAIL: u8 = 1;
/// **Default** per-call firing probability of the **forced torn tail** BUGGIFY
/// site (its rate is a knob: [`WritePathRates`]): its
/// own location, consulted on a `Sync` whose stage holds fresh appends, that
/// takes the fsync site's *lost* leg with the torn coin already decided. The
/// torn-tail shape ("storage: a crash-truncatable tail is discarded on boot")
/// is otherwise the compound of four coins — the fsync site firing at
/// [`P_FSYNC_FAIL`], its lost leg, [`P_TORN_TAIL`], and fresh appends being
/// staged at that moment — which reached only ~1–2% of raw seeds; a
/// coverage-guided schedule clustered on a few roots starved it for a
/// thousand iterations on one CI build. Per BUGGIFY doctrine the
/// rare-but-valid shape gets a location that makes it *likely* on the seeds
/// that activate it, instead of waiting for the swarm to stumble into it.
/// The fault it injects is the ordinary fsync loss (same ledger entry, same
/// budget check, same crash decision by the driver), so every downstream
/// invariant sees exactly what the unforced leg produces.
const P_FORCE_TORN_TAIL: f64 = PCT_FORCE_TORN_TAIL as f64 / 100.0;
/// [`P_FORCE_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_FORCE_TORN_TAIL: u8 = 5;
/// Coin on the fsync *lost* leg: the crash tore the batch instead of losing
/// it whole — a prefix of the staged fresh appends reaches disk without
/// identifiers (Stage 7's per-record torn durability; the `CrashTail` leg of
/// the disentanglement table). A plain seeded coin like the fsyncgate
/// `persisted` decision, NOT its own BUGGIFY location: the *location* is the
/// fsync failure; whole-loss vs torn is the world's outcome-shaping of that
/// one fault, and per-seed location activation must not suppress the torn
/// flavor (the whole-loss leg is already the clean-crash model's default).
/// Its *value* is still a knob ([`WritePathRates`]) — a knob's un-activated
/// draw is the default, so shaping the coin per seed cannot suppress a leg the
/// way gating the coin behind an activation would.
const P_TORN_TAIL: f64 = PCT_TORN_TAIL as f64 / 100.0;
/// [`P_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_TORN_TAIL: u8 = 75;
/// Per-boot firing probabilities of the Stage-7 rot BUGGIFY sites — each fault
/// family its own independent location (per-seed activation × per-boot
/// firing), modelling latent faults that surfaced while the node was down and
/// are read back by the boot scan that immediately follows.
const P_ENTRY_ROT: f64 = 0.06;
const P_LOST_WRITE: f64 = 0.04;
const P_MISDIRECT: f64 = 0.04;
const P_SNAPSHOT_ROT: f64 = 0.05;
const P_PROMISE_ROT: f64 = 0.04;
const P_META_FAULT: f64 = 0.03;
const P_READ_EIO: f64 = 0.05;
/// Per-boot chunk rot on the retained decided snapshot point (#101): one
/// chunk's bytes fail their checksum while the point's identity survives —
/// the recoverable class the driver's chunk-repair layer pulls from peers.
const P_SNAP_CHUNK_ROT: f64 = 0.05;
/// Cap on the crash-truncatable window at boot: the maximum concurrently
/// in-flight accept writes one torn batch can leave unwitnessed (a `Ready`
/// batch is the driver's flush unit, and its accept count is bounded by the
/// leader's recovery page size).
///
/// **Deliberately not a knob.** It is not a tunable that shapes a run but a
/// bound the classifier's correctness depends on: it must equal the driver's
/// real maximum unwitnessed in-flight window. Widen it and the boot scan may
/// discard as "crash tail" a record that *was* acknowledged — data loss the
/// scan invents. Narrow it and legitimate crash tails are classified as
/// corruption and park otherwise healthy nodes. Neither extreme is a valid
/// configuration, so this constant tracks [`paros::LEADER_RECOVERY_BATCH`]
/// instead of being drawn — and is *defined* as that constant so the two
/// cannot drift.
const MAX_TORN_TAIL: usize = paros::LEADER_RECOVERY_BATCH;

/// A paros node in the simulation.
pub struct NodeProcess;

/// One inert topology member that keeps the simulator lifecycle open while a
/// workload drives something else (the storage contract suite).
pub(crate) struct IdleProcess;

#[async_trait]
impl Process for IdleProcess {
    fn name(&self) -> &'static str {
        "paros-idle"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}

/// A paros node for the **#113 evaluation corpus**: no BUGGIFY perturbation and
/// no swarm fault sites (every fault is a scripted, targeted injection from the
/// corpus workload), the per-record budget lifted (masks may exceed it, and the
/// world records the unrecoverable ground truth the analytic derivation
/// cross-checks), and a **scripted lifecycle** — the workload deterministically
/// restarts, holds, and releases each node through the world's restart epochs.
pub(crate) struct CorpusNodeProcess;

#[async_trait]
impl Process for CorpusNodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        scripted_corpus_loop(ctx).await
    }
}

/// The corpus node loop: [`NodeProcess`]'s recovery loop with the swarm fault
/// sites dark and the crash/restart schedule owned by the corpus workload
/// (via [`StorageWorld::restart_epochs`] / [`StorageWorld::held`]) instead of
/// moonpool attrition. Dropping a `run_node` incarnation mid-await is a
/// faithful clean crash: the staged, un-synced writes die with the handle and
/// the incarnation guard cancels its spawned tasks.
// One recovery loop with per-exit-kind handling, mirroring `NodeProcess::run`:
// splitting the arms would scatter the crash/hold/restart contract it *is*.
#[allow(clippy::too_many_lines)]
async fn scripted_corpus_loop(ctx: &SimContext) -> SimulationResult<()> {
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

    let world = storage_world(ctx.state());
    {
        let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
        guard.set_cluster_size(members.len());
        // The corpus's declared mode: masks may exceed the per-record budget,
        // and every injection records the unrecoverable ground truth.
        guard.unbudgeted = true;
    }
    // Every fault on this axis is a scripted injection: the swarm sites and
    // the driver's BUGGIFY hooks stay dark so a case replays as choreographed.
    let faults = StorageFaults::new(ctx.time().clone(), Duration::ZERO, false);
    let hooks = BuggifyHooks::new(ctx.time().clone(), Duration::ZERO, false);
    let checker = audit_world(ctx.state());
    let audit = NodeAudit::new(ctx.time().clone(), checker.clone());

    loop {
        // Hold gate: a held node stays down between incarnations until the
        // workload releases it. Terminal parking keeps its ordinary contract.
        loop {
            let (held, parked) = {
                let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                (guard.held.contains(&my_ip), guard.parked.contains(&my_ip))
            };
            if parked {
                checker.note_storage_dead(self_rank.0);
                tracing::info!(node = self_rank.0, "storage_parked");
                return Ok(());
            }
            if !held {
                break;
            }
            if ctx.shutdown().is_cancelled() {
                return Ok(());
            }
            ctx.time().sleep(Duration::from_millis(10)).await.ok();
        }
        let boot_epoch = {
            world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .restart_epochs
                .get(&my_ip)
                .copied()
                .unwrap_or(0)
        };
        let storage = DurableStorage::restore(
            config.clone(),
            Arc::downgrade(&world),
            my_ip.clone(),
            self_rank.0,
            faults.clone(),
            checker.clone(),
        );
        let restart = scripted_restart_signal(ctx, &world, &my_ip, boot_epoch);
        let exited = moonpool_sim::select! {
            result = run_node(
                ctx.providers().clone(),
                storage,
                parse_addr(&my_ip)?,
                members.clone(),
                // Scripted corpus runs keep the production transport shape:
                // every perturbation on this axis is a targeted injection.
                DriverTunables::default(),
                ctx.shutdown().clone(),
                &hooks,
                &audit,
            ) => Some(result),
            () = restart => None,
        };
        match exited {
            // Scripted restart: the incarnation was dropped mid-await — a
            // clean crash — and the next loop pass re-restores (or waits held).
            None => {
                tracing::info!(node = self_rank.0, "corpus_scripted_restart");
            }
            Some(Err(RunError::SeamCrash(_))) => {}
            Some(Err(RunError::Storage(_))) => {
                let parked = world
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .parked
                    .contains(&my_ip);
                if parked {
                    checker.note_storage_dead(self_rank.0);
                    tracing::info!(node = self_rank.0, "storage_parked");
                    return Ok(());
                }
            }
            Some(Err(RunError::Infra(e))) => return Err(e),
            Some(Ok(())) => return Ok(()),
        }
    }
}

/// Resolve when the workload bumps `ip`'s restart epoch past `boot_epoch`.
/// Provider-time polling keeps the wake-up deterministic per seed.
async fn scripted_restart_signal(
    ctx: &SimContext,
    world: &Arc<Mutex<StorageWorld>>,
    ip: &str,
    boot_epoch: u64,
) {
    loop {
        let current = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .restart_epochs
            .get(ip)
            .copied()
            .unwrap_or(0);
        if current > boot_epoch {
            return;
        }
        if ctx.time().sleep(Duration::from_millis(10)).await.is_err() {
            // Sleep only fails on teardown; let the run_node arm observe the
            // shutdown instead of spinning.
            std::future::pending::<()>().await;
        }
    }
}

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    // One recovery loop with per-exit-kind handling; splitting the arms would
    // scatter the crash/park/restart contract this function *is*.
    #[allow(clippy::too_many_lines)]
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
        let hooks = BuggifyHooks::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            true,
        );
        // The budgeted storage-fault layer (issue #19 B/C) shares the driver
        // hooks' chaos window: after the cutoff the world stops injecting
        // **new** faults but never heals the consequences of old ones —
        // recovery through the tail must be genuine.
        let faults = StorageFaults::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            true,
        );
        // The per-iteration shared audit: pure observation, published beside the
        // storage world so every node folds its transitions into one incremental
        // checker. It never influences the driver — that is `hooks`' job.
        let checker = audit_world(ctx.state());
        let audit = NodeAudit::new(ctx.time().clone(), checker.clone());

        // Driver transport tunables — born workload-buggified (prong 2): the
        // defaults are production's constants, and an activated seed draws an
        // extreme. A handful-sized peer queue makes mailbox overflow (the
        // `dropped_at_mailbox` audit path) likely — a leader recovery page
        // bursts up to 64 Accepts into it at once — while the extreme's floor
        // stays at 4 so one tick's steady-state traffic (heartbeat ack +
        // catch-up request + snap ack + an accepted) still fits: a queue that
        // cannot hold one tick's worth deterministically starves whichever
        // class is enqueued last *every* tick, which defeats eventual
        // synchrony outright (witness seed 8560136109856440322: a capacity-1
        // queue held each beat's heartbeat ack, so every catch-up request of
        // a 62-second tail was dropped and the node wedged below a chosen
        // gap). A one-message delivery batch maximizes framing pressure. Two
        // independent knob locations; drawn once per node per seed, stable
        // across this node's restarts.
        let tunables = {
            let defaults = DriverTunables::default();
            let peer_queue_capacity =
                buggify_knob!(defaults.peer_queue_capacity, 4_usize..17_usize);
            // The batch extreme's floor keeps the per-peer throughput ceiling
            // (~batch / delivery round trip, and an in-sim round trip can
            // approach a whole tick under load) above the protocol's
            // steady-state per-peer rate: a one-message batch capped delivery
            // near 20 msg/s for the entire run — below what a leader's beat +
            // accepts + commits need — which is a permanent partition in
            // disguise, and 7/500 seeds wedged without ever converging
            // (witness seed 4877033065878342564: an n=2 cluster that never
            // chose a single slot in 67 s). Eight-to-32 still shrinks frames
            // 2-8x against the default 64 without making the run unwinnable.
            let delivery_batch = buggify_knob!(defaults.delivery_batch, 8_usize..33_usize);
            if peer_queue_capacity != defaults.peer_queue_capacity {
                // BUGGIFY pairing: the capacity extreme genuinely runs.
                assert_reachable!("a node runs with an extreme peer-queue capacity");
            }
            if delivery_batch != defaults.delivery_batch {
                // BUGGIFY pairing: the delivery-batch extreme genuinely runs.
                assert_reachable!("a node runs with an extreme delivery batch");
            }
            DriverTunables {
                peer_queue_capacity,
                delivery_batch,
            }
        };

        // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
        // drop the volatile node, rebuild storage from the (surviving) world, and
        // re-run — a faithful clean crash + recovery. Attrition (process kill) is
        // handled by the harness; this covers the seams *inside* a Ready batch
        // that attrition cannot reach.
        loop {
            // A node terminally parked by a detected persistent corruption
            // stays down: attrition may revive the *process*, but the boot
            // scan would re-detect the same rotted record forever, and a
            // second crash report for one detection would break the 1:1
            // injected⇔detected correlation. Exit before touching the store.
            if world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .parked
                .contains(&my_ip)
            {
                checker.note_storage_dead(self_rank.0);
                tracing::info!(node = self_rank.0, "storage_parked");
                return Ok(());
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
                tunables,
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
                Err(RunError::Storage(_)) => {
                    // Stage 7's baseline for a *persistent* detected fault —
                    // a rotted record or an FS-metadata fault — is detect ⇒
                    // crash, and restarting cannot help: the boot scan would
                    // re-detect the same record forever. The node stays down
                    // for the run (the availability disaster the CTRL paper
                    // measures; Stage 8 buys it back), bounded by the world's
                    // dead-node budget so the cluster keeps a live quorum. The
                    // audit is told so convergence excuses exactly these
                    // nodes, and only these.
                    let parked = world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .parked
                        .contains(&my_ip);
                    if parked {
                        assert_reachable!("storage: a corruption-crashed node stays down");
                        checker.note_storage_dead(self_rank.0);
                        tracing::info!(node = self_rank.0, "storage_parked");
                        return Ok(());
                    }
                    assert_reachable!("a storage-fault crash recovers through the restart path");
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        // BUGGIFY pairing: the storage-crash restart-delay knob
                        // fired (the seam-crash twin above has its own gate).
                        assert_reachable!("a storage-fault crash restarts after a buggified delay");
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

/// The **contract-suite workload** (issue #21 item F): runs the shared
/// [`paros::storage_contract_suite`] against the world-backed [`DurableStorage`]
/// inside one quiet simulation iteration, so the sim's storage fake can never
/// drift from the trait contract [`paros::MemStorage`] pins. Faults are off —
/// the suite drives the clean path both implementations must share; the budget
/// logic stays outside the contract (#70).
pub(crate) struct ContractSuiteWorkload;

#[async_trait]
impl moonpool_sim::Workload for ContractSuiteWorkload {
    fn name(&self) -> &'static str {
        "storage-contract-suite"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let world = storage_world(ctx.state());
        world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_cluster_size(1);
        let checker = audit_world(ctx.state());
        let faults = StorageFaults::new(ctx.time().clone(), Duration::ZERO, false);
        let config = Config {
            id: NodeId(0),
            peers: vec![NodeId(0)],
            ..Config::default()
        };
        let mut instance = 0_u64;
        let fresh = || {
            instance += 1;
            DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                format!("10.9.9.{instance}"),
                100 + instance,
                faults.clone(),
                checker.clone(),
            )
        };
        // A reopen is a clean reboot of the same store: drop the handle and
        // re-restore from the world's durable records under the same key.
        let reopen = |old: DurableStorage<_>| {
            let (key, node_id) = (old.key.clone(), old.node_id);
            drop(old);
            DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                key,
                node_id,
                faults.clone(),
                checker.clone(),
            )
        };
        paros::storage_contract_suite(fresh, reopen);
        Ok(())
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

/// Semantic health of one durable record — the world stores **records, not
/// bytes** (#20 fixed decision), so every corruption-family member is modeled
/// as a first-class read outcome the boot scan classifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum RecordHealth {
    /// The record verifies.
    #[default]
    Clean,
    /// The bytes fail their checksum (bit flip, latent sector error, torn
    /// write).
    Faulty,
    /// The checksum passes but the identity inside the checksummed region
    /// names a different record (misdirected write).
    Misdirected,
    /// The bytes are absent where the identifier / reserved-record contract
    /// says they must exist (lost write).
    Lost,
}

impl RecordHealth {
    fn integrity_fault(self) -> Option<IntegrityFault> {
        match self {
            RecordHealth::Clean => None,
            RecordHealth::Faulty => Some(IntegrityFault::ChecksumMismatch),
            RecordHealth::Misdirected => Some(IntegrityFault::Misdirected),
            RecordHealth::Lost => Some(IntegrityFault::LostWrite),
        }
    }
}

/// One accepted entry's record + persist-witness health.
#[derive(Clone, Copy, Debug)]
struct SlotHealth {
    entry: RecordHealth,
    id: WitnessStatus,
}

impl Default for SlotHealth {
    fn default() -> Self {
        Self {
            entry: RecordHealth::Clean,
            id: WitnessStatus::Present,
        }
    }
}

impl SlotHealth {
    fn clean(self) -> bool {
        self.entry == RecordHealth::Clean && self.id == WitnessStatus::Present
    }
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
    /// Health of the accepted-entry records; a slot absent from this map is
    /// clean and witnessed.
    entry_health: BTreeMap<Slot, SlotHealth>,
    /// The two checksummed `HardState` copies (CTRL metainfo doctrine): one
    /// bad ⇒ repair from the twin, both bad ⇒ crash.
    promise_health: [RecordHealth; 2],
    chosen_health: RecordHealth,
    truncation_health: RecordHealth,
    snapshot_health: RecordHealth,
    /// A file-granularity FS-metadata fault on the whole store.
    meta_fault: Option<MetadataFault>,
    /// One pending transient read-`EIO` target, cleared when it surfaces (the
    /// retry — the next boot — reads clean).
    read_eio: Option<StorageRecord>,
    /// The latest **decided snapshot point** (#101): the `Snap` marker's slot
    /// and the byte-identical boundary state every node captured there. Only
    /// the latest point is retained.
    snap_point: Option<(u64, ChainState)>,
    /// Per-chunk health of the retained point's blob (fixed
    /// [`SNAP_CHUNK_BYTES`] chunking of the encoded state).
    snap_chunk_health: Vec<RecordHealth>,
}

impl NodeDisk {
    /// Health of the accepted record at `slot` (clean + witnessed when
    /// untracked).
    fn slot_health(&self, slot: Slot) -> SlotHealth {
        self.entry_health.get(&slot).copied().unwrap_or_default()
    }
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

/// One Stage-7 corruption injection — the family it models (issue #20 item D;
/// each kind is its own independent BUGGIFY location).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptionKind {
    /// Bit flip / latent sector error on a persisted record.
    BitFlip,
    /// A write acknowledged clean that never reached the medium.
    LostWrite,
    /// A block-aligned write landing on the wrong record.
    Misdirected,
    /// A transient `EIO` on the read path.
    ReadEio,
    /// A torn un-synced batch: fresh appends durable without identifiers.
    TornTail,
    /// One (or both) `HardState` copies rotted.
    PromiseCopy,
    /// A file-granularity FS-metadata fault.
    Metadata,
}

/// What became of one Stage-7 injection — the exercised-vs-dormant tracking
/// the injected⇔detected oracle folds (an injected fault on a never-read
/// record is not a detection failure).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptionOutcome {
    /// Injected but not read back yet (legal only on a node that parked
    /// before the read, or a torn tail the run ended before rebooting).
    Dormant,
    /// Read, detected, and surfaced as the driver's typed crash decision.
    Crashed,
    /// Detected in the same scan as a [`Crashed`](CorruptionOutcome::Crashed)
    /// record and covered by that one crash decision (a block fault's other
    /// members, the second promise copy).
    CoDetected,
    /// Read, detected, and repaired from the twin copy (`HardState` only).
    Repaired,
    /// Read, classified crash-truncatable, and discarded locally (the one
    /// legal discard: the record was never acknowledged to anyone).
    DiscardedTail,
    /// Stage 8: read, classified recoverable — identity known, value lost —
    /// and **reported** into the protocol's tri-state instead of crashing.
    /// The record stays faulty on disk until a clean re-write resolves it to
    /// [`Recovered`](CorruptionOutcome::Recovered) (or truncation supersedes
    /// it); a run may legally end with a report still standing (the WAITED
    /// leg, or a run that simply ended first).
    Reported,
    /// Stage 8: the faulty record was genuinely re-written by a clean flush
    /// (an in-place repair — a re-sent `Accept`, a learned chosen value, a
    /// decided no-op) or superseded by truncation/snapshot custodianship.
    Recovered,
}

/// Ground truth of one Stage-7 corruption injection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CorruptionInjection {
    pub(crate) node: u64,
    pub(crate) record: StorageRecord,
    pub(crate) kind: CorruptionKind,
    /// Part of a multi-record block fault (a contiguous run of entries).
    pub(crate) block: bool,
    pub(crate) outcome: CorruptionOutcome,
}

/// Sticky per-family / per-verdict facts for the Stage-7 coverage gates,
/// recorded at the detection instant and read once per run by
/// [`check_storage_gates`]. Independent bits, not a state machine (the
/// [`crate::audit`] flag-set waiver).
#[derive(Default, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct Stage7Flags {
    bitflip_detected: bool,
    lost_write_detected: bool,
    misdirected_detected: bool,
    read_eio_detected: bool,
    torn_tail_discarded: bool,
    snapshot_detected: bool,
    promise_repaired: bool,
    metadata_crashed: bool,
    corruption_below_tail: bool,
    last_entry_ambiguity: bool,
    identifier_lost: bool,
    // --- Stage 8 (issue #21) sticky facts -----------------------------------
    /// A recoverable rotted record was reported into the tri-state.
    faulty_reported: bool,
    /// A reported record was genuinely re-written by a clean flush.
    record_recovered: bool,
    /// A corrupted application snapshot was reset for local log replay
    /// (floor = 0: CTRL's cheap path).
    snapshot_reset_local: bool,
    /// A corrupted application snapshot was reset under a truncated log
    /// (recovery requires a peer's `InstallSnapshot`).
    snapshot_reset_remote: bool,
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
    /// write was injected-lost, torn, or corrupted, and not yet re-written by
    /// a clean flush (or discarded as a crash tail). These are what the
    /// budget counts.
    marks: BTreeMap<String, BTreeSet<u64>>,
    /// Ground truth of every Stage-7 corruption injection, in order.
    corruptions: Vec<CorruptionInjection>,
    /// Nodes terminally crashed by a detected persistent fault (detect ⇒
    /// crash; restarting cannot help a store whose record genuinely rotted).
    /// Bounded by [`StorageWorld::dead_budget`] so a live quorum survives.
    parked: BTreeSet<String>,
    /// The same set by numeric node id, for correlating the injection ledger.
    parked_ids: BTreeSet<u64>,
    /// Sticky Stage-7 gate facts.
    s7: Stage7Flags,
    /// Unbudgeted mode (the scripted corpus): a targeted injection may take
    /// every copy of a record; each slot driven to zero readable copies is
    /// recorded in `unrecoverable`, the ground truth the corpus's analytic
    /// derivation is cross-checked against.
    unbudgeted: bool,
    /// Slots with no readable copy anywhere — no clean log record on any node
    /// and no post-truncation snapshot covering them (only ever populated in
    /// unbudgeted runs).
    unrecoverable: BTreeSet<u64>,
    /// Scripted lifecycle (the #113 corpus): per-node restart epochs. Bumping
    /// one makes the corpus node loop drop its current incarnation — a clean
    /// crash, staged un-synced writes dying with it — and re-restore from the
    /// world. A paros-side stand-in for the explicit scripted-lifecycle API
    /// tracked upstream as moonpool#182.
    restart_epochs: BTreeMap<String, u64>,
    /// Scripted lifecycle: nodes held down between incarnations until the
    /// corpus workload releases them.
    held: BTreeSet<String>,
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

    /// Clean live copies of the accepted-log record at `slot`: cluster members
    /// that are neither fault-marked for it, truncated past it, nor terminally
    /// parked by a detected corruption. A node the world has never seen a
    /// flush from is a clean potential copy.
    fn clean_copies(&self, slot: u64) -> usize {
        let mut unclean: BTreeSet<&String> = BTreeSet::new();
        for (node, marks) in &self.marks {
            if marks.contains(&slot) {
                unclean.insert(node);
            }
        }
        for (node, disk) in &self.disks {
            if disk.first_slot.0 > slot {
                unclean.insert(node);
            }
        }
        for node in &self.parked {
            unclean.insert(node);
        }
        self.cluster_size.saturating_sub(unclean.len())
    }

    /// How many nodes a run may terminally lose to detected corruption while
    /// keeping a live quorum.
    fn dead_budget(&self) -> usize {
        self.cluster_size.saturating_sub(self.quorum())
    }

    /// Whether terminally parking `node_key` (detect ⇒ crash, stays down) is
    /// inside the dead-node budget AND leaves every accepted record the node
    /// still holds with a clean quorum of live copies elsewhere. Checked
    /// *before* a persistent-corruption injection, so the availability cost is
    /// paid only where the cluster can absorb it.
    fn may_park(&self, node_key: &str) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.parked.contains(node_key) {
            return true;
        }
        if self.parked.len() + 1 > self.dead_budget() {
            return false;
        }
        let quorum = self.quorum();
        if let Some(disk) = self.disks.get(node_key) {
            for slot in disk.accepted.keys() {
                let already_unclean = self
                    .marks
                    .get(node_key)
                    .is_some_and(|marks| marks.contains(&slot.0));
                let hypothetical = usize::from(!already_unclean);
                if self.clean_copies(slot.0).saturating_sub(hypothetical) < quorum {
                    return false;
                }
            }
        }
        // The accepted-map walk above misses slots this node no longer holds
        // (truncated past) or never flushed — but the availability
        // re-derivation counts *every* parked node unclean for *every* marked
        // slot (a dead node serves neither the record nor its superseding
        // snapshot). Close the composition hole: for each slot marked faulty
        // anywhere in the cluster, parking this node must still leave that
        // slot its clean quorum under the re-derivation's own formula.
        for slot in self.marks.values().flatten() {
            let mut unclean: BTreeSet<&str> = self
                .marks
                .iter()
                .filter(|(_, marks)| marks.contains(slot))
                .map(|(node, _)| node.as_str())
                .chain(self.parked.iter().map(String::as_str))
                .collect();
            unclean.insert(node_key);
            if self.cluster_size.saturating_sub(unclean.len()) < quorum {
                return false;
            }
        }
        true
    }

    /// Terminally park a node: detect ⇒ crash, and it stays down.
    fn park(&mut self, key: &str, node: u64) {
        self.parked.insert(key.to_string());
        self.parked_ids.insert(node);
    }

    /// Whether a **recoverable** corruption of the accepted record at `slot`
    /// on `node_key` is inside the per-record budget: the record must keep a
    /// clean quorum of live copies (#70's rule, re-counted over live copies at
    /// injection time). The unbudgeted corpus lifts the bound, and
    /// [`StorageWorld::note_if_unrecoverable`] records the ground truth its
    /// analytic derivation is checked against.
    fn may_corrupt_record(&self, node_key: &str, slot: u64) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.unbudgeted {
            return true;
        }
        let already = self
            .marks
            .get(node_key)
            .is_some_and(|marks| marks.contains(&slot));
        self.clean_copies(slot)
            .saturating_sub(usize::from(!already))
            >= self.quorum()
    }

    /// Whether `slot` has **no readable copy anywhere**: every disk that holds
    /// its accepted record holds it unhealthy or sits on a parked node, no
    /// disk truncated past it with a healthy snapshot (the snapshot covers the
    /// folded prefix), no disk retains a fully clean decided snapshot point
    /// covering it (#101), and no disk holds it clean. Ground truth for the
    /// corpus.
    fn slot_unrecoverable(&self, slot: u64) -> bool {
        for (key, disk) in &self.disks {
            if self.parked.contains(key) {
                continue;
            }
            if disk.first_slot.0 > slot && disk.snapshot_health == RecordHealth::Clean {
                return false;
            }
            if disk.accepted.contains_key(&Slot(slot)) && disk.slot_health(Slot(slot)).clean() {
                return false;
            }
        }
        // #101: a decided snapshot point at or past the slot is custody too —
        // and because the point is byte-identical cluster-wide, chunk repair
        // reassembles it from *any* clean copy of each chunk, so the clause is
        // cluster-assembled, never per-disk: the point is readable unless some
        // chunk has no clean copy on any live holder.
        let points: BTreeSet<u64> = self
            .disks
            .iter()
            .filter(|(key, _)| !self.parked.contains(*key))
            .filter_map(|(_, disk)| disk.snap_point.map(|(at, _)| at))
            .filter(|at| *at >= slot)
            .collect();
        for at in points {
            let chunk_count = self
                .disks
                .values()
                .find_map(|disk| {
                    disk.snap_point
                        .filter(|(point, _)| *point == at)
                        .map(|(_, state)| snap_chunk_count(state.encode().len()))
                })
                .unwrap_or(0);
            let assemblable = (0..chunk_count).all(|chunk| {
                self.disks.iter().any(|(key, disk)| {
                    !self.parked.contains(key)
                        && disk.snap_point.is_some_and(|(point, _)| point == at)
                        && disk
                            .snap_chunk_health
                            .get(usize::try_from(chunk).unwrap_or(usize::MAX))
                            .is_none_or(|health| *health == RecordHealth::Clean)
                })
            });
            if assemblable {
                return false;
            }
        }
        true
    }

    /// After an unbudgeted injection touched `slot`, record it unrecoverable if
    /// no readable copy survives.
    fn note_if_unrecoverable(&mut self, slot: u64) {
        if self.unbudgeted && !self.unrecoverable.contains(&slot) && self.slot_unrecoverable(slot) {
            self.unrecoverable.insert(slot);
            tracing::info!(slot, "slot_unrecoverable");
        }
    }

    /// A clean flush (or truncation custodianship) genuinely resolved the
    /// standing report on `(node, record)`.
    fn note_recovered(&mut self, node: u64, record: StorageRecord) {
        for injection in &mut self.corruptions {
            if injection.node == node
                && injection.record == record
                && matches!(
                    injection.outcome,
                    CorruptionOutcome::Dormant | CorruptionOutcome::Reported
                )
            {
                injection.outcome = CorruptionOutcome::Recovered;
                self.s7.record_recovered = true;
            }
        }
    }

    /// Record one Stage-7 injection's ground truth.
    fn note_corruption(&mut self, injection: CorruptionInjection) {
        tracing::info!(
            node = injection.node,
            record = %injection.record,
            kind = ?injection.kind,
            block = injection.block,
            "corruption_injected"
        );
        self.corruptions.push(injection);
    }

    /// Resolve the dormant ledger entries matching `(node, record)` — the boot
    /// scan read them back. The first match takes `outcome`; when `outcome` is
    /// [`CorruptionOutcome::Crashed`], further matches co-resolve (one crash
    /// decision covers the whole scan).
    fn resolve_corruption(&mut self, node: u64, record: StorageRecord, outcome: CorruptionOutcome) {
        let mut first = true;
        for injection in &mut self.corruptions {
            if injection.node == node
                && injection.record == record
                && injection.outcome == CorruptionOutcome::Dormant
            {
                injection.outcome = if first {
                    outcome
                } else {
                    CorruptionOutcome::CoDetected
                };
                first = false;
                // A resolved member of a multi-record block fault proves the
                // contiguous-run family was genuinely read back and detected.
                // A reachable anchor, NOT a sometimes: the composition needs a
                // log still holding a contiguous clean run when the rot site
                // fires, and truncation-heavy runs keep logs short — CI's
                // coverage-guided schedule starved a per-sweep gate on it
                // (0/452 checks, run 33117691030) even after the sub-roll was
                // widened, while every other family gate fired. Per the
                // assertion doctrine, a leg the sweep is not *certain* to
                // reach anchors exploration when hit and never fails coverage.
                if injection.block {
                    assert_reachable!(
                        "storage: a block fault corrupts a contiguous run of entries"
                    );
                }
            }
        }
    }

    /// Targeted, budget-checked corruption of one durable record — issue
    /// #21's single-injection API (`corrupt(node, record)`): the adversarial
    /// promise-corruption test is one call. Returns whether the budget
    /// permitted it.
    #[allow(dead_code)] // armed for #21's targeted tests; the swarm sites drive it today
    pub(crate) fn corrupt(&mut self, node_key: &str, node: u64, record: StorageRecord) -> bool {
        if !self.may_park(node_key) {
            return false;
        }
        let Some(disk) = self.disks.get_mut(node_key) else {
            return false;
        };
        let kind = match record {
            StorageRecord::Accepted(slot) => {
                if !disk.accepted.contains_key(&slot) {
                    return false;
                }
                disk.entry_health.insert(
                    slot,
                    SlotHealth {
                        entry: RecordHealth::Faulty,
                        id: WitnessStatus::Present,
                    },
                );
                self.marks
                    .entry(node_key.to_string())
                    .or_default()
                    .insert(slot.0);
                CorruptionKind::BitFlip
            }
            StorageRecord::Promise => {
                disk.promise_health = [RecordHealth::Faulty, RecordHealth::Faulty];
                CorruptionKind::PromiseCopy
            }
            StorageRecord::ChosenIndex => {
                disk.chosen_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            StorageRecord::Truncation => {
                disk.truncation_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            StorageRecord::Snapshot => {
                disk.snapshot_health = RecordHealth::Faulty;
                CorruptionKind::BitFlip
            }
            _ => return false,
        };
        self.park(node_key, node);
        self.note_corruption(CorruptionInjection {
            node,
            record,
            kind,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
        true
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
}

/// Per-boot rot firing rates, one **independent knob location per fault
/// family** (AGENTS.md prong 2). The defaults are this module's documented
/// `P_*` constants; an activated seed multiplies one family's rate toward its
/// extreme.
///
/// **The floor is the cap plus the budget.** Each rate is clamped to 0.5, so a
/// boot can never rot *every* candidate record, and every family still passes
/// through [`StorageWorld::may_corrupt_record`]'s per-record clean-quorum
/// budget (or [`StorageWorld::may_park`]'s dead-node budget for the families
/// that crash), which is what keeps a live quorum readable. Density buys a
/// denser fault *window*, never a longer one: the sites are rolled only while
/// [`StorageFaults::active`] holds.
#[derive(Clone, Copy)]
struct RotRates {
    entry: f64,
    lost_write: f64,
    misdirect: f64,
    snapshot: f64,
    promise: f64,
    meta: f64,
    read_eio: f64,
    snap_chunk: f64,
}

impl RotRates {
    fn for_boot() -> Self {
        #[allow(clippy::cast_precision_loss)]
        let dense = |base: f64, multiplier: u64| (base * multiplier as f64).min(0.5);
        Self {
            entry: dense(P_ENTRY_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            lost_write: dense(P_LOST_WRITE, buggify_knob!(1_u64, 2_u64..6_u64)),
            misdirect: dense(P_MISDIRECT, buggify_knob!(1_u64, 2_u64..6_u64)),
            snapshot: dense(P_SNAPSHOT_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            promise: dense(P_PROMISE_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
            meta: dense(P_META_FAULT, buggify_knob!(1_u64, 2_u64..6_u64)),
            read_eio: dense(P_READ_EIO, buggify_knob!(1_u64, 2_u64..6_u64)),
            snap_chunk: dense(P_SNAP_CHUNK_ROT, buggify_knob!(1_u64, 2_u64..6_u64)),
        }
    }

    /// Whether any family drew above its default — the BUGGIFY pairing's
    /// condition.
    fn any_dense(self) -> bool {
        self.entry > P_ENTRY_ROT
            || self.lost_write > P_LOST_WRITE
            || self.misdirect > P_MISDIRECT
            || self.snapshot > P_SNAPSHOT_ROT
            || self.promise > P_PROMISE_ROT
            || self.meta > P_META_FAULT
            || self.read_eio > P_READ_EIO
            || self.snap_chunk > P_SNAP_CHUNK_ROT
    }
}

/// Roll the Stage-7 rot sites for one booting node: latent faults that
/// surfaced while it was down, injected at the boot that will immediately read
/// them back (the boot scan runs before anything else in `run_node`, with no
/// await in between, so injection → detection is atomic per boot). Each fault
/// family is its own independent BUGGIFY location; every *persistent* family
/// terminally parks the node (detect ⇒ crash, and restarting cannot help), so
/// each is gated on [`StorageWorld::may_park`]'s dead-node budget.
#[allow(clippy::too_many_lines)] // one flat block per independent BUGGIFY location
fn roll_boot_rot(world: &mut StorageWorld, key: &str, node: u64) {
    // Rot density is workload-buggified config (prong 2), and **one knob per
    // family**: each multiplies its own family's *firing* probability toward
    // the extreme, capped so a probability stays a probability. Per family
    // rather than one shared multiplier because per-seed activation has to
    // compose — a seed whose boots rot lost writes hard but flip no bits is a
    // different disk from one that does the reverse, and a single location can
    // only ever select "all families at once". Only the firing rates scale:
    // the per-record clean-quorum budget and the budget-off axis semantics are
    // untouched.
    let rates = RotRates::for_boot();
    if rates.any_dense() {
        // BUGGIFY pairing: a boot genuinely rolled at the dense extreme.
        assert_reachable!("storage: a boot rolls rot at buggified density");
    }
    let clean_slots = |world: &StorageWorld| -> Vec<Slot> {
        world.disks.get(key).map_or_else(Vec::new, |disk| {
            disk.accepted
                .keys()
                .filter(|slot| disk.slot_health(**slot).clean())
                .copied()
                .collect()
        })
    };
    // Pick a rot target: half the time the *last* retained slot, so the
    // proven-undecidable last-entry row of the disentanglement table is
    // genuinely visited, not just the interior corruption row.
    let pick = |slots: &[Slot]| -> Slot {
        if sim_random::<f64>() < 0.5 {
            slots[slots.len() - 1]
        } else {
            slots[usize::try_from(sim_random::<u64>()).unwrap_or(0) % slots.len()]
        }
    };
    let mark_entry = |world: &mut StorageWorld, slot: Slot, health: SlotHealth, kind, block| {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.entry_health.insert(slot, health);
        }
        world
            .marks
            .entry(key.to_string())
            .or_default()
            .insert(slot.0);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Accepted(slot),
            kind,
            block,
            outcome: CorruptionOutcome::Dormant,
        });
    };

    // Bit flip / latent sector error on one persisted entry — with sub-rolls
    // for a multi-record *block* fault (CTRL injects per FS block: a
    // contiguous run mismatches at once, and recovery must not assume faults
    // are singletons) and for the identifier rotting with its entry. Stage 8:
    // a record whose identity survives is **recoverable** — the node reports
    // it faulty and keeps running — so the gate is the per-record budget, not
    // the dead-node budget. Only the identifier-lost sub-case (unidentifiable
    // ⇒ crash) still needs to park, so it also needs the dead-node budget.
    if buggify_with_prob!(rates.entry) {
        let slots = clean_slots(world);
        let permitted: Vec<Slot> = slots
            .iter()
            .copied()
            .filter(|slot| world.may_corrupt_record(key, slot.0))
            .collect();
        if !permitted.is_empty() {
            let primary = pick(&permitted);
            // Generous coin: the identifier-lost row has its own per-verdict
            // sometimes-gate, and the entry-rot events that draw this coin
            // are budget-capped per run, so the sweep needs a fat coin to be
            // certain of the composition within a bounded seed schedule.
            let id_faulty = sim_random::<f64>() < 0.5 && world.may_park(key);
            // The block sub-roll needs a contiguous clean run at the primary,
            // which short (frequently truncated) logs often lack, so it rolls
            // generously to stay reachable across a bounded sweep.
            let block = sim_random::<f64>() < 0.4;
            let members: Vec<Slot> = if block {
                permitted
                    .iter()
                    .copied()
                    .filter(|s| s.0 >= primary.0.saturating_sub(2) && *s <= primary)
                    .collect()
            } else {
                vec![primary]
            };
            let is_block = members.len() > 1;
            for slot in members {
                let id = if slot == primary && id_faulty {
                    WitnessStatus::Faulty
                } else {
                    WitnessStatus::Present
                };
                mark_entry(
                    world,
                    slot,
                    SlotHealth {
                        entry: RecordHealth::Faulty,
                        id,
                    },
                    CorruptionKind::BitFlip,
                    is_block,
                );
                world.note_if_unrecoverable(slot.0);
            }
            if id_faulty {
                // Unidentifiable record: the scan can only crash, terminally.
                world.park(key, node);
            }
        }
    }
    // A lost write: the entry reads back as its reserved record where the
    // identifier exists (absence made detectable by the reserved-record
    // contract). Identity known ⇒ recoverable ⇒ per-record budget, no park.
    if buggify_with_prob!(rates.lost_write) {
        let slots = clean_slots(world);
        if let Some(slot) = pick_permitted(world, key, &slots) {
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Lost,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::LostWrite,
                false,
            );
            world.note_if_unrecoverable(slot.0);
        }
    }
    // A misdirected write: valid checksum, wrong identity — the identity
    // check inside the checksummed region catches it. Recoverable likewise.
    if buggify_with_prob!(rates.misdirect) {
        let slots = clean_slots(world);
        if let Some(slot) = pick_permitted(world, key, &slots) {
            mark_entry(
                world,
                slot,
                SlotHealth {
                    entry: RecordHealth::Misdirected,
                    id: WitnessStatus::Present,
                },
                CorruptionKind::Misdirected,
                false,
            );
            world.note_if_unrecoverable(slot.0);
        }
    }
    // Snapshot corruption is its own kind and its own gate (#71) — a
    // first-class target, not a byproduct of log-entry coverage. Stage 8
    // recovers it (local log replay at floor 0, a peer's InstallSnapshot
    // otherwise), so no park; a singleton under a truncated log has no peer
    // to recover from, so budget-on skips that one unrecoverable shape.
    if buggify_with_prob!(rates.snapshot)
        && world
            .disks
            .get(key)
            .is_some_and(|d| d.chain.applied_count > 0)
        && (world.unbudgeted
            || world.cluster_size > 1
            || world.disks.get(key).is_some_and(|d| d.first_slot.0 == 0))
    {
        if let Some(disk) = world.disks.get_mut(key) {
            disk.snapshot_health = RecordHealth::Faulty;
        }
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Snapshot,
            kind: CorruptionKind::BitFlip,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
        // Slots this node truncated past lose their local custody: re-derive
        // the unrecoverable ground truth over the folded prefix (mirrors
        // `corpus_corrupt_snapshot`; unbudgeted only — a budgeted run never
        // permits the shape).
        let floor = world.disks.get(key).map_or(0, |d| d.first_slot.0);
        for slot in 0..floor {
            world.note_if_unrecoverable(slot);
        }
    }
    // HardState copy rot (CTRL metainfo doctrine): usually one copy — used and
    // repaired from its twin, no availability cost — and rarely both, which is
    // the one unrecoverable scalar loss (the node cannot know what it
    // promised, and no peer can tell it).
    if buggify_with_prob!(rates.promise) && world.disks.contains_key(key) {
        let both = sim_random::<f64>() < 0.25;
        if both {
            if world.may_park(key) {
                if let Some(disk) = world.disks.get_mut(key) {
                    disk.promise_health = [RecordHealth::Faulty, RecordHealth::Faulty];
                }
                world.park(key, node);
                for _copy in 0..2 {
                    world.note_corruption(CorruptionInjection {
                        node,
                        record: StorageRecord::Promise,
                        kind: CorruptionKind::PromiseCopy,
                        block: false,
                        outcome: CorruptionOutcome::Dormant,
                    });
                }
            }
        } else {
            let copy = usize::from(sim_random::<f64>() < 0.5);
            // The single-copy leg must stay recoverable: if the twin is
            // already faulty (an earlier single-copy rot that no boot healed
            // yet), rotting this copy would assemble the terminal both-lost
            // shape *outside* the park-guarded branch above — the node would
            // then crash on every boot forever, never parking, inflating the
            // detection count past the ledger. That shape belongs solely to
            // the deliberate `both` branch.
            let twin_clean = world
                .disks
                .get(key)
                .is_some_and(|d| d.promise_health[1 - copy] == RecordHealth::Clean);
            if twin_clean {
                if let Some(disk) = world.disks.get_mut(key) {
                    disk.promise_health[copy] = RecordHealth::Faulty;
                }
                world.note_corruption(CorruptionInjection {
                    node,
                    record: StorageRecord::Promise,
                    kind: CorruptionKind::PromiseCopy,
                    block: false,
                    outcome: CorruptionOutcome::Dormant,
                });
            }
        }
    }
    // A file-granularity FS-metadata fault: reliably crash, never recover
    // (item E) — the whole store is the record.
    if buggify_with_prob!(rates.meta) && world.disks.contains_key(key) && world.may_park(key) {
        let fault = match sim_random::<u64>() % 3 {
            0 => MetadataFault::Missing,
            1 => MetadataFault::WrongSize,
            _ => MetadataFault::ReadOnly,
        };
        if let Some(disk) = world.disks.get_mut(key) {
            disk.meta_fault = Some(fault);
        }
        world.park(key, node);
        world.note_corruption(CorruptionInjection {
            node,
            record: StorageRecord::Store,
            kind: CorruptionKind::Metadata,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
    // #101: chunk rot on the retained decided snapshot point — the value of
    // one fixed-size chunk is lost while the point's identity (and every
    // other chunk) survives. Recoverable by construction: the point is
    // byte-identical cluster-wide, so any peer can serve the chunk back. The
    // budget keeps a clean quorum of each chunk across the holders of the
    // same point (budget-off lifts it, like every other family).
    if buggify_with_prob!(rates.snap_chunk)
        && let Some((at, state)) = world.disks.get(key).and_then(|d| d.snap_point)
    {
        let chunks = snap_chunk_count(state.encode().len());
        if chunks > 0 {
            let chunk = u32::try_from(sim_random::<u64>()).unwrap_or(0) % chunks;
            let clean_copies = world
                .disks
                .iter()
                .filter(|(peer, d)| {
                    !world.parked.contains(*peer)
                        && d.snap_point.is_some_and(|(peer_at, _)| peer_at == at)
                        && d.snap_chunk_health
                            .get(usize::try_from(chunk).unwrap_or(0))
                            .is_none_or(|h| *h == RecordHealth::Clean)
                })
                .count();
            let quorum = world.quorum();
            if (world.unbudgeted || clean_copies.saturating_sub(1) >= quorum)
                && let Some(disk) = world.disks.get_mut(key)
            {
                let index = usize::try_from(chunk).unwrap_or(0);
                if disk.snap_chunk_health.len() <= index {
                    disk.snap_chunk_health
                        .resize(usize::try_from(chunks).unwrap_or(0), RecordHealth::Clean);
                }
                if disk.snap_chunk_health[index] == RecordHealth::Clean {
                    disk.snap_chunk_health[index] = RecordHealth::Faulty;
                    world.note_corruption(CorruptionInjection {
                        node,
                        record: StorageRecord::SnapChunk(Slot(at), chunk),
                        kind: CorruptionKind::BitFlip,
                        block: false,
                        outcome: CorruptionOutcome::Dormant,
                    });
                    // The point is custody for the folded prefix: losing its
                    // last clean copy of a chunk can strand every slot below
                    // the floor. Re-derive the unrecoverable ground truth
                    // (mirrors `corpus_corrupt_snap_chunk`; unbudgeted only).
                    let floor = world.disks.get(key).map_or(0, |d| d.first_slot.0);
                    for slot in 0..floor {
                        world.note_if_unrecoverable(slot);
                    }
                }
            }
        }
    }
    // A transient EIO on the read path: collapses into the corruption channel
    // (one detection path), crashes the node once, and the retry — the next
    // boot — reads clean. The only Stage-7 family with no availability cost.
    if buggify_with_prob!(rates.read_eio) && world.disks.contains_key(key) {
        let record = world
            .disks
            .get(key)
            .and_then(|d| d.accepted.keys().next_back().copied())
            .map_or(StorageRecord::Promise, StorageRecord::Accepted);
        if let Some(disk) = world.disks.get_mut(key) {
            disk.read_eio = Some(record);
        }
        world.note_corruption(CorruptionInjection {
            node,
            record,
            kind: CorruptionKind::ReadEio,
            block: false,
            outcome: CorruptionOutcome::Dormant,
        });
    }
}

/// Pick one budget-permitted rot target from `slots` (see
/// [`roll_boot_rot`]'s `pick` bias: half the time the last retained slot).
fn pick_permitted(world: &StorageWorld, key: &str, slots: &[Slot]) -> Option<Slot> {
    let permitted: Vec<Slot> = slots
        .iter()
        .copied()
        .filter(|slot| world.may_corrupt_record(key, slot.0))
        .collect();
    if permitted.is_empty() {
        return None;
    }
    Some(if sim_random::<f64>() < 0.5 {
        permitted[permitted.len() - 1]
    } else {
        permitted[usize::try_from(sim_random::<u64>()).unwrap_or(0) % permitted.len()]
    })
}

/// This node's durable evidence as of one boot, collected under the world
/// lock in `restore` and consumed by the boot scan.
#[derive(Default)]
struct BootEvidence {
    /// Every retained accepted slot in order, with its record health and its
    /// accepted ballot (the identity a recoverable classification reports).
    records: Vec<(Slot, SlotHealth, Ballot)>,
    promise: [RecordHealth; 2],
    chosen: RecordHealth,
    truncation: RecordHealth,
    snapshot: RecordHealth,
    meta: Option<MetadataFault>,
    read_eio: Option<StorageRecord>,
    /// The retained decided snapshot point's per-chunk health (#101), if any.
    snap_point: Option<(u64, Vec<RecordHealth>)>,
}

impl BootEvidence {
    fn collect(disk: &NodeDisk) -> Self {
        Self {
            records: disk
                .accepted
                .iter()
                .map(|(slot, (ballot, _command))| (*slot, disk.slot_health(*slot), *ballot))
                .collect(),
            promise: disk.promise_health,
            chosen: disk.chosen_health,
            truncation: disk.truncation_health,
            snapshot: disk.snapshot_health,
            meta: disk.meta_fault,
            read_eio: disk.read_eio,
            snap_point: disk
                .snap_point
                .map(|(at, _)| (at, disk.snap_chunk_health.clone())),
        }
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
    /// Per-node write-path fault rates, drawn once per seed (see
    /// [`WritePathRates`]).
    rates: WritePathRates,
}

/// The write-path fault rates, **born workload-buggified** (AGENTS.md prong 2):
/// the defaults are this module's documented constants, and an activated seed
/// draws an extreme. Four independent knob locations, so the sweep can select
/// "this seed's disk fails writes often" separately from "this seed's disk
/// fails fsyncs often" and from either torn-tail shaping.
///
/// **The floor on all four is structural rather than numeric**: every write
/// site is gated on [`StorageFaults::active`], so all of them stop at the chaos
/// cutoff, and the accepted-record sites additionally pass through
/// [`StorageWorld::permit_and_record`]'s per-record clean-quorum budget and its
/// never-fault-every-record-of-one-node cap. A run therefore cannot be made
/// unwinnable by turning a rate up: the extremes buy a *denser* fault window,
/// never a longer one, and the recovery tail that follows (an order of
/// magnitude longer than the window) is always fault-free.
#[derive(Clone, Copy)]
struct WritePathRates {
    write_eio: f64,
    fsync_fail: f64,
    force_torn_tail: f64,
    torn_tail: f64,
}

impl Default for WritePathRates {
    fn default() -> Self {
        Self {
            write_eio: P_WRITE_EIO,
            fsync_fail: P_FSYNC_FAIL,
            force_torn_tail: P_FORCE_TORN_TAIL,
            torn_tail: P_TORN_TAIL,
        }
    }
}

impl WritePathRates {
    /// Draw this node's rates for one timeline. The knobs are integer
    /// percentages (`buggify_knob!` draws from an integer range) converted to
    /// probabilities here.
    fn for_timeline() -> Self {
        // A disk that returns `EIO` on one write in twelve rather than one in a
        // hundred. The ambiguity contract is unchanged (the world still decides
        // persisted-vs-lost per fault), so the extreme only makes the *recovery*
        // path — boot from whatever the disk actually holds — the common case
        // instead of the rare one.
        let write_eio = buggify_knob!(u64::from(PCT_WRITE_EIO), 2_u64..9_u64);
        // The batch-fsync twin, independently selectable for the same reason
        // the two sites are independent locations at all: the sweep must be
        // able to pick per-record ambiguity without whole-batch ambiguity, and
        // the other way round.
        let fsync_fail = buggify_knob!(u64::from(PCT_FSYNC_FAIL), 2_u64..9_u64);
        // Forcing the torn shape harder. It rides the ordinary fsync ledger,
        // budget and crash decision, so a high rate buys more
        // crash-truncatable tails, not a new fault.
        let force_torn_tail = buggify_knob!(u64::from(PCT_FORCE_TORN_TAIL), 10_u64..41_u64);
        // Outcome-shaping of one fault, not a fault of its own: how a lost
        // fsync leg *lands* (a torn prefix vs. a whole-batch loss). Both legs
        // stay legal at either extreme — whole-batch loss is also what every
        // seam crash before the fsync produces — so the knob only moves which
        // shape this seed's boots have to classify.
        let torn_tail = buggify_knob!(u64::from(PCT_TORN_TAIL), 25_u64..101_u64);
        if write_eio != u64::from(PCT_WRITE_EIO) || fsync_fail != u64::from(PCT_FSYNC_FAIL) {
            // BUGGIFY pairing: a node genuinely runs on a dense-failure disk.
            assert_reachable!("storage: a node runs with a buggified write-fault rate");
        }
        if force_torn_tail != u64::from(PCT_FORCE_TORN_TAIL)
            || torn_tail != u64::from(PCT_TORN_TAIL)
        {
            // BUGGIFY pairing: the torn-tail shaping knobs genuinely fire.
            assert_reachable!("storage: a node runs with a buggified torn-tail rate");
        }
        #[allow(clippy::cast_precision_loss)]
        let pct = |v: u64| v as f64 / 100.0;
        Self {
            write_eio: pct(write_eio),
            fsync_fail: pct(fsync_fail),
            force_torn_tail: pct(force_torn_tail),
            torn_tail: pct(torn_tail),
        }
    }
}

impl<T: TimeProvider> StorageFaults<T> {
    fn new(time: T, cutoff: Duration, enabled: bool) -> Self {
        // Only an enabled (perturbing) node draws: the scripted corpus and the
        // contract suite must not spend randomness they never use.
        let rates = if enabled {
            WritePathRates::for_timeline()
        } else {
            WritePathRates::default()
        };
        Self {
            time,
            cutoff,
            enabled,
            rates,
        }
    }

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
    /// This incarnation's application state, including transitions staged for
    /// the next durability flush.
    application: ChainState,
    /// This boot's durable-record evidence, consumed by the boot scan.
    evidence: BootEvidence,
    /// The scan's recoverable classification, served to the core through
    /// [`Storage::faulty_entries`].
    faulty_list: Vec<(Slot, Ballot)>,
    /// The scan's rotted-chunk classification of the retained decided
    /// snapshot point (#101), served through
    /// [`NodeStorage::faulty_snap_chunks`].
    faulty_chunks: Vec<(Slot, u32)>,
    /// A decided snapshot point staged for the next durability flush (#101).
    staged_snap_point: Option<(Slot, ChainState)>,
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

impl<T: TimeProvider> DurableStorage<T> {
    /// Build storage for `config`, seeding the read view from any durable records
    /// a prior boot of this node (same IP, same iteration) left in the world.
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
        let mut evidence = BootEvidence::default();
        if let Some(strong) = world.upgrade() {
            let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            // Stage 7 rot: latent faults that surfaced while the node was
            // down, rolled at the boot that immediately scans them. Gated on
            // the chaos window like every other injection.
            if faults.active() {
                roll_boot_rot(&mut guard, &key, node_id);
            }
            let guard = &*guard;
            // Coverage: the recovery paths only matter if boots genuinely
            // re-read a prior incarnation's records (attrition + seam crashes
            // make this common across the sweep).
            assert_sometimes!(
                guard.disks.contains_key(&key),
                "a node boots from a prior incarnation's durable records"
            );
            if let Some(disk) = guard.disks.get(&key) {
                application = disk.chain;
                // Read-back pair of the flush ordering `sync` claims: a floor
                // that reached the disk never outruns the chosen index that
                // reached the disk (the flush applies the floor last, and a
                // crash drops the whole stage together).
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
                // Collect this boot's integrity evidence for the scan the
                // driver runs before anything reads the store.
                evidence = BootEvidence::collect(disk);
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
                    // The detector withholds bytes: only clean, witnessed
                    // records enter the read view. Anything else either
                    // crashes the scan (so the view is never consulted) or is
                    // a crash-truncatable tail the scan discards.
                    if disk.slot_health(*slot).clean() {
                        let _ = boot.append_accepted(*slot, *ballot, command.clone());
                    }
                }
                if let Some(ci) = disk.hard_state.chosen_index {
                    let _ = boot.set_chosen_index(ci);
                }
                let _ = boot.sync(MustSync::Sync);
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
            evidence,
            faulty_list: Vec::new(),
            faulty_chunks: Vec::new(),
            staged_snap_point: None,
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

    /// Whether a torn flush would tear anything right now: the stage holds at
    /// least one accept above this disk's durable maximum (only fresh appends
    /// tear, see [`Self::torn_flush`]) **and** the first such record clears the
    /// per-record budget (a clean quorum of live copies survives its loss).
    /// The budget is what makes the shape rare — a leader's first write of a
    /// slot has no clean copies elsewhere yet, so only a node writing slots a
    /// quorum already holds can tear — which is why the forcing site consults
    /// this predicate rather than the bare fresh-append check.
    fn tearable_stage(&self) -> bool {
        let key = self.key.clone();
        let staged: Vec<Slot> = self.staged_accepted.keys().copied().collect();
        self.with_world(|w| {
            let durable_max = w
                .disks
                .get(&key)
                .and_then(|d| d.accepted.keys().next_back().copied());
            let Some(first) = staged
                .into_iter()
                .find(|slot| durable_max.is_none_or(|max| *slot > max))
            else {
                return false;
            };
            let already = w.marks.get(&key).is_some_and(|m| m.contains(&first.0));
            w.clean_copies(first.0)
                .saturating_sub(usize::from(!already))
                >= w.quorum()
        })
        .unwrap_or(false)
    }

    /// The torn leg of a lost fsync (Stage 7): a prefix of the batch's
    /// **fresh appends** reaches the disk without identifiers — the crash tore
    /// the batch mid-write instead of losing it whole. Only fresh appends
    /// tear: tearing an overwrite would rot a previously *witnessed* record
    /// (the rot family's job), and fresh appends are what guarantees the torn
    /// records sit at the log's very tail, where the boot scan's
    /// disentanglement classifies them crash-truncatable and discards them —
    /// the one legal discard (never acknowledged: `Accepted` only ever leaves
    /// after a clean sync, per persist-before-send). The last torn record's
    /// bytes may themselves be damaged, so both `CrashTail` rows of the
    /// decision table are visited.
    fn torn_flush(&mut self) {
        let key = self.key.clone();
        let node = self.node_id;
        let staged: Vec<(Slot, (Ballot, Command))> = self
            .staged_accepted
            .iter()
            .map(|(slot, record)| (*slot, record.clone()))
            .collect();
        let _ = self.with_world(|w| {
            let durable_max = w
                .disks
                .get(&key)
                .and_then(|d| d.accepted.keys().next_back().copied());
            let fresh: Vec<(Slot, (Ballot, Command))> = staged
                .into_iter()
                .filter(|(slot, _)| durable_max.is_none_or(|max| *slot > max))
                .collect();
            if fresh.is_empty() {
                return;
            }
            let count = 1 + usize::try_from(sim_random::<u64>()).unwrap_or(0) % fresh.len();
            // The same per-record budget as every other injection: never leave
            // a record without a clean quorum of live copies.
            let quorum = w.quorum();
            let mut torn: Vec<(Slot, (Ballot, Command))> = Vec::new();
            for (slot, record) in fresh.into_iter().take(count) {
                let already = w.marks.get(&key).is_some_and(|m| m.contains(&slot.0));
                if w.clean_copies(slot.0).saturating_sub(usize::from(!already)) < quorum {
                    break;
                }
                torn.push((slot, record));
            }
            if torn.is_empty() {
                return;
            }
            let torn_entry_faulty = sim_random::<f64>() < 0.5;
            let last = torn.len() - 1;
            let d = w.disks.entry(key.clone()).or_default();
            for (i, (slot, record)) in torn.iter().enumerate() {
                d.accepted.insert(*slot, record.clone());
                d.entry_health.insert(
                    *slot,
                    SlotHealth {
                        entry: if i == last && torn_entry_faulty {
                            RecordHealth::Faulty
                        } else {
                            RecordHealth::Clean
                        },
                        id: WitnessStatus::Absent,
                    },
                );
            }
            // Deliberately NOT noted as flushed ground truth: a torn record is
            // not durably witnessed, and the boot scan discarding it must not
            // read as recovered-vs-persisted divergence.
            for (slot, _) in &torn {
                w.marks.entry(key.clone()).or_default().insert(slot.0);
                w.note_corruption(CorruptionInjection {
                    node,
                    record: StorageRecord::Accepted(*slot),
                    kind: CorruptionKind::TornTail,
                    block: false,
                    outcome: CorruptionOutcome::Dormant,
                });
            }
        });
    }

    /// BUGGIFY site 1: this per-record write returns `EIO`. Returns
    /// `Some(persisted)` when the fault fires and the budget permits it —
    /// `persisted` is the world's seeded ambiguity decision (item C), recorded
    /// as ground truth and never told to the node.
    fn roll_write_eio(&mut self, record: StorageRecord) -> Option<bool> {
        if !self.faults.active() || !buggify_with_prob!(self.faults.rates.write_eio) {
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
    /// from the write site; same ambiguity contract. `force_lost` is the
    /// forced-torn-tail site's verdict: it skips this site's own coin and
    /// takes the lost leg outright, through the same ledger and budget.
    fn roll_fsync_fault(&mut self, force_lost: bool) -> Option<bool> {
        if !self.faults.active() {
            return None;
        }
        if !force_lost && !buggify_with_prob!(self.faults.rates.fsync_fail) {
            return None;
        }
        let persisted = !force_lost && sim_random::<f64>() < 0.5;
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
    /// reported anyway. Clean re-writes clear their lost-leg fault marks.
    // One linear flush in write order; the health-healing lines belong beside
    // the writes whose durability they mirror.
    #[allow(clippy::too_many_lines)]
    fn flush_stage(&mut self) -> Result<(), StorageError> {
        let config_id = self.staged_config_id.take();
        let ballot = self.staged_ballot.take();
        let accepted = std::mem::take(&mut self.staged_accepted);
        let chosen = self.staged_chosen.take();
        let floor = self.staged_floor.take();
        let snapshot = self.staged_snapshot.take();
        let snap_point = self.staged_snap_point.take();
        let applies = std::mem::take(&mut self.staged_applies);
        let sealed = std::mem::take(&mut self.staged_sealed);
        let flushed_slots: Vec<u64> = accepted.keys().map(|s| s.0).collect();
        let flushed_hashes: Vec<(u64, u64)> = accepted
            .iter()
            .map(|(slot, (_ballot, command))| (slot.0, command_hash(command)))
            .collect();
        let key = self.key.clone();
        let node = self.node_id;
        self.with_world(|w| {
            w.clear_marks(&key, flushed_slots.iter().copied());
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
                // A clean re-write of the HardState copies restores their
                // health — genuine recovery, not the world healing anything.
                d.promise_health = [RecordHealth::Clean; 2];
            }
            let mut healed: Vec<Slot> = Vec::new();
            for (slot, record) in accepted {
                // A clean flush re-writes the record and its identifier: any
                // prior torn/rotted health for the slot is genuinely replaced —
                // this is the Stage-8 in-place repair landing on disk.
                if d.entry_health.remove(&slot).is_some() {
                    healed.push(slot);
                }
                d.accepted.insert(slot, record);
            }
            if let Some(c) = chosen {
                d.hard_state.chosen_index = Some(c);
                d.chosen_health = RecordHealth::Clean;
            }
            // Apply the truncation last, after the chosen index it sits behind, so
            // a flushed floor never outruns the flushed chosen index.
            if let Some(f) = floor {
                d.first_slot = d.first_slot.max(f);
                d.accepted.retain(|s, _| *s >= d.first_slot);
                let new_floor = d.first_slot;
                // A fault mark dropped by the floor raise is superseded, not
                // repaired: the record's information migrated into the applied
                // application state (truncation is decided over the applied
                // prefix) — resolve its report as recovered-by-custodianship.
                healed.extend(
                    d.entry_health
                        .range(..new_floor)
                        .map(|(slot, _)| *slot)
                        .collect::<Vec<_>>(),
                );
                d.entry_health.retain(|s, _| *s >= new_floor);
                d.truncation_health = RecordHealth::Clean;
            }
            if let Some(installed) = snapshot {
                d.chain = installed;
                d.snapshot_health = RecordHealth::Clean;
            }
            if let Some(last) = applies.last() {
                d.chain = last.transition.next;
                d.snapshot_health = RecordHealth::Clean;
            }
            // A decided snapshot point (#101): retain the new point with a
            // fresh all-clean chunk map. Advancing the point supersedes the
            // old one's outstanding chunk reports — custody moved to the new
            // byte-identical blob.
            let mut superseded_chunks: Vec<(u64, u32)> = Vec::new();
            if let Some((at, state)) = snap_point {
                if let Some((old_at, _)) = d.snap_point
                    && old_at != at.0
                {
                    superseded_chunks = d
                        .snap_chunk_health
                        .iter()
                        .enumerate()
                        .filter(|(_, health)| **health != RecordHealth::Clean)
                        .map(|(index, _)| (old_at, u32::try_from(index).unwrap_or(u32::MAX)))
                        .collect();
                }
                let chunks = snap_chunk_count(state.encode().len());
                d.snap_point = Some((at.0, state));
                d.snap_chunk_health =
                    vec![RecordHealth::Clean; usize::try_from(chunks).unwrap_or(0)];
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
            // Resolve the reports the flush genuinely healed (or truncation
            // superseded), and the snapshot report once fresh application
            // state landed (an install, or replayed applies).
            for slot in healed {
                w.note_recovered(node, StorageRecord::Accepted(slot));
            }
            if snapshot.is_some() || !applies.is_empty() {
                w.note_recovered(node, StorageRecord::Snapshot);
            }
            for (old_at, chunk) in superseded_chunks {
                w.note_recovered(node, StorageRecord::SnapChunk(Slot(old_at), chunk));
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

        // The application-state facts, reported to the audit as they become
        // durable (and traced for humans). An install is a jump; every apply
        // is one contiguous transition.
        if let Some(installed) = snapshot {
            self.checker
                .app_snapshot(self.node_id, installed.applied_count, installed.chain_hash);
            tracing::info!(
                node = self.node_id,
                index = installed.applied_count,
                state = %hash_text(installed.chain_hash),
                "chain_snapshot_installed"
            );
        }
        for pending in applies {
            let next = pending.transition.next;
            self.checker.app_applied(
                self.node_id,
                next.applied_count,
                pending.transition.cmd_hash,
                pending.transition.kind == "user",
                pending.transition.kind == "noop",
                next.chain_hash,
            );
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
    /// The Stage-7 detection layer at the seam (CLStore-equivalent): verify
    /// every durable record's read-back evidence, classify each mismatch with
    /// the total decision function (`paros::classify_log`), and surface the
    /// first crash-verdict as the typed error the driver crashes on. The scan
    /// resolves exactly two things itself — a crash-truncatable tail is
    /// discarded (never acknowledged to anyone) and a single bad `HardState`
    /// copy is repaired from its twin — and **never truncates on a corruption
    /// verdict**.
    ///
    /// That last rule is CTRL Figure 2's bug class, found in both `ZooKeeper`
    /// and `LogCabin`: a scan that drops the log from the faulty entry onward
    /// and keeps running as if the log simply ended earlier silently discards
    /// possibly-chosen records (and regresses the derived chosen index with
    /// them), so the node can then win an election against lagging peers and
    /// erase committed data cluster-wide. It was proven load-bearing by
    /// mutation: wiring the scan to truncate here instead of crashing turned
    /// the audit's recovered-vs-persisted divergence leg ("storage: a
    /// recovered log omits a persisted record only after a detected corruption
    /// crash") red. Detect ⇒ crash is the baseline; a crash-truncatable tail is
    /// the only thing a scan may ever discard.
    #[allow(clippy::too_many_lines)] // one linear scan: metadata → scalars → log
    fn boot_scan(&mut self) -> Result<(), StorageError> {
        let evidence = std::mem::take(&mut self.evidence);
        let key = self.key.clone();
        let node = self.node_id;
        // FS metadata (item E): the store itself is the record; reliably
        // crash, never attempt recovery — Stage 8 either.
        if let Some(fault) = evidence.meta {
            let _ = self.with_world(|w| {
                w.s7.metadata_crashed = true;
                w.resolve_corruption(node, StorageRecord::Store, CorruptionOutcome::Crashed);
            });
            return Err(StorageError::Metadata { fault });
        }
        // A transient EIO on the read path collapses into the corruption
        // channel (zero-fill ⇒ mismatch): same detection, same crash. The
        // world clears the fault, so the retry — the next boot — reads clean.
        if let Some(record) = evidence.read_eio {
            let _ = self.with_world(|w| {
                w.s7.read_eio_detected = true;
                if let Some(disk) = w.disks.get_mut(&key) {
                    disk.read_eio = None;
                }
                w.resolve_corruption(node, record, CorruptionOutcome::Crashed);
            });
            return Err(StorageError::Corruption {
                record,
                fault: IntegrityFault::ReadError,
                verdict: CorruptionVerdict::Corrupted,
            });
        }
        // The two HardState copies (CTRL metainfo doctrine): one bad ⇒ use
        // the other and repair it; both bad ⇒ crash — the node cannot know
        // what it promised, and no peer can tell it (Stage 8's safety
        // argument, pre-stated).
        let promise_faults = [
            evidence.promise[0].integrity_fault(),
            evidence.promise[1].integrity_fault(),
        ];
        match promise_faults {
            [Some(fault), Some(_)] => {
                let _ = self.with_world(|w| {
                    w.resolve_corruption(node, StorageRecord::Promise, CorruptionOutcome::Crashed);
                    // Terminal by doctrine (detect ⇒ crash, stays down): with
                    // both copies lost the promise is unknowable and no boot
                    // will ever read past this point, so park the node here
                    // rather than crash-looping — one typed crash decision,
                    // matched 1:1 by the ledger. The injection site only
                    // builds this shape under `may_park`, so this is defense
                    // in depth for any other path assembling it.
                    w.park(&key, node);
                });
                assert_reachable!(
                    "storage: both promise copies are lost (crash: unknowable promise)"
                );
                return Err(StorageError::Corruption {
                    record: StorageRecord::Promise,
                    fault,
                    verdict: CorruptionVerdict::Corrupted,
                });
            }
            [Some(_), None] | [None, Some(_)] => {
                let _ = self.with_world(|w| {
                    w.s7.promise_repaired = true;
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.promise_health = [RecordHealth::Clean; 2];
                    }
                    w.resolve_corruption(node, StorageRecord::Promise, CorruptionOutcome::Repaired);
                });
                tracing::info!(node, "promise_copy_repaired");
            }
            [None, None] => {}
        }
        // The remaining scalars follow the atomic-rename discipline: a partial
        // write is discarded, so a mismatch is always corruption — no
        // crash-tail leg exists for them.
        for (health, record) in [
            (evidence.chosen, StorageRecord::ChosenIndex),
            (evidence.truncation, StorageRecord::Truncation),
        ] {
            if let Some(fault) = health.integrity_fault() {
                let _ = self.with_world(|w| {
                    w.park(&key, node);
                    w.resolve_corruption(node, record, CorruptionOutcome::Crashed);
                });
                return Err(StorageError::Corruption {
                    record,
                    fault,
                    verdict: CorruptionVerdict::Corrupted,
                });
            }
        }
        // Snapshot corruption: its own kind and its own gate (#71). Stage 8
        // recovers instead of crashing — a snapshot is never discardable and
        // all its data is committed by definition, so the node must never
        // install or serve the garbage, and must recover the state: with the
        // log intact from slot 0 the ordinary boot replay rebuilds it locally
        // (CTRL's cheap path, with the core's duplicate-suppression decisions
        // re-derived exactly); under a truncated log only a peer's
        // `InstallSnapshot` covers the folded prefix, so the driver opens a
        // below-floor application repair and the node *waits* on it — serving
        // consensus for every slot it can read, applying nothing.
        if evidence.snapshot.integrity_fault().is_some() {
            let floor = self.boot.first_slot();
            // #101: a fully clean decided snapshot point that covers the
            // floor restores the lost application state *locally* — the
            // CTRL payoff of consensus-decided snapshot points: no whole-blob
            // transfer, no wait, just the retained byte-identical state.
            let point_state = self
                .with_world(|w| {
                    let disk = w.disks.get(&key)?;
                    let (at, state) = disk.snap_point?;
                    let all_clean = disk
                        .snap_chunk_health
                        .iter()
                        .all(|health| *health == RecordHealth::Clean);
                    (all_clean && at + 1 >= floor.0).then_some((at, state))
                })
                .ok()
                .flatten();
            if floor.0 > 0
                && let Some((at, state)) = point_state
            {
                let _ = self.with_world(|w| {
                    w.s7.snapshot_detected = true;
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.chain = state;
                        disk.snapshot_health = RecordHealth::Clean;
                    }
                    w.resolve_corruption(
                        node,
                        StorageRecord::Snapshot,
                        CorruptionOutcome::Reported,
                    );
                    w.note_recovered(node, StorageRecord::Snapshot);
                });
                self.application = state;
                tracing::info!(node, at, "snapshot_restored_from_point");
                // For the application-agreement checker this is a reset (the
                // pre-crash prefix may sit past the point, so the jump can go
                // backward) followed by an install-shaped landing at the
                // point; the re-walk from there is contiguous again.
                self.checker.app_reset(node);
                self.checker
                    .app_snapshot(node, state.applied_count, state.chain_hash);
                tracing::info!(node, floor = floor.0, "snapshot_reset_for_recovery");
                tracing::info!(
                    node,
                    index = state.applied_count,
                    state = %hash_text(state.chain_hash),
                    "chain_snapshot_installed"
                );
                assert_reachable!(
                    "storage: a corrupted snapshot is restored from the decided snapshot point"
                );
            } else {
                let _ = self.with_world(|w| {
                    w.s7.snapshot_detected = true;
                    if floor.0 == 0 {
                        w.s7.snapshot_reset_local = true;
                    } else {
                        w.s7.snapshot_reset_remote = true;
                    }
                    if let Some(disk) = w.disks.get_mut(&key) {
                        disk.chain = ChainState::default();
                        disk.snapshot_health = RecordHealth::Clean;
                    }
                    w.resolve_corruption(
                        node,
                        StorageRecord::Snapshot,
                        CorruptionOutcome::Reported,
                    );
                });
                self.application = ChainState::default();
                self.checker.app_reset(node);
                tracing::info!(node, floor = floor.0, "snapshot_reset_for_recovery");
                if floor.0 == 0 {
                    assert_reachable!(
                        "storage: a corrupted snapshot is rebuilt from the local log"
                    );
                } else {
                    assert_reachable!(
                        "storage: a corrupted snapshot awaits a peer snapshot transfer"
                    );
                }
            }
        }
        // #101: rotted chunks of the retained decided snapshot point are the
        // recoverable class by construction — the point's identity survives
        // and every peer holds the byte-identical blob — so they are
        // classified and reported for the driver's chunk-repair pull, never a
        // crash.
        if let Some((at, chunk_health)) = &evidence.snap_point {
            let rotted: Vec<(Slot, u32)> = chunk_health
                .iter()
                .enumerate()
                .filter(|(_, health)| **health != RecordHealth::Clean)
                .map(|(index, _)| (Slot(*at), u32::try_from(index).unwrap_or(u32::MAX)))
                .collect();
            if !rotted.is_empty() {
                let _ = self.with_world(|w| {
                    for (point, chunk) in &rotted {
                        w.resolve_corruption(
                            node,
                            StorageRecord::SnapChunk(*point, *chunk),
                            CorruptionOutcome::Reported,
                        );
                    }
                });
                tracing::info!(
                    node,
                    at = *at,
                    chunks = rotted.len() as u64,
                    "snap_chunks_classified"
                );
                assert_reachable!("storage: rotted snapshot chunks are classified for peer repair");
                self.faulty_chunks = rotted;
            }
        }
        // The log: reduce every retained record to its evidence booleans and
        // run the total classifier (batching rule + TigerBeetle hardening).
        let records: Vec<SlotRecord> = evidence
            .records
            .iter()
            .map(|(slot, health, _ballot)| SlotRecord {
                slot: *slot,
                entry_faulty: health.entry != RecordHealth::Clean,
                identifier: health.id,
            })
            .collect();
        let cases = classify_log(&records, MAX_TORN_TAIL);
        let mut discard: Vec<Slot> = Vec::new();
        // Stage 8's crash-relevance split (the issue-#21 table): a record whose
        // *identity* is known — the identifier survived, or the entry's own
        // checksummed identity region did — is **recoverable**: reported into
        // the tri-state, repaired in place, never a crash. A record whose
        // identity is also lost is unidentifiable (the node cannot even ask
        // peers the right question), and the abandoned-window undecidables
        // break head-certainty likewise: those stay the Stage-7 crash.
        let mut recover: Vec<(Slot, Ballot, RecoveryCase)> = Vec::new();
        let mut crashes: Vec<(Slot, SlotHealth, RecoveryCase)> = Vec::new();
        for ((slot, case), (_, health, ballot)) in cases.iter().zip(evidence.records.iter()) {
            match case.verdict() {
                None => {}
                Some(CorruptionVerdict::CrashTail) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    discard.push(*slot);
                }
                Some(CorruptionVerdict::Corrupted | CorruptionVerdict::Undecidable) => {
                    tracing::info!(node, slot = slot.0, case = case.label(), "boot_scan_case");
                    match case {
                        RecoveryCase::CorruptionBelowTail
                        | RecoveryCase::IdentifierFaulty
                        | RecoveryCase::LastEntryAmbiguity => recover.push((*slot, *ballot, *case)),
                        _ => crashes.push((*slot, *health, *case)),
                    }
                }
            }
        }
        // Family / per-verdict coverage flags fire at *classification* — the
        // detection channels are exercised whether the reaction is a crash or
        // a report (the message strings are Stage 7's, unchanged).
        let _ = self.with_world(|w| {
            for ((_slot, case), (_, health, _ballot)) in cases.iter().zip(evidence.records.iter()) {
                if case.verdict().is_none() {
                    continue;
                }
                match case {
                    RecoveryCase::CorruptionBelowTail => w.s7.corruption_below_tail = true,
                    RecoveryCase::LastEntryAmbiguity => w.s7.last_entry_ambiguity = true,
                    RecoveryCase::IdentifierLostWithEntry => w.s7.identifier_lost = true,
                    _ => {}
                }
                match health.entry {
                    RecordHealth::Faulty => w.s7.bitflip_detected = true,
                    RecordHealth::Lost => w.s7.lost_write_detected = true,
                    RecordHealth::Misdirected => w.s7.misdirected_detected = true,
                    RecordHealth::Clean => {}
                }
            }
        });
        if let Some(&(slot, health, case)) = crashes.first() {
            let _ = self.with_world(|w| {
                // Detection is certain and terminal for an unidentifiable
                // record: restarting cannot help (rot injection already parked
                // the node; classifier-derived verdicts park it here).
                w.park(&key, node);
                for (i, (crash_slot, _crash_health, _crash_case)) in crashes.iter().enumerate() {
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*crash_slot),
                        if i == 0 {
                            CorruptionOutcome::Crashed
                        } else {
                            CorruptionOutcome::CoDetected
                        },
                    );
                }
            });
            return Err(StorageError::Corruption {
                record: StorageRecord::Accepted(slot),
                // An id-missing verdict on a clean entry means the *witness*
                // was lost, not the bytes.
                fault: health
                    .entry
                    .integrity_fault()
                    .unwrap_or(IntegrityFault::LostWrite),
                verdict: case.verdict().unwrap_or(CorruptionVerdict::Corrupted),
            });
        }
        if !recover.is_empty() {
            let _ = self.with_world(|w| {
                for (slot, _ballot, case) in &recover {
                    w.s7.faulty_reported = true;
                    tracing::info!(
                        node,
                        slot = slot.0,
                        case = case.label(),
                        "faulty_entry_reported"
                    );
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*slot),
                        CorruptionOutcome::Reported,
                    );
                }
            });
            self.faulty_list = recover
                .iter()
                .map(|(slot, ballot, _case)| (*slot, *ballot))
                .collect();
        }
        if !discard.is_empty() {
            // The one legal discard: a crash-truncatable tail was never
            // acknowledged to anyone (persist-before-send), so dropping it
            // locally is a faithful crash, not data loss.
            let _ = self.with_world(|w| {
                w.s7.torn_tail_discarded = true;
                w.clear_marks(&key, discard.iter().map(|slot| slot.0));
                if let Some(disk) = w.disks.get_mut(&key) {
                    for slot in &discard {
                        disk.accepted.remove(slot);
                        disk.entry_health.remove(slot);
                    }
                }
                for slot in &discard {
                    w.resolve_corruption(
                        node,
                        StorageRecord::Accepted(*slot),
                        CorruptionOutcome::DiscardedTail,
                    );
                }
                for slot in &discard {
                    tracing::info!(node, slot = slot.0, "crash_tail_discarded");
                }
            });
        }
        Ok(())
    }

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
        // The forced torn tail (its own BUGGIFY location): only a stage whose
        // fresh appends clear the per-record budget can tear, so the site is
        // consulted only where it can have an effect (AGENTS.md: consult a
        // hook only when the choice is observable).
        let force_torn = self.faults.active()
            && self.tearable_stage()
            && buggify_with_prob!(self.faults.rates.force_torn_tail);
        // BUGGIFY site 2: the fsync fails — only when the stage actually holds
        // something (an empty flush has nothing at stake). On the durable leg
        // the flush happens anyway before the error is reported (fsyncgate);
        // on the lost leg the stage stays un-flushed and dies with the
        // incarnation the driver's crash decision is about to unwind.
        if !self.staged_records().is_empty()
            && let Some(persisted) = self.roll_fsync_fault(force_torn)
        {
            if persisted {
                self.flush_stage()?;
            } else if self.faults.active()
                && (force_torn || sim_random::<f64>() < self.faults.rates.torn_tail)
            {
                if force_torn {
                    // BUGGIFY pairing: the forcing site genuinely fired.
                    assert_reachable!("storage: a torn tail is forced by its BUGGIFY site");
                }
                // Stage 7's per-record torn durability, paired with the crash
                // the driver is about to take on this error: a prefix of the
                // batch's fresh appends lands unwitnessed, so the next boot
                // scan walks the CrashTail rows of the disentanglement table.
                self.torn_flush();
            }
            return Err(StorageError::FsyncFailed {
                record: StorageRecord::Batch,
                outcome: WriteOutcome::Unknown,
            });
        }
        self.flush_stage()
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

    fn snapshot(&self) -> Vec<u8> {
        self.application.encode()
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
        // A transferred snapshot that fails to decode is a mismatch on the
        // wire-to-disk path; if it ever fires, the injected⇔detected
        // correlation flags the uninjected detection as a bug.
        let installed = ChainState::decode(&snapshot).map_err(|_| StorageError::Corruption {
            record: StorageRecord::Snapshot,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
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

    fn record_snapshot(&mut self, at: Slot) -> Result<(), StorageError> {
        // Captured at the apply seam, when the staged application state IS the
        // marker's boundary state; durable with the batch's application fsync.
        self.staged_snap_point = Some((at, self.application));
        Ok(())
    }

    fn latest_snap_point(&self) -> Option<Slot> {
        if let Some((at, _)) = self.staged_snap_point {
            return Some(at);
        }
        let key = self.key.clone();
        self.with_world(|w| {
            w.disks
                .get(&key)
                .and_then(|d| d.snap_point.map(|(at, _)| Slot(at)))
        })
        .ok()
        .flatten()
    }

    fn snap_chunk_count(&self, at: Slot) -> Option<u32> {
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (point, state) = disk.snap_point?;
            (point == at.0).then(|| snap_chunk_count(state.encode().len()))
        })
        .ok()
        .flatten()
    }

    fn read_snap_chunk(&self, at: Slot, chunk: u32) -> Option<Vec<u8>> {
        let key = self.key.clone();
        self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (point, state) = disk.snap_point?;
            if point != at.0 {
                return None;
            }
            let index = usize::try_from(chunk).ok()?;
            // A rotted chunk answers nothing (silence, never garbage).
            if disk
                .snap_chunk_health
                .get(index)
                .is_some_and(|health| *health != RecordHealth::Clean)
            {
                return None;
            }
            let blob = state.encode();
            let start = index.checked_mul(SNAP_CHUNK_BYTES)?;
            if start >= blob.len() {
                return None;
            }
            let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
            Some(blob[start..end].to_vec())
        })
        .ok()
        .flatten()
    }

    fn write_snap_chunk(
        &mut self,
        at: Slot,
        chunk: u32,
        bytes: &[u8],
    ) -> Result<bool, StorageError> {
        let key = self.key.clone();
        let node = self.node_id;
        let bytes = bytes.to_vec();
        self.with_world(move |w| {
            let outcome = {
                let Some(disk) = w.disks.get_mut(&key) else {
                    return false;
                };
                let Some((point, state)) = disk.snap_point else {
                    return false;
                };
                if point != at.0 {
                    return false;
                }
                let Some(index) = usize::try_from(chunk).ok() else {
                    return false;
                };
                let blob = state.encode();
                let Some(start) = index.checked_mul(SNAP_CHUNK_BYTES) else {
                    return false;
                };
                if start >= blob.len() {
                    return false;
                }
                let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
                // The received chunk must be byte-identical to the decided
                // state — the identity the `Snap` marker exists to guarantee,
                // asserted against the world's ground truth.
                assert_always!(
                    bytes == blob[start..end],
                    "chain: a repaired snapshot chunk matches the decided point",
                    { "at" => at.0, "chunk" => chunk }
                );
                if bytes != blob[start..end] {
                    return false;
                }
                if disk.snap_chunk_health.len() <= index {
                    disk.snap_chunk_health.resize(
                        usize::try_from(snap_chunk_count(blob.len())).unwrap_or(0),
                        RecordHealth::Clean,
                    );
                }
                let was_faulty = disk.snap_chunk_health[index] != RecordHealth::Clean;
                // Models an atomic per-chunk file replace; the driver flushes
                // right after installing a response's chunks.
                disk.snap_chunk_health[index] = RecordHealth::Clean;
                let all_clean = disk
                    .snap_chunk_health
                    .iter()
                    .all(|health| *health == RecordHealth::Clean);
                (was_faulty, all_clean)
            };
            let (was_faulty, all_clean) = outcome;
            if was_faulty {
                w.note_recovered(node, StorageRecord::SnapChunk(at, chunk));
            }
            all_clean
        })
    }

    fn faulty_snap_chunks(&self) -> Vec<(Slot, u32)> {
        self.faulty_chunks.clone()
    }

    fn restore_from_snap_point(&mut self) -> Result<Option<Slot>, StorageError> {
        let key = self.key.clone();
        let candidate = self.with_world(|w| {
            let disk = w.disks.get(&key)?;
            let (at, state) = disk.snap_point?;
            let all_clean = disk
                .snap_chunk_health
                .iter()
                .all(|health| *health == RecordHealth::Clean);
            // The point must be whole and must cover the compaction floor —
            // replay from the floor is contiguous only from `at + 1`.
            (all_clean && at + 1 >= disk.first_slot.0).then_some((at, state))
        })?;
        let Some((at, state)) = candidate else {
            return Ok(None);
        };
        if self
            .application
            .applied_slot()
            .is_some_and(|applied| applied.0 >= at)
        {
            return Ok(None);
        }
        // Stage the restored state exactly like a snapshot install: the flush
        // sets the durable application state, heals the live-snapshot health,
        // and emits the `chain_snapshot_installed` fact.
        self.application = state;
        self.staged_snapshot = Some(state);
        Ok(Some(Slot(at)))
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
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty_list.clone()
    }
}

/// Simulation hooks for driver decisions that process-level attrition cannot
/// reach. Every behavior has its own `BUGGIFY` location, so activation is
/// independent and replayable. All hooks turn off with the chaos window, leaving
/// the settle tail genuinely quiet for convergence.
///
/// Every method is consulted from the driver's node loop and nowhere else.
/// That is load-bearing for replay, not incidental: a BUGGIFY decision is a
/// randomness draw, and a draw taken inside a detached task can outlive its
/// simulation and shift the *next* run's stream (see `PeerMailbox` in
/// `paros::driver` for the CI failure that proved it).
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
        // Only a perturbing node draws: the scripted corpus must not spend
        // randomness it never uses (the same rule as `StorageFaults::new`).
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = if enabled {
            buggify_knob!(1_u64, 4_u64..11_u64) as f64
        } else {
            1.0
        };
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
                // The chunk-repair pipeline's two durability points (the only
                // durable writes outside the Ready seam machinery), each its
                // own independently selectable location.
                Seam::BeforeChunkSync => buggify_with_prob!(prob),
                Seam::AfterChunkRestoreBeforeSync => buggify_with_prob!(prob),
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

    fn overtake_in_mailbox(&self, _to: NodeId, _msg: &Message) -> bool {
        // Per message on a non-empty mailbox; a per-peer stream is otherwise
        // delivered in enqueue order, so this is the only in-stream reorder.
        let fired = self.active() && buggify_with_prob!(0.02);
        if fired {
            // BUGGIFY pairing: the overtake genuinely fires.
            assert_reachable!("mailbox: a message overtakes its peer queue");
        }
        fired
    }

    fn hold_peer_delivery(&self, _to: NodeId) -> bool {
        // Per enqueue onto a non-empty mailbox, arming the next drain — and
        // the arm is a *latch*, so this rate does not compose the way a
        // per-drain rate would: a leader that enqueues a dozen messages in one
        // tick rolls this a dozen times and the arms collapse into one hold.
        // The effective per-drain hold frequency is therefore far above the
        // per-call rate, which is why the per-call rate is an order of
        // magnitude below the drain-side rate this started as. Holding most
        // drains would halve per-peer throughput for the whole chaos window —
        // a partition in disguise (moonpool's job) rather than a delay. One
        // tick per hold is the bound, so the backlog one hold builds is
        // exactly one tick's traffic: enough to cross the shed threshold,
        // never enough to wedge a link.
        let fired = self.active() && buggify_with_prob!(0.01);
        if fired {
            // BUGGIFY pairing: a drain genuinely parked for a tick.
            assert_reachable!("mailbox: a peer drain is held for a tick");
        }
        fired
    }

    fn reverse_delivery_batch(&self, _to: NodeId) -> bool {
        // Per enqueue that makes a reorderable batch possible — the drain-side
        // twin of `overtake_in_mailbox`. Same latch composition as
        // `hold_peer_delivery`, and the ceiling matters more here: the arm
        // survives until a batch with something to reorder actually drains, so
        // a rate that arms on most ticks reverses *most* batches, which makes
        // the per-peer stream systematically backwards instead of occasionally
        // so — a fixed reordering the protocol could be tuned around rather
        // than the sporadic one it has to tolerate.
        let fired = self.active() && buggify_with_prob!(0.01);
        if fired {
            // BUGGIFY pairing: a delivery batch genuinely arrives reversed.
            assert_reachable!("mailbox: a delivery batch is reversed");
        }
        fired
    }

    fn skip_snapshot_offer(&self, _to: NodeId) -> bool {
        // Consulted only when an offer is about to go out. Skipping costs the
        // requester one beat — it re-asks every tick, and any other custodian
        // may answer — so the rate can be generous: the state worth reaching is
        // "nobody served me this round", and a below-floor node needs a
        // snapshot offer rarely enough that a shy rate would never build a
        // streak of unserved beats.
        let fired = self.active() && buggify_with_prob!(0.25);
        if fired {
            // BUGGIFY pairing: a snapshot offer is genuinely withheld.
            assert_reachable!("the driver skips a snapshot offer beat");
        }
        fired
    }

    fn stretch_tick_interval(&self) -> bool {
        // Per tick, per node. Deliberately shy: every core timeout is counted
        // in ticks, so a node that stretches most of its ticks runs its whole
        // protocol clock at half speed for the chaos window — an election
        // timeout that never fires relative to its peers' is a stalled node,
        // not a slow one. At this rate a node loses a handful of ticks across
        // the window, which is enough to desynchronize the cluster's protocol
        // clocks (the shape moonpool's clock skew reaches only for the *wall*
        // clock) without any node falling permanently behind. Off after the
        // cutoff, so the recovery tail runs at the honest cadence.
        let fired = self.active() && buggify_with_prob!(0.05);
        if fired {
            // BUGGIFY pairing: a node genuinely ticked at the stretched cadence.
            assert_reachable!("a node stretches its tick interval");
        }
        fired
    }

    fn evict_across_kinds(&self, _to: NodeId, _msg: &Message) -> bool {
        // Per overflow. Kept occasional on purpose: a *systematic* cross-kind
        // eviction is the starvation `PeerMailbox`'s per-kind default exists
        // to prevent (a class crowded out on every round trip), and the point
        // here is to prove the liveness argument survives sporadic pressure,
        // not to reinstate the bug as a fault model.
        let fired = self.active() && buggify_with_prob!(0.10);
        if fired {
            // BUGGIFY pairing: a full mailbox genuinely evicted across kinds.
            assert_reachable!("mailbox: overflow evicts across kinds");
        }
        fired
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }

    fn initiate_handoff(&self, ctx: HandoffContext) -> bool {
        if !self.active() {
            return false;
        }
        // Three independent locations, one per *shape* of transfer, rather
        // than one uniform draw, biased toward the hard states: a handoff
        // carrying unfinished business — an accepted-but-unchosen tail, or a
        // leader still healing a hole of its own — is the interesting one, and
        // it fires an order of magnitude more often than the clean case. The
        // clean case stays armed (a settled handoff is the common production
        // shape and must keep working), just rarer, so it never crowds the
        // hard states out.
        //
        // The rates sit in the same range as `resign_leadership` (0.004), not
        // an order above it, and that ceiling is load-bearing. A handoff
        // *replaces* an election rather than adding to it, so an aggressive
        // rate does not merely add coverage — it becomes the dominant way
        // leadership moves and starves every campaign that needs a settled
        // cluster to reach its own rare state. `ctx.healing` is the trap:
        // it reads true for any leader holding a pipelined slot decided out of
        // order, which is the ordinary streaming state rather than a rare one,
        // so a high probability there is effectively a high *unconditional*
        // rate. At 0.30 it moved leadership every few ticks, which pushed the
        // budget-off (WAITED-leg) axis into `convergence_timeout` and left its
        // "no clean copy of a committed item remains" gate unreached.
        //
        // Consulted only when the core says the leadership is transferable, so
        // every `true` here has an observable effect.
        let fired = if ctx.healing {
            buggify_with_prob!(0.03)
        } else if !ctx.settled {
            buggify_with_prob!(0.02)
        } else {
            buggify_with_prob!(0.002)
        };
        if fired {
            // BUGGIFY pairing: each shape genuinely fires on some seed. Split
            // in three so saturation cannot hide a shape behind another's
            // samples (a run that only ever hands over settled leaderships
            // never exercises the inherited-recovery path at all).
            if ctx.healing {
                assert_reachable!("a handoff leaves a leader that is still healing a hole");
            } else if ctx.settled {
                assert_reachable!("a handoff leaves a fully settled leader");
            } else {
                assert_reachable!("a handoff carries an accepted-but-unchosen tail");
            }
        }
        fired
    }

    fn handoff_target(&self, candidates: &[NodeId]) -> Option<NodeId> {
        if !self.active() || candidates.is_empty() {
            return None;
        }
        // Target selection is its own location: the driver's own randomized
        // pick is uniform, and this occasionally overrides it with the
        // *lowest*-id candidate instead, so a seed can concentrate repeated
        // handoffs on one successor (the chain A -> B -> A -> B a uniform draw
        // spreads out). Every candidate is equally valid — the successor
        // validates the transfer itself — so this only steers which valid
        // state the run explores.
        if buggify_with_prob!(0.5) {
            assert_reachable!("a handoff target is chosen by the pinning selector");
            return candidates.first().copied();
        }
        None
    }

    fn shortest_election_timeout(&self) -> bool {
        self.active() && buggify_with_prob!(0.5)
    }

    fn longest_election_timeout(&self) -> bool {
        // Only consulted when the shortest hook stayed quiet, so the two
        // jitter extremes are independent locations that never both apply.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the high jitter extreme genuinely fires (the
            // audit's `election_timeout_extreme` reach gate belongs to the
            // shortest extreme).
            assert_reachable!("the driver selects the longest valid election timeout");
        }
        fired
    }

    fn skip_snap_advertisement(&self) -> bool {
        // Consulted only when an advertisement is due; skipping loses one
        // custody beat toward the leader's truncation-coupling tally.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the advertisement-pacing location fires.
            assert_reachable!("the driver skips a snapshot custody advertisement");
        }
        fired
    }

    fn skip_chunk_pull(&self) -> bool {
        // Consulted only when rotted chunks are pending; skipping delays the
        // repair one beat and stretches the faulty window.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the chunk-pull pacing location fires.
            assert_reachable!("the driver skips a chunk-repair pull beat");
        }
        fired
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
            // The pull direction of catch-up: a lost request starves the
            // lagging node one beat; the next tick re-asks.
            Message::CatchUpRequest { .. } => buggify_with_prob!(0.10),
            // The snap-repair plane, one location per kind: a lost custody
            // ack delays the leader's truncation-coupling tally; a lost chunk
            // request/response stretches the faulty-chunk window one beat.
            Message::SnapAck { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkRequest { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkResponse { .. } => buggify_with_prob!(0.10),
            // The whole handoff, lost in one message. The correctness claim is
            // that this costs *availability only*: the outgoing leader has
            // already stepped down, so the cluster simply has no leader until
            // an ordinary Phase 1 elects one. Aggressive, because that fallback
            // is the path that must always work.
            Message::Relinquish { .. } => buggify_with_prob!(0.25),
            // Aggressive like the Nack location. Inert today — `CheckLeader`
            // is a tick-injected self-event that never crosses the transport —
            // but armed so a future remote leader probe is born chaos-covered.
            Message::CheckLeader { .. } => buggify_with_prob!(0.25),
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
            // A duplicated catch-up request must only cost a redundant reply.
            Message::CatchUpRequest { .. } => buggify_with_prob!(0.10),
            // A re-delivered handoff must be a no-op at its addressee (never an
            // allocator rewind) and refused everywhere else — the structural
            // half of authority uniqueness, kept honest by firing it often.
            Message::Relinquish { .. } => buggify_with_prob!(0.25),
            // The snap-repair plane must stay idempotent: the leader's custody
            // tally is a set, and a re-delivered chunk response finds its
            // chunks no longer pending. One location per kind keeps the two
            // idempotency claims independently selectable.
            Message::SnapAck { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkRequest { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkResponse { .. } => buggify_with_prob!(0.10),
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
        // live lost-write/corruption mark or a terminally parked node makes a
        // copy unclean. The budget stays conservative (truncated peers don't
        // count there); this independent count is what an unavailable run is
        // judged against.
        let unclean: BTreeSet<&String> = guard
            .marks
            .iter()
            .filter(|(_, marks)| marks.contains(&slot))
            .map(|(node, _)| node)
            .chain(guard.parked.iter())
            .collect();
        if guard.cluster_size.saturating_sub(unclean.len()) < quorum {
            // Red-path diagnostic: name the slot and the unclean set, so an
            // availability violation is attributable without a re-run.
            eprintln!(
                "clean-quorum lost: slot={slot} unclean={unclean:?} parked={:?} marks={:?}",
                guard.parked, guard.marks
            );
            stats.clean_quorum_everywhere = false;
        }
    }
    stats
}

/// Slots an unbudgeted (corpus) run drove to zero readable copies; empty on
/// the budgeted main campaign.
pub(crate) fn unrecoverable_slots(handle: &StateHandle) -> BTreeSet<u64> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.unrecoverable.clone()
}

/// The IPs of nodes terminally parked by a detected persistent corruption
/// (detect ⇒ crash, stays down). The workload's convergence probe skips
/// exactly these — the availability cost Stage 7's baseline deliberately pays,
/// bounded by the dead-node budget so the cluster keeps serving.
pub(crate) fn parked_nodes(handle: &StateHandle) -> BTreeSet<String> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.parked.clone()
}

// --- #113 corpus support: scripted lifecycle + targeted mask injection --------

/// Bump `ip`'s restart epoch: the corpus node loop drops its live incarnation
/// (a clean crash) and re-restores from the world's durable records.
pub(crate) fn corpus_restart_node(handle: &StateHandle, ip: &str) {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.restart_epochs.entry(ip.to_string()).or_insert(0) += 1;
}

/// Hold `ip` down: drop its live incarnation now and keep it down until
/// [`corpus_release_node`].
pub(crate) fn corpus_hold_node(handle: &StateHandle, ip: &str) {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.held.insert(ip.to_string());
    *guard.restart_epochs.entry(ip.to_string()).or_insert(0) += 1;
}

/// Release a held node: its next loop pass re-restores from the world.
pub(crate) fn corpus_release_node(handle: &StateHandle, ip: &str) {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.held.remove(ip);
}

/// One node's durable evidence, for the corpus workloads' deterministic waits
/// (world-truth probes: replication, floors, and the durable application
/// state — the corpus still verifies final outcomes over live RPC reads).
pub(crate) struct CorpusDiskProbe {
    /// Retained accepted slots whose record reads back clean and witnessed.
    pub(crate) clean_slots: BTreeSet<u64>,
    /// The durable compaction floor.
    pub(crate) floor: u64,
    /// The durable application state's applied count.
    pub(crate) applied_count: u64,
    /// The durable application state's chain digest.
    pub(crate) chain_hash: u64,
    /// The retained decided snapshot point, if any (#101).
    pub(crate) snap_point: Option<u64>,
    /// The retained point's rotted chunk indexes.
    pub(crate) faulty_chunks: BTreeSet<u32>,
}

pub(crate) fn corpus_disk_probe(handle: &StateHandle, ip: &str) -> Option<CorpusDiskProbe> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.disks.get(ip).map(|disk| CorpusDiskProbe {
        clean_slots: disk
            .accepted
            .keys()
            .filter(|slot| disk.slot_health(**slot).clean())
            .map(|slot| slot.0)
            .collect(),
        floor: disk.first_slot.0,
        applied_count: disk.chain.applied_count,
        chain_hash: disk.chain.chain_hash,
        snap_point: disk.snap_point.map(|(at, _)| at),
        faulty_chunks: disk
            .snap_chunk_health
            .iter()
            .enumerate()
            .filter(|(_, health)| **health != RecordHealth::Clean)
            .map(|(index, _)| u32::try_from(index).unwrap_or(u32::MAX))
            .collect(),
    })
}

/// Targeted chunk corruption of the retained decided snapshot point (#101):
/// one chunk's value lost, the point's identity — and every other chunk —
/// intact. Unbudgeted like every corpus injection; below-floor slots are
/// re-evaluated against the world's unrecoverable ground truth (the point is
/// custody, so losing its last clean copy can strand a folded prefix).
pub(crate) fn corpus_corrupt_snap_chunk(
    handle: &StateHandle,
    ip: &str,
    node: u64,
    chunk: u32,
) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let (at, floor) = {
        let Some(disk) = guard.disks.get_mut(ip) else {
            return false;
        };
        let Some((at, state)) = disk.snap_point else {
            return false;
        };
        let chunks = usize::try_from(snap_chunk_count(state.encode().len())).unwrap_or(0);
        let Some(index) = usize::try_from(chunk).ok().filter(|index| *index < chunks) else {
            return false;
        };
        if disk.snap_chunk_health.len() < chunks {
            disk.snap_chunk_health.resize(chunks, RecordHealth::Clean);
        }
        if disk.snap_chunk_health[index] != RecordHealth::Clean {
            return false;
        }
        disk.snap_chunk_health[index] = RecordHealth::Faulty;
        (at, disk.first_slot.0)
    };
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::SnapChunk(Slot(at), chunk),
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    for slot in 0..floor {
        guard.note_if_unrecoverable(slot);
    }
    true
}

/// Targeted E1 mask corruption of one accepted record — value lost, identity
/// preserved (the recoverable class). Deliberately unbudgeted: the world
/// records the unrecoverable ground truth the analytic mask derivation
/// cross-checks. Returns whether a clean record was
/// there to corrupt.
pub(crate) fn corpus_corrupt_entry(handle: &StateHandle, ip: &str, node: u64, slot: u64) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(disk) = guard.disks.get_mut(ip) else {
        return false;
    };
    if !disk.accepted.contains_key(&Slot(slot)) || !disk.slot_health(Slot(slot)).clean() {
        return false;
    }
    disk.entry_health.insert(
        Slot(slot),
        SlotHealth {
            entry: RecordHealth::Faulty,
            id: WitnessStatus::Present,
        },
    );
    guard.marks.entry(ip.to_string()).or_default().insert(slot);
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::Accepted(Slot(slot)),
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    guard.note_if_unrecoverable(slot);
    true
}

/// Targeted corruption of one node's durable application snapshot. Slots the
/// node had truncated past lose their only local custody, so each is
/// re-evaluated against the world's unrecoverable ground truth.
pub(crate) fn corpus_corrupt_snapshot(handle: &StateHandle, ip: &str, node: u64) -> bool {
    let world = storage_world(handle);
    let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(disk) = guard.disks.get_mut(ip) else {
        return false;
    };
    disk.snapshot_health = RecordHealth::Faulty;
    let floor = disk.first_slot.0;
    guard.note_corruption(CorruptionInjection {
        node,
        record: StorageRecord::Snapshot,
        kind: CorruptionKind::BitFlip,
        block: false,
        outcome: CorruptionOutcome::Dormant,
    });
    for slot in 0..floor {
        guard.note_if_unrecoverable(slot);
    }
    true
}

/// Snapshot of the Stage-7 corruption ground truth, folded for `check()`.
pub(crate) struct CorruptionStats {
    /// Corruption injections recorded this run, in total.
    pub(crate) injected: usize,
    /// Ledger entries whose read surfaced as the one typed crash decision.
    pub(crate) crashed: u64,
    /// Every ledger entry is accounted for: resolved, or legitimately dormant
    /// (on a parked node the scan never re-reads, or a torn tail the run
    /// ended before rebooting).
    pub(crate) accounted: bool,
    /// Terminally parked nodes, and whether they stayed within the dead-node
    /// budget (a live quorum survives).
    pub(crate) parked: usize,
    pub(crate) parked_within_budget: bool,
    /// Sticky per-family / per-verdict gate facts.
    flags: Stage7Flags,
}

pub(crate) fn corruption_stats(handle: &StateHandle) -> CorruptionStats {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut crashed = 0_u64;
    let mut accounted = true;
    for injection in &guard.corruptions {
        match injection.outcome {
            CorruptionOutcome::Crashed => crashed += 1,
            // Exercised-vs-dormant tracking (the oracle must not overclaim):
            // an injected fault on a never-read record is not a detection
            // failure — but "never read" must be genuine: the node parked
            // before the scan could reach this record, or a torn tail the run
            // ended before rebooting through.
            CorruptionOutcome::Dormant => {
                let legal = injection.kind == CorruptionKind::TornTail
                    || guard.parked_ids.contains(&injection.node);
                if !legal {
                    accounted = false;
                }
            }
            CorruptionOutcome::CoDetected
            | CorruptionOutcome::Repaired
            | CorruptionOutcome::DiscardedTail
            // Stage 8: a standing report (a run may end mid-repair, or WAITED)
            // and a genuinely re-written record are both fully accounted.
            | CorruptionOutcome::Reported
            | CorruptionOutcome::Recovered => {}
        }
    }
    CorruptionStats {
        injected: guard.corruptions.len(),
        crashed,
        accounted,
        parked: guard.parked.len(),
        parked_within_budget: guard.parked.len() <= guard.dead_budget(),
        flags: guard.s7,
    }
}

/// The storage-fault coverage gates + the injected↔detected correlation,
/// evaluated once per run from the workload's `check()` (the shared-gate
/// doctrine in [`crate::audit`]).
pub(crate) fn check_storage_gates(handle: &StateHandle) {
    let stats = storage_fault_stats(handle);
    let corruption = corruption_stats(handle);
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
    check_corruption_gates(handle, &corruption);
    // The #71 compound gate: corruption x partition x a lagging follower
    // reached in one run (the network swarm IS the partition; lag is the
    // audit's observed fact). It used to ride the safety-only axis, which was
    // the only one carrying swarm network turbulence; since #126 folded that
    // turbulence into the main campaign, the main campaign is where the
    // compound is reachable and where it must saturate.
    assert_sometimes!(
        corruption.injected > 0 && audit_world(handle).lag_observed(),
        "storage: corruption compounds with a partition and a lagging follower"
    );
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
}

/// The Stage-7 half of the storage oracle (issue #20 F): the exercised ⇔
/// detected correlation, the dead-node budget, and the per-family /
/// per-verdict coverage gates.
fn check_corruption_gates(handle: &StateHandle, corruption: &CorruptionStats) {
    // Every exercised corruption is detected as exactly one typed crash
    // decision — a spontaneous corruption detection (nothing injected) or a
    // swallowed injection both break the count — and every injection is
    // accounted for (crashed, co-detected, repaired, discarded, or genuinely
    // never read).
    let corruption_crashes = audit_world(handle).corruption_faults_detected();
    assert_always!(
        corruption_crashes == corruption.crashed,
        "storage: every exercised corruption is exactly one typed crash decision",
        {
            "ledger_crashed" => corruption.crashed,
            "detected" => corruption_crashes
        }
    );
    assert_always!(
        corruption.accounted,
        "storage: every corruption injection is detected, resolved, or genuinely unread"
    );
    // The availability cost of detect ⇒ crash is bounded a priori: a
    // corruption-parked minority never costs the cluster its live quorum.
    assert_always!(
        corruption.parked_within_budget,
        "storage: corruption never parks a quorum of nodes",
        { "parked" => u64::try_from(corruption.parked).unwrap_or(u64::MAX) }
    );
    // Stage-7 family gates (issue #20 D): one per injected fault family,
    // proving the sweep genuinely visits each detection channel.
    let s7 = corruption.flags;
    assert_sometimes!(
        s7.bitflip_detected,
        "storage: a bit-flip corruption is detected on read"
    );
    assert_sometimes!(
        s7.lost_write_detected,
        "storage: a lost write is detected by the reserved-record contract"
    );
    assert_sometimes!(
        s7.misdirected_detected,
        "storage: a misdirected write is caught by the identity check"
    );
    assert_sometimes!(
        s7.read_eio_detected,
        "storage: a read EIO degrades to a checksum mismatch"
    );
    assert_sometimes!(
        s7.torn_tail_discarded,
        "storage: a crash-truncatable tail is discarded on boot"
    );
    assert_sometimes!(
        s7.snapshot_detected,
        "storage: snapshot corruption is detected as its own record"
    );
    assert_sometimes!(
        s7.promise_repaired,
        "storage: one bad promise copy is repaired from its twin"
    );
    assert_sometimes!(
        s7.metadata_crashed,
        "storage: an fs-metadata fault reliably crashes the node"
    );
    // Per-verdict gates (issue #20 C): the disentanglement table's ambiguous
    // rows are genuinely reached, not just the happy path.
    assert_sometimes!(
        s7.corruption_below_tail,
        "storage: corruption below the tail is classified"
    );
    assert_sometimes!(
        s7.last_entry_ambiguity,
        "storage: the last-entry ambiguity is classified"
    );
    assert_sometimes!(
        s7.identifier_lost,
        "storage: an identifier lost with its entry is classified"
    );
    // Stage 8 (issue #21): the recover-or-wait flip is genuinely exercised —
    // rotted-but-identified records are *reported* (the node keeps serving)
    // and genuinely *repaired* by the cluster, not merely classified.
    assert_sometimes!(
        s7.faulty_reported,
        "storage: a rotted record is reported faulty and the node keeps serving"
    );
    assert_sometimes!(
        s7.record_recovered,
        "storage: a reported faulty record is repaired by the cluster"
    );
}
