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
//!
//! [`StorageWorld`]: crate::world::StorageWorld

use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, TimeProvider, assert_reachable, buggify_knob,
};

use crate::audit::{AuditWorld, NodeAudit, audit_world};
use crate::hooks::BuggifyHooks;
use crate::world::storage::{DurableStorage, StorageFaults};
use crate::world::{StorageWorld, storage_world};
use paros::{Config, DriverTunables, NodeId, RunError, parse_addr, run_node};

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
        guard.set_unbudgeted();
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
                (guard.is_held(&my_ip), guard.is_parked(&my_ip))
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
        let boot_epoch = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .restart_epoch(&my_ip);
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
                    .is_parked(&my_ip);
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
            .restart_epoch(ip);
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
                .is_parked(&my_ip)
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
                        .is_parked(&my_ip);
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
        let faults = StorageFaults::new(ctx.time().clone(), Duration::ZERO, false);
        let config = Config {
            id: NodeId(0),
            peers: vec![NodeId(0)],
            ..Config::default()
        };
        let mut instance = 0_u64;
        // No client, no protocol: each fresh store is its own one-node
        // "cluster" with a private checker that still runs every
        // per-transition storage check.
        let fresh = || {
            instance += 1;
            DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                format!("10.9.9.{instance}"),
                100 + instance,
                faults.clone(),
                Arc::new(AuditWorld::client_free()),
            )
        };
        // A reopen is a clean reboot of the same store: drop the handle and
        // re-restore from the world's durable records under the same key, and
        // under the same checker (the boot replay re-walks its applied prefix).
        let reopen = |old: DurableStorage<_>| {
            let (key, node_id, checker) = (old.key.clone(), old.node_id, old.checker.clone());
            drop(old);
            DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                key,
                node_id,
                faults.clone(),
                checker,
            )
        };
        paros::storage_contract_suite(fresh, reopen);
        Ok(())
    }
}
