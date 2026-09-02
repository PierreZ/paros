//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. It
//! derives a cluster-consistent membership from the topology, wires the node to a
//! per-node handle on the shared [`StorageWorld`] (the sim's stand-in for durable
//! disk), and runs the same `run_node` a production `tokio::main` would — inside a
//! recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: `run_node` unwinds, the volatile `RawNode` is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`]. A process kill
//! — moonpool attrition on the main campaign, the scripted lifecycle on the
//! corpus — aborts the task outright; the next incarnation restores the same
//! way.
//!
//! [`StorageWorld`]: crate::world::StorageWorld

use std::net::IpAddr;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, TimeProvider, assert_reachable, buggify_knob,
};

use crate::audit::{AuditWorld, NodeAudit, audit_world};
use crate::hooks::BuggifyHooks;
use crate::world::storage::{DurableStorage, StorageFaults};
use crate::world::storage_world;
use paros::{Config, DriverTunables, NodeId, RunError, parse_addr, run_node};

/// The wall-clock floor of every driver timeout that races the network: one
/// Phase-1 round trip over moonpool's default cross-datacenter link plus one
/// delivery batch, i.e. production's `5 ticks × 50 ms`. See the knob block in
/// [`NodeProcess::run`] for why it is a floor and not a tunable.
const ROUND_TRIP_FLOOR_MS: u64 = 250;

/// A paros node in the simulation.
pub(crate) struct NodeProcess {
    mode: NodeMode,
}

/// How a node is perturbed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeMode {
    /// The main campaign: the driver's BUGGIFY hooks, the disk's fault sites
    /// and the transport knobs are all live inside the chaos window.
    Chaotic,
    /// The corpus: every fault is a targeted injection from the workload, so
    /// the swarm sites and the hooks stay dark (a case replays as
    /// choreographed) and the world runs unbudgeted (masks may exceed the
    /// per-record budget; the world records the unrecoverable ground truth).
    Scripted,
}

impl NodeProcess {
    pub(crate) fn chaotic() -> Self {
        Self {
            mode: NodeMode::Chaotic,
        }
    }

    pub(crate) fn scripted() -> Self {
        Self {
            mode: NodeMode::Scripted,
        }
    }
}

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
        let perturb = self.mode == NodeMode::Chaotic;
        {
            let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
            guard.set_cluster_size(members.len());
            if perturb {
                // The application's snapshot size, drawn once per run (the
                // first node to boot fixes it): 1 to 128 digest lanes is a
                // blob of 1 to 17 chunks, so the chunk-repair plane sees the
                // single-chunk and the many-chunk shapes instead of always
                // five. Floor 1: one lane is a complete, valid application.
                guard.set_lane_count(buggify_knob!(crate::chain::DEFAULT_LANES, 1_u8..129_u8));
            } else {
                guard.set_unbudgeted();
                guard.set_lane_count(crate::chain::DEFAULT_LANES);
            }
        }
        let hooks = BuggifyHooks::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            perturb,
        );
        // The budgeted storage-fault layer (issue #19 B/C) shares the driver
        // hooks' chaos window: after the cutoff the world stops injecting
        // **new** faults but never heals the consequences of old ones —
        // recovery through the tail must be genuine.
        let faults = StorageFaults::new(
            ctx.time().clone(),
            Duration::from_millis(crate::CHAOS_DURATION_MS),
            perturb,
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
        let tunables = if perturb {
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
            // The remaining knobs, one location each (AGENTS.md: a seed may
            // be extreme in one dimension and ordinary in the next). Every
            // duration that races the network has the same structural floor,
            // `ROUND_TRIP_FLOOR_MS`: moonpool's default cross-datacenter link
            // is 20-80 ms one way, so a Phase-1 round trip plus one delivery
            // batch is ~250 ms — production's own `5 ticks × 50 ms`. An
            // election timeout below it makes a candidate abandon its own
            // round before its promises return (witness: base 3 × 50 ms on a
            // 4-node cluster campaigned 1,185 times in 80 s and never once
            // collected a quorum); a keep-alive, connect or delivery timeout
            // below it kills every stream on every round trip. Both are a
            // permanent partition wearing a knob's clothes, not a
            // configuration. The tick itself may go fast (a 10 ms tick is 25
            // heartbeats per round trip); the tick-counted timeouts are then
            // raised to keep their wall-clock floor. The ranges still cross
            // the client's knobbed deadline (350 ms..3 s) in both directions:
            // a node slower than the client's patience is a valid, ambiguous
            // outcome, never a wrong one.
            let ms = Duration::from_millis;
            let tick_ms = buggify_knob!(50_u64, 10_u64..201_u64);
            let floor_ticks = ROUND_TRIP_FLOOR_MS.div_ceil(tick_ms);
            DriverTunables {
                tick_interval: ms(tick_ms),
                election_timeout_base: buggify_knob!(5_u64, 2_u64..13_u64).max(floor_ticks),
                keep_alive_interval: ms(buggify_knob!(2000_u64, ROUND_TRIP_FLOOR_MS..5001_u64)),
                keep_alive_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
                connection_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
                delivery_timeout: ms(buggify_knob!(1000_u64, ROUND_TRIP_FLOOR_MS..3001_u64)),
                read_retry_ticks: buggify_knob!(10_u64, 1_u64..41_u64).max(floor_ticks),
                snapshot_queue_capacity: buggify_knob!(4_usize, 1_usize..9_usize),
                client_inbox_capacity: buggify_knob!(256_usize, 1_usize..17_usize),
                peer_inbox_capacity: buggify_knob!(1024_usize, 1_usize..65_usize),
                peer_queue_capacity,
                delivery_batch,
            }
        } else {
            // Scripted runs keep the production transport shape: every
            // perturbation on that axis is a targeted injection.
            DriverTunables::default()
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
