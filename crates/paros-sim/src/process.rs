//! The sim-side adapter: the moonpool [`Process`]es that run the
//! provider-generic [`paros::run_node`] and [`paros::run_matchmaker`] drivers
//! under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. Each
//! role is its own moonpool process group (`crate::roles`): a [`NodeProcess`]
//! — an **acceptor** — wires the node to a per-node handle on the shared
//! [`StorageWorld`] (the sim's stand-in for durable disk) and runs the same
//! `run_node` a production `tokio::main` would; a [`MatchmakerProcess`] runs
//! `run_matchmaker` over its own slice of the same world. Both sit inside a
//! recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: the driver unwinds, the volatile core is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`]. A process kill
//! — moonpool attrition on the main campaign, the scripted lifecycle on the
//! corpus — aborts the task outright; the next incarnation restores the same
//! way.
//!
//! [`StorageWorld`]: crate::world::StorageWorld

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationError, SimulationResult, TimeProvider, assert_always,
    assert_reachable, buggify_knob,
};

use crate::audit::{AuditWorld, NodeAudit, audit_world};
use crate::hooks::BuggifyHooks;
use crate::roles::{ACCEPTOR_GROUP, Deployment, MATCHMAKER_GROUP, Role};
use crate::world::matchmaker::DurableMatchmakerStorage;
use crate::world::storage::{DurableStorage, StorageFaults, WritePathRates};
use crate::world::storage_world;
use paros::{
    Config, MatchmakerConfig, MatchmakerId, NodeId, RunError, parse_addr, run_matchmaker, run_node,
};

/// A paros node (an acceptor) in the simulation.
pub(crate) struct NodeProcess {
    mode: NodeMode,
    /// A scripted case's fixed bootstrap size (`Some(n)`: ranks `0..n` are
    /// the bootstrap acceptors, the rest spares); `None` draws per the mode.
    bootstrap: Option<usize>,
}

/// How a process is perturbed.
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
            bootstrap: None,
        }
    }

    pub(crate) fn scripted() -> Self {
        Self {
            mode: NodeMode::Scripted,
            bootstrap: None,
        }
    }

    /// A scripted node whose bootstrap configuration is the first
    /// `bootstrap` ranks of the pool — a corpus case that needs a spare.
    pub(crate) fn scripted_with_bootstrap(bootstrap: usize) -> Self {
        Self {
            mode: NodeMode::Scripted,
            bootstrap: Some(bootstrap),
        }
    }
}

/// A matchmaker in the simulation: its own process group, so a seed draws
/// how many it deploys independently of the acceptor pool and attrition can
/// be scoped to it.
pub(crate) struct MatchmakerProcess {
    /// Whether the driver hooks, the shape knobs and the loss coin are live
    /// (the main campaign) or dark (a scripted corpus case).
    perturb: bool,
}

impl MatchmakerProcess {
    pub(crate) fn chaotic() -> Self {
        Self { perturb: true }
    }

    pub(crate) fn scripted() -> Self {
        Self { perturb: false }
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        ACCEPTOR_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The deployment map is read off the topology's process groups, so
        // every process derives the *same* map without coordination.
        let my_ip = ctx.my_ip().to_string();
        let deployment = crate::roles::deployment(ctx.topology());
        let perturb = self.mode == NodeMode::Chaotic;
        match deployment.role_of(&my_ip) {
            Some(Role::Acceptor(self_rank)) => {
                run_acceptor(ctx, &deployment, self_rank, &my_ip, perturb, self.bootstrap).await
            }
            other => {
                assert_always!(
                    false,
                    "every node process is mapped to the acceptor role",
                    { "ip" => my_ip.as_str(), "role" => format!("{other:?}") }
                );
                Err(SimulationError::InvalidState(format!(
                    "{my_ip} is not an acceptor of the deployment"
                )))
            }
        }
    }
}

#[async_trait]
impl Process for MatchmakerProcess {
    fn name(&self) -> &'static str {
        MATCHMAKER_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let my_ip = ctx.my_ip().to_string();
        let deployment = crate::roles::deployment(ctx.topology());
        match deployment.role_of(&my_ip) {
            Some(Role::Matchmaker(id)) => run_matchmaker_role(ctx, id, &my_ip, self.perturb).await,
            other => {
                assert_always!(
                    false,
                    "every matchmaker process is mapped to the matchmaker role",
                    { "ip" => my_ip.as_str(), "role" => format!("{other:?}") }
                );
                Err(SimulationError::InvalidState(format!(
                    "{my_ip} is not a matchmaker of the deployment"
                )))
            }
        }
    }
}

/// An acceptor: the provider-generic node driver inside the crash/recovery
/// loop (see the module doc).
// One recovery loop with per-exit-kind handling; splitting the arms would
// scatter the crash/park/restart contract this function *is*.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_rank.0))]
async fn run_acceptor(
    ctx: &SimContext,
    deployment: &Deployment,
    self_rank: NodeId,
    my_ip: &str,
    perturb: bool,
    fixed_bootstrap: Option<usize>,
) -> SimulationResult<()> {
    // The node pool is the map's acceptor list, in `NodeId` order — never
    // "every process in the topology". The matchmaker set is the map's
    // matchmaker list, empty on a plain seed. The bootstrap membership is
    // protocol data drawn once per seed (`crate::shape::bootstrap_ranks`):
    // the whole pool by default, and on a matchmaker seed possibly a subset
    // that leaves spares for a reconfiguration to pull in.
    let members = deployment
        .acceptors()
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            parse_addr(ip)
                .map(|addr| (NodeId(u64::try_from(i).expect("node index fits u64")), addr))
        })
        .collect::<SimulationResult<Vec<_>>>()?;
    let matchmakers = deployment
        .matchmakers()
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            parse_addr(ip).map(|addr| {
                (
                    MatchmakerId(u64::try_from(i).expect("matchmaker index fits u64")),
                    addr,
                )
            })
        })
        .collect::<SimulationResult<Vec<_>>>()?;
    let pool: Vec<NodeId> = members.iter().map(|(id, _)| *id).collect();
    let bootstrap: Vec<NodeId> = match fixed_bootstrap {
        Some(n) => crate::shape::fixed_bootstrap_ranks(ctx.state(), n),
        None => {
            crate::shape::bootstrap_ranks(ctx.state(), pool.len(), !matchmakers.is_empty(), perturb)
        }
    }
    .into_iter()
    .map(NodeId)
    .collect();
    // The matchmaker *pool* is the address book (`matchmakers`, every
    // matchmaker process); the bootstrap matchmaker set (#125) is protocol
    // data drawn once per seed, possibly a subset that leaves spares.
    let matchmaker_pool: Vec<MatchmakerId> = matchmakers.iter().map(|(id, _)| *id).collect();
    let matchmaker_bootstrap: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_pool.len(), perturb)
            .into_iter()
            .map(MatchmakerId)
            .collect();
    let config = Config {
        id: self_rank,
        peers: bootstrap.clone(),
        nodes: pool,
        matchmakers: matchmaker_bootstrap,
        matchmaker_pool,
        ..Config::default()
    };

    // The per-iteration durable-storage world, shared by every node and
    // surviving crash/restart (it lives in the `StateHandle`, fresh per seed
    // but stable across a process's reboots). Each node reaches it through a
    // `Weak` handle upgraded per op.
    let world = storage_world(ctx.state());
    // This node's shape: every knob the swarm draws *for the node* (the
    // driver tunables, the write-window crash bias, the disk's fault
    // rates), drawn by its first incarnation of the seed and handed back
    // unchanged to every later one — an attrition restart re-enters this
    // function from a fresh factory instance, and the shape is what makes
    // that re-entry a *restart* of the same node rather than a new node
    // with new knobs (see `crate::shape`). Durable Paxos state is the
    // world's business, never the shape's.
    let incarnation = crate::shape::boot(ctx.state(), my_ip, perturb);
    let shape = incarnation.shape;
    {
        let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
        // The copy budget is sized by the run's configuration floor
        // (`crate::shape::config_floor`): the whole pool on a plain seed, the
        // smallest set a reconfiguration may shrink to on a matchmaker seed.
        guard.set_cluster_size(crate::shape::config_floor(
            config.pool().len(),
            config.has_matchmakers(),
        ));
        if !perturb {
            guard.set_unbudgeted();
        }
        guard.set_lane_count(crate::shape::lane_count(ctx.state(), perturb));
    }
    let hooks = BuggifyHooks::new(
        ctx.time().clone(),
        Duration::from_millis(crate::CHAOS_DURATION_MS),
        perturb,
        shape.seam_crash_bias,
    );
    // The budgeted storage-fault layer (issue #19 B/C) shares the driver
    // hooks' chaos window: after the cutoff the world stops injecting
    // **new** faults but never heals the consequences of old ones —
    // recovery through the tail must be genuine.
    let faults = StorageFaults::new(
        ctx.time().clone(),
        Duration::from_millis(crate::CHAOS_DURATION_MS),
        perturb,
        shape.write_rates,
    );
    // The per-iteration shared audit: pure observation, published beside the
    // storage world so every node folds its transitions into one incremental
    // checker. It never influences the driver — that is `hooks`' job.
    let checker = audit_world(ctx.state());
    let audit = NodeAudit::new(ctx.time().clone(), checker.clone());
    let tunables = shape.tunables;
    if incarnation.is_restart() {
        // A process-level revival (attrition on the main campaign, the
        // script on the corpus). Told to the audit so it can judge the
        // overlap this boot may be ending: a node that was down while a
        // peer sat terminally parked (persistent storage loss + transient
        // process loss at once) is the composition that costs a small
        // cluster its quorum until exactly this boot returns it.
        let parked_peers = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .parked_count_excluding(my_ip);
        checker.note_process_restart(
            self_rank.0,
            parked_peers,
            crate::shape::config_floor(config.pool().len(), config.has_matchmakers()),
        );
        // The disk's wipe coin (#124): a restart that comes back on an empty
        // disk. Moonpool's own `prob_wipe` reaches only its storage provider,
        // which paros does not use (the fake disk is the world), so the
        // amnesia fault is the world's, drawn here at the one place a lost
        // disk shows — a reboot. The identity never boots again: a wiped
        // node is replaced through an acceptor reconfiguration, never
        // rejoined (an empty disk under an old identity would answer a
        // Phase 1 with "nothing accepted" for slots it voted on). Only a
        // matchmaker deployment can replace, so the coin is dark on a plain
        // seed; the world's dead-node budget bounds it either way.
        let wipe = perturb
            && config.has_matchmakers()
            && ctx.time().now() < Duration::from_millis(crate::CHAOS_DURATION_MS)
            && moonpool_sim::buggify_with_prob!(0.35);
        if wipe
            && world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .wipe(my_ip, self_rank.0)
        {
            // BUGGIFY pairing: the wipe coin fired within the budget.
            assert_reachable!("storage: a restarted node's disk is wiped and the identity retired");
            checker.note_wiped(self_rank.0);
            tracing::info!(node = self_rank.0, "storage_wiped_exit");
            return Ok(());
        }
    }

    // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
    // drop the volatile node, rebuild storage from the (surviving) world, and
    // re-run — a faithful clean crash + recovery. Attrition (process kill) is
    // handled by the harness; this covers the seams *inside* a Ready batch
    // that attrition cannot reach.
    loop {
        // A node that is down for good never boots again: retired by the
        // operator (#123), wiped (#124), or terminally parked by a detected
        // persistent corruption (attrition may revive the *process*, but the
        // boot scan would re-detect the same rotted record forever, and a
        // second crash report for one detection would break the 1:1
        // injected⇔detected correlation). Exit before touching the store.
        let (parked, wiped, retired) = {
            let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
            (
                guard.is_parked(my_ip),
                guard.is_wiped(my_ip),
                guard.is_retired(my_ip),
            )
        };
        if retired {
            checker.note_retired_boot(self_rank.0);
            tracing::info!(node = self_rank.0, "retired_stays_down");
            return Ok(());
        }
        if wiped {
            checker.note_wiped(self_rank.0);
            tracing::info!(node = self_rank.0, "wiped_stays_down");
            return Ok(());
        }
        if parked {
            checker.note_storage_dead(self_rank.0);
            tracing::info!(node = self_rank.0, "storage_parked");
            return Ok(());
        }
        let storage = DurableStorage::restore(
            config.clone(),
            Arc::downgrade(&world),
            my_ip.to_string(),
            self_rank.0,
            faults.clone(),
            checker.clone(),
        );
        match run_node(
            ctx.providers().clone(),
            storage,
            parse_addr(my_ip)?,
            members.clone(),
            matchmakers.clone(),
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
            // floor and independently exercises snapshot recovery. Drawn
            // per *crash*, deliberately not per node (it is not part of
            // the node's shape): the delay describes one event, and two
            // crashes of the same node should be free to look different.
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
                    .is_parked(my_ip);
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

/// A matchmaker: the provider-generic registry driver inside the same
/// crash/recovery loop as the node — a seam crash unwinds `run_matchmaker`,
/// the volatile `Matchmaker` is dropped, and the next incarnation restores
/// its registry from the durable world.
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = id.0))]
async fn run_matchmaker_role(
    ctx: &SimContext,
    id: MatchmakerId,
    my_ip: &str,
    perturb: bool,
) -> SimulationResult<()> {
    let world = storage_world(ctx.state());
    let deployment = crate::roles::deployment(ctx.topology());
    // Generation 0's set is protocol data drawn once per seed (#125), the
    // same draw every node makes; this matchmaker may be a spare outside it.
    let bootstrap: Vec<MatchmakerId> = crate::shape::matchmaker_bootstrap_ranks(
        ctx.state(),
        deployment.matchmakers().len(),
        perturb,
    )
    .into_iter()
    .map(MatchmakerId)
    .collect();
    let config = MatchmakerConfig {
        id,
        bootstrap: bootstrap.clone(),
    };
    // A matchmaker has a shape too: its transport tunables and its
    // write-window crash bias, drawn once per seed like a node's.
    let incarnation = crate::shape::boot(ctx.state(), my_ip, perturb);
    let shape = incarnation.shape;
    let hooks = BuggifyHooks::new(
        ctx.time().clone(),
        Duration::from_millis(crate::CHAOS_DURATION_MS),
        perturb,
        shape.seam_crash_bias,
    );
    let checker = audit_world(ctx.state());
    let audit = NodeAudit::new(ctx.time().clone(), checker.clone());
    if incarnation.is_restart()
        && ctx.time().now() < Duration::from_millis(crate::CHAOS_DURATION_MS)
        && moonpool_sim::buggify_with_prob!(0.35)
        && world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .park_matchmaker(my_ip, bootstrap.len())
    {
        // The registry's loss coin (#125): a restart that finds its durable
        // state unusable. There is no in-place repair — the registry stays
        // down for good and the surviving quorum reconstructs a successor
        // set without it. BUGGIFY pairing: the coin fired within the budget.
        assert_reachable!("matchmaker: a restarted matchmaker's registry is lost for good");
        checker.note_matchmaker_lost(id.0);
        tracing::info!(matchmaker = id.0, "matchmaker_lost_exit");
        return Ok(());
    }
    loop {
        if world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_matchmaker_parked(my_ip)
        {
            checker.note_matchmaker_lost(id.0);
            tracing::info!(matchmaker = id.0, "matchmaker_stays_down");
            return Ok(());
        }
        let storage = DurableMatchmakerStorage::restore(Arc::downgrade(&world), my_ip.to_string());
        match run_matchmaker(
            ctx.providers().clone(),
            storage,
            parse_addr(my_ip)?,
            config.clone(),
            shape.tunables,
            ctx.shutdown().clone(),
            &hooks,
            &audit,
        )
        .await
        {
            // A seam crash (the registry store injects no faults of its own,
            // so a storage exit takes the same path): rebuild from the
            // durable world, after the matchmaker's own restart-delay knob.
            // Its floor is structural: a matchmaker held down is a
            // matchmaking phase that waits, never a cluster that stalls.
            Err(RunError::SeamCrash(_) | RunError::Storage(_)) => {
                let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                if delay_ms > 0 {
                    // BUGGIFY pairing: the matchmaker restart-delay knob fired.
                    assert_reachable!("a seam-crashed matchmaker restarts after a buggified delay");
                    ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                }
            }
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let world = storage_world(ctx.state());
        world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_cluster_size(1);
        let faults = StorageFaults::new(
            ctx.time().clone(),
            Duration::ZERO,
            false,
            WritePathRates::default(),
        );
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
        // The matchmaker registry's contract, against its world-backed store.
        let mut registry_instance = 0_u64;
        let fresh_registry = || {
            registry_instance += 1;
            DurableMatchmakerStorage::restore(
                Arc::downgrade(&world),
                format!("10.9.8.{registry_instance}"),
            )
        };
        let reopen_registry = |old: DurableMatchmakerStorage| {
            let key = old.key().to_string();
            drop(old);
            DurableMatchmakerStorage::restore(Arc::downgrade(&world), key)
        };
        paros::matchmaker_storage_contract_suite(fresh_registry, reopen_registry);
        // The crash half the shared suite cannot express (an in-memory store
        // has no un-synced stage): a registration or a watermark raise that
        // was staged but never fsynced does not survive the incarnation, so a
        // reboot reads back exactly the last flush — the read-side pair of
        // the driver's persist-before-reply ordering.
        {
            use paros::{
                AcceptorConfig, Ballot, MatchmakerStorage, NodeId, Registration, RegistryStorage,
            };
            let config = Registration::belief(AcceptorConfig::new(
                vec![NodeId(0)],
                paros::QuorumSystem::Majority,
            ));
            let ballot = |round: u64| Ballot {
                round,
                node: NodeId(1),
            };
            let key = "10.9.7.1".to_string();
            let mut store = DurableMatchmakerStorage::restore(Arc::downgrade(&world), key.clone());
            store.register(ballot(1), &config).expect("register 1");
            store.sync().expect("sync 1");
            store
                .register(ballot(2), &config)
                .expect("register 2 (never synced)");
            store
                .set_gc_watermark(ballot(1))
                .expect("raise (never synced)");
            drop(store);
            let rebooted = DurableMatchmakerStorage::restore(Arc::downgrade(&world), key);
            assert_always!(
                rebooted.registered_ballots() == vec![ballot(1)]
                    && rebooted.registration(ballot(2)).is_none(),
                "matchmaker: an un-synced registration does not survive a crash"
            );
            assert_always!(
                rebooted.initial_state().gc_watermark == Ballot::zero(),
                "matchmaker: an un-synced watermark raise does not survive a crash"
            );
        }
        Ok(())
    }
}
