//! The sim-side adapter: the moonpool [`Process`]es that run the
//! provider-generic [`paros::run_node`] and [`paros::run_matchmaker`] drivers
//! under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. Each
//! role is its own moonpool process group (`crate::roles`): a [`NodeProcess`]
//! — an **acceptor** — wires the node to a per-node handle on the shared
//! [`StorageWorld`] (the sim's stand-in for durable disk) and runs the same
//! `run_node` a production `tokio::main` would; a [`MatchmakerProcess`] runs
//! `run_matchmaker` over its own slice of the same world; a [`ProxyProcess`]
//! runs `run_proxy` (#142) with no slice at all — a proxy leader holds
//! nothing durable, so a kill simply reboots it empty; a [`ReplicaProcess`]
//! runs `run_replica` (#144) over its own fault-free disk in the same world,
//! outside the copy budget — a replica is not an acceptor. Every role with a
//! disk sits inside a recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: the driver unwinds, the volatile core is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`]. A process kill
//! — moonpool attrition on the main campaign, the scripted lifecycle on the
//! corpus — aborts the task outright; the next incarnation restores the same
//! way.
//!
//! [`StorageWorld`]: crate::world::StorageWorld

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultFocus, Process, SimContext, SimTimeProvider, SimulationError, SimulationResult,
    TimeProvider, assert_always, assert_reachable, buggify_knob,
};

use crate::audit::journals::{journal_board, lock as board_lock};
use crate::audit::{AuditWorld, NodeAudit, audit_world, audit_world_for};
use crate::hooks::BuggifyHooks;
use crate::roles::{
    ACCEPTOR_GROUP, Deployment, MATCHMAKER_GROUP, PROXY_GROUP, REPLICA_GROUP, Role, replica_node_id,
};
use crate::world::matchmaker::DurableMatchmakerStorage;
use crate::world::node_store::{LedgeredJournal, NodeStore, Recovering};
use crate::world::storage::{DurableStorage, StorageFaults, WritePathRates};
use crate::world::{ParkReason, StorageWorld, storage_world, storage_world_for};
use paros::{
    AcceptorConfig, BootKind, BootRefusal, Config, JournalStorage, JournalStoreConfig,
    JournalStores, LogStorage, MatchmakerConfig, MatchmakerId, NodeId, ProxyConfig, ProxyId,
    ReplicaId, RunError, SystemPlan, parse_addr, run_journals, run_matchmaker, run_proxy,
    run_replica,
};

/// One role's address book: the group's IPs in rank order, each paired with
/// the identity its rank names — `NodeId`, `MatchmakerId` or `ProxyId`.
fn ranked<I>(ips: &[String], id: fn(u64) -> I) -> SimulationResult<Vec<(I, String)>> {
    ips.iter()
        .enumerate()
        .map(|(rank, ip)| {
            parse_addr(ip).map(|addr| (id(u64::try_from(rank).expect("rank fits u64")), addr))
        })
        .collect()
}

/// The replica tier's address book (#144): each replica's wire `NodeId`
/// ([`replica_node_id`], outside the pool) and address, in `ReplicaId`
/// order; empty on a seed without replicas.
fn replica_book(deployment: &Deployment) -> SimulationResult<Vec<(NodeId, String)>> {
    ranked(deployment.replicas(), |rank| {
        replica_node_id(ReplicaId(rank))
    })
}

/// The **bootstrap acceptor configuration** of a seed, as every node and every
/// proxy leader derives it: the bootstrap ranks (`crate::shape::bootstrap_ranks`)
/// under the run's quorum-system policy at their own size. Protocol data drawn
/// once per seed, so every process reads the same one.
fn bootstrap_config(
    ctx: &SimContext,
    pool: usize,
    has_matchmakers: bool,
    perturb: bool,
) -> AcceptorConfig {
    let bootstrap: Vec<NodeId> =
        crate::shape::bootstrap_ranks(ctx.state(), pool, has_matchmakers, perturb)
            .into_iter()
            .map(NodeId)
            .collect();
    let policy = crate::shape::quorum_policy(ctx.state(), pool, perturb);
    let system = policy.system(bootstrap.len());
    AcceptorConfig::new(bootstrap, system)
}

/// One role incarnation's harness rig, armed the same way for every role:
/// the incarnation and its shape (`crate::shape::boot` — the rig's **only**
/// draw, so a caller keeps it exactly where its own registry draws expect
/// it), the driver hooks over the chaos window at the shape's crash bias,
/// and the per-iteration audit — the shared checker every role folds into,
/// and this incarnation's port. The disk's fault layer is not here: a proxy
/// has no disk (see [`storage_faults`]).
struct RoleRig {
    incarnation: crate::shape::Incarnation,
    hooks: BuggifyHooks<SimTimeProvider>,
    checker: Arc<AuditWorld>,
    audit: NodeAudit<SimTimeProvider>,
}

/// Arm `my_ip`'s rig for this incarnation. The shape is what makes a
/// re-entry from a fresh factory instance a *restart* of the same node
/// rather than a new node with new knobs (see `crate::shape`); durable
/// state is the world's business, never the shape's. The audit is pure
/// observation, published beside the storage world so every node folds its
/// transitions into one incremental checker — it never influences the
/// driver; that is the hooks' job.
fn arm_role(ctx: &SimContext, my_ip: &str, perturb: bool) -> RoleRig {
    let incarnation = crate::shape::boot(ctx.state(), my_ip, perturb);
    let hooks = BuggifyHooks::new(
        ctx.time().clone(),
        crate::CHAOS_DURATION,
        perturb,
        incarnation.shape.seam_crash_bias,
    );
    let checker = audit_world(ctx.state());
    let audit = NodeAudit::new(ctx.time().clone(), checker.clone());
    RoleRig {
        incarnation,
        hooks,
        checker,
        audit,
    }
}

/// The budgeted write-path fault layer of a role with a disk (issue #19
/// B/C), sharing the driver hooks' chaos window: after the cutoff the world
/// stops injecting **new** faults but never heals the consequences of old
/// ones — recovery through the tail must be genuine.
fn storage_faults(
    ctx: &SimContext,
    perturb: bool,
    rates: WritePathRates,
) -> StorageFaults<SimTimeProvider> {
    StorageFaults::new(ctx.time().clone(), crate::CHAOS_DURATION, perturb, rates)
}

/// Why an incarnation exits for good instead of booting (see the recovery
/// loops below): told to the audit, so convergence excuses exactly these
/// identities, and traced.
#[derive(Clone, Copy)]
enum Down {
    /// A node terminally parked by a detected persistent corruption.
    StorageParked(u64),
    /// A node the operator retired (#123).
    Retired(u64),
    /// A matchmaker whose registry was wiped and whose boot the library
    /// refused (#125, #183).
    MatchmakerLost(u64),
}

fn stay_down(checker: &AuditWorld, down: Down) {
    match down {
        Down::StorageParked(node) => {
            checker.note_storage_dead(node);
            tracing::info!(node, "storage_parked");
        }
        Down::Retired(node) => {
            checker.note_retired_boot(node);
            tracing::info!(node, "retired_stays_down");
        }
        Down::MatchmakerLost(matchmaker) => {
            checker.note_matchmaker_lost();
            tracing::info!(matchmaker, "matchmaker_stays_down");
        }
    }
}

/// One restart-delay BUGGIFY site: the delay a crashed role waits before its
/// next incarnation boots, workload-buggified config (prong 2) that
/// stretches the durability-seam crash window process-level attrition
/// cannot reach — a node held down while the cluster keeps committing and
/// truncating returns below the compaction floor and independently
/// exercises the trim-point jump. Drawn per *crash*, deliberately not per node
/// (it is not part of the node's shape): the delay describes one event, and
/// two crashes of the same node should be free to look different. The floor
/// is structural: a held-down node is a recovery the tail must absorb, never
/// a cluster that stalls.
///
/// A macro, never a fn: moonpool keys a BUGGIFY location by the `file:line` of
/// the outermost macro invocation, so every invocation below stays its own
/// independently selectable location with its own fired gate (`$fired`,
/// the reachable that proves the knob fired there).
macro_rules! restart_delay {
    ($ctx:expr, $fired:literal) => {{
        let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
        if delay_ms > 0 {
            // BUGGIFY pairing: this site's restart-delay knob fired.
            assert_reachable!($fired);
            $ctx.time()
                .sleep(Duration::from_millis(delay_ms))
                .await
                .ok();
        }
    }};
}

/// A paros node (an acceptor) in the simulation.
pub(crate) struct NodeProcess {
    mode: NodeMode,
    /// A scripted case's choreography (empty on the main campaign).
    options: ScriptedOptions,
}

/// What a scripted corpus case fixes about its nodes beyond the dark swarm
/// sites: every field `None` is the plain three-node case.
#[derive(Clone, Copy, Default)]
pub(crate) struct ScriptedOptions {
    /// A fixed bootstrap size (`Some(n)`: ranks `0..n` are the bootstrap
    /// acceptors, the rest spares — a case that needs a spare); `None`
    /// bootstraps on the whole pool.
    pub(crate) bootstrap: Option<usize>,
    /// Withhold every GC request (`DriverHooks::withhold_gc_requests`): a
    /// case whose prior configuration must stay answerable (#124) cannot
    /// race the new leader's GC floor.
    pub(crate) withhold_gc: bool,
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
            options: ScriptedOptions::default(),
        }
    }

    /// A scripted corpus node: every swarm site dark, choreographed by
    /// `options`.
    pub(crate) fn scripted_with(options: ScriptedOptions) -> Self {
        Self {
            mode: NodeMode::Scripted,
            options,
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

/// A proxy leader in the simulation (#142): its own process group, so a seed
/// draws how many it deploys independently of the other pools and attrition
/// can be scoped to it. Nothing durable: a kill reboots it empty.
pub(crate) struct ProxyProcess {
    /// Whether the driver hooks and the shape knobs are live (the main
    /// campaign) or dark (a scripted case — none registers proxies today).
    perturb: bool,
}

impl ProxyProcess {
    pub(crate) fn chaotic() -> Self {
        Self { perturb: true }
    }
}

/// A replica in the simulation (#144): its own process group, so a seed
/// draws how many it deploys independently of the other pools and attrition
/// can be scoped to it. Durable: a kill reboots it from its disk, and it
/// catches up from the acceptors.
pub(crate) struct ReplicaProcess {
    /// Whether the driver hooks and the shape knobs are live (the main
    /// campaign) or dark (a scripted case — none registers replicas today).
    perturb: bool,
}

impl ReplicaProcess {
    pub(crate) fn chaotic() -> Self {
        Self { perturb: true }
    }
}

/// A joiner in the simulation (#189): its own process group, a node outside
/// the genesis pool. On a seed that runs the system journals it follows the
/// directory and the registry from the seeds, is admitted to the pool when a
/// client registers it, and serves every journal the directory creates
/// naming it; on any other seed it idles.
pub(crate) struct JoinerProcess {
    /// Whether the driver hooks and the shape knobs are live.
    perturb: bool,
}

impl JoinerProcess {
    pub(crate) fn chaotic() -> Self {
        Self { perturb: true }
    }
}

#[async_trait]
impl Process for JoinerProcess {
    fn name(&self) -> &'static str {
        crate::roles::JOINER_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let perturb = self.perturb;
        dispatch(
            ctx,
            "every joiner process is mapped to the joiner role",
            "a joiner",
            |role| match role {
                Role::Joiner(id) => Some(id),
                _ => None,
            },
            |deployment, id, my_ip| async move {
                Box::pin(run_joiner(ctx, &deployment, id, &my_ip, perturb)).await
            },
        )
        .await
    }
}

/// A joiner (#189): `run_journals` with no journal of its own and the
/// system plan, in the same seam-crash recovery loop as a node. Its disk is
/// fault-free (every seat it gets is a created journal's), and it is not an
/// attrition victim: a joiner's lifecycle is the registry's.
#[tracing::instrument(level = "debug", skip_all, fields(node = id.0))]
async fn run_joiner(
    ctx: &SimContext,
    deployment: &Deployment,
    id: NodeId,
    my_ip: &str,
    perturb: bool,
) -> SimulationResult<()> {
    let has_matchmakers = !deployment.matchmakers().is_empty();
    if !crate::shape::system_journals(ctx.state(), perturb) {
        // No system journals on this seed: nothing to join.
        ctx.shutdown().cancelled().await;
        return Ok(());
    }
    let members = ranked(deployment.acceptors(), NodeId)?;
    // A joiner that joins the default journal as a spare campaigns through
    // the matchmakers like any member of it.
    let matchmakers = ranked(deployment.matchmakers(), MatchmakerId)?;
    let plan = crate::shape::journals(ctx.state(), has_matchmakers, perturb);
    let board = crate::audit::system::system_board(ctx.state());
    let (system_plan, _) = system_plan(ctx, deployment, &members, &plan, id);
    let RoleRig {
        incarnation, hooks, ..
    } = arm_role(ctx, my_ip, perturb);
    let tunables = incarnation.shape.tunables;
    let faults = quiet_faults(ctx);
    let mut seats: Vec<Seat> = Vec::new();
    loop {
        let stores = SimStores {
            ctx,
            seats: &mut seats,
            ip: my_ip,
            rank: id.0,
            faults: &faults,
            journal_store: None,
            recovering: Recovering::default(),
            system: Some(board.clone()),
        };
        match Box::pin(run_journals(
            ctx.providers().clone(),
            stores,
            parse_addr(my_ip)?,
            members.clone(),
            matchmakers.clone(),
            Vec::new(),
            Vec::new(),
            Some(system_plan.clone()),
            tunables,
            ctx.shutdown().clone(),
            &hooks,
        ))
        .await
        {
            Err(RunError::SeamCrash(_) | RunError::Storage(_)) => {
                restart_delay!(
                    ctx,
                    "a seam-crashed joiner restarts after a buggified delay"
                );
            }
            Err(RunError::Refused(refusal)) => {
                assert_always!(
                    false,
                    "system: a joiner's quiet store is never refused",
                    { "node" => id.0, "refusal" => format!("{refusal:?}") }
                );
                return Err(SimulationError::InvalidState(format!(
                    "joiner {} refused a boot: {refusal:?}",
                    id.0
                )));
            }
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}

/// Run one process as the role the deployment map gives its IP. The map is
/// read off the topology's process groups, so every process derives the
/// *same* map without coordination; `id` picks this group's role out of it
/// (its identity), and `run` runs it. A process whose IP the map puts in
/// another group is a harness bug: recorded under `unmapped` (the group's
/// always-assertion) and refused as not `what` of the deployment.
async fn dispatch<I, Fut>(
    ctx: &SimContext,
    unmapped: &'static str,
    what: &'static str,
    id: fn(Role) -> Option<I>,
    run: impl FnOnce(Deployment, I, String) -> Fut,
) -> SimulationResult<()>
where
    Fut: Future<Output = SimulationResult<()>> + Send,
{
    let my_ip = ctx.my_ip().to_string();
    let deployment = crate::roles::deployment(ctx.topology());
    let role = deployment.role_of(&my_ip);
    if let Some(id) = role.and_then(id) {
        run(deployment, id, my_ip).await
    } else {
        assert_always!(
            false,
            unmapped,
            { "ip" => my_ip.as_str(), "role" => format!("{role:?}") }
        );
        Err(SimulationError::InvalidState(format!(
            "{my_ip} is not {what} of the deployment"
        )))
    }
}

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        ACCEPTOR_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let perturb = self.mode == NodeMode::Chaotic;
        let options = self.options;
        dispatch(
            ctx,
            "every node process is mapped to the acceptor role",
            "an acceptor",
            |role| match role {
                Role::Acceptor(rank) => Some(rank),
                _ => None,
            },
            |deployment, self_rank, my_ip| async move {
                Box::pin(run_acceptor(
                    ctx,
                    &deployment,
                    self_rank,
                    &my_ip,
                    perturb,
                    options,
                ))
                .await
            },
        )
        .await
    }
}

#[async_trait]
impl Process for MatchmakerProcess {
    fn name(&self) -> &'static str {
        MATCHMAKER_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let perturb = self.perturb;
        dispatch(
            ctx,
            "every matchmaker process is mapped to the matchmaker role",
            "a matchmaker",
            |role| match role {
                Role::Matchmaker(id) => Some(id),
                _ => None,
            },
            |_, id, my_ip| async move { run_matchmaker_role(ctx, id, &my_ip, perturb).await },
        )
        .await
    }
}

#[async_trait]
impl Process for ProxyProcess {
    fn name(&self) -> &'static str {
        PROXY_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let perturb = self.perturb;
        dispatch(
            ctx,
            "every proxy process is mapped to the proxy role",
            "a proxy leader",
            |role| match role {
                Role::Proxy(id) => Some(id),
                _ => None,
            },
            |deployment, id, my_ip| async move {
                run_proxy_role(ctx, &deployment, id, &my_ip, perturb).await
            },
        )
        .await
    }
}

#[async_trait]
impl Process for ReplicaProcess {
    fn name(&self) -> &'static str {
        REPLICA_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let perturb = self.perturb;
        dispatch(
            ctx,
            "every replica process is mapped to the replica role",
            "a replica",
            |role| match role {
                Role::Replica(rank) => Some(rank),
                _ => None,
            },
            |deployment, rank, my_ip| async move {
                run_replica_role(ctx, &deployment, rank, &my_ip, perturb).await
            },
        )
        .await
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
    options: ScriptedOptions,
) -> SimulationResult<()> {
    // The node pool is the map's acceptor list, in `NodeId` order — never
    // "every process in the topology". The matchmaker set is the map's
    // matchmaker list, empty on a plain seed. The bootstrap membership is
    // protocol data drawn once per seed (`crate::shape::bootstrap_ranks`):
    // the whole pool by default, and on a matchmaker seed possibly a subset
    // that leaves spares for a reconfiguration to pull in.
    let members = ranked(deployment.acceptors(), NodeId)?;
    let matchmakers = ranked(deployment.matchmakers(), MatchmakerId)?;
    // The proxy leaders (#142): the map's proxy list, whose length is the
    // `Config`'s `proxy_count` — zero on a seed without proxies, the plain
    // deployment whose every Phase 2 stays colocated.
    let proxies = ranked(deployment.proxies(), ProxyId)?;
    // The replica tier (#144): the learners outside the pool, each under
    // its wire `NodeId`; empty on a seed without replicas.
    let replicas = replica_book(deployment)?;
    let pool: Vec<NodeId> = members.iter().map(|(id, _)| *id).collect();
    let bootstrap: Vec<NodeId> = match options.bootstrap {
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
    // The quorum system is protocol data too (#140): the run's policy, drawn
    // once per seed, applied to the bootstrap configuration's own size. A
    // majority on a plain or unperturbed seed; a flexible split on the seeds
    // the swarm turns it on for.
    let policy = crate::shape::quorum_policy(ctx.state(), pool.len(), perturb);
    let quorum_system = policy.system(bootstrap.len());
    let config = Config {
        journal: paros::JournalKey::default(),
        id: self_rank,
        peers: bootstrap,
        quorum_system,
        nodes: pool,
        matchmakers: matchmaker_bootstrap,
        matchmaker_pool,
        proxy_count: proxies.len(),
        replica_count: deployment.replica_count(),
    };
    // The run's journals (#188): the static list every node serves — the
    // default journal alone unless the deployment is plain and the seed drew
    // more — and the one held on every node for the chaos window, if any.
    let plan = crate::shape::journals(ctx.state(), !matchmakers.is_empty(), perturb);
    // The store (#187): the world-backed store, or — on a plain seed that
    // drew it — the library's `JournalStorage` on the simulated disk.
    let journal_store = crate::shape::journal_store(ctx.state(), !matchmakers.is_empty(), perturb);
    let board = journal_board(ctx.state());
    board_lock(&board).arm(&plan);

    // This node's rig: every knob the swarm draws *for the node* (the driver
    // tunables, the write-window crash bias, the disk's fault rates), drawn
    // by its first incarnation of the seed and handed back unchanged to
    // every later one. Armed here, after the registry draws above, so the
    // seed's draw schedule keeps its order.
    let RoleRig {
        incarnation,
        hooks,
        checker: _,
        audit: _,
    } = arm_role(ctx, my_ip, perturb);
    let hooks = if options.withhold_gc {
        hooks.withholding_gc()
    } else {
        hooks
    };
    let hooks = hooks.holding_journal(plan.held);
    let shape = incarnation.shape;
    // The copy budget is sized by the run's configuration floor
    // (`crate::shape::config_floor`): the whole pool on a plain seed, the
    // smallest set a reconfiguration may shrink to on a matchmaker seed —
    // and by the clean copies the run's quorum-system policy demands over
    // every size the run may put in force, floor to pool (a majority, the
    // split's Phase-1 quorum, or — on a grid seed — the whole floor: a grid
    // tolerates no permanent loss). The storage world's budget and the
    // audit's restart note below read the same two numbers.
    // One seat per journal (#188): its own configuration, its own
    // per-iteration durable world (shared by every node's copy of the
    // journal, surviving crash/restart, reached through a `Weak` handle
    // upgraded per op), its own audit world and audit port. Nothing crosses:
    // a journal's budget, fault ledger, parked identities and oracles are
    // its own. The default journal is the seed's deployment (its
    // matchmakers, proxies, replicas and bootstrap); every other journal is
    // plain Multi-Paxos over the whole pool — the matchmaker plane, the
    // proxy leaders and the replica tier each serve one journal.
    let mut seats: Vec<Seat> = plan
        .ids
        .iter()
        .map(|&journal| {
            let config = if journal == paros::JournalKey::default() {
                config.clone()
            } else {
                Config {
                    journal,
                    peers: config.pool().to_vec(),
                    quorum_system: policy.system(config.pool().len()),
                    matchmakers: Vec::new(),
                    matchmaker_pool: Vec::new(),
                    proxy_count: 0,
                    replica_count: 0,
                    ..config.clone()
                }
            };
            let floor = crate::shape::config_floor(config.pool().len(), config.has_matchmakers());
            let clean_copies = policy.clean_copies(floor, config.pool().len());
            let world = storage_world_for(ctx.state(), journal);
            {
                let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                guard.set_budget(floor, clean_copies);
                // The pool above that floor is the retirement budget (#123):
                // every identity a configuration may leave behind.
                guard.set_pool_size(config.pool().len());
                if !perturb {
                    guard.set_unbudgeted();
                }
            }
            let checker = audit_world_for(ctx.state(), journal);
            let audit = NodeAudit::new(ctx.time().clone(), checker.clone())
                .in_journal(journal, board.clone());
            Seat {
                journal,
                config,
                world,
                checker,
                audit,
                floor,
                clean_copies,
                quiet: false,
                created: false,
                deleted: false,
            }
        })
        .collect();
    // The system journals (#189), on a seed that drew them: every node
    // follows the directory and the registry, and the seeds — the lowest
    // ranks — host them, a static configuration of plain Multi-Paxos on a
    // fault-free disk.
    let system = crate::shape::system_journals(ctx.state(), perturb)
        .then(|| system_rig(ctx, deployment, &members, &plan, &mut seats, self_rank));
    let faults = storage_faults(ctx, perturb, shape.write_rates);
    let tunables = shape.tunables;
    if incarnation.is_restart() {
        // A process-level revival (attrition on the main campaign, the
        // script on the corpus). Told to every journal's audit so it can
        // judge the overlap this boot may be ending: a node that was down
        // while a peer sat terminally parked (persistent storage loss +
        // transient process loss at once) is the composition that costs a
        // small cluster its quorum until exactly this boot returns it.
        for seat in &seats {
            let parked_peers = seat
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .parked_count_excluding(my_ip);
            seat.checker.note_process_restart(
                self_rank.0,
                parked_peers,
                seat.floor,
                seat.clean_copies,
            );
        }
        // The disk's wipe coin (#124): a restart that comes back on an empty
        // disk. Moonpool's own `prob_wipe` reaches only its storage provider,
        // which paros does not use (the fake disk is the world), so the
        // amnesia fault is the world's, drawn here at the one place a lost
        // disk shows — a reboot. What happens next is the **library's**
        // call (#147): the identity boots below as an existing member on an
        // empty store, and `run_node` refuses the amnesiac store. A wiped
        // node is replaced through an acceptor reconfiguration, never
        // rejoined (an empty disk under an old identity would answer a
        // Phase 1 with "nothing accepted" for slots it voted on). Only a
        // matchmaker deployment can replace, so the coin is dark on a plain
        // seed — and a matchmaker deployment runs one journal; the world's
        // dead-node budget bounds it either way.
        let wipe = perturb
            && config.has_matchmakers()
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.wipe_pct) / 100.0);
        if wipe
            && seats[0]
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .wipe(my_ip, self_rank.0)
        {
            // BUGGIFY pairing: the wipe coin fired within the budget.
            assert_reachable!("storage: a restarted node's disk is wiped and the identity retired");
            tracing::info!(node = self_rank.0, "storage_wiped");
        }
    }
    // #207: the operator restarts this node with an edited configuration
    // file — a peer dropped from the bootstrap membership — and the library
    // refuses the store that was formatted under the old one; the operator
    // restores the file and restarts (the `ConfigMismatch` arm below). Only
    // a node whose every journal it serves is the one seat, on a seed
    // without system journals: a refused journal is down for good on a node
    // that still serves another, and the operator's correction is a restart
    // of the whole process. The edit is applied below, inside the loop and
    // only to an identity the provisioning ledger knows: a first boot under
    // an edited file would format the edit.
    let mut edit = OperatorEdit::new(
        incarnation.is_restart()
            && perturb
            && seats.len() == 1
            && system.is_none()
            && seats[0].config.peers.len() > 1
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.config_edit_pct) / 100.0),
    );

    // Recovery loop: a `buggify`-injected seam crash unwinds the driver, we
    // drop the volatile nodes, rebuild storage from the (surviving) worlds,
    // and re-run — a faithful clean crash + recovery. Attrition (process
    // kill) is handled by the harness; this covers the seams *inside* a
    // Ready batch that attrition cannot reach. A journal that is down for
    // good on this node — retired by the operator (#123), or terminally
    // parked by a detected persistent corruption — is one the opener
    // declines (`SimStores::open`), and a node with every journal down
    // exits cleanly: it stays down. A **wiped** identity (#124) is not on
    // that list: it boots, and the library refuses it (#147, below).
    loop {
        if journal_store.is_some() {
            resolve_provisioning(ctx, &seats, my_ip, journal_store).await;
        }
        let provisioned = seats[0]
            .world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(my_ip);
        if edit.apply(provisioned, &mut seats[0].config, |config| {
            config.peers.pop();
        }) {
            // BUGGIFY pairing: the operator's edit genuinely reaches a boot.
            assert_reachable!("operator: a node restarts under an edited configuration");
            tracing::info!(node = self_rank.0, "config_edited");
        }
        let stores = SimStores {
            ctx,
            seats: &mut seats,
            ip: my_ip,
            rank: self_rank.0,
            faults: &faults,
            journal_store: journal_store.map(|layout| (ctx.storage().clone(), layout)),
            recovering: Recovering::default(),
            system: system.as_ref().map(|(_, board)| board.clone()),
        };
        // Boxed: the node loop's future is large (every arm's state lives
        // in it), and this incarnation loop awaits it on its own frame.
        match Box::pin(run_journals(
            ctx.providers().clone(),
            stores,
            parse_addr(my_ip)?,
            members.clone(),
            matchmakers.clone(),
            proxies.clone(),
            replicas.clone(),
            system.as_ref().map(|(plan, _)| plan.clone()),
            tunables,
            ctx.shutdown().clone(),
            &hooks,
        ))
        .await
        {
            // Simulated crash at a durability seam: fall through to recover
            // and re-run (rebuilding volatile state from the durable world),
            // after this crash's own restart delay (`restart_delay!`).
            Err(RunError::SeamCrash(_)) => {
                restart_delay!(ctx, "a seam-crashed node restarts after a buggified delay");
            }
            // An injected storage fault surfaced as the driver's typed
            // crash decision (issue #19 A) and took the node's last live
            // journal (#188: a fault quarantines its journal; a node left
            // with none is the fail-stop crash), so the node re-enters the
            // same Stage-4 crash/restart path — the next iteration boots from
            // whatever the disks *actually* hold, which is how an ambiguous
            // write's two possible outcomes both resolve. Its restart delay
            // is its own independent BUGGIFY location.
            Err(RunError::Storage(_)) => {
                // Stage 7's baseline for a *persistent* detected fault —
                // a rotted record or an FS-metadata fault — is detect ⇒
                // crash, and restarting cannot help: the boot scan would
                // re-detect the same record forever. The journal stays down
                // on this node for the run (the availability disaster the
                // CTRL paper measures; Stage 8 buys it back), bounded by its
                // world's dead-node budget so the cluster keeps a live
                // quorum. A node whose every journal is parked stays down;
                // its audits are told so convergence excuses exactly these
                // nodes, and only these.
                let parked = seats.iter().all(|seat| {
                    seat.world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_parked(my_ip)
                });
                if parked {
                    assert_reachable!("storage: a corruption-crashed node stays down");
                    for seat in &seats {
                        stay_down(&seat.checker, Down::StorageParked(self_rank.0));
                    }
                    return Ok(());
                }
                assert_reachable!("a storage-fault crash recovers through the restart path");
                restart_delay!(
                    ctx,
                    "a storage-fault crash restarts after a buggified delay"
                );
            }
            // The library refused the store (#147). Amnesia is the wipe
            // coin's outcome and the one the rule exists for: the identity
            // stays down for the run, replaced by reconfiguration — the
            // audit already excused it from convergence when the driver
            // reported the refusal. The harness cross-checks the refusal
            // against its own injection: only a wiped disk is ever
            // amnesiac here.
            Err(RunError::Refused(BootRefusal::Amnesia)) => {
                let wiped = seats.iter().any(|seat| {
                    seat.world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_wiped(my_ip)
                });
                assert_always!(
                    wiped,
                    "storage: an amnesia refusal names a wiped identity",
                    { "node" => self_rank.0 }
                );
                tracing::info!(node = self_rank.0, "amnesia_refused_stays_down");
                return Ok(());
            }
            // #207: the library refused a store formatted under another
            // configuration. Only the operator's edit above ever changes
            // one, so the refusal must name it; the operator restores the
            // file and restarts, and the node comes back as the member it
            // was — nothing was written by the refused boot.
            Err(RunError::Refused(BootRefusal::ConfigMismatch)) => {
                let restored = edit.restore(&mut seats[0].config);
                assert_always!(
                    restored,
                    "storage: a configuration refusal names an operator's edit",
                    { "node" => self_rank.0 }
                );
                if !restored {
                    return Err(SimulationError::InvalidState(format!(
                        "node {} refused a configuration nobody edited",
                        self_rank.0
                    )));
                }
                tracing::info!(node = self_rank.0, "config_restored");
                restart_delay!(
                    ctx,
                    "a node refused under an edited configuration restarts under the restored one"
                );
            }
            // A first boot on a formatted store is a harness bug: the
            // provisioning ledger and the disks disagree.
            Err(RunError::Refused(BootRefusal::AlreadyFormatted)) => {
                assert_always!(
                    false,
                    "storage: a first boot never meets a formatted store",
                    { "node" => self_rank.0 }
                );
                return Err(SimulationError::InvalidState(format!(
                    "node {} booted as first boot on a formatted store",
                    self_rank.0
                )));
            }
            // The only non-crash exit: a genuine infrastructure failure
            // propagates to the harness instead of being retried.
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
        }
    }
}

/// The operator's edit of a configuration file (#207): drawn at a restart,
/// applied to the first boot of an identity the provisioning ledger knows,
/// and undone when the library refuses it.
struct OperatorEdit<C> {
    /// The coin fired and the edit has not reached a boot yet.
    pending: bool,
    /// The configuration the edit replaced, until the refusal restores it.
    edited_from: Option<C>,
}

impl<C: Clone> OperatorEdit<C> {
    fn new(pending: bool) -> Self {
        Self {
            pending,
            edited_from: None,
        }
    }

    /// Apply the pending edit to `config` when `provisioned` (a first boot
    /// under an edited file would format the edit); `true` when it did.
    fn apply(&mut self, provisioned: bool, config: &mut C, edit: impl FnOnce(&mut C)) -> bool {
        if !self.pending || !provisioned {
            return false;
        }
        self.pending = false;
        self.edited_from = Some(config.clone());
        edit(config);
        true
    }

    /// The library refused the edit: restore the original into `config`;
    /// `false` when there was no edit to restore (a refusal nobody caused).
    fn restore(&mut self, config: &mut C) -> bool {
        match self.edited_from.take() {
            Some(original) => {
                *config = original;
                true
            }
            None => false,
        }
    }
}

/// One journal's seat on an acceptor process (#188): its configuration, its
/// storage world, its audit world and its audit port.
struct Seat {
    journal: paros::JournalKey,
    config: Config,
    world: Arc<Mutex<StorageWorld>>,
    checker: Arc<AuditWorld>,
    audit: NodeAudit<SimTimeProvider>,
    /// The journal's configuration floor and its clean-copy budget (the
    /// numbers its storage world was sized by).
    floor: usize,
    clean_copies: usize,
    /// A system journal or a journal the directory created (#189): stored on
    /// a fault-free world-backed disk, outside the copy budget — the storage
    /// fault model is the genesis journals' business.
    quiet: bool,
    /// Created at runtime by the directory (#189): opened only when the
    /// directory names it, never at boot.
    created: bool,
    /// The directory tombstoned it (#189): never opened again.
    deleted: bool,
}

impl Seat {
    /// A fault-free seat for a system journal or a created one (#189): its
    /// own storage world (unbudgeted, nothing is injected into it) and its
    /// own audit world and port, reporting to the system board too.
    fn quiet(
        ctx: &SimContext,
        journal: paros::JournalKey,
        config: Config,
        system: &Arc<Mutex<crate::audit::system::SystemBoard>>,
    ) -> Self {
        let world = storage_world_for(ctx.state(), journal);
        // A journal of its own (a system or a created one) is outside every
        // budget; a joiner's seat on a genesis journal (#189, a spare) shares
        // that journal's world and leaves its budget as the genesis nodes
        // sized it — a fault-free copy only ever adds to what it defends.
        if journal != paros::JournalKey::default() {
            world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .set_unbudgeted();
        }
        let checker = audit_world_for(ctx.state(), journal);
        let audit = NodeAudit::new(ctx.time().clone(), checker.clone())
            .in_journal(journal, journal_board(ctx.state()))
            .with_system(system.clone());
        let members = config.peers.len();
        Self {
            journal,
            config,
            world,
            checker,
            audit,
            floor: members,
            clean_copies: members,
            quiet: true,
            created: false,
            deleted: false,
        }
    }
}

/// The acceptor's journal stores (#188): each journal's world-backed disk,
/// opened as a process restart finds it (`DurableStorage::restore`), with the
/// operator's boot claim read off the journal's provisioning ledger (#147).
/// A journal down for good on this node — retired, or parked by a detected
/// corruption — is declined, and its audit is told it stays down.
struct SimStores<'a> {
    ctx: &'a SimContext,
    seats: &'a mut Vec<Seat>,
    ip: &'a str,
    rank: u64,
    faults: &'a StorageFaults<SimTimeProvider>,
    /// The simulated disk and the journal layout, on a journal-store seed
    /// (#187).
    journal_store: Option<(SimStorageProvider, JournalStoreConfig)>,
    /// This incarnation's journals still holding a faulty vote.
    recovering: Recovering,
    /// The system board, on a seed that runs the system journals (#189):
    /// the directory's created journals get seats here at runtime.
    system: Option<Arc<Mutex<crate::audit::system::SystemBoard>>>,
}

/// The directory a journal's store lives in on a node's simulated disk
/// (#187, #188: one directory per journal, by its frame, #235).
fn journal_dir(journal: paros::JournalKey) -> String {
    format!("paros/journals/{}/{}", journal.tenant.0, journal.journal.0)
}

/// Resolve an interrupted provisioning before a boot (#187): a journal
/// store's format marker lands only with the sync after the format, and a
/// process killed in between leaves the operator's ledger saying "begun"
/// and nothing else. The operator does what an operator would: looks at the
/// disk — a store that carries the marker was provisioned, one that does
/// not was not, and its next boot is a first boot again.
#[tracing::instrument(level = "debug", skip_all, fields(ip = %ip))]
async fn resolve_provisioning(
    ctx: &SimContext,
    seats: &[Seat],
    ip: &str,
    journal_store: Option<JournalStoreConfig>,
) {
    let Some(layout) = journal_store else {
        return;
    };
    for seat in seats {
        let ambiguous = seat
            .world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioning_ambiguous(ip);
        if !ambiguous {
            continue;
        }
        let mut probe = JournalStorage::new(
            ctx.storage().clone(),
            journal_dir(seat.journal),
            seat.config.clone(),
            layout,
        );
        let formatted = probe.boot_scan().await.is_ok() && probe.is_formatted();
        drop(probe);
        let mut guard = seat.world.lock().unwrap_or_else(PoisonError::into_inner);
        if formatted {
            guard.note_provisioned(ip);
        } else {
            guard.abandon_provisioning(ip);
        }
        assert_reachable!("journal store: an interrupted provisioning is resolved from the disk");
    }
}

impl SimStores<'_> {
    fn seat(&self, journal: paros::JournalKey) -> Option<&Seat> {
        self.seats.iter().find(|seat| seat.journal == journal)
    }
}

impl JournalStores for SimStores<'_> {
    type Store = NodeStore;
    type Audit = NodeAudit<SimTimeProvider>;

    fn journals(&self) -> Vec<paros::JournalKey> {
        self.seats
            .iter()
            .filter(|seat| !seat.created)
            .map(|seat| seat.journal)
            .collect()
    }

    fn open(&mut self, journal: paros::JournalKey) -> Option<(Self::Store, BootKind)> {
        let seat = self.seat(journal).filter(|seat| !seat.deleted)?;
        let (parked, boot) = {
            let guard = seat.world.lock().unwrap_or_else(PoisonError::into_inner);
            (
                guard.park_reason(self.ip),
                // The operator's claim (#147): an identity the world's
                // provisioning ledger knows is an existing member — a wiped
                // one included, which is the whole point — and any other is
                // a first boot the driver formats.
                if guard.provisioned(self.ip) {
                    BootKind::ExistingMember
                } else {
                    BootKind::FirstBoot
                },
            )
        };
        match parked {
            Some(ParkReason::Retired) => {
                stay_down(&seat.checker, Down::Retired(self.rank));
                return None;
            }
            Some(ParkReason::Corruption) => {
                stay_down(&seat.checker, Down::StorageParked(self.rank));
                return None;
            }
            Some(ParkReason::Wiped) | None => {}
        }
        if seat.quiet {
            // A system or created journal (#189): the world store, and no
            // fault is ever injected into it.
            let storage = DurableStorage::restore(
                seat.config.clone(),
                Arc::downgrade(&seat.world),
                self.ip.to_string(),
                self.rank,
                quiet_faults(self.ctx),
                seat.checker.clone(),
            );
            return Some((NodeStore::World(storage), boot));
        }
        if let Some((provider, layout)) = &self.journal_store {
            if seat.clean_copies == seat.floor {
                // A quorum system that tolerates no lost copy (a grid, a
                // phase-1 quorum of the whole pool): no replicated fault
                // pattern can damage a record anywhere and keep it
                // recoverable, so this disk takes no damage at all. Only a
                // shut-down simulation refuses.
                let _ = provider.focus_faults(FaultFocus::new().background(0.0));
            }
            let store = LedgeredJournal::new(
                JournalStorage::new(
                    provider.clone(),
                    journal_dir(journal),
                    seat.config.clone(),
                    *layout,
                ),
                Arc::downgrade(&seat.world),
                self.ip.to_string(),
                self.rank,
                provider.clone(),
                (journal, self.recovering.clone()),
            );
            return Some((NodeStore::Journal(store), boot));
        }
        let storage = DurableStorage::restore(
            seat.config.clone(),
            Arc::downgrade(&seat.world),
            self.ip.to_string(),
            self.rank,
            self.faults.clone(),
            seat.checker.clone(),
        );
        Some((NodeStore::World(storage), boot))
    }

    fn audit(&self, journal: paros::JournalKey) -> Self::Audit {
        self.seat(journal).map_or_else(
            || {
                // A node that serves no seat for `journal` — a joiner's
                // node-level port (#189): the default journal's world.
                let audit = NodeAudit::new(
                    self.ctx.time().clone(),
                    audit_world_for(self.ctx.state(), journal),
                );
                match &self.system {
                    Some(board) => audit.with_system(board.clone()),
                    None => audit,
                }
            },
            |seat| seat.audit.clone(),
        )
    }

    /// A journal the directory created naming this node (#189): a quiet seat
    /// under `config`, kept across incarnations (a restart re-folds the
    /// directory and asks again).
    fn create(&mut self, journal: paros::JournalKey, config: Config) -> bool {
        let Some(board) = &self.system else {
            return false;
        };
        if let Some(seat) = self.seats.iter().find(|seat| seat.journal == journal) {
            return !seat.deleted;
        }
        let mut seat = Seat::quiet(self.ctx, journal, config, board);
        seat.created = true;
        self.seats.push(seat);
        true
    }

    /// A journal a storage fault quarantined whose store the world has
    /// parked for good (a detected persistent corruption) will never open
    /// again: the audit hears the node is down for it now, as it would from
    /// a one-journal node's fail-stop exit, rather than at a re-open the run
    /// may end before (a node that serves the system journals keeps running
    /// with the journal down; witness 18183308543219257601).
    fn quarantined(&mut self, journal: paros::JournalKey) {
        if let Some(seat) = self.seat(journal)
            && seat
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .park_reason(self.ip)
                == Some(ParkReason::Corruption)
        {
            stay_down(&seat.checker, Down::StorageParked(self.rank));
        }
    }

    fn delete(&mut self, journal: paros::JournalKey) {
        if let Some(seat) = self.seats.iter_mut().find(|seat| seat.journal == journal) {
            seat.deleted = true;
        }
    }
}

/// The write-path fault layer of a quiet seat (#189): never active.
fn quiet_faults(ctx: &SimContext) -> StorageFaults<SimTimeProvider> {
    StorageFaults::new(
        ctx.time().clone(),
        Duration::ZERO,
        false,
        WritePathRates::default(),
    )
}

/// Arm the system journals on a genesis node (#189): the board, the seats of
/// the system journals on a seed, every seat's port reporting to the board, and
/// the plan the driver follows them by.
fn system_rig(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    plan: &crate::shape::JournalPlan,
    seats: &mut Vec<Seat>,
    self_rank: NodeId,
) -> (SystemPlan, Arc<Mutex<crate::audit::system::SystemBoard>>) {
    let board = crate::audit::system::system_board(ctx.state());
    let (system_plan, seeds) = system_plan(ctx, deployment, members, plan, self_rank);
    for seat in seats.iter_mut() {
        seat.audit = seat.audit.clone().with_system(board.clone());
    }
    if seeds.contains(&self_rank) {
        for journal in [paros::system::DIRECTORY, paros::system::REGISTRY] {
            let config = Config {
                journal,
                id: self_rank,
                peers: seeds.clone(),
                quorum_system: paros::QuorumSystem::Majority,
                ..Config::default()
            };
            seats.push(Seat::quiet(ctx, journal, config, &board));
        }
    }
    (system_plan, board)
}

/// The system plan every node of the run follows (#189), with the seeds'
/// identities; arms the system board on the way.
fn system_plan(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    plan: &crate::shape::JournalPlan,
    self_id: NodeId,
) -> (SystemPlan, Vec<NodeId>) {
    let seeds: Vec<NodeId> = crate::shape::seed_ranks(members.len())
        .into_iter()
        .map(NodeId)
        .collect();
    let board = crate::audit::system::system_board(ctx.state());
    let spares = spare_template(ctx, deployment);
    crate::audit::system::lock(&board).arm(
        plan.ids.iter().copied(),
        members.iter().map(|(id, _)| id.0),
        !deployment.joiners().is_empty(),
        spares.is_some() && !deployment.joiners().is_empty(),
    );
    (
        SystemPlan {
            self_id,
            seeds: members
                .iter()
                .filter(|(id, _)| seeds.contains(id))
                .cloned()
                .collect(),
            genesis_pool: members.iter().map(|(id, _)| *id).collect(),
            genesis_journals: plan.ids.clone(),
            spares: spares.into_iter().collect(),
        },
        seeds,
    )
}

/// The default journal's configuration a joiner joins as a spare once the
/// registry admits it (#189) — the same run-level draws every genesis node
/// built its own from (the bootstrap ranks, the matchmaker set, the quorum
/// policy; each fixed by its first caller). Only where a reconfiguration can
/// pull a joiner in and every process of the deployment can reach it: a
/// seed with matchmakers, and neither proxy leaders nor replicas (their
/// address books and pools are static; a joiner leading would reach
/// neither).
fn spare_template(ctx: &SimContext, deployment: &Deployment) -> Option<Config> {
    if deployment.matchmakers().is_empty()
        || !deployment.proxies().is_empty()
        || !deployment.replicas().is_empty()
    {
        return None;
    }
    let pool_len = deployment.acceptors().len();
    let bootstrap: Vec<NodeId> = crate::shape::bootstrap_ranks(ctx.state(), pool_len, true, true)
        .into_iter()
        .map(NodeId)
        .collect();
    let matchmaker_len = deployment.matchmakers().len();
    let matchmakers: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_len, true)
            .into_iter()
            .map(MatchmakerId)
            .collect();
    let policy = crate::shape::quorum_policy(ctx.state(), pool_len, true);
    Some(Config {
        journal: paros::JournalKey::default(),
        id: NodeId(0),
        quorum_system: policy.system(bootstrap.len()),
        peers: bootstrap,
        nodes: (0..pool_len as u64).map(NodeId).collect(),
        matchmakers,
        matchmaker_pool: (0..matchmaker_len as u64).map(MatchmakerId).collect(),
        proxy_count: 0,
        replica_count: 0,
    })
}

/// A matchmaker: the provider-generic registry driver inside the same
/// crash/recovery loop as the node — a seam crash unwinds `run_matchmaker`,
/// the volatile `Matchmaker` is dropped, and the next incarnation restores
/// its registry from the durable world.
// One recovery loop with per-exit-kind handling, like `run_acceptor`'s.
#[allow(clippy::too_many_lines)]
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
    let mut config = MatchmakerConfig {
        id,
        bootstrap: bootstrap.clone(),
    };
    // A matchmaker has a shape too: its transport tunables and its
    // write-window crash bias, drawn once per seed like a node's.
    let RoleRig {
        incarnation,
        hooks,
        checker,
        audit,
    } = arm_role(ctx, my_ip, perturb);
    let shape = incarnation.shape;
    // The registry's own write path rides the seed's node-disk profile and
    // the same chaos window (see `world::matchmaker`): the only fault it
    // draws is the whole-batch fsync failure, budgeted by the world.
    let registry_faults = storage_faults(ctx, perturb, shape.write_rates);
    if incarnation.is_restart()
        && ctx.time().now() < crate::CHAOS_DURATION
        && moonpool_sim::buggify_with_prob!(f64::from(shape.matchmaker_loss_pct) / 100.0)
        && world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .wipe_matchmaker(my_ip, bootstrap.len())
    {
        // The registry's wipe coin (#125, #183): a restart that comes back
        // on an empty disk. What happens next is the **library's** call: the
        // matchmaker boots below as an existing member on an empty store, and
        // `run_matchmaker` refuses the amnesiac registry. There is no
        // in-place repair — the surviving quorum reconstructs a successor
        // set without it. BUGGIFY pairing: the coin fired within the budget.
        assert_reachable!("matchmaker: a restarted matchmaker's registry is lost for good");
        tracing::info!(matchmaker = id.0, "matchmaker_wiped");
    }
    // #207: the operator restarts this matchmaker with an edited bootstrap
    // set in its configuration file; the library refuses the registry
    // formatted under the old one, and the operator restores the file and
    // restarts (the `ConfigMismatch` arm below). Applied in the loop, and
    // only to a matchmaker the provisioning ledger knows.
    let mut edit = OperatorEdit::new(
        incarnation.is_restart()
            && perturb
            && config.bootstrap.len() > 1
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.config_edit_pct) / 100.0),
    );
    loop {
        // The operator's claim is the provisioning ledger (#183), kept
        // outside the disks: a wipe erases the marker, never the memory of
        // having provisioned the matchmaker.
        let boot = if world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(my_ip)
        {
            BootKind::ExistingMember
        } else {
            BootKind::FirstBoot
        };
        if edit.apply(boot == BootKind::ExistingMember, &mut config, |config| {
            config.bootstrap.pop();
        }) {
            // BUGGIFY pairing: the operator's edit genuinely reaches a boot.
            assert_reachable!("operator: a matchmaker restarts under an edited configuration");
            tracing::info!(matchmaker = id.0, "matchmaker_config_edited");
        }
        let storage = DurableMatchmakerStorage::restore(
            Arc::downgrade(&world),
            my_ip.to_string(),
            registry_faults.clone(),
            bootstrap.len(),
        );
        match run_matchmaker(
            ctx.providers().clone(),
            storage,
            boot,
            parse_addr(my_ip)?,
            config.clone(),
            shape.tunables,
            ctx.shutdown().clone(),
            &hooks,
            &audit,
        )
        .await
        {
            // A seam crash, or the registry's own fsync failure: both mean
            // this incarnation's un-synced batch is gone, so both rebuild
            // from the durable world, after the matchmaker's own
            // restart-delay knob. A restart may then draw the wipe coin and
            // hand the replacement to a matchmaker-set reconfiguration.
            // Its floor is structural: a matchmaker held down is a
            // matchmaking phase that waits, never a cluster that stalls.
            Err(RunError::SeamCrash(_) | RunError::Storage(_)) => {
                restart_delay!(
                    ctx,
                    "a seam-crashed matchmaker restarts after a buggified delay"
                );
            }
            // The library refused the registry (#183). Amnesia is the wipe
            // coin's outcome and the one the rule exists for: the matchmaker
            // stays down for the run, replaced by a handover. The harness
            // cross-checks the refusal against its own injection: only a
            // wiped registry is ever amnesiac here.
            Err(RunError::Refused(BootRefusal::Amnesia)) => {
                let wiped = world
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .is_matchmaker_parked(my_ip);
                assert_always!(
                    wiped,
                    "matchmaker: an amnesia refusal names a wiped registry",
                    { "matchmaker" => id.0 }
                );
                stay_down(&checker, Down::MatchmakerLost(id.0));
                return Ok(());
            }
            // #207: the library refused a registry formatted under another
            // configuration; only the operator's edit above changes one.
            // The operator restores the file and restarts.
            Err(RunError::Refused(BootRefusal::ConfigMismatch)) => {
                let restored = edit.restore(&mut config);
                assert_always!(
                    restored,
                    "matchmaker: a configuration refusal names an operator's edit",
                    { "matchmaker" => id.0 }
                );
                if !restored {
                    return Err(SimulationError::InvalidState(format!(
                        "matchmaker {} refused a configuration nobody edited",
                        id.0
                    )));
                }
                tracing::info!(matchmaker = id.0, "matchmaker_config_restored");
                restart_delay!(
                    ctx,
                    "a matchmaker refused under an edited configuration restarts under the restored one"
                );
            }
            // A first boot on a formatted registry is a harness bug: the
            // provisioning ledger and the disks disagree.
            Err(RunError::Refused(BootRefusal::AlreadyFormatted)) => {
                assert_always!(
                    false,
                    "matchmaker: a first boot never meets a formatted registry",
                    { "matchmaker" => id.0 }
                );
                return Err(SimulationError::InvalidState(format!(
                    "matchmaker {} booted as first boot on a formatted registry",
                    id.0
                )));
            }
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
        }
    }
}

/// A proxy leader (#142): the provider-generic proxy driver, with no disk and
/// no recovery loop — the only exit it has is an infrastructure failure or the
/// shutdown, and a process kill (attrition on its own group) reboots it empty
/// through a fresh factory instance, exactly as production would restart the
/// process.
#[tracing::instrument(level = "debug", skip_all, fields(proxy = id.0))]
async fn run_proxy_role(
    ctx: &SimContext,
    deployment: &Deployment,
    id: ProxyId,
    my_ip: &str,
    perturb: bool,
) -> SimulationResult<()> {
    // The same address book every node sends through: the fan-out reaches
    // the column's acceptors and the `Commit` every learner, all of them
    // nodes of the pool.
    let members = ranked(deployment.acceptors(), NodeId)?;
    let has_matchmakers = !deployment.matchmakers().is_empty();
    let config = ProxyConfig {
        id,
        acceptors: bootstrap_config(ctx, members.len(), has_matchmakers, perturb),
        journal: paros::JournalKey::default(),
    };
    // A proxy has a shape too — its tick cadence and transport tunables —
    // drawn once per seed like a node's and kept across its reboots.
    let RoleRig {
        incarnation,
        hooks,
        audit,
        ..
    } = arm_role(ctx, my_ip, perturb);
    run_proxy(
        ctx.providers().clone(),
        parse_addr(my_ip)?,
        config,
        members,
        replica_book(deployment)?,
        incarnation.shape.tunables,
        ctx.shutdown().clone(),
        &hooks,
        &audit,
    )
    .await
    .map_err(|e| match e {
        RunError::Infra(e) => e,
        // A proxy has no storage and no seam: the driver's other exits are
        // unreachable here, and one showing up is a driver bug.
        other => SimulationError::InvalidState(format!("proxy {} exited with {other}", id.0)),
    })
}

/// A replica's configuration: the bootstrap membership it learns from,
/// outside the pool, and the matchmakers too, as `parosd` hands them
/// (#206): on a matchmaker deployment a replica follows the configuration
/// the beats carry, and a quorum read it serves is bound to the one the
/// acceptors it asks are in. Without them it stays bound to the bootstrap,
/// and every acceptor that registered a campaign answers from a later
/// configuration — a one-node deployment's replica never served a read.
fn replica_config(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    id: NodeId,
    perturb: bool,
) -> Config {
    let has_matchmakers = !deployment.matchmakers().is_empty();
    let bootstrap = bootstrap_config(ctx, members.len(), has_matchmakers, perturb);
    let matchmaker_pool: Vec<MatchmakerId> = (0..deployment.matchmakers().len() as u64)
        .map(MatchmakerId)
        .collect();
    let matchmakers: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_pool.len(), perturb)
            .into_iter()
            .map(MatchmakerId)
            .collect();
    Config {
        id,
        peers: bootstrap.members().to_vec(),
        quorum_system: bootstrap.quorum_system(),
        nodes: members.iter().map(|(node, _)| *node).collect(),
        matchmakers,
        matchmaker_pool,
        replica_count: deployment.replica_count(),
        ..Config::default()
    }
}

/// A replica (#144): the provider-generic replica driver inside the same
/// crash/recovery loop as a node — a seam crash unwinds `run_replica`, the
/// volatile `ReplicaNode` is dropped, and the next incarnation rebuilds it
/// from the durable world. Its disk is its own slice of the world under its
/// IP, registered outside the copy budget and fault-free: the budget defends
/// the acceptors' copies, and a replica's records are never one. A replica
/// holds no promise, so a lost disk would only mean a first boot that
/// relearns the log; the world never takes one away.
#[tracing::instrument(level = "debug", skip_all, fields(replica = rank.0))]
async fn run_replica_role(
    ctx: &SimContext,
    deployment: &Deployment,
    rank: ReplicaId,
    my_ip: &str,
    perturb: bool,
) -> SimulationResult<()> {
    let members = ranked(deployment.acceptors(), NodeId)?;
    let id = replica_node_id(rank);
    let config = replica_config(ctx, deployment, &members, id, perturb);
    let RoleRig {
        incarnation,
        hooks,
        checker,
        audit,
    } = arm_role(ctx, my_ip, perturb);
    let world = storage_world(ctx.state());
    {
        let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
        guard.note_replica(my_ip);
    }
    let faults = StorageFaults::new(
        ctx.time().clone(),
        Duration::ZERO,
        false,
        WritePathRates::default(),
    );
    loop {
        let boot = if world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(my_ip)
        {
            BootKind::ExistingMember
        } else {
            BootKind::FirstBoot
        };
        let storage = DurableStorage::restore(
            config.clone(),
            Arc::downgrade(&world),
            my_ip.to_string(),
            id.0,
            faults.clone(),
            checker.clone(),
        );
        match run_replica(
            ctx.providers().clone(),
            storage,
            parse_addr(my_ip)?,
            members.clone(),
            boot,
            incarnation.shape.tunables,
            ctx.shutdown().clone(),
            &hooks,
            &audit,
        )
        .await
        {
            Err(RunError::SeamCrash(_)) => {
                restart_delay!(
                    ctx,
                    "a seam-crashed replica restarts after a buggified delay"
                );
            }
            // A fault-free disk never fails a write; one showing up is a
            // harness bug, recorded and refused.
            Err(RunError::Storage(e)) => {
                assert_always!(
                    false,
                    "replica: a fault-free replica disk never fails",
                    { "replica" => id.0, "error" => e.to_string() }
                );
                return Err(SimulationError::InvalidState(format!(
                    "replica {} storage fault: {e}",
                    id.0
                )));
            }
            // The world never wipes a replica and the provisioning ledger
            // records its format, so its claim always matches its disk.
            Err(RunError::Refused(refusal)) => {
                assert_always!(
                    false,
                    "replica: a replica's boot claim matches its disk",
                    { "replica" => id.0, "refusal" => format!("{refusal:?}") }
                );
                return Err(SimulationError::InvalidState(format!(
                    "replica {} refused a boot: {refusal:?}",
                    id.0
                )));
            }
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
        }
    }
}

use moonpool_sim::SimStorageProvider;

/// The two contract suites against the library's journal stores
/// ([`paros::JournalStorage`], [`paros::JournalMatchmakerStorage`]) on the
/// simulated disk, each store in a directory of its own.
#[tracing::instrument(level = "debug", skip_all)]
async fn journal_contract_suites(provider: SimStorageProvider) {
    use paros::{
        JournalMatchmakerStorage, JournalStorage, JournalStoreConfig, LogStorage, MatchmakerStorage,
    };
    let config = Config {
        id: NodeId(0),
        peers: vec![NodeId(0)],
        ..Config::default()
    };
    let store = JournalStoreConfig {
        checkpoint_after: 4,
        ..JournalStoreConfig::small()
    };
    let open_node = |provider: SimStorageProvider, dir: String, config: Config| async move {
        let mut node = JournalStorage::new(provider, dir, config, store);
        node.boot_scan().await.expect("a clean journal store boots");
        node
    };
    let mut instance = 0_u64;
    let fresh = || {
        instance += 1;
        open_node(
            provider.clone(),
            format!("journal-contract/node-{instance}"),
            config.clone(),
        )
    };
    let reopen = |old: JournalStorage<SimStorageProvider>| {
        let dir = old.dir().to_string();
        drop(old);
        open_node(provider.clone(), dir, config.clone())
    };
    Box::pin(paros::storage_contract_suite(fresh, reopen)).await;
    let open_registry = |provider: SimStorageProvider, dir: String| async move {
        let mut registry = JournalMatchmakerStorage::new(provider, dir, store);
        registry
            .boot_scan()
            .await
            .expect("a clean journal registry boots");
        registry
    };
    let mut registry_instance = 0_u64;
    let fresh_registry = || {
        registry_instance += 1;
        open_registry(
            provider.clone(),
            format!("journal-contract/mm-{registry_instance}"),
        )
    };
    let reopen_registry = |old: JournalMatchmakerStorage<SimStorageProvider>| {
        let dir = old.dir().to_string();
        drop(old);
        open_registry(provider.clone(), dir)
    };
    Box::pin(paros::matchmaker_storage_contract_suite(
        fresh_registry,
        reopen_registry,
    ))
    .await;
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
        {
            let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
            guard.set_budget(1, 1);
            guard.set_pool_size(1);
        }
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
            std::future::ready(DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                format!("10.9.9.{instance}"),
                100 + instance,
                faults.clone(),
                Arc::new(AuditWorld::client_free()),
            ))
        };
        // A reopen is a clean reboot of the same store: drop the handle and
        // re-restore from the world's durable records under the same key, and
        // under the same checker (the boot replay re-walks its applied prefix).
        let reopen = |old: DurableStorage<_>| {
            let (key, node_id, checker) = (old.key.clone(), old.node_id, old.checker.clone());
            drop(old);
            std::future::ready(DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                key,
                node_id,
                faults.clone(),
                checker,
            ))
        };
        Box::pin(paros::storage_contract_suite(fresh, reopen)).await;
        // The matchmaker registry's contract, against its world-backed store.
        let mut registry_instance = 0_u64;
        let fresh_registry = || {
            registry_instance += 1;
            std::future::ready(DurableMatchmakerStorage::restore(
                Arc::downgrade(&world),
                format!("10.9.8.{registry_instance}"),
                faults.clone(),
                0,
            ))
        };
        let reopen_registry = |old: DurableMatchmakerStorage<_>| {
            let key = old.key().to_string();
            drop(old);
            std::future::ready(DurableMatchmakerStorage::restore(
                Arc::downgrade(&world),
                key,
                faults.clone(),
                0,
            ))
        };
        paros::matchmaker_storage_contract_suite(fresh_registry, reopen_registry).await;
        // The durable stores the library ships (`paros::journal`), over this
        // simulation's own disk: the same two suites, reopened through a
        // real journal open and boot scan.
        Box::pin(journal_contract_suites(ctx.storage().clone())).await;
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
            let mut store = DurableMatchmakerStorage::restore(
                Arc::downgrade(&world),
                key.clone(),
                faults.clone(),
                0,
            );
            store
                .register(ballot(1), &config)
                .await
                .expect("register 1");
            store.sync().await.expect("sync 1");
            store
                .register(ballot(2), &config)
                .await
                .expect("register 2 (never synced)");
            store
                .set_gc_watermark(ballot(1))
                .await
                .expect("raise (never synced)");
            drop(store);
            let rebooted =
                DurableMatchmakerStorage::restore(Arc::downgrade(&world), key, faults.clone(), 0);
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
