//! The sim-side adapter: the moonpool [`Process`]es that run the
//! provider-generic [`paros::run_node`] and [`paros::run_matchmaker`] drivers
//! under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. Each
//! role is its own moonpool process group (`crate::roles`): a [`NodeProcess`]
//! — an **acceptor** — wires the node to its journal stores on the simulated
//! disk (`JournalStorage`, through the storage ledger) and runs the same
//! `run_node` a production `tokio::main` would; a [`MatchmakerProcess`] runs
//! `run_matchmaker` over `JournalMatchmakerStorage`; a [`ProxyProcess`]
//! runs `run_proxy` (#142) with no store at all — a proxy leader holds
//! nothing durable, so a kill simply reboots it empty; a [`ReplicaProcess`]
//! runs `run_replica` (#144) over its own ordered journal store with the
//! injector dark, outside the copy budget — a replica is not an acceptor. Every role with a
//! disk sits inside a recovery loop that restarts it after a fail-stop
//! storage fault, as `parosd`'s supervisor would: the driver unwinds, the
//! volatile core is dropped, and the next iteration rebuilds it from its
//! journal store on the simulated disk. A process kill — moonpool attrition,
//! a driver `hint!` the attrition regime strikes (#294), or the chain
//! client's scripted reboot — aborts the task outright; the next incarnation
//! restores the same way.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimTimeProvider, SimulationError, SimulationResult, assert_always,
};

use crate::audit::{AuditWorld, NodeAudit, audit_world};
use crate::roles::{
    ACCEPTOR_GROUP, Deployment, MATCHMAKER_GROUP, PROXY_GROUP, REPLICA_GROUP, Role, replica_node_id,
};
use paros::{AcceptorConfig, NodeId, ReplicaId, parse_addr};

mod acceptor;
mod contract;
mod joiner;
mod matchmaker;
mod proxy;
mod replica;
mod stores;

use acceptor::run_acceptor;
pub(crate) use contract::ContractSuiteWorkload;
pub(crate) use joiner::{IdleProcess, JoinerProcess};
use matchmaker::run_matchmaker_role;
use proxy::run_proxy_role;
use replica::run_replica_role;

/// One role's address book: the group's IPs in rank order, each paired with
/// the identity its rank names — `NodeId`, `MatchmakerId` or `ProxyId`.
pub(super) fn ranked<I>(ips: &[String], id: fn(u64) -> I) -> SimulationResult<Vec<(I, String)>> {
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
pub(super) fn replica_book(deployment: &Deployment) -> SimulationResult<Vec<(NodeId, String)>> {
    ranked(deployment.replicas(), |rank| {
        replica_node_id(ReplicaId(rank))
    })
}

/// The **bootstrap acceptor configuration** of a seed, as every node and every
/// proxy leader derives it: the bootstrap ranks (`crate::shape::bootstrap_ranks`)
/// under the run's quorum-system policy at their own size. Protocol data drawn
/// once per seed, so every process reads the same one.
pub(super) fn bootstrap_config(
    ctx: &SimContext,
    pool: usize,
    has_matchmakers: bool,
) -> AcceptorConfig {
    let bootstrap: Vec<NodeId> = crate::shape::bootstrap_ranks(ctx.state(), pool, has_matchmakers)
        .into_iter()
        .map(NodeId)
        .collect();
    let policy = crate::shape::quorum_policy(ctx.state(), pool);
    let system = policy.system(bootstrap.len());
    AcceptorConfig::new(bootstrap, system)
}

/// One role incarnation's harness rig, armed the same way for every role:
/// the incarnation and its shape (`crate::shape::boot` — the rig's **only**
/// draw, so a caller keeps it exactly where its own registry draws expect
/// it), and the per-iteration audit — the shared checker every role folds into,
/// and this incarnation's port. The disk's fault layer is not here: a proxy
/// has no disk.
pub(crate) struct RoleRig {
    pub(crate) incarnation: crate::shape::Incarnation,
    pub(crate) checker: Arc<AuditWorld>,
    pub(crate) audit: NodeAudit<SimTimeProvider>,
}

/// Arm `my_ip`'s rig for this incarnation. The shape is what makes a
/// re-entry from a fresh factory instance a *restart* of the same node
/// rather than a new node with new knobs (see `crate::shape`); durable
/// state is the world's business, never the shape's. The audit is pure
/// observation, published beside the storage world so every node folds its
/// transitions into one incremental checker — it never influences the
/// driver; that is the inline BUGGIFY sites' job.
pub(crate) fn arm_role(ctx: &SimContext, my_ip: &str) -> RoleRig {
    let incarnation = crate::shape::boot(ctx.state(), my_ip);
    let checker = audit_world(ctx.state());
    let audit = NodeAudit::new(ctx.time().clone(), checker.clone());
    RoleRig {
        incarnation,
        checker,
        audit,
    }
}

/// Why an incarnation exits for good instead of booting (see the recovery
/// loops below): told to the audit, so convergence excuses exactly these
/// identities, and traced.
#[derive(Clone, Copy)]
pub(super) enum Down {
    /// A node terminally parked by a detected persistent corruption.
    StorageParked(u64),
    /// A node the operator retired (#123).
    Retired(u64),
    /// A matchmaker whose registry was wiped and whose boot the library
    /// refused (#125, #183), or whose journal registry a budgeted power cut
    /// left refusing to open (#176).
    MatchmakerLost(u64),
}

pub(super) fn stay_down(checker: &AuditWorld, down: Down) {
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

/// A paros node (an acceptor) in the simulation.
pub(crate) struct NodeProcess;

impl NodeProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

/// A matchmaker in the simulation: its own process group, so a seed draws
/// how many it deploys independently of the acceptor pool and attrition can
/// be scoped to it.
pub(crate) struct MatchmakerProcess;

impl MatchmakerProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

/// A proxy leader in the simulation (#142): its own process group, so a seed
/// draws how many it deploys independently of the other pools and attrition
/// can be scoped to it. Nothing durable: a kill reboots it empty.
pub(crate) struct ProxyProcess;

impl ProxyProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

/// A replica in the simulation (#144): its own process group, so a seed
/// draws how many it deploys independently of the other pools and attrition
/// can be scoped to it. Durable: a kill reboots it from its disk, and it
/// catches up from the acceptors.
pub(crate) struct ReplicaProcess;

impl ReplicaProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

/// Run one process as the role the deployment map gives its IP. The map is
/// read off the topology's process groups, so every process derives the
/// *same* map without coordination; `id` picks this group's role out of it
/// (its identity), and `run` runs it. A process whose IP the map puts in
/// another group is a harness bug: recorded under `unmapped` (the group's
/// always-assertion) and refused as not `what` of the deployment.
pub(crate) async fn dispatch<I, Fut>(
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
        dispatch(
            ctx,
            "every node process is mapped to the acceptor role",
            "an acceptor",
            |role| match role {
                Role::Acceptor(rank) => Some(rank),
                _ => None,
            },
            |deployment, self_rank, my_ip| async move {
                Box::pin(run_acceptor(ctx, &deployment, self_rank, &my_ip)).await
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
        dispatch(
            ctx,
            "every matchmaker process is mapped to the matchmaker role",
            "a matchmaker",
            |role| match role {
                Role::Matchmaker(id) => Some(id),
                _ => None,
            },
            |_, id, my_ip| async move { run_matchmaker_role(ctx, id, &my_ip).await },
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
        dispatch(
            ctx,
            "every proxy process is mapped to the proxy role",
            "a proxy leader",
            |role| match role {
                Role::Proxy(id) => Some(id),
                _ => None,
            },
            |deployment, id, my_ip| async move {
                run_proxy_role(ctx, &deployment, id, &my_ip).await
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
        dispatch(
            ctx,
            "every replica process is mapped to the replica role",
            "a replica",
            |role| match role {
                Role::Replica(rank) => Some(rank),
                _ => None,
            },
            |deployment, rank, my_ip| async move {
                run_replica_role(ctx, &deployment, rank, &my_ip).await
            },
        )
        .await
    }
}
