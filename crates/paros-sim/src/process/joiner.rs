//! The joiner process (#189) and the idle process the contract suite runs beside.

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationError, SimulationResult, assert_always};

use super::RoleRig;
use super::acceptor::Seat;
use super::acceptor::system_plan;
use super::arm_role;
use super::dispatch;
use super::ranked;
use super::stores::SimStores;
use super::stores::resolve_provisioning;
use crate::roles::{Deployment, Role};
use paros::{MatchmakerId, NodeId, RunError, parse_addr, run_journals};

/// A joiner in the simulation (#189): its own process group, a node outside
/// the genesis pool. On a seed that runs the system journals it follows the
/// registry from the seeds, is admitted to the pool when a client registers
/// it, and joins the default journal as a spare where a reconfiguration may
/// name it; on any other seed it idles.
pub(crate) struct JoinerProcess;

impl JoinerProcess {
    pub(crate) fn chaotic() -> Self {
        Self
    }
}

#[async_trait]
impl Process for JoinerProcess {
    fn name(&self) -> &'static str {
        crate::roles::JOINER_GROUP
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        dispatch(
            ctx,
            "every joiner process is mapped to the joiner role",
            "a joiner",
            |role| match role {
                Role::Joiner(id) => Some(id),
                _ => None,
            },
            |deployment, id, my_ip| async move {
                Box::pin(run_joiner(ctx, &deployment, id, &my_ip)).await
            },
        )
        .await
    }
}

/// A joiner (#189): `run_journals` with no journal of its own and the
/// system plan, in the same recovery loop as a node. Its disk is
/// fault-free (every seat it gets is a created journal's), and it is not an
/// attrition victim (so no driver `hint!` kills it either): a joiner's
/// lifecycle is the registry's.
#[tracing::instrument(level = "debug", skip_all, fields(node = id.0))]
async fn run_joiner(
    ctx: &SimContext,
    deployment: &Deployment,
    id: NodeId,
    my_ip: &str,
) -> SimulationResult<()> {
    if !crate::shape::system_journals(ctx.state()) {
        // No system journals on this seed: nothing to join.
        ctx.shutdown().cancelled().await;
        return Ok(());
    }
    let members = ranked(deployment.acceptors(), NodeId)?;
    // A joiner that joins the default journal as a spare campaigns through
    // the matchmakers like any member of it.
    let matchmakers = ranked(deployment.matchmakers(), MatchmakerId)?;
    let board = crate::audit::system::system_board(ctx.state());
    let (system_plan, _) = system_plan(ctx, deployment, &members, id);
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let tunables = incarnation.shape.tunables;
    let layout = crate::shape::journal_layout(ctx.state());
    let mut seats: Vec<Seat> = Vec::new();
    loop {
        resolve_provisioning(ctx, &seats, my_ip).await;
        let stores = SimStores {
            ctx,
            seats: &mut seats,
            ip: my_ip,
            rank: id.0,
            journal_store: (ctx.storage().clone(), layout),
            system: Some(board.clone()),
        };
        match Box::pin(run_journals(
            ctx.providers().clone(),
            stores,
            id,
            parse_addr(my_ip)?,
            members.clone(),
            matchmakers.clone(),
            Vec::new(),
            Vec::new(),
            Some(system_plan.clone()),
            None,
            tunables,
            ctx.shutdown().clone(),
        ))
        .await
        {
            // A failed storage call is a fail-stop exit (`parosd` exits
            // 75): the supervisor starts it again at once.
            Err(RunError::Storage(_)) => {}
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
