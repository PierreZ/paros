//! The proxy leader role (#142): no disk and no recovery loop.

use moonpool_sim::{SimContext, SimulationError, SimulationResult};

use super::RoleRig;
use super::arm_role;
use super::bootstrap_config;
use super::ranked;
use super::replica_book;
use crate::roles::Deployment;
use paros::{NodeId, ProxyConfig, ProxyId, RunError, parse_addr, run_proxy};

/// A proxy leader (#142): the provider-generic proxy driver, with no disk and
/// no recovery loop — the only exit it has is an infrastructure failure or the
/// shutdown, and a process kill (attrition on its own group) reboots it empty
/// through a fresh factory instance, exactly as production would restart the
/// process.
#[tracing::instrument(level = "debug", skip_all, fields(proxy = id.0))]
pub(super) async fn run_proxy_role(
    ctx: &SimContext,
    deployment: &Deployment,
    id: ProxyId,
    my_ip: &str,
) -> SimulationResult<()> {
    // The same address book every node sends through: the fan-out reaches
    // the column's acceptors and the `Commit` every learner, all of them
    // nodes of the pool.
    let members = ranked(deployment.acceptors(), NodeId)?;
    let has_matchmakers = !deployment.matchmakers().is_empty();
    // The stalled-proxy scenario decides the driver's named location before
    // the proxy's first vote (#341).
    crate::shape::stalled_proxy(ctx.state());
    let config = ProxyConfig {
        id,
        acceptors: bootstrap_config(ctx, members.len(), has_matchmakers),
        journal: crate::shape::identifiers(ctx.state()).main,
    };
    // A proxy has a shape too — its tick cadence and transport tunables —
    // drawn once per seed like a node's and kept across its reboots.
    let RoleRig {
        incarnation, audit, ..
    } = arm_role(ctx, my_ip);
    run_proxy(
        ctx.providers().clone(),
        parse_addr(my_ip)?,
        config,
        members,
        replica_book(deployment)?,
        incarnation.shape.tunables,
        ctx.shutdown().clone(),
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
