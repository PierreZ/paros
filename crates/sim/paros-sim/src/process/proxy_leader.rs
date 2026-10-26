//! The proxy leader role (#142): no disk and no recovery loop.

use moonpool_sim::{SimContext, SimulationError, SimulationResult};

use super::RoleRig;
use super::arm_role;
use super::ranked;
use super::replica_book;
use super::{journal_config, main_config};
use crate::audit::{NodeAudit, audit_world_for};
use crate::roles::Deployment;
use paros::core::NodeId;
use paros::core::membership::ProxyId;
use paros::roles::node::RunError;
use paros::roles::node::parse_addr;
use paros::roles::proxy_leader::ProxyConfig;
use paros::roles::proxy_leader::ProxyJournal;
use paros::roles::proxy_leader::run_proxy;

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
    // The stalled-proxy scenario decides the driver's named location before
    // the proxy's first vote (#341).
    crate::shape::stalled_proxy(ctx.state());
    // Every journal of the run, each over its own bootstrap configuration
    // (#193): one proxy leader per journal inside the one process.
    let main = main_config(ctx, deployment, &members, NodeId(0));
    let policy = crate::shape::quorum_policy(ctx.state(), members.len());
    let plan = crate::shape::journals(ctx.state());
    let config = ProxyConfig {
        id,
        journals: plan
            .ids
            .iter()
            .map(|&journal| {
                let config = journal_config(&main, policy, &plan, journal);
                ProxyJournal {
                    journal,
                    acceptors: config.bootstrap(),
                }
            })
            .collect(),
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
        crate::tls::serving(ctx, paros::net::tls::Role::Machine, my_ip)?,
        ctx.shutdown().clone(),
        &audit,
        // Each journal's facts report to its own audit world (#193).
        |journal| NodeAudit::new(ctx.time().clone(), audit_world_for(ctx.state(), journal)),
    )
    .await
    .map_err(|e| match e {
        RunError::Infra(e) => e,
        // A proxy has no storage and no seam: the driver's other exits are
        // unreachable here, and one showing up is a driver bug.
        other => SimulationError::InvalidState(format!("proxy {} exited with {other}", id.0)),
    })
}
