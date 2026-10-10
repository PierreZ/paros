//! The replica role (#144): its own ordered journal store, outside the copy budget.

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use moonpool_sim::{SimContext, SimulationError, SimulationResult, assert_always};

use super::RoleRig;
use super::arm_role;
use super::bootstrap_config;
use super::ranked;
use super::stores::journal_dir;
use super::stores::resolve_journal_provisioning;
use crate::roles::{Deployment, replica_node_id};
use crate::world::node_store::LedgeredJournal;
use crate::world::storage_world;
use paros::{
    BootKind, Config, JournalStorage, JournalStoreConfig, MatchmakerId, NodeId, ReplicaId,
    RunError, parse_addr, run_replica,
};

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
) -> Config {
    let has_matchmakers = !deployment.matchmakers().is_empty();
    let bootstrap = bootstrap_config(ctx, members.len(), has_matchmakers);
    let matchmaker_pool: Vec<MatchmakerId> = (0..deployment.matchmakers().len() as u64)
        .map(MatchmakerId)
        .collect();
    let matchmakers: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_pool.len())
            .into_iter()
            .map(MatchmakerId)
            .collect();
    Config {
        peers: bootstrap.members().to_vec(),
        quorum_system: bootstrap.quorum_system(),
        nodes: members.iter().map(|(node, _)| *node).collect(),
        matchmakers,
        matchmaker_pool,
        replica_count: deployment.replica_count(),
        ..Config::new(id, crate::shape::identifiers(ctx.state()).main)
    }
}

/// A replica (#144): the provider-generic replica driver inside the same
/// recovery loop as a node — a fail-stop storage fault unwinds
/// `run_replica`, the volatile `ReplicaNode` is dropped, and the next incarnation rebuilds it
/// from the durable world. Its disk is its own slice of the world under its
/// IP, registered outside the copy budget and fault-free: the budget defends
/// the acceptors' copies, and a replica's records are never one. A replica
/// holds no promise, so a lost disk would only mean a first boot that
/// relearns the log; the world never takes one away.
#[tracing::instrument(level = "debug", skip_all, fields(replica = rank.0))]
pub(super) async fn run_replica_role(
    ctx: &SimContext,
    deployment: &Deployment,
    rank: ReplicaId,
    my_ip: &str,
) -> SimulationResult<()> {
    let members = ranked(deployment.acceptors(), NodeId)?;
    let id = replica_node_id(rank);
    let config = replica_config(ctx, deployment, &members, id);
    let RoleRig {
        incarnation,
        checker,
        audit,
        ..
    } = arm_role(ctx, my_ip);
    let world = storage_world(ctx.state());
    {
        let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
        guard.note_replica(my_ip);
    }
    // The replica's store: the run's journal layout with two syncs per
    // commit (a cut mid-commit is torn or whole, never ambiguous) and no
    // injected damage (a zero chaos window): the replica tier stays outside
    // the copy budget (#144).
    let layout = JournalStoreConfig {
        durability: paros::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    loop {
        resolve_journal_provisioning(ctx, &world, config.journal, my_ip).await;
        let boot = if world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(my_ip)
        {
            BootKind::ExistingMember
        } else {
            BootKind::FirstBoot
        };
        let storage = LedgeredJournal::new(
            JournalStorage::new(
                ctx.storage().clone(),
                journal_dir(config.journal),
                config.clone(),
                layout,
            ),
            Arc::downgrade(&world),
            my_ip.to_string(),
            (ctx.state().clone(), ctx.time().clone()),
            checker.clone(),
            crate::world::node_store::DamagePolicy {
                chaos_until: Duration::ZERO,
                cut_budget: None,
                inject: false,
            },
            ctx.storage().clone(),
        );
        match run_replica(
            ctx.providers().clone(),
            storage,
            parse_addr(my_ip)?,
            members.clone(),
            boot,
            incarnation.shape.tunables,
            ctx.shutdown().clone(),
            &audit,
        )
        .await
        {
            // The simulated disk's own chaos (a failed sync, a short
            // transfer) reaches a replica's journal too: a fail-stop the
            // process restarts from, never a lost copy (nothing is injected
            // into it).
            Err(RunError::Storage(_)) => {}
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
