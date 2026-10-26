//! The replica role (#144): its own ordered journal store, outside the copy budget.

use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{SimContext, SimTimeProvider, SimulationError, SimulationResult, assert_always};

use super::RoleRig;
use super::arm_role;
use super::ranked;
use super::stores::journal_dir;
use super::stores::resolve_journal_provisioning;
use super::{journal_config, main_config};
use crate::audit::{AuditWorld, NodeAudit, audit_world_for};
use crate::roles::{Deployment, replica_node_id};
use crate::world::StorageWorld;
use crate::world::node_store::LedgeredJournal;
use crate::world::storage_world_for;
use paros::{
    BootKind, Config, JournalIdentifier, JournalStorage, JournalStoreConfig, JournalStores, NodeId,
    ReplicaId, RunError, parse_addr, run_replica,
};

/// A replica's configuration of every journal of the run (#193): the main
/// journal's bootstrap membership it learns from, outside the pool, and the
/// matchmakers too, as `parosd` hands them (#206): on a matchmaker
/// deployment a replica follows the configuration the beats carry, and a
/// quorum read it serves is bound to the one the acceptors it asks are in.
/// Without them it stays bound to the bootstrap, and every acceptor that
/// registered a campaign answers from a later configuration — a one-node
/// deployment's replica never served a read. Every other journal is
/// derived as the acceptors derive it ([`journal_config`]). A replica
/// delegates nothing: its proxy count is zero.
fn replica_configs(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    id: NodeId,
) -> Vec<Config> {
    let main = main_config(ctx, deployment, members, id);
    let policy = crate::shape::quorum_policy(ctx.state(), members.len());
    let plan = crate::shape::journals(ctx.state());
    plan.ids
        .iter()
        .map(|&journal| Config {
            proxy_count: 0,
            ..journal_config(&main, policy, &plan, journal)
        })
        .collect()
}

/// One journal of a replica: its configuration, its storage ledger and its
/// audit world.
struct ReplicaSeat {
    config: Config,
    world: Arc<Mutex<StorageWorld>>,
    checker: Arc<AuditWorld>,
}

/// A replica's journal stores (#193): one ordered `JournalStorage` per
/// journal of the run on the replica's simulated disk, the boot claim read
/// off each journal's provisioning ledger.
struct ReplicaStores<'a> {
    ctx: &'a SimContext,
    ip: &'a str,
    seats: &'a [ReplicaSeat],
    layout: JournalStoreConfig,
}

impl ReplicaStores<'_> {
    fn seat(&self, journal: JournalIdentifier) -> Option<&ReplicaSeat> {
        self.seats
            .iter()
            .find(|seat| seat.config.journal == journal)
    }
}

impl JournalStores for ReplicaStores<'_> {
    type Store = LedgeredJournal;
    type Audit = NodeAudit<SimTimeProvider>;

    fn journals(&self) -> Vec<JournalIdentifier> {
        self.seats.iter().map(|seat| seat.config.journal).collect()
    }

    async fn open(&mut self, journal: JournalIdentifier) -> Option<(Self::Store, BootKind)> {
        let seat = self.seat(journal)?;
        resolve_journal_provisioning(self.ctx, &seat.world, journal, self.ip).await;
        // The world never wipes a replica and the provisioning ledger
        // records its format, so its claim always matches its disk.
        let boot = if seat
            .world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(self.ip)
        {
            BootKind::ExistingMember
        } else {
            BootKind::FirstBoot
        };
        let store = LedgeredJournal::new(
            JournalStorage::new(
                self.ctx.storage().clone(),
                journal_dir(journal),
                seat.config.clone(),
                self.layout,
            ),
            Arc::downgrade(&seat.world),
            self.ip.to_string(),
            self.ctx.state().clone(),
            seat.checker.clone(),
            crate::world::node_store::DamagePolicy {
                chaotic: false,
                cut_budget: None,
                inject: false,
            },
            self.ctx.storage().clone(),
        );
        Some((store, boot))
    }

    fn audit(&self, journal: JournalIdentifier) -> Self::Audit {
        NodeAudit::new(
            self.ctx.time().clone(),
            audit_world_for(self.ctx.state(), journal),
        )
    }

    /// The replica's own facts report to the main journal's audit world.
    fn node_audit(&self) -> Self::Audit {
        self.audit(crate::shape::identifiers(self.ctx.state()).main)
    }
}

/// A replica (#144): the provider-generic replica driver inside the same
/// recovery loop as a node — a fail-stop storage fault unwinds
/// `run_replica`, the volatile `ReplicaNode`s are dropped, and the next
/// incarnation rebuilds them from the durable world. It serves every journal
/// of the run (#193), each from its own store. Its disk is its own slice of
/// the world under its IP, registered outside the copy budget and
/// fault-free: the budget defends the acceptors' copies, and a replica's
/// records are never one. A replica holds no promise, so a lost disk would
/// only mean a first boot that relearns the log; the world never takes one
/// away.
#[tracing::instrument(level = "debug", skip_all, fields(replica = rank.0))]
pub(super) async fn run_replica_role(
    ctx: &SimContext,
    deployment: &Deployment,
    rank: ReplicaId,
    my_ip: &str,
) -> SimulationResult<()> {
    let members = ranked(deployment.acceptors(), NodeId)?;
    let id = replica_node_id(rank);
    let configs = replica_configs(ctx, deployment, &members, id);
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let seats: Vec<ReplicaSeat> = configs
        .into_iter()
        .map(|config| {
            let world = storage_world_for(ctx.state(), config.journal);
            world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .note_replica(my_ip);
            let checker = audit_world_for(ctx.state(), config.journal);
            ReplicaSeat {
                config,
                world,
                checker,
            }
        })
        .collect();
    // The replica's stores: the run's journal layout with two syncs per
    // commit (a cut mid-commit is torn or whole, never ambiguous) and no
    // injected damage (a zero chaos window): the replica tier stays outside
    // the copy budget (#144).
    let layout = JournalStoreConfig {
        durability: paros::storage::journal::Durability::Ordered,
        ..crate::shape::journal_layout(ctx.state())
    };
    loop {
        let stores = ReplicaStores {
            ctx,
            ip: my_ip,
            seats: &seats,
            layout,
        };
        match run_replica(
            ctx.providers().clone(),
            stores,
            parse_addr(my_ip)?,
            members.clone(),
            paros::DriverTunables {
                apply_page: crate::shape::replica_apply_page(
                    ctx.state(),
                    incarnation.shape.tunables.apply_page,
                ),
                ..incarnation.shape.tunables
            },
            crate::tls::serving(ctx, paros::net::tls::Role::Machine, my_ip)?,
            ctx.shutdown().clone(),
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
