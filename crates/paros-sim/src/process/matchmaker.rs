//! The matchmaker role: the registry driver inside its recovery loop.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

use moonpool_sim::{
    SimContext, SimulationError, SimulationResult, TimeProvider, assert_always, assert_reachable,
};

use super::Down;
use super::RoleRig;
use super::acceptor::OperatorEdit;
use super::arm_role;
use super::stay_down;
use crate::audit::{AuditWorld, NodeAudit, audit_world_for};
use crate::world::registry_store::LedgeredRegistry;
use crate::world::storage_world;
use paros::{
    BootKind, BootRefusal, HostedSet, JournalId, JournalIdentifier, MatchmakerConfig, MatchmakerId,
    RunError, TenantId, parse_addr, run_matchmaker,
};

/// A matchmaker: the provider-generic registry driver inside the same
/// recovery loop as the node — a fail-stop storage fault unwinds
/// `run_matchmaker`, the volatile `Matchmaker` is dropped, and the next
/// incarnation restores its registry from the durable world.
// One recovery loop with per-exit-kind handling, like `run_acceptor`'s.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = id.0))]
pub(super) async fn run_matchmaker_role(
    ctx: &SimContext,
    id: MatchmakerId,
    my_ip: &str,
) -> SimulationResult<()> {
    let world = storage_world(ctx.state());
    let deployment = crate::roles::deployment(ctx.topology());
    // Generation 0's set is protocol data drawn once per seed (#125), the
    // same draw every node makes; this matchmaker may be a spare outside it.
    let bootstrap: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), deployment.matchmakers().len())
            .into_iter()
            .map(MatchmakerId)
            .collect();
    let mut config = MatchmakerConfig {
        id,
        bootstrap: bootstrap.clone(),
    };
    // A matchmaker has a shape too: its transport tunables and its
    // write-window crash bias, drawn once per seed like a node's.
    let RoleRig { incarnation, .. } = arm_role(ctx, my_ip);
    let shape = incarnation.shape;
    // One set per tenant of the run's journals (#190): every journal of a
    // matchmaker seed names the matchmakers, so each tenant's set keeps a
    // registry per journal of that tenant, and each journal's audit world
    // judges its own registry.
    let plan = crate::shape::journals(ctx.state());
    let mut tenants: BTreeMap<TenantId, Vec<JournalId>> = BTreeMap::new();
    for journal in &plan.ids {
        tenants
            .entry(journal.tenant)
            .or_default()
            .push(journal.journal);
    }
    if tenants.values().any(|journals| journals.len() > 1) {
        // BUGGIFY pairing: the plan's draw put two journals of one tenant
        // on a matchmaker seed.
        assert_reachable!("matchmaker: a set serves several journals");
    }
    if tenants.len() > 1 {
        assert_reachable!("matchmaker: a process hosts the sets of several tenants");
    }
    let checkers = |tenant: TenantId| -> BTreeMap<JournalId, Arc<AuditWorld>> {
        tenants[&tenant]
            .iter()
            .map(|journal| {
                (
                    *journal,
                    audit_world_for(ctx.state(), JournalIdentifier::new(tenant, *journal)),
                )
            })
            .collect()
    };
    let every_checker: Vec<Arc<AuditWorld>> = tenants
        .keys()
        .flat_map(|tenant| checkers(*tenant).into_values())
        .collect();
    let time = ctx.time().clone();
    let audit = |journal: JournalIdentifier| {
        NodeAudit::new(time.clone(), audit_world_for(ctx.state(), journal))
    };
    // The store (#176): the library's `JournalMatchmakerStorage` on the
    // simulated disk, with the seed's commit protocol and the library's
    // small geometry (a registry's installs carry every live registration
    // in one batch; the acceptors' geometry knob is not theirs to shrink).
    let layout = paros::JournalStoreConfig {
        geometry: paros::journal::Geometry::small(),
        ..crate::shape::journal_layout(ctx.state())
    };
    if incarnation.is_restart()
        && ctx.time().now() < crate::CHAOS_DURATION
        && moonpool_sim::buggify_with_prob!(f64::from(shape.matchmaker_loss_pct) / 100.0)
        && world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .wipe_matchmaker(my_ip, bootstrap.len())
    {
        // The disk is genuinely emptied: every set's registry files go,
        // durably.
        for tenant in tenants.keys() {
            crate::world::wipe::wipe_dir(
                ctx.storage(),
                &crate::world::registry_store::registry_dir(*tenant),
            )
            .await;
        }
        assert_reachable!("journal store: a wiped matchmaker's registry is deleted");
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
            && config.bootstrap.len() > 1
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.config_edit_pct) / 100.0),
    );
    loop {
        // An interrupted provisioning is resolved from the disk before the
        // claim is read (#176), set by set.
        for tenant in tenants.keys() {
            crate::world::registry_store::resolve_registry_provisioning(
                ctx.storage(),
                &world,
                *tenant,
                my_ip,
            )
            .await;
        }
        // The operator's claim is the provisioning ledger (#183), kept
        // outside the disks: a wipe erases the marker, never the memory of
        // having provisioned the matchmaker's set.
        let boots: BTreeMap<TenantId, BootKind> = tenants
            .keys()
            .map(|tenant| {
                let provisioned = world
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .provisioned(&crate::world::registry_store::set_key(my_ip, *tenant));
                let boot = if provisioned {
                    BootKind::ExistingMember
                } else {
                    BootKind::FirstBoot
                };
                (*tenant, boot)
            })
            .collect();
        let existing = boots.values().any(|boot| *boot == BootKind::ExistingMember);
        if edit.apply(existing, &mut config, |config| {
            config.bootstrap.pop();
        }) {
            // BUGGIFY pairing: the operator's edit genuinely reaches a boot.
            assert_reachable!("operator: a matchmaker restarts under an edited configuration");
            tracing::info!(matchmaker = id.0, "matchmaker_config_edited");
        }
        let sets: Vec<HostedSet<LedgeredRegistry>> = tenants
            .iter()
            .map(|(tenant, journals)| HostedSet {
                tenant: *tenant,
                journals: journals.clone(),
                storage: LedgeredRegistry::new(
                    ctx.storage().clone(),
                    *tenant,
                    layout,
                    Arc::downgrade(&world),
                    my_ip.to_string(),
                    ctx.state().clone(),
                    checkers(*tenant),
                    id.0,
                    (layout.durability == paros::journal::Durability::Batched)
                        .then_some(bootstrap.len()),
                ),
                boot: boots[tenant],
            })
            .collect();
        match run_matchmaker(
            ctx.providers().clone(),
            sets,
            parse_addr(my_ip)?,
            config.clone(),
            shape.tunables,
            ctx.shutdown().clone(),
            audit,
        )
        .await
        {
            // The registry's own failed sync: this incarnation's un-synced
            // batch is gone, so it rebuilds from the disk. A restart may then
            // draw the wipe coin and hand the replacement to a
            // matchmaker-set reconfiguration.
            Err(RunError::Storage(failure)) => {
                // A failed sync is a fail-stop the process restarts from. A
                // journal registry the budgeted cut left with an ambiguous
                // live registration refuses to open for good (a crash
                // verdict): the matchmaker is the run's one matchmaker loss,
                // down for the run and replaced by a handover, like a wiped
                // one.
                if matches!(failure, paros::StorageError::Corruption { .. })
                    && world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_cut_matchmaker(my_ip)
                {
                    assert_reachable!(
                        "journal store: a cut matchmaker's registry is lost for good"
                    );
                    for checker in &every_checker {
                        stay_down(checker, Down::MatchmakerLost(id.0));
                    }
                    return Ok(());
                }
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
                for checker in &every_checker {
                    stay_down(checker, Down::MatchmakerLost(id.0));
                }
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
