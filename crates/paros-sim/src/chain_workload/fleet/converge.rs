//! Every quiet journal world converges (#447 (model checks for every
//! journal world)): the machines' journals report to their own audit worlds,
//! and the plan's final check judges only the plan's. Once every operator
//! settled, the universe journal, every tenant control journal and every live
//! created journal take no more writes; each a machine still serves must
//! end with every node it does not excuse on its applied frontier, every acked and every decided
//! slot applied. The cell control journal and the election journal are not
//! quiet (the coordinator's liveness writes and renewals go on), so their
//! own oracles judge them (`final_registry`, `election_settles`).
//!
//! A tenant whose matchmaker set lost its majority to wipes owes no slot
//! either (PR #684 (lost set excuse)): every campaign registers at a
//! matchmaker quorum before its first `Prepare`, a wiped matchmaker never
//! rejoins, and the handover that would replace it needs a stop quorum of
//! the same lost generation. No journal of the tenant elects a leader
//! again, so a member that missed a decided slot never learns it. The
//! status check excuses such a journal the same way (`healthy::set_lost`).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_reachable, assert_sometimes};
use paros::{JournalIdentifier, TenantId};

use super::cell::Cell;

/// The journals no writer touches once every operator settled: the formed
/// cell's universe journal, every tenant control journal a machine folded, and
/// every created journal no machine folded a tombstone of.
fn quiet_journals(ctx: &SimContext) -> BTreeSet<JournalIdentifier> {
    let board = crate::audit::tenants::tenant_board(ctx.state());
    let board = crate::audit::tenants::lock(&board);
    let created = board
        .created_modes()
        .into_keys()
        .filter(|journal| !board.tombstoned(*journal));
    let universe = crate::machine::formed_cell(ctx.state()).and_then(|cell| cell.fleet);
    // A dropped tenant's journals owe nothing: every machine that folds the
    // drop stops them, and one that lagged never reports a stop (#529).
    universe
        .into_iter()
        .chain(board.controls())
        .chain(created)
        .filter(|journal| !board.dropped(journal.tenant))
        .collect()
}

/// The tenants of `quiet` whose matchmaker set in force lost its majority
/// to `wiped`, read through `cell` (`None` when the operator never learned
/// the cell: nothing is excused).
async fn lost_tenants(
    cell: Option<&Cell>,
    quiet: &BTreeSet<JournalIdentifier>,
    wiped: &BTreeSet<u64>,
) -> BTreeSet<TenantId> {
    let mut lost = BTreeSet::new();
    let Some(cell) = cell else {
        return lost;
    };
    if wiped.is_empty() {
        return lost;
    }
    let Some(registry) = super::healthy::read(cell, 0).await else {
        return lost;
    };
    for tenant in quiet
        .iter()
        .map(|journal| journal.tenant)
        .collect::<BTreeSet<_>>()
    {
        if super::healthy::set_lost(cell, &registry, tenant, wiped).await {
            lost.insert(tenant);
        }
    }
    lost
}

/// Wait until every quiet journal world converged, or until `deadline`,
/// then judge each with the final convergence claim. A lost cell owes
/// nothing, a tenant whose set is lost owes nothing (see the module doc),
/// and a run shut down by a sibling is not judged.
pub(super) async fn worlds_converge(ctx: &SimContext, cell: Option<&Cell>, deadline: Duration) {
    if crate::machine::formed_cell(ctx.state()).is_none() || crate::machine::cell_lost(ctx.state())
    {
        return;
    }
    let wiped = crate::machine::wiped_ids(ctx.state());
    let board = crate::audit::tenants::tenant_board(ctx.state());
    let mut quiet = quiet_journals(ctx);
    let lost = lost_tenants(cell, &quiet, &wiped).await;
    if !lost.is_empty() {
        assert_reachable!("convergence: a lost matchmaker set excuses a quiet journal");
        quiet.retain(|journal| !lost.contains(&journal.tenant));
    }
    let stops: Vec<_> = {
        let board = crate::audit::tenants::lock(&board);
        quiet
            .into_iter()
            .map(|journal| (journal, board.stopped_on(journal)))
            .collect()
    };
    let mut worlds = Vec::new();
    let mut lost_batches = Vec::new();
    for (journal, stopped) in stops {
        let world = crate::audit::audit_world_for(ctx.state(), journal);
        for node in &wiped {
            world.note_wiped(*node);
        }
        // A machine that stopped the journal (a delete, or its tenant's
        // drop) owes it nothing; a journal no machine serves any more owes
        // no slot (seed 10237668095788132297: a tenant dropped while its
        // created journal's last append was decided and not yet applied).
        for node in stopped {
            world.note_stopped(node);
        }
        if world.served() {
            if crate::audit::tenants::lock(&board).batch_lost(journal) {
                lost_batches.push(Arc::clone(&world));
            }
            worlds.push(world);
        }
    }
    while !worlds.iter().all(|world| world.converged())
        && ctx.time().now() < deadline
        && !ctx.shutdown().is_cancelled()
    {
        if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
            break;
        }
    }
    if ctx.shutdown().is_cancelled() {
        return;
    }
    for world in &worlds {
        world.check_final_convergence(world.answered_max());
    }
    // #599 (durability mode per journal): a `Batched` journal a machine
    // booted with its last batch lost repaired it from its peers and
    // converged with the rest.
    for world in &lost_batches {
        assert_sometimes!(
            world.converged(),
            "journal store: a batched journal converges after it lost its last batch"
        );
    }
    if !worlds.is_empty() {
        assert_reachable!("audit: every quiet journal world converges after chaos");
    }
}
