//! Every quiet journal world converges (#447 (model checks for every
//! journal world)): the machines' journals report to their own audit worlds,
//! and the plan's final check judges only the plan's. Once every operator
//! settled, the universe journal, every tenant control journal and every live
//! created journal take no more writes; each a machine still serves must
//! end with every node not down for good on its applied frontier, every acked and every decided
//! slot applied. The cell control journal and the election journal are not
//! quiet (the coordinator's liveness writes and renewals go on), so their
//! own oracles judge them (`final_registry`, `election_settles`).
//!
//! A tenant whose matchmaker set lost its majority to wipes is under the
//! one verdict from ground truth (`crate::tail::judge_lost`), which judges
//! it: every campaign registers at a matchmaker quorum before its first
//! `Prepare`, a wiped matchmaker never rejoins, and the handover that would
//! replace it needs a stop quorum of the same lost generation, so no
//! journal of the tenant elects a leader again. So is a lost cell. A
//! journal that does not converge has one typed cause
//! (`AuditWorld::check_final_convergence`).
//!
//! A machine that stopped a journal for good owes it nothing: it folded
//! the journal's delete, its tenant's drop, or the release of its booking
//! (`journal_released`: the cell coordinator re-placed its slot while it
//! was down). A stop may come in the tail, after the wait began: a machine
//! that reboots there folds the registry and leaves the journal its slot
//! moved off (recipe 9431890306931267739 590039:9389328808641023734 on
//! 936348a: an initial member crashed at 84 s, its slot re-placed, rebooted at
//! 100 s and stopped the journal with no slot applied). So the wait reads
//! the stops again on every pass and once more before it judges.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_reachable};
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;

use crate::audit::AuditWorld;
use crate::audit::tenants::TenantBoard;

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
    // drop stops them, and one that lagged never reports a stop (#529). A
    // machine folds the drop as an entry or inside a registry checkpoint
    // it restored from (`Audit::checkpoint_folded` feeds the board too).
    universe
        .into_iter()
        .chain(board.controls())
        .chain(created)
        .filter(|journal| !board.dropped(journal.tenant))
        .collect()
}

/// The tenants of every quiet journal: the tenants whose matchmaker sets
/// the one verdict judges (`crate::tail::judge_lost`).
pub(super) fn quiet_tenants(ctx: &SimContext) -> BTreeSet<TenantId> {
    quiet_journals(ctx)
        .iter()
        .map(|journal| journal.tenant)
        .collect()
}

/// Take out of `world`'s convergence wait every machine the tenant board
/// holds as having stopped `journal` for good, as reported so far.
pub(super) fn note_stops(
    board: &Mutex<TenantBoard>,
    journal: JournalIdentifier,
    world: &AuditWorld,
) {
    let stopped = crate::audit::tenants::lock(board).stopped_on(journal);
    for node in stopped {
        world.note_stopped(node);
    }
}

/// Wait until every quiet journal world converged, or until `deadline`,
/// then judge each with the final convergence claim. A run where no cell
/// formed has no quiet journal: `final_fleet` judges the init. A lost cell
/// and a tenant whose set is lost are the one verdict's (see the module
/// doc).
pub(super) async fn worlds_converge(ctx: &SimContext, deadline: Duration) {
    if crate::machine::formed_cell(ctx.state()).is_none() || crate::tail::cell_lost(ctx.state()) {
        return;
    }
    let wiped = crate::machine::wiped_ids(ctx.state());
    let removed = crate::machine::removed_ids(ctx.state());
    let board = crate::audit::tenants::tenant_board(ctx.state());
    let mut quiet = quiet_journals(ctx);
    let tenants: BTreeSet<TenantId> = quiet.iter().map(|journal| journal.tenant).collect();
    let lost = crate::tail::lost_sets(ctx.state(), &tenants);
    quiet.retain(|journal| !lost.contains(&journal.tenant));
    let mut worlds = Vec::new();
    for journal in quiet {
        let world = crate::audit::audit_world_for(ctx.state(), journal);
        for node in &wiped {
            world.note_wiped(*node);
        }
        // A removed machine left the cell, and its operator may have shut
        // it down for good (#858 (boot from the cached registry)): it owes
        // no journal anything.
        for node in &removed {
            world.note_stopped(*node);
        }
        // A machine that stopped the journal (a delete, its tenant's drop,
        // or its booking released) owes it nothing; a journal no machine
        // serves any more owes no slot (seed 10237668095788132297: a tenant
        // dropped while its created journal's last append was decided and
        // not yet applied).
        note_stops(&board, journal, &world);
        if world.served() {
            worlds.push((journal, world));
        }
    }
    // A stop reported in the tail takes its node out of the wait too (the
    // module doc).
    let refresh = |worlds: &[(JournalIdentifier, Arc<AuditWorld>)]| {
        for (journal, world) in worlds {
            note_stops(&board, *journal, world);
        }
    };
    while !worlds.iter().all(|(_, world)| world.converged())
        && ctx.time().now() < deadline
        && !ctx.shutdown().is_cancelled()
    {
        if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
            break;
        }
        refresh(&worlds);
    }
    if crate::tail::cut_short(ctx, "converge") {
        return;
    }
    refresh(&worlds);
    for (_, world) in &worlds {
        world.check_final_convergence(world.answered_max());
    }
    if !worlds.is_empty() {
        assert_reachable!("audit: every quiet journal world converges after chaos");
    }
}
