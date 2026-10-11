//! Every quiet journal world converges (#447 (model checks for every
//! journal world)): the machines' journals report to their own audit worlds,
//! and the plan's final check judges only the plan's. Once every operator
//! settled, the universe journal, every tenant control journal and every live
//! created journal take no more writes; each a machine still serves must
//! end with every node it does not excuse on its applied frontier, every acked and every decided
//! slot applied. The cell control journal and the election journal are not
//! quiet (the coordinator's liveness writes and renewals go on), so their
//! own oracles judge them (`final_registry`, `election_settles`).

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_reachable};
use paros::JournalIdentifier;

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
    universe
        .into_iter()
        .chain(board.controls())
        .chain(created)
        .collect()
}

/// Wait until every quiet journal world converged, or until `deadline`,
/// then judge each with the final convergence claim. A lost cell owes
/// nothing, and a run shut down by a sibling is not judged.
pub(super) async fn worlds_converge(ctx: &SimContext, deadline: Duration) {
    if crate::machine::formed_cell(ctx.state()).is_none() || crate::machine::cell_lost(ctx.state())
    {
        return;
    }
    let wiped = crate::machine::wiped_ids(ctx.state());
    let board = crate::audit::tenants::tenant_board(ctx.state());
    let quiet = quiet_journals(ctx);
    let stops: Vec<_> = {
        let board = crate::audit::tenants::lock(&board);
        quiet
            .into_iter()
            .map(|journal| (journal, board.stopped_on(journal)))
            .collect()
    };
    let mut worlds = Vec::new();
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
    if !worlds.is_empty() {
        assert_reachable!("audit: every quiet journal world converges after chaos");
    }
}
