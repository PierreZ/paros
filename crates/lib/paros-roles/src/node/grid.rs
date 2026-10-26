//! Where a round goes on a grid: a stuck round moved to another column (#533), the Phase 2 column (#141) and the fast row and column around slow members (#361).

use std::collections::BTreeSet;

use paros_core::{ColocatedNode, NodeId, QuorumSystem};

/// The most doublings of a moved round's budget (#533 (move a stuck
/// round)): a round that moved `m` times waits
/// `after * 2^min(m, MOVE_BACKOFF_CAP)` re-sends before its next move. The
/// cap keeps the wait finite, so a round in a column that stays stuck
/// still moves. At the cap, production's budget (20 beats) becomes 1,280
/// beats. Not a tunable: the liveness argument of [`move_budget`] rests on
/// it.
pub(crate) const MOVE_BACKOFF_CAP: u64 = 6;
const _: () = assert!(
    MOVE_BACKOFF_CAP >= 1,
    "a moved round waits longer than a new one"
);
const _: () = assert!(
    MOVE_BACKOFF_CAP < 63,
    "the back-off shift stays inside a u64"
);

/// The re-sends a round that moved `moves` times waits before its next
/// move: `after` for a round that never moved, doubled for each move up to
/// [`MOVE_BACKOFF_CAP`] doublings, saturating.
///
/// Why: a move resets the round's tally, so the new column must collect
/// all its acks inside the budget. Under load the `Accept` flood makes the
/// round trip longer than a budget of one or two re-sends. Then the round
/// flips between columns without end, and no column decides it. Red on
/// main (CI run 38823114601, `replay-recipe 7816651028622460903
/// 216:14501162423016305769` at 629aa62): a `Grid { rows: 3, cols: 2 }`
/// leader with a budget of 1 to 3 moved its rounds 9,844 times, and the
/// apply walk stalled at slot 14 of 19 decided. With the doubling the same
/// recipe is green: each new column gets a longer window, so after a few
/// moves the round trip fits.
pub(crate) fn move_budget(after: u64, moves: u64) -> u64 {
    let shift = moves.min(MOVE_BACKOFF_CAP);
    assert!(shift <= MOVE_BACKOFF_CAP, "the back-off is capped");
    let budget = after.saturating_mul(1_u64 << shift);
    assert!(
        budget >= after,
        "a moved round never waits less than a new one"
    );
    budget
}

/// Move every colocated grid round re-sent at least its budget without a
/// decision to another column (#533 (move a stuck round)): the next column
/// round-robin, or on a BUGGIFY decision the one after it, so a slot may
/// skip a column. The budget is `after` for a round that never moved, and
/// it doubles with each move ([`move_budget`]). A no-op on a node that is
/// not the leader and under a majority or a flexible split. Drawn on the
/// node loop.
pub(crate) fn move_stuck_rounds(node: &mut ColocatedNode, after: u64, self_id: u64) {
    let QuorumSystem::Grid { cols, .. } = node.acceptors().quorum_system() else {
        return;
    };
    if cols < 2 || !node.is_leader() {
        return;
    }
    let stuck: Vec<(paros_core::Slot, usize)> = node
        .proposer()
        .rounds()
        .iter()
        .filter(|(_, round)| round.proxy().is_none() && round.resends() >= after)
        .filter(|(_, round)| {
            let due = round.resends() >= move_budget(after, round.moves());
            if !due {
                // Only a moved round has a budget above `after`.
                assert!(
                    round.moves() > 0,
                    "only a moved round waits past the budget"
                );
                moonpool_assertions::reachable!("grid: the driver backs off a moved round");
            }
            due
        })
        .filter_map(|(slot, round)| round.column().map(|column| (*slot, column)))
        .collect();
    for (slot, column) in stuck {
        let step = if cols >= 3 && moonpool_buggify::buggify_fault_with_prob!(0.5) {
            moonpool_assertions::reachable!(
                "grid: the driver skips a column when it moves a round"
            );
            2
        } else {
            1
        };
        let next = (column + step) % cols;
        assert!(next != column, "a stuck round moves to another column");
        if node.move_round(slot, next) {
            moonpool_assertions::reachable!("grid: the driver moves a stuck round");
            tracing::info!(
                node = self_id,
                slot = slot.0,
                from = column,
                to = next,
                "round_moved"
            );
        }
    }
}

/// The column of a grid this proposal's Phase 2 goes to instead of the
/// core's `slot % cols` (#141): `(slot + 1) % cols`, the next slot's column,
/// so consecutive slots land on one column and a handoff successor's
/// re-proposal lands on another. Every column is a Phase-2 quorum, so the
/// choice is always valid. Its own location; silent in the recovery tail.
pub(crate) fn phase2_column(slot: paros_core::Slot, cols: usize) -> Option<usize> {
    if cols < 2 || !moonpool_buggify::buggify_fault_with_prob!(0.10) {
        return None;
    }
    // BUGGIFY pairing: the override genuinely fires.
    moonpool_assertions::reachable!("grid: the driver overrides a round's column");
    let modulus = u64::try_from(cols).unwrap_or(u64::MAX);
    let column = usize::try_from(slot.0.wrapping_add(1) % modulus).unwrap_or(0);
    assert!(column < cols, "an overridden column is one the grid has");
    Some(column)
}

/// The members of `node`'s configuration the cell holds slow (#361 (slow
/// machines)), from the registry fold: empty outside a grid, where no
/// column or row choice reads them, and without a control plan.
pub(crate) fn slow_members(
    node: &ColocatedNode,
    registry: Option<&paros_model::registry::Registry>,
) -> BTreeSet<NodeId> {
    let (QuorumSystem::Grid { .. }, Some(registry)) = (node.acceptors().quorum_system(), registry)
    else {
        return BTreeSet::new();
    };
    let slow: BTreeSet<NodeId> = node
        .acceptors()
        .members()
        .iter()
        .copied()
        .filter(|id| registry.is_slow(*id))
        .collect();
    assert!(
        slow.iter().all(|id| node.acceptors().contains(*id)),
        "a slow member is a member"
    );
    slow
}

/// The column of a grid this leader's Phase 2 goes to when the core's own
/// column holds an acceptor the cell holds slow (#361 (slow machines)) and
/// another column holds none: the first such column after the core's. Every
/// column is a Phase-2 quorum, so the choice is always valid; it only
/// prefers fast acceptors. `None` keeps the core's choice.
pub(crate) fn fast_column(
    config: &paros_core::AcceptorConfig,
    slot: paros_core::Slot,
    is_slow: impl Fn(NodeId) -> bool,
) -> Option<usize> {
    let QuorumSystem::Grid { cols, .. } = config.quorum_system() else {
        return None;
    };
    let own = config.column_of(slot)?;
    let holds_slow = |column: usize| {
        config
            .phase2_addressees(Some(column))
            .into_iter()
            .any(&is_slow)
    };
    if !holds_slow(own) {
        return None;
    }
    let column = (1..cols)
        .map(|step| (own + step) % cols)
        .find(|column| !holds_slow(*column))?;
    assert!(column != own, "a fast column is another column");
    assert!(column < cols, "a fast column is one the grid has");
    moonpool_assertions::reachable!("grid: a round's column avoids a slow acceptor");
    Some(column)
}

/// The row of a grid a quorum read asks when the core's own row holds an
/// acceptor the cell holds slow (#361 (slow machines)) and another row
/// holds none: the twin of [`fast_column`], under the same condition as the
/// row override (a full column, so every row is a read quorum).
pub(crate) fn fast_row(
    config: &paros_core::AcceptorConfig,
    ctx: u64,
    slow: impl Fn(NodeId) -> bool,
) -> Option<usize> {
    let QuorumSystem::Grid { rows, acks, .. } = config.quorum_system() else {
        return None;
    };
    if rows < 2 || acks != rows {
        return None;
    }
    let own = config.row_of(ctx)?;
    let holds_slow = |row: usize| config.phase1_addressees(Some(row)).into_iter().any(&slow);
    if !holds_slow(own) {
        return None;
    }
    let row = (1..rows)
        .map(|step| (own + step) % rows)
        .find(|row| !holds_slow(*row))?;
    assert!(row != own, "a fast row is another row");
    assert!(
        config.admits_read_row(Some(row)),
        "a fast row is a read row"
    );
    moonpool_assertions::reachable!("grid: a quorum read's row avoids a slow acceptor");
    Some(row)
}
