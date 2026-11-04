//! The fast column and the fast row around slow members (#361 (slow
//! machines)).

use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_core::{NodeId, Slot};

use super::{fast_column, fast_row, move_budget};

/// A grid of `rows` by `cols` over nodes `1..=rows*cols`, every column a
/// full Phase-2 quorum.
fn grid(rows: usize, cols: usize) -> AcceptorConfig {
    let count = u64::try_from(rows * cols).expect("a small grid");
    AcceptorConfig::new(
        (1..=count).map(NodeId).collect(),
        QuorumSystem::Grid {
            rows,
            cols,
            acks: rows,
        },
    )
}

#[test]
fn a_round_whose_column_holds_a_slow_member_moves_to_the_first_fast_one() {
    let config = grid(2, 3);
    let slot = Slot(4);
    let own = config.column_of(slot).expect("a grid slot has a column");
    let in_column = |column: usize| config.phase2_addressees(Some(column));
    let slow_one = in_column(own)[0];
    let column = fast_column(&config, slot, |id| id == slow_one).expect("a fast column");
    assert_eq!(column, (own + 1) % 3, "the first column after its own");
    assert!(!in_column(column).contains(&slow_one));
    // The next column is slow too: the one after it.
    let next_slow = in_column((own + 1) % 3)[0];
    assert_eq!(
        fast_column(&config, slot, |id| id == slow_one || id == next_slow),
        Some((own + 2) % 3)
    );
    // Its own column is fast, or every column is slow: the core's choice.
    let other = in_column((own + 1) % 3)[0];
    assert_eq!(fast_column(&config, slot, |id| id == other), None);
    assert_eq!(fast_column(&config, slot, |_| true), None);
}

#[test]
fn a_majority_has_no_column_and_no_row() {
    let config = AcceptorConfig::new(
        vec![NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::Majority,
    );
    assert_eq!(fast_column(&config, Slot(1), |_| true), None);
    assert_eq!(fast_row(&config, 1, |_| true), None);
}

#[test]
fn a_read_whose_row_holds_a_slow_member_asks_the_first_fast_row() {
    let config = grid(3, 2);
    let ctx = 5;
    let own = config.row_of(ctx).expect("a full-column grid has rows");
    let in_row = |row: usize| config.phase1_addressees(Some(row));
    let slow_one = in_row(own)[0];
    assert_eq!(
        fast_row(&config, ctx, |id| id == slow_one),
        Some((own + 1) % 3)
    );
    let other = in_row((own + 1) % 3)[0];
    assert_eq!(
        fast_row(&config, ctx, |id| id == other),
        None,
        "its row is fast"
    );
    assert_eq!(fast_row(&config, ctx, |_| true), None, "every row is slow");
}

#[test]
fn a_grid_whose_rows_are_no_read_quorum_keeps_the_cores_row() {
    // One accept of two chooses: a row is no read quorum.
    let config = AcceptorConfig::new(
        (1..=4).map(NodeId).collect(),
        QuorumSystem::Grid {
            rows: 2,
            cols: 2,
            acks: 1,
        },
    );
    assert_eq!(fast_row(&config, 3, |_| true), None);
    // One row: nowhere else to go.
    assert_eq!(fast_row(&grid(1, 3), 3, |_| true), None);
}

#[test]
fn a_moved_rounds_budget_doubles_up_to_the_cap() {
    assert_eq!(move_budget(20, 0), 20);
    assert_eq!(move_budget(20, 1), 40);
    assert_eq!(move_budget(20, 6), 1_280);
    assert_eq!(move_budget(20, 60), 1_280, "capped");
    assert_eq!(move_budget(u64::MAX, 3), u64::MAX, "saturating");
}
