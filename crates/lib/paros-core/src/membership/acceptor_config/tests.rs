use std::collections::BTreeSet;

use super::{AcceptorConfig, QuorumSystem};
use crate::types::{NodeId, Slot};

fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
    ids.into_iter().map(NodeId).collect()
}

fn voters(ids: impl IntoIterator<Item = u64>) -> BTreeSet<NodeId> {
    ids.into_iter().map(NodeId).collect()
}

/// acceptors the two phase predicates differ for the first time — two
/// accepts choose, two promises do not elect — while a majority would
/// have taken three of either.
#[test]
fn flexible_predicates_differ_by_phase() {
    let flexible = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 3, q2: 2 });
    let majority = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Majority);
    let two = voters([1, 2]);
    let three = voters([1, 2, 3]);
    assert!(flexible.has_phase2_quorum(&two));
    assert!(!flexible.has_phase1_quorum(&two));
    assert!(flexible.has_phase1_quorum(&three));
    assert!(!majority.has_phase2_quorum(&two));
    assert!(!majority.has_phase1_quorum(&two));
    assert!(majority.has_phase1_quorum(&three));
    assert!(majority.has_phase2_quorum(&three));
    // Two Phase-2 quorums need not intersect under the flexible split;
    // every Phase-1 quorum still meets every Phase-2 quorum.
    assert!(flexible.has_phase2_quorum(&voters([3, 4])));
    assert_eq!(flexible.quorum_system().phase1_quorum_size(4), 3);
    assert_eq!(flexible.quorum_system().phase2_quorum_size(4), 2);
    assert_eq!(flexible.phase2_addressees(None), flexible.members());
    assert_eq!(flexible.column_of(Slot(7)), None);
}

/// The `2 × 3` grid of Compartmentalized Paxos §3.2 over six acceptors:
/// row `{1, 2, 3}` and `{4, 5, 6}`, columns `{1, 4}`, `{2, 5}`, `{3, 6}`.
fn grid() -> AcceptorConfig {
    AcceptorConfig::new(nodes(1..=6), QuorumSystem::grid(2, 3))
}

/// The point of the variant: a full row is a Phase-1 quorum and a full
/// column a Phase-2 quorum — set membership, not a count.
#[test]
fn a_row_elects_and_a_column_decides() {
    let grid = grid();
    assert!(grid.has_phase1_quorum(&voters([1, 2, 3])));
    assert!(grid.has_phase1_quorum(&voters([4, 5, 6])));
    assert!(grid.has_phase1_quorum(&voters([4, 5, 6, 1])));
    assert!(grid.has_phase2_quorum(&voters([1, 4])));
    assert!(grid.has_phase2_quorum(&voters([3, 6])));
    assert!(grid.has_phase2_quorum_in(&voters([2, 5]), Some(1)));
    assert_eq!(grid.quorum_system().phase1_quorum_size(6), 3);
    assert_eq!(grid.quorum_system().phase2_quorum_size(6), 2);
    assert!(grid.quorum_system().cross_intersects(6));
}

/// The negative space: a row is not a Phase-2 quorum, a column not a
/// Phase-1 quorum, and a majority-sized set that is neither is nothing.
#[test]
fn a_row_does_not_decide_and_a_column_does_not_elect() {
    let grid = grid();
    assert!(!grid.has_phase2_quorum(&voters([1, 2, 3])));
    assert!(!grid.has_phase1_quorum(&voters([1, 4])));
    // Four of six, a majority anywhere else, but no full row.
    assert!(!grid.has_phase1_quorum(&voters([1, 2, 4, 5])));
    // Three of six with a member of every column and no full column.
    assert!(!grid.has_phase2_quorum(&voters([1, 2, 6])));
    // Any four of six do contain a full column (a two-node complement
    // can break at most two of three columns): the grid's Phase-2
    // quorums are small, and that is the point.
    assert!(grid.has_phase2_quorum(&voters([1, 2, 5, 6])));
    // A full column that is not the round's column does not decide the
    // round; the round's own column does.
    assert!(!grid.has_phase2_quorum_in(&voters([1, 4]), Some(1)));
    assert!(grid.has_phase2_quorum_in(&voters([1, 4]), Some(0)));
    // A column index past the grid is no column.
    assert!(!grid.has_phase2_quorum_in(&voters([1, 2, 3, 4, 5, 6]), Some(3)));
    // Strangers never count.
    assert!(!grid.has_phase1_quorum(&voters([1, 2, 9])));
}

/// Column addressing: `slot % cols` is the column, the addressees are
/// exactly that column in membership order, and an acceptor outside it
/// is not an addressee even though it is a member.
#[test]
fn a_slot_is_addressed_to_its_column() {
    let grid = grid();
    assert_eq!(grid.column_of(Slot(0)), Some(0));
    assert_eq!(grid.column_of(Slot(1)), Some(1));
    assert_eq!(grid.column_of(Slot(2)), Some(2));
    assert_eq!(grid.column_of(Slot(3)), Some(0));
    assert_eq!(grid.column_of(Slot(u64::MAX)), Some(0));
    assert_eq!(grid.phase2_addressees(Some(0)), nodes([1, 4]));
    assert_eq!(grid.phase2_addressees(Some(2)), nodes([3, 6]));
    assert_eq!(grid.phase2_addressees(None), nodes(1..=6));
    assert!(grid.is_phase2_addressee(NodeId(4), Some(0)));
    assert!(!grid.is_phase2_addressee(NodeId(5), Some(0)));
    assert!(grid.is_phase2_addressee(NodeId(5), None));
    assert!(!grid.is_phase2_addressee(NodeId(9), None));
    let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
    assert!(majority.is_phase2_addressee(NodeId(2), None));
    assert!(!majority.is_phase2_addressee(NodeId(9), None));
}

/// The read side of the grid (#143): a row is addressed by `ctx % rows`,
/// judged by set membership, and a row that is *not* a column still
/// completes a Phase-1 quorum — while a column never does.
#[test]
fn a_grid_row_is_addressed_by_the_read_token_and_judged_as_a_row() {
    let grid = QuorumSystem::grid(2, 3);
    let config = AcceptorConfig::new(nodes(1..=6), grid);
    assert_eq!(config.row_of(0), Some(0));
    assert_eq!(config.row_of(1), Some(1));
    assert_eq!(config.row_of(7), Some(1));
    assert_eq!(config.phase1_addressees(Some(0)), nodes([1, 2, 3]));
    assert_eq!(config.phase1_addressees(Some(1)), nodes([4, 5, 6]));
    assert_eq!(config.phase1_addressees(None), nodes(1..=6));
    assert!(config.is_phase1_addressee(NodeId(5), Some(1)));
    assert!(!config.is_phase1_addressee(NodeId(5), Some(0)));
    assert!(config.is_phase1_addressee(NodeId(5), None));
    assert!(!config.is_phase1_addressee(NodeId(9), None));
    // Row 1 is whole: a Phase-1 quorum in row 1, not in row 0.
    assert!(config.has_phase1_quorum_in(&voters([4, 5, 6]), Some(1)));
    assert!(!config.has_phase1_quorum_in(&voters([4, 5, 6]), Some(0)));
    assert!(config.has_phase1_quorum_in(&voters([4, 5, 6]), None));
    // Negative space: a full column is not a row, and a row is not a
    // column — the two phases' quorums differ by shape, not by count.
    assert!(!config.has_phase1_quorum_in(&voters([1, 4]), Some(0)));
    assert!(!config.has_phase1_quorum_in(&voters([1, 4]), None));
    assert!(!config.has_phase2_quorum(&voters([4, 5, 6])));
    // A row index past the grid is never a quorum.
    assert!(!config.has_phase1_quorum_in(&voters(1..=6), Some(2)));
    // A majority names no row: the whole membership is the row.
    let majority = AcceptorConfig::new(nodes(0..3), QuorumSystem::Majority);
    assert_eq!(majority.row_of(5), None);
    assert_eq!(majority.phase1_addressees(None), nodes(0..3));
    assert!(majority.has_phase1_quorum_in(&voters([0, 1]), None));
}

#[test]
#[should_panic(expected = "only a grid names a row")]
fn naming_a_row_under_a_majority_is_a_programmer_error() {
    let majority = AcceptorConfig::new(nodes(0..3), QuorumSystem::Majority);
    let _ = majority.has_phase1_quorum_in(&voters([0, 1]), Some(0));
}

#[test]
#[should_panic(expected = "an acceptor configuration admits its quorum system")]
fn a_grid_that_does_not_tile_is_unconstructible() {
    let _ = AcceptorConfig::new(nodes(1..=5), QuorumSystem::grid(2, 3));
}

/// A column is a grid notion: naming one under a majority is a
/// programmer error, asserted.
#[test]
#[should_panic(expected = "only a grid names a column for a Phase-2 quorum")]
fn a_majority_refuses_a_column() {
    let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
    let _ = majority.has_phase2_quorum_in(&voters([1, 2]), Some(0));
}

/// A voter outside the membership never counts, under either system.
#[test]
fn strangers_never_count() {
    let flexible = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 3, q2: 2 });
    assert!(!flexible.has_phase2_quorum(&voters([1, 9])));
    assert!(!flexible.has_phase1_quorum(&voters([1, 2, 9])));
    let majority = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Majority);
    assert!(!majority.has_phase2_quorum(&voters([1, 9])));
}

/// The constructor is the only site, and it refuses a split whose
/// phases do not cross-intersect.
#[test]
#[should_panic(expected = "an acceptor configuration admits its quorum system")]
fn flexible_without_cross_intersection_is_unconstructible() {
    let _ = AcceptorConfig::new(nodes(1..=4), QuorumSystem::Flexible { q1: 2, q2: 2 });
}

#[test]
#[should_panic(expected = "an acceptor configuration admits its quorum system")]
fn flexible_larger_than_the_membership_is_unconstructible() {
    let _ = AcceptorConfig::new(nodes(1..=3), QuorumSystem::Flexible { q1: 4, q2: 1 });
}

/// Normalization happens before the arm: duplicates collapse, so a
/// split that admits the *deduplicated* size is what counts.
#[test]
fn well_formedness_is_judged_over_the_deduplicated_membership() {
    let config =
        AcceptorConfig::new(nodes([2, 1, 2, 1]), QuorumSystem::Flexible { q1: 2, q2: 1 });
    assert_eq!(config.members(), nodes([1, 2]));
    assert!(config.is_well_formed());
}
