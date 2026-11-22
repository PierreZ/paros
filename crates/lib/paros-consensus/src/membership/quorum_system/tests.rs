use super::QuorumSystem;

/// The well-formedness arm: the layout must tile the membership.
#[test]
fn a_grid_admits_exactly_what_it_tiles() {
    let g = QuorumSystem::grid;
    assert!(g(2, 3).admits(6));
    assert!(g(3, 2).admits(6));
    assert!(g(1, 6).admits(6), "a 1 × n grid tiles n");
    assert!(g(6, 1).admits(6), "n × 1 is its mirror");
    assert!(g(1, 1).admits(1));
    assert!(!g(2, 3).admits(5));
    assert!(!g(2, 3).admits(7));
    assert!(!g(0, 6).admits(6));
    assert!(!g(6, 0).admits(6));
    assert!(!g(usize::MAX, 2).admits(6), "no overflow");
    let acks = |acks| QuorumSystem::Grid {
        rows: 3,
        cols: 2,
        acks,
    };
    assert!(acks(1).admits(6));
    assert!(acks(3).admits(6));
    assert!(!acks(0).admits(6), "a column decides on one accept or more");
    assert!(!acks(4).admits(6), "no more accepts than the column holds");
}

/// Acceptor partitions (#532 (acceptor partitions)): a majority of one
/// partition decides, a majority of every partition elects, and acks
/// from two partitions never add up.
#[test]
fn partitions_decide_on_a_majority_of_one_column() {
    use std::collections::BTreeSet;
    // Two partitions of three: columns {0, 2, 4} and {1, 3, 5}.
    let system = QuorumSystem::partitions(1, 2);
    assert_eq!(
        system,
        QuorumSystem::Grid {
            rows: 3,
            cols: 2,
            acks: 2
        }
    );
    let members: Vec<u8> = (0..6).collect();
    let set = |v: &[u8]| v.iter().copied().collect::<BTreeSet<u8>>();
    assert!(system.admits(6));
    assert_eq!(system.phase2_quorum_size(6), 2);
    assert_eq!(system.phase1_quorum_size(6), 4);
    assert!(system.is_phase2_quorum_in(&members, &set(&[0, 4]), Some(0)));
    assert!(!system.is_phase2_quorum_in(&members, &set(&[0, 1]), Some(0)));
    assert!(
        !system.is_phase2_quorum(&members, &set(&[0, 1])),
        "acks from two partitions never add up"
    );
    assert!(system.is_phase1_quorum(&members, &set(&[0, 2, 1, 5])));
    assert!(
        !system.is_phase1_quorum(&members, &set(&[0, 2, 4, 1])),
        "a Phase-1 quorum holds a majority of every partition"
    );
    assert_eq!(system.row_of(7), None, "a read asks every partition");
    // The full-column grid keeps its row reads, and one acceptor of
    // every column elects.
    let full = QuorumSystem::grid(3, 2);
    assert_eq!(full.row_of(7), Some(1));
    assert!(full.is_phase1_quorum(&members, &set(&[0, 3])));
    assert!(!full.is_phase2_quorum_in(&members, &set(&[0, 2]), Some(0)));
}

/// A majority admits every non-empty membership.
#[test]
fn a_majority_admits_every_nonempty_membership() {
    for n in 1..=7 {
        assert!(QuorumSystem::Majority.admits(n));
        assert!(QuorumSystem::Majority.cross_intersects(n));
    }
    assert!(!QuorumSystem::Majority.admits(0));
}

/// The failures a system survives with both phases still possible (PR
/// #1064 (gone-node gap)): a majority survives a minority, and the
/// acceptor partitions survive `F`.
#[test]
fn a_system_survives_the_failures_both_phases_allow() {
    assert_eq!(QuorumSystem::Majority.tolerated_failures(3), 1);
    assert_eq!(QuorumSystem::Majority.tolerated_failures(4), 1);
    assert_eq!(QuorumSystem::Majority.tolerated_failures(5), 2);
    for f in 0..=3 {
        for p in 1..=3 {
            let system = QuorumSystem::partitions(f, p);
            let n = (2 * f + 1) * p;
            assert_eq!(system.tolerated_failures(n), f, "F={f} P={p}");
        }
    }
    assert_eq!(
        QuorumSystem::grid(2, 2).tolerated_failures(4),
        1,
        "a full-column grid survives one failure"
    );
    assert_eq!(QuorumSystem::grid(2, 2).tolerated_failures(5), 0, "untiled");
}

/// A grid whose column needs every accept survives no failure: a lost
/// acceptor leaves its column one accept short (`rows - acks + 1` is one).
#[test]
fn a_grid_that_waits_for_a_whole_column_survives_nothing() {
    let all_acks = QuorumSystem::Grid {
        rows: 3,
        cols: 1,
        acks: 3,
    };
    assert_eq!(all_acks.tolerated_failures(3), 0);
    let two_columns = QuorumSystem::Grid {
        rows: 3,
        cols: 2,
        acks: 3,
    };
    assert_eq!(
        two_columns.tolerated_failures(6),
        1,
        "Phase 2 survives one lost acceptor while the other column stays whole"
    );
}

/// A grid cross-intersects only while a column decides on one accept or
/// more and on no more accepts than it holds.
#[test]
fn a_grid_with_acks_out_of_its_column_does_not_cross_intersect() {
    let acks = |acks| QuorumSystem::Grid {
        rows: 3,
        cols: 2,
        acks,
    };
    assert!(acks(1).cross_intersects(6));
    assert!(acks(3).cross_intersects(6));
    assert!(!acks(0).cross_intersects(6));
    assert!(!acks(4).cross_intersects(6));
}

/// A column past the last one holds no Phase-2 quorum, even when the
/// members it would step over voted.
#[test]
fn a_column_out_of_the_grid_holds_no_phase2_quorum() {
    use std::collections::BTreeSet;
    let system = QuorumSystem::Grid {
        rows: 2,
        cols: 3,
        acks: 1,
    };
    let members: Vec<u8> = (0..6).collect();
    let voters: BTreeSet<u8> = [3].into_iter().collect();
    assert!(system.is_phase2_quorum_in(&members, &voters, Some(0)));
    assert!(!system.is_phase2_quorum_in(&members, &voters, Some(3)));
}

/// The seal of decision 31 (#1209): on a full-column grid a Phase-1 quorum
/// and one voter in each row; elsewhere the Phase-1 quorum alone.
#[test]
fn a_seal_asks_one_voter_in_each_row_of_a_full_grid_only() {
    use std::collections::BTreeSet;
    let set = |v: &[u8]| v.iter().copied().collect::<BTreeSet<u8>>();
    // A 3 x 2 full grid: rows {0, 1}, {2, 3}, {4, 5}; columns {0, 2, 4}
    // and {1, 3, 5}.
    let members: Vec<u8> = (0..6).collect();
    let full = QuorumSystem::grid(3, 2);
    assert!(full.is_phase1_quorum(&members, &set(&[2, 3])));
    assert!(
        !full.is_phase1_seal(&members, &set(&[2, 3])),
        "one row is a Phase-1 quorum, not a seal"
    );
    assert!(
        !full.is_phase1_seal(&members, &set(&[0, 2, 3])),
        "the row {{4, 5}} heard nothing"
    );
    assert!(full.is_phase1_seal(&members, &set(&[0, 3, 4])));
    assert!(
        !full.is_phase1_seal(&members, &set(&[0, 2, 4])),
        "one voter in each row is no seal without a Phase-1 quorum"
    );
    assert!(
        !full.is_phase1_seal(&members, &set(&[0, 7, 9])),
        "a stranger seals no row"
    );
    // A partitioned grid and a majority read the whole membership: the
    // seal is the Phase-1 quorum.
    let partitions = QuorumSystem::partitions(1, 2);
    for voters in [set(&[0, 2, 1, 3]), set(&[0, 2, 4, 1]), set(&[0, 1])] {
        assert_eq!(
            partitions.is_phase1_seal(&members, &voters),
            partitions.is_phase1_quorum(&members, &voters)
        );
    }
    let majority = QuorumSystem::Majority;
    for voters in [set(&[0, 1, 2]), set(&[0, 1, 2, 3]), set(&[5])] {
        assert_eq!(
            majority.is_phase1_seal(&members, &voters),
            majority.is_phase1_quorum(&members, &voters)
        );
    }
}
