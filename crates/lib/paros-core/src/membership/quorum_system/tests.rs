use super::QuorumSystem;

/// The well-formedness arm: the layout must tile the membership.
#[test]
fn a_grid_admits_exactly_what_it_tiles() {
    let g = QuorumSystem::grid;
    assert!(g(2, 3).admits(6));
    assert!(g(3, 2).admits(6));
    assert!(g(1, 6).admits(6), "1 × n is Flexible's |Q1| = 1 experiment");
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

/// The well-formedness arm, spelled out: `1 <= q1 <= n`, `1 <= q2 <= n`,
/// `q1 + q2 > n`. A majority admits every non-empty membership.
#[test]
fn admits_is_the_flexible_arm() {
    for n in 1..=7 {
        assert!(QuorumSystem::Majority.admits(n));
        assert!(QuorumSystem::Majority.cross_intersects(n));
    }
    assert!(!QuorumSystem::Majority.admits(0));
    let f = |q1, q2| QuorumSystem::Flexible { q1, q2 };
    assert!(f(3, 2).admits(4));
    assert!(f(2, 3).admits(4));
    assert!(f(4, 1).admits(4));
    assert!(f(1, 4).admits(4));
    assert!(f(3, 3).admits(4));
    assert!(!f(2, 2).admits(4), "q1 + q2 = n does not intersect");
    assert!(!f(0, 4).admits(4), "an empty Phase-1 quorum");
    assert!(!f(4, 0).admits(4), "an empty Phase-2 quorum");
    assert!(!f(5, 1).admits(4), "a quorum larger than the membership");
    assert!(!f(usize::MAX, usize::MAX).admits(4), "no overflow");
    assert!(!f(1, 1).admits(0));
}
