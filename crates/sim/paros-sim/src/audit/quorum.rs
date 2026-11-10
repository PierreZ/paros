//! The audit's own Phase-2 quorum rule (#909 (oracles that do not judge)).
//!
//! An oracle that asks the core's `has_phase2_quorum` whether a slot was
//! chosen judges `membership/` with `membership/`: a bug there makes the
//! oracle wrong in the same way, and the run stays green. This module
//! writes the rule again from the papers, from the configuration's data
//! only (its sorted members and its quorum system), and shares no code with
//! the core's predicates. Its unit tests check it against every subset of
//! every small configuration.

use std::collections::BTreeSet;

use paros::core::membership::{AcceptorConfig, QuorumSystem};

/// Whether `voters` choose a value under `config`, with no column named:
/// a majority of the members (Lamport), `q2` of them (Flexible Paxos §4,
/// simple quorums), or `acks` of one column of the grid that lays the
/// sorted members out row-major (Compartmentalized Paxos §3.2: member `i`
/// is in column `i % cols`). A voter outside the members never counts.
pub(crate) fn chooses<Id: Copy + Ord>(config: &AcceptorConfig<Id>, voters: &BTreeSet<Id>) -> bool {
    let members = config.members();
    let positions: Vec<usize> = members
        .iter()
        .enumerate()
        .filter(|(_, member)| voters.contains(member))
        .map(|(position, _)| position)
        .collect();
    assert!(
        positions.len() <= members.len(),
        "the votes counted are the members' votes"
    );
    match config.quorum_system() {
        QuorumSystem::Majority => 2 * positions.len() > members.len(),
        QuorumSystem::Flexible { q2, .. } => positions.len() >= q2,
        QuorumSystem::Grid { cols, acks, .. } => (0..cols).any(|column| {
            positions
                .iter()
                .filter(|position| **position % cols == column)
                .count()
                >= acks
        }),
    }
}

/// Whether `slow` marked machines of a pool of `pool` leave a quorum of
/// the pool unmarked: fewer than half of it. Written from the rule, never
/// through the coordinator's own cap (`SlowJudge::cap`).
pub(crate) fn slow_marks_leave_a_quorum(slow: usize, pool: usize) -> bool {
    2 * slow < pool
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use paros::core::membership::{AcceptorConfig, QuorumSystem};

    use super::{chooses, slow_marks_leave_a_quorum};

    /// Every quorum system a configuration of `n` members admits: the
    /// majority, every flexible split and every grid.
    fn systems(n: usize) -> Vec<QuorumSystem> {
        let mut out = vec![QuorumSystem::Majority];
        for q1 in 1..=n {
            for q2 in 1..=n {
                let system = QuorumSystem::Flexible { q1, q2 };
                if system.admits(n) {
                    out.push(system);
                }
            }
        }
        for rows in 1..=n {
            if !n.is_multiple_of(rows) {
                continue;
            }
            for acks in 1..=rows {
                let system = QuorumSystem::Grid {
                    rows,
                    cols: n / rows,
                    acks,
                };
                if system.admits(n) {
                    out.push(system);
                }
            }
        }
        out
    }

    #[test]
    fn the_rule_agrees_with_the_core_on_every_small_subset() {
        for n in 1..=7_u64 {
            // Spread ids, so a position is not an id.
            let members: Vec<u64> = (0..n).map(|i| 10 + 3 * i).collect();
            for system in systems(usize::try_from(n).expect("small")) {
                let config = AcceptorConfig::new(members.clone(), system);
                for mask in 0..(1_u32 << n) {
                    let mut voters: BTreeSet<u64> = members
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| mask & (1 << i) != 0)
                        .map(|(_, m)| *m)
                        .collect();
                    // A stranger's vote never counts.
                    voters.insert(1);
                    assert_eq!(
                        chooses(&config, &voters),
                        config.has_phase2_quorum(&voters),
                        "{system:?} over {members:?}, voters {voters:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_grid_counts_one_column_only() {
        // Two rows, two columns: members 1 and 3 are column 0, 2 and 4
        // column 1. One vote in each column is no quorum of two acks.
        let config = AcceptorConfig::new(
            vec![1_u64, 2, 3, 4],
            QuorumSystem::Grid {
                rows: 2,
                cols: 2,
                acks: 2,
            },
        );
        assert!(!chooses(&config, &BTreeSet::from([1, 2])));
        assert!(chooses(&config, &BTreeSet::from([1, 3])));
        assert!(chooses(&config, &BTreeSet::from([2, 4])));
    }

    #[test]
    fn slow_marks_never_take_half() {
        assert!(slow_marks_leave_a_quorum(0, 1));
        assert!(!slow_marks_leave_a_quorum(1, 2));
        assert!(slow_marks_leave_a_quorum(1, 3));
        assert!(!slow_marks_leave_a_quorum(2, 4));
        assert!(slow_marks_leave_a_quorum(2, 5));
        assert!(!slow_marks_leave_a_quorum(3, 5));
    }
}
