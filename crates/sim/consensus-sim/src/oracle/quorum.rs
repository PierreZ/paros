//! The Phase-2 quorum rule, written again from the papers.
//!
//! An oracle must not share code with the code it judges (#909 (oracles
//! that do not judge)). This module answers "do these voters choose a
//! value?" for the two quorum systems of paros-consensus without a call to
//! `membership/`:
//!
//! - **Majority** (Lamport, *Paxos Made Simple*): more than half of the
//!   members.
//! - **Grid** (Compartmentalized Paxos §3.2; Flexible Paxos §4): the sorted
//!   members laid out row-major in `rows × cols`, member `i` at
//!   `(i / cols, i % cols)`. A Phase-2 quorum is `acks` members of one
//!   column. Votes of two columns never add up.

use std::collections::BTreeSet;

use paros_consensus::NodeId;
use paros_consensus::membership::QuorumSystem;

/// The acceptors of one static configuration and the rule that says when
/// their votes choose a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase2Rule {
    members: Vec<NodeId>,
    rule: Rule,
}

/// The two rules, as plain numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    Majority,
    Grid { cols: usize, acks: usize },
}

impl Phase2Rule {
    /// The rule of `members` under `system`.
    ///
    /// # Panics
    ///
    /// If `members` is empty, or a grid does not lay the members out in
    /// full rows: the walk builds no such configuration.
    #[must_use]
    pub fn new(members: &[NodeId], system: QuorumSystem) -> Self {
        assert!(!members.is_empty(), "a configuration names an acceptor");
        let mut members = members.to_vec();
        members.sort_unstable();
        members.dedup();
        let rule = match system {
            QuorumSystem::Majority => Rule::Majority,
            QuorumSystem::Grid { rows, cols, acks } => {
                assert!(rows * cols == members.len(), "a grid has full rows");
                assert!((1..=rows).contains(&acks), "a column can decide");
                Rule::Grid { cols, acks }
            }
        };
        Self { members, rule }
    }

    /// Whether the votes of `voters` choose a value. A voter that is not a
    /// member counts for nothing.
    #[must_use]
    pub fn chooses(&self, voters: &BTreeSet<NodeId>) -> bool {
        let positions: Vec<usize> = self
            .members
            .iter()
            .enumerate()
            .filter(|(_, member)| voters.contains(member))
            .map(|(position, _)| position)
            .collect();
        match self.rule {
            Rule::Majority => positions.len() * 2 > self.members.len(),
            Rule::Grid { cols, acks } => {
                (0..cols).any(|col| positions.iter().filter(|p| *p % cols == col).count() >= acks)
            }
        }
    }

    /// The members, sorted.
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(ids: &[u64]) -> BTreeSet<NodeId> {
        ids.iter().copied().map(NodeId).collect()
    }

    fn members(n: u64) -> Vec<NodeId> {
        (0..n).map(NodeId).collect()
    }

    #[test]
    fn a_majority_is_more_than_half() {
        let rule = Phase2Rule::new(&members(3), QuorumSystem::Majority);
        assert!(!rule.chooses(&ids(&[0])));
        assert!(rule.chooses(&ids(&[0, 2])));
        let rule = Phase2Rule::new(&members(4), QuorumSystem::Majority);
        assert!(
            !rule.chooses(&ids(&[0, 3])),
            "half of four is not a majority"
        );
        assert!(rule.chooses(&ids(&[0, 1, 3])));
    }

    #[test]
    fn a_stranger_counts_for_nothing() {
        let rule = Phase2Rule::new(&members(3), QuorumSystem::Majority);
        assert!(!rule.chooses(&ids(&[0, 7, 8])));
    }

    #[test]
    fn a_grid_decides_in_one_column_only() {
        // 2 × 2, full columns: member i at (i / 2, i % 2). Column 0 is {0, 2}.
        let rule = Phase2Rule::new(&members(4), QuorumSystem::grid(2, 2));
        assert!(rule.chooses(&ids(&[0, 2])));
        assert!(rule.chooses(&ids(&[1, 3])));
        assert!(!rule.chooses(&ids(&[0, 1])), "a row is not a column");
        assert!(!rule.chooses(&ids(&[0, 3])), "two columns never add up");
    }

    #[test]
    fn grid_partitions_decide_on_a_majority_of_one_column() {
        // 3 × 2 with acks 2: column 0 is {0, 2, 4}, column 1 is {1, 3, 5}.
        let system = QuorumSystem::Grid {
            rows: 3,
            cols: 2,
            acks: 2,
        };
        let rule = Phase2Rule::new(&members(6), system);
        assert!(rule.chooses(&ids(&[0, 4])));
        assert!(rule.chooses(&ids(&[3, 5])));
        assert!(!rule.chooses(&ids(&[0, 1])), "one vote per column");
    }

    /// The rule agrees with the core's on every subset of small
    /// configurations: two independent texts of the same papers.
    #[test]
    fn the_rule_agrees_with_the_core_on_every_subset() {
        let systems = [
            (3, QuorumSystem::Majority),
            (5, QuorumSystem::Majority),
            (4, QuorumSystem::grid(2, 2)),
            (6, QuorumSystem::grid(2, 3)),
            (
                6,
                QuorumSystem::Grid {
                    rows: 3,
                    cols: 2,
                    acks: 2,
                },
            ),
        ];
        for (n, system) in systems {
            let all = members(n);
            let config = paros_consensus::membership::AcceptorConfig::new(all.clone(), system);
            let rule = Phase2Rule::new(&all, system);
            for mask in 0u32..(1 << n) {
                let voters: BTreeSet<NodeId> = all
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, id)| *id)
                    .collect();
                assert_eq!(
                    rule.chooses(&voters),
                    config.has_phase2_quorum(&voters),
                    "{system:?} over {voters:?}"
                );
            }
        }
    }
}
