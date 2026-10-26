use std::collections::BTreeSet;

use super::{AcceptorConfig, FailureDomain};

/// The verdict of the **zone rule** over one configuration (#215 (zones),
/// `docs/architecture/05-failure-model-and-zones.md` §5): two predicates over the members' failure
/// domains, each asked through the configuration's own quorum predicates,
/// never a count.
///
/// 1. **It survives any one failure domain**: for every domain, the members
///    outside it still hold a Phase-1 quorum and a Phase-2 quorum.
/// 2. **No domain commits alone**: for every domain, the members inside it
///    hold no Phase-2 quorum.
///
/// Under acceptor partitions (a grid, #532 (acceptor partitions)) both
/// predicates apply per partition, because the grid's predicates do: a
/// Phase-1 quorum needs enough of every column and a Phase-2 quorum is
/// enough of one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZoneRule {
    /// The configuration carries no failure domains: the rule is not asked
    /// (the plain arm).
    Unlabeled,
    /// Both predicates hold.
    Holds,
    /// Losing this domain leaves the other members without a Phase-1 or a
    /// Phase-2 quorum.
    LosesQuorum(FailureDomain),
    /// The members in this domain hold a Phase-2 quorum alone.
    CommitsAlone(FailureDomain),
}

impl ZoneRule {
    /// Whether the rule holds.
    #[must_use]
    pub fn holds(&self) -> bool {
        matches!(self, Self::Holds)
    }
}

impl<Id: Copy + Ord> AcceptorConfig<Id> {
    /// The distinct failure domains this configuration's members stand in;
    /// empty when it is unlabeled.
    #[must_use]
    pub fn distinct_domains(&self) -> BTreeSet<&FailureDomain> {
        self.domains().iter().collect()
    }

    /// The **zone rule** over this configuration ([`ZoneRule`]): whether it
    /// survives the loss of any one failure domain and no domain commits
    /// alone, judged through [`AcceptorConfig::has_phase1_quorum`] and
    /// [`AcceptorConfig::has_phase2_quorum`]. A configuration in one domain
    /// never holds it: losing that domain loses everything.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn zone_rule(&self) -> ZoneRule {
        if !self.is_zoned() {
            return ZoneRule::Unlabeled;
        }
        let members = self.members();
        let domains = self.domains();
        assert_eq!(
            members.len(),
            domains.len(),
            "a labeled configuration labels every member"
        );
        for domain in self.distinct_domains() {
            let mut inside = BTreeSet::new();
            let mut outside = BTreeSet::new();
            for (member, d) in members.iter().zip(domains) {
                if d == domain {
                    inside.insert(*member);
                } else {
                    outside.insert(*member);
                }
            }
            assert!(
                !inside.is_empty(),
                "a domain of a configuration holds a member"
            );
            assert_eq!(
                inside.len() + outside.len(),
                members.len(),
                "a domain splits the membership in two"
            );
            if !(self.has_phase1_quorum(&outside) && self.has_phase2_quorum(&outside)) {
                return ZoneRule::LosesQuorum(domain.clone());
            }
            if self.has_phase2_quorum(&inside) {
                return ZoneRule::CommitsAlone(domain.clone());
            }
        }
        // Surviving a domain needs a member outside every domain.
        assert!(
            self.distinct_domains().len() >= 2,
            "a configuration that holds the zone rule spans two domains"
        );
        ZoneRule::Holds
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, QuorumSystem};

    fn zoned(members: &[(u64, &str)], quorum_system: QuorumSystem) -> AcceptorConfig {
        AcceptorConfig::zoned(
            members
                .iter()
                .map(|(id, az)| (NodeId(*id), FailureDomain::new("", *az)))
                .collect(),
            quorum_system,
        )
    }

    #[test]
    fn an_unlabeled_configuration_is_not_judged() {
        let config = AcceptorConfig::new(
            vec![NodeId(1), NodeId(2), NodeId(3)],
            QuorumSystem::Majority,
        );
        assert_eq!(config.zone_rule(), ZoneRule::Unlabeled);
    }

    #[test]
    fn a_majority_spread_one_per_zone_holds() {
        let config = zoned(&[(1, "a"), (2, "b"), (3, "c")], QuorumSystem::Majority);
        assert!(config.zone_rule().holds());
        let config = zoned(
            &[(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "c")],
            QuorumSystem::Majority,
        );
        assert!(config.zone_rule().holds(), "2/2/1 holds");
    }

    #[test]
    fn two_zones_never_hold() {
        let config = zoned(&[(1, "a"), (2, "a"), (3, "b")], QuorumSystem::Majority);
        assert_eq!(
            config.zone_rule(),
            ZoneRule::LosesQuorum(FailureDomain::new("", "a"))
        );
    }

    #[test]
    fn a_zone_holding_a_majority_fails() {
        // 3/1/1: losing zone a loses the majority.
        let config = zoned(
            &[(1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "c")],
            QuorumSystem::Majority,
        );
        assert!(!config.zone_rule().holds());
        // Four over three zones: 2/1/1 loses its majority with zone a.
        let config = zoned(
            &[(1, "a"), (2, "a"), (3, "b"), (4, "c")],
            QuorumSystem::Majority,
        );
        assert!(!config.zone_rule().holds());
    }

    #[test]
    fn a_small_phase2_quorum_commits_alone() {
        // Flexible q2 = 1: one acceptor decides, so its zone commits alone.
        let config = zoned(
            &[(1, "a"), (2, "b"), (3, "c")],
            QuorumSystem::Flexible { q1: 3, q2: 1 },
        );
        assert!(!config.zone_rule().holds());
    }

    #[test]
    fn partitions_are_judged_per_partition() {
        // Two partitions of three, row-major: column 0 is {1, 3, 5}, column
        // 1 is {2, 4, 6}. Each column one per zone: it holds.
        let grid = QuorumSystem::Grid {
            rows: 3,
            cols: 2,
            acks: 2,
        };
        let config = zoned(
            &[(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "c"), (6, "c")],
            grid,
        );
        assert!(config.zone_rule().holds());
        // Column 0 holds two members of zone a: losing it loses column 0.
        let config = zoned(
            &[(1, "a"), (2, "b"), (3, "a"), (4, "c"), (5, "b"), (6, "c")],
            grid,
        );
        assert!(!config.zone_rule().holds());
    }
}
