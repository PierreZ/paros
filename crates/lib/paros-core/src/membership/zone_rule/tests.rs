use super::*;
use crate::NodeId;
use crate::membership::QuorumSystem;

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
