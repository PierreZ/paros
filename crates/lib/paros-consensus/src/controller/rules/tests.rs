//! One table test for each rule and its negative, on a majority and on a
//! grid.

use super::*;
use crate::contract::feedback::{
    LeaderFeedback, MatchmakerFeedback, NodeFeedback, NodeInsight, ProxyFeedback,
};
use crate::contract::write::RetireMark;
use crate::membership::{
    AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem,
};
use crate::types::{Ballot, NodeId};

const POLICY: Policy = Policy {
    max_age: 3,
    silent_windows: 2,
};

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn ids(members: &[u64]) -> Vec<NodeId> {
    members.iter().copied().map(NodeId).collect()
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(ids(members), QuorumSystem::Majority)
}

/// Two rows of two, full columns (`acks = rows`): columns `{1, 3}` and
/// `{2, 4}`, rows `{1, 2}` and `{3, 4}`.
fn grid() -> AcceptorConfig {
    AcceptorConfig::new(ids(&[1, 2, 3, 4]), QuorumSystem::grid(2, 2))
}

/// The leader's ballot in every test.
const LEADS: Ballot = Ballot {
    round: 5,
    node: NodeId(1),
};
/// The ballot its configuration is bound to.
const SINCE: Ballot = Ballot {
    round: 4,
    node: NodeId(1),
};

fn follower(id: u64, config: &AcceptorConfig) -> NodeFeedback {
    NodeFeedback {
        id: NodeId(id),
        ballot: LEADS,
        leader: Some(NodeId(1)),
        acceptors: config.clone(),
        since: SINCE,
        heard: true,
        pool: ids(&[1, 2, 3, 4, 5, 6, 7]),
        retire_mark: RetireMark::default(),
        gc_floor_heard: None,
        leading: None,
        tick: 0,
        quorum_windows_held: 0,
        quorum_lost_step_downs: 0,
        matchmaking_timeouts: 0,
        retained_bytes: 0,
        insights: Vec::new(),
    }
}

/// Node 1, leading `config` at [`LEADS`], with `missed` windows.
fn leader(config: &AcceptorConfig, missed: &[(u64, u64)]) -> NodeFeedback {
    NodeFeedback {
        leading: Some(LeaderFeedback {
            settled: true,
            prior: Vec::new(),
            gc_effective: None,
            missed: missed.iter().map(|(id, w)| (NodeId(*id), *w)).collect(),
        }),
        ..follower(1, config)
    }
}

fn matchmaker(id: u64, config: &AcceptorConfig) -> MatchmakerFeedback {
    MatchmakerFeedback {
        id: MatchmakerId(id),
        set: MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
        ),
        active: true,
        gc_watermark: Ballot::zero(),
        effective: Some((SINCE, config.clone())),
        highest: Some(LEADS),
    }
}

/// A view where node 1 leads `config`, every other member in `fresh`
/// reports, and the three matchmakers report.
fn view_of(config: &AcceptorConfig, missed: &[(u64, u64)], fresh: &[u64]) -> View {
    let mut view = View::new();
    view.observe_node(leader(config, missed));
    for &id in fresh {
        if id != 1 {
            view.observe_node(follower(id, config));
        }
    }
    for id in 0..3 {
        view.observe_matchmaker(matchmaker(id, config));
    }
    view
}

fn decide(view: &View) -> Decision {
    evaluate(view, &[], &POLICY)
}

// ---- rule 1 ---------------------------------------------------------------

#[test]
fn rule_1_holds_without_a_fresh_leader() {
    assert_eq!(decide(&View::new()), Decision::Hold(HoldReason::NoLeader));
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    for _ in 0..=POLICY.max_age {
        view.tick();
    }
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::NoLeader),
        "a leader feedback past max_age is not fresh"
    );
}

#[test]
fn rule_1_passes_with_a_fresh_leader() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    for _ in 0..POLICY.max_age {
        view.tick();
    }
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "age max_age is still fresh"
    );
}

// ---- rule 2 ---------------------------------------------------------------

#[test]
fn rule_2_holds_when_a_node_or_a_proxy_reports_a_higher_ballot() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_node(NodeFeedback {
        ballot: ballot(6, 2),
        ..follower(2, &config)
    });
    assert_eq!(decide(&view), Decision::Hold(HoldReason::LeadershipMoves));

    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_proxy(ProxyFeedback {
        id: ProxyId(0),
        ballot: ballot(6, 2),
        pool: ids(&[1, 2, 3]),
        open_rounds: 0,
        decided: 0,
        expired: 0,
        relayed_nacks: 0,
        insights: Vec::new(),
    });
    assert_eq!(decide(&view), Decision::Hold(HoldReason::LeadershipMoves));
}

#[test]
fn rule_2_passes_when_the_higher_ballot_is_stale() {
    let config = majority(&[1, 2, 3]);
    let mut view = View::new();
    view.observe_node(NodeFeedback {
        ballot: ballot(6, 2),
        ..follower(2, &config)
    });
    for _ in 0..=POLICY.max_age {
        view.tick();
    }
    for id in 0..3 {
        view.observe_matchmaker(matchmaker(id, &config));
    }
    view.observe_node(leader(&config, &[]));
    view.observe_node(follower(3, &config));
    assert_eq!(decide(&view), Decision::Steady);
}

fn proxy(ballot: Ballot) -> ProxyFeedback {
    ProxyFeedback {
        id: ProxyId(0),
        ballot,
        pool: ids(&[1, 2, 3]),
        open_rounds: 0,
        decided: 0,
        expired: 0,
        relayed_nacks: 0,
        insights: Vec::new(),
    }
}

#[test]
fn rule_2_passes_on_a_proxy_at_the_leaders_ballot_or_a_stale_one_above() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_proxy(proxy(LEADS));
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "a fresh proxy that runs the leader's ballot is no move"
    );

    let mut view = View::new();
    view.observe_proxy(proxy(ballot(6, 2)));
    for _ in 0..=POLICY.max_age {
        view.tick();
    }
    for id in 0..3 {
        view.observe_matchmaker(matchmaker(id, &config));
    }
    view.observe_node(leader(&config, &[]));
    view.observe_node(follower(2, &config));
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "a stale proxy above the leader is no move"
    );
}

// ---- rule 3 ---------------------------------------------------------------

#[test]
fn rule_3_holds_without_a_fresh_matchmaker_majority() {
    let config = majority(&[1, 2, 3]);
    let mut view = View::new();
    view.observe_node(leader(&config, &[]));
    view.observe_node(follower(2, &config));
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Matchmakers),
        "no matchmaker reports"
    );
    view.observe_matchmaker(matchmaker(0, &config));
    assert_eq!(decide(&view), Decision::Hold(HoldReason::Matchmakers));
    view.observe_matchmaker(MatchmakerFeedback {
        active: false,
        ..matchmaker(1, &config)
    });
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Matchmakers),
        "an inactive matchmaker does not count"
    );
    view.observe_matchmaker(matchmaker(2, &config));
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "two of three are a majority"
    );
}

#[test]
fn rule_3_holds_when_a_matchmaker_holds_a_later_configuration() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_matchmaker(MatchmakerFeedback {
        effective: Some((ballot(5, 2), majority(&[1, 2, 4]))),
        ..matchmaker(2, &config)
    });
    assert_eq!(decide(&view), Decision::Hold(HoldReason::Matchmakers));
}

#[test]
fn rule_3_holds_a_plain_journal_with_no_matchmaker() {
    let config = majority(&[1, 2, 3]);
    let mut view = View::new();
    view.observe_node(leader(&config, &[(3, 9)]));
    view.observe_node(follower(2, &config));
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Matchmakers),
        "a plain journal refuses every reconfiguration, so it gets no order"
    );
}

#[test]
fn rule_3_counts_no_matchmaker_of_another_set() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    let other = MatchmakerFeedback {
        id: MatchmakerId(7),
        set: MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(7), MatchmakerId(8), MatchmakerId(9)],
        ),
        ..matchmaker(7, &config)
    };
    view.observe_matchmaker(other);
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "a matchmaker of another set at the same generation does not panic the controller"
    );
    let mut view = View::new();
    view.observe_node(leader(&config, &[]));
    view.observe_node(follower(2, &config));
    view.observe_matchmaker(matchmaker(0, &config));
    for id in [7, 8] {
        view.observe_matchmaker(MatchmakerFeedback {
            id: MatchmakerId(id),
            ..matchmaker(0, &config)
        });
    }
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Matchmakers),
        "outsiders that claim the set are not a majority of it"
    );
}

#[test]
fn rule_3_an_inactive_matchmaker_of_a_later_generation_does_not_count() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_matchmaker(MatchmakerFeedback {
        id: MatchmakerId(4),
        set: MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(4), MatchmakerId(5), MatchmakerId(6)],
        ),
        active: false,
        ..matchmaker(4, &config)
    });
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "an inactive member of a set to come does not move the set in force"
    );
}

// ---- two leaders ----------------------------------------------------------

#[test]
fn the_leader_with_the_lower_ballot_is_ignored() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    let stale = NodeFeedback {
        id: NodeId(2),
        ballot: ballot(1, 2),
        ..leader(&config, &[(3, 9)])
    };
    view.observe_node(stale);
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "node 1 leads at the higher ballot and misses no member"
    );
}

// ---- rule 4 ---------------------------------------------------------------

#[test]
fn rule_4_holds_without_a_fresh_phase1_quorum_on_a_majority() {
    let config = majority(&[1, 2, 3]);
    let view = view_of(&config, &[(2, 9), (3, 9)], &[1]);
    assert_eq!(decide(&view), Decision::Hold(HoldReason::Phase1Lost));
    let view = view_of(&config, &[(3, 9)], &[1, 2]);
    assert_ne!(decide(&view), Decision::Hold(HoldReason::Phase1Lost));
}

#[test]
fn rule_4_holds_without_a_fresh_member_in_a_column_of_a_grid() {
    // Phase 1 on the full grid needs one member of each column.
    let config = grid();
    let view = view_of(&config, &[], &[1, 3]);
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Phase1Lost),
        "column {{2, 4}} has no fresh member"
    );
    let view = view_of(&config, &[], &[1, 2]);
    assert_eq!(
        decide(&view),
        Decision::Steady,
        "a full row is a Phase-1 quorum"
    );
}

// ---- rule 5 ---------------------------------------------------------------

#[test]
fn rule_5_a_lost_column_alone_does_not_hold() {
    // On the full grid one silent member blocks its column; Phase 1 still
    // holds, so the controller goes on to the silent member.
    let config = grid();
    let view = view_of(&config, &[(4, 2)], &[1, 2, 3]);
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::MemberSilent { node: NodeId(4) })
    );
}

// ---- rule 6 ---------------------------------------------------------------

fn with_floor(view: &mut View, config: &AcceptorConfig, floor: Ballot) {
    let mut leader = leader(config, &[]);
    leader.leading.as_mut().expect("a leader").gc_effective = Some((floor, ids(&[4])));
    view.observe_node(leader);
}

fn outside(id: u64, config: &AcceptorConfig, insights: Vec<NodeInsight>) -> NodeFeedback {
    NodeFeedback {
        insights,
        ..follower(id, config)
    }
}

#[test]
fn rule_6_orders_the_retirement_of_a_node_outside_at_the_effective_floor() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, LEADS);
    view.observe_node(outside(
        4,
        &config,
        vec![
            NodeInsight::Outside,
            NodeInsight::RetirementUnchecked { floor: LEADS },
        ],
    ));
    assert_eq!(
        decide(&view),
        Decision::Order(Order::Retire {
            node: NodeId(4),
            watermark: LEADS,
        })
    );
}

#[test]
fn rule_6_orders_nothing_without_an_effective_floor_or_on_another_floor() {
    let config = majority(&[1, 2, 3]);
    let unchecked = vec![
        NodeInsight::Outside,
        NodeInsight::RetirementUnchecked { floor: LEADS },
    ];
    // No effective floor.
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    view.observe_node(outside(4, &config, unchecked.clone()));
    assert_eq!(decide(&view), Decision::Steady);
    // The node heard another floor.
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, ballot(6, 1));
    view.observe_node(outside(4, &config, unchecked.clone()));
    assert_eq!(decide(&view), Decision::Steady);
    // The node is checked already: its driver reads `may_retire`.
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, LEADS);
    view.observe_node(NodeFeedback {
        retire_mark: RetireMark {
            incarnation: 0,
            checked: true,
        },
        ..outside(4, &config, unchecked.clone())
    });
    assert_eq!(decide(&view), Decision::Steady);
    // The node is stale.
    let mut view = View::new();
    view.observe_node(outside(4, &config, unchecked));
    for _ in 0..=POLICY.max_age {
        view.tick();
    }
    let fresh = view_of(&config, &[], &[1, 2, 3]);
    for aged in fresh.nodes().values() {
        view.observe_node(aged.feedback.clone());
    }
    for aged in fresh.matchmakers().values() {
        view.observe_matchmaker(aged.feedback.clone());
    }
    with_floor(&mut view, &config, LEADS);
    assert_eq!(decide(&view), Decision::Steady);
}

// ---- rule 7 ---------------------------------------------------------------

#[test]
fn rule_7_is_steady_while_no_member_is_silent_on_two_witnesses() {
    let config = majority(&[1, 2, 3]);
    // The leader missed node 3 once: under the threshold.
    let view = view_of(&config, &[(3, 1)], &[1, 2]);
    assert_eq!(decide(&view), Decision::Steady);
    // The leader missed node 3 twice, but node 3 reports: one witness.
    let view = view_of(&config, &[(3, 2)], &[1, 2, 3]);
    assert_eq!(decide(&view), Decision::Steady);
}

#[test]
fn rule_7_names_a_member_silent_on_both_witnesses() {
    let config = majority(&[1, 2, 3]);
    let view = view_of(&config, &[(3, 2)], &[1, 2]);
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::MemberSilent { node: NodeId(3) })
    );
    let config = majority(&[1, 2, 3, 4, 5]);
    let view = view_of(&config, &[(5, 4), (4, 3)], &[1, 2, 3]);
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::MemberSilent { node: NodeId(4) }),
        "the lowest silent member first"
    );
}

#[test]
#[should_panic(expected = "a member is silent for one window at least")]
fn a_policy_with_no_silent_window_is_refused() {
    let _ = evaluate(
        &View::new(),
        &[],
        &Policy {
            max_age: 1,
            silent_windows: 0,
        },
    );
}
