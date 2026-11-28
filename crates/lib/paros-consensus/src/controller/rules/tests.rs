//! One table test for each rule and its negative, on a majority and on a
//! grid.

use super::*;
use crate::contract::feedback::{
    LeaderFeedback, MatchmakerFeedback, NodeFeedback, NodeInsight, ProxyFeedback,
};
use crate::contract::write::RetireMark;
use crate::membership::{
    AcceptorConfig, FailureDomain, MatchmakerGeneration, MatchmakerId, MatchmakerSet, ProxyId,
    QuorumSystem,
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
fn rule_3_counts_no_member_that_reports_another_set_of_its_generation() {
    let config = majority(&[1, 2, 3]);
    let mut view = View::new();
    view.observe_node(leader(&config, &[]));
    view.observe_node(follower(2, &config));
    view.observe_node(follower(3, &config));
    view.observe_matchmaker(matchmaker(0, &config));
    // Members 1 and 2 of the set {0, 1, 2} report another set of the same
    // generation: their feedback is not of the set, so the set has one voter.
    let other = MatchmakerSet::new(
        MatchmakerGeneration(0),
        vec![
            MatchmakerId(1),
            MatchmakerId(2),
            MatchmakerId(4),
            MatchmakerId(5),
            MatchmakerId(6),
        ],
    );
    for id in [1, 2] {
        view.observe_matchmaker(MatchmakerFeedback {
            set: other.clone(),
            ..matchmaker(id, &config)
        });
    }
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::Matchmakers),
        "a member counts for a set only when it reports that set"
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
        Decision::Hold(HoldReason::NoSpare { node: NodeId(4) })
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
fn rule_6_orders_a_new_check_of_a_node_checked_on_an_earlier_floor() {
    // The matchmakers checked node 4 on an earlier floor, and a new
    // leadership moved its belief to the floor in force before it retired:
    // its durable mark is checked, but it waits for a check at this floor.
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, LEADS);
    view.observe_node(NodeFeedback {
        retire_mark: RetireMark {
            incarnation: 0,
            checked: true,
        },
        ..outside(
            4,
            &config,
            vec![
                NodeInsight::Outside,
                NodeInsight::RetirementUnchecked { floor: LEADS },
            ],
        )
    });
    assert_eq!(
        decide(&view),
        Decision::Order(Order::Retire {
            node: NodeId(4),
            watermark: LEADS,
        }),
        "a check on an earlier floor does not let the node retire at this one"
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
    // The node is checked at the floor already: its driver reads
    // `may_retire`.
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, LEADS);
    view.observe_node(NodeFeedback {
        retire_mark: RetireMark {
            incarnation: 0,
            checked: true,
        },
        ..outside(
            4,
            &config,
            vec![
                NodeInsight::Outside,
                NodeInsight::MayRetire { floor: LEADS },
            ],
        )
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
        Decision::Hold(HoldReason::NoSpare { node: NodeId(3) })
    );
    let config = majority(&[1, 2, 3, 4, 5]);
    let view = view_of(&config, &[(5, 4), (4, 3)], &[1, 2, 3]);
    assert_eq!(
        decide(&view),
        Decision::Hold(HoldReason::NoSpare { node: NodeId(4) }),
        "the lowest silent member first"
    );
}

// ---- rules 8 to 14 ---------------------------------------------------------

fn spare(id: u64) -> Spare {
    Spare {
        node: NodeId(id),
        domain: None,
    }
}

/// The view of [`view_of`] on a majority `{1, 2, 3}` where node 3 is silent
/// on both witnesses, with the fresh spares `spares` running the journal.
fn silent_three(spares: &[u64]) -> (AcceptorConfig, View) {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[(3, 2)], &[1, 2]);
    for &id in spares {
        view.observe_node(follower(id, &config));
    }
    (config, view)
}

fn reconfigure_to(config: AcceptorConfig) -> Decision {
    Decision::Order(Order::Reconfigure {
        leader: NodeId(1),
        ballot: LEADS,
        since: SINCE,
        config,
    })
}

fn relead(view: &mut View, config: &AcceptorConfig, edit: impl FnOnce(&mut LeaderFeedback)) {
    let mut leader = leader(config, &[(3, 2)]);
    edit(leader.leading.as_mut().expect("a leader"));
    view.observe_node(leader);
}

#[test]
fn rule_8_holds_while_the_leadership_is_not_settled() {
    let (config, mut view) = silent_three(&[4]);
    relead(&mut view, &config, |leading| leading.settled = false);
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        Decision::Hold(HoldReason::Unsettled)
    );
}

#[test]
fn rule_8_and_9_do_not_hold_without_a_silent_member() {
    let config = majority(&[1, 2, 3]);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    let mut leader = leader(&config, &[]);
    let leading = leader.leading.as_mut().expect("a leader");
    leading.settled = false;
    leading.prior = vec![majority(&[1, 2, 4])];
    view.observe_node(leader);
    assert_eq!(evaluate(&view, &[spare(4)], &POLICY), Decision::Steady);
}

#[test]
fn rule_9_holds_while_a_prior_configuration_is_in_force() {
    let (config, mut view) = silent_three(&[4]);
    relead(&mut view, &config, |leading| {
        leading.prior = vec![majority(&[1, 2, 5])];
    });
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        Decision::Hold(HoldReason::GcNotEffective)
    );
}

#[test]
fn rule_10_holds_without_a_spare_that_can_replace() {
    let no_spare = Decision::Hold(HoldReason::NoSpare { node: NodeId(3) });
    let (_, view) = silent_three(&[4]);
    assert_eq!(evaluate(&view, &[], &POLICY), no_spare, "no spare");
    assert_eq!(
        evaluate(&view, &[spare(2)], &POLICY),
        no_spare,
        "a member is no spare"
    );
    assert_eq!(
        evaluate(&view, &[spare(3)], &POLICY),
        no_spare,
        "the silent member is no spare of itself"
    );
    assert_eq!(
        evaluate(&view, &[spare(5)], &POLICY),
        no_spare,
        "a spare with no feedback does not run the journal"
    );
    // A spare whose feedback is stale.
    let (config, view) = silent_three(&[]);
    let mut stale = View::new();
    stale.observe_node(follower(4, &config));
    for _ in 0..=POLICY.max_age {
        stale.tick();
    }
    for aged in view.nodes().values() {
        stale.observe_node(aged.feedback.clone());
    }
    for aged in view.matchmakers().values() {
        stale.observe_matchmaker(aged.feedback.clone());
    }
    assert_eq!(evaluate(&stale, &[spare(4)], &POLICY), no_spare, "stale");
    // A retired spare.
    let (config, mut view) = silent_three(&[]);
    view.observe_node(NodeFeedback {
        retire_mark: RetireMark {
            incarnation: 0,
            checked: true,
        },
        ..follower(4, &config)
    });
    assert_eq!(evaluate(&view, &[spare(4)], &POLICY), no_spare, "retired");
}

fn zone(az: &str) -> FailureDomain {
    FailureDomain::new("r", az)
}

fn zoned() -> AcceptorConfig {
    AcceptorConfig::zoned(
        vec![
            (NodeId(1), zone("a")),
            (NodeId(2), zone("b")),
            (NodeId(3), zone("c")),
        ],
        QuorumSystem::Majority,
    )
}

#[test]
fn rule_10_on_a_zoned_configuration_keeps_a_spare_of_the_same_domain() {
    let config = zoned();
    let mut view = view_of(&config, &[(3, 2)], &[1, 2]);
    view.observe_node(follower(4, &config));
    view.observe_node(follower(5, &config));
    let wrong = Spare {
        node: NodeId(4),
        domain: Some(zone("a")),
    };
    let unknown = spare(4);
    let right = Spare {
        node: NodeId(5),
        domain: Some(zone("c")),
    };
    assert_eq!(
        evaluate(&view, &[wrong.clone(), unknown], &POLICY),
        Decision::Hold(HoldReason::NoSpare { node: NodeId(3) }),
        "a spare of another domain or of no domain does not replace"
    );
    assert_eq!(
        evaluate(&view, &[wrong, right], &POLICY),
        reconfigure_to(AcceptorConfig::zoned(
            vec![
                (NodeId(1), zone("a")),
                (NodeId(2), zone("b")),
                (NodeId(5), zone("c")),
            ],
            QuorumSystem::Majority,
        ))
    );
}

#[test]
fn rule_10_on_a_configuration_with_no_zones_does_not_read_the_domain() {
    let (_, view) = silent_three(&[4]);
    let with_domain = Spare {
        node: NodeId(4),
        domain: Some(zone("z")),
    };
    assert_eq!(
        evaluate(&view, &[with_domain], &POLICY),
        reconfigure_to(majority(&[1, 2, 4]))
    );
}

#[test]
fn rule_11_holds_when_a_role_cannot_address_the_spare() {
    let not_admitted = Decision::Hold(HoldReason::SpareNotAdmitted { node: NodeId(3) });
    // The leader's pool.
    let (config, mut view) = silent_three(&[8]);
    assert_eq!(
        evaluate(&view, &[spare(8)], &POLICY),
        not_admitted,
        "leader"
    );
    // A fresh member's pool.
    view.observe_node(follower(4, &config));
    view.observe_node(NodeFeedback {
        pool: ids(&[1, 2, 3]),
        ..follower(2, &config)
    });
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        not_admitted,
        "member"
    );
    // A fresh proxy's pool.
    let (_, mut view) = silent_three(&[4]);
    view.observe_proxy(ProxyFeedback {
        id: ProxyId(0),
        ballot: LEADS,
        pool: ids(&[1, 2, 3]),
        open_rounds: 0,
        decided: 0,
        expired: 0,
        relayed_nacks: 0,
        insights: Vec::new(),
    });
    assert_eq!(evaluate(&view, &[spare(4)], &POLICY), not_admitted, "proxy");
}

#[test]
fn rule_11_reads_no_pool_of_a_fresh_node_outside_the_configuration() {
    // Node 9 is fresh and outside `C`, and its pool lacks spare 4. It sends
    // no `Prepare` and no `Accept` to the spare, so its pool does not count.
    let (config, mut view) = silent_three(&[4]);
    view.observe_node(follower(4, &config));
    view.observe_node(NodeFeedback {
        pool: ids(&[1, 2, 3]),
        ..follower(9, &config)
    });
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        reconfigure_to(majority(&[1, 2, 4]))
    );
}

#[test]
fn rule_11_takes_the_first_spare_that_every_role_can_address() {
    let (_, view) = silent_three(&[8, 5, 4]);
    assert_eq!(
        evaluate(&view, &[spare(8), spare(5), spare(4)], &POLICY),
        reconfigure_to(majority(&[1, 2, 5])),
        "8 is outside the leader's pool; 5 comes before 4 in the caller's list"
    );
}

#[test]
fn rule_13_holds_when_a_column_of_the_new_grid_has_no_fresh_quorum() {
    // Full columns {1, 3} and {2, 4}. Node 4 is silent; node 3 sent no
    // fresh feedback but the leader did not miss it. Phase 1 of the grid
    // holds on the row {1, 2}. With spare 5 the grid is {1, 2, 3, 5}, and
    // its column {1, 3} has no fresh quorum.
    let config = grid();
    let mut view = view_of(&config, &[(4, 2)], &[1, 2]);
    view.observe_node(follower(5, &config));
    assert_eq!(
        evaluate(&view, &[spare(5)], &POLICY),
        Decision::Hold(HoldReason::NewConfigurationLost { node: NodeId(4) })
    );
}

#[test]
fn rule_14_replaces_the_silent_member_of_a_grid() {
    let config = grid();
    let mut view = view_of(&config, &[(4, 2)], &[1, 2, 3]);
    view.observe_node(follower(5, &config));
    assert_eq!(
        evaluate(&view, &[spare(5)], &POLICY),
        reconfigure_to(AcceptorConfig::new(
            ids(&[1, 2, 3, 5]),
            QuorumSystem::grid(2, 2)
        ))
    );
}

#[test]
fn rule_14_keeps_the_counts_of_proxy_leaders_and_replicas() {
    let config = majority(&[1, 2, 3]).with_counts(2, 1);
    let mut view = view_of(&config, &[(3, 2)], &[1, 2]);
    view.observe_node(follower(4, &config));
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        reconfigure_to(majority(&[1, 2, 4]).with_counts(2, 1))
    );
}

#[test]
fn rule_14_names_the_leadership_that_the_view_saw() {
    let (_, view) = silent_three(&[4]);
    let Decision::Order(Order::Reconfigure {
        leader,
        ballot,
        since,
        config,
    }) = evaluate(&view, &[spare(4)], &POLICY)
    else {
        panic!("an order to reconfigure");
    };
    assert_eq!((leader, ballot, since), (NodeId(1), LEADS, SINCE));
    assert!(!config.contains(NodeId(3)), "the silent member leaves");
    assert!(config.contains(NodeId(4)), "the spare joins");
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

// ---- a swap that moves members to other columns ----------------------------

/// A view of `config` where node 1 leads, the leader missed `silent` for
/// two windows, and every node of `fresh` and every spare of `spares` runs
/// the journal, each with the pool `1..=pool`.
fn wide_view(config: &AcceptorConfig, silent: u64, fresh: &[u64], pool: u64) -> View {
    let pool: Vec<NodeId> = (1..=pool).map(NodeId).collect();
    let mut view = View::new();
    view.observe_node(NodeFeedback {
        pool: pool.clone(),
        ..leader(config, &[(silent, 2)])
    });
    for &id in fresh {
        if id != 1 {
            view.observe_node(NodeFeedback {
                pool: pool.clone(),
                ..follower(id, config)
            });
        }
    }
    for id in 0..3 {
        view.observe_matchmaker(matchmaker(id, config));
    }
    view
}

/// Nodes 1 to 6 on a grid of three rows and two columns (`acks = 2`),
/// with the domains `a, c, b, a, c, b`: its columns are `{1:a, 3:b, 5:c}`
/// and `{2:c, 4:a, 6:b}`.
fn zoned_grid() -> AcceptorConfig {
    let domains = ["a", "c", "b", "a", "c", "b"];
    AcceptorConfig::zoned(
        (1..=6)
            .zip(domains)
            .map(|(id, az)| (NodeId(id), zone(az)))
            .collect(),
        QuorumSystem::partitions(1, 2),
    )
}

#[test]
fn rule_13_holds_when_the_swap_breaks_the_zone_rule_of_a_grid() {
    // Node 2 (c) is silent, and spare 7 (c) sorts last. So the new grid is
    // {1, 3, 4, 5, 6, 7}, with the columns {1:a, 4:a, 6:b} and
    // {3:b, 5:c, 7:c}: domain a holds a Phase-2 quorum of a column alone.
    let config = zoned_grid();
    assert_eq!(config.zone_rule(), ZoneRule::Holds);
    let view = wide_view(&config, 2, &[1, 3, 4, 5, 6, 7], 7);
    let spare = Spare {
        node: NodeId(7),
        domain: Some(zone("c")),
    };
    assert_eq!(
        evaluate(&view, &[spare], &POLICY),
        Decision::Hold(HoldReason::ZoneRuleLost { node: NodeId(2) })
    );
}

#[test]
fn rule_14_on_a_zoned_grid_orders_a_swap_that_keeps_the_zone_rule() {
    // Node 6 (b) is silent, and spare 7 (b) sorts into its place.
    let config = zoned_grid();
    let view = wide_view(&config, 6, &[1, 2, 3, 4, 5, 7], 7);
    let spare = Spare {
        node: NodeId(7),
        domain: Some(zone("b")),
    };
    let next = AcceptorConfig::zoned(
        [(1, "a"), (2, "c"), (3, "b"), (4, "a"), (5, "c"), (7, "b")]
            .into_iter()
            .map(|(id, az)| (NodeId(id), zone(az)))
            .collect(),
        QuorumSystem::partitions(1, 2),
    );
    assert_eq!(next.zone_rule(), ZoneRule::Holds);
    assert_eq!(evaluate(&view, &[spare], &POLICY), reconfigure_to(next));
}

#[test]
fn rule_13_holds_when_the_swap_moves_two_missing_members_into_one_column() {
    // Nodes 1 to 15 on a grid of five rows and three columns (`acks = 2`):
    // a Phase-1 quorum needs four members of each column. Node 4 (column
    // 0) is silent; nodes 2 (column 1) and 6 (column 2) sent no fresh
    // feedback. So each column of the grid in force misses one member.
    // With spare 16, the members after node 4 move one place down: node 6
    // moves to column 1, beside node 2, and that column of the new grid
    // has three fresh members only. Its Phase 2 still holds (two acks).
    let grid = QuorumSystem::Grid {
        rows: 5,
        cols: 3,
        acks: 2,
    };
    let config = AcceptorConfig::new((1..=15).map(NodeId).collect(), grid);
    let fresh: Vec<u64> = (1..=16).filter(|id| ![2, 4, 6].contains(id)).collect();
    let view = wide_view(&config, 4, &fresh, 16);
    assert_eq!(
        evaluate(&view, &[spare(16)], &POLICY),
        Decision::Hold(HoldReason::NewConfigurationLost { node: NodeId(4) })
    );
}

#[test]
fn rule_6_never_retires_a_spare_of_the_caller() {
    // Node 4 runs the journal outside `{1, 2, 3}`, and the leader made a
    // floor effective: without the spare list, rule 6 retires it.
    let config = majority(&[1, 2, 3]);
    let floor = ballot(4, 1);
    let mut view = view_of(&config, &[], &[1, 2, 3]);
    with_floor(&mut view, &config, floor);
    view.observe_node(outside(
        4,
        &config,
        vec![NodeInsight::RetirementUnchecked { floor }],
    ));
    assert_eq!(
        evaluate(&view, &[], &POLICY),
        Decision::Order(Order::Retire {
            node: NodeId(4),
            watermark: floor,
        })
    );
    assert_eq!(
        evaluate(&view, &[spare(4)], &POLICY),
        Decision::Steady,
        "a spare that the caller offers stays"
    );
}

#[test]
fn rule_11_holds_when_the_spare_would_not_vote_for_the_leader() {
    // A node drops a `Prepare` or an `Accept` from outside its own pool. On
    // the full grid of 2 x 2, the new column {2, 5} needs spare 5, so a
    // spare whose pool lacks the leader would stall it.
    let config = grid();
    let mut view = view_of(&config, &[(4, 2)], &[1, 2, 3]);
    view.observe_node(NodeFeedback {
        pool: ids(&[5]),
        ..follower(5, &config)
    });
    assert_eq!(
        evaluate(&view, &[spare(5)], &POLICY),
        Decision::Hold(HoldReason::SpareNotAdmitted { node: NodeId(4) })
    );
    view.observe_node(NodeFeedback {
        pool: ids(&[1, 2, 3, 5]),
        ..follower(5, &config)
    });
    assert_eq!(
        evaluate(&view, &[spare(5)], &POLICY),
        reconfigure_to(AcceptorConfig::new(
            ids(&[1, 2, 3, 5]),
            QuorumSystem::grid(2, 2)
        )),
        "the pool of the spare needs the leader and the members that stay, not the silent one"
    );
}

#[test]
fn rule_13_takes_the_first_spare_whose_new_grid_is_live() {
    // The grid of five rows and three columns again, on the ids 10 to 150.
    // Node 40 is silent; nodes 20 and 60 sent no fresh feedback. Spare 160
    // sorts last and moves node 60 beside node 20; spare 45 sorts into the
    // place of node 40 and moves nobody.
    let grid = QuorumSystem::Grid {
        rows: 5,
        cols: 3,
        acks: 2,
    };
    let members: Vec<u64> = (1..=15).map(|i| i * 10).collect();
    let config = AcceptorConfig::new(ids(&members), grid);
    let fresh: Vec<u64> = members
        .iter()
        .copied()
        .chain([45, 160])
        .filter(|id| ![20, 40, 60].contains(id))
        .collect();
    let pool: Vec<NodeId> = members
        .iter()
        .copied()
        .chain([45, 160])
        .map(NodeId)
        .collect();
    let mut view = View::new();
    view.observe_node(NodeFeedback {
        id: NodeId(10),
        pool: pool.clone(),
        ..leader(&config, &[(40, 2)])
    });
    for &id in &fresh {
        if id != 10 {
            view.observe_node(NodeFeedback {
                pool: pool.clone(),
                leader: Some(NodeId(10)),
                ..follower(id, &config)
            });
        }
    }
    for id in 0..3 {
        view.observe_matchmaker(matchmaker(id, &config));
    }
    assert_eq!(
        evaluate(&view, &[spare(160)], &POLICY),
        Decision::Hold(HoldReason::NewConfigurationLost { node: NodeId(40) })
    );
    let next: Vec<u64> = members
        .iter()
        .copied()
        .map(|id| if id == 40 { 45 } else { id })
        .collect();
    assert_eq!(
        evaluate(&view, &[spare(160), spare(45)], &POLICY),
        Decision::Order(Order::Reconfigure {
            leader: NodeId(10),
            ballot: LEADS,
            since: SINCE,
            config: AcceptorConfig::new(ids(&next), grid),
        })
    );
}

#[test]
fn rules_10_to_13_replace_a_higher_silent_member_when_the_lowest_has_no_spare() {
    // A zoned majority of five, with the domains a, b, c, a, b. Nodes 2 (b)
    // and 3 (c) are silent; the one spare, 6, is of domain c.
    let domains = ["a", "b", "c", "a", "b"];
    let config = AcceptorConfig::zoned(
        (1..=5)
            .zip(domains)
            .map(|(id, az)| (NodeId(id), zone(az)))
            .collect(),
        QuorumSystem::Majority,
    );
    let mut view = view_of(&config, &[(2, 2), (3, 2)], &[1, 4, 5]);
    view.observe_node(follower(6, &config));
    let spare = Spare {
        node: NodeId(6),
        domain: Some(zone("c")),
    };
    assert_eq!(
        evaluate(&view, &[], &POLICY),
        Decision::Hold(HoldReason::NoSpare { node: NodeId(2) }),
        "with no spare, the hold names the lowest silent member"
    );
    let next = AcceptorConfig::zoned(
        [(1, "a"), (2, "b"), (4, "a"), (5, "b"), (6, "c")]
            .into_iter()
            .map(|(id, az)| (NodeId(id), zone(az)))
            .collect(),
        QuorumSystem::Majority,
    );
    assert_eq!(evaluate(&view, &[spare], &POLICY), reconfigure_to(next));
}
