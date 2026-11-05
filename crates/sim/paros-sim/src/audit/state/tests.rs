//! The per-journal safety oracles of [`AuditState`] on hand-built event
//! sequences: each test feeds one honest sequence that must pass and one
//! broken step the oracle that owns it must call red.

use std::collections::BTreeSet;

use crate::audit::testing::red;
use paros::core::contract::Party;
use paros::core::membership::{AcceptorConfig, FailureDomain, QuorumSystem};
use paros::core::{Ballot, NodeId, Slot};

use super::{AuditState, Floor};

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn config(members: &[u64], quorum: QuorumSystem) -> AcceptorConfig {
    AcceptorConfig::new(members.iter().map(|m| NodeId(*m)).collect(), quorum)
}

fn majority(members: &[u64]) -> AcceptorConfig {
    config(members, QuorumSystem::Majority)
}

/// A journal of three acceptors on plain Multi-Paxos.
fn three() -> AuditState {
    AuditState {
        bootstrap: Some(majority(&[0, 1, 2])),
        ..AuditState::default()
    }
}

const LEADER: Party = Party::Node(NodeId(0));

#[test]
fn a_floor_answers_the_value_established_strictly_before_an_instant() {
    let mut floor = Floor::default();
    floor.raise(4, 10);
    assert_eq!(floor.now, 4);
    assert_eq!(
        floor.strictly_before(10),
        0,
        "the same instant sees the old floor"
    );
    assert_eq!(floor.strictly_before(11), 4);
    floor.raise(6, 10);
    assert_eq!(
        floor.strictly_before(10),
        0,
        "two raises in one instant keep the first old floor"
    );
    floor.raise(3, 12);
    assert_eq!(floor.now, 6, "a lower request is a no-op");
    assert_eq!(floor.raised_ms, 10);
    floor.raise(8, 12);
    assert_eq!(floor.strictly_before(12), 6);
    assert_eq!(floor.strictly_before(13), 8);
}

#[test]
fn a_configuration_is_bound_to_its_ballot_once() {
    let mut st = three();
    let b = ballot(2, 0);
    let c = majority(&[0, 1, 3]);
    assert!(!red(|| {
        st.bind_config(b, &c);
        st.bind_config(b, &c);
    }));
    assert_eq!(st.config_of(b), Some(&c));
    assert_eq!(st.config_of(ballot(9, 1)), st.bootstrap.as_ref());
    assert!(red(|| st.bind_config(b, &majority(&[0, 1, 2]))));
    let zoned = AcceptorConfig::zoned(
        vec![
            (NodeId(0), FailureDomain::new("r", "a")),
            (NodeId(1), FailureDomain::new("r", "b")),
            (NodeId(3), FailureDomain::new("r", "c")),
        ],
        QuorumSystem::Majority,
    );
    assert!(
        red(|| st.bind_config(b, &zoned)),
        "the same members under other labels"
    );
}

#[test]
fn the_known_member_sets_are_the_bootstrap_each_ballot_and_each_prior() {
    let mut st = three();
    st.bind_config(ballot(2, 0), &majority(&[0, 1, 3]));
    st.note_prior(0, ballot(2, 0), &[majority(&[4, 5, 6])]);
    let sets: BTreeSet<BTreeSet<u64>> = [[0, 1, 2], [0, 1, 3], [4, 5, 6]]
        .iter()
        .map(|s| s.iter().copied().collect())
        .collect();
    assert_eq!(st.known_member_sets(), sets);
    assert_eq!(st.prior_of(ballot(2, 0)).map(<[_]>::len), Some(1));
    // Only the ballot's owner's matchmaking counts.
    st.note_prior(1, ballot(3, 0), &[majority(&[7])]);
    assert!(st.prior_of(ballot(3, 0)).is_none());
}

#[test]
fn a_prepare_reaches_only_the_configuration_and_its_priors() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(!red(|| st.observe_prepare_send(0, 2, b)));
    assert!(red(|| st.observe_prepare_send(0, 5, b)));
    st.note_prior(0, b, &[majority(&[5, 6, 7])]);
    assert!(!red(|| st.observe_prepare_send(0, 5, b)));
    let mut bare = AuditState::default();
    assert!(
        !red(|| bare.observe_prepare_send(0, 5, b)),
        "no configuration known: nothing judged"
    );
}

#[test]
fn a_promise_sender_outside_the_configuration_is_noted() {
    let mut st = three();
    let b = ballot(2, 0);
    st.observe_promise_send(1, b);
    assert!(!st.removed_member_promised);
    st.observe_promise_send(7, b);
    assert!(st.removed_member_promised);
    assert_eq!(
        st.promise_senders[&(2, 0)],
        [1, 7].into_iter().collect::<BTreeSet<_>>()
    );
}

#[test]
fn one_ballot_proposes_at_most_one_command_for_a_slot() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(!red(|| {
        st.observe_proposal(LEADER, b, 4, 11);
        st.observe_proposal(LEADER, b, 4, 11);
        st.observe_proposal(LEADER, ballot(3, 0), 4, 12);
    }));
    assert!(st.any_proposal_checked);
    assert!(red(|| st.observe_proposal(LEADER, b, 4, 12)));
}

#[test]
fn an_accept_reaches_only_the_ballots_own_acceptors() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(!red(|| st.observe_accept_send(LEADER, 1, b)));
    assert!(!st.joined_member_accepted);
    assert!(red(|| st.observe_accept_send(LEADER, 5, b)));
    let mut bare = AuditState::default();
    assert!(!red(|| bare.observe_accept_send(LEADER, 5, b)));
    // A ballot bound to a configuration with a joined member.
    st.bind_config(ballot(4, 0), &majority(&[0, 1, 5]));
    assert!(!red(|| st.observe_accept_send(LEADER, 5, ballot(4, 0))));
    assert!(st.joined_member_accepted);
}

#[test]
fn no_accept_leaves_before_every_prior_configuration_promised_a_quorum() {
    let mut st = three();
    let b = ballot(2, 0);
    st.note_prior(0, b, &[majority(&[0, 1, 2]), majority(&[0, 3, 4])]);
    // The owner's own vote and node 1 cover the first, not the second.
    st.observe_promise_send(1, b);
    assert!(red(|| st.observe_accept_send(LEADER, 1, b)));
    st.observe_promise_send(3, b);
    assert!(!red(|| st.observe_accept_send(LEADER, 1, b)));
    assert!(st.cross_config_phase1_checked);
}

#[test]
fn a_promised_ballot_never_decreases() {
    let mut st = three();
    assert!(!red(|| {
        st.observe_promise(0, ballot(2, 0));
        st.observe_promise(0, ballot(2, 0));
        st.observe_promise(0, ballot(3, 1));
    }));
    assert!(red(|| st.observe_promise(0, ballot(2, 2))));
}

#[test]
fn a_recovered_value_is_the_reported_one_or_the_one_in_flight() {
    let mut st = three();
    assert!(!red(|| st.judge_recovered_value(0, 4, 11, None)));
    assert_eq!(
        st.persisted[&(0, 4)],
        11,
        "an unreported landing becomes the reference"
    );
    assert!(!red(|| st.judge_recovered_value(0, 4, 11, None)));
    assert!(red(|| st.judge_recovered_value(0, 4, 12, None)));
    assert!(red(|| st.judge_recovered_value(0, 4, 12, Some(13))));
    assert!(!red(|| st.judge_recovered_value(0, 4, 12, Some(12))));
    assert_eq!(
        st.persisted[&(0, 4)],
        12,
        "the landed record is the new reference"
    );
}

#[test]
fn take_in_flight_drains_one_node_only() {
    let mut st = three();
    st.in_flight.insert((0, 3), 30);
    st.in_flight.insert((0, 5), 50);
    st.in_flight.insert((1, 3), 31);
    let taken = st.take_in_flight(0);
    assert_eq!(
        taken.into_iter().collect::<Vec<_>>(),
        vec![(3, 30), (5, 50)]
    );
    assert!(st.take_in_flight(0).is_empty());
    assert_eq!(st.in_flight.len(), 1);
}

#[test]
fn a_boot_counts_a_dropped_accept_only_under_its_chosen_index() {
    let mut st = three();
    let b = ballot(2, 0);
    st.dropped_in_flight.insert((0, 3), (b, 30));
    st.dropped_in_flight.insert((0, 5), (b, 50));
    st.dropped_in_flight.insert((1, 3), (b, 30));
    assert!(!red(|| st.settle_dropped_in_flight(0, Some(Slot(4)))));
    assert!(st.accepted.contains_key(&(3, 2, 0)));
    assert!(
        !st.accepted.contains_key(&(5, 2, 0)),
        "beyond the chosen index it never landed"
    );
    assert_eq!(st.dropped_in_flight.len(), 1);
    assert!(!red(|| st.settle_dropped_in_flight(1, None)));
    assert!(st.dropped_in_flight.is_empty());
}

#[test]
fn a_durable_quorum_decides_and_never_decides_twice() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(!red(|| {
        st.observe_durable_accept(0, 4, b, 11);
        st.observe_durable_accept(0, 4, b, 11);
    }));
    assert!(st.decided.is_empty(), "one vote of three decides nothing");
    st.observe_durable_accept(1, 4, b, 11);
    assert_eq!(st.decided.get(&4), Some(&(2, 0, 11)));
    assert_eq!(st.decided_max, Some(4));
    assert!(!st.decided_below_majority);
    // At most one command per (slot, ballot).
    assert!(red(|| st.observe_durable_accept(2, 4, b, 12)));
    // An accept at or above the decided ballot carries the decided value.
    let higher = ballot(3, 1);
    assert!(red(|| st.observe_durable_accept(0, 4, higher, 13)));
    // Below the decided ballot anything may have been accepted.
    assert!(!red(|| st.observe_durable_accept(2, 4, ballot(1, 2), 14)));
}

#[test]
fn two_quorums_that_decide_two_values_are_red() {
    let mut st = three();
    st.observe_durable_accept(0, 4, ballot(2, 0), 11);
    st.observe_durable_accept(1, 4, ballot(2, 0), 11);
    // A quorum at a lower ballot completes later, with another value.
    let lower = ballot(1, 2);
    st.observe_durable_accept(2, 4, lower, 12);
    assert!(red(|| st.observe_durable_accept(0, 4, lower, 12)));
}

#[test]
fn a_learner_outside_the_configuration_casts_no_vote() {
    let mut st = three();
    let b = ballot(2, 0);
    st.observe_durable_accept(0, 4, b, 11);
    st.observe_durable_accept(7, 4, b, 11);
    assert!(st.decided.is_empty());
}

#[test]
fn a_flexible_quorum_decides_below_a_majority() {
    let mut st = AuditState {
        bootstrap: Some(config(
            &[0, 1, 2, 3],
            QuorumSystem::Flexible { q1: 3, q2: 2 },
        )),
        ..AuditState::default()
    };
    st.observe_durable_accept(0, 4, ballot(2, 0), 11);
    st.observe_durable_accept(1, 4, ballot(2, 0), 11);
    assert_eq!(st.decided_vhash(4), Some(11));
    assert!(st.decided_below_majority);
    assert!(!st.decided_on_column);
}

#[test]
fn a_grid_decision_off_the_slots_own_column_is_noted() {
    let grid = QuorumSystem::Grid {
        rows: 2,
        cols: 2,
        acks: 2,
    };
    let mut st = AuditState {
        bootstrap: Some(config(&[0, 1, 2, 3], grid)),
        ..AuditState::default()
    };
    // Slot 0's own column is {0, 2}.
    st.observe_durable_accept(0, 0, ballot(2, 0), 11);
    st.observe_durable_accept(2, 0, ballot(2, 0), 11);
    assert!(st.decided_on_column && !st.decided_off_column);
    st.observe_durable_accept(1, 2, ballot(2, 0), 12);
    st.observe_durable_accept(3, 2, ballot(2, 0), 12);
    assert_eq!(st.decided_vhash(2), Some(12));
    assert!(st.decided_off_column);
}

#[test]
fn the_cluster_floor_is_the_lowest_booted_voters() {
    let mut st = three();
    assert_eq!(st.cluster_min_floor(), 0);
    st.booted.extend([0, 1]);
    st.floor.insert(
        0,
        Floor {
            now: 8,
            ..Floor::default()
        },
    );
    assert_eq!(st.cluster_min_floor(), 0, "node 1 has no floor yet");
    st.floor.insert(
        1,
        Floor {
            now: 5,
            ..Floor::default()
        },
    );
    assert_eq!(st.cluster_min_floor(), 5);
    st.prune_below_floor();
    assert_eq!(st.pruned_floor, 5);
    // A node that boots late lowers the cluster floor, never the pruned one.
    st.booted.insert(2);
    st.prune_below_floor();
    assert_eq!(st.pruned_floor, 5);
}

#[test]
fn a_prune_keeps_the_decided_witness_and_drops_the_tallies() {
    let mut st = three();
    let b = ballot(2, 0);
    for slot in [3, 6] {
        st.observe_durable_accept(0, slot, b, slot * 10);
        st.observe_durable_accept(1, slot, b, slot * 10);
    }
    st.booted.extend([0, 1, 2]);
    for node in 0..3 {
        st.floor.insert(
            node,
            Floor {
                now: 5,
                ..Floor::default()
            },
        );
    }
    st.prune_below_floor();
    assert!(!st.decided.contains_key(&3));
    assert!(st.decided.contains_key(&6));
    assert_eq!(st.decided_vhash(3), Some(30));
    assert_eq!(st.decided_vhash(6), Some(60));
    assert_eq!(st.decided_vhash(4), None);
    assert!(st.accept_sets.keys().all(|(slot, _, _)| *slot >= 5));
}

#[test]
fn an_authority_is_relinquished_by_its_holder_and_never_exercised_again() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(!red(|| {
        st.observe_authority_use(0, b);
        st.observe_authority_use(0, b);
    }));
    assert!(
        red(|| st.observe_authority_use(1, b)),
        "two holders at once"
    );
    let mut st = three();
    st.observe_authority_use(0, b);
    assert!(red(|| st.observe_authority_release(1, b, Slot(4))));
    let mut st = three();
    st.observe_authority_use(0, b);
    assert!(!red(|| st.observe_authority_release(0, b, Slot(4))));
    assert!(
        red(|| st.observe_authority_use(0, b)),
        "the retired node again"
    );
    assert!(
        red(|| st.observe_authority_release(1, b, Slot(3))),
        "the frontier rewinds"
    );
    let mut st = three();
    st.observe_authority_use(0, b);
    st.observe_authority_release(0, b, Slot(4));
    assert!(!red(|| st.observe_authority_use(1, b)));
    assert!(
        st.handoff_streamed_without_phase1,
        "node 1 streams node 0's ballot"
    );
}

#[test]
fn a_proxy_commit_is_backed_by_a_durable_quorum_and_carries_the_proposal() {
    let mut st = three();
    let b = ballot(2, 0);
    assert!(
        red(|| st.observe_proxy_decision(9, 4, b, 11)),
        "no durable accept"
    );
    st.observe_durable_accept(0, 4, b, 11);
    st.observe_durable_accept(1, 4, b, 11);
    assert!(!red(|| st.observe_proxy_decision(9, 4, b, 11)));
    assert!(st.decided_through_proxy);
    st.observe_proposal(LEADER, b, 4, 11);
    assert!(
        red(|| st.observe_proxy_decision(9, 4, b, 12)),
        "not the delegated command"
    );
    assert!(!st.proxied_round_survived_handoff);
    st.fanout_leaders
        .insert((4, 2, 0), [0, 1].into_iter().collect());
    st.observe_proxy_decision(9, 4, b, 11);
    assert!(st.proxied_round_survived_handoff);
}

#[test]
fn an_applied_prefix_advances_one_slot_at_a_time() {
    let mut st = three();
    assert!(!red(|| {
        st.observe_applied_index(0, 0);
        st.observe_applied_index(0, 1);
        st.observe_applied_index(0, 1);
        st.observe_applied_index(0, 0);
    }));
    assert!(red(|| st.observe_applied_index(0, 5)));
    // A jump is legal at the compaction floor, at a landing and over
    // slots the node reported faulty.
    let mut st = three();
    st.floor.insert(
        0,
        Floor {
            now: 5,
            ..Floor::default()
        },
    );
    st.landings.insert(1, [7].into_iter().collect());
    st.reported_faulty
        .insert(2, [0, 1, 2].into_iter().collect());
    assert!(!red(|| {
        st.check_no_gaps(0, 5);
        st.check_no_gaps(1, 7);
        st.check_no_gaps(2, 3);
    }));
    assert_eq!(st.frontier[&0], 6);
    assert!(red(|| st.check_no_gaps(2, 9)), "a slot not reported faulty");
}

#[test]
fn the_applied_fold_tracks_the_high_water_mark_and_the_laggards() {
    let mut st = three();
    st.observe_applied_index(0, 0);
    st.observe_applied_index(1, 0);
    st.observe_applied_index(0, 1);
    assert_eq!(st.cluster_applied_max, Some(1));
    assert_eq!(st.applied_max[&0], 1);
    assert_eq!(st.applied_max[&1], 0);
    assert!(st.lagged.contains(&1));
    assert!(!st.lagged.contains(&0));
    st.observe_applied_index(0, 0);
    assert_eq!(st.applied_max[&0], 1, "a replay never lowers the prefix");
}

#[test]
fn a_confirmed_read_index_never_regresses_within_a_boot() {
    let mut st = three();
    assert!(!red(|| {
        st.check_read_frontier(NodeId(0), None);
        st.check_read_frontier(NodeId(0), Some(Slot(3)));
        st.check_read_frontier(NodeId(0), Some(Slot(3)));
        st.check_read_frontier(NodeId(1), Some(Slot(1)));
    }));
    assert!(red(|| st.check_read_frontier(NodeId(0), Some(Slot(2)))));
    assert!(red(|| st.check_read_frontier(NodeId(0), None)));
}

#[test]
fn a_deposed_leader_streak_closes_after_a_silent_beat_period() {
    let mut st = three();
    let b = ballot(2, 0);
    st.election_timeouts.insert(0, 3);
    st.promised.insert(1, ballot(5, 1));
    st.promised.insert(2, ballot(5, 1));
    st.observe_beat(0, b);
    assert!(st.deposed_streaks[&0].deposed);
    for _ in 0..=paros::core::node::HEARTBEAT_TICKS {
        st.observe_tick(0);
    }
    assert!(!st.deposed_streaks.contains_key(&0));
    // A beat at a ballot no promise majority passed is not deposed.
    let mut st = three();
    st.observe_beat(0, b);
    assert!(!st.deposed_streaks[&0].deposed);
    assert!(!red(|| {
        for _ in 0..20 {
            st.observe_beat(0, b);
            st.observe_tick(0);
        }
    }));
}

#[test]
fn a_new_ballot_restarts_the_deposed_streak() {
    let mut st = three();
    st.election_timeouts.insert(0, 1);
    st.promised.insert(1, ballot(5, 1));
    st.promised.insert(2, ballot(5, 1));
    for _ in 0..2 {
        st.observe_beat(0, ballot(2, 0));
        st.observe_tick(0);
    }
    assert_eq!(st.deposed_streaks[&0].ticks, 2);
    st.observe_beat(0, ballot(3, 0));
    assert_eq!(st.deposed_streaks[&0].ticks, 0);
    assert_eq!(st.deposed_streaks[&0].round, 3);
    // An ack from outside the configuration, or at another ballot, does
    // not restart it.
    st.observe_tick(0);
    st.observe_ack_received(0, 7, ballot(3, 0));
    st.observe_ack_received(0, 1, ballot(2, 0));
    assert_eq!(st.deposed_streaks[&0].ticks, 1);
    st.observe_ack_received(0, 1, ballot(3, 0));
    assert_eq!(st.deposed_streaks[&0].ticks, 0);
}

#[test]
fn a_floor_raised_to_where_it_stands_moves_nothing() {
    let mut floor = Floor {
        now: 4,
        before_last_raise: 2,
        raised_ms: 1,
    };
    floor.raise(4, 9);
    assert_eq!(
        (floor.now, floor.before_last_raise, floor.raised_ms),
        (4, 2, 1)
    );
    floor.raise(5, 9);
    assert_eq!(
        (floor.now, floor.before_last_raise, floor.raised_ms),
        (5, 4, 9)
    );
}

#[test]
fn a_boot_takes_only_its_own_commits_in_flight() {
    let mut st = three();
    st.in_flight.insert((1, 0), 10);
    st.in_flight.insert((1, 3), 13);
    st.in_flight.insert((2, 0), 20);
    let taken = st.take_in_flight(1);
    assert_eq!(
        taken.into_iter().collect::<Vec<_>>(),
        vec![(0, 10), (3, 13)]
    );
    assert_eq!(
        st.in_flight.keys().copied().collect::<Vec<_>>(),
        vec![(2, 0)]
    );
}

#[test]
fn a_boot_counts_a_dropped_accept_its_chosen_index_covers() {
    let mut st = three();
    st.dropped_in_flight.insert((1, 0), (ballot(1, 0), 10));
    st.dropped_in_flight.insert((1, 2), (ballot(1, 0), 12));
    st.dropped_in_flight.insert((1, 3), (ballot(1, 0), 13));
    st.dropped_in_flight.insert((2, 0), (ballot(1, 0), 20));
    assert!(!red(|| st.settle_dropped_in_flight(1, Some(Slot(2)))));
    assert_eq!(st.accepted.get(&(0, 1, 0)), Some(&10), "slot 0 counts");
    assert_eq!(
        st.accepted.get(&(2, 1, 0)),
        Some(&12),
        "the index itself counts"
    );
    assert!(!st.accepted.contains_key(&(3, 1, 0)), "past the index");
    assert_eq!(
        st.dropped_in_flight.keys().copied().collect::<Vec<_>>(),
        vec![(2, 0)],
        "another node's stay"
    );
    st.dropped_in_flight.insert((1, 5), (ballot(1, 0), 15));
    assert!(!red(|| st.settle_dropped_in_flight(1, None)));
    assert!(!st.accepted.contains_key(&(5, 1, 0)));
}

#[test]
fn a_floor_of_one_prunes_slot_zero() {
    let mut st = three();
    st.decided.insert(0, (1, 0, 10));
    st.decided.insert(1, (1, 0, 11));
    for node in 0..3 {
        st.booted.insert(node);
        st.floor.insert(
            node,
            Floor {
                now: 1,
                before_last_raise: 0,
                raised_ms: 0,
            },
        );
    }
    assert_eq!(st.cluster_min_floor(), 1);
    st.prune_below_floor();
    assert_eq!(st.pruned_floor, 1);
    assert!(!st.decided.contains_key(&0) && st.decided.contains_key(&1));
    assert_eq!(st.decided_vhash(0), Some(10));
}

#[test]
fn a_beat_is_outvoted_only_by_promises_strictly_past_it() {
    let mut st = three();
    st.promised.insert(1, ballot(2, 0));
    st.promised.insert(2, ballot(2, 0));
    st.observe_beat(0, ballot(2, 0));
    let streak = &st.deposed_streaks[&0];
    assert!(!streak.deposed, "a promise at the ballot acks it");
    assert_eq!(streak.timeout, 0, "a node with no timeout reported");
}

#[test]
fn a_beat_that_is_not_outvoted_clears_the_streak() {
    let mut st = three();
    st.election_timeouts.insert(0, 9);
    st.promised.insert(1, ballot(5, 1));
    st.promised.insert(2, ballot(5, 1));
    st.observe_beat(0, ballot(2, 0));
    st.observe_tick(0);
    assert!(st.deposed_streaks[&0].deposed);
    assert_eq!(st.deposed_streaks[&0].ticks, 1);
    st.promised.insert(1, ballot(1, 1));
    st.observe_beat(0, ballot(2, 0));
    let streak = &st.deposed_streaks[&0];
    assert!(!streak.deposed);
    assert_eq!(streak.ticks, 0);
    assert_eq!(streak.timeout, 9);
}

#[test]
fn a_first_applied_index_past_zero_is_a_gap_without_a_floor() {
    let mut st = three();
    assert!(red(|| st.check_no_gaps(0, 1)));
    let mut st = three();
    assert!(!red(|| {
        st.check_no_gaps(0, 0);
        st.check_no_gaps(0, 1);
        st.check_no_gaps(0, 1);
    }));
    assert!(red(|| st.check_no_gaps(0, 3)));
    assert_eq!(st.frontier[&0], 4);
}

#[test]
fn the_linearizability_search_runs_once_every_client_merged() {
    let mut st = three();
    st.lin.merged = 1;
    st.check_client_history(2);
    assert!(!st.lin.searched, "a client still to merge");
    st.check_client_history(1);
    assert!(st.lin.searched);
    st.lin.merged = 3;
    st.check_client_history(1);
    assert!(st.lin.searched);
}
