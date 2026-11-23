//! The local protocol assertions, each driven to its trip point through the
//! public API: a message that a correct peer can never send lands on the
//! assertion that names the rule it breaks, and the rejection paths leave the
//! state they refused to touch untouched.
//!
//! Every negative test here is a *programmer/protocol* invariant, not an
//! operating condition: the inputs are ones only a broken proposer (two
//! commands under one ballot, a decision that contradicts an earlier one) could
//! produce, which is exactly what crash-beats-corruption is for.

use super::*;
use crate::contract::write::{AcceptorWrite, WriteOp};

fn accept(from: u64, b: Ballot, slot: u64, command: Command) -> Message {
    Message::Accept {
        reply_to: Party::Node(NodeId(from)),
        leader: NodeId(from),
        ballot: b,
        slot: Slot(slot),
        command,
        config: None,
    }
}

fn commit(from: u64, b: Ballot, slot: u64, command: Command) -> Message {
    Message::Commit {
        from: Party::Node(NodeId(from)),
        ballot: b,
        slot: Slot(slot),
        command,
    }
}

fn promise(from: u64, b: Ballot, accepted: BTreeMap<Slot, (Ballot, Command)>) -> Message {
    terminal_promise(NodeId(from), b, accepted)
}

// ---- accepted-state mutation ----------------------------------------------

/// P2b at the acceptor: one ballot has one proposer, so a second `Accept` for
/// the same `(slot, ballot)` must repeat the command.
#[test]
#[should_panic(expected = "an accept at or below the recorded ballot carries the recorded command")]
fn same_slot_same_ballot_different_command_trips_the_acceptor() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(accept(1, ballot(1, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(accept(1, ballot(1, 1), 0, ucmd(1, 1, 20)));
}

/// P2c at the acceptor: a value chosen at a *lower* ballot than the record this
/// node holds is, by quorum intersection, the value it already accepted. A
/// `Commit` that regresses the ballot with a different command is a broken
/// decision, not a stale message.
#[test]
#[should_panic(expected = "an accept at or below the recorded ballot carries the recorded command")]
fn a_lower_ballot_decision_with_a_different_command_trips_the_acceptor() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(accept(1, ballot(2, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(commit(2, ballot(1, 2), 0, ucmd(1, 1, 20)));
}

/// The same regression with the *same* command is legal (the P2c chain): the
/// slot is learned, and the record keeps its higher ballot. An acceptor's
/// recorded ballot never falls: Phase 1 and the CTRL thresholds read it as
/// the highest ballot it accepted (#529 (sets oracles after PR #515)).
#[test]
fn a_lower_ballot_decision_with_the_same_command_is_learned() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(accept(1, ballot(2, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(commit(2, ballot(1, 2), 0, ucmd(1, 1, 10)));
    assert_eq!(chosen_at(&n, 0), Some(val(10)));
    assert_eq!(n.acceptor().records()[&Slot(0)].0, ballot(2, 1));
}

/// A higher ballot may replace the command outright: the earlier accept was a
/// value no quorum chose (the gap-fill terrain).
#[test]
fn a_higher_ballot_accept_replaces_a_stale_command() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(accept(1, ballot(1, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(accept(2, ballot(2, 2), 0, Command::Control(Control::Noop)));
    assert_eq!(
        n.acceptor().records()[&Slot(0)],
        (ballot(2, 2), Command::Control(Control::Noop))
    );
}

/// A refused `Accept` is a pure reply: no record, no promise, no durable write.
#[test]
fn a_nacked_accept_leaves_the_accepted_log_and_the_batch_untouched() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(Message::Prepare {
        reply_to: NodeId(1),
        ballot: ballot(5, 1),
        from_slot: Slot(0),
        config: None,
    });
    drain(&mut n);
    n.step(accept(2, ballot(3, 2), 0, ucmd(1, 1, 10)));
    assert!(
        n.acceptor().records().is_empty(),
        "a nacked accept records nothing"
    );
    assert!(
        n.pending_writes.is_empty(),
        "a nacked accept stages no durable write"
    );
    assert_eq!(n.hard_state().max_promised_ballot, ballot(5, 1));
    let out = drain(&mut n);
    assert!(matches!(
        out.as_slice(),
        [(NodeId(2), Message::Nack { .. })]
    ));
}

// ---- learning a decision ----------------------------------------------------

/// Agreement, locally: a slot chosen here is only ever relearned with the
/// value it was chosen with.
#[test]
#[should_panic(expected = "a slot already chosen here is relearned with the same value")]
fn a_duplicate_decision_with_a_different_value_trips_the_learner() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(commit(1, ballot(1, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(commit(1, ballot(1, 1), 0, ucmd(1, 1, 20)));
}

/// The same decision twice is idempotent (a duplicated `Commit`).
#[test]
fn a_duplicate_decision_with_the_same_value_is_idempotent() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(commit(1, ballot(1, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(commit(1, ballot(1, 1), 0, ucmd(1, 1, 10)));
    assert_eq!(chosen_at(&n, 0), Some(val(10)));
    assert!(
        n.pending_writes.is_empty(),
        "relearning a known decision writes nothing"
    );
}

/// A decision at the ballot of the leader's own open round carries the round's
/// command (one proposer per ballot).
#[test]
#[should_panic(expected = "a decision at the open round's ballot carries the round's command")]
fn a_decision_contradicting_the_open_round_trips_the_leader() {
    let mut nodes = cluster_with_three_chosen();
    let _ = nodes[0].propose(entry(1, 4, 40));
    drain(&mut nodes[0]);
    let b = nodes[0].ballot();
    nodes[0].step(commit(1, b, 3, ucmd(9, 9, 99)));
}

// ---- Phase-1 merge -----------------------------------------------------------

/// Two acceptors reporting one `(slot, ballot)` with different commands is a
/// P2b violation somewhere upstream; the merge refuses to pick one.
#[test]
#[should_panic(expected = "two Phase-1 reports of one (slot, ballot) agree on the command")]
fn conflicting_equal_ballot_promise_reports_trip_the_merge() {
    let mut n = node(0, &[0, 1, 2, 3, 4]);
    campaign(&mut n);
    drain(&mut n);
    let b = n.ballot();
    let mut first = BTreeMap::new();
    first.insert(Slot(0), (ballot(0, 3), ucmd(1, 1, 10)));
    n.step(promise(1, b, first));
    let mut second = BTreeMap::new();
    second.insert(Slot(0), (ballot(0, 3), ucmd(1, 1, 20)));
    n.step(promise(2, b, second));
}

/// The rule the merge encodes: the highest reported ballot wins, and a lower
/// report arriving later never displaces it.
#[test]
fn the_merge_keeps_the_highest_ballot_report() {
    let mut n = node(0, &[0, 1, 2, 3, 4]);
    campaign(&mut n);
    drain(&mut n);
    let b = n.ballot();
    let mut high = BTreeMap::new();
    high.insert(Slot(0), (ballot(0, 4), ucmd(1, 1, 40)));
    n.step(promise(1, b, high));
    let mut low = BTreeMap::new();
    low.insert(Slot(0), (ballot(0, 2), ucmd(1, 1, 20)));
    n.step(promise(2, b, low));
    // The third promise wins the election; the recovered value re-proposed
    // for slot 0 is the highest-ballot report.
    n.step(promise(3, b, BTreeMap::new()));
    assert!(n.is_leader());
    let round = n
        .proposer
        .rounds()
        .get(&Slot(0))
        .expect("slot 0 is re-proposed");
    assert_eq!(*round.command(), ucmd(1, 1, 40));
}

// ---- truncation and trim-point boundaries ------------------------------------

/// A trim point at or below the floor is an operating condition (a stale
/// answer): ignored whole, with no frontier moving backwards.
#[test]
fn a_stale_trim_point_never_rewinds_a_frontier() {
    let mut nodes = cluster_with_three_chosen();
    let before = nodes[1].hard_state();
    let floor = nodes[1].acceptor().first_slot();
    nodes[1].step(Message::TrimmedTo {
        from: NodeId(0),
        point: floor,
        state: crate::journal::JournalState::default(),
    });
    assert_eq!(nodes[1].hard_state(), before);
    assert_eq!(nodes[1].acceptor().first_slot(), floor);
    assert!(
        nodes[1].pending_writes.is_empty(),
        "a stale trim point writes nothing"
    );
}

// ---- each invariant check, fed a state it must refuse ----------------------

#[test]
#[should_panic(expected = "a recovery page is never empty")]
fn an_empty_recovery_page_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.limits.recovery_page = 0;
    n.assert_invariants();
}

/// A faulty record under the chosen prefix with no fold hole at or below
/// it: the fold passed a slot this node cannot read.
#[test]
#[should_panic(expected = "the fold's hole covers every faulty slot under the chosen prefix")]
fn a_fold_past_a_faulty_slot_fails_the_invariants() {
    let nodes = cluster_with_three_chosen();
    let mut storage = MemStorage::from_node(&nodes[1]);
    storage.rot(Slot(1));
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    assert_eq!(n.replica.fold_hole(), Some(Slot(1)));
    n.replica.learn(Slot(1), &ucmd(1, 2, 20));
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "a plain deployment stores no read basis")]
fn a_plain_read_basis_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.read_basis = Some(crate::quorum_read::ReadBasis {
        config: cfg(&[0, 1, 2]),
        since: Ballot::zero(),
        fence: None,
    });
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "the membership fence covers the registration fence")]
fn a_registration_fence_above_the_membership_fence_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.registration_fence = ballot(5, 0);
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "a plain deployment's configuration is bound to no ballot")]
fn a_plain_configuration_bound_to_a_ballot_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.acceptors_since = ballot(3, 0);
    n.belief_fact = Ballot::zero();
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "a plain deployment matches no reconfiguration fact")]
fn a_plain_reconfiguration_fact_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.belief_fact = ballot(1, 0);
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "the active configuration is drawn from the node pool")]
fn a_configuration_outside_the_pool_fails_the_invariants() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    n.acceptors = cfg(&[0, 1, 9]);
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "a candidate holds exactly one open campaign phase")]
fn a_candidate_with_no_campaign_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.role = NodeRole::Candidate;
    n.assert_invariants();
}

/// A matchmaker campaign that names this node stays under the retirement
/// fence: the node's `last_member_ballot` covers the ballot it campaigns at.
/// The test lowers both fences, so only the campaign rule is broken.
#[test]
#[should_panic(expected = "a campaign that names this node is under its retirement fence")]
fn a_campaign_above_the_retirement_fence_fails_the_invariants() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    n.assert_invariants();
    n.last_member_ballot = Ballot::zero();
    n.registration_fence = Ballot::zero();
    n.assert_invariants();
}

/// A matchmaker deployment campaigns only on a belief it heard.
#[test]
#[should_panic(expected = "a matchmaker deployment campaigns only on a heard belief")]
fn a_campaign_on_the_bootstrap_belief_fails_the_invariants() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    campaign(&mut n);
    assert_eq!(n.role(), NodeRole::Candidate);
    n.belief_source = BeliefSource::Bootstrap;
    n.assert_invariants();
}

#[test]
#[should_panic(expected = "a leader knows itself as leader")]
fn a_leader_that_names_another_fails_the_invariants() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    nodes[0].leader = Some(NodeId(1));
    nodes[0].assert_invariants();
}

#[test]
#[should_panic(expected = "only a leader carries an inherited leadership origin")]
fn a_follower_with_an_inherited_origin_fails_the_invariants() {
    let mut n = node(0, &[0, 1, 2]);
    n.leadership_origin = LeadershipOrigin::Handoff { from: NodeId(1) };
    n.assert_invariants();
}

/// The marks of one node, read back on a node with a lower promise.
#[test]
#[should_panic(expected = "a node's promise never decreases")]
fn a_promise_that_falls_fails_the_marks() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let high = nodes[0].durable_marks();
    let low = node(1, &[0, 1, 2]);
    low.assert_marks_monotone(high);
}

// ---- the batch's own write-order checks -------------------------------------

#[test]
#[should_panic(expected = "a batch only ever raises the promise")]
fn a_batch_that_repeats_a_promise_fails_its_check() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let _ = drain(&mut nodes[0]);
    let promised = nodes[0].hard_state().max_promised_ballot;
    for _ in 0..2 {
        nodes[0]
            .pending_writes
            .push(WriteOp::Acceptor(AcceptorWrite::SetPromise(promised)));
    }
    let _ = nodes[0].ready().writes();
}

#[test]
#[should_panic(expected = "a batch only ever advances the chosen index")]
fn a_batch_that_repeats_a_chosen_index_fails_its_check() {
    let mut nodes = cluster_with_three_chosen();
    let _ = drain(&mut nodes[0]);
    for _ in 0..2 {
        nodes[0]
            .pending_writes
            .push(WriteOp::SetChosenIndex(Slot(1)));
    }
    let _ = nodes[0].ready().writes();
}

#[test]
#[should_panic(expected = "a batch only ever raises the floor")]
fn a_batch_that_repeats_a_trim_point_fails_its_check() {
    let mut nodes = cluster_with_three_chosen();
    let _ = drain(&mut nodes[0]);
    for _ in 0..2 {
        nodes[0].pending_writes.push(WriteOp::TrimmedTo {
            point: Slot(1),
            state: crate::journal::state::JournalState::default(),
        });
    }
    let _ = nodes[0].ready().writes();
}

/// A durable log with a silent hole under its chosen index refuses to boot.
#[test]
#[should_panic(expected = "every retained slot below the chosen prefix has a durable record")]
fn a_silent_hole_under_the_chosen_index_refuses_to_boot() {
    let nodes = cluster_with_three_chosen();
    let mut storage = MemStorage::from_node(&nodes[1]);
    storage.accepted.remove(&Slot(1));
    let _ = ColocatedNode::new(&storage, Limits::DEFAULT);
}

// ---- the view and the counters -----------------------------------------------

#[test]
fn the_view_reports_the_clock_and_the_matchmaker_pool() {
    let mut n = node(0, &[0, 1, 2]);
    assert_eq!(n.counters().election_gap_fills, 0);
    assert_eq!(n.counters().matchmaking_timeouts, 0);
    assert_eq!(n.counters().blocked_repairs, 0);
    n.set_election_timeout(7);
    assert_eq!(n.election_timeout(), 7);
    n.tick();
    n.tick();
    assert_eq!(n.election_elapsed(), 2);
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    assert_eq!(
        nodes[0].election_elapsed(),
        0,
        "a leader's clock is stopped"
    );
    let deployed = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 3);
    assert_eq!(
        deployed.matchmaker_pool(),
        &[MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)]
    );
}

/// The read basis takes a new fence at the same ballot, and refuses one
/// at a lower ballot.
#[test]
fn a_read_basis_moves_to_a_new_fence_at_the_same_ballot() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    n.learn_read_basis(cfg(&[0, 1, 2]), ballot(3, 1), Some(Slot(1)));
    n.learn_read_basis(cfg(&[0, 1, 2]), ballot(3, 1), Some(Slot(4)));
    assert_eq!(n.read_basis().and_then(|b| b.fence), Some(Slot(4)));
    n.learn_read_basis(cfg(&[0, 1, 2]), ballot(2, 1), Some(Slot(9)));
    assert_eq!(n.read_basis().and_then(|b| b.fence), Some(Slot(4)));
}

/// The matched reconfiguration fact survives a binding of the same
/// configuration at or above it, and goes with any other configuration.
#[test]
fn the_matched_fact_follows_its_configuration() {
    let mut n = deployed_node(0, &[0, 1, 2], &[0, 1, 2], 1);
    n.adopt_configuration(cfg(&[0, 1, 2]), ballot(3, 0));
    n.belief_fact = ballot(3, 0);
    n.adopt_configuration(cfg(&[0, 1, 2]), ballot(3, 0));
    assert_eq!(n.belief_fact, ballot(3, 0), "the same binding keeps it");
    n.adopt_configuration(cfg(&[0, 1, 2]), ballot(5, 0));
    assert_eq!(n.belief_fact, ballot(3, 0), "a later binding keeps it");
    n.adopt_configuration(cfg(&[0, 1]), ballot(6, 0));
    assert_eq!(
        n.belief_fact,
        Ballot::zero(),
        "another configuration drops it"
    );
}

// ---- the checks of #907 (core unit tests) and #1100 (contract) -------------

/// Agreement at the acceptor (P2c, #1100 (contract and assertions)): an
/// `Accept` at a higher ballot for a slot this node holds chosen carries
/// the chosen command. Another command is a detected safety violation.
#[test]
#[should_panic(expected = "an Accept for a chosen slot carries the chosen command")]
fn an_accept_for_a_chosen_slot_with_another_command_trips_the_node() {
    let mut nodes = cluster_with_three_chosen();
    assert!(nodes[1].replica.is_chosen(Slot(0)));
    nodes[1].step(accept(2, ballot(9, 2), 0, ucmd(1, 1, 99)));
}

/// The same `Accept` with the chosen command is a correct re-proposal
/// (P2c): it lands.
#[test]
fn an_accept_for_a_chosen_slot_with_the_chosen_command_lands() {
    let mut nodes = cluster_with_three_chosen();
    let chosen = nodes[1].replica.chosen_at(Slot(0)).cloned().unwrap();
    nodes[1].step(accept(2, ballot(9, 2), 0, chosen.clone()));
    assert_eq!(
        nodes[1].acceptor().record(Slot(0)),
        Some(&(ballot(9, 2), chosen))
    );
}

/// The read side of the chosen/record pair: a chosen slot whose record
/// carries another command breaks the check.
#[test]
#[should_panic(expected = "a chosen slot's record carries the chosen command")]
fn a_chosen_slot_whose_record_differs_breaks_the_check() {
    let mut nodes = cluster_with_three_chosen();
    let mut writes: Vec<WriteOp> = Vec::new();
    nodes[1].acceptor.set_promise(ballot(9, 2), &mut writes);
    nodes[1]
        .acceptor
        .record_accepted(Slot(2), ballot(9, 2), ucmd(1, 9, 99), &mut writes);
    nodes[1].assert_invariants();
}

/// The `None` arm: a plain deployment hears no GC floor.
#[test]
#[should_panic(expected = "a plain deployment hears no GC floor")]
fn a_plain_node_with_a_heard_floor_breaks_the_check() {
    let mut n = node(0, &[0, 1, 2]);
    n.gc_floor_heard = Some(ballot(1, 1));
    n.assert_invariants();
}

/// Only a handoff successor ages an inherited fence.
#[test]
#[should_panic(expected = "only a handoff successor ages an inherited fence")]
fn a_follower_aging_an_inherited_fence_breaks_the_check() {
    let mut n = node(0, &[0, 1, 2]);
    n.handoff_fence_elapsed = 1;
    n.assert_invariants();
}

/// A pull counts at a chosen index this node held.
#[test]
#[should_panic(expected = "a pull counts at a chosen index this node held")]
fn a_pull_above_the_chosen_index_breaks_the_check() {
    let mut n = node(0, &[0, 1, 2]);
    n.pull_index = Some(Slot(3));
    n.assert_invariants();
}

/// An open election's configuration is drawn from the pool.
#[test]
#[should_panic(expected = "an open election's configuration is drawn from the pool")]
fn an_election_outside_the_pool_breaks_the_check() {
    let mut n = node(0, &[0, 1, 2]);
    let outside = AcceptorConfig::new(
        vec![NodeId(0), NodeId(7), NodeId(8)],
        crate::membership::QuorumSystem::Majority,
    );
    let _ = n.proposer.open_phase1(
        crate::proposer::Campaign {
            me: Some(NodeId(0)),
            ballot: ballot(1, 0),
            config: outside.clone(),
            prior: vec![outside],
            from_slot: Slot(0),
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    n.assert_pooled_configurations();
}

// ---- one batch in flight (#1100 (contract and assertions)) ------------------

/// A released batch flies until the driver gives it back: an input in
/// between panics.
#[test]
#[should_panic(expected = "no input reaches a node while its batch is in flight")]
fn a_step_while_a_batch_flies_panics() {
    let mut n = node(0, &[0, 1, 2]);
    n.campaign();
    let _flight = n.ready().release();
    n.tick();
}

/// A call that changes nothing still refuses a node in flight: `propose` on
/// a follower returns early, before any invariant check, and the entry
/// assertion stops it first.
#[test]
#[should_panic(expected = "no input reaches a node while its batch is in flight")]
fn a_proposal_to_a_follower_in_flight_panics() {
    let mut n = node(0, &[0, 1, 2]);
    let _flight = n.ready().release();
    let _ = n.propose(entry(1, 0, 7));
}

/// No batch is drained while an earlier one flies.
#[test]
#[should_panic(expected = "no batch is drained while an earlier batch is in flight")]
fn a_ready_while_a_batch_flies_panics() {
    let mut n = node(0, &[0, 1, 2]);
    n.campaign();
    let _flight = n.ready().release();
    let _ = n.ready();
}

/// A flight goes back to the node that released it, once.
#[test]
#[should_panic(expected = "a flight is given back to the node that released it")]
fn a_flight_given_to_another_node_panics() {
    let mut a = node(0, &[0, 1, 2]);
    let mut b = node(1, &[0, 1, 2]);
    let flight = a.ready().release();
    let _other = b.ready().release();
    b.durable(flight);
}

/// One process serves many journals under one node id: a flight goes back
/// to the journal that released it.
#[test]
#[should_panic(expected = "a flight is given back to the journal that released it")]
fn a_flight_given_to_another_journal_of_the_node_panics() {
    use crate::journal::{JournalId, JournalIdentifier, TenantId};
    let mut a = node(0, &[0, 1, 2]);
    let config = crate::contract::config::Config::new(
        NodeId(0),
        JournalIdentifier::new(TenantId(1), JournalId(2)),
    )
    .with_peers([NodeId(0), NodeId(1), NodeId(2)]);
    let mut b = ColocatedNode::new(&MemStorage::new(config), Limits::DEFAULT);
    let flight = a.ready().release();
    let other = b.ready().release();
    assert_eq!(flight.batch, other.batch, "both journals number from zero");
    b.durable(flight);
}

/// A token of an earlier flight is stale.
#[test]
#[should_panic(expected = "a flight is given back once, while it is in flight")]
fn a_stale_flight_panics() {
    let mut n = node(0, &[0, 1, 2]);
    let first = n.ready().release();
    let copy = crate::node::Flight {
        node: first.node,
        journal: first.journal,
        batch: first.batch,
    };
    n.durable(first);
    n.durable(copy);
}

/// The whole path: release, persist, give back, then the node takes inputs
/// again, and each flight has its own number.
#[test]
fn a_landed_flight_lets_the_node_take_inputs() {
    let mut n = node(0, &[0, 1, 2]);
    n.campaign();
    let first = n.ready().release();
    assert!(n.in_flight());
    let number = first.batch;
    n.durable(first);
    assert!(!n.in_flight());
    n.tick();
    let second = n.ready().release();
    assert!(second.batch > number, "each flight has its own number");
    n.durable(second);
}
