//! The local protocol assertions, each driven to its trip point through the
//! public API: a message that a correct peer can never send lands on the
//! assertion that names the rule it breaks, and the rejection paths leave the
//! state they refused to touch untouched.
//!
//! Every negative test here is a *programmer/protocol* invariant, not an
//! operating condition: the inputs are ones only a broken proposer (two
//! commands under one ballot, a decision that contradicts an earlier one) could
//! produce, which is exactly what crash-beats-corruption is for.

#[allow(clippy::wildcard_imports)]
use super::*;

fn accept(from: u64, b: Ballot, slot: u64, command: Command) -> Message {
    Message::Accept {
        reply_to: NodeId(from),
        leader: NodeId(from),
        ballot: b,
        slot: Slot(slot),
        command,
    }
}

fn commit(from: u64, b: Ballot, slot: u64, command: Command) -> Message {
    Message::Commit {
        from: NodeId(from),
        ballot: b,
        slot: Slot(slot),
        command,
    }
}

fn promise(from: u64, b: Ballot, accepted: BTreeMap<Slot, (Ballot, Command)>) -> Message {
    Message::Promise {
        from: NodeId(from),
        ballot: b,
        from_slot: Slot(0),
        accepted,
        faulty: BTreeMap::new(),
        next_from_slot: None,
    }
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
/// record's ballot may drop to the choosing ballot, the command stays.
#[test]
fn a_lower_ballot_decision_with_the_same_command_is_learned() {
    let mut n = node(0, &[0, 1, 2]);
    n.step(accept(1, ballot(2, 1), 0, ucmd(1, 1, 10)));
    drain(&mut n);
    n.step(commit(2, ballot(1, 2), 0, ucmd(1, 1, 10)));
    assert_eq!(chosen_at(&n, 0), Some(val(10)));
    assert_eq!(n.acceptor().records()[&Slot(0)].0, ballot(1, 2));
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
        leader: NodeId(1),
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
    let _ = nodes[0].propose(ClientId(1), ClientSeq(4), val(40));
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
    n.set_election_timeout(1);
    n.tick();
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
    n.set_election_timeout(1);
    n.tick();
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

// ---- truncation and snapshot boundaries --------------------------------------

/// An open application repair pins the floor below its cursor: the records it
/// still needs are never truncated underneath it.
#[test]
fn compaction_stops_below_an_open_application_repair() {
    let nodes = cluster_with_three_chosen();
    let mut storage = TestStorage::from_node(&nodes[1]);
    storage.rot(Slot(1));
    let mut n = ColocatedNode::new(&storage);
    n.open_app_repair(Slot(1));
    assert_eq!(
        n.replica().app_repair(),
        Some(Slot(1)),
        "the rotted slot keeps the repair open"
    );
    let floor = n.compact(Slot(2));
    assert_eq!(floor, Slot(1), "the floor stops at the repair cursor");
    assert_eq!(n.acceptor().first_slot(), Slot(1));
    assert_eq!(n.replica().app_repair(), Some(Slot(1)));
}

/// A snapshot behind the prefix is an operating condition (a stale offer):
/// ignored whole, with no frontier moving backwards.
#[test]
fn a_stale_snapshot_never_rewinds_a_frontier() {
    let mut nodes = cluster_with_three_chosen();
    let before = nodes[1].hard_state();
    let floor = nodes[1].acceptor().first_slot();
    nodes[1].step(Message::InstallSnapshot {
        from: NodeId(0),
        ballot: ballot(1, 0),
        chosen_index: Slot(0),
        snapshot: Value(vec![]),
        sessions: Vec::new(),
    });
    assert_eq!(nodes[1].hard_state(), before);
    assert_eq!(nodes[1].acceptor().first_slot(), floor);
    assert!(nodes[1].pending_writes.is_empty());
}

/// The immediate `Chosen` names the identity's own applied slot.
#[test]
fn an_immediate_chosen_names_the_identitys_applied_slot() {
    let mut nodes = cluster_with_three_chosen();
    assert_eq!(
        nodes[0].propose(ClientId(1), ClientSeq(2), val(20)),
        ProposeResult::Chosen(Slot(1))
    );
}
