//! The durable-claim oracles (#458 (core oracles)) on hand-built state:
//! each honest claim stays clean, each false claim goes red, and each
//! excuse holds only in its own case.

use std::collections::BTreeMap;

use paros::core::journal::{Command, Control, Entry, LeaderUuid, Seq};
use paros::core::membership::{AcceptorConfig, QuorumSystem};
use paros::core::{Ballot, NodeId, Slot, Value};
use paros::roles::node::command_hash;

use super::PromisePage;
use crate::audit::state::AuditState;
use crate::audit::testing::red;
use crate::excuses::Excuse;

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().map(|m| NodeId(*m)).collect(),
        QuorumSystem::Majority,
    )
}

/// A journal of three acceptors on plain Multi-Paxos.
fn three() -> AuditState {
    AuditState {
        bootstrap: Some(majority(&[0, 1, 2])),
        ..AuditState::default()
    }
}

const NOOP: Command = Command::Control(Control::Noop);

fn write() -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(0),
        records: vec![Value(b"x".to_vec())],
    })
}

/// `node` durably accepts `command` at `slot` and `b`, as the node audit
/// folds a live report: the tally, and the record it holds now.
fn hold(st: &mut AuditState, node: u64, slot: u64, b: Ballot, command: &Command) {
    let vhash = command_hash(command);
    st.observe_durable_accept(node, slot, b, vhash);
    st.held.insert((node, slot), (b.round, b.node.0, vhash));
}

/// The records of a `Promise` page.
fn page(entries: &[(u64, Ballot, Command)]) -> BTreeMap<Slot, (Ballot, Command)> {
    entries
        .iter()
        .map(|(slot, eb, command)| (Slot(*slot), (*eb, command.clone())))
        .collect()
}

fn judge(
    st: &mut AuditState,
    node: u64,
    b: Ballot,
    from_slot: u64,
    accepted: &BTreeMap<Slot, (Ballot, Command)>,
    next: Option<u64>,
) {
    st.observe_promise_content(
        node,
        &PromisePage {
            ballot: b,
            from_slot: Slot(from_slot),
            accepted,
            next_from_slot: next.map(Slot),
        },
    );
}

#[test]
fn a_promise_reports_only_records_its_sender_held() {
    let mut st = three();
    hold(&mut st, 1, 3, ballot(2, 0), &NOOP);
    let promised = ballot(4, 2);
    let honest = page(&[(3, ballot(2, 0), NOOP)]);
    assert!(!red(|| judge(&mut st, 1, promised, 0, &honest, None)));
    let unheld = page(&[(3, ballot(2, 0), NOOP), (5, ballot(2, 0), NOOP)]);
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &unheld, None)),
        "a record the sender never held"
    );
    let other = page(&[(3, ballot(2, 0), write())]);
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &other, None)),
        "another value at the held ballot"
    );
    assert!(
        red(|| judge(&mut st, 2, promised, 0, &honest, None)),
        "another node's record"
    );
    hold(&mut st, 1, 4, ballot(5, 0), &NOOP);
    let above = page(&[(4, ballot(5, 0), NOOP)]);
    assert!(
        red(|| judge(&mut st, 1, promised, 4, &above, None)),
        "a record above the promised ballot"
    );
    assert!(st.claim_excuses.is_empty());
}

#[test]
fn a_promise_record_below_the_pruned_tally_is_excused_by_the_kept_value() {
    let mut st = three();
    hold(&mut st, 1, 3, ballot(2, 0), &NOOP);
    hold(&mut st, 1, 6, ballot(2, 0), &NOOP);
    st.accept_sets.clear();
    st.pruned_floor = 5;
    let promised = ballot(4, 2);
    let pruned = page(&[(3, ballot(2, 0), NOOP)]);
    assert!(!red(|| judge(&mut st, 1, promised, 0, &pruned, Some(4))));
    assert!(st.claim_excuses.contains(&Excuse::PromiseTallyPruned));
    st.claim_excuses.clear();
    let other = page(&[(3, ballot(2, 0), write())]);
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &other, Some(4))),
        "a value the tally never named"
    );
    let above_floor = page(&[(6, ballot(2, 0), NOOP)]);
    assert!(
        red(|| judge(&mut st, 1, promised, 6, &above_floor, None)),
        "a record above the pruned floor with no holder"
    );
    assert!(st.claim_excuses.is_empty());
}

#[test]
fn a_promise_reports_every_held_record_below_its_ballot_in_its_range() {
    let mut st = three();
    hold(&mut st, 1, 3, ballot(2, 0), &NOOP);
    hold(&mut st, 1, 7, ballot(2, 0), &NOOP);
    let promised = ballot(4, 2);
    let empty = page(&[]);
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &empty, None)),
        "a held record left out"
    );
    let first = page(&[(3, ballot(2, 0), NOOP)]);
    assert!(
        !red(|| judge(&mut st, 1, promised, 0, &first, Some(5))),
        "slot 7 lies past the page"
    );
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &first, None)),
        "the last page owes slot 7"
    );
    assert!(
        !red(|| judge(
            &mut st,
            1,
            promised,
            4,
            &page(&[(7, ballot(2, 0), NOOP)]),
            None
        )),
        "slot 3 lies before the page"
    );
    // A record at or above the promised ballot was accepted after it.
    hold(&mut st, 1, 9, ballot(4, 2), &NOOP);
    hold(&mut st, 1, 10, ballot(5, 0), &NOOP);
    assert!(!red(|| judge(
        &mut st,
        1,
        promised,
        4,
        &page(&[(7, ballot(2, 0), NOOP)]),
        None
    )));
}

#[test]
fn a_promise_older_than_a_learned_chosen_record_of_its_batch_is_excused() {
    let mut st = three();
    let promised = ballot(4, 2);
    let empty = page(&[]);
    // A `Commit` stepped after the `Prepare` in the same batch: the
    // record lands at its choosing ballot, below the promise.
    hold(&mut st, 1, 3, ballot(2, 0), &NOOP);
    st.batch_accepts.entry(1).or_default().insert(3);
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &empty, None)),
        "a record of the batch that is not decided"
    );
    hold(&mut st, 0, 3, ballot(2, 0), &NOOP);
    assert_eq!(st.decided_vhash(3), Some(command_hash(&NOOP)));
    assert!(!red(|| judge(&mut st, 1, promised, 0, &empty, None)));
    assert!(st.claim_excuses.contains(&Excuse::PromiseLateLearn));
    st.claim_excuses.clear();
    st.batch_accepts.clear();
    assert!(
        red(|| judge(&mut st, 1, promised, 0, &empty, None)),
        "a decided record of an earlier batch"
    );
    assert!(st.claim_excuses.is_empty());
}

#[test]
fn a_plain_leadership_stands_on_a_phase1_quorum_of_promises() {
    let mut st = three();
    let won = ballot(2, 0);
    let config = majority(&[0, 1, 2]);
    assert!(
        red(|| st.observe_plain_election(0, won, &config)),
        "the candidate's own promise alone"
    );
    st.observe_promise_send(1, ballot(1, 0));
    assert!(
        red(|| st.observe_plain_election(0, won, &config)),
        "a promise at another ballot"
    );
    st.observe_promise_send(7, won);
    assert!(
        red(|| st.observe_plain_election(0, won, &config)),
        "a promise from outside the configuration"
    );
    st.observe_promise_send(1, won);
    assert!(!red(|| st.observe_plain_election(0, won, &config)));
}

/// `node` sends an `Accept` for `slot` at `b`, then starts two batches:
/// the claim is judged once a whole batch was reported after it.
fn accept_then_two_batches(st: &mut AuditState, node: u64, b: Ballot, slot: u64, vhash: u64) {
    st.observe_accept_record(node, b, slot, vhash);
    st.judge_accept_claims(node);
    st.judge_accept_claims(node);
}

#[test]
fn an_accept_names_a_round_its_leader_recorded() {
    let mut st = three();
    let b = ballot(2, 0);
    let noop = command_hash(&NOOP);
    st.observe_promise(0, b);
    assert!(
        red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)),
        "no record, the promise at the ballot"
    );
    hold(&mut st, 0, 3, b, &NOOP);
    assert!(!red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)));
    assert!(
        red(|| accept_then_two_batches(&mut st, 0, b, 3, command_hash(&write()))),
        "another command"
    );
    assert!(
        red(|| accept_then_two_batches(&mut st, 1, b, 3, noop)),
        "another node's record"
    );
    assert!(st.claim_excuses.is_empty());
}

#[test]
fn an_accept_sent_before_its_flush_waits_for_the_batch() {
    let mut st = three();
    let b = ballot(2, 0);
    let noop = command_hash(&NOOP);
    st.observe_promise(0, b);
    // #578: the `Accept` leaves, then its batch starts and reports the
    // record.
    assert!(!red(|| {
        st.observe_accept_record(0, b, 3, noop);
        st.judge_accept_claims(0);
    }));
    hold(&mut st, 0, 3, b, &NOOP);
    assert!(!red(|| st.judge_accept_claims(0)));
    assert!(!st.accept_claims.contains_key(&0));
    // A crash before the flush: the boot drops the claim.
    st.observe_accept_record(0, b, 4, noop);
    st.judge_accept_claims(0);
    st.drop_accept_claims(0);
    assert!(!red(|| st.judge_accept_claims(0)));
}

#[test]
fn an_accept_whose_leader_promise_moved_past_its_ballot_is_excused() {
    let mut st = three();
    let b = ballot(2, 0);
    let noop = command_hash(&NOOP);
    st.observe_promise(0, ballot(3, 1));
    assert!(!red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)));
    assert_eq!(
        st.claim_excuses.iter().copied().collect::<Vec<_>>(),
        vec![Excuse::AcceptPromiseMoved]
    );
    let mut equal = three();
    equal.observe_promise(0, b);
    assert!(
        red(|| accept_then_two_batches(&mut equal, 0, b, 3, noop)),
        "a promise at the ballot records the round"
    );
    // A promise raised after the send excuses nothing.
    let mut later = three();
    later.observe_promise(0, b);
    assert!(red(|| {
        later.observe_accept_record(0, b, 3, noop);
        later.observe_promise(0, ballot(3, 1));
        later.judge_accept_claims(0);
        later.judge_accept_claims(0);
    }));
    assert!(later.claim_excuses.is_empty());
}

#[test]
fn an_accept_for_a_slot_its_batch_compacted_is_excused() {
    let mut st = three();
    let b = ballot(2, 0);
    let noop = command_hash(&NOOP);
    st.observe_promise(0, b);
    hold(&mut st, 0, 3, b, &NOOP);
    st.accept_sets.clear();
    st.pruned_floor = 5;
    assert!(
        red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)),
        "pruned, but never decided"
    );
    st.decided_below_floor.insert(3, noop);
    assert!(!red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)));
    assert!(st.claim_excuses.contains(&Excuse::AcceptTallyPruned));
    st.claim_excuses.clear();
    st.pruned_floor = 3;
    assert!(
        red(|| accept_then_two_batches(&mut st, 0, b, 3, noop)),
        "at the pruned floor the tally is live"
    );
}
