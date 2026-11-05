//! A formed member answers the decree from its record (#277): its vote at
//! any ballot, a preemption below it, another cell's plan refused, and an
//! admission into its own cell acked. The teacher's catch-up over the
//! network is in `lifecycle/tests.rs`.

use paros_core::{Ballot, NodeId};
use paros_wire::machine as wire;
use tokio::sync::watch;

use super::{FormedCell, FormedExt as _, Learner, Teacher, token_refused, vote_ballot};
use crate::rig::{facts, machine as member, machine_addr as addr, plan};
use crate::{Admission, CachedRegistry, Class, ballot_to_wire};
use paros_model::address::{Address, Names};

/// Machine 1, formed in cell 9 over machines 1 and 2 at round 20.
fn formed() -> FormedCell {
    FormedCell {
        facts: facts(NodeId(1), addr(1), Class::All),
        plan: plan(9, vec![member(1), member(2)]),
        ballot: Ballot {
            round: 20,
            node: NodeId(1),
        },
        cached: None,
        token: None,
    }
}

fn at(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(2),
    }
}

#[test]
fn a_formed_member_promises_any_ballot_and_reports_its_vote() {
    let cell = formed();
    let ack = cell.prepare_ack();
    assert!(ack.promised);
    assert_eq!(ack.refusal, "");
    assert_eq!(ack.identity.as_ref().map(|i| i.cell_id), Some(9));
    let vote = ack.vote.expect("its vote");
    assert_eq!(vote_ballot(&vote), Some(cell.ballot));
    assert_eq!(crate::CellPlan::from_form(&vote), Ok(cell.plan));
}

#[test]
fn a_formed_member_acks_its_own_plan_at_any_ballot() {
    let cell = formed();
    for round in [1, 20, 99] {
        let ack = cell.form_ack(&cell.plan.form_request(at(round)));
        assert!(ack.formed, "round {round}");
        assert_eq!(ack.refusal, "");
    }
}

#[test]
fn another_plan_below_the_vote_is_a_preemption() {
    let cell = formed();
    let other = plan(8, vec![member(1), member(2)]);
    let ack = cell.form_ack(&other.form_request(at(19)));
    assert!(!ack.formed);
    assert_eq!(ack.refusal, "promised_higher");
    assert_eq!(ack.promise, Some(ballot_to_wire(cell.ballot)));
}

#[test]
fn another_plan_at_or_above_the_vote_is_another_cells() {
    let cell = formed();
    let other = plan(8, vec![member(1), member(2)]);
    for ballot in [cell.ballot, at(21)] {
        let ack = cell.form_ack(&other.form_request(ballot));
        assert!(!ack.formed);
        assert_eq!(ack.refusal, "other_cell");
        assert_eq!(ack.promise, None);
    }
}

#[test]
fn a_malformed_form_is_refused() {
    let ack = formed().form_ack(&wire::FormCell::default());
    assert!(!ack.formed);
    assert_eq!(ack.refusal, "malformed");
}

#[test]
fn an_admission_into_its_own_cell_is_acked_and_any_other_refused() {
    let cell = formed();
    let ours = Admission {
        cell: cell.plan.control_journals(),
        members: vec![member(1), member(2), member(3)],
        founders: vec![NodeId(1), NodeId(2)],
    };
    let ack = cell.admit_ack(&ours.to_wire());
    assert!(ack.admitted && ack.in_cell, "{ack:?}");
    assert_eq!(ack.refusal, "");
    let theirs = Admission {
        cell: plan(8, vec![member(5)]).control_journals(),
        ..ours
    };
    let ack = cell.admit_ack(&theirs.to_wire());
    assert!(!ack.admitted && !ack.in_cell);
    assert_eq!(ack.refusal, "other_cell");
    let ack = cell.admit_ack(&wire::Admit::default());
    assert!(!ack.admitted && !ack.in_cell);
    assert_eq!(ack.refusal, "malformed");
}

#[test]
fn only_a_pin_refusal_ends_the_teaching_of_a_member() {
    for label in ["invalid_token", "expired", "forbidden"] {
        assert!(token_refused(label), "{label}");
    }
    for label in ["", "promised_higher", "storage", "other_cell"] {
        assert!(!token_refused(label), "{label}");
    }
}

fn teacher(books: watch::Receiver<Option<CachedRegistry>>) -> Teacher {
    Teacher {
        cell: formed(),
        names: Names::literal(),
        books,
        period: std::time::Duration::from_secs(1),
        patience: std::time::Duration::from_secs(1),
    }
}

#[test]
fn the_teacher_reads_each_identify_answer() {
    let (_tx, books) = watch::channel(None);
    let teacher = teacher(books);
    let me = facts(NodeId(2), addr(2), Class::All);
    let idle = me.identify_ack(0);
    assert_eq!(teacher.learner(NodeId(2), None), Learner::Unknown);
    assert_eq!(teacher.learner(NodeId(2), Some(&idle)), Learner::Idle);
    assert_eq!(
        teacher.learner(NodeId(2), Some(&me.identify_ack(9))),
        Learner::Formed
    );
    assert_eq!(
        teacher.learner(NodeId(2), Some(&me.identify_ack(8))),
        Learner::Other,
        "in another cell"
    );
    let wiped = facts(NodeId(7), addr(2), Class::All).identify_ack(0);
    assert_eq!(
        teacher.learner(NodeId(2), Some(&wiped)),
        Learner::Other,
        "another machine at the member's address"
    );
    let stateless = facts(NodeId(2), addr(2), Class::Stateless).identify_ack(0);
    assert_eq!(teacher.learner(NodeId(2), Some(&stateless)), Learner::Other);
}

#[test]
fn the_teacher_drops_a_member_the_book_names_elsewhere() {
    let (tx, books) = watch::channel(None);
    let mut teacher = teacher(books);
    let mut pending = std::collections::BTreeMap::from([
        (Address::from(addr(2)), NodeId(2)),
        (Address::from(addr(3)), NodeId(3)),
    ]);
    teacher.moved_away(&mut pending);
    assert_eq!(pending.len(), 2, "no book yet: nobody moved");
    tx.send(Some(CachedRegistry {
        node: NodeId(1),
        position: 4,
        machines: vec![member(1), (NodeId(2), Address::from(addr(12))), member(3)],
    }))
    .expect("sent");
    teacher.moved_away(&mut pending);
    assert_eq!(
        pending,
        std::collections::BTreeMap::from([(Address::from(addr(3)), NodeId(3))]),
        "machine 2 registered at another address: it formed"
    );
}

#[test]
#[should_panic(expected = "a formed member voted at a ballot")]
fn a_formed_member_holds_a_vote_at_a_set_ballot() {
    let mut cell = formed();
    cell.ballot.round = 0;
    let _ = cell.prepare_ack();
}

#[test]
#[should_panic(expected = "a formed plan names its cell")]
fn a_teacher_teaches_a_plan_that_names_its_cell() {
    let (_tx, books) = watch::channel(None);
    let mut teacher = teacher(books);
    teacher.cell.plan.cell_id = 0;
    let _ = teacher.learner(NodeId(2), None);
}
