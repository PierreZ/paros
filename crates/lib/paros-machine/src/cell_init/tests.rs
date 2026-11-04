//! The judgments of one decree ballot (#277, #246): the answers to the ask,
//! the promises folded into the decree, the members still intact, and each
//! answer to `FormCell`. The whole ballot over the network is in
//! `wait/tests.rs`.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{Ballot, NodeId};
use paros_model::tags::Tag;
use paros_wire::machine as wire;

use super::{
    Attempt, Formed, Phase1, Phase2, decree_over, fold_form, fold_promises, intact, judge_asks,
    majority,
};
use crate::{CellPlan, Class, Locality, ballot_to_wire, machine_tags};
use paros_model::address::Address;

fn addr(index: u64) -> Address {
    Address::parse(&format!("10.0.0.{index}:4500")).expect("a test address")
}

fn id(index: u64) -> NodeId {
    NodeId(100 + index)
}

fn ballot(round: u64) -> Ballot {
    Ballot { round, node: id(1) }
}

fn identity(node: u64, class: Class) -> wire::IdentifyAck {
    wire::IdentifyAck {
        node_id: node,
        tags: machine_tags(class, &Locality::default())
            .iter()
            .map(Tag::to_wire)
            .collect(),
        ..wire::IdentifyAck::default()
    }
}

/// Machine `index`'s promise, with no vote.
fn promise(index: u64) -> wire::PrepareCellAck {
    wire::PrepareCellAck {
        identity: Some(identity(id(index).0, Class::All)),
        promised: true,
        ..wire::PrepareCellAck::default()
    }
}

/// Machine `index`'s refusal under a higher promise at `round`.
fn nack(index: u64, round: u64) -> wire::PrepareCellAck {
    wire::PrepareCellAck {
        promised: false,
        promise: Some(ballot_to_wire(ballot(round))),
        ..promise(index)
    }
}

/// Machine `index`'s promise, reporting `plan` voted at `round`.
fn voted(index: u64, plan: &CellPlan, round: u64) -> wire::PrepareCellAck {
    wire::PrepareCellAck {
        vote: Some(plan.form_request(ballot(round))),
        ..promise(index)
    }
}

/// A plan of cell `cell` over machines `members` at their addresses.
fn plan(cell: u64, members: &[u64]) -> CellPlan {
    crate::rig::plan(cell, members.iter().map(|i| (id(*i), addr(*i))).collect())
}

fn asked(acks: Vec<(u64, wire::PrepareCellAck)>) -> Vec<(Address, Option<wire::PrepareCellAck>)> {
    acks.into_iter()
        .map(|(i, ack)| (addr(i), Some(ack)))
        .collect()
}

fn listed(members: &[u64]) -> Vec<Address> {
    members.iter().map(|i| addr(*i)).collect()
}

/// The decree at `round` over machines `members`, proposing `proposal`.
fn decree(
    round: u64,
    members: &[u64],
    proposal: CellPlan,
) -> paros_core::decree::Decree<NodeId, CellPlan> {
    let ids: BTreeSet<NodeId> = members.iter().map(|i| id(*i)).collect();
    decree_over(ballot(round), ids, proposal)
}

#[test]
fn a_majority_of_the_listed_machines_chooses() {
    assert_eq!(
        [1, 2, 3, 4, 5].map(majority),
        [1, 2, 2, 3, 3],
        "a strict majority"
    );
}

#[test]
fn every_listed_machine_answering_as_itself_passes_the_ask() {
    let asked = judge_asks(asked(vec![(1, promise(1)), (2, promise(2))])).expect("passes");
    assert_eq!(
        asked.identities,
        BTreeMap::from([(addr(1), id(1)), (addr(2), id(2))])
    );
    let ids: Vec<NodeId> = asked.promises.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![id(1), id(2)]);
}

#[test]
fn the_ask_refuses_each_bad_answer_with_its_label() {
    let refused = |refusal: &str| wire::PrepareCellAck {
        refusal: refusal.into(),
        ..promise(2)
    };
    let cases: Vec<(Option<wire::PrepareCellAck>, &str)> = vec![
        (None, "member_unreachable"),
        (Some(refused("stateless")), "stateless_member"),
        (Some(refused("in_cell")), "cell_exists"),
        (Some(refused("expired")), "expired"),
        (Some(refused("forbidden")), "forbidden"),
        (Some(refused("invalid_token")), "invalid_token"),
        (Some(refused("storage")), "member_unreachable"),
        (
            Some(wire::PrepareCellAck {
                identity: None,
                ..promise(2)
            }),
            "malformed",
        ),
        (
            Some(wire::PrepareCellAck {
                identity: Some(identity(id(2).0, Class::Stateless)),
                ..promise(2)
            }),
            "stateless_member",
        ),
        (
            Some(wire::PrepareCellAck {
                identity: Some(wire::IdentifyAck::default()),
                ..promise(2)
            }),
            "stateless_member",
        ),
        (
            Some(wire::PrepareCellAck {
                identity: Some(identity(0, Class::All)),
                ..promise(2)
            }),
            "malformed",
        ),
        // One machine at two listed addresses.
        (Some(promise(1)), "malformed"),
    ];
    for (answer, label) in cases {
        let answers = vec![(addr(1), Some(promise(1))), (addr(2), answer.clone())];
        assert_eq!(
            judge_asks(answers).err(),
            Some(label),
            "the answer {answer:?}"
        );
    }
}

#[test]
fn fresh_promises_select_the_drawn_plan() {
    let drawn = plan(9, &[1, 2, 3]);
    let mut decree = decree(10, &[1, 2, 3], drawn.clone());
    let promises = vec![
        (id(1), promise(1)),
        (id(2), promise(2)),
        (id(3), promise(3)),
    ];
    let Ok(Phase1::Selected(selected)) = fold_promises(&mut decree, promises, &listed(&[1, 2, 3]))
    else {
        panic!("selected");
    };
    assert_eq!(selected, drawn);
    assert!(!decree.adopted_prior_vote());
}

#[test]
fn every_member_must_promise_before_a_plan_is_selected() {
    let mut decree = decree(10, &[1, 2, 3], plan(9, &[1, 2, 3]));
    let promises = vec![(id(1), promise(1)), (id(2), promise(2))];
    assert_eq!(
        fold_promises(&mut decree, promises, &listed(&[1, 2, 3])).err(),
        Some("malformed"),
        "a Phase-1 quorum is every listed machine"
    );
}

#[test]
fn a_reported_vote_of_this_list_is_adopted() {
    let reported = plan(5, &[1, 2]);
    let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
    let promises = vec![(id(1), promise(1)), (id(2), voted(2, &reported, 4))];
    let Ok(Phase1::Selected(selected)) = fold_promises(&mut decree, promises, &listed(&[1, 2]))
    else {
        panic!("selected");
    };
    assert_eq!(
        selected, reported,
        "P2c: the reported plan, not the drawn one"
    );
    assert!(decree.adopted_prior_vote());
}

#[test]
fn of_two_reported_votes_the_highest_ballot_is_adopted() {
    let low = plan(5, &[1, 2]);
    let high = plan(6, &[1, 2]);
    let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
    let promises = vec![(id(1), voted(1, &high, 7)), (id(2), voted(2, &low, 4))];
    let Ok(Phase1::Selected(selected)) = fold_promises(&mut decree, promises, &listed(&[1, 2]))
    else {
        panic!("selected");
    };
    assert_eq!(selected, high);
}

#[test]
fn a_higher_promise_preempts_the_ballot() {
    let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
    let promises = vec![(id(1), promise(1)), (id(2), nack(2, 20))];
    let Ok(Phase1::Preempted(by)) = fold_promises(&mut decree, promises, &listed(&[1, 2])) else {
        panic!("preempted");
    };
    assert_eq!(by, ballot(20));
}

#[test]
fn a_vote_above_the_ballot_preempts_it() {
    let reported = plan(5, &[1, 2]);
    let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
    let promises = vec![(id(1), promise(1)), (id(2), voted(2, &reported, 30))];
    let Ok(Phase1::Preempted(by)) = fold_promises(&mut decree, promises, &listed(&[1, 2])) else {
        panic!("preempted");
    };
    assert_eq!(by, ballot(30));
}

#[test]
fn bad_promises_are_malformed() {
    let reported = plan(5, &[1, 2]);
    let other = plan(6, &[1, 2]);
    let broken_vote = wire::PrepareCellAck {
        vote: Some(wire::FormCell::default()),
        ..promise(2)
    };
    let cases = vec![
        // A refusal that names no higher promise.
        nack(2, 10),
        nack(2, 3),
        wire::PrepareCellAck {
            promise: None,
            ..nack(2, 20)
        },
        // A vote at this receiver's own fresh ballot.
        voted(2, &reported, 10),
        // A vote that does not parse.
        broken_vote,
    ];
    for ack in cases {
        let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
        let promises = vec![(id(1), promise(1)), (id(2), ack.clone())];
        assert_eq!(
            fold_promises(&mut decree, promises, &listed(&[1, 2])).err(),
            Some("malformed"),
            "the promise {ack:?}"
        );
    }
    // Two votes at one ballot for two plans break agreement.
    let mut decree = decree(10, &[1, 2], plan(9, &[1, 2]));
    let promises = vec![
        (id(1), voted(1, &reported, 4)),
        (id(2), voted(2, &other, 4)),
    ];
    assert_eq!(
        fold_promises(&mut decree, promises, &listed(&[1, 2])).err(),
        Some("malformed")
    );
}

#[test]
fn another_cells_vote_alone_refuses_the_list() {
    // Machine 3 voted in a cell over machines 3 and 4: another list.
    let foreign = plan(7, &[3, 4]);
    let mut decree = decree(10, &[1, 3], plan(9, &[1, 3]));
    let promises = vec![(id(1), promise(1)), (id(3), voted(3, &foreign, 4))];
    assert_eq!(
        fold_promises(&mut decree, promises, &listed(&[1, 3])).err(),
        Some("other_cell_init")
    );
}

#[test]
fn another_cells_vote_beside_this_lists_vote_is_passed_over() {
    // A wiped member's address now hosts a machine of another cell; the
    // others still report this list's plan, which is adopted.
    let ours = plan(5, &[1, 2, 3]);
    let foreign = plan(7, &[3, 4]);
    let mut decree = decree(10, &[1, 2, 3], plan(9, &[1, 2, 3]));
    let promises = vec![
        (id(1), voted(1, &ours, 4)),
        (id(2), promise(2)),
        (id(3), voted(3, &foreign, 40)),
    ];
    let Ok(Phase1::Selected(selected)) = fold_promises(&mut decree, promises, &listed(&[1, 2, 3]))
    else {
        panic!("selected");
    };
    assert_eq!(
        selected, ours,
        "another cell's higher vote preempts nothing"
    );
}

#[test]
fn intact_members_are_those_at_their_planned_address() {
    let planned = plan(5, &[1, 2, 3]);
    let all: BTreeMap<Address, NodeId> = (1..=3).map(|i| (addr(i), id(i))).collect();
    assert_eq!(
        intact(&planned, &all, 2),
        Ok(listed(&[1, 2, 3]).into_iter().collect())
    );
    // Machine 3 was wiped: a new machine answers at its address.
    let mut wiped = all.clone();
    wiped.insert(addr(3), NodeId(999));
    assert_eq!(
        intact(&planned, &wiped, 2),
        Ok(listed(&[1, 2]).into_iter().collect())
    );
    // A majority wiped: no plan can be chosen any more.
    wiped.insert(addr(2), NodeId(998));
    assert_eq!(intact(&planned, &wiped, 2), Err("cell_lost"));
}

fn formed() -> wire::FormCellAck {
    wire::FormCellAck {
        formed: true,
        ..wire::FormCellAck::default()
    }
}

fn refused_form(refusal: &str, promise: Option<u64>) -> wire::FormCellAck {
    wire::FormCellAck {
        formed: false,
        refusal: refusal.into(),
        promise: promise.map(|round| ballot_to_wire(ballot(round))),
    }
}

/// The decree over machines 1 to 3 in Phase 2, proposing `proposal`.
fn in_phase2(proposal: &CellPlan) -> paros_core::decree::Decree<NodeId, CellPlan> {
    let mut decree = decree(10, &[1, 2, 3], proposal.clone());
    let promises = (1..=3).map(|i| (id(i), promise(i))).collect();
    assert!(matches!(
        fold_promises(&mut decree, promises, &listed(&[1, 2, 3])),
        Ok(Phase1::Selected(_))
    ));
    decree
}

#[test]
fn a_majority_of_accepts_chooses_the_plan() {
    let proposal = plan(9, &[1, 2, 3]);
    let mut decree = in_phase2(&proposal);
    assert_eq!(
        fold_form(&mut decree, id(2), Some(formed())),
        Ok(Formed::Accepted(None))
    );
    assert_eq!(
        fold_form(&mut decree, id(3), Some(formed())),
        Ok(Formed::Accepted(Some(proposal)))
    );
}

#[test]
fn a_new_machine_at_a_members_address_is_no_accept() {
    let mut decree = in_phase2(&plan(9, &[1, 2, 3]));
    assert_eq!(
        fold_form(&mut decree, id(2), Some(refused_form("not_a_member", None))),
        Ok(Formed::Not)
    );
    assert!(decree.preempted().is_none());
}

#[test]
fn a_higher_promise_in_phase2_preempts_the_decree() {
    let mut decree = in_phase2(&plan(9, &[1, 2, 3]));
    assert_eq!(
        fold_form(
            &mut decree,
            id(2),
            Some(refused_form("promised_higher", Some(50)))
        ),
        Ok(Formed::Not)
    );
    assert_eq!(decree.preempted(), Some(ballot(50)));
}

#[test]
fn a_bad_form_answer_refuses_with_its_label() {
    let cases = [
        (None, "member_unreachable"),
        (Some(refused_form("promised_higher", Some(10))), "malformed"),
        (Some(refused_form("promised_higher", None)), "malformed"),
        (Some(refused_form("other_cell", None)), "other_cell_init"),
        (Some(refused_form("storage", None)), "member_unreachable"),
        (Some(refused_form("forbidden", None)), "forbidden"),
        (Some(refused_form("expired", None)), "expired"),
        (Some(refused_form("what", None)), "malformed"),
    ];
    for (answer, label) in cases {
        let mut decree = in_phase2(&plan(9, &[1, 2, 3]));
        assert_eq!(
            fold_form(&mut decree, id(2), answer.clone()),
            Err(label),
            "the answer {answer:?}"
        );
    }
}

#[test]
fn phase2_counts_accepts_from_none_to_a_choice() {
    let proposal = plan(9, &[1, 2, 3]);
    let identities: BTreeMap<Address, NodeId> = (1..=3).map(|i| (addr(i), id(i))).collect();
    // No answer yet: short of a quorum.
    let none = Phase2::new(in_phase2(&proposal), &identities);
    assert!(matches!(none.end(&proposal, 2), Attempt::Short));
    // One accept of three: still short.
    let mut one = Phase2::new(in_phase2(&proposal), &identities);
    assert!(one.fold(vec![(addr(2), Some(formed()))]).is_ok());
    assert_eq!(one.accepted, 1);
    assert!(matches!(one.end(&proposal, 2), Attempt::Short));
    // Two accepts of three: chosen.
    let mut two = Phase2::new(in_phase2(&proposal), &identities);
    assert!(
        two.fold(vec![(addr(2), Some(formed())), (addr(3), Some(formed()))])
            .is_ok()
    );
    assert!(matches!(two.end(&proposal, 2), Attempt::Chosen(p) if p == proposal));
}

#[test]
fn phase2_ends_on_a_refusal_or_a_higher_promise() {
    let proposal = plan(9, &[1, 2, 3]);
    let identities: BTreeMap<Address, NodeId> = (1..=3).map(|i| (addr(i), id(i))).collect();
    let mut refused = Phase2::new(in_phase2(&proposal), &identities);
    assert!(matches!(
        refused.fold(vec![(addr(2), None)]),
        Err(Attempt::Failed("member_unreachable"))
    ));
    let mut preempted = Phase2::new(in_phase2(&proposal), &identities);
    let higher = Some(refused_form("promised_higher", Some(50)));
    assert!(matches!(
        preempted.fold(vec![(addr(2), higher)]),
        Err(Attempt::Preempted(b)) if b == ballot(50)
    ));
}
