//! The machine reports' oracles on a hand-built board: an honest boot,
//! record and formatting stay clean, and each broken one goes red.

use std::sync::Mutex;

use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
use paros::core::{Ballot, NodeId};
use paros::machine::{Admission, CellPlan, Class, Locality, MachineRecord};
use paros::model::address::Address;

use super::{booted, formatting, judge_minted, recorded};
use crate::audit::testing::red;
use crate::machine::board::{MachineBoard, lock};

fn addr(n: u8) -> Address {
    Address::parse(&format!("10.0.0.{n}:4000")).expect("an address")
}

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn plan(cell_id: u64, members: &[(u64, u8)]) -> CellPlan {
    CellPlan {
        cell_id,
        members: members
            .iter()
            .map(|&(id, a)| (NodeId(id), addr(a)))
            .collect(),
        control: identifier(cell_id, 1),
        fleet: None,
        election: identifier(cell_id, 2),
        journals: vec![identifier(cell_id, 1), identifier(cell_id, 2)],
    }
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

fn record(node: u64) -> MachineRecord {
    MachineRecord {
        node_id: NodeId(node),
        class: Class::All,
        locality: Locality::default(),
        name: format!("m-{node}"),
        promised: Ballot::default(),
        plan: None,
        admitted: None,
        created: Vec::new(),
    }
}

/// A board whose `init` listed founders at addresses 1 and 2.
fn board() -> Mutex<MachineBoard> {
    Mutex::new(MachineBoard {
        init_sent: true,
        founders: [addr(1), addr(2)].into(),
        ..MachineBoard::default()
    })
}

#[test]
fn a_minted_id_is_new_to_the_run() {
    let board = board();
    recorded(&board, &addr(1), &record(11));
    let b = lock(&board);
    assert!(
        !red(|| judge_minted(&b, &addr(1), 11)),
        "the same machine again"
    );
    assert!(!red(|| judge_minted(&b, &addr(2), 12)));
    assert!(
        red(|| judge_minted(&b, &addr(2), 11)),
        "another machine's id"
    );
    drop(b);
    booted(&board, &addr(1), None);
    let b = lock(&board);
    assert!(b.nodes.is_empty() && b.wiped.contains(&(addr(1), 11)));
    assert!(red(|| judge_minted(&b, &addr(1), 11)), "a wiped id");
    assert!(!red(|| judge_minted(&b, &addr(1), 13)));
}

#[test]
fn a_machine_boots_with_the_id_it_minted() {
    let board = board();
    recorded(&board, &addr(1), &record(11));
    assert!(!red(|| booted(&board, &addr(1), Some(&record(11)))));
    assert!(red(|| booted(&board, &addr(1), Some(&record(12)))));
    assert!(
        !red(|| booted(&board, &addr(3), Some(&record(12)))),
        "a first boot"
    );
    assert!(
        !red(|| booted(&board, &addr(4), None)),
        "a wipe of no machine"
    );
}

#[test]
fn a_boot_on_a_vote_it_never_reported_records_it() {
    let board = board();
    let mut voted = record(11);
    voted.plan = Some((ballot(1, 11), plan(7, &[(11, 1), (12, 2)])));
    booted(&board, &addr(1), Some(&voted));
    let b = lock(&board);
    assert!(b.voters.contains_key(&11) && b.formed.contains(&11));
}

#[test]
fn a_recorded_promise_never_falls() {
    let board = board();
    let mut promised = record(11);
    promised.promised = ballot(3, 11);
    assert!(!red(|| recorded(&board, &addr(1), &promised)));
    promised.promised = ballot(4, 12);
    assert!(!red(|| recorded(&board, &addr(1), &promised)));
    promised.promised = ballot(2, 12);
    assert!(red(|| recorded(&board, &addr(1), &promised)));
    assert_eq!(lock(&board).first_promiser, Some(11));
}

#[test]
fn every_machine_forms_the_one_cell_init_drew() {
    let board = board();
    let one = plan(7, &[(11, 1), (12, 2)]);
    let mut first = record(11);
    first.plan = Some((ballot(1, 11), one.clone()));
    assert!(!red(|| recorded(&board, &addr(1), &first)));
    let mut second = record(12);
    second.plan = Some((ballot(2, 12), one));
    assert!(!red(|| recorded(&board, &addr(2), &second)));
    assert_eq!(lock(&board).votes[&7].len(), 2, "two ballots of one plan");
    let mut other = record(12);
    other.plan = Some((ballot(3, 12), plan(8, &[(11, 1), (12, 2)])));
    assert!(red(|| recorded(&board, &addr(2), &other)));
}

#[test]
fn an_admission_names_a_formed_cell_and_holds_no_vote() {
    let board = board();
    let cell = plan(7, &[(11, 1), (12, 2)]);
    let mut admitted = record(13);
    admitted.admitted = Some(Admission {
        cell: cell.control_journals(),
        members: cell.members.clone(),
        initial_members: vec![NodeId(11), NodeId(12)],
    });
    assert!(
        red(|| recorded(&board, &addr(3), &admitted)),
        "no cell formed yet"
    );
    let mut founder = record(11);
    founder.plan = Some((ballot(1, 11), cell.clone()));
    recorded(&board, &addr(1), &founder);
    assert!(!red(|| recorded(&board, &addr(3), &admitted)));
    assert!(lock(&board).admitted.contains_key(&13));
    let mut promised = admitted.clone();
    promised.node_id = NodeId(14);
    promised.promised = ballot(1, 14);
    assert!(red(|| recorded(&board, &addr(4), &promised)));
}

#[test]
fn only_the_founders_form_and_only_after_init() {
    let board = board();
    let founders = plan(7, &[(11, 1), (12, 2)]);
    assert!(!red(|| formatting(&board, &addr(1), NodeId(11), &founders)));
    assert!(lock(&board).governs.contains_key(&founders.election));
    assert!(
        red(|| formatting(&board, &addr(3), NodeId(13), &founders)),
        "not a founder"
    );
    let short = plan(7, &[(11, 1)]);
    assert!(
        red(|| formatting(&board, &addr(1), NodeId(11), &short)),
        "not every founder"
    );
    let early = Mutex::new(MachineBoard {
        founders: [addr(1), addr(2)].into(),
        ..MachineBoard::default()
    });
    assert!(
        red(|| formatting(&early, &addr(1), NodeId(11), &founders)),
        "before init"
    );
}

#[test]
fn the_other_cell_is_judged_apart() {
    let board = board();
    lock(&board).other_founder = Some(addr(5));
    let other = plan(8, &[(15, 5)]);
    assert!(
        !red(|| formatting(&board, &addr(5), NodeId(15), &other)),
        "the other cell's founder is no founder of the run"
    );
    let mut founder = record(11);
    founder.plan = Some((ballot(1, 11), plan(7, &[(11, 1), (12, 2)])));
    recorded(&board, &addr(1), &founder);
    let mut vote = record(15);
    vote.plan = Some((ballot(1, 15), other));
    assert!(!red(|| recorded(&board, &addr(5), &vote)));
    assert!(lock(&board).others.contains_key(&15));
    let mut same = record(15);
    same.plan = Some((ballot(2, 15), plan(7, &[(15, 5)])));
    assert!(
        red(|| recorded(&board, &addr(5), &same)),
        "never the run's cell"
    );
}
