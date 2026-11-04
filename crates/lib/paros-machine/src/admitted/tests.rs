//! An admitted machine (#216, #505 (stranded admitted machine)) acks its
//! own admission again, adopts a newer book an `Admit` carries, refuses
//! another cell's, and refuses the cell decree as a machine in a cell.

use paros_core::NodeId;
use paros_wire::machine as wire;
use tokio::sync::watch;

use super::AdmittedMachine;
use crate::rig::{facts, machine as member, machine_addr as addr, plan};
use crate::{Admission, CacheSink, CachedRegistry, Class};
use paros_model::address::Address;

/// Machine 3, admitted into cell 9 founded by machines 1 and 2.
fn admitted(cached: Option<CachedRegistry>) -> AdmittedMachine {
    AdmittedMachine {
        facts: facts(NodeId(3), addr(3), Class::All),
        admission: Admission {
            cell: plan(9, vec![member(1), member(2)]).control_journals(),
            members: vec![member(1), member(2), member(3)],
            founders: vec![NodeId(1), NodeId(2)],
        },
        cached,
    }
}

fn sink() -> CacheSink {
    CacheSink::new(watch::channel(None).0)
}

/// An `Admit` into the machine's own cell over `members`, taken at
/// `position`.
fn admit(machine: &AdmittedMachine, members: Vec<(NodeId, Address)>, position: u64) -> wire::Admit {
    let mut request = Admission {
        members,
        ..machine.admission.clone()
    }
    .to_wire();
    request.registry_position = position;
    request
}

#[test]
fn its_own_admission_is_acked_again() {
    let machine = admitted(None);
    let cache = sink();
    let ack = machine.admit_ack(
        &admit(&machine, machine.admission.members.clone(), 0),
        &cache,
    );
    assert!(ack.admitted && ack.in_cell, "{ack:?}");
    assert_eq!(ack.refusal, "");
    assert!(cache.latest().is_none(), "the same book is not adopted");
}

#[test]
fn another_cells_or_a_malformed_admission_is_refused() {
    let machine = admitted(None);
    let other = Admission {
        cell: plan(8, vec![member(5)]).control_journals(),
        ..machine.admission.clone()
    };
    let cache = sink();
    for (request, refusal) in [
        (other.to_wire(), "other_cell"),
        (wire::Admit::default(), "malformed"),
    ] {
        let ack = machine.admit_ack(&request, &cache);
        assert!(!ack.admitted && !ack.in_cell);
        assert_eq!(ack.refusal, refusal);
    }
    assert!(cache.latest().is_none());
}

#[test]
fn a_newer_book_in_an_admit_is_adopted_and_the_identity_shows_it() {
    let machine = admitted(None);
    let cache = sink();
    assert_eq!(machine.identify_ack(&cache).registry_position, 0);
    let moved = vec![member(1), member(3), member(4)];
    let ack = machine.admit_ack(&admit(&machine, moved.clone(), 12), &cache);
    assert!(ack.admitted);
    let adopted = cache.latest().expect("adopted");
    assert_eq!(adopted.machines, moved);
    assert_eq!(adopted.position, 12);
    assert_eq!(adopted.node, NodeId(3));
    let identity = machine.identify_ack(&cache);
    assert_eq!(identity.registry_position, 12);
    assert_eq!(identity.cell_id, 9);
    assert_eq!(identity.node_id, 3);
}

#[test]
fn a_book_at_or_below_its_newest_fold_is_not_adopted() {
    let held = CachedRegistry {
        node: NodeId(3),
        position: 20,
        machines: vec![member(1), member(2), member(3)],
    };
    let machine = admitted(Some(held));
    let cache = sink();
    for position in [5, 20] {
        let ack = machine.admit_ack(
            &admit(&machine, vec![member(1), member(4)], position),
            &cache,
        );
        assert!(ack.admitted);
        assert!(cache.latest().is_none(), "position {position}");
    }
    assert_eq!(machine.identify_ack(&cache).registry_position, 20);
    // Past the fold, but the book it holds: nothing new to adopt.
    let same = machine.book();
    machine.admit_ack(&admit(&machine, same, 21), &cache);
    assert!(cache.latest().is_none());
    // Past the fold and another book: adopted.
    machine.admit_ack(&admit(&machine, vec![member(1), member(4)], 21), &cache);
    assert_eq!(cache.latest().map(|c| c.position), Some(21));
}

#[test]
fn an_admitted_machine_refuses_the_cell_decree_as_in_a_cell() {
    let ack = admitted(None).prepare_ack();
    assert_eq!(ack.refusal, "in_cell");
    assert!(!ack.promised);
    assert_eq!(ack.identity.map(|i| i.cell_id), Some(9));
}

#[test]
fn it_knows_the_admissions_machines_where_its_cache_dials_them() {
    let cached = CachedRegistry {
        node: NodeId(3),
        position: 4,
        machines: vec![(NodeId(1), Address::from(addr(11))), member(5)],
    };
    let machine = admitted(Some(cached));
    assert_eq!(
        machine.book(),
        vec![
            (NodeId(1), Address::from(addr(11))),
            member(2),
            member(3),
            member(5)
        ]
    );
    assert_eq!(machine.known().len(), 4);
}
