//! `Resolve` names the machines the registry books a tenant's batchers,
//! frontends and replicas on, at the address the cell's book holds, each
//! with its interface and zone (#723, #765, #404, #673).

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_model::address::Address;
use paros_model::registry::{BookingTarget, Registry, Role, SystemCommand};

use super::{booked_on, member};
use crate::{Class, Locality};

fn fold(registry: &mut Registry, command: &SystemCommand) {
    let seq = registry.next_seq();
    registry.fold(seq, &command.encode());
}

fn addr(id: u64) -> Address {
    Address::parse(&format!("10.0.4.{id}:4500")).expect("a test address")
}

/// A registry of machines 1 to 4 (each in zone `z<id>`, its interface
/// `[id]`) with `bookings` of (machine, role, tenant).
fn registry(bookings: &[(u64, Role, u64)]) -> Registry {
    let mut registry = Registry::default();
    for id in 1..=4 {
        fold(
            &mut registry,
            &SystemCommand::RegisterNode {
                id: NodeId(id),
                addr: addr(id).to_string(),
                class: Class::All,
                locality: Locality::zone(format!("z{id}")),
                name: format!("m{id}"),
                incarnation: u128::from(id),
                machine_interface: vec![u8::try_from(id).expect("small")],
            },
        );
    }
    for (booking, (node, role, tenant)) in (1..).zip(bookings) {
        let target = if *role == Role::Frontend {
            BookingTarget::Tenant {
                tenant: TenantId(*tenant),
            }
        } else {
            BookingTarget::Journal(JournalIdentifier::new(
                TenantId(*tenant),
                JournalId(booking),
            ))
        };
        fold(
            &mut registry,
            &SystemCommand::BookCapacity {
                booking,
                node: NodeId(*node),
                role: *role,
                target,
            },
        );
    }
    registry
}

fn book(ids: &[u64]) -> Vec<(NodeId, Address)> {
    ids.iter().map(|id| (NodeId(*id), addr(*id))).collect()
}

fn ids(members: &[paros_wire::machine::Member]) -> Vec<u64> {
    members.iter().map(|m| m.node_id).collect()
}

#[test]
fn a_tenants_booked_machines_are_named_once_each_by_role() {
    let registry = registry(&[
        (2, Role::Batcher, 7),
        (2, Role::Batcher, 7),
        (3, Role::Batcher, 7),
        (4, Role::Batcher, 8),
        (1, Role::Replica, 7),
        (4, Role::Frontend, 7),
    ]);
    let all = book(&[1, 2, 3, 4]);
    let batchers = booked_on(&registry, TenantId(7), Role::Batcher, &all);
    assert_eq!(
        ids(&batchers),
        vec![2, 3],
        "another tenant's batcher is not named"
    );
    assert_eq!(
        ids(&booked_on(&registry, TenantId(7), Role::Replica, &all)),
        vec![1]
    );
    assert_eq!(
        ids(&booked_on(&registry, TenantId(7), Role::Frontend, &all)),
        vec![4]
    );
    assert!(booked_on(&registry, TenantId(9), Role::Batcher, &all).is_empty());
}

#[test]
fn a_booked_machine_the_book_does_not_place_is_left_out() {
    let registry = registry(&[(2, Role::Batcher, 7), (3, Role::Batcher, 7)]);
    let named = booked_on(&registry, TenantId(7), Role::Batcher, &book(&[3]));
    assert_eq!(ids(&named), vec![3]);
}

#[test]
fn a_member_carries_its_book_address_interface_and_zone() {
    let registry = registry(&[]);
    let moved = Address::parse("10.0.4.99:4500").expect("an address");
    let named = member(Some(&registry), NodeId(3), &moved);
    assert_eq!(named.node_id, 3);
    assert_eq!(
        named.addr,
        moved.to_string(),
        "the book's address, not the record's"
    );
    assert_eq!(named.machine_interface, vec![3]);
    assert_eq!(named.zone, "z3");
    let unknown = member(None, NodeId(5), &moved);
    assert!(unknown.machine_interface.is_empty());
    assert!(unknown.zone.is_empty());
}
