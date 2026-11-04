//! A capacity request judged over a folded registry: malformed, answered
//! from its record, an unknown tenant, fresh booking ids, and the grant or
//! the refusal it writes.

use super::*;
use crate::fixture;
use paros_client::capacity::Wanted;
use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_model::machine::Class;
use paros_model::registry::{BookingTarget, Role};

const TENANT: TenantId = TenantId(50);

fn journals(fleet: Option<u64>) -> ControlJournals {
    ControlJournals {
        cell_id: fixture::CELL,
        cell: fixture::journal(10, 11),
        fleet: fleet.map(|t| fixture::journal(t, t + 1)),
        election: Some(fixture::journal(10, 12)),
    }
}

/// Founder 1 hosting tenant 50; storage machine 2 admitted, 3 not.
fn cell() -> Registry {
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    fixture::register(&mut registry, 3, Class::Storage);
    fixture::admit(&mut registry, 2);
    fixture::host(&mut registry, 50, 51, 1);
    registry
}

fn request(id: u64, tenant: TenantId, nodes: &[u64]) -> CapacityRequest {
    CapacityRequest {
        request: id,
        tenant,
        slots: nodes
            .iter()
            .map(|n| Wanted {
                node: NodeId(*n),
                role: Role::Acceptor,
                target: BookingTarget::Journal(fixture::journal(tenant.0, 52)),
            })
            .collect(),
    }
}

/// A draw that gives `ids` in turn.
fn draws(ids: &'static [u64]) -> impl FnMut() -> u64 {
    let mut ids = ids.iter().copied();
    move || ids.next().unwrap_or(0)
}

fn booked(decision: &Decision) -> Vec<(u64, u64)> {
    let Decision::Grant(slots) = decision else {
        panic!("a grant: {decision:?}");
    };
    slots.iter().map(|s| (s.booking, s.node.0)).collect()
}

#[test]
fn a_request_with_no_id_or_no_slot_is_malformed() {
    let registry = cell();
    let malformed = Decision::Answer(CapacityAnswer::Malformed);
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(0, TENANT, &[2]),
            draws(&[7])
        ),
        malformed
    );
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[]),
            draws(&[7])
        ),
        malformed
    );
}

#[test]
fn a_request_of_a_tenant_the_cell_does_not_know_is_refused() {
    let registry = cell();
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(9, TenantId(70), &[2]),
            draws(&[7])
        ),
        Decision::Answer(CapacityAnswer::UnknownTenant)
    );
    // The cell tenant and the universe tenant are special: they take slots.
    assert_eq!(
        booked(&decide(
            &registry,
            journals(None),
            &request(9, TenantId(10), &[2]),
            draws(&[7])
        )),
        [(7, 2)]
    );
    assert_eq!(
        booked(&decide(
            &registry,
            journals(Some(20)),
            &request(9, TenantId(20), &[2]),
            draws(&[7])
        )),
        [(7, 2)]
    );
}

#[test]
fn a_grant_draws_a_fresh_id_per_slot() {
    let registry = cell();
    // 0 is no id, and a second slot never takes the first one's id.
    assert_eq!(
        booked(&decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[1, 2]),
            draws(&[0, 7, 7, 8])
        )),
        [(7, 1), (8, 2)]
    );
    // An id the registry ever booked is spent: the footprint's is 50000.
    assert_eq!(
        booked(&decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[2]),
            draws(&[50_000, 6])
        )),
        [(6, 2)]
    );
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[2]),
            draws(&[0, 0, 0, 0, 5])
        ),
        Decision::Answer(CapacityAnswer::Unavailable),
        "four draws at most"
    );
}

#[test]
fn a_slot_on_a_machine_that_does_not_serve_is_refused() {
    let registry = cell();
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[2, 3]),
            draws(&[7, 8])
        ),
        Decision::Refuse
    );
}

#[test]
fn a_recorded_request_is_answered_from_its_record() {
    let mut registry = cell();
    let Decision::Grant(slots) = decide(
        &registry,
        journals(None),
        &request(9, TENANT, &[2]),
        draws(&[7]),
    ) else {
        panic!("a grant");
    };
    fixture::fold(
        &mut registry,
        &SystemCommand::GrantCapacity {
            key: GrantKey::Request(9),
            tenant: TENANT,
            slots: Some(slots),
        },
    );
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(9, TENANT, &[2]),
            draws(&[8])
        ),
        Decision::Answer(CapacityAnswer::Granted(vec![7]))
    );
    assert_eq!(
        decide(
            &registry,
            journals(Some(20)),
            &request(9, TenantId(20), &[2]),
            draws(&[8])
        ),
        Decision::Answer(CapacityAnswer::Malformed),
        "another tenant's id is never its answer"
    );
    fixture::fold(
        &mut registry,
        &SystemCommand::GrantCapacity {
            key: GrantKey::Request(10),
            tenant: TENANT,
            slots: None,
        },
    );
    assert_eq!(
        decide(
            &registry,
            journals(None),
            &request(10, TENANT, &[2]),
            draws(&[8])
        ),
        Decision::Answer(CapacityAnswer::Refused)
    );
}
