//! The coordinator calls' wire forms, pinned: every answer round-trips, an
//! answer this client does not know is retried, and a request that names
//! too little is refused where it enters.

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_model::address::Address;
use paros_model::registry::{BookingTarget, Role, UniversePointer};
use paros_wire::machine as wire;

use crate::capacity::{CapacityAnswer, CapacityRequest, Wanted};
use crate::resolve::{Resolution, from_wire};
use crate::universe::{JoinAnswer, JoinRequest};

fn journal(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn member(node_id: u64, addr: &str) -> wire::Member {
    wire::Member {
        node_id,
        addr: addr.into(),
        machine_interface: Vec::new(),
        zone: String::new(),
    }
}

#[test]
fn a_join_answer_round_trips_and_an_unknown_one_is_retried() {
    let answers = [
        JoinAnswer::Joined,
        JoinAnswer::NotCoordinator,
        JoinAnswer::Unavailable,
        JoinAnswer::NotListed,
        JoinAnswer::OtherUniverse,
        JoinAnswer::Malformed,
    ];
    for answer in answers {
        assert_eq!(JoinAnswer::from_wire(&answer.to_wire()), answer);
    }
    let retried: Vec<_> = answers.into_iter().filter(|a| a.is_retryable()).collect();
    assert_eq!(
        retried,
        vec![
            JoinAnswer::NotCoordinator,
            JoinAnswer::Unavailable,
            JoinAnswer::NotListed
        ]
    );
    let unknown = wire::JoinUniverseAck {
        outcome: "later".into(),
    };
    assert_eq!(JoinAnswer::from_wire(&unknown), JoinAnswer::Unavailable);
}

#[test]
fn a_join_request_round_trips_and_names_its_universe_directory_and_hosts() {
    let request = JoinRequest {
        fleet_id: 4,
        pointer: UniversePointer {
            directory: journal(5, 6),
            hosts: vec![(7, Address::parse("10.0.0.7:4500").expect("an address"))],
        },
    };
    let sent = request.to_wire();
    assert_eq!(JoinRequest::from_wire(&sent), Ok(request));
    let refused = |edit: fn(&mut wire::JoinUniverse)| {
        let mut bad = sent.clone();
        edit(&mut bad);
        JoinRequest::from_wire(&bad).expect_err("refused")
    };
    let names = "a join names its universe, its directory and a host";
    assert_eq!(refused(|r| r.fleet_id = 0), names);
    assert_eq!(refused(|r| r.directory_journal = 0), names);
    assert_eq!(refused(|r| r.hosts.clear()), names);
    assert_eq!(refused(|r| r.hosts[0].node_id = 0), "a host names its node");
    assert_eq!(
        refused(|r| r.hosts[0].addr = "not an address".into()),
        "a host's address"
    );
}

#[test]
fn a_capacity_answer_round_trips_and_an_unknown_one_is_retried() {
    let answers = [
        CapacityAnswer::Granted(vec![3, 4]),
        CapacityAnswer::Refused,
        CapacityAnswer::NotCoordinator,
        CapacityAnswer::Unavailable,
        CapacityAnswer::UnknownTenant,
        CapacityAnswer::Malformed,
    ];
    for answer in &answers {
        assert_eq!(&CapacityAnswer::from_wire(answer.to_wire()), answer);
        assert_eq!(
            answer.is_retryable(),
            matches!(
                answer,
                CapacityAnswer::NotCoordinator | CapacityAnswer::Unavailable
            )
        );
    }
    let unknown = wire::CapacityRequestAck {
        outcome: "later".into(),
        bookings: vec![9],
    };
    assert_eq!(
        CapacityAnswer::from_wire(unknown),
        CapacityAnswer::Unavailable
    );
}

#[test]
fn a_capacity_request_round_trips_every_target() {
    let tenant = TenantId(5);
    let request = CapacityRequest {
        request: 11,
        tenant,
        slots: vec![
            Wanted {
                node: NodeId(1),
                role: Role::Acceptor,
                target: BookingTarget::Journal(journal(5, 8)),
            },
            Wanted {
                node: NodeId(2),
                role: Role::Matchmaker,
                target: BookingTarget::Set { tenant, set: 3 },
            },
            Wanted {
                node: NodeId(3),
                role: Role::Frontend,
                target: BookingTarget::Tenant { tenant },
            },
        ],
    };
    let sent = request.to_wire();
    let kinds: Vec<_> = sent
        .slots
        .iter()
        .map(|s| (s.journal, s.set, s.tenant_level))
        .collect();
    assert_eq!(kinds, vec![(8, 0, false), (0, 3, false), (0, 0, true)]);
    assert_eq!(CapacityRequest::from_wire(&sent), Ok(request));
}

#[test]
fn a_capacity_request_that_names_too_little_is_refused() {
    let slot = |journal, set, tenant_level| wire::CapacitySlot {
        node: 1,
        role: Role::Acceptor.as_str().into(),
        journal,
        set,
        tenant_level,
    };
    let request = |id, tenant, slots| wire::CapacityRequest {
        request: id,
        tenant,
        slots,
    };
    let names = "a capacity request names its id, its tenant and a slot";
    let one = "a slot names one journal, one set, or its tenant";
    let refusal = |r: &wire::CapacityRequest| CapacityRequest::from_wire(r).expect_err("refused");
    assert_eq!(refusal(&request(0, 5, vec![slot(1, 0, false)])), names);
    assert_eq!(refusal(&request(1, 0, vec![slot(1, 0, false)])), names);
    assert_eq!(refusal(&request(1, 5, vec![])), names);
    for bad in [
        slot(0, 0, false),
        slot(1, 2, false),
        slot(1, 0, true),
        slot(0, 2, true),
    ] {
        assert_eq!(refusal(&request(1, 5, vec![bad])), one);
    }
    let mut unknown_role = slot(1, 0, false);
    unknown_role.role = "juggler".into();
    assert!(CapacityRequest::from_wire(&request(1, 5, vec![unknown_role])).is_err());
}

/// A well-formed resolve answer for tenant 1 in cell 1.
fn resolved_ack() -> wire::ResolveAck {
    let mut machine = member(1, "10.0.0.1:4500");
    machine.machine_interface = vec![9];
    machine.zone = "a".into();
    let mut batcher = member(3, "10.0.0.3:4500");
    batcher.zone = "b".into();
    wire::ResolveAck {
        universe_id: 1,
        cell_id: 1,
        tenant_cell: 1,
        tenant: 1,
        control: Some(wire::JournalIdentifier {
            tenant: 1,
            journal: 6,
        }),
        machines: vec![machine, member(2, "10.0.0.2:4500")],
        batchers: vec![batcher],
        at: 7,
        ..wire::ResolveAck::default()
    }
}

#[test]
fn a_resolve_answer_names_the_machines_their_interfaces_and_zones() {
    let Resolution::Resolved(resolved) = from_wire(&resolved_ack()) else {
        panic!("resolved");
    };
    assert_eq!(resolved.cell_id, 1);
    assert_eq!(resolved.tenant, TenantId(1));
    assert_eq!(resolved.control, journal(1, 6));
    let ids: Vec<_> = resolved.machines.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![NodeId(1), NodeId(2)]);
    assert_eq!(
        resolved.interfaces.into_iter().collect::<Vec<_>>(),
        vec![(NodeId(1), vec![9])]
    );
    let zones: Vec<_> = resolved.zones.into_iter().collect();
    assert_eq!(
        zones,
        vec![(NodeId(1), "a".to_string()), (NodeId(3), "b".to_string())]
    );
    assert_eq!(resolved.batchers.len(), 1);
    assert_eq!(resolved.at, 7);
}

#[test]
fn a_resolve_refusal_names_the_cells() {
    let ack = wire::ResolveAck {
        refusal: "other_cell".into(),
        cell_id: 2,
        tenant_cell: 3,
        ..wire::ResolveAck::default()
    };
    let Resolution::Refused {
        refusal,
        cell_id,
        tenant_cell,
        ..
    } = from_wire(&ack)
    else {
        panic!("refused");
    };
    assert_eq!(
        (refusal.as_str(), cell_id, tenant_cell),
        ("other_cell", 2, 3)
    );
}

#[test]
fn a_resolve_answer_that_breaks_its_shape_is_malformed() {
    let edits: [fn(&mut wire::ResolveAck); 9] = [
        |a| a.cell_id = 0,
        |a| a.tenant = 0,
        |a| a.tenant_cell = 9,
        |a| a.control = None,
        |a| a.control.as_mut().expect("set").tenant = 4,
        |a| a.machines.clear(),
        |a| a.machines[1].node_id = 0,
        |a| a.machines[0].addr = "nowhere".into(),
        |a| a.batchers[0].node_id = 0,
    ];
    for edit in edits {
        let mut ack = resolved_ack();
        edit(&mut ack);
        assert_eq!(from_wire(&ack), Resolution::Malformed, "{ack:?}");
    }
}

#[test]
fn a_claim_s_set_leader_answer_is_its_outcome() {
    use crate::{ClaimOutcome, SetLeaderOutcome};
    use paros_core::journal::JournalView;
    let state = JournalView::default();
    let denial = paros_model::authz::Denial::Expired;
    let cases = [
        (SetLeaderOutcome::Won { state }, ClaimOutcome::Won { state }),
        (
            SetLeaderOutcome::Lost { state },
            ClaimOutcome::Lost { state },
        ),
        (
            SetLeaderOutcome::WrongMode { state },
            ClaimOutcome::WrongMode { state },
        ),
        (
            SetLeaderOutcome::Redirect { leader: Some(3) },
            ClaimOutcome::Redirect { leader: Some(3) },
        ),
        (
            SetLeaderOutcome::UnknownJournal,
            ClaimOutcome::UnknownJournal,
        ),
        (SetLeaderOutcome::Malformed, ClaimOutcome::Malformed),
        (
            SetLeaderOutcome::Denied(denial),
            ClaimOutcome::Denied(denial),
        ),
        (SetLeaderOutcome::Ambiguous, ClaimOutcome::Ambiguous),
    ];
    for (set_leader, claim) in cases {
        assert_eq!(ClaimOutcome::from(set_leader), claim);
    }
}
