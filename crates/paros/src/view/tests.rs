use paros_core::JournalId;

use super::*;
use crate::system::{Class, HostedTenant, SystemCommand};
use crate::tenant::Survives;

const CELL: u64 = 0xce11;
const FLEET: u64 = 0xf1ee7;

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn founders() -> Vec<(NodeId, Address)> {
    vec![
        (NodeId(1), "10.0.0.1:4500".parse().expect("an address")),
        (NodeId(2), "10.0.0.2:4500".parse().expect("an address")),
    ]
}

/// A registry over two founders: one registered by name, a joiner, and two
/// hosted tenants.
fn registry() -> Registry {
    let mut registry = Registry::new([NodeId(1), NodeId(2)]);
    let commands = [
        SystemCommand::RegisterNode {
            id: NodeId(1),
            addr: "10.0.0.1:4500".into(),
            class: Class::Storage,
            capacity: 2,
            failure_domain: "zone-a".into(),
            name: "alpha".into(),
            incarnation: 7,
        },
        SystemCommand::RegisterNode {
            id: NodeId(3),
            addr: "10.0.0.3:4500".into(),
            class: Class::Storage,
            capacity: 2,
            failure_domain: "zone-b".into(),
            name: "gamma".into(),
            incarnation: 9,
        },
        SystemCommand::JoinFleet {
            fleet_id: FLEET,
            cell_id: CELL,
            version: crate::fleet::METADATA_VERSION,
        },
        SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TenantId(50),
            hosted: HostedTenant {
                control: JournalId(51),
                name: b"acme".to_vec(),
                survives: Survives::Az,
            },
        },
        SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TenantId(60),
            hosted: HostedTenant {
                control: JournalId(61),
                name: b"globex".to_vec(),
                survives: Survives::Az,
            },
        },
    ];
    for (seq, command) in (0..).zip(&commands) {
        registry.fold(seq, &command.encode());
    }
    registry
}

fn facts<'a>(founders: &'a [(NodeId, Address)], registry: &'a Registry) -> CellFacts<'a> {
    CellFacts {
        cell_id: CELL,
        answered_by: NodeId(1),
        founders,
        control: identifier(10, 11),
        election: identifier(10, 12),
        universe: None,
        registry,
        directory: None,
        tenants: &[],
        coordinator: Some((2, 4)),
    }
}

#[test]
fn an_admin_sees_every_machine_and_every_tenant_of_the_cell() {
    let (founders, registry) = (founders(), registry());
    let reply = cell_view(&Scope::Admin, &facts(&founders, &registry));
    assert!(reply.refusal.is_empty());
    let names: Vec<&str> = reply.machines.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["alpha", "", "gamma"]);
    assert_eq!(reply.machines[1].standing, "founding");
    assert_eq!(reply.machines[0].addr, "10.0.0.1:4500");
    assert_eq!(reply.coordinator, 2);
    let tenants: Vec<&[u8]> = reply.tenants.iter().map(|t| t.name.as_slice()).collect();
    assert_eq!(tenants, [&b""[..], b"acme", b"globex"]);
}

#[test]
fn a_tenant_scope_sees_only_its_own_spread() {
    let (founders, registry) = (founders(), registry());
    let facts = facts(&founders, &registry);
    let scope = Scope::Tenant(b"acme".to_vec());
    assert_eq!(cell_view(&scope, &facts).refusal, "forbidden");
    assert_eq!(tenant_view(&scope, &facts, b"globex").refusal, "forbidden");
    let reply = tenant_view(&scope, &facts, b"acme");
    assert!(reply.refusal.is_empty());
    assert!(within_tenant_scope(&reply, b"acme"));
    assert_eq!(reply.tenants.len(), 1);
    // The control journal on both founders, and the coordinator among them.
    let used: Vec<u64> = reply.machines.iter().map(|m| m.node_id).collect();
    assert_eq!(used, [1, 2]);
    assert_eq!(reply.machines[0].name, "alpha");
    assert_eq!(reply.machines[0].failure_domain, "zone-a");
    // An admin asking the same view sees the addresses.
    let admin = tenant_view(&Scope::Admin, &facts, b"acme");
    assert!(!within_tenant_scope(&admin, b"acme"));
    assert_eq!(tenant_view(&Scope::Admin, &facts, b"initech").refusal, "unknown_tenant");
}

#[test]
fn a_claim_is_the_scope_until_tokens() {
    assert_eq!(authorize(Some(&Scope::Admin.to_wire())), Ok(Scope::Admin));
    let tenant = Scope::Tenant(b"acme".to_vec());
    assert_eq!(authorize(Some(&tenant.to_wire())), Ok(tenant));
    assert!(authorize(None).is_err());
    assert!(authorize(Some(&Scope::Tenant(Vec::new()).to_wire())).is_err());
}
