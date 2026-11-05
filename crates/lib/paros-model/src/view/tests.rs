use paros_core::journal::JournalId;

use super::*;
use crate::status::Word;

/// Status words shown at once, at the instant zero.
fn words(status: &mut Hysteresis) -> (&mut Hysteresis, Duration) {
    (status, Duration::ZERO)
}
use crate::registry::{BookingTarget, Class, GrantKey, HostedTenant, Role, Slot, SystemCommand};
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
            locality: crate::machine::Locality::zone("zone-a"),
            name: "alpha".into(),
            incarnation: 7,
            machine_interface: Vec::new(),
        },
        SystemCommand::RegisterNode {
            id: NodeId(3),
            addr: "10.0.0.3:4500".into(),
            class: Class::Storage,
            locality: crate::machine::Locality::zone("zone-b"),
            name: "gamma".into(),
            incarnation: 9,
            machine_interface: Vec::new(),
        },
        SystemCommand::JoinFleet {
            fleet_id: FLEET,
            cell_id: CELL,
            version: crate::fleet::METADATA_VERSION,
            universe: None,
        },
        // Its footprint (#476), on the founder that registered.
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TenantId(50)),
            tenant: TenantId(50),
            slots: Some(vec![Slot {
                booking: 50,
                node: NodeId(1),
                role: Role::Acceptor,
                target: BookingTarget::Tenant {
                    tenant: TenantId(50),
                },
            }]),
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
        // Its footprint (#476), on the founder that registered.
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TenantId(60)),
            tenant: TenantId(60),
            slots: Some(vec![Slot {
                booking: 60,
                node: NodeId(1),
                role: Role::Acceptor,
                target: BookingTarget::Tenant {
                    tenant: TenantId(60),
                },
            }]),
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
        balance_spread: 1,
    }
}

#[test]
fn an_admin_sees_every_machine_and_every_tenant_of_the_cell() {
    let (founders, registry) = (founders(), registry());
    let mut status = Hysteresis::new(Duration::ZERO);
    let reply = cell_view(
        &Scope::Admin,
        &facts(&founders, &registry),
        words(&mut status),
    );
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
    let mut status = Hysteresis::new(Duration::ZERO);
    let scope = Scope::Tenant(b"acme".to_vec());
    assert_eq!(
        cell_view(&scope, &facts, words(&mut status)).refusal,
        "forbidden"
    );
    assert_eq!(
        tenant_view(&scope, &facts, b"globex", words(&mut status)).refusal,
        "forbidden"
    );
    let reply = tenant_view(&scope, &facts, b"acme", words(&mut status));
    assert!(reply.refusal.is_empty());
    assert!(within_tenant_scope(&reply, b"acme"));
    assert_eq!(reply.tenants.len(), 1);
    // The control journal on both founders, and the coordinator among them.
    let used: Vec<u64> = reply.machines.iter().map(|m| m.node_id).collect();
    assert_eq!(used, [1, 2]);
    assert_eq!(reply.machines[0].name, "alpha");
    assert_eq!(
        crate::tags::value_of(&reply.machines[0].tags, crate::tags::reserved::ZONE),
        Some("zone-a")
    );
    assert!(
        crate::tags::value_of(&reply.machines[0].tags, crate::tags::reserved::ROLES).is_none(),
        "a tenant scope sees no machine's class"
    );
    // An admin asking the same view sees the addresses.
    let admin = tenant_view(&Scope::Admin, &facts, b"acme", words(&mut status));
    assert!(!within_tenant_scope(&admin, b"acme"));
    assert_eq!(
        tenant_view(&Scope::Admin, &facts, b"initech", words(&mut status)).refusal,
        "unknown_tenant"
    );
}

#[test]
fn a_claim_is_the_scope_until_tokens() {
    assert_eq!(authorize(Some(&Scope::Admin.to_wire())), Ok(Scope::Admin));
    let tenant = Scope::Tenant(b"acme".to_vec());
    assert_eq!(authorize(Some(&tenant.to_wire())), Ok(tenant));
    assert!(authorize(None).is_err());
    assert!(authorize(Some(&Scope::Tenant(Vec::new()).to_wire())).is_err());
}

#[test]
fn every_journal_carries_its_status_word() {
    let (founders, mut registry) = (founders(), registry());
    let mut status = Hysteresis::new(Duration::ZERO);
    let reply = cell_view(
        &Scope::Admin,
        &facts(&founders, &registry),
        words(&mut status),
    );
    let journals: Vec<(&str, &wire::JournalView)> = reply
        .tenants
        .iter()
        .flat_map(|t| t.journals.iter().map(|j| (t.kind.as_str(), j)))
        .collect();
    assert!(!journals.is_empty());
    for (_, journal) in &journals {
        // A control journal wants `2F + 1` of the three storage machines
        // (#685 (special tenants are tenants)): a tenant's, and the cell
        // plan's journals since they grow (steps 4 and 5). On the two
        // founders alone, before the coordinator books the third, each is
        // degraded.
        let (desired, word) = (3, Word::Degraded);
        assert_eq!(journal.desired_acceptors, desired);
        assert_eq!(journal.observed, word.as_str());
        assert_eq!(journal.word, word.as_str());
        assert_eq!(crate::status::observe(journal, &reply.machines), Some(word));
    }
    assert!(
        journals
            .iter()
            .any(|(t, j)| *t == "users" && j.kind == "control")
    );
    // A founder down: a majority of two is two, so no quorum is left.
    registry.fold(
        registry.next_seq(),
        &SystemCommand::MachineDown {
            id: NodeId(2),
            incarnation: 0,
        }
        .encode(),
    );
    let reply = cell_view(
        &Scope::Admin,
        &facts(&founders, &registry),
        words(&mut status),
    );
    for journal in reply.tenants.iter().flat_map(|t| &t.journals) {
        assert_eq!(journal.observed, "unavailable");
        assert_eq!(journal.word, "unavailable");
    }
    // The tenant scope sees the word, judged before its machines narrowed.
    let reply = tenant_view(
        &Scope::Tenant(b"acme".to_vec()),
        &facts(&founders, &registry),
        b"acme",
        words(&mut status),
    );
    assert_eq!(reply.tenants[0].journals[0].word, "unavailable");
}

#[test]
fn a_control_journal_with_its_third_acceptor_booked_is_healthy() {
    let (founders, mut registry) = (founders(), registry());
    // The coordinator booked acme's control journal a slot on gamma
    // (#685 (special tenants are tenants)), and reconfigured onto it.
    let seq = registry.next_seq();
    registry.fold(
        seq,
        &SystemCommand::BookCapacity {
            booking: 70,
            node: NodeId(3),
            role: Role::Acceptor,
            target: BookingTarget::Journal(identifier(50, 51)),
        }
        .encode(),
    );
    let mut status = Hysteresis::new(Duration::ZERO);
    let reply = cell_view(
        &Scope::Admin,
        &facts(&founders, &registry),
        words(&mut status),
    );
    let acme = reply
        .tenants
        .iter()
        .find(|t| t.name == b"acme")
        .expect("acme is hosted");
    let control = acme
        .journals
        .iter()
        .find(|j| j.kind == "control")
        .expect("acme has its control journal");
    assert_eq!(control.acceptors, [1, 2, 3]);
    assert_eq!(control.desired_acceptors, 3);
    assert_eq!(control.word, "healthy");
    // globex's control journal has no third slot: still degraded.
    let globex = reply
        .tenants
        .iter()
        .find(|t| t.name == b"globex")
        .expect("globex is hosted");
    let control = globex
        .journals
        .iter()
        .find(|j| j.kind == "control")
        .expect("globex has its control journal");
    assert_eq!(control.acceptors, [1, 2]);
    assert_eq!(control.word, "degraded");
}

/// Acme's control fold with one data journal, `orders` (id 501),
/// created on founders 1 and 2 under a flexible quorum.
fn acme_with_orders() -> crate::tenant::TenantControl {
    use crate::tenant::{Desired, StoreDurability, TenantCommand, TenantControl};
    use paros_core::journal::WriterMode;
    let mut control = TenantControl::new(TenantId(50), JournalId(51));
    let create = TenantCommand::CreateJournal {
        request: 1,
        id: JournalId(501),
        name: b"orders".to_vec(),
        writer: WriterMode::Single,
        desired: Desired::DEFAULT,
        config: AcceptorConfig::new(
            vec![NodeId(1), NodeId(2)],
            QuorumSystem::Flexible { q1: 2, q2: 1 },
        ),
        tags: crate::tags::UserTags::none(),
        store: StoreDurability::Ordered,
    };
    control.fold(0, &create.encode());
    control
}

/// The `orders` journal's acceptors and quorum system in an admin's cell view.
fn orders_placed(registry: &Registry, tenants: &[crate::tenant::TenantControl]) -> AcceptorConfig {
    let founders = founders();
    let facts = CellFacts {
        tenants,
        ..facts(&founders, registry)
    };
    let mut status = Hysteresis::new(Duration::ZERO);
    let reply = cell_view(&Scope::Admin, &facts, words(&mut status));
    let orders = reply
        .tenants
        .iter()
        .flat_map(|t| &t.journals)
        .find(|j| j.name == b"orders")
        .expect("acme's orders journal is shown");
    assert_eq!(
        (orders.kind.as_str(), orders.writer.as_str()),
        ("data", "single")
    );
    let placed = paros_wire::config_from_proto(orders.placed.clone())
        .expect("a placed config decodes")
        .expect("a data journal shows its placed config");
    let acceptors: Vec<u64> = placed.members().iter().map(|n| n.0).collect();
    assert_eq!(orders.acceptors, acceptors);
    placed
}

fn book_orders(registry: &mut Registry, booking: u64, node: u64) {
    registry.fold(
        registry.next_seq(),
        &SystemCommand::BookCapacity {
            booking,
            node: NodeId(node),
            role: Role::Acceptor,
            target: BookingTarget::Journal(identifier(50, 501)),
        }
        .encode(),
    );
}

#[test]
fn a_data_journal_is_shown_where_its_acceptor_slots_place_it() {
    let tenants = [acme_with_orders()];
    let mut registry = registry();
    let flexible = QuorumSystem::Flexible { q1: 2, q2: 1 };
    // No slot: placed where it was created.
    let placed = orders_placed(&registry, &tenants);
    assert_eq!(placed.members(), [NodeId(1), NodeId(2)]);
    assert_eq!(placed.quorum_system(), flexible);
    // As many slots as members: the slots, under the created quorum system.
    book_orders(&mut registry, 80, 1);
    book_orders(&mut registry, 81, 3);
    let placed = orders_placed(&registry, &tenants);
    assert_eq!(placed.members(), [NodeId(1), NodeId(3)]);
    assert_eq!(placed.quorum_system(), flexible);
    // In the middle of a move: the slots, under a majority.
    registry.fold(
        registry.next_seq(),
        &SystemCommand::ReleaseCapacity { booking: 80 }.encode(),
    );
    let placed = orders_placed(&registry, &tenants);
    assert_eq!(placed.members(), [NodeId(3)]);
    assert_eq!(placed.quorum_system(), QuorumSystem::Majority);
}

#[test]
fn a_draining_machine_is_drained_once_it_holds_no_slot() {
    let mut registry = registry();
    let standing_of = |registry: &Registry| {
        let founders = founders();
        let mut status = Hysteresis::new(Duration::ZERO);
        let reply = cell_view(
            &Scope::Admin,
            &facts(&founders, registry),
            words(&mut status),
        );
        reply
            .machines
            .iter()
            .find(|m| m.node_id == 3)
            .map(|m| m.standing.clone())
    };
    assert_eq!(standing_of(&registry).as_deref(), Some("registered"));
    book_orders(&mut registry, 80, 3);
    let drain = SystemCommand::DrainNode { id: NodeId(3) };
    registry.fold(registry.next_seq(), &drain.encode());
    assert_eq!(standing_of(&registry).as_deref(), Some("draining"));
    registry.fold(
        registry.next_seq(),
        &SystemCommand::ReleaseCapacity { booking: 80 }.encode(),
    );
    assert_eq!(standing_of(&registry).as_deref(), Some("drained"));
    let retire = SystemCommand::RetireNode { id: NodeId(3) };
    registry.fold(registry.next_seq(), &retire.encode());
    assert_eq!(standing_of(&registry).as_deref(), Some("retired"));
}

#[test]
fn only_an_admin_reads_the_universe_and_the_balance() {
    let (founders, registry) = (founders(), registry());
    let facts = facts(&founders, &registry);
    let tenant = Scope::Tenant(b"acme".to_vec());
    let directory = FleetDirectory::default();
    assert_eq!(
        universe_view(&tenant, &facts, &directory).refusal,
        "forbidden"
    );
    assert_eq!(balance_view(&tenant, &facts).refusal, "forbidden");
    let universe = universe_view(&Scope::Admin, &facts, &directory);
    assert!(universe.refusal.is_empty());
    assert!(universe.cells.is_empty() && universe.tenants.is_empty());
    assert_eq!(universe.cell_id, CELL);
    let balance = balance_view(&Scope::Admin, &facts);
    assert!(balance.refusal.is_empty());
    assert!(balance.machines.is_empty());
    let shown = balance.balance.expect("an admin sees the balance");
    assert!(
        !shown.machines.is_empty(),
        "the balance names the cell's machines"
    );
}

mod balance;

#[test]
fn the_universe_view_counts_each_cells_users_tenants() {
    use crate::fleet::buckets::Placement;
    use crate::fleet::{CellKind, CellState, FleetCommand, FleetEntry};
    const OTHER: u64 = 0x0ce2;
    let mut directory = FleetDirectory::default();
    let mut seq = 0;
    let mut fold = |command: FleetCommand| {
        directory.fold(seq, &FleetEntry::new(FLEET, command).encode());
        seq += 1;
    };
    fold(FleetCommand::FormFleet {
        control: identifier(0x3e7a, 0x51),
        name: Vec::new(),
    });
    for (cell_id, tenant) in [(CELL, 0xc0de), (OTHER, 0xc1de)] {
        fold(FleetCommand::AddCell {
            cell_id,
            control: identifier(tenant, 0x52),
            name: Vec::new(),
            kind: CellKind::default(),
            endpoint: Vec::new(),
        });
        fold(FleetCommand::MarkCell {
            cell_id,
            state: CellState::Ready,
        });
    }
    for (tenant, name, cell_id) in [
        (0x700, "acme", CELL),
        (0x800, "globex", OTHER),
        (0x900, "initech", OTHER),
    ] {
        fold(FleetCommand::RegisterTenant {
            control: identifier(tenant, tenant + 1),
            name: name.as_bytes().to_vec(),
            cell_id,
            survives: Survives::Az,
            tags: crate::tags::UserTags::none(),
            placement: Placement::override_for("named"),
        });
    }
    let (founders, registry) = (founders(), registry());
    let universe = universe_view(&Scope::Admin, &facts(&founders, &registry), &directory);
    let mut counts: Vec<(u64, u64)> = universe
        .cells
        .iter()
        .map(|c| (c.cell_id, c.tenants))
        .collect();
    counts.sort_unstable();
    assert_eq!(
        counts,
        [(OTHER, 2), (CELL, 1)],
        "internal tenants are not counted"
    );
    assert_eq!(
        universe.tenants.len(),
        6,
        "the fleet, two cell and three users tenants"
    );
}
