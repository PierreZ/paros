//! Test fixtures: a registry built the way the cell control journal folds
//! it, one `SystemCommand` at a time, so a unit test reads the same facts a
//! term reads.

use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_model::machine::{Class, Locality};
use paros_model::registry::{
    BookingTarget, GrantKey, HostedTenant, MaintenanceScope, Registry, Role, Slot, SystemCommand,
};

/// The fleet id every fixture cell joins.
pub(crate) const FLEET: u64 = 7;

/// The cell id every fixture cell has.
pub(crate) const CELL: u64 = 8;

/// The incarnation every fixture machine registers as.
pub(crate) const INCARNATION: u128 = 1;

/// A journal of `tenant`.
pub(crate) fn journal(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

/// Fold `command` into `registry` at its next position.
///
/// # Panics
///
/// If the fold does not move the registry's position: the test wrote an
/// entry the fold does not read.
pub(crate) fn fold(registry: &mut Registry, command: &SystemCommand) {
    let seq = registry.next_seq();
    registry.fold(seq, &command.encode());
    assert!(registry.next_seq() > seq, "the fold reads the entry");
}

/// A registry over the initial members `initial_members`, each registered as a
/// storage machine in zone `z<id>`.
pub(crate) fn cell(initial_members: &[u64]) -> Registry {
    let mut registry = Registry::new(initial_members.iter().copied().map(NodeId));
    for id in initial_members {
        register(&mut registry, *id, Class::Storage);
    }
    registry
}

/// Register machine `id` of `class`, in zone `z<id>`.
pub(crate) fn register(registry: &mut Registry, id: u64, class: Class) {
    fold(
        registry,
        &SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("m{id}.paros:1"),
            class,
            locality: Locality::zone(format!("z{id}")),
            name: format!("m{id}"),
            incarnation: INCARNATION,
            machine_interface: Vec::new(),
        },
    );
}

/// Mark machine `id` down.
pub(crate) fn down(registry: &mut Registry, id: u64) {
    fold(
        registry,
        &SystemCommand::MachineDown {
            id: NodeId(id),
            incarnation: INCARNATION,
        },
    );
    assert!(!registry.liveness(NodeId(id)).up, "machine {id} is down");
}

/// Book slot `booking` of machine `node` for `role` of `target`.
pub(crate) fn book(
    registry: &mut Registry,
    booking: u64,
    node: u64,
    (role, target): (Role, BookingTarget),
) {
    fold(
        registry,
        &SystemCommand::BookCapacity {
            booking,
            node: NodeId(node),
            role,
            target,
        },
    );
}

/// Open a maintenance window of `for_ms` over machine `id`.
pub(crate) fn maintain(registry: &mut Registry, id: u64, for_ms: u64) {
    fold(
        registry,
        &SystemCommand::Maintenance {
            scope: MaintenanceScope::Machine(NodeId(id)),
            for_ms,
        },
    );
}

/// Join the fleet, then host `tenant` with control journal `control`,
/// its footprint one acceptor slot on `footprint_on`, the machine it is
/// born on.
pub(crate) fn host(registry: &mut Registry, tenant: u64, control: u64, footprint_on: u64) {
    if registry.fleet().is_none() {
        fold(
            registry,
            &SystemCommand::JoinFleet {
                fleet_id: FLEET,
                cell_id: CELL,
                version: paros_model::fleet::METADATA_VERSION,
                universe: None,
            },
        );
    }
    fold(
        registry,
        &SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TenantId(tenant)),
            tenant: TenantId(tenant),
            slots: Some(vec![
                Slot {
                    booking: tenant * 1000,
                    node: NodeId(footprint_on),
                    role: Role::Acceptor,
                    target: BookingTarget::Tenant {
                        tenant: TenantId(tenant),
                    },
                },
                // The machine the tenant is born on (#685 (no founders)).
                Slot {
                    booking: tenant * 1000 + 1,
                    node: NodeId(footprint_on),
                    role: Role::Acceptor,
                    target: BookingTarget::Journal(paros_core::journal::JournalIdentifier::new(
                        TenantId(tenant),
                        JournalId(control),
                    )),
                },
            ]),
        },
    );
    fold(
        registry,
        &SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TenantId(tenant),
            hosted: HostedTenant {
                control: JournalId(control),
                name: format!("t{tenant}").into_bytes(),
                survives: paros_model::tenant::Survives::Az,
                members: vec![NodeId(footprint_on)],
            },
        },
    );
    assert!(
        registry.hosts(TenantId(tenant)),
        "tenant {tenant} is hosted"
    );
}

/// Record that registered machine `id` answers in the cell: placement may
/// book it.
pub(crate) fn admit(registry: &mut Registry, id: u64) {
    fold(registry, &SystemCommand::MachineAdmitted { id: NodeId(id) });
    assert!(registry.serves(NodeId(id)), "machine {id} serves the cell");
}

/// Drain machine `id` (a registered one: a node that never registered has no standing to change).
pub(crate) fn drain(registry: &mut Registry, id: u64) {
    fold(registry, &SystemCommand::DrainNode { id: NodeId(id) });
    assert!(
        registry
            .get(NodeId(id))
            .is_some_and(|n| n.standing == paros_model::registry::NodeStanding::Draining),
        "machine {id} drains"
    );
}

/// A tenant desk of a term over initial members `initial_members`, its seed
/// `seed`.
pub(crate) fn desk(initial_members: &[u64], seed: u128) -> crate::tenant::TenantDesk {
    let initial_members: Vec<NodeId> = initial_members.iter().copied().map(NodeId).collect();
    crate::tenant::TenantDesk::new(
        paros_core::journal::LeaderUuid(0xfeed),
        journal(10, 11),
        &initial_members,
        (seed, false, false, false),
        paros_client::ClientTunables::default().checkpoint_policy(),
    )
}

/// A booking of `node` for `role` of `target`.
pub(crate) fn booking(
    node: u64,
    role: Role,
    target: BookingTarget,
) -> paros_model::registry::Booking {
    paros_model::registry::Booking {
        node: NodeId(node),
        role,
        target,
    }
}
