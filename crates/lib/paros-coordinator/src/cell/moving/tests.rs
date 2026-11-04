use super::*;

use paros_core::journal::JournalId;
use paros_model::checkpoint::Checkpointable as _;
use paros_model::fleet::{CellKind, CellState, FleetCommand, FleetEntry, buckets};
use paros_model::registry::{BookingTarget, Class, Role};
use paros_model::tags::UserTags;
use paros_model::tenant::Survives;

const FLEET: u64 = 0xf1ee7;
const SOURCE: u64 = 0xce11;
const TARGET: u64 = 0x0ce2;
const TENANT: TenantId = TenantId(300);

fn id(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn target_endpoint() -> Vec<(u64, Address)> {
    vec![(21, Address::parse("10.0.2.1:4500").unwrap())]
}

/// A universe of two `READY` cells, with tenant 300 `READY` in `SOURCE`.
fn directory() -> (FleetDirectory, u64) {
    let mut directory = FleetDirectory::default();
    let commands = [
        FleetCommand::FormFleet {
            control: id(0x3e7a, 0x51),
            name: Vec::new(),
        },
        FleetCommand::AddCell {
            cell_id: SOURCE,
            control: id(0xc0de, 0x52),
            name: Vec::new(),
            kind: CellKind::default(),
            endpoint: Vec::new(),
        },
        FleetCommand::MarkCell {
            cell_id: SOURCE,
            state: CellState::Ready,
        },
        FleetCommand::AddCell {
            cell_id: TARGET,
            control: id(0xc1de, 0x53),
            name: b"west".to_vec(),
            kind: CellKind::default(),
            endpoint: target_endpoint(),
        },
        FleetCommand::MarkCell {
            cell_id: TARGET,
            state: CellState::Ready,
        },
        FleetCommand::RegisterTenant {
            control: id(300, 301),
            name: b"acme".to_vec(),
            cell_id: SOURCE,
            survives: Survives::Az,
            tags: UserTags::none(),
            placement: buckets::Placement::Bucket,
        },
        FleetCommand::MarkTenant {
            tenant: TENANT,
            state: paros_model::fleet::TenantState::Ready,
        },
    ];
    let mut seq = 0;
    for command in commands {
        directory.fold(seq, &FleetEntry::new(FLEET, command).encode());
        seq += 1;
    }
    assert_eq!(directory.fleet(), Some(FLEET));
    (directory, seq)
}

fn fold(directory: &mut FleetDirectory, seq: &mut u64, command: FleetCommand) {
    directory.fold(*seq, &FleetEntry::new(FLEET, command).encode());
    *seq += 1;
}

/// The registry of cell `cell`, founded on machine `node`, joined to the
/// fleet.
fn cell(cell: u64, node: u64) -> (Registry, u64) {
    let mut registry = Registry::new([NodeId(node)]);
    let records = [
        SystemCommand::RegisterNode {
            id: NodeId(node),
            addr: format!("10.0.0.{node}:4500"),
            class: Class::Storage,
            locality: paros_model::machine::Locality::default(),
            name: format!("m{node}"),
            incarnation: 1,
            machine_interface: Vec::new(),
        },
        SystemCommand::JoinFleet {
            fleet_id: FLEET,
            cell_id: cell,
            version: 1,
            universe: None,
        },
    ];
    let mut seq = 0;
    for record in records {
        registry.apply(seq, &record.encode());
        seq += 1;
    }
    (registry, seq)
}

fn apply(registry: &mut Registry, seq: &mut u64, steps: Vec<SystemCommand>) -> SystemCommand {
    assert_eq!(steps.len(), 1, "one step per tenant: {steps:?}");
    let step = steps.into_iter().next().unwrap();
    registry.apply(*seq, &step.encode());
    *seq += 1;
    step
}

#[test]
fn a_move_releases_adopts_names_peers_then_forgets() {
    let (mut directory, mut dseq) = directory();
    let (mut source, mut sseq) = cell(SOURCE, 1);
    // The source hosts the tenant.
    for step in [
        crate::universe::footprint_grant(&source, TENANT).unwrap(),
        SystemCommand::HostTenant {
            fleet_id: FLEET,
            tenant: TENANT,
            hosted: HostedTenant {
                control: JournalId(301),
                name: b"acme".to_vec(),
                survives: Survives::Az,
            },
        },
    ] {
        apply(&mut source, &mut sseq, vec![step]);
    }
    assert!(source.hosts(TENANT));
    let (mut target, mut tseq) = cell(TARGET, 21);
    let none = BTreeMap::new();
    assert!(
        steps(&directory, &source, (SOURCE, &[NodeId(1)]), &none).is_empty(),
        "no move, no step"
    );
    fold(
        &mut directory,
        &mut dseq,
        FleetCommand::MoveTenant {
            tenant: TENANT,
            to: TARGET,
        },
    );
    // The target waits for the release.
    let at_source = BTreeMap::from([(SOURCE, (&source, &[][..]))]);
    assert!(steps(&directory, &target, (TARGET, &[NodeId(21)]), &at_source).is_empty());
    // The source releases, with its founders as the birth cell.
    let step = {
        let s = steps(&directory, &source, (SOURCE, &[NodeId(1)]), &none);
        apply(&mut source, &mut sseq, s)
    };
    assert!(matches!(step, SystemCommand::ReleaseTenant { ref born, .. } if born == &[NodeId(1)]));
    assert_eq!(source.leaving(TENANT), Some(TARGET));
    // The target books the footprint, adopts, then names the source's
    // machines.
    let mut kinds = Vec::new();
    for _ in 0..3 {
        let at_source = BTreeMap::from([(SOURCE, (&source, &[][..]))]);
        kinds.push({
            let s = steps(&directory, &target, (TARGET, &[NodeId(21)]), &at_source);
            apply(&mut target, &mut tseq, s)
        });
    }
    assert!(matches!(kinds[0], SystemCommand::GrantCapacity { .. }));
    assert!(
        matches!(kinds[1], SystemCommand::AdoptTenant { from: SOURCE, ref born, .. } if born == &[NodeId(1)])
    );
    assert!(
        matches!(kinds[2], SystemCommand::MovePeers { cell_id: SOURCE, ref peers, .. } if peers.len() == 1 && peers[0].id == NodeId(1))
    );
    assert_eq!(target.arriving(TENANT), Some(SOURCE));
    assert_eq!(target.born(TENANT), Some(&[NodeId(1)][..]));
    let at_source = BTreeMap::from([(SOURCE, (&source, &[][..]))]);
    assert!(
        steps(&directory, &target, (TARGET, &[NodeId(21)]), &at_source).is_empty(),
        "the same peers are named once"
    );
    // The target books a slot of the tenant's control journal; the source
    // names that machine.
    let slot = SystemCommand::GrantCapacity {
        key: paros_model::registry::GrantKey::Request(9),
        tenant: TENANT,
        slots: Some(vec![paros_model::registry::Slot {
            booking: 7,
            node: NodeId(21),
            role: Role::Acceptor,
            target: BookingTarget::Journal(id(300, 301)),
        }]),
    };
    apply(&mut target, &mut tseq, vec![slot]);
    assert!(target.bookings().any(|(_, b)| b.node == NodeId(21)));
    let endpoint = target_endpoint();
    let at_target = BTreeMap::from([(TARGET, (&target, endpoint.as_slice()))]);
    let step = {
        let s = steps(&directory, &source, (SOURCE, &[NodeId(1)]), &at_target);
        apply(&mut source, &mut sseq, s)
    };
    assert!(
        matches!(step, SystemCommand::MovePeers { cell_id: TARGET, ref peers, .. } if peers[0].id == NodeId(21))
    );
    // The flip: the source forgets the tenant, and nothing more is asked.
    fold(
        &mut directory,
        &mut dseq,
        FleetCommand::FinishMove { tenant: TENANT },
    );
    let step = {
        let s = steps(&directory, &source, (SOURCE, &[NodeId(1)]), &none);
        apply(&mut source, &mut sseq, s)
    };
    assert!(matches!(
        step,
        SystemCommand::MovedTenant { to: TARGET, .. }
    ));
    assert_eq!(source.moved(TENANT), Some(TARGET));
    assert!(steps(&directory, &source, (SOURCE, &[NodeId(1)]), &none).is_empty());
    assert!(steps(&directory, &target, (TARGET, &[NodeId(21)]), &none).is_empty());
}

#[test]
fn a_cell_is_reached_through_its_endpoint() {
    let (directory, _) = directory();
    let (registry, _) = cell(SOURCE, 1);
    let (journal, endpoint) = reach(&directory, &registry, TARGET).unwrap();
    assert_eq!(journal, id(0xc1de, 0x53));
    assert_eq!(endpoint, target_endpoint());
    // The universe's own cell names no endpoint, and this registry no
    // universe pointer.
    assert!(reach(&directory, &registry, SOURCE).is_none());
    assert!(reach(&directory, &registry, 0x999).is_none());
    assert_eq!(sides(&directory, SOURCE), BTreeSet::new());
}
