use paros_core::journal::{JournalId, JournalIdentifier};
use prost::Message;

use super::*;
use crate::checkpoint::Checkpointable;
use crate::registry::records::SlowFacts;
fn hosted(control: u64) -> HostedTenant {
    HostedTenant {
        control: JournalId(control),
        name: b"acme".to_vec(),
        survives: crate::tenant::Survives::Az,
    }
}

fn one(command: &SystemCommand) -> Vec<u8> {
    command.encode()
}

#[test]
#[allow(clippy::too_many_lines)] // one entry per command
fn every_command_round_trips() {
    let commands = [
        SystemCommand::RegisterNode {
            id: NodeId(100),
            addr: "10.0.5.1:4500".into(),
            class: Class::Storage,
            locality: crate::machine::Locality::zone("rack-a"),
            name: "storage-7".into(),
            incarnation: (7_u128 << 64) | 9,
            machine_interface: vec![1, 2, 3],
        },
        SystemCommand::DrainNode { id: NodeId(100) },
        SystemCommand::RetireNode { id: NodeId(100) },
        SystemCommand::SetControlPlane {
            request: 5,
            mode: control_plane::ControlPlaneMode::Auto { target: 2 },
        },
        SystemCommand::SetControlPlane {
            request: 6,
            mode: control_plane::ControlPlaneMode::Manual { failures: 0 },
        },
        SystemCommand::MachineAdmitted { id: NodeId(100) },
        SystemCommand::SetBalance {
            request: 9,
            paused: true,
        },
        SystemCommand::BalanceStarted {
            booking: 7,
            journal: JournalIdentifier::new(TenantId(300), JournalId(400)),
            from: NodeId(100),
            to: NodeId(200),
            reason: balancing::BalanceReason::Busy,
        },
        SystemCommand::BalanceAbandoned { booking: 7 },
        SystemCommand::BookCapacity {
            booking: 7,
            node: NodeId(100),
            role: Role::Frontend,
            target: BookingTarget::Journal(JournalIdentifier::new(TenantId(300), JournalId(400))),
        },
        SystemCommand::BookCapacity {
            booking: 8,
            node: NodeId(100),
            role: Role::Matchmaker,
            target: BookingTarget::Set {
                tenant: TenantId(300),
                set: 2,
            },
        },
        SystemCommand::MachineDown {
            id: NodeId(100),
            incarnation: u128::MAX,
        },
        SystemCommand::MachineUp {
            id: NodeId(100),
            incarnation: 1,
        },
        SystemCommand::MachineSlow {
            id: NodeId(100),
            incarnation: 1,
            slow: true,
            facts: SlowFacts {
                rtt_us: 250_000,
                disk_busy_permille: 990,
                cpu_busy_permille: 12,
            },
        },
        SystemCommand::JoinFleet {
            fleet_id: 11,
            cell_id: 13,
            version: 4,
            universe: Some(UniversePointer {
                directory: JournalIdentifier::new(TenantId(5), JournalId(6)),
                hosts: vec![(7, crate::address::Address::parse("10.0.0.7:4500").unwrap())],
            }),
        },
        SystemCommand::ReleaseCapacity { booking: 7 },
        SystemCommand::JoinFleet {
            fleet_id: 11,
            cell_id: 12,
            version: 1,
            universe: None,
        },
        SystemCommand::HostTenant {
            fleet_id: 11,
            tenant: TenantId(300),
            hosted: hosted(301),
        },
        SystemCommand::DropTenant {
            fleet_id: 11,
            tenant: TenantId(300),
        },
        SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TenantId(300)),
            tenant: TenantId(300),
            slots: Some(vec![Slot {
                booking: 9,
                node: NodeId(100),
                role: Role::Coordinator,
                target: BookingTarget::Tenant {
                    tenant: TenantId(300),
                },
            }]),
        },
        SystemCommand::GrantCapacity {
            key: GrantKey::Request(77),
            tenant: TenantId(300),
            slots: None,
        },
    ];
    for command in commands {
        assert_eq!(SystemCommand::decode(&command.encode()), Ok(command));
    }
    assert!(SystemCommand::decode(b"\xff\xff").is_err());
}

#[test]
fn the_pool_grows_by_registration_and_shrinks_by_retirement() {
    let mut reg = Registry::new([NodeId(0), NodeId(1)]);
    let register = |id: u64| register(id, Class::Storage);
    // A genesis node registers its address (#349), and stays in the
    // pool once.
    assert!(matches!(
        reg.fold(0, &register(0)),
        RegistryEvent::Registered { .. }
    ));
    assert_eq!(reg.address(NodeId(0)), Some("n0"));
    assert_eq!(reg.address(NodeId(1)), None);
    assert!(matches!(
        reg.fold(1, &register(100)),
        RegistryEvent::Registered { .. }
    ));
    assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1), NodeId(100)]);
    // Retiring needs a drain first.
    assert_eq!(
        reg.fold(2, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
        RegistryEvent::Refused(RegistryRefusal::NotDraining { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(3, &one(&SystemCommand::DrainNode { id: NodeId(100) })),
        RegistryEvent::Draining { id: NodeId(100) }
    );
    assert!(
        reg.contains(NodeId(100)),
        "a draining node stays in the pool"
    );
    assert_eq!(
        reg.fold(4, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
        RegistryEvent::Retired { id: NodeId(100) }
    );
    assert!(!reg.contains(NodeId(100)));
    assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1)]);
    // A retired id is never registered again, a re-registration
    // included.
    assert_eq!(
        reg.fold(5, &register(100)),
        RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id: NodeId(100) })
    );
    // Genesis nodes are neither drained nor retired.
    assert_eq!(
        reg.fold(6, &one(&SystemCommand::DrainNode { id: NodeId(0) })),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
    );
}

fn register(id: u64, class: Class) -> Vec<u8> {
    one(&SystemCommand::RegisterNode {
        id: NodeId(id),
        addr: format!("n{id}"),
        class,
        locality: crate::machine::Locality::default(),
        name: format!("m{id}"),
        incarnation: u128::from(id),
        machine_interface: Vec::new(),
    })
}

fn book(booking: u64, node: u64, class: Class) -> Vec<u8> {
    let role = Role::of(class).next().expect("every class has a role");
    one(&SystemCommand::BookCapacity {
        booking,
        node: NodeId(node),
        role,
        target: BookingTarget::Journal(JournalIdentifier::new(TenantId(300), JournalId(booking))),
    })
}

fn liveness(id: u64, incarnation: u128, up: bool) -> Vec<u8> {
    let id = NodeId(id);
    one(&if up {
        SystemCommand::MachineUp { id, incarnation }
    } else {
        SystemCommand::MachineDown { id, incarnation }
    })
}

#[test]
fn every_role_names_its_class_and_parses_back() {
    for role in Role::ALL {
        assert_eq!(role.as_str().parse::<Role>(), Ok(role));
        assert!(Role::of(role.class()).any(|r| r == role));
    }
    assert_eq!(Role::of(Class::Storage).count(), 3);
    assert!("seed".parse::<Role>().is_err());
    assert!(BookingTarget::from_wire(1, 2, 3, false).is_err());
    assert!(BookingTarget::from_wire(1, 2, 0, true).is_err());
}

#[test]
fn a_booking_id_is_never_booked_twice_even_after_its_release() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    assert!(matches!(
        reg.fold(1, &book(1, 100, Class::Storage)),
        RegistryEvent::Booked { booking: 1, .. }
    ));
    reg.fold(2, &one(&SystemCommand::ReleaseCapacity { booking: 1 }));
    assert_eq!(
        reg.fold(3, &book(1, 100, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
    );
    assert_eq!(reg.spent().collect::<Vec<_>>(), vec![1]);
    // One slot per role instance: the same role of the same journal
    // twice on one node is refused, another role is not.
    let again = |booking, role| {
        one(&SystemCommand::BookCapacity {
            booking,
            node: NodeId(100),
            role,
            target: BookingTarget::Journal(JournalIdentifier::new(TenantId(300), JournalId(9))),
        })
    };
    assert!(matches!(
        reg.fold(4, &again(2, Role::Acceptor)),
        RegistryEvent::Booked { .. }
    ));
    assert_eq!(
        reg.fold(5, &again(3, Role::Acceptor)),
        RegistryEvent::Refused(RegistryRefusal::AlreadyBooked { id: NodeId(100) })
    );
    assert!(matches!(
        reg.fold(6, &again(4, Role::Replica)),
        RegistryEvent::Booked { .. }
    ));
    // The spent ids cross a checkpoint.
    let mut restored = Registry::new([NodeId(0)]);
    restored
        .restore(6, &reg.checkpoint())
        .expect("a registry's own checkpoint restores");
    assert_eq!(
        restored,
        Registry {
            next_seq: 7,
            ..reg.clone()
        }
    );
    assert_eq!(
        restored.fold(7, &book(1, 100, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
    );
}

#[test]
fn liveness_entries_are_changes_only() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    // A registration is up, as its incarnation.
    assert_eq!(
        reg.fold(1, &liveness(100, 100, true)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(2, &liveness(100, 5, false)),
        RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(3, &liveness(100, 100, false)),
        RegistryEvent::MachineDown { id: NodeId(100) }
    );
    assert!(!reg.seen_alive(NodeId(100)));
    // No `Down` after `Down`.
    assert_eq!(
        reg.fold(4, &liveness(100, 100, false)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    // Back as a new incarnation: a reboot.
    assert_eq!(
        reg.fold(5, &liveness(100, 101, true)),
        RegistryEvent::MachineUp {
            id: NodeId(100),
            incarnation: 101,
            was_down: true
        }
    );
    assert_eq!(reg.get(NodeId(100)).map(|n| n.incarnation), Some(101));
    // A genesis node is watched too; an unknown node is not.
    assert_eq!(
        reg.fold(6, &liveness(0, 9, true)),
        RegistryEvent::MachineUp {
            id: NodeId(0),
            incarnation: 9,
            was_down: false
        }
    );
    assert_eq!(
        reg.fold(7, &liveness(7, 9, true)),
        RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
    );
    assert!(reg.seen_alive(NodeId(0)) && reg.seen_alive(NodeId(100)));
}

fn slow(id: u64, incarnation: u128, slow: bool) -> Vec<u8> {
    SystemCommand::MachineSlow {
        id: NodeId(id),
        incarnation,
        slow,
        facts: SlowFacts::default(),
    }
    .encode()
}

#[test]
fn slow_marks_are_changes_only_and_move_roles() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    assert!(reg.keeps_roles(NodeId(100)) && !reg.is_slow(NodeId(100)));
    // A registration starts fast.
    assert_eq!(
        reg.fold(1, &slow(100, 100, false)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(2, &slow(100, 5, true)),
        RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(3, &slow(100, 100, true)),
        RegistryEvent::MachineSlow {
            id: NodeId(100),
            slow: true
        }
    );
    // Slow is alive, and its roles move.
    assert!(reg.is_slow(NodeId(100)) && reg.seen_alive(NodeId(100)));
    assert!(!reg.keeps_roles(NodeId(100)));
    assert_eq!(
        reg.fold(4, &slow(100, 100, true)),
        RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id: NodeId(100) })
    );
    // Down keeps the judgement; a new incarnation starts fast.
    reg.fold(5, &liveness(100, 100, false));
    assert!(reg.is_slow(NodeId(100)));
    reg.fold(6, &liveness(100, 101, true));
    assert!(!reg.is_slow(NodeId(100)) && reg.keeps_roles(NodeId(100)));
    // A checkpoint keeps a slow mark.
    reg.fold(7, &slow(100, 101, true));
    let mut restored = Registry::new([NodeId(0)]);
    restored.restore(7, &reg.checkpoint()).expect("restores");
    assert!(restored.is_slow(NodeId(100)));
    assert_eq!(
        reg.fold(8, &slow(7, 1, true)),
        RegistryEvent::Refused(RegistryRefusal::NotInPool { id: NodeId(7) })
    );
}

#[test]
fn a_machine_serves_once_its_admission_is_recorded() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    // A genesis node serves from its start; a registration alone does
    // not (#511 (placement books machines that never served)).
    assert!(reg.serves(NodeId(0)));
    assert!(!reg.serves(NodeId(100)));
    let admitted = |id| one(&SystemCommand::MachineAdmitted { id: NodeId(id) });
    assert_eq!(
        reg.fold(1, &admitted(0)),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
    );
    assert_eq!(
        reg.fold(2, &admitted(7)),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(7) })
    );
    assert_eq!(
        reg.fold(3, &admitted(100)),
        RegistryEvent::MachineAdmitted { id: NodeId(100) }
    );
    assert!(reg.serves(NodeId(100)));
    assert_eq!(reg.fold(4, &admitted(100)), RegistryEvent::Unchanged);
    // A reboot keeps the admission; a checkpoint carries it.
    reg.fold(5, &register(100, Class::Storage));
    assert!(reg.serves(NodeId(100)));
    let mut restored = Registry::new([NodeId(0)]);
    restored
        .restore(6, &reg.checkpoint())
        .expect("a checkpoint restores");
    assert!(restored.serves(NodeId(100)));
    // A retired machine serves no more.
    reg.fold(6, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
    assert!(reg.serves(NodeId(100)));
    reg.fold(7, &one(&SystemCommand::RetireNode { id: NodeId(100) }));
    assert!(!reg.serves(NodeId(100)));
    assert_eq!(
        reg.fold(8, &admitted(100)),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(100) })
    );
}

#[test]
fn a_reboot_registers_again_under_the_same_class() {
    let mut reg = Registry::new([NodeId(0)]);
    assert!(matches!(
        reg.fold(0, &register(100, Class::Storage)),
        RegistryEvent::Registered { .. }
    ));
    assert_eq!(
        reg.fold(1, &register(100, Class::Storage)),
        RegistryEvent::Reregistered {
            id: NodeId(100),
            addr: "n100".into(),
        }
    );
    let node = reg.get(NodeId(100)).expect("registered");
    assert_eq!(node.registrations, 2);
    assert_eq!(
        reg.fold(2, &register(100, Class::Stateless)),
        RegistryEvent::Refused(RegistryRefusal::ClassChanged { id: NodeId(100) })
    );
}

#[test]
fn a_reboot_may_take_more_roles_never_fewer() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    assert!(matches!(
        reg.fold(1, &register(100, Class::All)),
        RegistryEvent::Reregistered { .. }
    ));
    assert_eq!(reg.get(NodeId(100)).map(|n| n.class), Some(Class::All));
    // A machine of class `all` takes a role of either class.
    assert!(matches!(
        reg.fold(2, &book(1, 100, Class::Stateless)),
        RegistryEvent::Booked { .. }
    ));
    assert!(matches!(
        reg.fold(3, &book(2, 100, Class::Storage)),
        RegistryEvent::Booked { .. }
    ));
    for (seq, narrower) in [(4, Class::Storage), (5, Class::Stateless)] {
        assert_eq!(
            reg.fold(seq, &register(100, narrower)),
            RegistryEvent::Refused(RegistryRefusal::ClassChanged { id: NodeId(100) })
        );
    }
    assert_eq!(reg.get(NodeId(100)).map(|n| n.class), Some(Class::All));
}

#[test]
fn a_machine_carries_its_reserved_tags_and_its_user_tags_once_per_request() {
    let mut reg = Registry::new([NodeId(0)]);
    let placed = |zone: &str| {
        one(&SystemCommand::RegisterNode {
            id: NodeId(100),
            addr: "n100".into(),
            class: Class::Storage,
            locality: Locality {
                zone: zone.into(),
                rack: "r1".into(),
            },
            name: "m100".into(),
            incarnation: 100,
            machine_interface: Vec::new(),
        })
    };
    reg.fold(0, &placed("zone-a"));
    let tags = |texts: &[&str]| UserTags::parse(texts).unwrap();
    let set = |request: u64, id: u64, texts: &[&str]| {
        one(&SystemCommand::SetMachineTags {
            request,
            id: NodeId(id),
            tags: tags(texts),
        })
    };
    assert_eq!(
        reg.fold(1, &set(1, 100, &["disk:nvme"])),
        RegistryEvent::Tagged { id: NodeId(100) }
    );
    assert_eq!(
        reg.fold(2, &set(2, 100, &["disk:ssd"])),
        RegistryEvent::Tagged { id: NodeId(100) }
    );
    // A retry of the first request never undoes the second.
    assert_eq!(
        reg.fold(3, &set(1, 100, &["disk:nvme"])),
        RegistryEvent::TagRepeated { request: 1 }
    );
    assert_eq!(
        reg.fold(4, &set(3, 999, &["disk:ssd"])),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(999) })
    );
    // A reboot in another zone moves its reserved tags and keeps its user
    // tags.
    reg.fold(5, &placed("zone-b"));
    let node = reg.get(NodeId(100)).unwrap();
    assert_eq!(
        node.all_tags().label(),
        "disk:ssd,paros/rack:r1,paros/roles:storage,paros/zone:zone-b"
    );
    // A registration carrying a user tag, or no class, is malformed.
    let mut forged = wire::SystemEntry::decode(placed("z").as_slice()).unwrap();
    if let Some(wire::system_entry::Kind::RegisterNode(register)) = &mut forged.kind {
        register
            .tags
            .retain(|tag| tag.key != crate::tags::reserved::ROLES);
    }
    assert_eq!(
        reg.fold(6, &forged.encode_to_vec()),
        RegistryEvent::Refused(RegistryRefusal::Malformed)
    );
    // Both cross a checkpoint.
    let mut restored = Registry::new([NodeId(0)]);
    restored.restore(6, &reg.checkpoint()).unwrap();
    assert_eq!(restored.get(NodeId(100)), reg.get(NodeId(100)));
    assert!(restored.tagged(1) && restored.tagged(2) && !restored.tagged(3));
}

#[test]
fn a_booking_takes_a_slot_of_the_nodes_own_class_within_its_capacity() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register(100, Class::Storage));
    reg.fold(1, &register(200, Class::Stateless));
    // A storage machine never takes stateless work, and the reverse.
    assert_eq!(
        reg.fold(2, &book(1, 100, Class::Stateless)),
        RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(100) })
    );
    assert_eq!(
        reg.fold(3, &book(1, 200, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(200) })
    );
    // A genesis node, or one never registered, is not bookable.
    assert_eq!(
        reg.fold(4, &book(1, 0, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
    );
    assert!(matches!(
        reg.fold(5, &book(1, 100, Class::Storage)),
        RegistryEvent::Booked { booking: 1, .. }
    ));
    // A live booking id is booked at most once.
    assert_eq!(
        reg.fold(6, &book(1, 100, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
    );
    assert!(matches!(
        reg.fold(7, &book(2, 100, Class::Storage)),
        RegistryEvent::Booked { booking: 2, .. }
    ));
    // No capacity in role slots (#837 (remove role slots)): a third
    // booking lands too, and the machine hosts three roles.
    assert!(matches!(
        reg.fold(8, &book(3, 100, Class::Storage)),
        RegistryEvent::Booked { booking: 3, .. }
    ));
    assert_eq!(reg.booked(NodeId(100)), 3);
    assert_eq!(
        reg.fold(9, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
        RegistryEvent::Released {
            booking: 1,
            node: NodeId(100)
        }
    );
    assert_eq!(
        reg.fold(10, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
        RegistryEvent::Refused(RegistryRefusal::UnknownBooking { booking: 1 })
    );
    assert_eq!(reg.booked(NodeId(100)), 2);
    // A draining node takes no new work; a retired one drops its bookings.
    reg.fold(11, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
    assert_eq!(
        reg.fold(12, &book(4, 100, Class::Storage)),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(100) })
    );
    reg.fold(13, &one(&SystemCommand::RetireNode { id: NodeId(100) }));
    assert_eq!(reg.bookings().count(), 0);
}

#[test]
fn a_registered_genesis_node_crosses_a_checkpoint() {
    use crate::checkpoint::{Folded, Folder, run};
    // A founding member registers its address (#349), then the owner
    // checkpoints: a fold that restores there keeps the address.
    let records = [register(7, Class::Storage)];
    let mut whole = Folder::new(Registry::new([NodeId(7)]));
    assert!(matches!(whole.fold(0, &records[0]), Some(Folded::Entry(_))));
    assert!(whole.state().address(NodeId(7)).is_some());
    let run = run(1, &whole.state().checkpoint(), 64);
    let mut restored = Folder::new(Registry::new([NodeId(7)]));
    restored.jump(1);
    let end = run.len() as u64;
    for (seq, record) in (1..end).zip(&run) {
        assert_eq!(restored.fold(seq, record), Some(Folded::Run));
    }
    assert_eq!(
        restored.fold(end, run.last().expect("a run has an End")),
        Some(Folded::Checkpoint {
            covers_up_to: 1,
            verified: None
        })
    );
    assert!(restored.is_whole());
    assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
    assert_eq!(restored.state().address(NodeId(7)), Some("n7"));
    assert!(restored.state().contains(NodeId(7)));
}

#[test]
fn a_registry_restored_from_its_checkpoint_is_the_registry_folded_whole() {
    use crate::checkpoint::{Folded, Folder, run};
    let records = [
        register(100, Class::Storage),
        register(200, Class::Stateless),
        book(1, 100, Class::Storage),
        register(100, Class::Storage),
        one(&SystemCommand::DrainNode { id: NodeId(200) }),
    ];
    let mut whole = Folder::new(Registry::new([NodeId(0)]));
    for (seq, record) in (0..).zip(&records) {
        assert!(matches!(whole.fold(seq, record), Some(Folded::Entry(_))));
    }
    let at = records.len() as u64;
    // A small chunk: the run spans many records.
    let run = run(at, &whole.state().checkpoint(), 8);
    assert!(run.len() > 3, "the registry's state takes several chunks");
    let end = at + run.len() as u64 - 1;
    let fold_run = |fold: &mut Folder<Registry>| {
        for (seq, record) in (at..end).zip(&run) {
            assert_eq!(fold.fold(seq, record), Some(Folded::Run));
        }
        fold.fold(end, run.last().unwrap())
    };
    // The fold that held every position verifies the checkpoint.
    assert_eq!(
        fold_run(&mut whole),
        Some(Folded::Checkpoint {
            covers_up_to: at,
            verified: Some(true)
        })
    );
    // A fold that jumped to the floor restores from it, and agrees.
    let mut restored = Folder::new(Registry::new([NodeId(0)]));
    restored.jump(at);
    assert!(!restored.is_whole());
    assert_eq!(
        fold_run(&mut restored),
        Some(Folded::Checkpoint {
            covers_up_to: at,
            verified: None
        })
    );
    assert!(restored.is_whole());
    assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
    let next = register(300, Class::Storage);
    assert_eq!(whole.fold(end + 1, &next), restored.fold(end + 1, &next));
    assert_eq!(restored.state(), whole.state());
}

#[allow(clippy::too_many_lines)]
#[test]
fn the_cell_joins_one_fleet_and_hosts_tenants_only_for_it() {
    let fleet = 0xf1ee7;
    let cell = 0xce11;
    let join = |fleet_id, cell_id, version| {
        one(&SystemCommand::JoinFleet {
            fleet_id,
            cell_id,
            version,
            universe: None,
        })
    };
    let host = |fleet_id, tenant: u64| {
        one(&SystemCommand::HostTenant {
            fleet_id,
            tenant: TenantId(tenant),
            hosted: hosted(tenant + 1),
        })
    };
    let drop = |fleet_id, tenant| {
        one(&SystemCommand::DropTenant {
            fleet_id,
            tenant: TenantId(tenant),
        })
    };
    let mut registry = Registry::new([NodeId(0)]);
    registry.fold(0, &register(0, Class::Storage));
    let footprint = |tenant: u64, slots| {
        one(&SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(TenantId(tenant)),
            tenant: TenantId(tenant),
            slots,
        })
    };
    // A tenant whose footprint was refused is never hosted (#476).
    assert_eq!(
        registry.fold(1, &footprint(301, None)),
        RegistryEvent::GrantRefused {
            key: GrantKey::Footprint(TenantId(301))
        }
    );
    let slot = Slot {
        booking: 1,
        node: NodeId(0),
        role: Role::Acceptor,
        target: BookingTarget::Tenant {
            tenant: TenantId(300),
        },
    };
    assert!(matches!(
        registry.fold(2, &footprint(300, Some(vec![slot]))),
        RegistryEvent::Granted { .. }
    ));
    // Nothing is hosted before the cell joins a fleet.
    assert_eq!(
        registry.fold(3, &host(fleet, 300)),
        RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
    );
    assert_eq!(
        registry.fold(4, &join(fleet, cell, crate::fleet::METADATA_VERSION + 1)),
        RegistryEvent::Refused(RegistryRefusal::Unsupported)
    );
    assert_eq!(
        registry.fold(5, &join(fleet, cell, 1)),
        RegistryEvent::JoinedFleet {
            fleet_id: fleet,
            cell_id: cell
        }
    );
    assert_eq!(
        registry.fold(6, &join(fleet, cell, 1)),
        RegistryEvent::Unchanged
    );
    assert_eq!(
        registry.fold(7, &join(fleet, cell + 1, 1)),
        RegistryEvent::Refused(RegistryRefusal::OtherFleet { fleet_id: fleet })
    );
    assert_eq!(
        registry.fold(8, &host(fleet, 300)),
        RegistryEvent::TenantHosted {
            tenant: TenantId(300),
            control: JournalId(301),
        }
    );
    assert_eq!(
        registry.fold(9, &host(fleet, 300)),
        RegistryEvent::Unchanged
    );
    assert_eq!(
        registry.fold(10, &host(fleet + 1, 301)),
        RegistryEvent::Refused(RegistryRefusal::OtherFleet {
            fleet_id: fleet + 1
        })
    );
    assert_eq!(
        registry.fold(11, &host(fleet, 0)),
        RegistryEvent::Refused(RegistryRefusal::Malformed)
    );
    assert!(registry.hosts(TenantId(300)));
    assert!(!registry.hosts(TenantId(301)));
    // Only the control journal the host recorded is the tenant's control.
    let control = |tenant, journal| JournalIdentifier::new(TenantId(tenant), JournalId(journal));
    assert!(registry.hosts_control(control(300, 301)));
    assert!(!registry.hosts_control(control(300, 302)));
    assert!(!registry.hosts_control(control(301, 301)));
    // The checkpoint carries the registration and the tenant list.
    let mut restored = Registry::new([NodeId(0)]);
    restored
        .restore(11, &registry.checkpoint())
        .expect("restores");
    assert_eq!(restored, registry);
    assert_eq!(
        registry.fold(12, &drop(fleet, 300)),
        RegistryEvent::TenantDropped {
            tenant: TenantId(300)
        }
    );
    assert_eq!(
        registry.fold(13, &drop(fleet, 300)),
        RegistryEvent::Unchanged
    );
    assert_eq!(registry.hosted().count(), 0);
    assert!(!registry.hosts_control(control(300, 301)));
    // A dropped tenant is never hosted again: a stale host is refused.
    assert_eq!(
        registry.fold(14, &host(fleet, 300)),
        RegistryEvent::Refused(RegistryRefusal::TenantDropped {
            tenant: TenantId(300)
        })
    );
    // A drop of a tenant never hosted still tombstones it.
    assert_eq!(
        registry.fold(15, &drop(fleet, 301)),
        RegistryEvent::TenantDropped {
            tenant: TenantId(301)
        }
    );
    assert!(registry.dropped(TenantId(301)));
    let mut restored = Registry::new([NodeId(0)]);
    restored
        .restore(15, &registry.checkpoint())
        .expect("restores");
    assert_eq!(restored, registry);
}
#[test]
fn a_grant_books_every_slot_or_none_and_a_host_needs_its_footprint() {
    let fleet = 0xf1ee7;
    let tenant = TenantId(300);
    let target = BookingTarget::Tenant { tenant };
    let mut registry = Registry::new([NodeId(0)]);
    registry.fold(0, &register(0, Class::Storage));
    registry.fold(
        1,
        &one(&SystemCommand::JoinFleet {
            fleet_id: fleet,
            cell_id: 0xce11,
            version: 1,
            universe: None,
        }),
    );
    let host = one(&SystemCommand::HostTenant {
        fleet_id: fleet,
        tenant,
        hosted: hosted(301),
    });
    assert_eq!(
        registry.fold(2, &host),
        RegistryEvent::Refused(RegistryRefusal::NoFootprint { tenant })
    );
    let slot = |booking| Slot {
        booking,
        node: NodeId(0),
        role: Role::Acceptor,
        target,
    };
    let grant = |slots: Vec<Slot>| {
        one(&SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(tenant),
            tenant,
            slots: Some(slots),
        })
    };
    // A third slot on a machine never registered: it undoes the first two.
    let stray = Slot {
        node: NodeId(55),
        ..slot(3)
    };
    assert_eq!(
        registry.fold(3, &grant(vec![slot(1), slot(2), stray])),
        RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(55) })
    );
    assert_eq!(registry.booked(NodeId(0)), 0);
    assert!(
        registry.footprint(tenant).is_none(),
        "a slot refused records nothing"
    );
    // Two slots of one role stack on one machine: a footprint is slots.
    assert!(matches!(
        registry.fold(4, &grant(vec![slot(1), slot(2)])),
        RegistryEvent::Granted { .. }
    ));
    assert_eq!(registry.booked(NodeId(0)), 2);
    assert_eq!(
        registry.fold(5, &grant(vec![slot(4)])),
        RegistryEvent::Unchanged,
        "the first grant under a key wins"
    );
    // A slot for another tenant is malformed; so is a request id of 0.
    let foreign = one(&SystemCommand::GrantCapacity {
        key: GrantKey::Request(9),
        tenant,
        slots: Some(vec![Slot {
            target: BookingTarget::Tenant {
                tenant: TenantId(7),
            },
            ..slot(5)
        }]),
    });
    assert_eq!(
        registry.fold(6, &foreign),
        RegistryEvent::Refused(RegistryRefusal::Malformed)
    );
    assert!(matches!(
        registry.fold(7, &host),
        RegistryEvent::TenantHosted { .. }
    ));
    let mut restored = Registry::new([NodeId(0)]);
    restored
        .restore(7, &registry.checkpoint())
        .expect("restores");
    assert_eq!(restored, registry);
}

#[test]
fn the_control_plane_mode_is_set_once_per_request_and_refused_when_too_wide() {
    use control_plane::ControlPlaneMode::{Auto, Manual};
    let mut reg = Registry::new([NodeId(0)]);
    // A cell that recorded no mode follows auto, target 1, and one founder
    // places machines=0.
    assert_eq!(reg.control_plane_mode(), Auto { target: 1 });
    assert_eq!(reg.control_plane(1).desired, 0);
    let set = |request: u64, mode| one(&SystemCommand::SetControlPlane { request, mode });
    // Manual machines=1 needs three storage machines: one founder refuses it.
    assert_eq!(
        reg.fold(0, &set(1, Manual { failures: 1 })),
        RegistryEvent::Refused(RegistryRefusal::ControlPlaneTooWide {
            failures: 1,
            storage: 1
        })
    );
    assert!(!reg.control_plane_applied(1));
    let storage = |seq: u64, id: u64, class: Class| {
        one(&SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class,
            locality: crate::machine::Locality::default(),
            name: format!("m{id}"),
            incarnation: u128::from(seq) + 1,
            machine_interface: Vec::new(),
        })
    };
    reg.fold(1, &storage(1, 100, Class::Storage));
    reg.fold(2, &storage(2, 200, Class::Stateless));
    // A stateless machine never counts: two storage machines still refuse.
    assert_eq!(reg.storage_machines(), 2);
    assert!(matches!(
        reg.fold(3, &set(2, Manual { failures: 1 })),
        RegistryEvent::Refused(RegistryRefusal::ControlPlaneTooWide { .. })
    ));
    reg.fold(4, &storage(4, 101, Class::Storage));
    assert_eq!(reg.control_plane(1).desired, 1, "auto grows to its target");
    assert_eq!(
        reg.fold(5, &set(3, Manual { failures: 1 })),
        RegistryEvent::ControlPlaneSet {
            mode: Manual { failures: 1 }
        }
    );
    assert_eq!(
        reg.fold(6, &set(4, Auto { target: 3 })),
        RegistryEvent::ControlPlaneSet {
            mode: Auto { target: 3 }
        }
    );
    // A retry of the first applied request never undoes the second.
    assert_eq!(
        reg.fold(7, &set(3, Manual { failures: 1 })),
        RegistryEvent::ControlPlaneRepeated { request: 3 }
    );
    assert_eq!(reg.control_plane_mode(), Auto { target: 3 });
    assert_eq!(
        reg.control_plane(1).desired,
        1,
        "three machines place machines=1"
    );
    // A request with no id, or past machines=3, is malformed.
    assert_eq!(
        reg.fold(8, &set(0, Auto { target: 1 })),
        RegistryEvent::Refused(RegistryRefusal::Malformed)
    );
    let mut forged = wire::SystemEntry::decode(set(9, Auto { target: 1 }).as_slice()).unwrap();
    if let Some(wire::system_entry::Kind::SetControlPlane(plane)) = &mut forged.kind {
        plane.failures = 4;
    }
    assert_eq!(
        reg.fold(9, &forged.encode_to_vec()),
        RegistryEvent::Refused(RegistryRefusal::Malformed)
    );
    // The mode and the applied requests cross a checkpoint.
    let mut restored = Registry::new([NodeId(0)]);
    restored.restore(9, &reg.checkpoint()).unwrap();
    assert_eq!(restored.control_plane_mode(), Auto { target: 3 });
    assert!(restored.control_plane_applied(3) && restored.control_plane_applied(4));
    assert!(!restored.control_plane_applied(1));
    assert_eq!(restored, reg);
}

#[test]
fn a_balance_move_is_judged_at_apply_and_ends_with_its_release() {
    use balancing::{BalanceOutcome, BalanceReason};
    let journal = JournalIdentifier::new(TenantId(300), JournalId(1));
    let mut reg = Registry::new([NodeId(0)]);
    let mut seq = 0;
    let mut fold = |reg: &mut Registry, record: Vec<u8>| {
        let event = reg.fold(seq, &record);
        seq += 1;
        event
    };
    for id in [100, 200] {
        fold(&mut reg, register(id, Class::Storage));
        fold(
            &mut reg,
            one(&SystemCommand::MachineAdmitted { id: NodeId(id) }),
        );
    }
    fold(&mut reg, book(1, 100, Class::Storage));
    let start = |booking: u64, from: u64, to: u64| {
        one(&SystemCommand::BalanceStarted {
            booking,
            journal,
            from: NodeId(from),
            to: NodeId(to),
            reason: BalanceReason::Roles,
        })
    };
    let refused = |booking| RegistryEvent::Refused(RegistryRefusal::NoBalanceMove { booking });
    // The booking is not on that machine, or not live.
    assert_eq!(fold(&mut reg, start(1, 200, 100)), refused(1));
    assert_eq!(fold(&mut reg, start(5, 100, 200)), refused(5));
    // A paused balancer starts nothing.
    let pause = |request, paused| one(&SystemCommand::SetBalance { request, paused });
    assert_eq!(
        fold(&mut reg, pause(3, true)),
        RegistryEvent::BalanceSet { paused: true }
    );
    assert_eq!(fold(&mut reg, start(1, 100, 200)), refused(1));
    assert_eq!(
        fold(&mut reg, pause(3, false)),
        RegistryEvent::BalanceRepeated { request: 3 }
    );
    assert!(reg.balance().paused(), "a repeat changes nothing");
    assert_eq!(
        fold(&mut reg, pause(4, false)),
        RegistryEvent::BalanceSet { paused: false }
    );
    assert_eq!(
        fold(&mut reg, start(1, 100, 200)),
        RegistryEvent::BalanceStarted { booking: 1 }
    );
    // One at a time.
    assert_eq!(fold(&mut reg, start(1, 100, 200)), refused(1));
    assert_eq!(reg.balance().moving().map(|m| m.to), Some(NodeId(200)));
    // The release of its booking ends it.
    let released = fold(
        &mut reg,
        one(&SystemCommand::ReleaseCapacity { booking: 1 }),
    );
    assert!(matches!(
        released,
        RegistryEvent::Released { booking: 1, .. }
    ));
    assert!(reg.balance().moving().is_none());
    let ended: Vec<_> = reg.balance().ended().map(|e| e.outcome).collect();
    assert_eq!(ended, [BalanceOutcome::Done]);
    // An abandon of a move not in flight is refused; of one in flight ends it.
    assert_eq!(
        fold(
            &mut reg,
            one(&SystemCommand::BalanceAbandoned { booking: 1 })
        ),
        refused(1)
    );
    fold(&mut reg, book(2, 100, Class::Storage));
    let journal2 = JournalIdentifier::new(TenantId(300), JournalId(2));
    let started = fold(
        &mut reg,
        one(&SystemCommand::BalanceStarted {
            booking: 2,
            journal: journal2,
            from: NodeId(100),
            to: NodeId(200),
            reason: BalanceReason::Busy,
        }),
    );
    assert_eq!(started, RegistryEvent::BalanceStarted { booking: 2 });
    assert_eq!(
        fold(
            &mut reg,
            one(&SystemCommand::BalanceAbandoned { booking: 2 })
        ),
        RegistryEvent::BalanceAbandoned { booking: 2 }
    );
    let ended: Vec<_> = reg.balance().ended().map(|e| e.outcome).collect();
    assert_eq!(ended, [BalanceOutcome::Done, BalanceOutcome::Abandoned]);
    // The state crosses a checkpoint.
    let restored = {
        let mut restored = Registry::new([NodeId(0)]);
        restored
            .restore(reg.next_seq() - 1, &reg.checkpoint())
            .expect("restores");
        restored
    };
    assert_eq!(restored.balance(), reg.balance());
}
