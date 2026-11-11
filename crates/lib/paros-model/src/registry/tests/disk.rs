//! Disk counts (#985 (placement counts store disk)): the registry counts one
//! store per booking that opens one, refuses a booking the disk cannot
//! hold, releases the count with the slot, and keeps the disk across a
//! checkpoint.

use super::*;
use crate::machine::Disk;

const STORE: u64 = 64 << 20;

fn register_with(id: u64, class: Class, disk: Disk) -> Vec<u8> {
    one(&SystemCommand::RegisterNode {
        id: NodeId(id),
        addr: format!("n{id}"),
        class,
        locality: crate::machine::Locality::default(),
        name: format!("m{id}"),
        incarnation: u128::from(id),
        machine_interface: Vec::new(),
        disk,
    })
}

fn book_as(booking: u64, node: u64, role: Role, target: BookingTarget) -> Vec<u8> {
    one(&SystemCommand::BookCapacity {
        booking,
        node: NodeId(node),
        role,
        target,
    })
}

fn journal(id: u64) -> BookingTarget {
    BookingTarget::Journal(JournalIdentifier::new(TenantId(300), JournalId(id)))
}

#[test]
fn only_a_storage_role_of_a_journal_or_set_opens_a_store() {
    let set = BookingTarget::set_of(TenantId(300));
    let tenant = BookingTarget::Tenant {
        tenant: TenantId(300),
    };
    for role in [Role::Acceptor, Role::Replica, Role::Matchmaker] {
        assert!(Booking::opens_store_for(role, journal(1)));
        assert!(Booking::opens_store_for(role, set));
        assert!(!Booking::opens_store_for(role, tenant));
    }
    for role in Role::of(Class::Stateless) {
        assert!(!Booking::opens_store_for(role, journal(1)));
        assert!(!Booking::opens_store_for(role, tenant));
    }
}

#[test]
fn a_booking_the_disk_cannot_hold_is_refused_and_a_release_gives_the_room_back() {
    let mut reg = Registry::new([NodeId(0)]);
    // Two stores and a half: the disk holds two.
    reg.fold(
        0,
        &register_with(100, Class::All, Disk::new(5 * STORE / 2, STORE)),
    );
    assert_eq!(reg.store_room(NodeId(100)), Some(2));
    assert!(matches!(
        reg.fold(1, &book_as(1, 100, Role::Acceptor, journal(1))),
        RegistryEvent::Booked { .. }
    ));
    assert!(matches!(
        reg.fold(
            2,
            &book_as(
                2,
                100,
                Role::Matchmaker,
                BookingTarget::set_of(TenantId(300))
            )
        ),
        RegistryEvent::Booked { .. }
    ));
    assert_eq!(reg.stores(NodeId(100)), 2);
    assert_eq!(reg.store_room(NodeId(100)), Some(0));
    assert_eq!(
        reg.fold(3, &book_as(3, 100, Role::Replica, journal(2))),
        RegistryEvent::Refused(RegistryRefusal::DiskFull { id: NodeId(100) })
    );
    // A refused booking spends no id: the writer books it elsewhere.
    assert!(!reg.spent().any(|id| id == 3));
    // A role that opens no store still lands on a full disk.
    assert!(matches!(
        reg.fold(4, &book_as(4, 100, Role::ProxyLeader, journal(1))),
        RegistryEvent::Booked { .. }
    ));
    assert_eq!(reg.stores(NodeId(100)), 2);
    // A release gives one store back.
    assert!(matches!(
        reg.fold(5, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
        RegistryEvent::Released { .. }
    ));
    assert_eq!(reg.store_room(NodeId(100)), Some(1));
    assert!(matches!(
        reg.fold(6, &book_as(6, 100, Role::Replica, journal(2))),
        RegistryEvent::Booked { .. }
    ));
}

#[test]
fn an_unreported_disk_holds_any_number_of_stores() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(0, &register_with(100, Class::Storage, Disk::UNKNOWN));
    for j in 1..=50 {
        assert!(matches!(
            reg.fold(j, &book_as(j, 100, Role::Acceptor, journal(j))),
            RegistryEvent::Booked { .. }
        ));
    }
    assert_eq!(reg.store_room(NodeId(100)), None);
    // An initial member the seed holds reported no disk either.
    assert_eq!(reg.store_room(NodeId(0)), None);
}

#[test]
fn a_restart_reports_a_new_disk_and_keeps_the_stores_booked() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(
        0,
        &register_with(100, Class::Storage, Disk::new(2 * STORE, STORE)),
    );
    reg.fold(1, &book_as(1, 100, Role::Acceptor, journal(1)));
    reg.fold(2, &book_as(2, 100, Role::Acceptor, journal(2)));
    // The machine comes back with a disk of one store: both stay booked,
    // and no new store lands.
    reg.fold(
        3,
        &register_with(100, Class::Storage, Disk::new(STORE, STORE)),
    );
    assert_eq!(reg.stores(NodeId(100)), 2);
    assert_eq!(reg.store_room(NodeId(100)), Some(0));
    assert_eq!(
        reg.fold(4, &book_as(4, 100, Role::Acceptor, journal(4))),
        RegistryEvent::Refused(RegistryRefusal::DiskFull { id: NodeId(100) })
    );
    // A bigger disk takes it.
    reg.fold(
        5,
        &register_with(100, Class::Storage, Disk::new(3 * STORE, STORE)),
    );
    assert!(matches!(
        reg.fold(6, &book_as(6, 100, Role::Acceptor, journal(4))),
        RegistryEvent::Booked { .. }
    ));
}

#[test]
fn a_grant_with_a_slot_the_disk_cannot_hold_books_none() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(
        0,
        &register_with(100, Class::Storage, Disk::new(STORE, STORE)),
    );
    let slot = |booking: u64, target| Slot {
        booking,
        node: NodeId(100),
        role: Role::Acceptor,
        target,
    };
    let grant = SystemCommand::GrantCapacity {
        key: GrantKey::Request(9),
        tenant: TenantId(300),
        slots: Some(vec![slot(1, journal(1)), slot(2, journal(2))]),
    };
    assert_eq!(
        reg.fold(1, &one(&grant)),
        RegistryEvent::Refused(RegistryRefusal::DiskFull { id: NodeId(100) })
    );
    assert_eq!(reg.stores(NodeId(100)), 0, "a grant books all or none");
    assert!(reg.grant_of(GrantKey::Request(9)).is_none());
}

#[test]
fn the_disk_crosses_a_checkpoint() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(
        0,
        &register_with(100, Class::Storage, Disk::new(3 * STORE, STORE)),
    );
    reg.fold(1, &book_as(1, 100, Role::Acceptor, journal(1)));
    let mut restored = Registry::new([NodeId(0)]);
    restored.restore(1, &reg.checkpoint()).unwrap();
    assert_eq!(restored.get(NodeId(100)), reg.get(NodeId(100)));
    assert_eq!(restored.store_room(NodeId(100)), Some(2));
}

#[test]
fn placement_reads_the_room_the_registry_counts() {
    let mut reg = Registry::new([NodeId(0)]);
    reg.fold(
        0,
        &register_with(100, Class::Storage, Disk::new(STORE, STORE)),
    );
    reg.fold(1, &one(&SystemCommand::MachineAdmitted { id: NodeId(100) }));
    let offered = crate::placement::machines(&reg, [NodeId(100)]);
    assert_eq!(offered[0].room, Some(1));
    reg.fold(2, &book_as(1, 100, Role::Acceptor, journal(1)));
    let offered = crate::placement::machines(&reg, [NodeId(100)]);
    assert_eq!(offered[0].room, Some(0));
}
