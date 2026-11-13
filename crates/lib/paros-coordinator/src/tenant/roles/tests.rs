//! The role counts' plan over a folded registry: which machines take a
//! counted slot, which slot leaves, and what a count books or releases.

use super::*;
use crate::fixture;
use paros_core::membership::{AcceptorConfig, QuorumSystem};
use paros_model::tenant::Desired;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn ids(nodes: &[NodeId]) -> Vec<u64> {
    nodes.iter().map(|n| n.0).collect()
}

const TENANT: TenantId = TenantId(50);
const FRONTENDS: BookingTarget = BookingTarget::Tenant { tenant: TENANT };

fn journal_target() -> BookingTarget {
    BookingTarget::Journal(fixture::journal(50, 52))
}

/// Initial members 1 and 2 (storage) hosting tenant 50; storage machine 3 and
/// stateless machines 4, 5 and 6, all admitted; storage machine 7 never
/// admitted. Journal (50, 52) has acceptors on 1 and 2.
fn cell() -> Registry {
    let mut registry = fixture::cell(&[1, 2]);
    fixture::register(&mut registry, 3, Class::Storage);
    for id in [4, 5, 6] {
        fixture::register(&mut registry, id, Class::Stateless);
    }
    fixture::register(&mut registry, 7, Class::Storage);
    for id in [3, 4, 5, 6] {
        fixture::admit(&mut registry, id);
    }
    fixture::host(&mut registry, 50, 51, 1);
    fixture::book(&mut registry, 1, 1, (Role::Acceptor, journal_target()));
    fixture::book(&mut registry, 2, 2, (Role::Acceptor, journal_target()));
    registry
}

fn plan(
    registry: &Registry,
    leaving: &Leaving,
    (role, target): (Role, BookingTarget),
    wanted: u32,
) -> (usize, Vec<Step>) {
    let desk = fixture::desk(&[1, 2], 3);
    let (want, steps) =
        count_steps(registry, &desk, leaving, (role, target), wanted).expect("a plan");
    assert_eq!((want.role, want.target), (role, target));
    (want.count, steps)
}

fn booked(steps: &[Step]) -> BTreeSet<NodeId> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Book { node } => Some(*node),
            Step::Release { .. } => None,
        })
        .collect()
}

#[test]
fn a_counted_role_goes_to_admitted_staying_machines_of_its_class() {
    let mut registry = cell();
    let none = Leaving::default();
    assert_eq!(
        ids(&eligible(&registry, &none, Class::Stateless)),
        [4, 5, 6]
    );
    assert_eq!(
        ids(&eligible(&registry, &none, Class::Storage)),
        [1, 2, 3],
        "machine 7 never served the cell"
    );
    fixture::drain(&mut registry, 5);
    let gone = Leaving {
        gone: set(&[6]),
        ..Leaving::default()
    };
    assert_eq!(
        ids(&eligible(&registry, &gone, Class::Stateless)),
        [4],
        "a draining or gone machine takes no slot"
    );
}

#[test]
fn a_journal_counts_batchers_only_when_it_has_many_writers() {
    let config = AcceptorConfig::new(
        vec![NodeId(1), NodeId(2), NodeId(3)],
        QuorumSystem::Majority,
    );
    let mut journal = TenantJournal {
        name: b"j".to_vec(),
        writer: WriterMode::Single,
        desired: Desired {
            proxies: 2,
            batchers: 3,
            replicas: 4,
            ..Desired::DEFAULT
        },
        config: config.clone(),
        placed: config,
        deleted_at: None,
        tags: paros_model::tags::UserTags::default(),
        store: paros_model::tenant::StoreDurability::default(),
    };
    assert_eq!(journal_count(&journal, Role::ProxyLeader), 2);
    assert_eq!(journal_count(&journal, Role::Batcher), 0, "single writer");
    assert_eq!(journal_count(&journal, Role::Replica), 4);
    journal.writer = WriterMode::Multi;
    assert_eq!(journal_count(&journal, Role::Batcher), 3);
}

#[test]
fn a_slot_never_shares_a_machine_with_another_kind_or_storage_instance() {
    let current = vec![
        (1, fixture::booking(1, Role::Acceptor, journal_target())),
        (2, fixture::booking(4, Role::ProxyLeader, journal_target())),
        (3, fixture::booking(3, Role::Replica, journal_target())),
        (4, fixture::booking(5, Role::Batcher, journal_target())),
    ];
    assert_eq!(
        taken(&current, Role::Replica),
        set(&[1, 4, 5]),
        "an acceptor's machine and every stateless one; a replica's own is not taken"
    );
    assert_eq!(
        taken(&current, Role::ProxyLeader),
        set(&[1, 3]),
        "storage machines; a batcher's machine stays open"
    );
}

#[test]
fn a_count_books_the_slots_it_lacks_capped_at_the_machines() {
    let registry = cell();
    let none = Leaving::default();
    let (count, steps) = plan(&registry, &none, (Role::Frontend, FRONTENDS), 2);
    assert_eq!(count, 2);
    assert_eq!(steps.len(), 2, "two books");
    assert!(
        booked(&steps).is_subset(&set(&[4, 5, 6])),
        "on stateless machines"
    );
    let (count, steps) = plan(&registry, &none, (Role::Frontend, FRONTENDS), 9);
    assert_eq!(count, 3, "capped at the three stateless machines");
    assert_eq!(booked(&steps), set(&[4, 5, 6]));
    let (count, steps) = plan(&registry, &none, (Role::Frontend, FRONTENDS), 0);
    assert_eq!(count, 0);
    assert!(steps.is_empty(), "nothing wanted, nothing held");
}

#[test]
fn a_replica_count_skips_the_journals_acceptor_machines() {
    let registry = cell();
    let (count, steps) = plan(
        &registry,
        &Leaving::default(),
        (Role::Replica, journal_target()),
        3,
    );
    assert_eq!(count, 1, "machines 1 and 2 hold the journal's acceptors");
    assert_eq!(booked(&steps), set(&[3]));
}

#[test]
fn a_count_held_books_nothing_and_a_smaller_one_releases() {
    let mut registry = cell();
    fixture::book(&mut registry, 40, 4, (Role::Frontend, FRONTENDS));
    fixture::book(&mut registry, 50, 5, (Role::Frontend, FRONTENDS));
    let none = Leaving::default();
    let (_, steps) = plan(&registry, &none, (Role::Frontend, FRONTENDS), 2);
    assert!(steps.is_empty(), "the count holds");
    let (count, steps) = plan(&registry, &none, (Role::Frontend, FRONTENDS), 1);
    assert_eq!(count, 1);
    assert!(
        matches!(
            steps.as_slice(),
            [Step::Release {
                booking: 40 | 50,
                ..
            }]
        ),
        "one slot goes back: {steps:?}"
    );
}

#[test]
fn a_slot_on_a_machine_that_leaves_goes_first() {
    let mut registry = cell();
    fixture::book(&mut registry, 40, 4, (Role::Frontend, FRONTENDS));
    fixture::book(&mut registry, 50, 5, (Role::Frontend, FRONTENDS));
    let none = Leaving::default();
    assert_eq!(
        leaving_slot(&registry, &none, (Role::Frontend, FRONTENDS)),
        None
    );
    let gone = Leaving {
        gone: set(&[5]),
        ..Leaving::default()
    };
    assert_eq!(
        leaving_slot(&registry, &gone, (Role::Frontend, FRONTENDS)),
        Some(50)
    );
    fixture::drain(&mut registry, 4);
    assert_eq!(
        leaving_slot(&registry, &none, (Role::Frontend, FRONTENDS)),
        Some(40),
        "a draining machine leaves"
    );
}

#[test]
fn a_replica_leaves_a_machine_that_takes_one_of_its_acceptors() {
    let mut registry = cell();
    fixture::book(&mut registry, 30, 3, (Role::Replica, journal_target()));
    let none = Leaving::default();
    assert_eq!(
        leaving_slot(&registry, &none, (Role::Replica, journal_target())),
        None
    );
    // A move placed an acceptor on the replica's machine.
    fixture::book(&mut registry, 31, 3, (Role::Acceptor, journal_target()));
    assert_eq!(
        leaving_slot(&registry, &none, (Role::Replica, journal_target())),
        Some(30)
    );
}

#[test]
fn a_stray_replica_lands_on_a_member_of_a_journal_with_none() {
    let mut registry = cell();
    assert_eq!(
        stray_replica(&registry, journal_target()),
        Some(NodeId(1)),
        "the first member"
    );
    assert_eq!(
        stray_replica(&registry, BookingTarget::Journal(fixture::journal(50, 53))),
        None,
        "a journal with no acceptor has no member"
    );
    fixture::book(&mut registry, 9, 3, (Role::Replica, journal_target()));
    assert_eq!(
        stray_replica(&registry, journal_target()),
        None,
        "a journal that holds a replica takes no stray"
    );
}
