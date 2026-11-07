//! A move's decisions over a folded registry: which slots stay, which
//! journal moves next, what the target keeps, and when a cold member holds
//! the move back.

use super::*;
use crate::fixture;
use paros_core::journal::TenantId;
use paros_model::machine::Class;
use paros_model::registry::{SlowFacts, SystemCommand};

#[test]
fn a_member_lacks_the_slots_the_leader_chose_and_it_did_not() {
    assert_eq!(lacks(None, None), 0, "nothing chosen, nothing lacked");
    assert_eq!(lacks(None, Some(4)), 0);
    assert_eq!(lacks(Some(9), None), 10, "an empty member lacks every slot");
    assert_eq!(lacks(Some(9), Some(4)), 5);
    assert_eq!(lacks(Some(4), Some(9)), 0, "a member ahead lacks nothing");
}

#[test]
fn a_handoff_goes_to_a_member_of_both_configurations() {
    // The leader 1 leaves; 4 joins; 2 and 3 stay.
    let members = [1, 2, 3];
    let target = [2, 3, 4];
    assert_eq!(
        handoff_target(NodeId(1), &members, &target),
        Some(NodeId(2))
    );
    // A new member alone takes no leadership: it holds no Phase-2 state.
    assert_eq!(handoff_target(NodeId(1), &[1], &[4]), None);
    // The leader is never its own successor.
    assert_eq!(handoff_target(NodeId(2), &[2], &[2, 4]), None);
}

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn acceptor(journal: JournalIdentifier) -> (Role, BookingTarget) {
    (Role::Acceptor, BookingTarget::Journal(journal))
}

/// Founder 1 hosting tenants 50 (control journal 51) and 60 (control 61),
/// and admitted storage machines 2, 3 and 4. Journal (50, 52) has acceptors on
/// 1, 2 and 3 (bookings 1, 2, 3); journal (60, 62) on 2, 3 and 4 (bookings
/// 12, 13, 14); the control journal (50, 51) on 1 and 4 (bookings 21, 24).
fn cell() -> Registry {
    let mut registry = fixture::cell(&[1]);
    for id in [2, 3, 4] {
        fixture::register(&mut registry, id, Class::Storage);
        fixture::admit(&mut registry, id);
    }
    fixture::host(&mut registry, 50, 51, 1);
    fixture::host(&mut registry, 60, 61, 1);
    for node in [1, 2, 3] {
        fixture::book(
            &mut registry,
            node,
            node,
            acceptor(fixture::journal(50, 52)),
        );
    }
    for node in [2, 3, 4] {
        fixture::book(
            &mut registry,
            10 + node,
            node,
            acceptor(fixture::journal(60, 62)),
        );
    }
    for node in [1, 4] {
        fixture::book(
            &mut registry,
            20 + node,
            node,
            acceptor(fixture::journal(50, 51)),
        );
    }
    registry
}

fn slow(registry: &mut Registry, id: u64) {
    fixture::fold(
        registry,
        &SystemCommand::MachineSlow {
            id: NodeId(id),
            incarnation: fixture::INCARNATION,
            slow: true,
            facts: SlowFacts::default(),
        },
    );
    assert!(registry.is_slow(NodeId(id)), "machine {id} is slow");
}

fn rebalance(booking: u64, from: u64, to: u64) -> Leaving {
    Leaving {
        rebalance: Some(Rebalance {
            journal: fixture::journal(50, 52),
            booking,
            from: NodeId(from),
            to: NodeId(to),
            reason: paros_model::registry::balancing::BalanceReason::Roles,
        }),
        ..Leaving::default()
    }
}

#[test]
fn a_slot_stays_on_a_registered_machine_neither_slow_nor_gone() {
    let mut registry = cell();
    let none = Leaving::default();
    assert!(stays(&registry, &none, NodeId(1)));
    assert!(!stays(&registry, &none, NodeId(9)), "an unknown machine");
    fixture::drain(&mut registry, 2);
    assert!(!stays(&registry, &none, NodeId(2)), "a draining machine");
    let gone = Leaving {
        gone: set(&[3]),
        ..Leaving::default()
    };
    assert!(!stays(&registry, &gone, NodeId(3)), "a gone machine");
    slow(&mut registry, 4);
    assert!(!stays(&registry, &none, NodeId(4)), "a slow machine");
    let held = Leaving {
        held: set(&[4]),
        ..Leaving::default()
    };
    assert!(
        stays(&registry, &held, NodeId(4)),
        "a window holds a slow machine's roles"
    );
}

#[test]
fn a_move_takes_the_journals_with_a_leaving_slot_in_turn() {
    let mut registry = cell();
    let none = Leaving::default();
    assert_eq!(next_move(&registry, &none, None), None, "nothing leaves");
    fixture::drain(&mut registry, 3);
    let (first, slots) = next_move(&registry, &none, None).expect("a move");
    assert_eq!(first, fixture::journal(50, 52));
    assert_eq!(
        slots.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [1, 2, 3],
        "every acceptor slot of the journal"
    );
    let (second, _) = next_move(&registry, &none, Some(first)).expect("a move");
    assert_eq!(second, fixture::journal(60, 62));
    let (again, _) = next_move(&registry, &none, Some(second)).expect("a move");
    assert_eq!(again, first, "the turn wraps");
}

#[test]
fn a_control_journal_or_a_dropped_tenants_journal_never_moves_here() {
    let mut registry = cell();
    fixture::drain(&mut registry, 4);
    let none = Leaving::default();
    let (journal, _) = next_move(&registry, &none, None).expect("a move");
    assert_eq!(
        journal,
        fixture::journal(60, 62),
        "the control journal (50, 51) moves through the controls step"
    );
    fixture::fold(
        &mut registry,
        &SystemCommand::DropTenant {
            fleet_id: fixture::FLEET,
            tenant: TenantId(60),
        },
    );
    assert_eq!(next_move(&registry, &none, None), None, "a dropped tenant");
    assert!(is_control(&registry, fixture::journal(50, 51)));
    assert!(!is_control(&registry, fixture::journal(50, 52)));
}

#[test]
fn a_rebalanced_slot_leaves_though_its_machine_stays() {
    let registry = cell();
    let leaving = rebalance(2, 2, 4);
    let (journal, slots) = next_move(&registry, &leaving, None).expect("a move");
    assert_eq!(journal, fixture::journal(50, 52));
    let (staying, gone) = split(&registry, &leaving, &slots);
    assert_eq!(
        staying.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [1, 3]
    );
    assert_eq!(gone.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [2]);
    assert!(leaving.rebalances(2) && !leaving.rebalances(3));
}

#[test]
fn a_split_leaves_the_slots_on_machines_that_leave() {
    let mut registry = cell();
    fixture::drain(&mut registry, 3);
    let gone = Leaving {
        gone: set(&[4]),
        ..Leaving::default()
    };
    let (_, slots) = next_move(&registry, &gone, Some(fixture::journal(50, 52))).expect("a move");
    let (staying, leaving) = split(&registry, &gone, &slots);
    assert_eq!(staying.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [12]);
    assert_eq!(
        leaving.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [13, 14]
    );
}

#[test]
fn a_move_target_keeps_the_staying_members_then_the_spares() {
    let slot = |id: u64| {
        (
            id,
            fixture::booking(
                id,
                Role::Acceptor,
                BookingTarget::Journal(fixture::journal(50, 52)),
            ),
        )
    };
    let staying = [slot(2), slot(3), slot(5), slot(6)];
    assert_eq!(move_target(&[1, 2, 3], &staying, 3), [2, 3, 5]);
    assert_eq!(
        move_target(&[1, 2, 3], &staying, 5),
        [2, 3, 5, 6],
        "every spare"
    );
    assert_eq!(
        move_target(&[1, 2, 3], &staying, 2),
        [2, 3],
        "no room for a spare"
    );
    assert_eq!(move_target(&[1], &[slot(5)], 3), [5]);
}

#[test]
fn a_ballot_is_at_or_above_by_round_then_node() {
    let b = |round, node| Ballot { round, node };
    assert!(at_or_above(Some(&b(2, 1)), Some(&b(2, 1))));
    assert!(at_or_above(Some(&b(3, 1)), Some(&b(2, 9))));
    assert!(at_or_above(Some(&b(2, 2)), Some(&b(2, 1))));
    assert!(!at_or_above(Some(&b(2, 1)), Some(&b(2, 2))));
    assert!(
        at_or_above(Some(&b(0, 1)), None),
        "any ballot is above none"
    );
    assert!(!at_or_above(None, Some(&b(0, 1))));
    assert!(!at_or_above(None, None));
}

#[test]
fn a_cold_member_holds_its_move_back_at_most_the_warm_rounds() {
    let journal = fixture::journal(50, 52);
    let mut moves = Moves::new(Warm { lag: 4, rounds: 2 });
    assert!(!moves.waited(journal, false), "a first cold round waits");
    assert!(!moves.waited(journal, false), "a second one too");
    assert!(
        moves.waited(journal, false),
        "past the rounds the move goes on"
    );
    assert!(
        moves.warming.is_empty(),
        "a move that goes on waits no more"
    );
    assert!(!moves.waited(journal, false), "a new wait starts from zero");
    assert!(moves.waited(journal, true), "a warm member goes on at once");
    assert!(moves.warming.is_empty());
}

fn listed_journal(deleted_at: Option<u64>) -> TenantJournal {
    let config = AcceptorConfig::new(
        vec![NodeId(1), NodeId(2), NodeId(3)],
        paros_core::membership::QuorumSystem::Majority,
    );
    TenantJournal {
        name: b"j".to_vec(),
        writer: paros_core::journal::state::WriterMode::Single,
        desired: Desired::DEFAULT,
        config: config.clone(),
        placed: config,
        deleted_at,
        tags: paros_model::tags::UserTags::default(),
        store: paros_model::tenant::StoreDurability::default(),
    }
}

#[test]
fn a_move_reads_a_deleted_journal_as_deleted_and_a_missing_one_as_unknown() {
    let live = listed_journal(None);
    assert_eq!(
        Listed::of(Some(&live)),
        Listed::Live(live.desired, live.placed.clone()),
        "a live journal moves with its desired mode and placement"
    );
    // A deleted journal's slots hold nothing: the move releases them.
    assert_eq!(Listed::of(Some(&listed_journal(Some(7)))), Listed::Deleted);
    // No record yet: a create in flight may have booked the slots first,
    // so the move must not release them.
    assert_eq!(Listed::of(None), Listed::Unknown);
}
