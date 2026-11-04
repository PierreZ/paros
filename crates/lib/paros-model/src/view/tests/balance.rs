//! The balance view (#687 (rebalance a cell)): a move in flight and the
//! ended moves, with the names people read.

use super::*;
use crate::registry::balancing::BalanceReason;

fn fold(registry: &mut Registry, command: &SystemCommand) {
    registry.fold(registry.next_seq(), &command.encode());
}

/// Book acceptor slot `booking` of acme's `orders` on alpha (node 1), and
/// start its move to gamma (node 3).
fn start_move(registry: &mut Registry, booking: u64) {
    book_orders(registry, booking, 1);
    fold(
        registry,
        &SystemCommand::BalanceStarted {
            booking,
            journal: identifier(50, 501),
            from: NodeId(1),
            to: NodeId(3),
            reason: BalanceReason::Roles,
        },
    );
}

#[test]
fn a_move_shows_its_names_in_flight_and_its_outcome_once_ended() {
    let tenants = [acme_with_orders()];
    let mut registry = registry();
    fold(
        &mut registry,
        &SystemCommand::MachineAdmitted { id: NodeId(3) },
    );
    start_move(&mut registry, 90);
    let balance = cell_balance(&registry, &tenants, 0);
    assert_eq!(balance.spread, 1, "a spread is at least one role");
    assert_eq!(balance.state, "moving");
    let moving = balance.moving.expect("the move in flight");
    assert_eq!(
        (moving.tenant, moving.journal, moving.from, moving.to),
        (50, 501, 1, 3)
    );
    assert_eq!(
        (
            moving.tenant_name.as_slice(),
            moving.journal_name.as_slice(),
            moving.from_name.as_str(),
            moving.to_name.as_str(),
        ),
        (&b"acme"[..], &b"orders"[..], "alpha", "gamma")
    );
    assert_eq!(
        (moving.reason.as_str(), moving.outcome.as_str()),
        ("roles", "")
    );
    assert_eq!(moving.ended, 0);
    assert!(balance.ended.is_empty());
    // The release of its booking ends it: done.
    fold(
        &mut registry,
        &SystemCommand::ReleaseCapacity { booking: 90 },
    );
    let done_at = registry.next_seq() - 1;
    start_move(&mut registry, 91);
    fold(
        &mut registry,
        &SystemCommand::BalanceAbandoned { booking: 91 },
    );
    let abandoned_at = registry.next_seq() - 1;
    let balance = cell_balance(&registry, &tenants, 1);
    assert!(balance.moving.is_none());
    assert_ne!(balance.state, "moving");
    let ended: Vec<(&str, u64, &[u8])> = balance
        .ended
        .iter()
        .map(|e| (e.outcome.as_str(), e.ended, e.journal_name.as_slice()))
        .collect();
    assert_eq!(
        ended,
        [
            ("done", done_at, &b"orders"[..]),
            ("abandoned", abandoned_at, &b"orders"[..])
        ]
    );
}

#[test]
fn a_move_of_a_journal_the_view_cannot_name_shows_empty_names() {
    let mut registry = registry();
    fold(
        &mut registry,
        &SystemCommand::MachineAdmitted { id: NodeId(3) },
    );
    start_move(&mut registry, 90);
    // No tenant control fold: no journal name; the tenant is still hosted.
    let moving = cell_balance(&registry, &[], 1)
        .moving
        .expect("the move in flight");
    assert_eq!(moving.tenant_name, b"acme");
    assert!(moving.journal_name.is_empty());
}

#[test]
fn a_paused_balancer_shows_paused() {
    let mut registry = registry();
    fold(
        &mut registry,
        &SystemCommand::SetBalance {
            request: 1,
            paused: true,
        },
    );
    let balance = cell_balance(&registry, &[], 1);
    assert!(balance.paused);
    assert_eq!(balance.state, "paused");
}
