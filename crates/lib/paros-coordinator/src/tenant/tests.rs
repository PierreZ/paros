//! The tenant desk's pure parts: the claim memo of a watch round, the
//! storage machines a create may take, and a create's placement under the
//! zone rule.

use super::*;
use crate::fixture;
use paros_model::machine::Class;
use paros_model::tenant::Durability;

#[test]
fn a_forgotten_tenant_is_claimed_again_and_others_stay() {
    // Sweep recipe `276942:3538479639750839856` on seed
    // 13518265258372241998 on 69e683a: a term's fold of a tenant control
    // journal outlived a move out and back. An arrival forgets it, so the
    // next use claims the journal again and reads it to its tail.
    let mut desk = fixture::desk(&[1, 2, 3], 9);
    let (back, kept) = (TenantId(7), TenantId(8));
    for tenant in [back, kept] {
        let fold = Checkpointer::with_uuid(
            JournalIdentifier::new(tenant, JournalId(70)),
            desk.uuid,
            TenantControl::new(tenant, JournalId(70)),
            desk.policy,
        );
        desk.tenants.insert(tenant, fold);
    }
    assert!(desk.forget(back), "the desk held the tenant");
    assert!(!desk.tenants.contains_key(&back));
    assert!(desk.tenants.contains_key(&kept), "another tenant stays");
    assert!(!desk.forget(back), "a second forget finds nothing");
}

#[test]
fn an_unavailable_claim_is_not_asked_again_until_the_next_round() {
    let (sick, other) = (TenantId(7), TenantId(8));
    let mut unread = Unread::default();
    assert!(!unread.holds(sick), "a fresh round remembers nothing");
    unread.note(sick, &JournalAnswer::Unavailable);
    assert!(unread.holds(sick), "the round asks it no more");
    assert!(!unread.holds(other), "another tenant is still asked");
    // A fenced desk is the desk's own stop, never a memo.
    unread.note(other, &JournalAnswer::NotCoordinator);
    assert!(!unread.holds(other));
    unread.clear();
    assert!(!unread.holds(sick), "the next round asks it again");
}

fn machines(zones: &[&str]) -> Vec<placement::Machine> {
    zones
        .iter()
        .zip(1..)
        .map(|(zone, id)| placement::Machine {
            node: NodeId(id),
            class: Class::Storage,
            zone: (*zone).to_string(),
            busy: 0,
            booked: 0,
            light: 0,
            up: true,
            low_space: false,
            room: None,
        })
        .collect()
}

fn want(count: usize) -> Want {
    Want {
        role: Role::Acceptor,
        target: BookingTarget::Journal(JournalIdentifier::new(TenantId(50), JournalId(52))),
        count,
        disk: true,
    }
}

fn desired(survive_machines: u32) -> Desired {
    Desired {
        durability: Durability::new(survive_machines, 1).expect("a durability"),
        ..Desired::DEFAULT
    }
}

fn booked(plan: &Result<Vec<Step>, ZonePlan>) -> Vec<u64> {
    let Ok(steps) = plan else {
        panic!("a plan");
    };
    placement::booked(steps).iter().map(|n| n.0).collect()
}

#[test]
fn a_create_over_three_zones_meets_the_zone_rule() {
    let machines = machines(&["a", "a", "b", "c"]);
    let plan = zone_plan(&want(3), &machines, &[], 9, desired(1));
    let nodes = booked(&plan);
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&3) && nodes.contains(&4), "one per zone");
}

#[test]
fn a_shape_no_draw_lays_out_across_the_zones_is_refused() {
    // Five acceptors over five machines, three in one zone: that zone
    // holds a majority alone under every draw.
    let machines = machines(&["a", "a", "a", "b", "c"]);
    assert!(matches!(
        zone_plan(&want(5), &machines, &[], 9, desired(1)),
        Err(ZonePlan::Refused)
    ));
    // Where the rule does not bind (F = 0, or fewer than three zones),
    // the first plan stands.
    assert_eq!(
        booked(&zone_plan(&want(5), &machines, &[], 9, desired(0))).len(),
        5
    );
    let two = self::machines(&["a", "a", "a", "b", "b"]);
    assert_eq!(
        booked(&zone_plan(&want(5), &two, &[], 9, desired(1))).len(),
        5
    );
}

#[test]
fn a_create_with_too_few_machines_is_short() {
    let machines = machines(&["a", "b", "c"]);
    assert!(matches!(
        zone_plan(&want(4), &machines, &[], 9, desired(1)),
        Err(ZonePlan::Short(_))
    ));
}

#[test]
fn a_journal_with_one_partition_is_placed_on_at_most_every_storage_machine() {
    assert_eq!(placed_count(&desired(1), 5), 3);
    assert_eq!(placed_count(&desired(1), 2), 2, "capped");
    assert_eq!(placed_count(&desired(0), 0), 0, "a cell with no storage");
    let grid = Desired {
        durability: Durability::new(0, 3).expect("a durability"),
        ..Desired::DEFAULT
    };
    assert_eq!(
        placed_count(&grid, 1),
        grid.acceptors(),
        "a grid needs all its acceptors"
    );
}

#[test]
fn a_create_may_take_every_registered_storage_machine() {
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    fixture::register(&mut registry, 3, Class::All);
    fixture::register(&mut registry, 4, Class::Stateless);
    fixture::register(&mut registry, 5, Class::Storage);
    fixture::drain(&mut registry, 5);
    // A machine that never registered takes no journal (#685 (no founders)).
    assert_eq!(
        crate::tenant::TenantDesk::storage(&registry)
            .into_iter()
            .map(|n| n.0)
            .collect::<Vec<_>>(),
        [1, 2, 3],
        "neither a stateless nor a draining machine"
    );
}

#[test]
fn the_slots_of_a_role_are_its_live_bookings_for_the_target() {
    let journal = fixture::journal(50, 52);
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    let acceptor = (Role::Acceptor, BookingTarget::Journal(journal));
    fixture::book(&mut registry, 7, 1, acceptor);
    fixture::book(&mut registry, 8, 2, acceptor);
    fixture::book(
        &mut registry,
        9,
        2,
        (Role::Replica, BookingTarget::Journal(journal)),
    );
    fixture::book(
        &mut registry,
        10,
        2,
        (
            Role::Acceptor,
            BookingTarget::Journal(fixture::journal(50, 53)),
        ),
    );
    let ids = |slots: Vec<(u64, Booking)>| slots.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
    assert_eq!(ids(acceptor_slots(&registry, journal)), [7, 8]);
    assert_eq!(
        ids(slots_of(
            &registry,
            Role::Replica,
            BookingTarget::Journal(journal)
        )),
        [9]
    );
    fixture::fold(
        &mut registry,
        &SystemCommand::ReleaseCapacity { booking: 7 },
    );
    assert_eq!(
        ids(acceptor_slots(&registry, journal)),
        [8],
        "a released slot is gone"
    );
}
