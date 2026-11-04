//! The watch's judgements over a folded registry: when a machine is gone,
//! held, down, up, registered again, admitted or stuck.

use super::*;
use crate::fixture::{self, CELL, INCARNATION};
use paros_model::machine::{Class, Locality, incarnation_halves, machine_tags};
use paros_model::registry::{BookingTarget, Role};
use paros_model::tags::Tag;

const EVERY: Duration = Duration::from_secs(1);
const DOWN: Duration = Duration::from_secs(3);
const REPLACE: Duration = Duration::from_secs(10);

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

fn watch() -> Watch {
    Watch::new(
        Duration::ZERO,
        EVERY,
        (DOWN, REPLACE),
        SlowBounds {
            rtt: Duration::from_millis(500),
            busy_permille: 900,
            rounds: 2,
        },
    )
}

fn set(ids: &[u64]) -> std::collections::BTreeSet<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

/// A cell of founder 1 and registered storage machines 2 and 3, with an
/// acceptor slot of journal (50, 52) on each of 2 and 3.
fn booked() -> Registry {
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    fixture::register(&mut registry, 3, Class::Storage);
    let target = BookingTarget::Journal(fixture::journal(50, 52));
    fixture::book(&mut registry, 20, 2, (Role::Acceptor, target));
    fixture::book(&mut registry, 30, 3, (Role::Acceptor, target));
    registry
}

/// An `Identify` answer of machine `id` as `incarnation`, of `class`, in
/// cell `cell_id`, its registry fold at `position`.
fn ack(
    id: u64,
    incarnation: u128,
    class: Class,
    (cell_id, position): (u64, u64),
) -> wire::IdentifyAck {
    let (incarnation_high, incarnation_low) = incarnation_halves(incarnation);
    wire::IdentifyAck {
        node_id: id,
        incarnation_high,
        incarnation_low,
        tags: machine_tags(class, &Locality::zone(format!("z{id}")))
            .iter()
            .map(Tag::to_wire)
            .collect(),
        name: format!("m{id}"),
        cell_id,
        registry_position: position,
        ..wire::IdentifyAck::default()
    }
}

fn addr(id: u64) -> Address {
    Address::parse(&format!("m{id}.paros:1")).expect("a test address")
}

#[test]
fn a_down_machine_silent_past_the_bound_is_gone() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    let mut watch = watch();
    assert!(
        watch.gone(&registry, secs(9)).is_empty(),
        "silent below the bound: it keeps its roles"
    );
    assert_eq!(watch.gone(&registry, secs(10)), set(&[2]), "past the bound");
    assert_eq!(
        watch.departed,
        set(&[2]),
        "a gone machine with a slot departs"
    );
    assert_eq!(watch.seen, secs(10), "a judgement is a moment watched");
}

#[test]
fn a_machine_heard_lately_or_up_is_never_gone() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    let mut watch = watch();
    watch.heard.insert(NodeId(2), secs(5));
    assert!(
        watch.gone(&registry, secs(14)).is_empty(),
        "silent since its last answer, not since the start"
    );
    assert_eq!(watch.gone(&registry, secs(15)), set(&[2]));
    // Machine 3 is up: silence alone never names it gone.
    assert!(!watch.gone(&registry, secs(100)).contains(&NodeId(3)));
}

#[test]
fn a_gone_machine_with_no_slot_does_not_depart() {
    let mut registry = fixture::cell(&[1]);
    fixture::register(&mut registry, 2, Class::Storage);
    fixture::down(&mut registry, 2);
    let mut watch = watch();
    assert_eq!(watch.gone(&registry, secs(10)), set(&[2]));
    assert!(watch.departed.is_empty(), "nothing to move off it");
}

#[test]
fn a_departed_machine_stays_gone_until_it_is_up() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    let mut watch = watch();
    assert_eq!(watch.gone(&registry, secs(10)), set(&[2]));
    // Heard again, but still held down: it stays gone.
    watch.heard.insert(NodeId(2), secs(11));
    assert_eq!(watch.gone(&registry, secs(12)), set(&[2]));
    // Once the registry holds it up, it is bookable again.
    fixture::fold(
        &mut registry,
        &SystemCommand::MachineUp {
            id: NodeId(2),
            incarnation: INCARNATION,
        },
    );
    assert!(watch.gone(&registry, secs(13)).is_empty());
    assert!(watch.departed.is_empty());
}

#[test]
fn a_maintenance_window_holds_a_down_machines_roles() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    fixture::maintain(&mut registry, 2, 30_000);
    let mut watch = watch();
    // The window runs from the watch's first fold of it, at 10 s.
    assert!(watch.gone(&registry, secs(10)).is_empty(), "held");
    assert_eq!(watch.held(&registry, secs(10)), set(&[2]));
    assert!(watch.gone(&registry, secs(39)).is_empty(), "still held");
    assert_eq!(
        watch.gone(&registry, secs(40)),
        set(&[2]),
        "the window ended"
    );
    assert!(watch.held(&registry, secs(40)).is_empty());
}

#[test]
fn a_window_holds_a_machine_named_gone_before() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    let mut watch = watch();
    assert_eq!(watch.gone(&registry, secs(10)), set(&[2]));
    fixture::maintain(&mut registry, 2, 5_000);
    assert!(
        watch.gone(&registry, secs(11)).is_empty(),
        "a window over a departed machine holds its roles"
    );
    assert_eq!(watch.departed, set(&[2]), "it stays departed");
    assert_eq!(watch.gone(&registry, secs(16)), set(&[2]));
}

#[test]
fn a_replaced_window_restarts_its_clock() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    fixture::maintain(&mut registry, 2, 20_000);
    let mut watch = watch();
    assert!(watch.gone(&registry, secs(10)).is_empty());
    // The operator writes a new window over the same machine at 25 s.
    fixture::maintain(&mut registry, 2, 20_000);
    assert!(watch.gone(&registry, secs(25)).is_empty());
    assert!(
        watch.gone(&registry, secs(44)).is_empty(),
        "the new window runs from 25 s, not from 10 s"
    );
    assert_eq!(watch.gone(&registry, secs(45)), set(&[2]));
}

#[test]
fn a_resumed_watch_moves_its_moments_by_the_gap() {
    let mut watch = watch();
    watch.heard.insert(NodeId(2), secs(4));
    watch.asked.insert(NodeId(3), secs(5));
    watch.seen = secs(6);
    watch.ran = true;
    let watch = watch.resumed(secs(20));
    assert_eq!(watch.started, secs(14));
    assert_eq!(watch.heard[&NodeId(2)], secs(18));
    assert_eq!(watch.asked[&NodeId(3)], secs(19));
    assert_eq!((watch.seen, watch.next), (secs(20), secs(20)));
    assert!(!watch.ran(), "a resumed watch has not run its round");
    assert_eq!(watch.silent_since(NodeId(2)), secs(18));
    assert_eq!(watch.silent_since(NodeId(3)), secs(19));
    assert_eq!(watch.silent_since(NodeId(9)), secs(14), "never asked");
}

#[test]
fn a_resumed_window_moves_by_the_gap() {
    let mut registry = booked();
    fixture::down(&mut registry, 2);
    fixture::maintain(&mut registry, 2, 10_000);
    let mut watch = watch();
    assert!(watch.gone(&registry, secs(10)).is_empty());
    let mut watch = watch.resumed(secs(30));
    assert!(
        watch.gone(&registry, secs(39)).is_empty(),
        "the window lost the 20 s no round watched"
    );
    assert_eq!(watch.gone(&registry, secs(40)), set(&[2]));
}

#[test]
fn an_answer_of_no_incarnation_changes_nothing() {
    let registry = booked();
    let answer = ack(2, 0, Class::Storage, (CELL, 0));
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &answer),
        None
    );
}

#[test]
fn a_rebooted_machine_registers_again() {
    let registry = booked();
    let answer = ack(2, 9, Class::Storage, (CELL, 0));
    let Some(SystemCommand::RegisterNode {
        id,
        addr: at,
        class,
        locality,
        name,
        incarnation,
        ..
    }) = Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &answer)
    else {
        panic!("a reboot registers again");
    };
    assert_eq!((id, incarnation, class), (NodeId(2), 9, Class::Storage));
    assert_eq!((at.as_str(), name.as_str()), ("m2.paros:1", "m2"));
    assert_eq!(locality.zone, "z2");
}

#[test]
fn a_reboot_widens_its_roles_never_narrows_them() {
    let registry = booked();
    let narrow = ack(2, 9, Class::Stateless, (CELL, 0));
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &narrow),
        None,
        "the registry refuses fewer roles"
    );
    let wide = ack(2, 9, Class::All, (CELL, 0));
    assert!(matches!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &wide),
        Some(SystemCommand::RegisterNode {
            class: Class::All,
            ..
        })
    ));
    let mut untagged = ack(2, 9, Class::Storage, (CELL, 0));
    untagged.tags.clear();
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &untagged),
        None,
        "an answer with no roles registers nothing"
    );
}

#[test]
fn a_registered_machine_that_answers_in_the_cell_is_admitted() {
    let mut registry = booked();
    let answer = ack(2, INCARNATION, Class::Storage, (CELL, 0));
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &answer),
        Some(SystemCommand::MachineAdmitted { id: NodeId(2) })
    );
    let elsewhere = ack(2, INCARNATION, Class::Storage, (CELL + 1, 0));
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &elsewhere),
        None,
        "a machine of another cell is not admitted here"
    );
    assert_eq!(
        Watch::answered(&registry, 0, (NodeId(2), &addr(2)), &answer),
        None,
        "a cell with no id admits none"
    );
    fixture::fold(
        &mut registry,
        &SystemCommand::MachineAdmitted { id: NodeId(2) },
    );
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &answer),
        None,
        "admitted once"
    );
}

#[test]
fn a_down_machine_that_answers_is_up() {
    let mut registry = booked();
    fixture::fold(
        &mut registry,
        &SystemCommand::MachineAdmitted { id: NodeId(2) },
    );
    fixture::down(&mut registry, 2);
    let answer = ack(2, INCARNATION, Class::Storage, (CELL, 0));
    assert_eq!(
        Watch::answered(&registry, CELL, (NodeId(2), &addr(2)), &answer),
        Some(SystemCommand::MachineUp {
            id: NodeId(2),
            incarnation: INCARNATION
        })
    );
    // A founder the registry holds no record of, answering as another
    // incarnation than the one it holds, is up as the one that answered.
    let founder = Registry::new([NodeId(1)]);
    let answer = ack(1, 5, Class::Storage, (CELL, 0));
    assert_eq!(
        Watch::answered(&founder, CELL, (NodeId(1), &addr(1)), &answer),
        Some(SystemCommand::MachineUp {
            id: NodeId(1),
            incarnation: 5
        })
    );
}

#[test]
fn a_silent_machine_is_down_after_the_bound_and_two_misses() {
    let mut registry = booked();
    let mut watch = watch();
    watch.heard.insert(NodeId(2), secs(1));
    watch.missed.insert(NodeId(2), 2);
    assert_eq!(
        watch.silent(&registry, NodeId(2), secs(3)),
        None,
        "silent below down_after"
    );
    assert_eq!(
        watch.silent(&registry, NodeId(2), secs(4)),
        Some(SystemCommand::MachineDown {
            id: NodeId(2),
            incarnation: INCARNATION
        })
    );
    watch.missed.insert(NodeId(2), 1);
    assert_eq!(
        watch.silent(&registry, NodeId(2), secs(40)),
        None,
        "one missed ask never marks a machine down"
    );
    watch.missed.insert(NodeId(2), 2);
    fixture::down(&mut registry, 2);
    assert_eq!(
        watch.silent(&registry, NodeId(2), secs(40)),
        None,
        "a machine held down is not marked again"
    );
}

#[test]
fn an_admitted_machine_whose_fold_stopped_below_the_tail_is_stuck() {
    let mut registry = booked();
    fixture::fold(
        &mut registry,
        &SystemCommand::MachineAdmitted { id: NodeId(2) },
    );
    let members = [NodeId(1)];
    let mut watch = watch();
    let at = |position| ack(2, INCARNATION, Class::Storage, (CELL, position));
    let fold = (&registry, &members[..]);
    assert!(
        !watch.stuck(fold, (CELL, 10), NodeId(2), &at(4)),
        "a first answer is not a stop"
    );
    assert!(
        watch.stuck(fold, (CELL, 10), NodeId(2), &at(4)),
        "the same position twice"
    );
    assert!(
        !watch.stuck(fold, (CELL, 10), NodeId(2), &at(5)),
        "it moved"
    );
    assert!(
        !watch.stuck(fold, (CELL, 5), NodeId(2), &at(5)),
        "at the tail"
    );
    assert!(
        !watch.stuck(fold, (CELL + 1, 10), NodeId(2), &at(5)),
        "it serves another cell"
    );
    // A founding member never is: it folds its own copy.
    let one = ack(1, INCARNATION, Class::Storage, (CELL, 4));
    assert!(!watch.stuck(fold, (CELL, 10), NodeId(1), &one));
    assert!(!watch.stuck(fold, (CELL, 10), NodeId(1), &one));
    // A machine that does not serve the cell is not stuck in it.
    let three = ack(3, INCARNATION, Class::Storage, (CELL, 4));
    assert!(!watch.stuck(fold, (CELL, 10), NodeId(3), &three));
    assert!(!watch.stuck(fold, (CELL, 10), NodeId(3), &three));
}

#[test]
fn an_identify_names_its_machine_and_incarnation() {
    let answer = ack(2, 9, Class::Storage, (CELL, 0));
    assert!(identified(Some(answer.clone()), NodeId(2), 9));
    assert!(
        !identified(Some(answer.clone()), NodeId(3), 9),
        "another machine"
    );
    assert!(
        !identified(Some(answer), NodeId(2), 8),
        "another incarnation"
    );
    assert!(!identified(None, NodeId(2), 9), "no answer");
}
