//! The registry oracles of [`SystemBoard`] on hand-built folds. The events
//! come from a real [`Registry`] fold of encoded commands, so an honest
//! history stays clean; each broken report goes red at its oracle.

use crate::audit::testing::red;
use paros::core::NodeId;
use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
use paros::machine::CachedRegistry;
use paros::model::address::Address;
use paros::model::machine::Locality;
use paros::model::registry::{
    BookingTarget, Class, Registry, RegistryEvent, RegistryRefusal, Role, SystemCommand,
};

use super::{Plane, SystemBoard};

const REGISTRY: JournalIdentifier = JournalIdentifier::new(TenantId(2), JournalId(1));
const OTHER: JournalIdentifier = JournalIdentifier::new(TenantId(3), JournalId(1));
const TARGET: BookingTarget =
    BookingTarget::Journal(JournalIdentifier::new(TenantId(5), JournalId(7)));

fn addr(id: u64) -> String {
    format!("n{id}:7000")
}

fn register(id: u64, class: Class) -> SystemCommand {
    SystemCommand::RegisterNode {
        id: NodeId(id),
        addr: addr(id),
        class,
        locality: Locality::default(),
        name: format!("m{id}"),
        incarnation: u128::from(id),
        machine_interface: Vec::new(),
    }
}

fn book(booking: u64, node: u64) -> SystemCommand {
    SystemCommand::BookCapacity {
        booking,
        node: NodeId(node),
        role: Role::Acceptor,
        target: TARGET,
    }
}

/// A registry history, folded by the real registry: each command's event.
fn history(commands: &[SystemCommand]) -> (Registry, Vec<RegistryEvent>) {
    let mut registry = Registry::new([NodeId(1)]);
    let events = (0..)
        .zip(commands)
        .map(|(seq, command)| registry.fold(seq, &command.encode()))
        .collect();
    (registry, events)
}

fn honest() -> Vec<RegistryEvent> {
    history(&[
        register(2, Class::Storage),
        register(3, Class::Storage),
        SystemCommand::MachineAdmitted { id: NodeId(2) },
        book(1, 2),
        SystemCommand::ReleaseCapacity { booking: 1 },
        book(2, 2),
        SystemCommand::DrainNode { id: NodeId(3) },
        SystemCommand::RetireNode { id: NodeId(3) },
    ])
    .1
}

fn armed() -> SystemBoard {
    let mut board = SystemBoard::default();
    board.arm([1], true, true, [(2, Class::Storage), (3, Class::Storage)]);
    board
}

fn fold_all(board: &mut SystemBoard, node: u64, events: &[RegistryEvent], plane: Plane) {
    for (lsn, event) in (0..).zip(events) {
        board.folded(NodeId(node), REGISTRY, lsn, event, plane);
    }
}

#[test]
fn an_honest_registry_history_folded_by_two_nodes_stays_clean() {
    let events = honest();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, RegistryEvent::Refused(_))),
        "{events:?}"
    );
    let mut board = armed();
    assert!(!red(|| {
        fold_all(&mut board, 1, &events, Plane::Harness);
        fold_all(&mut board, 4, &events, Plane::Harness);
    }));
    assert!(board.holds_target(REGISTRY, 2, TARGET));
    assert!(!board.holds_target(REGISTRY, 3, TARGET));
    assert!(!board.holds_target(OTHER, 2, TARGET));
    assert!(board.learned_before_pool, "node 4 is in no pool");
    let tail = events.len() as u64;
    assert_eq!(board.registry_lagging(REGISTRY, &[1, 4, 5], tail), vec![5]);
    assert_eq!(board.registry_progress(REGISTRY, &[1, 4, 5]), 2 * tail);
    assert!(board.stores(2) && !board.stores(9));
}

#[test]
fn two_nodes_that_fold_one_position_differently_are_red() {
    let events = honest();
    let mut board = armed();
    fold_all(&mut board, 1, &events, Plane::Harness);
    assert!(red(|| board.folded(
        NodeId(4),
        REGISTRY,
        0,
        &events[1],
        Plane::Harness
    )));
}

#[test]
fn the_booking_model_meets_no_gap_a_checkpoint_does_not_heal() {
    let events = honest();
    let mut board = armed();
    board.folded(NodeId(1), REGISTRY, 0, &events[0], Plane::Harness);
    let malformed = RegistryEvent::Refused(RegistryRefusal::Malformed);
    assert!(!red(|| board.folded(
        NodeId(1),
        REGISTRY,
        5,
        &malformed,
        Plane::Harness
    )));
    assert!(red(|| board.folded(
        NodeId(1),
        REGISTRY,
        6,
        &events[1],
        Plane::Harness
    )));
    // Past the gap the model stops: nothing more is judged.
    assert!(!red(|| board.folded(
        NodeId(1),
        REGISTRY,
        7,
        &events[1],
        Plane::Harness
    )));
}

#[test]
fn a_booking_id_is_booked_once_on_a_node_of_its_class() {
    let booked = |booking, node| RegistryEvent::Booked {
        booking,
        node: NodeId(node),
        role: Role::Acceptor,
        target: TARGET,
    };
    let released = RegistryEvent::Released {
        booking: 1,
        node: NodeId(2),
    };
    let mut board = armed();
    board.folded(NodeId(1), REGISTRY, 0, &booked(1, 2), Plane::Harness);
    assert!(
        red(|| board.folded(NodeId(1), REGISTRY, 1, &booked(1, 3), Plane::Harness)),
        "a live id"
    );
    let mut board = armed();
    board.folded(NodeId(1), REGISTRY, 0, &booked(1, 2), Plane::Harness);
    board.folded(NodeId(1), REGISTRY, 1, &released, Plane::Harness);
    assert!(!board.holds_target(REGISTRY, 2, TARGET));
    assert!(
        red(|| board.folded(NodeId(1), REGISTRY, 2, &booked(1, 2), Plane::Harness)),
        "a spent id"
    );
    let mut board = armed();
    board.machine(NodeId(6), Class::Stateless);
    assert!(
        red(|| board.folded(NodeId(1), REGISTRY, 0, &booked(1, 6), Plane::Harness)),
        "a stateless node"
    );
}

#[test]
fn a_grant_books_each_of_its_slots_alike() {
    // A hand-built grant whose two slots share one booking id.
    let slot = |booking| paros::model::registry::Slot {
        booking,
        node: NodeId(2),
        role: Role::Acceptor,
        target: TARGET,
    };
    let granted = RegistryEvent::Granted {
        key: paros::model::registry::GrantKey::Request(1),
        slots: vec![slot(4), slot(4)],
    };
    let mut board = armed();
    assert!(red(|| board.folded(
        NodeId(1),
        REGISTRY,
        0,
        &granted,
        Plane::Harness
    )));
}

#[test]
fn a_machine_keeps_its_class_and_a_stateless_one_serves_no_journal() {
    let mut board = armed();
    assert!(!red(|| {
        board.machine(NodeId(2), Class::Storage);
        board.machine(NodeId(2), Class::All);
        board.started(NodeId(2), Plane::Machine);
    }));
    assert!(red(|| board.machine(NodeId(2), Class::Stateless)));
    let mut board = armed();
    board.machine(NodeId(6), Class::Stateless);
    assert!(red(|| board.started(NodeId(6), Plane::Machine)));
    assert!(!board.joiner_started);
    board.started(NodeId(2), Plane::Harness);
    assert!(board.joiner_started, "node 2 is outside the genesis pool");
}

#[test]
fn a_machine_folds_one_cells_registry() {
    let events = honest();
    let mut board = armed();
    board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Machine);
    assert!(red(|| board.folded(
        NodeId(2),
        OTHER,
        0,
        &events[0],
        Plane::Machine
    )));
}

#[test]
fn no_genesis_nodes_message_waits_on_the_registry_fold() {
    let mut board = armed();
    assert!(red(|| board.refused(NodeId(2), NodeId(1))));
    let mut board = armed();
    assert!(!red(|| board.refused(NodeId(1), NodeId(4))));
    board.admitted(NodeId(2), NodeId(4));
    assert!(!board.refused_then_admitted);
    board.admitted(NodeId(1), NodeId(4));
    assert!(board.refused_then_admitted);
    board.elected_under(&[1]);
    assert!(!board.joined_through_reconfigure);
    board.elected_under(&[1, 4]);
    assert!(board.joined_through_reconfigure);
}

#[test]
fn a_checkpoint_is_the_state_its_prefix_folds_to() {
    let (registry, _) = history(&[register(2, Class::Storage)]);
    let mut board = armed();
    assert!(red(|| board.checkpoint_folded(
        NodeId(1),
        REGISTRY,
        (3, Some(false)),
        &registry,
        Plane::Harness
    )));
}

#[test]
fn the_model_resumes_at_a_checkpoint_past_a_truncation() {
    let commands = [
        register(2, Class::Storage),
        SystemCommand::MachineAdmitted { id: NodeId(2) },
        book(1, 2),
    ];
    let (registry, _) = history(&commands);
    let mut board = armed();
    // No node folded 0..3: a restoring node meets the checkpoint at 3.
    assert!(!red(|| board.checkpoint_folded(
        NodeId(1),
        REGISTRY,
        (3, None),
        &registry,
        Plane::Harness
    )));
    assert!(board.resumed_at_checkpoint && board.restarted && board.booking_crossed);
    assert!(board.holds_target(REGISTRY, 2, TARGET));
    // The model goes on from position 4, where booking 1 is still spent.
    let rebooked = RegistryEvent::Booked {
        booking: 1,
        node: NodeId(2),
        role: Role::Acceptor,
        target: TARGET,
    };
    assert!(red(|| board.folded(
        NodeId(1),
        REGISTRY,
        4,
        &rebooked,
        Plane::Harness
    )));
}

#[test]
fn the_model_equals_every_checkpoint_it_reaches() {
    let commands = [
        register(2, Class::Storage),
        SystemCommand::MachineAdmitted { id: NodeId(2) },
        book(1, 2),
    ];
    let (registry, events) = history(&commands);
    let checkpoint = RegistryEvent::Checkpoint { covers_up_to: 2 };
    // An honest checkpoint at position 3, reported before the model folds
    // there.
    let mut board = armed();
    board.checkpoint_folded(
        NodeId(1),
        REGISTRY,
        (3, Some(true)),
        &registry,
        Plane::Harness,
    );
    fold_all(&mut board, 1, &events, Plane::Harness);
    assert!(!red(|| board.folded(
        NodeId(1),
        REGISTRY,
        3,
        &checkpoint,
        Plane::Harness
    )));
    // A checkpoint that holds another state.
    let (other, _) = history(&[register(2, Class::Storage)]);
    let mut board = armed();
    board.checkpoint_folded(NodeId(1), REGISTRY, (3, Some(true)), &other, Plane::Harness);
    fold_all(&mut board, 1, &events, Plane::Harness);
    assert!(red(|| board.folded(
        NodeId(1),
        REGISTRY,
        3,
        &checkpoint,
        Plane::Harness
    )));
}

#[test]
fn a_cached_registry_fold_holds_the_registrys_address_at_its_position() {
    let events = honest();
    let cache = |position, machines: Vec<(u64, String)>| CachedRegistry {
        node: NodeId(2),
        position,
        machines: machines
            .into_iter()
            .map(|(id, a)| (NodeId(id), Address::parse(&a).unwrap()))
            .collect(),
    };
    let mut board = armed();
    fold_all(&mut board, 2, &events, Plane::Machine);
    assert!(!red(|| board.cached(
        NodeId(2),
        &cache(3, vec![(3, addr(3)), (2, addr(9)), (8, addr(8))])
    )));
    assert!(board.cache_judged);
    assert!(
        red(|| board.cached(NodeId(2), &cache(3, vec![(3, addr(9))]))),
        "another address"
    );
    assert!(
        red(|| board.cached(NodeId(2), &cache(8, vec![(3, addr(3))]))),
        "a retired machine"
    );
    // A cache the model has not reached waits, then is judged.
    let mut board = armed();
    board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Machine);
    board.folded(NodeId(2), REGISTRY, 1, &events[1], Plane::Machine);
    assert!(!red(
        || board.cached(NodeId(2), &cache(4, vec![(3, addr(9))]))
    ));
    assert!(!red(|| board.folded(
        NodeId(2),
        REGISTRY,
        2,
        &events[2],
        Plane::Machine
    )));
    assert!(red(|| board.folded(
        NodeId(2),
        REGISTRY,
        3,
        &events[3],
        Plane::Machine
    )));
    // A machine that folds no registry yet: nothing to judge.
    assert!(!red(
        || armed().cached(NodeId(2), &cache(1, vec![(3, addr(9))]))
    ));
}

#[test]
fn a_resume_at_a_checkpoint_judges_no_unregistered_initial_member() {
    // Node 1 is the seeded fold's initial member: it never registers.
    let (registry, events) = history(&[
        register(2, Class::Storage),
        register(3, Class::Storage),
        SystemCommand::DrainNode { id: NodeId(3) },
        SystemCommand::RetireNode { id: NodeId(3) },
    ]);
    let cache = |machines: Vec<(u64, String)>| CachedRegistry {
        node: NodeId(2),
        position: 5,
        machines: machines
            .into_iter()
            .map(|(id, a)| (NodeId(id), Address::parse(&a).unwrap()))
            .collect(),
    };
    let resumed = || {
        let mut board = armed();
        board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Machine);
        // No node folded 1..4: the model resumes at the checkpoint at 4.
        board.checkpoint_folded(NodeId(2), REGISTRY, (4, None), &registry, Plane::Harness);
        assert!(board.resumed_at_checkpoint);
        board
    };
    let mut board = resumed();
    assert!(!red(|| board.cached(NodeId(2), &cache(vec![(1, addr(1))]))));
    assert!(board.cache_judged);
    let mut board = resumed();
    assert!(
        red(|| board.cached(NodeId(2), &cache(vec![(3, addr(3))]))),
        "a retired machine"
    );
}

#[test]
fn the_gates_never_go_red() {
    let board = armed();
    assert!(!red(|| board.check_gates()));
    let mut board = armed();
    board.truncated_to_checkpoint();
    board.reader_restarted();
    assert!(!red(|| board.check_gates()));
    assert!(!red(|| SystemBoard::default().check_gates()));
}

#[test]
fn a_digest_is_a_pure_function_of_the_event() {
    let a = RegistryEvent::Retired { id: NodeId(2) };
    let b = RegistryEvent::Retired { id: NodeId(3) };
    assert_eq!(super::digest(&a), super::digest(&a.clone()));
    assert_ne!(super::digest(&a), super::digest(&b));
}

#[test]
fn a_node_admits_only_an_id_the_registry_registered_below_its_position() {
    let events = honest();
    let mut board = armed();
    fold_all(&mut board, 2, &events, Plane::Machine);
    // Node 2 registered at position 0, node 3 at 1.
    assert!(!red(|| board.judge_admission(
        NodeId(4),
        NodeId(2),
        REGISTRY,
        1
    )));
    assert!(board.admission_judged);
    assert!(!red(|| board.judge_admission(
        NodeId(4),
        NodeId(3),
        REGISTRY,
        8
    )));
    assert!(
        red(|| board.judge_admission(NodeId(4), NodeId(3), REGISTRY, 1)),
        "registered at the position, not below it"
    );
    assert!(
        red(|| board.judge_admission(NodeId(4), NodeId(1), REGISTRY, 8)),
        "an initial member that never registered"
    );
    assert!(
        red(|| board.judge_admission(NodeId(4), NodeId(9), REGISTRY, 8)),
        "an id the registry never held"
    );
    assert!(board.excuses().is_empty());
}

#[test]
fn an_admission_ahead_of_the_model_waits_for_it() {
    let events = honest();
    let mut board = armed();
    board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Harness);
    // The admitting fold stands ahead of every fold the model saw.
    assert!(!red(|| board.judge_admission(
        NodeId(2),
        NodeId(3),
        REGISTRY,
        2
    )));
    assert!(!red(|| board.judge_admission(
        NodeId(2),
        NodeId(1),
        REGISTRY,
        3
    )));
    assert!(!board.admission_judged);
    assert!(!red(|| board.folded(
        NodeId(2),
        REGISTRY,
        1,
        &events[1],
        Plane::Harness
    )));
    assert!(board.admission_judged);
    assert!(
        red(|| board.folded(NodeId(2), REGISTRY, 2, &events[2], Plane::Harness)),
        "node 1 never registered"
    );
    // Past a gap no checkpoint healed, the model judges nothing more.
    let mut board = armed();
    board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Harness);
    board.judge_admission(NodeId(2), NodeId(9), REGISTRY, 3);
    assert!(
        red(|| board.folded(NodeId(2), REGISTRY, 2, &events[2], Plane::Harness)),
        "the gap oracle"
    );
    assert!(!red(|| board.folded(
        NodeId(2),
        REGISTRY,
        3,
        &events[3],
        Plane::Harness
    )));
}

#[test]
fn an_admission_below_a_resume_is_skipped_and_counted() {
    let (registry, events) = history(&[
        register(2, Class::Storage),
        register(3, Class::Storage),
        SystemCommand::DrainNode { id: NodeId(3) },
        SystemCommand::RetireNode { id: NodeId(3) },
    ]);
    let mut board = armed();
    board.folded(NodeId(2), REGISTRY, 0, &events[0], Plane::Machine);
    // No node folded 1..4: the model resumes at the checkpoint at 4, and
    // forgets the positions below 5.
    board.checkpoint_folded(NodeId(2), REGISTRY, (4, None), &registry, Plane::Machine);
    assert!(!red(|| board.judge_admission(
        NodeId(4),
        NodeId(9),
        REGISTRY,
        3
    )));
    assert!(!board.admission_judged);
    assert_eq!(
        board.excuses(),
        vec![crate::excuses::Excuse::SkipAdmissionBelowResume]
    );
    assert!(!red(|| board.judge_admission(
        NodeId(4),
        NodeId(2),
        REGISTRY,
        5
    )));
    assert!(board.admission_judged);
    assert!(
        red(|| board.judge_admission(NodeId(4), NodeId(1), REGISTRY, 5)),
        "the resume rebuilds no unregistered initial member"
    );
}
