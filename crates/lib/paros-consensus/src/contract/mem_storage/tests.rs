use super::MemStorage;
use crate::contract::config::Config;
use crate::contract::storage::Storage;
use crate::contract::write::{AcceptorWrite, WriteOp};
use crate::journal::state::JournalState;
use crate::journal::{Command, Control, JournalIdentifier};
use crate::types::{Ballot, NodeId, Slot};

fn store(slots: u64) -> MemStorage {
    let config = Config::new(NodeId(1), JournalIdentifier::UNSET).with_peers([NodeId(1)]);
    let mut disk = MemStorage::new(config);
    let ballot = Ballot {
        round: 1,
        node: NodeId(1),
    };
    let writes: Vec<WriteOp> = (0..slots)
        .map(|slot| {
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot: Slot(slot),
                ballot,
                value: Command::Control(Control::Noop),
            })
        })
        .collect();
    disk.apply(&writes);
    disk
}

/// A raised floor drops the faulty entries below it and keeps the ones at
/// or above it.
#[test]
fn a_raised_floor_keeps_the_faulty_entries_above_it() {
    let mut disk = store(6);
    disk.rot(Slot(1));
    let kept = disk.rot(Slot(4));
    disk.apply(&[WriteOp::Truncate {
        first: Slot(3),
        sealed: JournalState::default(),
    }]);
    assert_eq!(disk.faulty_entries(), vec![(Slot(4), kept)]);
    assert_eq!(disk.first_slot(), Slot(3));
}

/// A trim-point jump raises the chosen index to the slot below the point,
/// and never lowers it.
#[test]
fn a_trim_point_jump_raises_the_chosen_index_only() {
    let mut disk = store(0);
    disk.apply(&[WriteOp::TrimmedTo {
        point: Slot(5),
        state: JournalState::default(),
    }]);
    assert_eq!(disk.hard_state().chosen_index, Some(Slot(4)));
    disk.apply(&[WriteOp::SetChosenIndex(Slot(9))]);
    disk.apply(&[WriteOp::TrimmedTo {
        point: Slot(7),
        state: JournalState::default(),
    }]);
    assert_eq!(disk.hard_state().chosen_index, Some(Slot(9)));
}

/// An empty log has no last slot, and slot 0 is a real slot: no sentinel
/// stands for "empty" (#56).
#[test]
fn an_empty_log_has_no_last_slot() {
    assert_eq!(store(0).last_slot(), None);
    assert_eq!(store(1).last_slot(), Some(Slot(0)));
    assert_eq!(store(3).last_slot(), Some(Slot(2)));
}

/// A slot is readable, faulty or absent, never two of them: a write at a
/// rotted slot makes it readable again and drops its faulty entry, so a
/// boot after the repair reads no faulty record (#907 (core unit tests)).
#[test]
fn a_write_at_a_rotted_slot_heals_it() {
    let mut disk = promised(store(3));
    let rotted = disk.rot(Slot(1));
    assert_eq!(disk.faulty_entries(), vec![(Slot(1), rotted)]);
    assert!(
        disk.accepted(Slot(1)).is_none(),
        "a rotted slot is not readable"
    );
    disk.apply(&[WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
        slot: Slot(1),
        ballot: rotted,
        value: Command::Control(Control::Noop),
    })]);
    assert!(
        disk.faulty_entries().is_empty(),
        "the repair heals the slot"
    );
    assert!(
        disk.accepted(Slot(1)).is_some(),
        "the repaired slot is readable"
    );
    // A boot reads the healed store with no faulty record.
    let node = crate::node::ColocatedNode::new(&disk, crate::contract::limits::Limits::DEFAULT);
    assert!(node.acceptor().faulty().is_empty());
}

/// The test helper `from_node` keeps the faulty entries of a node: a reboot
/// of the snapshot reads the same faulty slots, never an absent slot.
#[test]
fn a_snapshot_of_a_node_keeps_its_faulty_entries() {
    let mut disk = promised(store(3));
    let rotted = disk.rot(Slot(2));
    let node = crate::node::ColocatedNode::new(&disk, crate::contract::limits::Limits::DEFAULT);
    assert_eq!(node.acceptor().faulty().get(&Slot(2)), Some(&rotted));
    let snapshot = MemStorage::from_node(&node);
    assert_eq!(snapshot.faulty_entries(), vec![(Slot(2), rotted)]);
    let again =
        crate::node::ColocatedNode::new(&snapshot, crate::contract::limits::Limits::DEFAULT);
    assert_eq!(again.acceptor().faulty(), node.acceptor().faulty());
}

/// `disk` with the promise at the ballot of its records, as a node that
/// accepted them wrote it.
fn promised(mut disk: MemStorage) -> MemStorage {
    disk.apply(&[WriteOp::Acceptor(AcceptorWrite::SetPromise(Ballot {
        round: 1,
        node: NodeId(1),
    }))]);
    disk
}
