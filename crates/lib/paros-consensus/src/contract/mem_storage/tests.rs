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
