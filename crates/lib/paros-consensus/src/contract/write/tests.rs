use super::{AcceptorWrite, MustSync, WriteOp};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::types::{Ballot, NodeId, Slot, Value};

fn ballot() -> Ballot {
    Ballot {
        round: 1,
        node: NodeId(0),
    }
}

fn append(slot: u64) -> WriteOp {
    WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
        slot: Slot(slot),
        ballot: ballot(),
        value: Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(1),
            records: vec![Value(vec![7])],
        }),
    })
}

fn promise() -> WriteOp {
    WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot()))
}

#[test]
fn promise_and_accept_need_fsync_chosen_index_does_not() {
    assert!(promise().needs_sync());
    assert!(append(0).needs_sync());
    assert!(!WriteOp::SetChosenIndex(Slot(0)).needs_sync());
    assert!(
        WriteOp::Truncate {
            first: Slot(1),
            sealed: crate::journal::state::JournalState::default()
        }
        .needs_sync()
    );
}

fn learned(slot: u64) -> WriteOp {
    WriteOp::Learned {
        slot: Slot(slot),
        ballot: ballot(),
        command: Command::Control(Control::Noop),
    }
}

fn trimmed_to(point: u64) -> WriteOp {
    WriteOp::TrimmedTo {
        point: Slot(point),
        state: crate::journal::state::JournalState::default(),
    }
}

/// A batch that only moves the chosen index, or that writes nothing,
/// carries no promise and no vote. It can use a relaxed write.
#[test]
fn a_batch_without_a_durable_record_is_relaxed() {
    assert_eq!(MustSync::for_batch(&[]), MustSync::Relaxed);
    assert_eq!(
        MustSync::for_batch(&[
            WriteOp::SetChosenIndex(Slot(1)),
            WriteOp::SetChosenIndex(Slot(2)),
        ]),
        MustSync::Relaxed
    );
}

/// One op that needs an fsync makes the full batch need an fsync, at any
/// position in the batch.
#[test]
fn one_synced_op_makes_the_batch_synced() {
    for op in [promise(), append(3), learned(3), trimmed_to(4)] {
        let first = [op.clone(), WriteOp::SetChosenIndex(Slot(3))];
        let last = [WriteOp::SetChosenIndex(Slot(3)), op];
        assert_eq!(MustSync::for_batch(&first), MustSync::Sync);
        assert_eq!(MustSync::for_batch(&last), MustSync::Sync);
    }
}
