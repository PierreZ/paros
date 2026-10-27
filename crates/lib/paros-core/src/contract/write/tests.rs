use super::{AcceptorWrite, WriteOp};
use crate::journal::{Command, Entry, LeaderUuid, Seq};
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
