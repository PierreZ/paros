//! The helpers that the examples of `paros-journal-io` share: the roles of
//! one journal, the register of its first segment, the client calls and
//! the loop that drives the processes.
//!
//! This is Cargo's place for code shared by examples: a file
//! `examples/common.rs` would be built as an example of its own.

#![allow(dead_code)]

use paros_journal::register::{Ballot, Change, RegisterValue};
use paros_journal::roles::{Campaign, LeaderPhase};
use paros_journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, Reply,
    SegmentId, Seq, Slot, Write,
};
use paros_journal_io::{
    Address, Assignment, CallId, CallOutcome, CallReply, Clock, Driver, Events, JournalId,
    Register, Storage, Transport,
};

/// The journal of the examples.
pub const JOURNAL: JournalId = JournalId(1);
/// The nanoseconds between two periodic steps.
pub const TICK: u64 = 1_000_000;
/// The acceptors of every grid: one column of three (`F = 1`).
pub const ACCEPTORS: [u64; 3] = [1, 2, 3];
/// The writer.
pub const A: LeaderUuid = LeaderUuid(0xA);

/// The role `node` of the journal.
pub fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

/// The replica set of segment `id`: `10 * id + 1 ..= 10 * id + 3`.
pub fn replicas(id: u64) -> Vec<NodeId> {
    (1..=3).map(|i| NodeId(10 * id + i)).collect()
}

/// The register after the first election of the controller.
pub fn elected() -> RegisterValue {
    RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("the first election of the controller")
}

/// The controller's reshape that opens segment `id` after `expected`.
pub fn reshape(value: &RegisterValue, id: u64) -> Change {
    let expected = (id > 1).then(|| SegmentId(id - 1));
    Change::Reshape {
        opening: OpeningCall {
            term: value.term().number,
            expected,
            grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a v1 grid"),
            replicas: replicas(id),
        },
        release: RELEASE,
    }
}

/// The assignment of an acceptor.
pub fn acceptor(node: u64) -> Assignment {
    Assignment::Acceptor {
        address: at(node),
        page: 16,
    }
}

/// The assignment of a replica of segment `id`.
pub fn replica(node: u64, id: u64) -> Assignment {
    Assignment::Replica {
        address: at(node),
        segment: SegmentId(id),
        set: replicas(id),
        initial: Leader::Unset,
    }
}

/// The assignment of a leader at `round`, bound to segment `id`, that
/// reads from `from`.
pub fn leader(node: u64, round: u64, id: u64, from: Slot) -> Assignment {
    Assignment::Leader {
        address: at(node),
        campaign: Campaign {
            ballot: Ballot {
                round,
                node: NodeId(node),
            },
            segment: SegmentId(id),
            from,
        },
    }
}

/// Whether the leader at `node` writes.
pub fn open<S, T, C, R, E>(driver: &Driver<S, T, C, R, E>, node: u64) -> bool
where
    S: Storage,
    T: Transport,
    C: Clock,
    R: Register,
    E: Events,
{
    driver
        .host()
        .leader(at(node))
        .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
}

/// A single-writer write of `text` by writer A at `seq`.
pub fn write(seq: u64, text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: A,
            expected_seq: Seq(seq),
        },
        records: vec![Record::new(text.as_bytes().to_vec())],
    })
}

/// Writer A takes the journal.
pub fn claim() -> Command {
    Command::SetLeader {
        new: Some(A),
        old: Leader::Unset,
    }
}

/// The answer of a write of one record at `seq`.
pub fn written(seq: u64) -> CallOutcome {
    CallOutcome::Reply(Reply::Answer(Answer::Written {
        seq: Seq(seq),
        count: 1,
    }))
}

/// The answer of `call` among `replies`, if it came.
pub fn reply_of(replies: &[CallReply], call: CallId) -> Option<&CallOutcome> {
    replies.iter().find(|r| r.call == call).map(|r| &r.outcome)
}

/// Print one step of an example: what was asked, and what came back.
pub fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}
