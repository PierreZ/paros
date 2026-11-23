//! **A journal in one process, through the `paros` crate alone.**
//!
//! Run it: `cargo run -p paros --example journal`
//!
//! A program depends on `paros` and nothing else. This example imports only
//! `paros::…` paths, so it shows what a user of the facade writes to run a
//! journal: the layers it meets (`paros::journal` for the commands and the
//! answers, `paros::journal_io` for the driver and the backends) and the
//! types it must name.
//!
//! One process serves every role of one journal: three acceptors (one
//! column, `F = 1`), three replicas of segment 1 and one leader. The
//! backends are the in-memory ones of `paros::journal_io::mem`, written with
//! the standard library alone. A real program brings its own storage,
//! transport, clock and register; `paros-journal-io`'s examples
//! (`in_memory`, `custom_storage`) show more processes, a crash and a seal.
//!
//! # What the run shows
//!
//! 1. The controller opens segment 1 in the register, and the leader runs
//!    Phase 1.
//! 2. Writer A takes the journal with `SetLeader(A, Unset)`.
//! 3. A writes three records, one at a time, at `expected_seq` 0, 1 and 2.
//! 4. Every replica folds the three writes: `next_seq` is 3 and A leads.
//!
//! Each step asserts its result, so CI runs this example as a test.

use paros::journal::register::{Ballot, Change, RegisterValue};
use paros::journal::roles::{Campaign, LeaderPhase};
use paros::journal::{
    Answer, Command, Fence, Grid, Leader, LeaderUuid, NodeId, OpeningCall, RELEASE, Record, Reply,
    SegmentId, Seq, Slot, Write,
};
use paros::journal_io::mem::{Endpoint, ManualClock, MemNetwork, MemRegister, MemStorage};
use paros::journal_io::{
    Address, Assignment, CallOutcome, Driver, HostConfig, JournalId, Register,
};

const JOURNAL: JournalId = JournalId(1);
const TICK: u64 = 1_000_000;
const ACCEPTORS: [u64; 3] = [1, 2, 3];
const REPLICAS: [u64; 3] = [11, 12, 13];
const LEADER: u64 = 100;
const A: LeaderUuid = LeaderUuid(0xA);

/// The one process: a driver over the in-memory backends, with no event
/// sink (`()`).
type Process = Driver<MemStorage, Endpoint, ManualClock, MemRegister, ()>;

fn at(node: u64) -> Address {
    Address::new(JOURNAL, NodeId(node))
}

fn main() {
    let clock = ManualClock::new();
    let register = open_segment();
    let mut process = Driver::new(
        HostConfig {
            tick: TICK,
            resend_page: 16,
        },
        MemStorage::new(),
        MemNetwork::new().endpoint(),
        clock.clone(),
        register,
        (),
    );
    for role in roles() {
        process.transport_mut().route(role.address());
        process.assign(role);
    }

    println!("1. Segment 1 opens; the leader runs Phase 1.");
    run_until(&mut process, &clock, |p| {
        p.host()
            .leader(at(LEADER))
            .is_some_and(|l| matches!(l.phase(), LeaderPhase::Open { .. }))
    });

    println!("2. Writer A takes the journal.");
    let set = call(
        &mut process,
        &clock,
        Command::SetLeader {
            new: Leader::uuid(A),
            old: Leader::Unset,
        },
    );
    show("SetLeader(A, Unset)", &set);
    assert!(matches!(
        set,
        CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(_)))
    ));

    println!("3. A writes three records.");
    for seq in 0..3 {
        let write = Command::Write(Write {
            fence: Fence::Leader {
                uuid: A,
                expected_seq: Seq(seq),
            },
            records: vec![Record::new(format!("record {seq}").into_bytes())],
        });
        let answer = call(&mut process, &clock, write);
        show(&format!("Write(A, expected_seq {seq})"), &answer);
        assert_eq!(
            answer,
            CallOutcome::Reply(Reply::Answer(Answer::Written {
                seq: Seq(seq),
                count: 1
            }))
        );
    }

    println!("4. Every replica folds the three writes.");
    for node in REPLICAS {
        let state = process
            .host()
            .replica(at(node))
            .and_then(|r| r.state())
            .expect("a replica with its base");
        show(
            &format!("replica {node}"),
            &(state.next_seq(), state.leader()),
        );
        assert_eq!(state.next_seq(), Seq(3));
        assert_eq!(state.leader(), Leader::Uuid(A));
    }
    println!("ok: three records written through `paros` alone.");
}

/// The journal's register: the controller is elected, then it opens
/// segment 1 on one column of three acceptors.
fn open_segment() -> MemRegister {
    let mut register = MemRegister::new();
    let elected = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("the first election of the controller");
    register.create(JOURNAL, elected.clone());
    let reshape = Change::Reshape {
        opening: OpeningCall {
            term: elected.term().number,
            expected: None,
            grid: Grid::new(1, vec![ACCEPTORS.map(NodeId).to_vec()]).expect("a v1 grid"),
            replicas: REPLICAS.map(NodeId).to_vec(),
        },
        release: RELEASE,
    };
    register.change(JOURNAL, &reshape).expect("the reshape");
    register
}

/// Every role of the journal, all in this process.
fn roles() -> Vec<Assignment> {
    let acceptors = ACCEPTORS.map(|node| Assignment::Acceptor {
        address: at(node),
        page: 16,
    });
    let replicas = REPLICAS.map(|node| Assignment::Replica {
        address: at(node),
        segment: SegmentId(1),
        set: REPLICAS.map(NodeId).to_vec(),
        initial: Leader::Unset,
    });
    let leader = Assignment::Leader {
        address: at(LEADER),
        proxies: Vec::new(),
        campaign: Campaign {
            ballot: Ballot {
                round: 1,
                node: NodeId(LEADER),
            },
            segment: SegmentId(1),
            from: Slot(0),
        },
    };
    acceptors
        .into_iter()
        .chain(replicas)
        .chain([leader])
        .collect()
}

/// The writer's call to the leader, and its answer.
fn call(process: &mut Process, clock: &ManualClock, command: Command) -> CallOutcome {
    let id = process.call(at(LEADER), command);
    let mut answer = None;
    run_until(process, clock, |p| {
        answer = answer
            .take()
            .or_else(|| p.take_replies().into_iter().find(|r| r.call == id));
        answer.is_some()
    });
    answer.expect("a reply").outcome
}

/// Poll the process until it has no work, then move the clock one tick,
/// until `done` holds.
fn run_until(
    process: &mut Process,
    clock: &ManualClock,
    mut done: impl FnMut(&mut Process) -> bool,
) {
    for _ in 0..100 {
        while process.poll() {}
        if done(process) {
            return;
        }
        clock.advance(TICK);
    }
    panic!("not reached in 100 ticks");
}

/// Print one step: what was asked, and what came back.
fn show(what: &str, answer: &impl std::fmt::Debug) {
    println!("  {what:<46} -> {answer:?}");
}
