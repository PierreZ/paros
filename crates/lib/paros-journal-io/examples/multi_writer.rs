//! **Multi-writer mode: many writers, no fence, the journal gives the
//! positions.**
//!
//! Run it: `cargo run -p paros-journal-io --example multi_writer`
//!
//! A journal has two modes (§J2.4). In single-writer mode one writer holds
//! the leader uuid, and each write names its position (`expected_seq`), so
//! a resend is exactly-once. `SetLeader(None, old)` moves the journal to
//! multi-writer mode: writes carry no fence ([`Fence::Open`]), any writer
//! may write, and the journal gives each write the next position at apply.
//! `SetLeader(Some(uuid), Multi)` moves it back.
//!
//! One process serves every role of the journal (three acceptors, three
//! replicas, one leader), on the backends of `paros_journal_io::mem`.
//!
//! # What the run shows
//!
//! 1. Segment 1 opens and the leader runs Phase 1.
//! 2. `SetLeader(None, Unset)`: the journal is multi-writer.
//! 3. Writers X and Y send their writes at the same time, before the
//!    process runs. The journal orders them: each gets its own position.
//! 4. A fenced write answers `Fenced`: no uuid holds the journal.
//! 5. `SetLeader(B, Multi)`: writer B holds the journal again, and writes at
//!    the next position with `expected_seq`.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use paros_journal::{
    Answer, Command, Fence, JournalView, Leader, LeaderUuid, Record, Reply, Seq, Slot, Write,
};
use paros_journal_io::mem::{Endpoint, ManualClock, MemNetwork, MemRegister, MemStorage};
use paros_journal_io::{CallId, CallOutcome, CallReply, Driver, HostConfig, Register};

use common::{
    JOURNAL, TICK, acceptor, at, elected, leader, open, replica, reply_of, reshape, show, written,
};

const LEADER: u64 = 100;
const B: LeaderUuid = LeaderUuid(0xB);

fn main() {
    let clock = ManualClock::new();
    let mut register = MemRegister::new();
    register.create(JOURNAL, elected());
    let value = register.read(JOURNAL).expect("the register");
    register
        .change(JOURNAL, &reshape(&value, 1))
        .expect("the reshape");
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
    let mut roles = vec![leader(LEADER, 1, 1, Slot(0))];
    for node in 1..=3 {
        roles.push(acceptor(node));
        roles.push(replica(10 + node, 1));
    }
    for role in roles {
        process.transport_mut().route(role.address());
        process.assign(role);
    }
    let mut writers = Writers {
        process,
        clock,
        replies: Vec::new(),
    };

    println!("1. Segment 1 opens; the leader runs Phase 1.");
    writers.run_until(|w| open(&w.process, LEADER));

    println!("2. SetLeader(None, Unset): the journal is multi-writer.");
    let answer = writers.call(Command::SetLeader {
        new: None,
        old: Leader::Unset,
    });
    show("SetLeader(None, Unset)", &answer);
    assert_eq!(answer, view_answer(Leader::Multi, 0));

    println!("3. Writers X and Y write at the same time; the journal orders them.");
    let x = writers.process.call(at(LEADER), open_write("x0"));
    let y = writers.process.call(at(LEADER), open_write("y0"));
    let x2 = writers.process.call(at(LEADER), open_write("x1"));
    writers.run_until(|w| {
        [x, y, x2]
            .iter()
            .all(|c| reply_of(&w.replies, *c).is_some())
    });
    let mut seqs = Vec::new();
    for (name, call) in [("X: x0", x), ("Y: y0", y), ("X: x1", x2)] {
        let answer = writers.take(call);
        show(&format!("Write(open, [{name}])"), &answer);
        let CallOutcome::Reply(Reply::Answer(Answer::Written { seq, count: 1 })) = answer else {
            panic!("an open write is written: {answer:?}");
        };
        seqs.push(seq);
    }
    seqs.sort();
    assert_eq!(
        seqs,
        [Seq(0), Seq(1), Seq(2)],
        "one position for each write"
    );

    println!("4. A fenced write is refused: no uuid holds the journal.");
    let answer = writers.call(fenced(3, "b3"));
    show("Write(B, expected_seq 3)", &answer);
    assert_eq!(
        answer,
        CallOutcome::Reply(Reply::Answer(Answer::Fenced(view(Leader::Multi, 3))))
    );

    println!("5. SetLeader(B, Multi): back to one writer.");
    let answer = writers.call(Command::SetLeader {
        new: Some(B),
        old: Leader::Multi,
    });
    show("SetLeader(B, Multi)", &answer);
    assert_eq!(answer, view_answer(Leader::Uuid(B), 3));
    let answer = writers.call(fenced(3, "b3"));
    show("Write(B, expected_seq 3)", &answer);
    assert_eq!(answer, written(3));
    println!("ok: three open writes, three positions, then one writer again.");
}

/// The one process, and the answers of its calls not taken yet.
struct Writers {
    process: Driver<MemStorage, Endpoint, ManualClock, MemRegister, ()>,
    clock: ManualClock,
    replies: Vec<CallReply>,
}

impl Writers {
    /// Poll the process until it has no work, then move the clock one
    /// tick, until `done` holds.
    fn run_until(&mut self, done: impl Fn(&Self) -> bool) {
        for _ in 0..100 {
            while self.process.poll() {}
            self.replies.extend(self.process.take_replies());
            if done(self) {
                return;
            }
            self.clock.advance(TICK);
        }
        panic!("not reached in 100 ticks");
    }

    /// The answer of `call`, taken out of the answers not taken yet.
    fn take(&mut self, call: CallId) -> CallOutcome {
        let outcome = reply_of(&self.replies, call).expect("a reply").clone();
        self.replies.retain(|r| r.call != call);
        outcome
    }

    /// One call to the leader, and its answer.
    fn call(&mut self, command: Command) -> CallOutcome {
        let call = self.process.call(at(LEADER), command);
        self.run_until(|w| reply_of(&w.replies, call).is_some());
        self.take(call)
    }
}

/// A multi-writer write of one record: no fence.
fn open_write(text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Open,
        records: vec![Record::new(text.as_bytes().to_vec())],
    })
}

/// A single-writer write of one record by writer B at `seq`.
fn fenced(seq: u64, text: &str) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid: B,
            expected_seq: Seq(seq),
        },
        records: vec![Record::new(text.as_bytes().to_vec())],
    })
}

fn view(leader: Leader, next: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(0),
    }
}

fn view_answer(leader: Leader, next: u64) -> CallOutcome {
    CallOutcome::Reply(Reply::Answer(Answer::LeaderSet(view(leader, next))))
}
