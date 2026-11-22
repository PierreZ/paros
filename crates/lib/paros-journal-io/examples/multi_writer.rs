//! **Multi-writer mode: many writers, no fence, the journal gives the
//! positions.**
//!
//! Run it: `cargo run -p paros-journal-io --example multi_writer`
//!
//! A journal has two modes (§J2.4). In single-writer mode one writer holds
//! the leader uuid, and each write names its position (`expected_seq`), so
//! a resend is exactly-once. `SetLeader(None, old)` moves the journal to
//! multi-writer mode: writes carry no fence, any writer may write, and the
//! journal gives each write the next position at apply.
//! `SetLeader(Some(uuid), Multi)` moves it back.
//!
//! Each writer is a program with a [`Client`](paros_journal::client::Client)
//! of `paros_journal::client` (#1331): a client that won no `SetLeader` of
//! a uuid writes unfenced. One process serves every role of the journal
//! (three acceptors, three replicas, one leader), on the backends of
//! `paros_journal_io::mem`.
//!
//! # What the run shows
//!
//! 1. Segment 1 opens, and writer X makes the journal multi-writer.
//! 2. Writers X and Y write at the same time. The journal orders them: each
//!    write gets its own position.
//! 3. A reply of Y is lost. An unfenced write has no position, so the
//!    client's resend lands a second time: at-least-once.
//! 4. Writer B claims the journal (`SetLeader(B, Multi)`) and writes at the
//!    next position with `expected_seq`. X's unfenced write is now refused
//!    `WrongMode`.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use paros_journal::client::{CallId, Mode, Outcome};
use paros_journal::{Answer, Leader, LeaderUuid, Record, Seq};

use common::{OneProcess, Program, show};

const LEADER: u64 = 100;
const B: LeaderUuid = LeaderUuid(0xB);

fn records(text: &str) -> Vec<Record> {
    vec![Record::new(text.as_bytes().to_vec())]
}

fn main() {
    let mut journal = OneProcess::new(LEADER);
    let mut x = Program::new(LEADER);
    let mut y = Program::new(LEADER);

    println!("1. Segment 1 opens; X makes the journal multi-writer.");
    let call = x.client.set_leader(None, Leader::Unset);
    let outcome = x.run(&mut journal, call);
    show("X: set_leader(None, Unset)", &outcome);
    assert!(matches!(outcome, Outcome::LeaderSet(v) if v.leader == Leader::Multi));
    assert_eq!(x.client.mode(), Mode::Open);

    println!("2. X and Y write at the same time; the journal orders them.");
    let x0 = x.client.write(records("x0"));
    let x1 = x.client.write(records("x1"));
    let y0 = y.client.write(records("y0"));
    let mut seqs = Vec::new();
    for (name, program, call) in run_both(&mut journal, &mut x, &mut y, &[x0, x1], &[y0]) {
        show(&format!("{name}: write"), &program);
        let Outcome::Written { seq, count: 1 } = program else {
            panic!("an open write is written: {call:?} {program:?}");
        };
        seqs.push(seq);
    }
    seqs.sort();
    assert_eq!(
        seqs,
        [Seq(0), Seq(1), Seq(2)],
        "one position for each write"
    );

    println!("3. A reply of Y is lost; the resend lands again: at-least-once.");
    let call = y.client.write(records("y1"));
    y.send(&mut journal);
    let lost = journal.settle();
    show("the reply nobody reads", &lost[0].outcome);
    y.lose();
    let outcome = y.run(&mut journal, call);
    show("Y: write(y1), after the resend", &outcome);
    assert!(matches!(
        outcome,
        Outcome::Written {
            seq: Seq(4),
            count: 1
        }
    ));
    assert_eq!(journal.fold(11).next_seq(), Seq(5), "y1 is at 3 and at 4");

    println!("4. B claims the journal; back to one writer.");
    let mut b = Program::new(LEADER);
    let call = b.client.set_leader(Some(B), Leader::Multi);
    let outcome = b.run(&mut journal, call);
    show("B: set_leader(B, Multi)", &outcome);
    assert!(matches!(outcome, Outcome::LeaderSet(_)));
    let call = b.client.write(records("b5"));
    let outcome = b.run(&mut journal, call);
    show("B: write(b5) at expected_seq 5", &outcome);
    assert_eq!(
        outcome,
        Outcome::Written {
            seq: Seq(5),
            count: 1
        }
    );
    let call = x.client.write(records("x2"));
    let outcome = x.run(&mut journal, call);
    show("X: write(x2), unfenced", &outcome);
    assert!(matches!(outcome, Outcome::Refused(Answer::WrongMode(_))));
    println!("ok: open writes ordered, one landed twice, then one writer again.");
}

/// Run programs `x` and `y` together until every call of `xs` and `ys`
/// ended: each call's name, outcome and id.
fn run_both(
    journal: &mut OneProcess,
    x: &mut Program,
    y: &mut Program,
    xs: &[CallId],
    ys: &[CallId],
) -> Vec<(&'static str, Outcome, CallId)> {
    let mut ended = Vec::new();
    for _ in 0..100 {
        x.send(journal);
        y.send(journal);
        let replies = journal.settle();
        x.receive(&replies);
        y.receive(&replies);
        for call in xs {
            if let Some(outcome) = x.outcome(*call) {
                ended.push(("X", outcome, *call));
            }
        }
        for call in ys {
            if let Some(outcome) = y.outcome(*call) {
                ended.push(("Y", outcome, *call));
            }
        }
        if ended.len() == xs.len() + ys.len() {
            return ended;
        }
        journal.clock.advance(common::TICK);
    }
    panic!("the writes did not end in 100 ticks");
}
