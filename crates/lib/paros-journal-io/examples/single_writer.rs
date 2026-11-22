//! **One writer, exactly-once: the pure journal client.**
//!
//! Run it: `cargo run -p paros-journal-io --example single_writer`
//!
//! A program writes through a [`Client`] of `paros_journal::client`
//! (#1331). The client is a pure state machine: it gives the program the
//! requests to send, the program gives it the answers, and the client says
//! how each call ended. The program never builds a command, never picks a
//! position and never retries by hand: the client keeps the writer's
//! position, `expected_seq`, and settles every ambiguous answer itself
//! (`docs/journal/02-data-plane.md` §J2.2).
//!
//! One process serves every role of the journal (three acceptors, the
//! replicas, one leader at a time), on the backends of
//! `paros_journal_io::mem`. The glue that sends the requests is
//! `common::Program`.
//!
//! # What the run shows
//!
//! 1. Writer A claims the journal with a fresh uuid, and writes.
//! 2. A reply is lost. The client sends the identical write again, at the
//!    same `expected_seq`; the journal answers `Duplicate`, and the write
//!    is in the journal once.
//! 3. A reply is lost, and the controller opens segment 2 before the
//!    resend: the new leader answers `Below`. The client asks a replica of
//!    segment 1 with `Resolve`: written.
//! 4. Writer A writes in segment 2, at the next position.
//! 5. Writer B claims the journal: A's next write is refused `Fenced`.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use paros_journal::client::{Mode, Outcome};
use paros_journal::{Answer, Leader, LeaderUuid, Record, Seq};

use common::{A, OneProcess, Program, show};

const L1: u64 = 100;
const L2: u64 = 200;
const B: LeaderUuid = LeaderUuid(0xB);

fn records(text: &str) -> Vec<Record> {
    vec![Record::new(text.as_bytes().to_vec())]
}

fn written(seq: u64) -> Outcome {
    Outcome::Written {
        seq: Seq(seq),
        count: 1,
    }
}

fn main() {
    let mut journal = OneProcess::new(L1);
    let mut a = Program::new(L1);

    println!("1. Writer A claims the journal, and writes.");
    let call = a.client.set_leader(Some(A), Leader::Unset);
    let outcome = a.run(&mut journal, call);
    show("set_leader(A, Unset)", &outcome);
    assert!(matches!(outcome, Outcome::LeaderSet(_)));
    assert_eq!(
        a.client.mode(),
        Mode::Fenced {
            uuid: A,
            next: Seq(0)
        }
    );
    let call = a.client.write(records("a0"));
    let outcome = a.run(&mut journal, call);
    show("write(a0)", &outcome);
    assert_eq!(outcome, written(0));

    println!("2. A reply is lost; the client sends the same write again.");
    let call = a.client.write(records("a1"));
    a.send(&mut journal);
    let lost = journal.settle();
    show("the reply nobody reads", &lost[0].outcome);
    a.lose();
    let outcome = a.run(&mut journal, call);
    show("write(a1), after the resend", &outcome);
    assert_eq!(outcome, written(1));
    assert_eq!(a.resolves, 0, "the resend was answered Duplicate");
    assert_eq!(
        journal.fold(11).next_seq(),
        Seq(2),
        "a1 is in the journal once"
    );

    println!("3. A reply is lost, and segment 2 opens before the resend.");
    let call = a.client.write(records("a2"));
    a.send(&mut journal);
    journal.settle();
    a.lose();
    journal.reshape(2, L1, L2);
    a.leader = L2;
    let outcome = a.run(&mut journal, call);
    show("write(a2): Below, then Resolve at segment 1", &outcome);
    assert_eq!(outcome, written(2));
    assert_eq!(a.resolves, 1, "the resend was answered Below");
    assert_eq!(
        journal.fold(21).next_seq(),
        Seq(3),
        "segment 2 starts after a2"
    );

    println!("4. Writer A writes in segment 2.");
    let call = a.client.write(records("a3"));
    let outcome = a.run(&mut journal, call);
    show("write(a3)", &outcome);
    assert_eq!(outcome, written(3));

    println!("5. Writer B claims the journal; A is deposed.");
    let mut b = Program::new(L2);
    let call = b.client.set_leader(Some(B), Leader::Uuid(A));
    let outcome = b.run(&mut journal, call);
    show("set_leader(B, A)", &outcome);
    assert!(matches!(outcome, Outcome::LeaderSet(_)));
    let call = a.client.write(records("a4"));
    let outcome = a.run(&mut journal, call);
    show("write(a4) by A", &outcome);
    assert!(matches!(outcome, Outcome::Refused(Answer::Fenced(_))));
    assert_eq!(journal.fold(21).next_seq(), Seq(4));
    println!("ok: four writes, each once, across a lost reply and a seal.");
}
