//! **The journal fold, alone: no network, no disk, no runtime.**
//!
//! Run it: `cargo run -p paros-journal --example fold`
//!
//! A journal is one ordered log. Paxos chooses one [`Command`] in each slot,
//! and each replica applies the chosen slots in order with the **journal
//! fold**, [`JournalState::apply`]. The fold decides what each command
//! means: a `Write` is accepted or refused there, at apply, never at
//! propose. This example calls the fold by hand, so it shows the meaning of
//! the log without the roles that choose it (the `in_memory` example of `paros-journal-io` shows those).
//!
//! # What the run shows
//!
//! 1. A fresh journal has no writer. `SetLeader(A, Unset)` makes writer A
//!    the one writer (single-writer mode).
//! 2. A writes at `expected_seq` 0, then at 2. A resend of the first write
//!    answers `Duplicate`: exactly-once comes from `expected_seq` alone, with
//!    no table of request ids.
//! 3. A write at a position that holds another write answers `Taken`; a
//!    write past the next position answers `Early`.
//! 4. Writer B takes the journal with `SetLeader(B, A)`. A late write of A
//!    answers `Fenced`.
//! 5. B truncates the journal up to position 2.
//! 6. **A seal.** The segment ends, and the next segment folds from its
//!    boundary state. A resend of a write of the old segment answers
//!    `Below`: the new fold does not hold the old positions. The writer
//!    settles it with [`resolve`] at a replica of the old segment.
//! 7. `SetLeader(None, B)` makes the journal multi-writer: writes carry no
//!    fence and the journal gives each one its position.
//!
//! Each step asserts its answer, so CI runs this example as a test.
//!
//! The rules are in `docs/journal/02-data-plane.md` (§J2) and
//! `docs/journal/03-segments-and-records.md` (§J3.1).

mod common;

use std::collections::BTreeMap;

use paros_journal::{
    Answer, BoundaryState, Command, JournalState, JournalView, Leader, LeaderUuid, ResolveAnswer,
    Seq, Write, resolve,
};

use common::{fenced, open, show, written};

const A: LeaderUuid = LeaderUuid(0xA);
const B: LeaderUuid = LeaderUuid(0xB);

/// One segment as a replica holds it: the fold, and the write that took
/// each position. The fold asks for the second one to tell a resend
/// (`Duplicate`) from another write (`Taken`): it keeps no copy of the
/// records itself.
struct Segment {
    state: JournalState,
    positions: BTreeMap<Seq, Write>,
}

impl Segment {
    /// A segment that folds from `boundary`.
    fn from(boundary: BoundaryState) -> Self {
        Self {
            state: JournalState::from_boundary(boundary),
            positions: BTreeMap::new(),
        }
    }

    /// Apply one chosen command, as a replica does in slot order.
    fn apply(&mut self, what: &str, command: Command) -> Answer {
        let positions = &self.positions;
        let answer = self.state.apply(&command, |seq| positions.get(&seq));
        if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
            self.positions.insert(*seq, write);
        }
        show(what, &answer);
        answer
    }

    /// The `Resolve` call at a replica of this segment: read only.
    fn resolve(&self, what: &str, write: &Write) -> ResolveAnswer {
        let paros_journal::Fence::Leader { uuid, expected_seq } = write.fence else {
            unreachable!("only a fenced write is resolved");
        };
        let answer = resolve(&self.state, uuid, expected_seq, &write.records, |seq| {
            self.positions.get(&seq)
        });
        show(what, &answer);
        answer
    }
}

fn view(leader: Leader, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn main() {
    let mut first = single_writer();
    new_writer(&mut first);
    let mut second = seal(&first);
    multi_writer(&mut second);
    println!("ok: the fold answered every step as §J2 says.");
}

/// Steps 1 to 3: one writer, its resends and its wrong positions.
fn single_writer() -> Segment {
    println!("1. A fresh journal: no writer until a SetLeader.");
    let mut first = Segment::from(BoundaryState::fresh(Leader::Unset));
    let answer = first.apply(
        "Write(A, expected_seq 0)",
        Command::Write(fenced(A, 0, &["x"])),
    );
    assert_eq!(answer, Answer::Fenced(view(Leader::Unset, 0, 0)));
    let answer = first.apply(
        "SetLeader(new A, old Unset)",
        Command::SetLeader {
            new: Some(A),
            old: Leader::Unset,
        },
    );
    assert_eq!(answer, Answer::LeaderSet(view(Leader::Uuid(A), 0, 0)));

    println!("2. A writes; a resend is a Duplicate.");
    let a0 = fenced(A, 0, &["a0", "a1"]);
    let answer = first.apply("Write(A, 0, [a0, a1])", Command::Write(a0.clone()));
    assert_eq!(answer, written(0, 2));
    let answer = first.apply("Write(A, 2, [a2])", Command::Write(fenced(A, 2, &["a2"])));
    assert_eq!(answer, written(2, 1));
    let answer = first.apply("resend Write(A, 0, [a0, a1])", Command::Write(a0));
    assert_eq!(
        answer,
        Answer::Duplicate {
            seq: Seq(0),
            count: 2
        }
    );

    println!("3. A wrong position: Taken below next_seq, Early above it.");
    let answer = first.apply(
        "Write(A, 2, [other])",
        Command::Write(fenced(A, 2, &["other"])),
    );
    assert_eq!(answer, Answer::Taken(view(Leader::Uuid(A), 3, 0)));
    let answer = first.apply("Write(A, 9, [a9])", Command::Write(fenced(A, 9, &["a9"])));
    assert_eq!(answer, Answer::Early(view(Leader::Uuid(A), 3, 0)));

    first
}

/// Steps 4 and 5: writer B fences writer A, then truncates.
fn new_writer(first: &mut Segment) {
    println!("4. B takes the journal; A is fenced.");
    let answer = first.apply(
        "SetLeader(new B, old A)",
        Command::SetLeader {
            new: Some(B),
            old: Leader::Uuid(A),
        },
    );
    assert_eq!(answer, Answer::LeaderSet(view(Leader::Uuid(B), 3, 0)));
    let answer = first.apply(
        "Write(A, 3, [late])",
        Command::Write(fenced(A, 3, &["late"])),
    );
    assert_eq!(answer, Answer::Fenced(view(Leader::Uuid(B), 3, 0)));
    let answer = first.apply("Write(B, 3, [b3])", Command::Write(fenced(B, 3, &["b3"])));
    assert_eq!(answer, written(3, 1));

    println!("5. B truncates up to position 2.");
    let answer = first.apply(
        "Truncate(B, up_to 2)",
        Command::Truncate {
            fence: Some(B),
            up_to: Seq(2),
        },
    );
    assert_eq!(answer, Answer::Trimmed(view(Leader::Uuid(B), 4, 2)));
    let answer = first.apply(
        "resend Write(A, 0, [a0, a1])",
        Command::Write(fenced(A, 0, &["a0", "a1"])),
    );
    assert_eq!(answer, Answer::Truncated(view(Leader::Uuid(B), 4, 2)));
}

/// Step 6: the seal, and a resend across it settled by `Resolve`.
fn seal(first: &Segment) -> Segment {
    println!("6. A seal: the next segment folds from the boundary state.");
    let a2 = fenced(A, 2, &["a2"]);
    let b3 = fenced(B, 3, &["b3"]);
    let boundary = first.state.boundary();
    assert_eq!(
        boundary,
        BoundaryState {
            next_seq: Seq(4),
            first_seq: Seq(2),
            leader: Leader::Uuid(B),
        }
    );
    let mut second = Segment::from(boundary);
    let answer = second.apply("resend Write(B, 3, [b3])", Command::Write(b3.clone()));
    assert_eq!(answer, Answer::Below(view(Leader::Uuid(B), 4, 2)));
    let answer = first.resolve("Resolve(B, 3, [b3]) at the old segment", &b3);
    assert_eq!(
        answer,
        ResolveAnswer::Written {
            seq: Seq(3),
            count: 1
        }
    );
    let answer = first.resolve(
        "Resolve(B, 3, [other]) at the old segment",
        &fenced(B, 3, &["other"]),
    );
    assert_eq!(answer, ResolveAnswer::NotWritten);
    let answer = first.resolve("Resolve(A, 2, [a2]) at the old segment", &a2);
    assert_eq!(
        answer,
        ResolveAnswer::Written {
            seq: Seq(2),
            count: 1
        }
    );
    let b4 = fenced(B, 4, &["b4"]);
    let answer = second.apply("Write(B, 4, [b4])", Command::Write(b4));
    assert_eq!(answer, written(4, 1));

    second
}

/// Step 7: the move to multi-writer.
fn multi_writer(second: &mut Segment) {
    println!("7. Multi-writer: SetLeader(None, B), then unfenced writes.");
    let answer = second.apply(
        "SetLeader(new None, old B)",
        Command::SetLeader {
            new: None,
            old: Leader::Uuid(B),
        },
    );
    assert_eq!(answer, Answer::LeaderSet(view(Leader::Multi, 5, 2)));
    let answer = second.apply("Write(open, [m0, m1])", Command::Write(open(&["m0", "m1"])));
    assert_eq!(answer, written(5, 2));
    let answer = second.apply("Write(B, 7, [b7])", Command::Write(fenced(B, 7, &["b7"])));
    assert_eq!(answer, Answer::Fenced(view(Leader::Multi, 7, 2)));

    assert_eq!(second.state.view(), view(Leader::Multi, 7, 2));
}
