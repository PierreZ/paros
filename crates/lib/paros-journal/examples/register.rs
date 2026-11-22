//! **The journal's matchmaker register, alone: the life of its segments.**
//!
//! Run it: `cargo run -p paros-journal --example register`
//!
//! Each journal has one register, a value that only changes by
//! compare-and-set ([`RegisterValue::apply`]). It holds the controller's
//! term and one record for each segment. Every step of the journal that
//! changes a segment (a reshape, a seal, a boundary, a retire, a trim) is
//! one [`Change`], and the register refuses a change whose guard does not
//! hold. This example applies the changes by hand, so it shows the guards
//! without the roles that call them (the `in_memory` example of
//! `paros-journal-io` shows those).
//!
//! # What the run shows
//!
//! 1. Controller C1 wins term 1 and opens segment 1, in force at slot 0.
//! 2. Controller C2 wins term 2. A late reshape of C1, at term 1, is
//!    refused: a deposed controller changes nothing.
//! 3. C2 opens segment 2, pending. The same call again answers ok (it is
//!    idempotent). A third segment is refused: segment 1 is not retired yet.
//! 4. The first leader of segment 2 seals segment 1 at slot 4, and segment
//!    2 is in force from slot 4, in one write.
//! 5. Segment 1 retires only with its boundary state and `F + 1` holders.
//! 6. Segment 1 is trimmed once a fold's floor passes its end.
//!
//! Each step asserts its answer, so CI runs this example as a test.
//!
//! The records and their guards are in `docs/journal/03-segments-and-records.md`
//! (§J3.4), and the controller's call in `docs/journal/06-controller.md` (§J6.1).

mod common;

use paros_journal::register::{Change, ChangeRefusal, RegisterValue};
use paros_journal::{
    BoundaryState, Grid, Leader, NodeId, OpeningCall, RELEASE, RecordRefusal, SegmentId,
    SegmentState, Seq, Slot,
};

use common::show;

const C1: NodeId = NodeId(1);
const C2: NodeId = NodeId(2);
const S1: SegmentId = SegmentId(1);
const S2: SegmentId = SegmentId(2);

/// The register of one journal, and the changes applied to it.
struct Register {
    value: RegisterValue,
}

impl Register {
    /// Apply `change`. A refusal leaves the value as it was.
    fn apply(&mut self, what: &str, change: &Change) -> Result<(), ChangeRefusal> {
        let answer = self.value.apply(change).map(|next| self.value = next);
        show(what, &answer);
        answer
    }

    fn start(&self, segment: SegmentId) -> Option<Slot> {
        self.value.segment(segment).expect("a record").start()
    }

    fn state(&self, segment: SegmentId) -> SegmentState {
        self.value.segment(segment).expect("a record").state()
    }
}

/// The controller's call at `term` that opens the segment after `expected`.
fn reshape(term: u64, expected: Option<SegmentId>, replicas: [u64; 3]) -> Change {
    Change::Reshape {
        opening: OpeningCall {
            term,
            expected,
            grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a v1 grid"),
            replicas: replicas.map(NodeId).to_vec(),
        },
        release: RELEASE,
    }
}

fn main() {
    let mut register = Register {
        value: RegisterValue::new(),
    };
    first_segment(&mut register);
    deposed(&mut register);
    second_segment(&mut register);
    seal(&mut register);
    retire(&mut register);
    trim(&mut register);
    println!("ok: the register took each change whose guard held, and only those.");
}

/// Step 1: the first controller and the first segment.
fn first_segment(register: &mut Register) {
    println!("1. C1 wins term 1 and opens segment 1.");
    let answer = register.apply("Elect(C1)", &Change::Elect { holder: C1 });
    assert_eq!(answer, Ok(()));
    let answer = register.apply(
        "Reshape(term 1, expected none)",
        &reshape(1, None, [11, 12, 13]),
    );
    assert_eq!(answer, Ok(()));
    assert_eq!(register.state(S1), SegmentState::InForce);
    assert_eq!(register.start(S1), Some(Slot(0)));
}

/// Step 2: a new term deposes C1.
fn deposed(register: &mut Register) {
    println!("2. C2 wins term 2; a late call of C1 is refused.");
    let answer = register.apply("Elect(C2)", &Change::Elect { holder: C2 });
    assert_eq!(answer, Ok(()));
    let answer = register.apply(
        "Reshape(term 1, expected 1) by C1",
        &reshape(1, Some(S1), [21, 22, 23]),
    );
    assert_eq!(answer, Err(ChangeRefusal::Term { held: 2, asked: 1 }));
}

/// Step 3: the next segment, a resend of its call, and one segment too many.
fn second_segment(register: &mut Register) {
    println!("3. C2 opens segment 2; a resend is ok; a third waits for segment 1 to retire.");
    let call = reshape(2, Some(S1), [21, 22, 23]);
    let answer = register.apply("Reshape(term 2, expected 1)", &call);
    assert_eq!(answer, Ok(()));
    assert_eq!(register.state(S2), SegmentState::Pending);
    let answer = register.apply("resend Reshape(term 2, expected 1)", &call);
    assert_eq!(answer, Ok(()));
    let answer = register.apply(
        "Reshape(term 2, expected 2)",
        &reshape(2, Some(S2), [31, 32, 33]),
    );
    assert_eq!(answer, Err(ChangeRefusal::Unretired(S1)));
}

/// Step 4: the seal of segment 1 puts segment 2 in force.
fn seal(register: &mut Register) {
    println!("4. The first leader of segment 2 seals segment 1 at slot 4.");
    let answer = register.apply(
        "Seal(segment 1, end 4)",
        &Change::Seal {
            segment: S1,
            end: Slot(4),
        },
    );
    assert_eq!(answer, Ok(()));
    assert_eq!(register.state(S1), SegmentState::Sealed);
    assert_eq!(register.state(S2), SegmentState::InForce);
    assert_eq!(register.start(S2), Some(Slot(4)));
}

/// Step 5: the retire needs the boundary state and `F + 1` holders.
fn retire(register: &mut Register) {
    println!("5. Segment 1 retires once its boundary is written and F + 1 replicas hold it.");
    let retire = |holders| Change::Retire {
        segment: S1,
        holders,
        unrepaired_damage: false,
    };
    let answer = register.apply("Retire(segment 1, 2 holders)", &retire(2));
    assert_eq!(
        answer,
        Err(ChangeRefusal::Record(S1, RecordRefusal::NoBoundary))
    );
    let boundary = BoundaryState {
        next_seq: Seq(3),
        first_seq: Seq(0),
        leader: Leader::Unset,
    };
    let answer = register.apply(
        "Boundary(segment 1, next_seq 3)",
        &Change::Boundary {
            segment: S1,
            state: boundary,
        },
    );
    assert_eq!(answer, Ok(()));
    let answer = register.apply("Retire(segment 1, 1 holder)", &retire(1));
    assert_eq!(
        answer,
        Err(ChangeRefusal::Record(
            S1,
            RecordRefusal::FewHolders {
                holders: 1,
                needed: 2
            }
        ))
    );
    let answer = register.apply("Retire(segment 1, 2 holders)", &retire(2));
    assert_eq!(answer, Ok(()));
    assert_eq!(register.state(S1), SegmentState::Retired);
}

/// Step 6: the trim of segment 1 once a fold's floor passes its end.
fn trim(register: &mut Register) {
    println!("6. Segment 1 is trimmed once a floor reaches its end.");
    let trim = |first_seq| Change::Trim {
        segment: S1,
        first_seq: Seq(first_seq),
    };
    let answer = register.apply("Trim(segment 1, first_seq 2)", &trim(2));
    assert_eq!(
        answer,
        Err(ChangeRefusal::Record(
            S1,
            RecordRefusal::FloorBelowEnd {
                first_seq: Seq(2),
                last: Seq(3)
            }
        ))
    );
    let answer = register.apply("Trim(segment 1, first_seq 3)", &trim(3));
    assert_eq!(answer, Ok(()));
    assert_eq!(register.state(S1), SegmentState::Trimmed);
}
