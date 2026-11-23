use std::collections::BTreeMap;

use super::*;
use crate::command::Record;

const U1: LeaderUuid = LeaderUuid(1);
const U2: LeaderUuid = LeaderUuid(2);
/// Two multi-writer modes, each with its own tag.
const M1: Leader = Leader::Multi(LeaderUuid(101));
const M2: Leader = Leader::Multi(LeaderUuid(102));

/// A segment under test: its fold and the writes it took, by position.
struct Segment {
    state: JournalState,
    log: BTreeMap<Seq, Write>,
}

impl Segment {
    fn fresh(leader: Leader) -> Self {
        Self::from(BoundaryState::fresh(leader))
    }

    fn from(boundary: BoundaryState) -> Self {
        Self {
            state: JournalState::from_boundary(boundary),
            log: BTreeMap::new(),
        }
    }

    fn apply(&mut self, command: &Command) -> Answer {
        let answer = self.state.apply(command, |seq| self.log.get(&seq));
        if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
            self.log.insert(*seq, write.clone());
        }
        answer
    }

    /// The next segment, after a seal.
    fn seal(&self) -> Self {
        Self::from(self.state.boundary())
    }
}

fn records(bytes: &[u8]) -> Vec<Record> {
    bytes.iter().map(|b| Record::new(vec![*b])).collect()
}

fn write(uuid: LeaderUuid, seq: u64, bytes: &[u8]) -> Command {
    Command::Write(Write {
        fence: Fence::Leader {
            uuid,
            expected_seq: Seq(seq),
        },
        records: records(bytes),
    })
}

fn open(bytes: &[u8]) -> Command {
    Command::Write(Write {
        fence: Fence::Open,
        records: records(bytes),
    })
}

fn set(new: Leader, old: Leader) -> Command {
    Command::SetLeader { new, old }
}

fn truncate(fence: Option<LeaderUuid>, up_to: u64) -> Command {
    Command::Truncate {
        fence,
        up_to: Seq(up_to),
    }
}

fn led(uuid: LeaderUuid) -> Leader {
    Leader::uuid(uuid)
}

/// A journal led by `U1` that holds `[a, b]` at 0 and `[c]` at 2.
fn claimed() -> Segment {
    let mut seg = Segment::fresh(Leader::Unset);
    assert!(matches!(
        seg.apply(&set(led(U1), Leader::Unset)),
        Answer::LeaderSet(_)
    ));
    assert_eq!(
        seg.apply(&write(U1, 0, b"ab")),
        Answer::Written {
            seq: Seq(0),
            count: 2
        }
    );
    assert_eq!(
        seg.apply(&write(U1, 2, b"c")),
        Answer::Written {
            seq: Seq(2),
            count: 1
        }
    );
    seg
}

#[test]
fn a_fresh_journal_refuses_every_write_until_a_set_leader() {
    let mut seg = Segment::fresh(Leader::Unset);
    assert!(matches!(seg.apply(&write(U1, 0, b"a")), Answer::Fenced(_)));
    assert!(matches!(seg.apply(&open(b"a")), Answer::WrongMode(_)));
    assert!(matches!(
        seg.apply(&truncate(Some(U1), 0)),
        Answer::Fenced(_)
    ));
    assert!(matches!(
        seg.apply(&truncate(None, 0)),
        Answer::WrongMode(_)
    ));
    assert_eq!(seg.state.next_seq(), Seq(0));
}

#[test]
fn a_fenced_write_lands_at_the_next_position_under_the_leader() {
    let seg = claimed();
    assert_eq!(seg.state.next_seq(), Seq(3));
    assert_eq!(seg.state.leader(), led(U1));
}

#[test]
fn a_write_above_the_next_position_is_early() {
    let mut seg = claimed();
    assert!(matches!(seg.apply(&write(U1, 4, b"d")), Answer::Early(_)));
}

#[test]
fn a_write_of_another_uuid_is_fenced() {
    let mut seg = claimed();
    let Answer::Fenced(view) = seg.apply(&write(U2, 3, b"d")) else {
        panic!("fenced");
    };
    assert_eq!(view.leader, led(U1), "the refusal names the leader");
    assert_eq!(view.next_seq, Seq(3), "and the next position");
}

#[test]
fn a_resend_of_the_same_write_is_a_duplicate() {
    let mut seg = claimed();
    assert_eq!(
        seg.apply(&write(U1, 0, b"ab")),
        Answer::Duplicate {
            seq: Seq(0),
            count: 2
        }
    );
}

#[test]
fn a_resend_with_another_leader_time_is_a_duplicate() {
    let mut seg = claimed();
    let mut resend = Write {
        fence: Fence::Leader {
            uuid: U1,
            expected_seq: Seq(2),
        },
        records: records(b"c"),
    };
    resend.records[0].headers.leader_time = Some(77);
    assert_eq!(
        seg.apply(&Command::Write(resend)),
        Answer::Duplicate {
            seq: Seq(2),
            count: 1
        }
    );
}

#[test]
fn another_write_at_a_taken_position_is_taken() {
    let mut seg = claimed();
    assert!(matches!(seg.apply(&write(U1, 0, b"xy")), Answer::Taken(_)));
    // A position inside a batch: no write starts there.
    assert!(matches!(seg.apply(&write(U1, 1, b"b")), Answer::Taken(_)));
}

#[test]
fn the_position_is_judged_before_the_fence() {
    // `dedup_after` of multi-writer/: after SetLeader(M1, U1), the resend
    // of a written write of U1 is a duplicate, not fenced.
    let mut seg = claimed();
    assert!(matches!(seg.apply(&set(M1, led(U1))), Answer::LeaderSet(_)));
    assert_eq!(
        seg.apply(&write(U1, 2, b"c")),
        Answer::Duplicate {
            seq: Seq(2),
            count: 1
        }
    );
    // An old position that holds another write: taken, not fenced.
    assert!(matches!(seg.apply(&write(U2, 2, b"z")), Answer::Taken(_)));
    // At the next position, the fence: fenced.
    assert!(matches!(seg.apply(&write(U1, 3, b"d")), Answer::Fenced(_)));
}

#[test]
fn a_write_below_the_floor_is_truncated() {
    let mut seg = claimed();
    assert!(matches!(
        seg.apply(&truncate(Some(U1), 2)),
        Answer::Trimmed(_)
    ));
    assert!(matches!(
        seg.apply(&write(U1, 0, b"ab")),
        Answer::Truncated(_)
    ));
    assert!(matches!(
        seg.apply(&write(U1, 2, b"c")),
        Answer::Duplicate { .. }
    ));
}

#[test]
fn an_empty_write_is_refused() {
    let mut seg = claimed();
    assert!(matches!(seg.apply(&write(U1, 3, b"")), Answer::Empty(_)));
    assert!(matches!(seg.apply(&open(b"")), Answer::Empty(_)));
}

#[test]
fn a_multi_writer_journal_takes_open_writes_at_the_next_position() {
    let mut seg = Segment::fresh(M1);
    assert_eq!(
        seg.apply(&open(b"ab")),
        Answer::Written {
            seq: Seq(0),
            count: 2
        }
    );
    assert_eq!(
        seg.apply(&open(b"ab")),
        Answer::Written {
            seq: Seq(2),
            count: 2
        },
        "a resend lands again: at-least-once"
    );
    assert!(matches!(seg.apply(&write(U1, 4, b"c")), Answer::Fenced(_)));
}

#[test]
fn an_open_write_on_a_single_writer_journal_is_wrong_mode() {
    let mut seg = claimed();
    assert!(matches!(seg.apply(&open(b"a")), Answer::WrongMode(_)));
}

#[test]
fn set_leader_none_and_back() {
    let mut seg = claimed();
    assert!(matches!(seg.apply(&set(M1, led(U1))), Answer::LeaderSet(_)));
    assert_eq!(seg.state.leader(), M1);
    assert!(matches!(seg.apply(&open(b"m")), Answer::Written { .. }));
    let Answer::LeaderSet(view) = seg.apply(&set(led(U2), M1)) else {
        panic!("set back");
    };
    assert_eq!(view.next_seq, Seq(4), "the view gives the writer its basis");
    assert!(matches!(seg.apply(&open(b"m")), Answer::WrongMode(_)));
    assert!(matches!(
        seg.apply(&write(U2, 4, b"d")),
        Answer::Written { .. }
    ));
}

#[test]
fn set_leader_is_a_compare_and_set() {
    let mut seg = claimed();
    assert!(matches!(
        seg.apply(&set(led(U2), Leader::Unset)),
        Answer::LeaderRefused(_)
    ));
    assert!(matches!(
        seg.apply(&set(led(U1), led(U1))),
        Answer::LeaderRefused(_)
    ));
    // A client can send any `new`: one that SetLeader may not install is
    // refused.
    for new in [
        Leader::Uuid(LeaderUuid::UNSET),
        Leader::Multi(LeaderUuid::UNSET),
        Leader::Unset,
    ] {
        assert!(matches!(
            seg.apply(&set(new, led(U1))),
            Answer::LeaderRefused(_)
        ));
    }
    assert!(matches!(
        seg.apply(&set(led(U2), led(U1))),
        Answer::LeaderSet(_)
    ));
    // A reinstated uuid leads again (decision C, §J2.3).
    assert!(matches!(
        seg.apply(&set(led(U1), led(U2))),
        Answer::LeaderSet(_)
    ));
    let mut multi = Segment::fresh(M1);
    assert!(matches!(
        multi.apply(&set(M1, M1)),
        Answer::LeaderRefused(_)
    ));
    // `old` names the mode with its tag: another tag is another value.
    assert!(matches!(
        multi.apply(&set(led(U1), M2)),
        Answer::LeaderRefused(_)
    ));
    // A new mode with a fresh tag replaces the one in force.
    assert!(matches!(multi.apply(&set(M2, M1)), Answer::LeaderSet(_)));
    assert_eq!(multi.state.leader(), M2);
}

#[test]
fn a_late_copy_of_a_set_leader_never_applies_after_a_later_mode() {
    // #1495 (a late copy of SetLeader reinstates a uuid), seed 100603 of the
    // client walk: SetLeader(U1, M1) applies, then SetLeader(M2, U1) makes
    // the journal multi-writer again, and U1's write is refused `Fenced`, a
    // final answer. A late copy of the first SetLeader must not bring U1
    // back, or a late copy of the refused write lands.
    let mut seg = Segment::fresh(M1);
    let claim = set(led(U1), M1);
    assert!(matches!(seg.apply(&claim), Answer::LeaderSet(_)));
    assert!(matches!(
        seg.apply(&write(U1, 0, b"a")),
        Answer::Written { .. }
    ));
    assert!(matches!(seg.apply(&set(M2, led(U1))), Answer::LeaderSet(_)));
    let refused = write(U1, 1, b"b");
    assert!(matches!(seg.apply(&refused), Answer::Fenced(_)));
    assert!(
        matches!(seg.apply(&claim), Answer::LeaderRefused(_)),
        "a late copy of SetLeader(U1, M1) does not match the mode M2"
    );
    assert_eq!(seg.state.leader(), M2);
    assert!(matches!(seg.apply(&refused), Answer::Fenced(_)));
    assert_eq!(
        seg.state.next_seq(),
        Seq(1),
        "the refused write never lands"
    );
}

#[test]
fn a_deposed_writer_is_fenced() {
    // The `Move` script of journal-state/.
    let mut seg = Segment::fresh(Leader::Unset);
    assert!(matches!(
        seg.apply(&set(led(U1), Leader::Unset)),
        Answer::LeaderSet(_)
    ));
    assert!(matches!(
        seg.apply(&set(led(U2), led(U1))),
        Answer::LeaderSet(_)
    ));
    assert!(matches!(seg.apply(&write(U1, 0, b"a")), Answer::Fenced(_)));
}

#[test]
fn truncate_is_fenced_monotone_and_clamped() {
    let mut seg = claimed();
    assert!(matches!(
        seg.apply(&truncate(Some(U2), 1)),
        Answer::Fenced(_)
    ));
    assert!(matches!(
        seg.apply(&truncate(None, 1)),
        Answer::WrongMode(_)
    ));
    let Answer::Trimmed(view) = seg.apply(&truncate(Some(U1), 9)) else {
        panic!("trimmed");
    };
    assert_eq!(view.first_seq, Seq(3), "clamped to next_seq");
    let Answer::Trimmed(view) = seg.apply(&truncate(Some(U1), 1)) else {
        panic!("trimmed");
    };
    assert_eq!(view.first_seq, Seq(3), "never lowered");
    let mut multi = Segment::fresh(M1);
    assert!(matches!(multi.apply(&open(b"ab")), Answer::Written { .. }));
    assert!(matches!(
        multi.apply(&truncate(None, 1)),
        Answer::Trimmed(_)
    ));
    assert!(matches!(
        multi.apply(&truncate(Some(U1), 2)),
        Answer::WrongMode(_)
    ));
    assert_eq!(multi.state.first_seq(), Seq(1));
}

#[test]
fn a_resend_across_a_seal_is_below() {
    // The `Resend` script of journal-state/ across a seal (#1301): the
    // fold of the next segment holds no record of the old one.
    let old = claimed();
    let mut next = old.seal();
    assert_eq!(next.state.start_seq(), Seq(3));
    assert!(matches!(next.apply(&write(U1, 0, b"ab")), Answer::Below(_)));
    assert!(matches!(next.apply(&write(U1, 2, b"zz")), Answer::Below(_)));
    // At the start of the new segment the fold judges as before.
    assert!(matches!(
        next.apply(&write(U1, 3, b"d")),
        Answer::Written { .. }
    ));
    assert!(matches!(
        next.apply(&write(U1, 3, b"d")),
        Answer::Duplicate { .. }
    ));
}

#[test]
fn truncated_comes_before_below() {
    let mut old = claimed();
    assert!(matches!(
        old.apply(&truncate(Some(U1), 2)),
        Answer::Trimmed(_)
    ));
    let mut next = old.seal();
    assert!(matches!(
        next.apply(&write(U1, 1, b"b")),
        Answer::Truncated(_)
    ));
    assert!(matches!(next.apply(&write(U1, 2, b"c")), Answer::Below(_)));
}

#[test]
fn the_boundary_carries_the_leader_and_the_floor() {
    let mut old = claimed();
    assert!(matches!(
        old.apply(&truncate(Some(U1), 1)),
        Answer::Trimmed(_)
    ));
    assert!(matches!(old.apply(&set(M1, led(U1))), Answer::LeaderSet(_)));
    let boundary = old.state.boundary();
    assert_eq!(
        boundary,
        BoundaryState {
            next_seq: Seq(3),
            first_seq: Seq(1),
            leader: M1
        }
    );
    let mut next = old.seal();
    assert!(matches!(
        next.apply(&open(b"m")),
        Answer::Written { seq: Seq(3), .. }
    ));
    assert!(next.state.boundary().follows(&boundary));
    assert!(!boundary.follows(&next.state.boundary()));
}

#[test]
fn an_empty_segment_passes_its_boundary_on() {
    // seal-chain/: a segment sealed before its first slot gives the record
    // of its predecessor again.
    let old = claimed();
    let empty = old.seal();
    let next = empty.seal();
    assert_eq!(next.state.boundary(), old.state.boundary());
}

#[test]
fn reserved_and_noop_move_nothing() {
    let mut seg = claimed();
    let before = seg.state;
    assert!(matches!(
        seg.apply(&Command::Upgrade {
            release: crate::codec::Release(2)
        }),
        Answer::Reserved(_)
    ));
    assert_eq!(seg.apply(&Command::Noop), Answer::Noop);
    assert_eq!(seg.state, before);
}

#[test]
fn a_refusal_names_the_state_it_was_judged_against() {
    let mut seg = claimed();
    let view = seg.state.view();
    for command in [
        write(U2, 3, b"x"),
        write(U1, 9, b"x"),
        write(U1, 0, b"zz"),
        open(b"x"),
        set(led(U2), M1),
    ] {
        let answer = seg.apply(&command);
        assert!(!answer.applied(), "{command:?} is refused");
        assert_eq!(seg.state.view(), view, "{command:?} moves nothing");
    }
}

#[test]
#[should_panic(expected = "a fresh journal is unset or multi-writer")]
fn a_fresh_journal_has_no_writer() {
    let _ = BoundaryState::fresh(led(U1));
}

#[test]
fn applied_names_the_answers_that_move_the_state() {
    let view = JournalState::from_boundary(BoundaryState::fresh(M1)).view();
    for moved in [
        Answer::Written {
            seq: Seq(0),
            count: 1,
        },
        Answer::LeaderSet(view),
        Answer::Trimmed(view),
    ] {
        assert!(moved.applied(), "{moved:?} moves the state");
    }
    for kept in [Answer::Noop, Answer::Fenced(view), Answer::Below(view)] {
        assert!(!kept.applied(), "{kept:?} moves nothing");
    }
}

#[test]
#[should_panic(expected = "a fold starts from a valid boundary state")]
fn a_boundary_with_its_floor_above_its_next_is_refused() {
    let _ = JournalState::from_boundary(BoundaryState {
        next_seq: Seq(1),
        first_seq: Seq(2),
        leader: M1,
    });
}

#[test]
#[should_panic(expected = "first position never passes")]
fn the_invariants_see_a_floor_above_next() {
    let mut state = JournalState::from_boundary(BoundaryState::fresh(M1));
    state.first_seq = Seq(1);
    state.assert_invariants();
}

#[test]
#[should_panic(expected = "a segment starts at or below")]
fn the_invariants_see_a_start_above_next() {
    let mut state = JournalState::from_boundary(BoundaryState::fresh(M1));
    state.start_seq = Seq(1);
    state.assert_invariants();
}

#[test]
#[should_panic(expected = "next_seq never decreases")]
fn the_transition_check_sees_next_seq_go_back() {
    let before = JournalState::from_boundary(BoundaryState {
        next_seq: Seq(3),
        first_seq: Seq(0),
        leader: M1,
    });
    let mut after = before;
    after.next_seq = Seq(2);
    after.assert_transition(&before, &Answer::Noop);
}

#[test]
fn a_fold_resumes_from_a_valid_boundary_at_or_below_its_next_position() {
    let boundary = BoundaryState {
        next_seq: Seq(4),
        first_seq: Seq(2),
        leader: Leader::Uuid(U1),
    };
    let state = JournalState::resume(boundary, Seq(3)).expect("a start below next_seq");
    assert_eq!(state.next_seq(), Seq(4));
    assert_eq!(state.first_seq(), Seq(2));
    assert_eq!(state.leader(), Leader::Uuid(U1));
    assert!(
        JournalState::resume(boundary, Seq(4)).is_some(),
        "a start at next_seq"
    );
    assert!(
        JournalState::resume(boundary, Seq(5)).is_none(),
        "a start above next_seq makes no fold"
    );
    let invalid = BoundaryState {
        next_seq: Seq(1),
        first_seq: Seq(2),
        leader: Leader::Uuid(U1),
    };
    assert!(!invalid.is_valid());
    assert!(
        JournalState::resume(invalid, Seq(0)).is_none(),
        "a boundary that is not valid makes no fold"
    );
}
