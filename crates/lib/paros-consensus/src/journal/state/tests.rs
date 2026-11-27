use super::{JournalState, Outcome, WriterMode};
use crate::journal::{
    Command, Control, Entry, Hold, LeaderUuid, ReplicaSet, Roll, Segment, SegmentId, Seq,
};
use crate::types::{NodeId, Slot, Value};

fn write(leader: u128, seq: u64, records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(leader),
        seq: Seq(seq),
        records: records.iter().map(|r| Value::from(r.to_vec())).collect(),
    })
}

fn truncate(leader: u128, up_to: u64) -> Command {
    Command::Control(Control::Truncate {
        leader: LeaderUuid(leader),
        up_to: Seq(up_to),
    })
}

fn set_leader(new: u128, old: Option<u128>) -> Command {
    Command::Control(Control::SetLeader {
        new: LeaderUuid(new),
        old: old.map(LeaderUuid),
    })
}

/// Apply `commands` in order against a log that remembers each accepted
/// write, as the replica's positions index does.
fn fold(commands: &[Command]) -> (JournalState, Vec<Outcome>) {
    fold_in(WriterMode::Single, commands)
}

fn fold_in(mode: WriterMode, commands: &[Command]) -> (JournalState, Vec<Outcome>) {
    let mut state = JournalState::default();
    let mut accepted: Vec<Entry> = Vec::new();
    let mut outcomes = Vec::new();
    for (slot, command) in commands.iter().enumerate() {
        let slot = Slot(u64::try_from(slot).expect("a test log fits in u64"));
        let outcome = state.apply(mode, slot, command, |seq| {
            accepted.iter().find(|e| e.seq == seq)
        });
        if let (Outcome::Accepted { .. }, Command::Write(entry)) = (&outcome, command) {
            accepted.push(entry.clone());
        }
        outcomes.push(outcome);
    }
    (state, outcomes)
}

#[test]
fn nobody_writes_before_the_first_set_leader() {
    let (state, outcomes) = fold(&[write(1, 0, &[b"a"])]);
    assert!(matches!(outcomes[0], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(0));
}

#[test]
fn a_leader_writes_dense_positions_and_a_batch_takes_one_per_record() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        write(1, 2, &[b"c"]),
    ]);
    assert_eq!(
        outcomes[1],
        Outcome::Accepted {
            seq: Seq(0),
            count: 2
        }
    );
    assert_eq!(
        outcomes[2],
        Outcome::Accepted {
            seq: Seq(2),
            count: 1
        }
    );
    assert_eq!(state.next_seq, Seq(3));
}

#[test]
fn a_gap_a_superseded_leader_and_a_foreign_uuid_are_refused() {
    let (_, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 1, &[b"gap"]),
        write(2, 0, &[b"foreign"]),
        set_leader(2, Some(1)),
        write(1, 0, &[b"stale"]),
    ]);
    assert!(matches!(outcomes[1], Outcome::Refused(_)));
    assert!(matches!(outcomes[2], Outcome::Refused(_)));
    assert!(matches!(outcomes[4], Outcome::Refused(v) if v.leader == Some(LeaderUuid(2))));
}

#[test]
fn a_retry_is_acked_only_with_the_same_bytes() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        write(1, 0, &[b"a"]),
        write(1, 0, &[b"other"]),
    ]);
    assert_eq!(
        outcomes[2],
        Outcome::Duplicate {
            seq: Seq(0),
            count: 1
        }
    );
    assert!(matches!(outcomes[3], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(1));
}

#[test]
fn a_retry_survives_a_leadership_change_but_not_a_truncation() {
    let (_, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        set_leader(2, Some(1)),
        write(1, 0, &[b"a"]),
        truncate(2, 1),
        write(1, 0, &[b"a"]),
    ]);
    assert!(matches!(outcomes[3], Outcome::Duplicate { .. }));
    assert!(matches!(outcomes[5], Outcome::Truncated(v) if v.first_seq == Seq(1)));
}

#[test]
fn set_leader_is_a_compare_and_set_and_raises_the_term() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        set_leader(2, None),
        set_leader(2, Some(1)),
    ]);
    assert!(matches!(outcomes[0], Outcome::Leader(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[1], Outcome::LeaderRefused(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[2], Outcome::Leader(_)));
    assert_eq!(state.leader, Some(LeaderUuid(2)));
    assert_eq!(state.term, 2);
}

#[test]
fn the_unset_uuid_and_the_current_leader_never_win() {
    let (state, outcomes) = fold(&[
        set_leader(0, None),
        set_leader(1, None),
        set_leader(1, Some(1)),
    ]);
    assert!(matches!(outcomes[0], Outcome::LeaderRefused(v) if v.leader.is_none()));
    assert!(matches!(outcomes[2], Outcome::LeaderRefused(_)));
    assert_eq!(state.term, 1);
}

#[test]
fn truncate_is_monotone_and_clamped_to_the_next_position() {
    let (state, _) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        truncate(1, 9),
        truncate(1, 1),
    ]);
    assert_eq!(state.first_seq, Seq(2));
}

#[test]
fn truncate_is_fenced_like_a_write() {
    let (state, outcomes) = fold(&[
        truncate(1, 0),
        set_leader(1, None),
        write(1, 0, &[b"a", b"b"]),
        truncate(2, 1),
        set_leader(2, Some(1)),
        truncate(1, 2),
        truncate(2, 1),
    ]);
    assert!(matches!(outcomes[0], Outcome::TruncateRefused(v) if v.leader.is_none()));
    assert!(matches!(outcomes[3], Outcome::TruncateRefused(v) if v.first_seq == Seq(0)));
    assert!(matches!(outcomes[5], Outcome::TruncateRefused(v) if v.leader == Some(LeaderUuid(2))));
    assert!(matches!(outcomes[6], Outcome::Trimmed(v) if v.first_seq == Seq(1)));
    assert_eq!(state.first_seq, Seq(1));
}

#[test]
fn a_multi_writer_journal_takes_any_write_at_the_next_position() {
    let (state, outcomes) = fold_in(
        WriterMode::Multi,
        &[
            write(0, 7, &[b"a", b"b"]),
            write(0, 0, &[b"a", b"b"]),
            write(0, 0, &[]),
        ],
    );
    assert_eq!(
        outcomes[0],
        Outcome::Accepted {
            seq: Seq(0),
            count: 2
        }
    );
    // No deduplication: the same bytes land again.
    assert_eq!(
        outcomes[1],
        Outcome::Accepted {
            seq: Seq(2),
            count: 2
        }
    );
    assert!(matches!(outcomes[2], Outcome::Refused(_)));
    assert_eq!(state.next_seq, Seq(4));
    assert_eq!(state.leader, None);
}

#[test]
fn a_multi_writer_journal_lets_anyone_truncate_and_refuses_a_leader() {
    let (state, outcomes) = fold_in(
        WriterMode::Multi,
        &[
            write(0, 0, &[b"a", b"b", b"c"]),
            truncate(0, 2),
            truncate(0, 1),
            set_leader(1, None),
            write(1, 3, &[b"d"]),
            truncate(1, 3),
        ],
    );
    assert!(matches!(outcomes[1], Outcome::Trimmed(v) if v.first_seq == Seq(2)));
    assert!(matches!(outcomes[2], Outcome::Trimmed(v) if v.first_seq == Seq(2)));
    assert!(matches!(outcomes[3], Outcome::WrongMode(v) if v.leader.is_none()));
    assert!(matches!(outcomes[4], Outcome::WrongMode(_)));
    assert!(matches!(outcomes[5], Outcome::WrongMode(_)));
    assert_eq!(state.first_seq, Seq(2));
    assert_eq!(state.next_seq, Seq(3));
    assert_eq!(state.term, 0);
}

#[test]
fn a_single_writer_journal_refuses_a_multi_writer_call() {
    let (state, outcomes) = fold(&[set_leader(1, None), write(0, 0, &[b"a"]), truncate(0, 0)]);
    assert!(matches!(outcomes[1], Outcome::WrongMode(v) if v.leader == Some(LeaderUuid(1))));
    assert!(matches!(outcomes[2], Outcome::WrongMode(_)));
    assert_eq!(state.next_seq, Seq(0));
}

#[test]
#[should_panic(expected = "a journal's first position never passes its next one")]
fn a_first_position_past_the_next_fails_the_invariants() {
    let state = JournalState {
        first_seq: Seq(3),
        next_seq: Seq(2),
        ..JournalState::default()
    };
    state.assert_invariants();
}

#[test]
#[should_panic(expected = "a multi-writer journal never gains a leader")]
fn a_multi_writer_journal_with_a_leader_fails_its_mode() {
    let state = JournalState {
        leader: Some(LeaderUuid(1)),
        term: 1,
        ..JournalState::default()
    };
    state.assert_mode(WriterMode::Multi, &Outcome::Noop);
}

fn roll(next: u64) -> Command {
    Command::Roll(Roll {
        next: SegmentId(next),
        replicas: None,
    })
}

#[test]
fn a_roll_to_the_next_segment_ends_the_tail_at_its_slot() {
    let (state, outcomes) = fold(&[set_leader(1, None), write(1, 0, &[b"a"]), roll(1)]);
    let tail = Segment {
        id: SegmentId(1),
        start: Slot(3),
    };
    assert_eq!(outcomes[2], Outcome::Rolled(tail));
    assert_eq!(state.segment, tail);
    // A Roll moves no position and no leader.
    assert_eq!(state.next_seq, Seq(1));
    assert_eq!(state.term, 1);
}

#[test]
fn a_roll_that_skips_or_repeats_a_segment_changes_nothing() {
    let (state, outcomes) = fold(&[roll(2), roll(0), roll(1), roll(1), roll(2)]);
    assert_eq!(outcomes[0], Outcome::RollIgnored(Segment::default()));
    assert_eq!(outcomes[1], Outcome::RollIgnored(Segment::default()));
    let first_roll = Segment {
        id: SegmentId(1),
        start: Slot(3),
    };
    assert_eq!(outcomes[2], Outcome::Rolled(first_roll));
    assert_eq!(outcomes[3], Outcome::RollIgnored(first_roll));
    assert_eq!(
        outcomes[4],
        Outcome::Rolled(Segment {
            id: SegmentId(2),
            start: Slot(5),
        })
    );
    assert_eq!(state.segment.id, SegmentId(2));
}

#[test]
fn a_roll_is_judged_alike_under_both_writer_modes_and_with_new_replicas() {
    let named = Command::Roll(Roll {
        next: SegmentId(1),
        replicas: Some(ReplicaSet::new([NodeId(4), NodeId(5)])),
    });
    for mode in [WriterMode::Single, WriterMode::Multi] {
        let (state, outcomes) = fold_in(mode, std::slice::from_ref(&named));
        assert!(matches!(outcomes[0], Outcome::Rolled(_)));
        assert_eq!(state.segment.start, Slot(1));
    }
}

/// A state from outside the core is judged by `is_well_formed`, one check
/// for each ordering that `assert_invariants` asserts.
#[test]
fn a_state_out_of_order_is_not_well_formed() {
    use crate::journal::segment::{Segment, SegmentId};
    let good = JournalState::default();
    assert!(good.is_well_formed());
    let tail = JournalState {
        segment: Segment {
            id: SegmentId(2),
            start: Slot(5),
        },
        ..good.clone()
    };
    assert!(tail.is_well_formed());
    let bad = [
        JournalState {
            first_seq: Seq(1),
            ..good.clone()
        },
        JournalState {
            term: 1,
            ..good.clone()
        },
        JournalState {
            leader: Some(LeaderUuid(1)),
            ..good.clone()
        },
        JournalState {
            leader: Some(LeaderUuid::UNSET),
            term: 1,
            ..good.clone()
        },
        JournalState {
            segment: Segment {
                id: SegmentId(1),
                start: Slot(0),
            },
            ..good.clone()
        },
        JournalState {
            segment: Segment {
                id: SegmentId(0),
                start: Slot(3),
            },
            ..good.clone()
        },
        JournalState {
            segment: Segment {
                id: SegmentId(4),
                start: Slot(3),
            },
            ..good.clone()
        },
    ];
    for state in bad {
        assert!(!state.is_well_formed(), "{state:?}");
    }
}

/// A valid `Roll` that names replicas gives them to the segment it opens; a
/// valid `Roll` with none keeps the set before it (D4); a `Roll` that is not
/// valid changes no set.
#[test]
fn a_roll_names_or_keeps_the_replicas_of_the_tail() {
    use crate::journal::segment::{ReplicaSet, Roll, SegmentId};
    let roll = |next: u64, replicas: Option<ReplicaSet>| {
        Command::Roll(Roll {
            next: SegmentId(next),
            replicas,
        })
    };
    let named = ReplicaSet::new([NodeId(3), NodeId(4)]);
    let other = ReplicaSet::new([NodeId(5)]);
    let mut state = JournalState::default();
    assert_eq!(state.replicas, None);
    let _ = state.apply(
        WriterMode::Multi,
        Slot(0),
        &roll(1, Some(named.clone())),
        |_| None,
    );
    assert_eq!(state.replicas.as_ref(), Some(&named));
    let _ = state.apply(WriterMode::Multi, Slot(1), &roll(2, None), |_| None);
    assert_eq!(
        state.replicas.as_ref(),
        Some(&named),
        "a Roll with no set keeps it"
    );
    let _ = state.apply(WriterMode::Multi, Slot(2), &roll(2, Some(other)), |_| None);
    assert_eq!(
        state.replicas.as_ref(),
        Some(&named),
        "an ignored Roll names nothing"
    );
    assert_eq!(state.segment.id, SegmentId(2));
}

fn hold(segment: u64, end: u64, next_seq: u64) -> Hold {
    Hold {
        segment: SegmentId(segment),
        end: Slot(end),
        next_seq: Seq(next_seq),
    }
}

/// A `Hold` extends the held prefix one segment at a time, only over a
/// sealed segment, and only with an end and a position inside the fold.
#[test]
fn a_hold_extends_the_held_prefix_by_one_sealed_segment() {
    let first = hold(0, 3, 1);
    let second = hold(1, 11, 2);
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        roll(1),
        Command::Hold(hold(1, 5, 2)),
        Command::Hold(hold(0, 4, 1)),
        Command::Hold(hold(0, 3, 2)),
        Command::Hold(hold(0, 0, 0)),
        Command::Hold(first),
        Command::Hold(first),
        write(1, 1, &[b"b"]),
        roll(2),
        Command::Hold(hold(1, 3, 2)),
        Command::Hold(hold(1, 11, 0)),
        Command::Hold(second),
        Command::Hold(hold(2, 12, 2)),
    ]);
    assert_eq!(
        outcomes[3],
        Outcome::HoldIgnored(None),
        "not the next segment"
    );
    assert_eq!(
        outcomes[4],
        Outcome::HoldIgnored(None),
        "an end past the tail"
    );
    assert_eq!(
        outcomes[5],
        Outcome::HoldIgnored(None),
        "a position past the fold"
    );
    assert_eq!(
        outcomes[6],
        Outcome::HoldIgnored(None),
        "an end before the Roll"
    );
    assert_eq!(outcomes[7], Outcome::HeldTo(first));
    assert_eq!(
        outcomes[8],
        Outcome::HoldIgnored(Some(first)),
        "held already"
    );
    assert_eq!(
        outcomes[11],
        Outcome::HoldIgnored(Some(first)),
        "an end at the held prefix's end"
    );
    assert_eq!(
        outcomes[12],
        Outcome::HoldIgnored(Some(first)),
        "a position before the held prefix"
    );
    assert_eq!(outcomes[13], Outcome::HeldTo(second));
    assert_eq!(
        outcomes[14],
        Outcome::HoldIgnored(Some(second)),
        "the tail is not sealed"
    );
    assert_eq!(state.held, Some(second));
    assert_eq!(state.next_seq, Seq(2), "a Hold moves no position");
}

/// A retry of a write inside the held prefix is `Held`, judged before the
/// log is asked: the acceptors may have dropped the write. A retry past it
/// is judged as before, and a write below `first_seq` is `Truncated`.
#[test]
fn a_retry_inside_the_held_prefix_is_held() {
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        write(1, 1, &[b"b"]),
        roll(1),
        Command::Hold(hold(0, 4, 2)),
        write(1, 2, &[b"c"]),
        write(1, 0, &[b"a"]),
        write(1, 1, &[b"x"]),
        write(1, 2, &[b"c"]),
        truncate(1, 1),
        write(1, 0, &[b"a"]),
        write(2, 1, &[b"b"]),
    ]);
    let view = state.view();
    assert_eq!(
        outcomes[6],
        Outcome::Held(super::JournalView {
            first_seq: Seq(0),
            ..view
        })
    );
    assert_eq!(
        outcomes[7],
        Outcome::Held(super::JournalView {
            first_seq: Seq(0),
            ..view
        }),
        "inside the held prefix, the bytes are not read"
    );
    assert_eq!(
        outcomes[8],
        Outcome::Duplicate {
            seq: Seq(2),
            count: 1
        }
    );
    assert!(matches!(outcomes[10], Outcome::Truncated(_)));
    assert!(
        matches!(outcomes[11], Outcome::Held(_)),
        "a stale uuid inside the held prefix is Held too: no fence verdict"
    );
    // The fold never asks the log for a position inside the held prefix.
    let asked = std::cell::Cell::new(false);
    let mut held = state.clone();
    let _ = held.apply(WriterMode::Single, Slot(11), &write(1, 1, &[b"b"]), |_| {
        asked.set(true);
        None
    });
    assert!(!asked.get(), "a held write is not looked up");
}

/// A `Hold` of the tail is ignored even when its end lies inside the fold:
/// only a sealed segment is held.
#[test]
fn a_hold_of_the_tail_is_ignored() {
    let first = hold(0, 2, 1);
    let (state, outcomes) = fold(&[
        set_leader(1, None),
        write(1, 0, &[b"a"]),
        roll(1),
        Command::Hold(first),
        Command::Hold(hold(1, 3, 1)),
    ]);
    assert_eq!(
        outcomes[3],
        Outcome::HeldTo(first),
        "an end inside the fold"
    );
    assert_eq!(outcomes[4], Outcome::HoldIgnored(Some(first)), "the tail");
    assert_eq!(state.held, Some(first));
}

/// A held prefix that fits the state at each bound is well formed, and one
/// past any bound is not.
#[test]
fn a_held_prefix_out_of_the_state_is_not_well_formed() {
    let base = JournalState {
        segment: Segment {
            id: SegmentId(2),
            start: Slot(5),
        },
        next_seq: Seq(1),
        ..JournalState::default()
    };
    let with = |held: Hold| JournalState {
        held: Some(held),
        ..base.clone()
    };
    assert!(with(hold(1, 5, 1)).is_well_formed(), "every bound reached");
    for held in [hold(2, 5, 1), hold(1, 6, 1), hold(1, 1, 1), hold(1, 5, 2)] {
        assert!(!with(held).is_well_formed(), "{held:?}");
    }
}

/// The postconditions of `apply` hold against a state they did not come
/// from: a term that went back is a bug.
#[test]
#[should_panic(expected = "a journal's term never decreases")]
fn a_term_that_goes_back_is_a_bug() {
    let before = JournalState {
        term: 1,
        leader: Some(LeaderUuid(1)),
        ..JournalState::default()
    };
    JournalState::default().assert_transition(&before, Slot(0), &Outcome::Noop);
}

/// A held prefix that moved with no valid `Hold` is a bug.
#[test]
#[should_panic(expected = "only a valid Hold moves the held prefix")]
fn a_held_prefix_moved_by_no_hold_is_a_bug() {
    let after = JournalState {
        held: Some(hold(0, 1, 0)),
        ..JournalState::default()
    };
    after.assert_held(&JournalState::default(), &Outcome::Noop);
}

/// A held prefix that does not fit the state is a bug.
#[test]
#[should_panic(expected = "a held segment is sealed")]
fn a_held_tail_is_a_bug() {
    let state = JournalState {
        held: Some(hold(0, 1, 0)),
        ..JournalState::default()
    };
    state.assert_invariants();
}

/// The tail segment's postconditions hold against a state they did not
/// come from: a tail that went back is a bug.
#[test]
#[should_panic(expected = "a journal's tail segment never goes back")]
fn a_tail_segment_that_goes_back_is_a_bug() {
    let before = JournalState {
        segment: Segment::default().rolled_at(Slot(2)),
        ..JournalState::default()
    };
    JournalState::default().assert_segment(&before, Slot(3), &Outcome::Noop);
}

/// A `Roll` that reports another tail than the one after it is a bug.
#[test]
#[should_panic(expected = "a Roll reports the tail after it")]
fn a_roll_that_reports_another_tail_is_a_bug() {
    let after = JournalState {
        segment: Segment::default().rolled_at(Slot(2)),
        ..JournalState::default()
    };
    let other = Segment::default().rolled_at(Slot(4));
    after.assert_segment(&JournalState::default(), Slot(2), &Outcome::Rolled(other));
}

/// An ignored `Roll` that names another tail than the one it was judged
/// against is a bug.
#[test]
#[should_panic(expected = "an ignored Roll names the tail it was judged against")]
fn an_ignored_roll_that_names_another_tail_is_a_bug() {
    let other = Segment::default().rolled_at(Slot(4));
    JournalState::default().assert_segment(
        &JournalState::default(),
        Slot(5),
        &Outcome::RollIgnored(other),
    );
}
