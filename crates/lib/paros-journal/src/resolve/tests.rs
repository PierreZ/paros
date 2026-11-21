use std::collections::BTreeMap;

use super::*;
use crate::command::{Command, Leader};
use crate::fold::{Answer, BoundaryState};

fn rec(byte: u8) -> Vec<Record> {
    vec![Record::new(vec![byte])]
}

/// A sealed segment that starts at position 2 and holds the writes of uuid
/// 1 at 2 and 3, then a truncate of the floor to 3.
fn sealed() -> (JournalState, BTreeMap<Seq, Write>) {
    let boundary = BoundaryState {
        next_seq: Seq(2),
        first_seq: Seq(0),
        leader: Leader::uuid(LeaderUuid(1)),
    };
    let mut state = JournalState::from_boundary(boundary);
    let mut log = BTreeMap::new();
    for (seq, byte) in [(2, 10), (3, 11)] {
        let write = Write {
            fence: Fence::Leader {
                uuid: LeaderUuid(1),
                expected_seq: Seq(seq),
            },
            records: rec(byte),
        };
        let answer = state.apply(&Command::Write(write.clone()), |at| log.get(&at));
        assert!(matches!(answer, Answer::Written { .. }));
        log.insert(Seq(seq), write);
    }
    (state, log)
}

#[test]
fn the_same_write_is_written() {
    let (state, log) = sealed();
    let answer = resolve(&state, LeaderUuid(1), Seq(3), &rec(11), |at| log.get(&at));
    assert_eq!(
        answer,
        ResolveAnswer::Written {
            seq: Seq(3),
            count: 1
        }
    );
}

#[test]
fn another_write_at_the_position_is_not_written() {
    let (state, log) = sealed();
    assert_eq!(
        resolve(&state, LeaderUuid(1), Seq(3), &rec(99), |at| log.get(&at)),
        ResolveAnswer::NotWritten,
        "other bytes"
    );
    // The control `resolve_uuid` of journal-state/: a compare of the uuid
    // only would call this written.
    assert_eq!(
        resolve(&state, LeaderUuid(2), Seq(3), &rec(11), |at| log.get(&at)),
        ResolveAnswer::NotWritten,
        "another uuid"
    );
}

#[test]
fn a_replica_answers_only_where_its_fold_covers() {
    let (state, log) = sealed();
    // The control `resolve_early`: at or above next_seq the replica has
    // not applied the position.
    assert_eq!(
        resolve(&state, LeaderUuid(1), Seq(4), &rec(11), |at| log.get(&at)),
        ResolveAnswer::NotCovered
    );
    // Below the start, the position is in another segment.
    assert_eq!(
        resolve(&state, LeaderUuid(1), Seq(1), &rec(11), |at| log.get(&at)),
        ResolveAnswer::NotCovered
    );
}

#[test]
fn below_the_floor_the_fate_is_unknown() {
    let (mut state, log) = sealed();
    let answer = state.apply(
        &Command::Truncate {
            fence: Some(LeaderUuid(1)),
            up_to: Seq(3),
        },
        |at| log.get(&at),
    );
    assert!(matches!(answer, Answer::Trimmed(_)));
    assert_eq!(
        resolve(&state, LeaderUuid(1), Seq(2), &rec(10), |at| log.get(&at)),
        ResolveAnswer::Truncated
    );
    assert_eq!(
        resolve(&state, LeaderUuid(1), Seq(3), &rec(11), |at| log.get(&at)),
        ResolveAnswer::Written {
            seq: Seq(3),
            count: 1
        }
    );
}

#[test]
fn a_resolve_changes_nothing() {
    let (state, log) = sealed();
    let before = state;
    let _ = resolve(&state, LeaderUuid(1), Seq(2), &rec(10), |at| log.get(&at));
    assert_eq!(state, before);
}
