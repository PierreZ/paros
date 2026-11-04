//! A cold read's two arms, pinned on their own (`ColdRead`): where a page
//! leaves the read, and where a truncation past it sends it.

use paros_core::journal::{JournalView, LeaderUuid, Seq};

use super::{ColdRead, ColdTurn, FLOOR_RACE_WAIT_MS, TAIL_CYCLES, TAIL_WAITS};
use crate::election::{Candidate, ElectionRecord};

fn view(next: u64, first: u64) -> JournalView {
    JournalView {
        leader: None,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

fn renew(term: u64) -> Vec<u8> {
    ElectionRecord::Renew {
        term,
        candidate: Candidate {
            id: 1,
            ..Candidate::default()
        },
        uuid: LeaderUuid(9),
    }
    .encode()
}

fn turn(turn: &ColdTurn) -> &'static str {
    match turn {
        ColdTurn::Done => "done",
        ColdTurn::Failed => "failed",
        ColdTurn::Wait => "wait",
        ColdTurn::Next => "next",
    }
}

#[test]
fn a_page_short_of_the_tail_reads_on_unless_it_is_empty() {
    let mut read = ColdRead::default();
    assert_eq!(turn(&read.page(0, &[renew(1)], view(3, 0))), "next");
    assert_eq!(read.target, Some(3), "the first page fixes the tail");
    assert_eq!(turn(&read.page(1, &[], view(5, 0))), "failed");
    assert_eq!(read.target, Some(3), "a later page moves no tail");
}

#[test]
fn a_known_fold_at_the_tail_is_done() {
    let mut read = ColdRead::default();
    assert_eq!(turn(&read.page(0, &[], view(0, 0))), "done");
    assert!(read.fold.is_known());
}

#[test]
fn an_unknown_fold_at_the_tail_waits_then_goes_back_to_the_floor() {
    let mut read = ColdRead::default();
    // The floor passed the read: it resumes at 4 knowing nothing.
    assert_eq!(read.truncated(0, view(6, 4), None), 0);
    assert!(read.chased);
    assert!(!read.fold.is_known());
    // At the tail, holding records the floor has not reached.
    for _ in 0..TAIL_WAITS {
        assert_eq!(turn(&read.page(4, &[], view(4, 4))), "wait");
    }
    assert_eq!(read.waits, TAIL_WAITS);
    let past = turn(&read.page(4, &[], view(4, 4)));
    assert!(past == "done" || past == "next", "{past}");
}

#[test]
fn a_floor_that_outruns_the_read_twice_sends_it_ahead_of_the_tail() {
    let mut read = ColdRead::default();
    assert_eq!(read.truncated(0, view(10, 4), None), 0, "the first: a gap");
    assert_eq!(read.fold.next(), 4);
    assert_eq!(read.cycles, 0);
    read.waits = 1;
    // The second: the floor jumped 3 past the read, the tail grew 2.
    assert_eq!(read.truncated(4, view(12, 7), Some(2)), FLOOR_RACE_WAIT_MS);
    assert_eq!(read.cycles, 1);
    assert_eq!(read.waits, 0, "a new cycle waits afresh");
    assert_eq!(read.fold.next(), 14, "the tail plus the lead, min(3, 2)");
    // No growth known: the lead is the whole jump.
    let mut cold = ColdRead {
        chased: true,
        ..ColdRead::default()
    };
    assert_eq!(cold.truncated(0, view(12, 5), None), FLOOR_RACE_WAIT_MS);
    assert_eq!(cold.fold.next(), 17);
    // A tail that did not grow still leads by one.
    let mut still = ColdRead {
        chased: true,
        ..ColdRead::default()
    };
    still.truncated(0, view(12, 5), Some(0));
    assert_eq!(still.fold.next(), 13);
}

#[test]
fn a_read_back_at_the_floor_stops_after_its_cycles() {
    let mut read = ColdRead {
        cycles: TAIL_CYCLES,
        waits: TAIL_WAITS,
        ..ColdRead::default()
    };
    read.truncated(0, view(6, 4), None);
    read.page(4, &[renew(1)], view(6, 4));
    assert_eq!(
        turn(&read.page(5, &[renew(1)], view(6, 4))),
        "done",
        "no cycle left: the fold as it stands"
    );
}
