use super::*;
use paros_consensus::contract::limits::PROMISE_BATCH;
use paros_consensus::membership::{AcceptorConfig, QuorumSystem};

use crate::segment::SegmentId;

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(101),
    }
}

fn segment(id: u64, members: &[u64], start: Option<u64>, end: Option<u64>) -> ReadSegment {
    ReadSegment {
        id: SegmentId(id),
        columns: vec![AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        )],
        replicas: Vec::new(),
        start: start.map(Slot),
        end: end.map(Slot),
    }
}

fn page(votes: &[(u64, u64, u8)], faulty: &[(u64, u64)], next: Option<u64>) -> PromisePage {
    PromisePage {
        votes: votes
            .iter()
            .map(|(slot, round, byte)| (Slot(*slot), (ballot(*round), noop_or(*byte))))
            .collect(),
        faulty: faulty.iter().map(|(s, r)| (Slot(*s), ballot(*r))).collect(),
        next: next.map(Slot),
        dropped: BTreeMap::new(),
    }
}

/// A distinct command for each byte: a `Noop` for 0, else a truncate.
fn noop_or(byte: u8) -> Command {
    if byte == 0 {
        Command::Noop
    } else {
        Command::Truncate {
            fence: None,
            up_to: paros_consensus::journal::Seq(u64::from(byte)),
        }
    }
}

#[test]
fn a_page_at_another_cursor_or_from_a_stranger_is_not_folded() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(2), Slot(0), &segments);
    assert_eq!(phase1.ballot(), ballot(2));
    assert_eq!(
        phase1.fold(NodeId(9), Slot(0), page(&[], &[], None)),
        PromiseFold::Ignored
    );
    assert_eq!(
        phase1.fold(NodeId(1), Slot(4), page(&[], &[], None)),
        PromiseFold::Ignored
    );
    assert_eq!(phase1.wanted().len(), 3);
    assert_eq!(
        phase1.fold(NodeId(1), Slot(0), page(&[(0, 1, 1)], &[], Some(4))),
        PromiseFold::Continue(Slot(4))
    );
    assert!(phase1.wanted().contains(&(NodeId(1), Slot(4))));
    assert_eq!(
        phase1.fold(NodeId(1), Slot(4), page(&[], &[], None)),
        PromiseFold::Answered
    );
    assert!(!phase1.wanted().iter().any(|(node, _)| *node == NodeId(1)));
    // A page after the last one is not folded.
    assert_eq!(
        phase1.fold(NodeId(1), Slot(4), page(&[(5, 1, 1)], &[], None)),
        PromiseFold::Ignored
    );
    phase1.fold(NodeId(2), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.safe,
        BTreeMap::from([(Slot(0), noop_or(1))]),
        "the late page is not a vote"
    );
}

/// An acceptor's votes and faulty entries are at different slots (the
/// core's acceptor keeps them apart), so the faulty vote that each slot
/// meets is another acceptor's.
#[test]
fn the_highest_vote_wins_and_a_higher_faulty_vote_blocks_the_slot() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(
        NodeId(1),
        Slot(0),
        page(&[(0, 1, 1), (1, 3, 1)], &[(2, 1)], None),
    );
    assert!(
        phase1.close(&segments).is_none(),
        "one answer is not a quorum"
    );
    phase1.fold(
        NodeId(2),
        Slot(0),
        page(&[(0, 2, 2), (2, 2, 2)], &[(1, 4)], None),
    );
    let close = phase1.close(&segments).expect("a quorum");
    // Slot 0: ballot 2 wins over 1. Slot 1: the faulty ballot 4 is above the
    // readable 3, so the slot is blocked. Slot 2: the readable 2 is above the
    // faulty 1.
    assert_eq!(
        close.safe,
        BTreeMap::from([(Slot(0), noop_or(2)), (Slot(2), noop_or(2))])
    );
    assert_eq!(close.blocked, vec![Slot(1)]);
    assert_eq!(close.end, None, "a segment in force is not sealed");
}

#[test]
fn a_faulty_vote_at_the_top_ballot_does_not_block_a_readable_one() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(0, 3, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(0, 3)], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(close.safe, BTreeMap::from([(Slot(0), noop_or(1))]));
    assert_eq!(close.blocked.len(), 0);
}

#[test]
fn an_acceptor_of_two_grids_is_read_once_at_every_slot() {
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[1, 2, 3], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    // One acceptor of both grids: its votes at 2 and at 4 are both read,
    // whatever the end.
    phase1.fold(NodeId(1), Slot(0), page(&[(2, 1, 1), (4, 1, 2)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.safe.keys().copied().collect::<Vec<_>>(),
        vec![Slot(2), Slot(4)]
    );
}

#[test]
fn an_old_grid_is_read_at_every_slot_and_the_higher_ballot_wins() {
    // A leader of segment 2 writes the slots below its start in its own
    // grid (the gap fill), so a later leader reads the new grid below the
    // start too. Phase 1 reads the old grid at every slot, its end too: at
    // slot 3 the vote of the new grid has the higher ballot and wins; at
    // slot 4 the old vote is the only one and is proposed again.
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[4, 5, 6], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    for node in [1, 2, 4, 5] {
        let votes: &[(u64, u64, u8)] = match node {
            1 => &[(3, 1, 1), (4, 1, 1)],
            4 => &[(1, 2, 2), (3, 2, 3)],
            _ => &[],
        };
        phase1.fold(NodeId(node), Slot(0), page(votes, &[], None));
    }
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.safe,
        BTreeMap::from([
            (Slot(1), noop_or(2)),
            (Slot(3), noop_or(3)),
            (Slot(4), noop_or(1))
        ])
    );
}

#[test]
fn the_seal_end_is_one_above_the_highest_old_vote() {
    let segments = [
        segment(1, &[1, 2, 3], Some(2), None),
        segment(2, &[4, 5, 6], None, None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(6, 1, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(8, 1)], None));
    phase1.fold(NodeId(4), Slot(0), page(&[(20, 1, 1)], &[], None));
    assert!(
        phase1.close(&segments).is_none(),
        "the new grid has no quorum yet"
    );
    phase1.fold(NodeId(5), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum of each grid");
    assert_eq!(
        close.end,
        Some(Slot(9)),
        "the faulty vote counts; a new-grid vote does not"
    );
}

#[test]
fn the_seal_end_is_at_least_the_start_of_the_segment_before() {
    let segments = [
        segment(1, &[1, 2, 3], Some(6), None),
        segment(2, &[4, 5, 6], None, None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    for node in [1, 2, 4, 5] {
        phase1.fold(NodeId(node), Slot(0), page(&[], &[], None));
    }
    assert_eq!(
        phase1.close(&segments).expect("a quorum").end,
        Some(Slot(6))
    );
}

#[test]
fn no_end_when_the_segment_before_is_sealed() {
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(4)),
        segment(2, &[4, 5, 6], None, None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    for node in [1, 2, 4, 5] {
        phase1.fold(NodeId(node), Slot(0), page(&[], &[], None));
    }
    assert_eq!(phase1.close(&segments).expect("a quorum").end, None);
}

/// A page whose shape breaks the paging rule is a page that a peer sends:
/// Phase 1 ignores it, and asks the acceptor again from the same cursor.
#[test]
fn a_page_of_the_wrong_shape_is_ignored_and_asked_again_from_its_cursor() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(3), &segments);
    let at_three = |phase1: &Phase1| phase1.wanted().contains(&(NodeId(1), Slot(3)));
    let bad = [
        (
            "a cursor that does not move",
            page(&[(3, 1, 1)], &[], Some(3)),
        ),
        ("a continuation with no entry", page(&[], &[], Some(4))),
        ("a slot below the cursor", page(&[(2, 1, 1)], &[], None)),
        ("a slot in both maps", page(&[(3, 1, 1)], &[(3, 2)], None)),
        (
            "a dropped range with no slot",
            PromisePage {
                dropped: BTreeMap::from([(Slot(4), Slot(4))]),
                ..page(&[], &[], None)
            },
        ),
        (
            "more than a page of entries",
            PromisePage {
                votes: (0..=PROMISE_BATCH as u64)
                    .map(|s| (Slot(3 + s), (ballot(1), noop_or(1))))
                    .collect(),
                ..page(&[], &[], None)
            },
        ),
    ];
    for (why, bad) in bad {
        assert_eq!(
            phase1.fold(NodeId(1), Slot(3), bad),
            PromiseFold::Ignored,
            "{why}"
        );
        assert!(at_three(&phase1), "{why}: asked again from the same cursor");
        assert!(phase1.complete().is_empty(), "{why}: no answer counts");
    }
    assert_eq!(
        phase1.fold(NodeId(1), Slot(3), page(&[(3, 1, 1)], &[], None)),
        PromiseFold::Answered,
        "a good page at the same cursor folds"
    );
}

#[test]
fn a_phase_one_with_no_segment_asks_nobody_and_never_closes() {
    // The Phase 1 of a leader whose bind runs.
    let phase1 = Phase1::new(ballot(5), Slot(0), &[]);
    assert!(phase1.wanted().is_empty(), "no segment, no acceptor to ask");
    assert_eq!(phase1.close(&[]), None);
}

#[test]
fn the_highest_faulty_ballot_of_the_quorum_blocks() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(1, 3, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(1, 4)], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.blocked,
        vec![Slot(1)],
        "the faulty ballot 4 is above the vote at 3"
    );
    assert_eq!(close.safe, BTreeMap::new());
    assert_eq!(
        phase1.decide(&segments, Slot(1)),
        None,
        "one answer qualifies"
    );
    assert_eq!(phase1.faulty_at(Slot(1)), vec![NodeId(2)]);
    // A late answer with a faulty vote at or below the threshold qualifies:
    // the tally decides the readable vote.
    phase1.fold(NodeId(3), Slot(0), page(&[], &[(1, 3)], None));
    assert_eq!(phase1.decide(&segments, Slot(1)), Some(noop_or(1)));
    assert_eq!(phase1.faulty_at(Slot(1)), vec![NodeId(2), NodeId(3)]);
}

#[test]
fn the_tally_decides_a_faulty_top_when_a_quorum_qualifies_without_it() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(1, 3, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(1, 4), (2, 2)], None));
    phase1.fold(NodeId(3), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(close.blocked, Vec::<Slot>::new(), "nodes 1 and 3 qualify");
    assert_eq!(
        close.safe,
        BTreeMap::from([(Slot(1), noop_or(1))]),
        "Case 1: the readable vote at the threshold"
    );
    // Case 2: no readable vote, so every faulty vote is above the
    // threshold; nodes 1 and 3 still hold a quorum, and the slot takes a
    // `Noop` (no entry in `safe`).
    assert_eq!(phase1.decide(&segments, Slot(2)), Some(Command::Noop));
    assert!(!close.safe.contains_key(&Slot(2)));
}

#[test]
fn a_faulty_entry_above_the_seal_end_goes_to_the_tally() {
    // Phase 1 reads every slot of a sealed grid: a faulty entry of the old
    // grid at or above its end is a report like any other. It blocks its
    // slot until the qualifying answers hold a quorum of every column of
    // every segment read, as the core's tally asks.
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[4, 5, 6], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[], &[(3, 4), (5, 4)], None));
    for node in [2, 4, 5] {
        phase1.fold(NodeId(node), Slot(0), page(&[], &[], None));
    }
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(close.blocked, vec![Slot(3), Slot(5)]);
    assert_eq!(phase1.faulty_at(Slot(3)), vec![NodeId(1)]);
    assert_eq!(phase1.decide(&segments, Slot(3)), None);
    // A late answer of the old grid decides both slots: no readable vote,
    // so a `Noop`.
    phase1.fold(NodeId(3), Slot(0), page(&[], &[], None));
    assert_eq!(phase1.decide(&segments, Slot(3)), Some(Command::Noop));
    assert_eq!(phase1.decide(&segments, Slot(5)), Some(Command::Noop));
}

#[test]
fn a_vote_above_the_seal_end_is_proposed_again() {
    // A vote of the sealed grid at or above its end is not hidden: with no
    // vote of the next grid there, it is safe to propose it again
    // (`end_applied_safe` of `specs/tla/journal-read/`).
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[4, 5, 6], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(4, 1, 7)], &[], None));
    for node in [2, 4, 5] {
        phase1.fold(NodeId(node), Slot(0), page(&[], &[], None));
    }
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(close.safe.get(&Slot(4)), Some(&noop_or(7)));
}

/// `page` with the ranges that the acceptor dropped.
fn page_dropped(dropped: &[(u64, u64)], next: Option<u64>) -> PromisePage {
    PromisePage {
        dropped: dropped.iter().map(|(s, b)| (Slot(*s), Slot(*b))).collect(),
        ..page(&[], &[], next)
    }
}

#[test]
fn a_vote_of_an_answer_that_never_completes_is_neither_proposed_nor_a_threshold() {
    // Phase 1 merges complete answers only (`Phase1On` of
    // `specs/tla/journal-ctrl/`): a higher vote on the first page of an
    // acceptor that never sends its last page does not count. Here it would
    // raise the threshold of the tally to 9 and let node 3 qualify.
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(10), Slot(0), &segments);
    assert_eq!(
        phase1.fold(NodeId(1), Slot(0), page(&[(0, 9, 3)], &[], Some(1))),
        PromiseFold::Continue(Slot(1))
    );
    phase1.fold(NodeId(2), Slot(0), page(&[(0, 2, 1)], &[], None));
    phase1.fold(NodeId(3), Slot(0), page(&[], &[(0, 5)], None));
    let close = phase1
        .close(&segments)
        .expect("a quorum of complete answers");
    assert_eq!(
        close.safe,
        BTreeMap::new(),
        "the partial vote is not proposed"
    );
    assert_eq!(close.blocked, vec![Slot(0)]);
    assert_eq!(phase1.qualifying(Slot(0)), BTreeSet::from([NodeId(2)]));
    assert_eq!(phase1.decide(&segments, Slot(0)), None);
}

#[test]
fn the_seal_end_counts_only_the_segment_before() {
    // The end of the segment in force counts the votes, the faulty votes
    // and the dropped ranges of its own acceptors, never a vote that only
    // an acceptor of an older unretired grid holds.
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(2)),
        segment(2, &[4, 5, 6], Some(2), None),
        segment(3, &[7, 8, 9], None, None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(10, 1, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[], None));
    phase1.fold(NodeId(4), Slot(0), page(&[(3, 1, 1)], &[(5, 1)], None));
    phase1.fold(NodeId(5), Slot(0), page_dropped(&[(2, 7)], None));
    phase1.fold(NodeId(7), Slot(0), page(&[], &[], None));
    phase1.fold(NodeId(8), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum of every grid");
    assert_eq!(
        close.end,
        Some(Slot(7)),
        "the dropped range 2..7 is the highest fact of the segment before"
    );
    // A vote, then a faulty vote, above the range raise the end.
    for (votes, faulty, end) in [(&[(8, 1, 1)][..], &[][..], 9), (&[][..], &[(9, 1)][..], 10)] {
        let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
        for node in [1, 2, 7, 8] {
            phase1.fold(NodeId(node), Slot(0), page(&[], &[], None));
        }
        phase1.fold(NodeId(4), Slot(0), page(votes, faulty, None));
        phase1.fold(NodeId(5), Slot(0), page_dropped(&[(2, 7)], None));
        assert_eq!(
            phase1.close(&segments).expect("a quorum").end,
            Some(Slot(end))
        );
    }
}

#[test]
fn two_segments_that_share_a_column_close_on_a_quorum_of_it() {
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[1, 2, 3], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[], &[], None));
    assert!(phase1.close(&segments).is_none(), "one of three");
    phase1.fold(NodeId(3), Slot(0), page(&[], &[], None));
    assert!(
        phase1.close(&segments).is_some(),
        "F + 1 of the shared column"
    );
}

#[test]
fn a_late_answer_that_dropped_a_blocked_slot_counts_once_complete() {
    // The leader skips a blocked slot that a complete answer dropped
    // (`dropped_by`), before the tally: the other answers may predate the
    // compaction (control `ctrl_probe_drop`). A partial answer changes
    // nothing yet.
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(1, 3, 1)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(1, 4)], None));
    assert_eq!(
        phase1.close(&segments).expect("a quorum").blocked,
        vec![Slot(1)]
    );
    // A continuation page is full (the core's page rule): it holds the
    // vote at slot 0, below the dropped range's last slot.
    phase1.fold(
        NodeId(3),
        Slot(0),
        PromisePage {
            votes: BTreeMap::from([(Slot(0), (ballot(1), noop_or(1)))]),
            ..page_dropped(&[(0, 2)], Some(2))
        },
    );
    assert_eq!(phase1.dropped_by(Slot(1)), None, "a partial answer");
    phase1.fold(NodeId(3), Slot(2), page(&[], &[], None));
    assert_eq!(phase1.dropped_by(Slot(1)), Some(NodeId(3)));
    assert_eq!(phase1.dropped_by(Slot(2)), None);
    // The tally alone would decide the slot now (nodes 1 and 3 qualify):
    // the leader must skip it first.
    assert_eq!(phase1.decide(&segments, Slot(1)), Some(noop_or(1)));
}
