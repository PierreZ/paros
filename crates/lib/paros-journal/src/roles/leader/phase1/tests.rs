use super::*;
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
    assert_eq!(phase1.fold(NodeId(9), Slot(0), page(&[], &[], None)), None);
    assert_eq!(phase1.fold(NodeId(1), Slot(4), page(&[], &[], None)), None);
    assert_eq!(phase1.wanted().len(), 3);
    assert_eq!(
        phase1.fold(NodeId(1), Slot(0), page(&[], &[], Some(4))),
        Some(Slot(4))
    );
    assert!(phase1.wanted().contains(&(NodeId(1), Slot(4))));
    assert_eq!(phase1.fold(NodeId(1), Slot(4), page(&[], &[], None)), None);
    assert!(!phase1.wanted().iter().any(|(node, _)| *node == NodeId(1)));
    // A page after the last one is not folded.
    assert_eq!(
        phase1.fold(NodeId(1), Slot(4), page(&[(5, 1, 1)], &[], None)),
        None
    );
    phase1.fold(NodeId(2), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert!(close.safe.is_empty(), "the late page is not a vote");
}

#[test]
fn the_highest_vote_wins_and_a_higher_faulty_vote_blocks_the_slot() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(
        NodeId(1),
        Slot(0),
        page(&[(0, 1, 1), (1, 3, 1), (2, 2, 2)], &[(2, 1)], None),
    );
    assert!(
        phase1.close(&segments).is_none(),
        "one answer is not a quorum"
    );
    phase1.fold(
        NodeId(2),
        Slot(0),
        page(&[(0, 2, 2), (1, 2, 2)], &[(1, 4)], None),
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
fn a_sealed_segment_is_read_below_its_end_only() {
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[1, 2, 3], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    // One acceptor of both grids: its vote at 2 is read in segment 1 and
    // its vote at 4 in segment 2.
    phase1.fold(NodeId(1), Slot(0), page(&[(2, 1, 1), (4, 1, 2)], &[], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.safe.keys().copied().collect::<Vec<_>>(),
        vec![Slot(2), Slot(4)]
    );
}

#[test]
fn an_old_grid_is_read_below_its_end_and_the_new_grid_everywhere() {
    // A leader of segment 2 writes the slots below its start in its own
    // grid (the gap fill), so a later leader reads the new grid below the
    // start too; the old grid holds nothing of segment 1 at or above its end.
    let segments = [
        segment(1, &[1, 2, 3], Some(0), Some(3)),
        segment(2, &[4, 5, 6], Some(3), None),
    ];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    for node in [1, 2, 4, 5] {
        let votes: &[(u64, u64, u8)] = match node {
            // A stray vote of the old grid at the end itself, at a higher
            // ballot, is not a vote of segment 1 either.
            1 => &[(3, 9, 1), (4, 1, 1)],
            4 => &[(1, 2, 2), (3, 2, 3)],
            _ => &[],
        };
        phase1.fold(NodeId(node), Slot(0), page(votes, &[], None));
    }
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.safe,
        BTreeMap::from([(Slot(1), noop_or(2)), (Slot(3), noop_or(3))])
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

#[test]
#[should_panic(expected = "starts at its cursor")]
fn a_page_with_a_slot_below_its_start_is_refused() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(3), &segments);
    phase1.fold(NodeId(1), Slot(3), page(&[(1, 1, 1)], &[], None));
}

#[test]
#[should_panic(expected = "moves forward")]
fn a_cursor_that_does_not_move_is_refused() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(3), &segments);
    phase1.fold(NodeId(1), Slot(3), page(&[], &[], Some(3)));
}

#[test]
#[should_panic(expected = "at least one segment")]
fn phase_one_reads_a_segment() {
    let _ = Phase1::new(ballot(5), Slot(0), &[]);
}

#[test]
fn the_highest_faulty_ballot_of_the_quorum_blocks() {
    let segments = [segment(1, &[1, 2, 3], Some(0), None)];
    let mut phase1 = Phase1::new(ballot(5), Slot(0), &segments);
    phase1.fold(NodeId(1), Slot(0), page(&[(1, 3, 1)], &[(1, 2)], None));
    phase1.fold(NodeId(2), Slot(0), page(&[], &[(1, 4)], None));
    phase1.fold(NodeId(3), Slot(0), page(&[], &[(1, 1)], None));
    let close = phase1.close(&segments).expect("a quorum");
    assert_eq!(
        close.blocked,
        vec![Slot(1)],
        "the faulty ballot 4 is above the vote at 3"
    );
    assert_eq!(close.safe, BTreeMap::new());
}

#[test]
fn a_faulty_entry_the_segment_does_not_read_blocks_nothing() {
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
    assert_eq!(close.blocked, Vec::<Slot>::new());
}
