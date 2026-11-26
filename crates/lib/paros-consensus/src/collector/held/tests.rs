use super::{HeldTally, SealedSegment};
use crate::journal::{ReplicaSet, SegmentId, Seq};
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::types::{NodeId, Slot};

const SEG: SegmentId = SegmentId(0);

fn three() -> AcceptorConfig {
    AcceptorConfig::new((1..=3).map(NodeId).collect(), QuorumSystem::Majority)
}

fn sealed(replicas: Option<&[u64]>) -> SealedSegment {
    SealedSegment {
        start: Slot(0),
        end: Slot(5),
        next_seq: Seq(1),
        replicas: replicas.map(|r| ReplicaSet::new(r.iter().copied().map(NodeId))),
    }
}

#[test]
fn a_segment_is_held_by_f_plus_one_replicas_with_its_end_in_the_chain() {
    let mut tally = HeldTally::default();
    assert!(!tally.report(NodeId(7), SEG, Slot(5)), "not noted yet");
    tally.note(SEG, sealed(Some(&[7, 8, 9])));
    tally.note(SEG, sealed(Some(&[7, 8, 9])));
    assert_eq!(tally.sealed(SEG), Some(&sealed(Some(&[7, 8, 9]))));
    assert!(tally.report(NodeId(7), SEG, Slot(5)));
    assert!(!tally.judge(SEG, &three(), true, Seq(0)), "F = 1: one copy");
    assert!(tally.report(NodeId(8), SEG, Slot(5)));
    assert_eq!(tally.reporters(SEG), 2);
    assert!(
        !tally.judge(SEG, &three(), false, Seq(0)),
        "the end is not in the chain"
    );
    assert!(!tally.is_held(SEG));
    assert!(tally.judge(SEG, &three(), true, Seq(0)));
    assert!(tally.is_held(SEG));
    assert_eq!(tally.held().collect::<Vec<_>>(), vec![SEG]);
    assert_eq!(tally.reporters(SEG), 0, "a held segment keeps no report");
    assert!(!tally.judge(SEG, &three(), true, Seq(0)), "marked once");
    assert!(tally.is_held(SEG));
}

#[test]
fn a_report_counts_only_from_a_replica_of_the_fold_with_its_end() {
    let mut tally = HeldTally::default();
    tally.note(SEG, sealed(Some(&[7, 8, 9])));
    tally.note(SegmentId(1), sealed(None));
    assert!(!tally.report(NodeId(1), SEG, Slot(5)), "not a replica");
    assert!(!tally.report(NodeId(7), SEG, Slot(4)), "another end");
    assert!(
        !tally.report(NodeId(7), SegmentId(1), Slot(5)),
        "no replicas"
    );
    assert!(!tally.judge(SegmentId(1), &three(), true, Seq(0)));
    assert!(tally.report(NodeId(7), SEG, Slot(5)));
    assert!(
        tally.report(NodeId(7), SEG, Slot(5)),
        "a repeat counts once"
    );
    assert_eq!(tally.reporters(SEG), 1);
    assert!(!tally.judge(SEG, &three(), true, Seq(0)));
    assert!(
        !tally.judge(SegmentId(2), &three(), true, Seq(0)),
        "not noted"
    );
    assert!(!tally.is_held(SEG));
}

#[test]
#[should_panic(expected = "R3: one segment is sealed one way")]
fn two_notes_of_one_segment_that_differ_are_a_bug() {
    let mut tally = HeldTally::default();
    tally.note(SEG, sealed(Some(&[7, 8, 9])));
    tally.note(SEG, sealed(Some(&[7, 8])));
}

#[test]
fn a_report_of_a_held_segment_changes_nothing() {
    let mut tally = HeldTally::default();
    tally.note(SEG, sealed(Some(&[7, 8, 9])));
    assert!(tally.report(NodeId(7), SEG, Slot(5)));
    assert!(tally.report(NodeId(8), SEG, Slot(5)));
    assert!(tally.judge(SEG, &three(), true, Seq(0)));
    assert!(
        !tally.report(NodeId(9), SEG, Slot(5)),
        "a held segment counts no report"
    );
    assert_eq!(tally.reporters(SEG), 0, "a held segment keeps no report");
    assert!(tally.is_held(SEG));
}

/// A segment whose writes all lie below `first_seq` is held with no report,
/// and a segment with no replicas too: a `Truncate` released its records.
/// Its end must still be in the chain.
#[test]
fn a_released_segment_is_held_with_no_report() {
    let mut tally = HeldTally::default();
    tally.note(SEG, sealed(None));
    tally.note(SegmentId(1), sealed(Some(&[7, 8, 9])));
    assert!(!tally.judge(SEG, &three(), true, Seq(0)), "not released");
    assert_eq!(
        tally.released(Seq(1)).collect::<Vec<_>>(),
        [SEG, SegmentId(1)]
    );
    assert!(
        !tally.judge(SEG, &three(), false, Seq(1)),
        "not in the chain"
    );
    assert!(tally.judge(SEG, &three(), true, Seq(1)), "released");
    assert!(tally.is_held(SEG));
    assert_eq!(tally.released(Seq(1)).collect::<Vec<_>>(), [SegmentId(1)]);
    assert!(
        tally.judge(SegmentId(1), &three(), true, Seq(2)),
        "released, with replicas"
    );
    assert_eq!(tally.held().collect::<Vec<_>>(), [SEG, SegmentId(1)]);
}
