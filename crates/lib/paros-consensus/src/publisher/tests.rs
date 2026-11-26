use super::Publisher;
use crate::journal::{Opening, SegmentId};
use crate::matchmaker::{SegmentAck, SegmentPublish};
use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::types::{NodeId, Slot};

fn set(generation: u64) -> MatchmakerSet {
    MatchmakerSet::new(
        MatchmakerGeneration(generation),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    )
}

fn publish(sealed: u64, end: u64) -> SegmentPublish {
    SegmentPublish {
        from: NodeId(1),
        generation: MatchmakerGeneration(0),
        sealed: SegmentId(sealed),
        start: Slot(if sealed == 0 { 0 } else { end - 1 }),
        end: Slot(end),
        opening: Opening {
            replicas: None,
            boundary: crate::journal::JournalState::default(),
        },
    }
}

fn ack(matchmaker: u64, sealed: u64, generation: u64) -> SegmentAck {
    SegmentAck {
        matchmaker: MatchmakerId(matchmaker),
        generation: MatchmakerGeneration(generation),
        sealed: SegmentId(sealed),
        applied: true,
        newer: None,
    }
}

#[test]
fn a_publish_completes_at_a_majority_of_its_generation() {
    let set = set(0);
    let mut publisher = Publisher::new(set.generation);
    assert!(!publisher.is_pending());
    publisher.note(publish(0, 4));
    publisher.note(publish(1, 6));
    publisher.note(publish(0, 4));
    assert_eq!(
        publisher.pending().collect::<Vec<_>>(),
        [SegmentId(0), SegmentId(1)]
    );
    assert_eq!(publisher.requests(&set).count(), 6);
    assert_eq!(publisher.fold_ack(&ack(0, 0, 0), &set), None);
    assert_eq!(publisher.requests(&set).count(), 5);
    assert!(!publisher.is_published(SegmentId(0)));
    assert_eq!(publisher.fold_ack(&ack(1, 0, 0), &set), Some(SegmentId(0)));
    assert_eq!(publisher.pending().collect::<Vec<_>>(), [SegmentId(1)]);
    assert!(publisher.is_published(SegmentId(0)));
    assert!(!publisher.is_published(SegmentId(1)));
    publisher.note(publish(0, 4));
    assert_eq!(
        publisher.pending().collect::<Vec<_>>(),
        [SegmentId(1)],
        "a completed publish noted again waits no more"
    );
    assert_eq!(
        publisher.fold_ack(&ack(2, 0, 0), &set),
        None,
        "a completed publish is gone"
    );
    assert!(publisher.is_pending());
}

#[test]
fn a_new_generation_keeps_every_publish_and_forgets_the_acks() {
    let mut publisher = Publisher::new(MatchmakerGeneration(0));
    publisher.note(publish(0, 4));
    assert_eq!(publisher.fold_ack(&ack(0, 0, 0), &set(0)), None);
    publisher.reset_for_generation(MatchmakerGeneration(1));
    assert_eq!(publisher.generation(), MatchmakerGeneration(1));
    let next = set(1);
    let requests: Vec<_> = publisher.requests(&next).collect();
    assert_eq!(requests.len(), 3, "the acks of the old set count no more");
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.generation == next.generation)
    );
    assert_eq!(publisher.fold_ack(&ack(1, 0, 0), &next), None, "an old ack");
    assert_eq!(publisher.fold_ack(&ack(1, 0, 1), &next), None);
    assert_eq!(publisher.fold_ack(&ack(2, 0, 1), &next), Some(SegmentId(0)));
}

#[test]
fn a_refusal_or_a_stranger_counts_for_nothing() {
    let current = set(0);
    let mut publisher = Publisher::new(current.generation);
    publisher.note(publish(0, 4));
    let refused = SegmentAck {
        applied: false,
        ..ack(0, 0, 0)
    };
    assert_eq!(publisher.fold_ack(&refused, &current), None);
    assert_eq!(publisher.fold_ack(&ack(7, 0, 0), &current), None);
    assert_eq!(
        publisher.fold_ack(&ack(1, 0, 0), &set(1)),
        None,
        "a set of another generation"
    );
    assert_eq!(publisher.requests(&current).count(), 3, "no ack counted");
}

#[test]
#[should_panic(expected = "R3: one segment's end is published with one value")]
fn two_ends_of_one_segment_are_a_bug() {
    let mut publisher = Publisher::new(MatchmakerGeneration(0));
    publisher.note(publish(0, 4));
    publisher.note(publish(0, 5));
}

#[test]
#[should_panic(expected = "a publish addresses the generation its acks are counted for")]
fn a_request_to_another_generation_is_a_bug() {
    let publisher = Publisher::new(MatchmakerGeneration(0));
    let _ = publisher.requests(&set(1)).count();
}
