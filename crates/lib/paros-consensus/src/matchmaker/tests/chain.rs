//! The chain of segments at a matchmaker (R5 of §4.7): a publish merges, a
//! frozen member refuses one, the frozen page reports the chain, and an
//! activation merges the reconstruction into the member's own chain.

use super::*;
use crate::journal::{Chain, Opening, ReplicaSet, SegmentId, SegmentRecord};
use crate::types::Slot;

fn publish(sealed: u64, start: u64, end: u64, generation: MatchmakerGeneration) -> SegmentPublish {
    SegmentPublish {
        from: NodeId(1),
        journal: J,
        generation,
        sealed: SegmentId(sealed),
        start: Slot(start),
        end: Slot(end),
        opening: Opening {
            replicas: Some(ReplicaSet::new([NodeId(3)])),
        },
    }
}

fn publish_at(
    mm: &mut Matchmaker,
    request: &SegmentPublish,
) -> (PublishOutcome, SegmentAck, Vec<MatchmakerWriteOp>) {
    let outcome = mm.publish_segment(request);
    let ack = mm.segment_ack(request, outcome);
    let ready = mm.ready();
    let writes = ready.writes().to_vec();
    ready.advance();
    (outcome, ack, writes)
}

#[test]
fn a_publish_grows_the_chain_durably_and_once() {
    let mut mm = fresh(0);
    assert!(mm.chain(J).is_none());
    let (outcome, ack, writes) = publish_at(&mut mm, &publish(0, 0, 5, G0));
    assert_eq!(outcome, PublishOutcome::Merged);
    assert!(ack.applied);
    assert_eq!(ack.sealed, SegmentId(0));
    assert!(ack.newer.is_none());
    assert!(
        matches!(writes.as_slice(), [MatchmakerWriteOp::SetScalars(s)] if s.journals[&J].segments.len() == 2)
    );
    let chain = mm.chain(J).expect("a chain").clone();
    assert_eq!(chain[&SegmentId(0)].end, Some(Slot(5)));
    assert_eq!(chain[&SegmentId(1)].start, Slot(5));
    // The same publish again changes nothing and is acked again.
    let (outcome, ack, writes) = publish_at(&mut mm, &publish(0, 0, 5, G0));
    assert_eq!(outcome, PublishOutcome::Unchanged);
    assert!(ack.applied);
    assert_eq!(writes, []);
    // The next seal fills the open record, and the chain survives a reboot.
    let (outcome, _, _) = publish_at(&mut mm, &publish(1, 5, 9, G0));
    assert_eq!(outcome, PublishOutcome::Merged);
    let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &image(&mm), Limits::DEFAULT);
    let chain = rebooted.chain(J).expect("a chain");
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[&SegmentId(1)].end, Some(Slot(9)));
    assert!(chain[&SegmentId(1)].opening.is_some());
}

#[test]
#[should_panic(expected = "R5: two records of one segment agree on its end")]
fn a_publish_with_another_end_is_a_safety_violation() {
    let mut mm = fresh(0);
    let _ = publish_at(&mut mm, &publish(0, 0, 5, G0));
    let _ = mm.publish_segment(&publish(0, 0, 6, G0));
}

#[test]
fn a_frozen_member_refuses_a_publish_and_reports_its_chain() {
    let mut mm = fresh(0);
    let _ = publish_at(&mut mm, &publish(0, 0, 5, G0));
    mm.step_reconfigure(ReconfigureRequest::Stop {
        from: NodeId(5),
        generation: G0,
        after: None,
    });
    let (_, replies) = drain_reconfigure(&mut mm);
    let ReconfigureReply::Stopped { registries, .. } = &replies[0] else {
        panic!("expected Stopped, got {:?}", replies[0]);
    };
    assert_eq!(Some(&registries[&J].segments), mm.chain(J));
    let (outcome, ack, writes) = publish_at(&mut mm, &publish(1, 5, 9, G0));
    assert_eq!(outcome, PublishOutcome::Refused);
    assert!(!ack.applied);
    assert_eq!(writes, []);
    assert!(!mm.chain(J).expect("a chain")[&SegmentId(1)].is_sealed());
    // A publish of another generation is refused too.
    let (outcome, _, _) = publish_at(&mut fresh(1), &publish(0, 0, 5, MatchmakerGeneration(3)));
    assert_eq!(outcome, PublishOutcome::Refused);
}

/// A member of the successor that took a publish no stop quorum reported
/// (a minority publish) keeps it: the activation merges the reconstruction
/// into its own chain (the control `chain_overwrite` of
/// `specs/tla/handover-scalars/` replaces it).
#[test]
fn an_activation_merges_the_reconstructed_chain_into_its_own() {
    let mut mm = fresh(0);
    let _ = publish_at(&mut mm, &publish(1, 5, 9, G0));
    let reconstructed: Chain = [
        (
            SegmentId(0),
            SegmentRecord {
                start: Slot(0),
                opening: None,
                end: Some(Slot(5)),
            },
        ),
        (
            SegmentId(1),
            SegmentRecord {
                start: Slot(5),
                opening: Some(Opening {
                    replicas: Some(ReplicaSet::new([NodeId(3)])),
                }),
                end: None,
            },
        ),
    ]
    .into_iter()
    .collect();
    let successor = MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(3)],
    );
    mm.step_reconfigure(ReconfigureRequest::Bootstrap {
        from: NodeId(9),
        bootstrap: PendingBootstrap {
            set: successor.clone(),
            registries: BTreeMap::from([(
                J,
                JournalRegistry {
                    gc_watermark: Ballot::zero(),
                    history: BTreeMap::new(),
                    effective: None,
                    retired: BTreeMap::new(),
                    segments: reconstructed,
                },
            )]),
            complete: true,
        }
        .page(None, REGISTRY_PAGE),
    });
    mm.ready().advance();
    let _ = chosen(&mut mm, 9, &successor);
    assert_eq!(*mm.set(), successor);
    let chain = mm.chain(J).expect("a chain");
    assert_eq!(
        chain.len(),
        3,
        "its own seal and the reconstruction's records"
    );
    assert_eq!(chain[&SegmentId(0)].end, Some(Slot(5)));
    assert_eq!(
        chain[&SegmentId(1)].end,
        Some(Slot(9)),
        "the minority seal stays"
    );
    assert!(chain[&SegmentId(1)].opening.is_some());
    let rebooted = Matchmaker::new(&mmconfig(0, &[0, 1, 2]), &image(&mm), Limits::DEFAULT);
    assert_eq!(rebooted.chain(J), mm.chain(J));
}
