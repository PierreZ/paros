//! The publish of the chain of segments (R5 of §4.7), pinned at the node:
//! a publish only after the walk applied the `Roll` (R3), its records, the
//! completion at a majority, and the acks a node ignores.

use super::reconfigure::deployed_cluster_with;
use super::*;
use crate::journal::{Opening, ReplicaSet, SegmentId};
use crate::matchmaker::{SegmentAck, SegmentPublish};
use crate::membership::{MatchmakerGeneration, MatchmakerSet};
use crate::node::RollResult;

fn drain_publishes(n: &mut ColocatedNode) -> Vec<(MatchmakerId, SegmentPublish)> {
    drain_with(n, |ready| ready.segment_publishes().to_vec()).1
}

/// Deliver `requests` to the matchmakers, and return their acks.
fn publish(mms: &mut [Matchmaker], requests: &[(MatchmakerId, SegmentPublish)]) -> Vec<SegmentAck> {
    let mut acks = Vec::new();
    for (id, request) in requests {
        let mm = &mut mms[usize::try_from(id.0).expect("matchmaker index")];
        let outcome = mm.publish_segment(request);
        acks.push(mm.segment_ack(request, outcome));
        mm.ready().advance();
    }
    acks
}

/// A cluster of three matchmakers whose leader rolled to `replicas` and
/// whose `Roll` every node walked. Returns the slot of the `Roll`.
fn rolled(replicas: Option<ReplicaSet>) -> ([ColocatedNode; 5], Vec<Matchmaker>, Slot) {
    let (mut nodes, mms) = deployed_cluster_with(3);
    let RollResult::Accepted { slot, .. } = nodes[0].roll(replicas) else {
        panic!("the leader admits a roll");
    };
    // R3: a proposal publishes nothing.
    assert!(!nodes[0].segment_publish_pending());
    let (q, publishes) = drain_with(&mut nodes[0], |ready| ready.segment_publishes().to_vec());
    assert_eq!(publishes, []);
    deliver_all(&mut nodes, q);
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = drain(&mut nodes[i]);
            deliver_all(&mut nodes, q);
        }
    }
    (nodes, mms, slot)
}

/// Every node whose walk applied the `Roll` publishes the end of the first
/// segment and the opening of the next, with the replicas the `Roll` named.
/// The publish completes at a majority of the matchmakers; what reached only
/// one of them is the same record.
#[test]
fn every_node_publishes_the_end_its_walk_applied_until_a_majority_holds_it() {
    let replicas = ReplicaSet::new([NodeId(3), NodeId(4)]);
    let (mut nodes, mut mms, slot) = rolled(Some(replicas.clone()));
    for n in &nodes {
        assert_eq!(n.replica().journal().segment.start, Slot(slot.0 + 1));
        assert!(
            n.segment_publish_pending(),
            "node {} publishes",
            n.config().id
        );
        assert_eq!(
            n.pending_segment_publishes().collect::<Vec<_>>(),
            [SegmentId(0)]
        );
    }
    let end = Slot(slot.0 + 1);
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    assert_eq!(requests.len(), 3, "one publish for each matchmaker");
    for (_, request) in &requests {
        assert_eq!(request.from, NodeId(1));
        assert_eq!(request.sealed, SegmentId(0));
        assert_eq!(request.start, Slot(0));
        assert_eq!(request.end, end);
        assert_eq!(
            request.opening,
            Opening {
                replicas: Some(replicas.clone())
            }
        );
    }
    let acks = publish(&mut mms, &requests[..2]);
    assert_eq!(
        nodes[1].on_segment_ack(&acks[0]),
        None,
        "one ack is no majority"
    );
    assert_eq!(
        nodes[1].on_segment_ack(&acks[0]),
        None,
        "a repeated ack counts once"
    );
    assert_eq!(nodes[1].on_segment_ack(&acks[1]), Some(SegmentId(0)));
    assert!(!nodes[1].segment_publish_pending());
    nodes[1].resend_segment_publishes();
    assert!(
        drain_publishes(&mut nodes[1]).is_empty(),
        "a completed publish is not sent again"
    );
    for mm in &mms[..2] {
        let chain = mm
            .chain(nodes[1].config().journal.journal)
            .expect("a chain");
        assert_eq!(chain[&SegmentId(0)].end, Some(end));
        assert_eq!(chain[&SegmentId(1)].start, end);
        assert!(!chain[&SegmentId(1)].is_sealed());
    }
    assert!(mms[2].chain(nodes[1].config().journal.journal).is_none());
    // Another node's publish of the same end is the same records.
    nodes[2].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[2]);
    let before = mms[0].chain(nodes[2].config().journal.journal).cloned();
    let acks = publish(&mut mms, &requests);
    assert!(acks.iter().all(|a| a.applied));
    assert_eq!(
        mms[0].chain(nodes[2].config().journal.journal).cloned(),
        before
    );
    assert_eq!(
        mms[2].chain(nodes[2].config().journal.journal).cloned(),
        before
    );
}

/// A node ignores an ack of another journal, of another generation, of a
/// segment it does not publish, from a matchmaker outside its set, and a
/// refusal that names no later set.
#[test]
fn a_node_ignores_an_ack_that_is_not_for_its_publish() {
    let (mut nodes, mut mms, _) = rolled(None);
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    let ack = publish(&mut mms, &requests[..1]).remove(0);
    let others = [
        SegmentAck {
            journal: crate::journal::JournalId(ack.journal.0 + 1),
            ..ack.clone()
        },
        SegmentAck {
            generation: MatchmakerGeneration(7),
            ..ack.clone()
        },
        SegmentAck {
            sealed: SegmentId(5),
            ..ack.clone()
        },
        SegmentAck {
            matchmaker: MatchmakerId(9),
            ..ack.clone()
        },
        SegmentAck {
            applied: false,
            ..ack.clone()
        },
    ];
    for other in &others {
        assert_eq!(nodes[1].on_segment_ack(other), None);
        assert!(nodes[1].on_segment_ack(other).is_none());
    }
    // None of them counted: two real acks still make the majority.
    assert_eq!(nodes[1].on_segment_ack(&ack), None);
    let second = publish(&mut mms, &requests[1..2]).remove(0);
    assert_eq!(nodes[1].on_segment_ack(&second), Some(SegmentId(0)));
}

/// A refusal that names a later matchmaker set teaches it, and the publish
/// is asked of the new set, with its acks counted afresh.
#[test]
fn a_refused_publish_moves_to_the_set_the_refusal_names() {
    let (mut nodes, mut mms, _) = rolled(None);
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    let ack = publish(&mut mms, &requests[..1]).remove(0);
    assert_eq!(nodes[1].on_segment_ack(&ack), None);
    let successor = MatchmakerSet::new(
        MatchmakerGeneration(1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    let refusal = SegmentAck {
        matchmaker: MatchmakerId(2),
        applied: false,
        newer: Some(successor.clone()),
        ..ack.clone()
    };
    assert_eq!(nodes[1].on_segment_ack(&refusal), None);
    let requests = drain_publishes(&mut nodes[1]);
    assert_eq!(requests.len(), 3, "the new set is asked whole");
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.generation == successor.generation)
    );
    // The ack of the replaced generation counts no more.
    let old = SegmentAck {
        matchmaker: MatchmakerId(1),
        ..ack
    };
    assert_eq!(nodes[1].on_segment_ack(&old), None);
    assert!(nodes[1].segment_publish_pending());
}

/// The leader rolls once more, keeping the replicas, and every node walks
/// the `Roll`. Returns its slot.
fn roll_again(nodes: &mut [ColocatedNode; 5]) -> Slot {
    let RollResult::Accepted { slot, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    for _ in 0..4 {
        for i in 0..nodes.len() {
            let q = drain(&mut nodes[i]);
            deliver_all(nodes, q);
        }
    }
    slot
}

/// The publishes `n` waits on, each as `(sealed, start, end)`.
fn pending_ends(n: &mut ColocatedNode) -> Vec<(SegmentId, Slot, Slot)> {
    n.resend_segment_publishes();
    let ends: std::collections::BTreeSet<_> = drain_publishes(n)
        .into_iter()
        .map(|(_, r)| (r.sealed, r.start, r.end))
        .collect();
    ends.into_iter().collect()
}

/// A node that restarts notes again every `Roll` its retained fold holds:
/// the tally is volatile, and the end must still reach a majority when every
/// node that walked it restarted.
#[test]
fn a_restarted_node_publishes_again_the_ends_its_fold_holds() {
    let (mut nodes, _, first) = rolled(None);
    let second = roll_again(&mut nodes);
    let storage = MemStorage::from_node(&nodes[1]);
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    assert_eq!(
        pending_ends(&mut n),
        [
            (SegmentId(0), Slot(0), Slot(first.0 + 1)),
            (SegmentId(1), Slot(first.0 + 1), Slot(second.0 + 1)),
        ]
    );
}

/// A refold past a healed hole and the walk after it, in one step: each
/// `Roll` is published with the start that the fold gives its segment, never
/// the tail that stood before the step.
#[test]
fn a_refold_past_a_healed_hole_publishes_each_end_with_its_own_start() {
    let (mut nodes, _, first) = rolled(None);
    let second = roll_again(&mut nodes);
    // Node 1 stops here, and its record of the first `Roll` rots.
    let mut storage = MemStorage::from_node(&nodes[1]);
    storage.rot(first);
    let third = roll_again(&mut nodes);
    let mut n = ColocatedNode::new(&storage, Limits::DEFAULT);
    assert_eq!(n.replica().folded(), first, "the fold stops at the hole");
    let _ = drain(&mut n);
    // One catch-up heals the hole and brings the slots up to the third
    // `Roll`, chosen above the durable chosen index.
    let entries = nodes[0]
        .acceptor()
        .records()
        .range(first..=third)
        .map(|(slot, record)| (*slot, record.clone()))
        .collect();
    n.step(Message::CatchUpResponse {
        from: NodeId(0),
        entries,
    });
    assert_eq!(n.replica().folded(), Slot(third.0 + 1));
    assert_eq!(
        pending_ends(&mut n),
        [
            (SegmentId(0), Slot(0), Slot(first.0 + 1)),
            (SegmentId(1), Slot(first.0 + 1), Slot(second.0 + 1)),
            (SegmentId(2), Slot(second.0 + 1), Slot(third.0 + 1)),
        ]
    );
}

/// A refusal of another journal teaches nothing, even when it names a later
/// matchmaker set: the publish stays asked of the set it was.
#[test]
fn a_refusal_of_another_journal_teaches_no_set() {
    let (mut nodes, mut mms, _) = rolled(None);
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    let ack = publish(&mut mms, &requests[..1]).remove(0);
    let generation = requests[0].1.generation;
    let successor = MatchmakerSet::new(
        MatchmakerGeneration(generation.0 + 1),
        vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
    );
    let foreign = SegmentAck {
        journal: crate::journal::JournalId(ack.journal.0 + 1),
        applied: false,
        newer: Some(successor),
        ..ack
    };
    assert_eq!(nodes[1].on_segment_ack(&foreign), None);
    nodes[1].resend_segment_publishes();
    let again = drain_publishes(&mut nodes[1]);
    assert!(!again.is_empty(), "the publish is still pending");
    assert!(
        again.iter().all(|(_, r)| r.generation == generation),
        "the publish is still asked of the same set"
    );
}
