//! The held tally at the node (R7 of §4.7): the reports of the replicas
//! that the fold names, more than `F` of them, and the end in the chain.

use super::chain::{drain_publishes, publish, rolled};
use super::*;
use crate::journal::{ReplicaSet, SegmentId};
use crate::node::RollResult;

fn held(from: u64, segment: u64, end: Slot) -> Message {
    Message::SegmentHeld {
        from: NodeId(from),
        segment: SegmentId(segment),
        end,
    }
}

/// Segment 1 (replicas 3 and 4, named by the first `Roll`) is sealed by a
/// second `Roll`. The leader marks it `Held` once two replicas (`F + 1`
/// over three acceptors) reported it and its own publish of the end
/// completed, in either order.
#[test]
fn a_segment_is_held_once_f_plus_one_replicas_report_it_and_its_end_is_published() {
    let (mut nodes, mut mms, _) = rolled(Some(ReplicaSet::new([NodeId(3), NodeId(4)])));
    let RollResult::Accepted { slot, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a second roll");
    };
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = drain(&mut nodes[i]);
            deliver_all(&mut nodes, q);
        }
    }
    let end = Slot(slot.0 + 1);
    assert_eq!(nodes[0].replica().journal().segment.id, SegmentId(2));
    // Reports that do not count: a node that is no replica of the
    // segment, another end, and a segment that names no replicas.
    nodes[0].step(held(1, 1, end));
    nodes[0].step(held(3, 1, slot));
    nodes[0].step(held(3, 0, Slot(1)));
    nodes[0].step(held(3, 7, end));
    nodes[0].step(held(3, 1, end));
    assert_eq!(nodes[0].held_segments().count(), 0, "one copy, F = 1");
    nodes[0].step(held(4, 1, end));
    assert_eq!(
        nodes[0].held_segments().count(),
        0,
        "the end is not in the chain yet"
    );
    nodes[0].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[0]);
    let acks = publish(&mut mms, &requests);
    for ack in acks.iter().filter(|a| a.sealed == SegmentId(1)) {
        let _ = nodes[0].on_segment_ack(ack);
    }
    assert_eq!(nodes[0].held_segments().collect::<Vec<_>>(), [SegmentId(1)]);
    // A follower that hears the reports after its publish completed marks
    // it at the last report.
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    for ack in publish(&mut mms, &requests) {
        let _ = nodes[1].on_segment_ack(&ack);
    }
    nodes[1].step(held(3, 1, end));
    assert_eq!(nodes[1].held_segments().count(), 0);
    nodes[1].step(held(4, 1, end));
    assert_eq!(nodes[1].held_segments().collect::<Vec<_>>(), [SegmentId(1)]);
}

/// A plain deployment ignores every report.
#[test]
fn a_plain_node_ignores_a_report() {
    let mut nodes: [ColocatedNode; 3] = cluster();
    nodes[0].step(held(1, 0, Slot(1)));
    assert_eq!(nodes[0].held_segments().count(), 0);
}
