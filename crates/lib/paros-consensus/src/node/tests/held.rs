//! The held tally at the node (R7 of §4.7): the reports of the replicas
//! that the fold names, more than `F` of them, and the end in the chain.

use super::chain::{drain_publishes, publish, rolled};
use super::*;
use crate::journal::{Command, Hold, Outcome, ReplicaSet, SegmentId, Seq};
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
    // One write in segment 1, so that no `Truncate` has released it.
    own_and_write(&mut nodes, &mut Vec::new());
    // The second `Roll` names other replicas, for segment 2: the replicas
    // of segment 1 are the ones its own `Roll` named.
    let RollResult::Accepted { slot, .. } =
        nodes[0].roll(Some(ReplicaSet::new([NodeId(5), NodeId(6)])))
    else {
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
    nodes[0].step(held(5, 1, end));
    nodes[0].step(held(6, 1, end));
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
    // The first segment holds no write, so it is released once its end is
    // in the chain: this follower's own publish completed.
    nodes[1].step(held(3, 1, end));
    assert_eq!(
        nodes[1].held_segments().collect::<Vec<_>>(),
        [SegmentId::FIRST]
    );
    nodes[1].step(held(4, 1, end));
    assert_eq!(
        nodes[1].held_segments().collect::<Vec<_>>(),
        [SegmentId::FIRST, SegmentId(1)]
    );
    // The leader's publish of the first segment's end has not completed,
    // so it does not hold that segment, and the held segments of the log
    // are a prefix: no `Hold` is proposed.
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = drain(&mut nodes[i]);
            deliver_all(&mut nodes, q);
        }
    }
    assert_eq!(nodes[0].replica().journal().held, None);
    assert!(
        nodes[0]
            .chosen_commands()
            .all(|(_, c)| !matches!(c, Command::Hold(_))),
        "a segment past one that is not held gets no Hold"
    );
}

/// The replicas of the first segment come from the configuration
/// (`Config::segment_replicas`).
#[test]
fn the_first_segment_is_held_by_the_replicas_of_the_configuration() {
    let first = ReplicaSet::new([NodeId(8), NodeId(9)]);
    let (mut nodes, mut mms) = super::route::cluster_with_first(Some(&first), 3);
    let q = super::route::matchmade(&mut nodes, 0, &mut mms);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    // One write in the first segment, so that no `Truncate` has released
    // it: only its replicas make it held.
    let mut routed = Vec::new();
    own_and_write(&mut nodes, &mut routed);
    let RollResult::Accepted { slot, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    // The leader routes the slots of the first segment to its replicas,
    // which are not nodes of this test.
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = super::route::drain_routed(&mut nodes[i], &mut routed);
            routed.extend(super::route::deliver_routed(&mut nodes, q));
        }
    }
    assert!(
        routed
            .iter()
            .any(|(_, segment, set, _)| *segment == SegmentId::FIRST && *set == first)
    );
    nodes[0].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[0]);
    for ack in publish(&mut mms, &requests) {
        let _ = nodes[0].on_segment_ack(&ack);
    }
    let end = Slot(slot.0 + 1);
    nodes[0].step(held(8, 0, end));
    assert_eq!(nodes[0].held_segments().count(), 0);
    nodes[0].step(held(9, 0, end));
    assert_eq!(
        nodes[0].held_segments().collect::<Vec<_>>(),
        [SegmentId::FIRST]
    );
    // The leader proposes the `Hold` of the first segment at once, with the
    // end and the position of its fold; every node folds it.
    let hold = Hold {
        segment: SegmentId::FIRST,
        end,
        next_seq: Seq(1),
    };
    let proposed: Vec<_> = nodes[0]
        .proposer
        .rounds()
        .iter()
        .filter(|(_, round)| *round.command() == Command::Hold(hold))
        .map(|(slot, _)| *slot)
        .collect();
    assert_eq!(proposed.len(), 1, "one Hold for the held segment");
    // Another report proposes no second `Hold`.
    nodes[0].step(held(8, 0, end));
    assert_eq!(
        nodes[0]
            .proposer
            .rounds()
            .iter()
            .filter(|(_, round)| matches!(round.command(), Command::Hold(_)))
            .count(),
        1
    );
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = super::route::drain_routed(&mut nodes[i], &mut routed);
            routed.extend(super::route::deliver_routed(&mut nodes, q));
        }
    }
    for node in &nodes {
        assert_eq!(node.replica().journal().held, Some(hold));
        assert_eq!(
            node.replica().outcome_at(proposed[0]),
            Some(&Outcome::HeldTo(hold))
        );
        // The acceptors drop the held segment at the fold of its `Hold`:
        // its replicas hold it whole, and no retry above the end reads it.
        assert_eq!(node.acceptor().first_slot(), end, "the drop to the end");
        assert_eq!(
            node.replica().journal().first_seq,
            Seq(0),
            "nothing truncated"
        );
    }
}

/// Claim the journal for leader uuid 1 and write one record at position 0,
/// decided on every node.
fn own_and_write(nodes: &mut [ColocatedNode], routed: &mut Vec<super::route::Routed>) {
    let _ = nodes[0].propose_control(Control::SetLeader {
        new: LeaderUuid(1),
        old: None,
    });
    let _ = nodes[0].propose(Entry {
        leader: LeaderUuid(1),
        seq: Seq(0),
        records: vec![val(1)],
    });
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = super::route::drain_routed(&mut nodes[i], routed);
            routed.extend(super::route::deliver_routed(nodes, q));
        }
    }
    assert_eq!(
        nodes[0].replica().journal().next_seq,
        Seq(1),
        "the write is accepted"
    );
}

/// A follower whose tally marks a segment proposes nothing: only the
/// leader proposes a `Hold`.
#[test]
fn a_follower_proposes_no_hold() {
    let first = ReplicaSet::new([NodeId(8), NodeId(9)]);
    let (mut nodes, mut mms) = super::route::cluster_with_first(Some(&first), 3);
    let q = super::route::matchmade(&mut nodes, 0, &mut mms);
    deliver_all(&mut nodes, q);
    let RollResult::Accepted { slot, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    let mut routed = Vec::new();
    for _ in 0..3 {
        for i in 0..nodes.len() {
            let q = super::route::drain_routed(&mut nodes[i], &mut routed);
            routed.extend(super::route::deliver_routed(&mut nodes, q));
        }
    }
    nodes[1].resend_segment_publishes();
    let requests = drain_publishes(&mut nodes[1]);
    for ack in publish(&mut mms, &requests) {
        let _ = nodes[1].on_segment_ack(&ack);
    }
    let end = Slot(slot.0 + 1);
    nodes[1].step(held(8, 0, end));
    nodes[1].step(held(9, 0, end));
    assert_eq!(
        nodes[1].held_segments().collect::<Vec<_>>(),
        [SegmentId::FIRST]
    );
    assert!(
        nodes[1].proposer.rounds().is_empty(),
        "a follower opens no round"
    );
    assert_eq!(nodes[1].known_held(), None);
}

/// A plain deployment ignores every report.
#[test]
fn a_plain_node_ignores_a_report() {
    let mut nodes: [ColocatedNode; 3] = cluster();
    nodes[0].step(held(1, 0, Slot(1)));
    assert_eq!(nodes[0].held_segments().count(), 0);
}
