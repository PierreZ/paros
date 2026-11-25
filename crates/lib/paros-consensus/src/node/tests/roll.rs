//! The `Roll` (§4.7 of `docs/architecture/04-7-segments.md`), pinned at the
//! node: the refusals of `roll`, the cut that every node folds at the same
//! slot, and a second roll pipelined above the first (no pause).

use super::reconfigure::deployed_cluster;
use super::*;
use crate::journal::{Outcome, ReplicaSet, Segment, SegmentId};
use crate::node::{RollRefusal, RollResult};

/// A plain deployment refuses a roll (D5), and a follower of a matchmaker
/// deployment names its leader.
#[test]
fn a_roll_is_refused_without_matchmakers_and_off_the_leader() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    assert_eq!(
        nodes[0].roll(None),
        RollResult::Refused(RollRefusal::NoMatchmakers)
    );
    assert!(
        drain(&mut nodes[0]).is_empty(),
        "a refused roll sends nothing"
    );

    let (mut nodes, _) = deployed_cluster();
    assert_eq!(nodes[1].roll(None), RollResult::NotLeader(Some(NodeId(0))));
}

/// `propose_control` never carries a `Roll`: it would skip the refusal of a
/// plain deployment.
#[test]
#[should_panic(expected = "a Roll is proposed by ColocatedNode::roll")]
fn a_roll_through_propose_control_is_a_driver_bug() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    let _ = nodes[0].propose_control(Control::Roll {
        next: SegmentId(1),
        replicas: None,
    });
}

/// The leader rolls twice without waiting (no pause): the second `Roll`
/// names the segment after the first, from the leader's open round (R4c).
/// Every node folds both at the same slots, so each segment has one end.
#[test]
fn every_node_folds_a_pipelined_pair_of_rolls_at_the_same_slots() {
    let (mut nodes, _) = deployed_cluster();
    let RollResult::Accepted { slot: first, next } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    assert_eq!(next, SegmentId(1));
    let RollResult::Accepted { slot: second, next } =
        nodes[0].roll(Some(ReplicaSet::new([NodeId(3), NodeId(4)])))
    else {
        panic!("the leader admits a second roll before the first is chosen");
    };
    assert_eq!(
        next,
        SegmentId(2),
        "the second roll builds on the open first"
    );
    assert!(second > first);
    let q = drain(&mut nodes[0]);
    deliver_all(&mut nodes, q);
    let one = Segment {
        id: SegmentId(1),
        start: Slot(first.0 + 1),
    };
    let two = Segment {
        id: SegmentId(2),
        start: Slot(second.0 + 1),
    };
    for n in &nodes[..3] {
        assert_eq!(n.replica().outcome_at(first), Some(&Outcome::Rolled(one)));
        assert_eq!(n.replica().outcome_at(second), Some(&Outcome::Rolled(two)));
        assert_eq!(n.replica().journal().segment, two);
    }
    // The next roll builds on the folded tail.
    let RollResult::Accepted { next, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a third roll");
    };
    assert_eq!(next, SegmentId(3));
}
