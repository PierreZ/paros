//! The route of a `Commit` to the replicas of its segment (R6, R4d of
//! §4.7), pinned at the node: a leader routes each slot its fold applied,
//! to the replicas of the segment the slot was judged in; a follower and
//! the plain deployment route nothing.

use super::reconfigure::deployed_cluster_with;
use super::*;
use crate::contract::Audience;
use crate::journal::{ReplicaSet, SegmentId};
use crate::node::RollResult;

/// One routed `Commit`: the node that sent it, its segment, its replicas
/// and its slot.
type Routed = (NodeId, SegmentId, ReplicaSet, Slot);

/// Drain `n`, keeping the `Commit`s it routed to the replicas of a segment.
fn drain_routed(n: &mut ColocatedNode, routed: &mut Vec<Routed>) -> Vec<(NodeId, Message)> {
    let me = n.config().id;
    let (queue, mut more) = drain_with(n, |ready| {
        ready
            .messages()
            .iter()
            .filter_map(|(audience, message)| match (audience, message) {
                (Audience::ReplicasOf { segment, replicas }, Message::Commit { slot, .. }) => {
                    Some((me, *segment, replicas.clone(), *slot))
                }
                (Audience::ReplicasOf { .. }, other) => {
                    panic!("only a Commit goes to the replicas of a segment: {other:?}")
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    });
    routed.append(&mut more);
    queue
}

/// Deliver `queue` among `nodes` until it is empty, keeping every routed
/// `Commit`; a message to an id outside the nodes (a replica) is dropped.
fn deliver_routed(nodes: &mut [ColocatedNode], mut queue: Vec<(NodeId, Message)>) -> Vec<Routed> {
    let mut routed = Vec::new();
    while let Some((to, msg)) = queue.pop() {
        let Some(n) = nodes.iter_mut().find(|n| n.config().id == to) else {
            continue;
        };
        n.step(msg);
        queue.extend(drain_routed(n, &mut routed));
    }
    routed
}

/// The leader routes the slots of the segment a `Roll` opened to the
/// replicas the `Roll` named, from its own fold; the `Roll` itself and the
/// slots before it lie in the first segment, which names no replicas here,
/// and go nowhere. No follower routes.
#[test]
fn the_leader_routes_each_walked_slot_to_the_replicas_of_its_segment() {
    let (mut nodes, _mms) = deployed_cluster_with(3);
    let replicas = ReplicaSet::new([NodeId(7), NodeId(8)]);
    let RollResult::Accepted { slot: roll, .. } = nodes[0].roll(Some(replicas.clone())) else {
        panic!("the leader admits a roll");
    };
    let mut routed = Vec::new();
    let queue = drain_routed(&mut nodes[0], &mut routed);
    assert_eq!(routed, [], "a proposal routes nothing (R4d)");
    routed.extend(deliver_routed(&mut nodes, queue));
    assert_eq!(routed, [], "the first segment names no replicas");
    nodes[0].propose(entry(1, 1, 10));
    let queue = drain_routed(&mut nodes[0], &mut routed);
    routed.extend(deliver_routed(&mut nodes, queue));
    let after = Slot(roll.0 + 1);
    assert_eq!(nodes[0].replica().journal().segment.start, after);
    assert_eq!(
        routed,
        [(NodeId(0), SegmentId(1), replicas, after)],
        "the slot past the Roll goes to the replicas the Roll named, from the leader only"
    );
}

/// With the replicas of the first segment in the configuration, a leader
/// routes every slot it walks to them; without them it routes nothing.
#[test]
fn the_first_segment_takes_the_replicas_of_the_configuration() {
    for first in [None, Some(ReplicaSet::new([NodeId(9)]))] {
        let mut nodes: [ColocatedNode; 3] = std::array::from_fn(|i| {
            let mut storage = MemStorage::for_node(i as u64, &[0, 1, 2]);
            storage.config.segment_replicas.clone_from(&first);
            ColocatedNode::new(&storage, Limits::DEFAULT)
        });
        make_leader(&mut nodes, 0);
        let mut routed = Vec::new();
        for seq in 1..=2 {
            nodes[0].propose(entry(1, seq, 10));
            let queue = drain_routed(&mut nodes[0], &mut routed);
            routed.extend(deliver_routed(&mut nodes, queue));
        }
        let walked = nodes[0].replica().folded();
        assert!(walked > Slot(0), "the leader walked its slots");
        let expected: Vec<Routed> = match &first {
            None => Vec::new(),
            Some(set) => (0..walked.0)
                .map(|s| (NodeId(0), SegmentId::FIRST, set.clone(), Slot(s)))
                .collect(),
        };
        let mut sorted = routed.clone();
        sorted.sort_by_key(|r| r.3);
        assert_eq!(sorted, expected, "first segment replicas {first:?}");
    }
}
