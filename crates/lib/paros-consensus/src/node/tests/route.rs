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
pub(super) type Routed = (NodeId, SegmentId, ReplicaSet, Slot);

/// Drain `n`, keeping the `Commit`s it routed to the replicas of a segment.
pub(super) fn drain_routed(
    n: &mut ColocatedNode,
    routed: &mut Vec<Routed>,
) -> Vec<(NodeId, Message)> {
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
pub(super) fn deliver_routed(
    nodes: &mut [ColocatedNode],
    mut queue: Vec<(NodeId, Message)>,
) -> Vec<Routed> {
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

/// Three nodes of a deployment with `matchmakers` matchmakers, whose
/// configuration names `first` as the replicas of the first segment, and
/// their matchmakers.
pub(super) fn cluster_with_first(
    first: Option<&ReplicaSet>,
    matchmakers: u64,
) -> ([ColocatedNode; 3], Vec<Matchmaker>) {
    let pool = [0, 1, 2];
    let nodes = std::array::from_fn(|i| {
        let mut storage = MemStorage::for_node(i as u64, &pool);
        storage.config.nodes = pool.iter().copied().map(NodeId).collect();
        storage.config.matchmakers = (0..matchmakers).map(MatchmakerId).collect();
        storage.config.segment_replicas = first.cloned();
        ColocatedNode::new(&storage, Limits::DEFAULT)
    });
    (nodes, registries(matchmakers))
}

/// Campaign `nodes[idx]` through the matchmakers, and hand back the
/// messages its matchmaking released (its `Prepare`s).
pub(super) fn matchmade(
    nodes: &mut [ColocatedNode],
    idx: usize,
    mms: &mut [Matchmaker],
) -> Vec<(NodeId, Message)> {
    campaign(&mut nodes[idx]);
    let requests = drain_match_requests(&mut nodes[idx]);
    for reply in matchmake(mms, requests) {
        nodes[idx].on_match_reply(reply);
    }
    drain(&mut nodes[idx])
}

/// With the replicas of the first segment in the configuration, a leader
/// of a matchmaker deployment routes every slot it walks to them; without
/// them, or without matchmakers (R10), it routes nothing.
#[test]
fn the_first_segment_takes_the_replicas_of_the_configuration() {
    let set = ReplicaSet::new([NodeId(9)]);
    for (first, matchmakers) in [(None, 3), (Some(set.clone()), 3), (Some(set), 0)] {
        let (mut nodes, mut mms) = cluster_with_first(first.as_ref(), matchmakers);
        if matchmakers == 0 {
            make_leader(&mut nodes, 0);
        } else {
            let q = matchmade(&mut nodes, 0, &mut mms);
            deliver_all(&mut nodes, q);
            assert!(nodes[0].is_leader());
        }
        let first = first.filter(|_| matchmakers > 0);
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

/// A routed `Commit` carries a ballot that chose its slot, never the
/// ballot of the node's own record. Node 0 decides a slot at its ballot
/// with node 1, and its `Commit` to node 2 is late. Node 2 wins a higher
/// ballot with node 1 alone, and votes the slot again at that ballot, with
/// no quorum behind the vote. When the late `Commit` arrives, node 2 folds
/// the slot as a leader and routes it at node 0's ballot: its own record's
/// ballot chose nothing.
#[test]
fn a_routed_commit_carries_the_ballot_that_chose_its_slot() {
    let set = ReplicaSet::new([NodeId(9)]);
    let (mut nodes, mut mms) = cluster_with_first(Some(&set), 3);
    let q = matchmade(&mut nodes, 0, &mut mms);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    let first = nodes[0].ballot();
    let from = nodes[0].replica().folded();
    // Node 0 decides the next slot with node 1; its `Commit`s wait.
    nodes[0].propose(entry(1, 1, 10));
    let mut held = Vec::new();
    let mut queue = drain(&mut nodes[0]);
    while let Some((to, msg)) = queue.pop() {
        match (&msg, to) {
            (Message::Commit { .. }, _) => held.push((to, msg)),
            (_, NodeId(2)) => {}
            _ => {
                let n = node_at(&mut nodes, to);
                n.step(msg);
                queue.extend(drain(n));
            }
        }
    }
    let late = held
        .into_iter()
        .find(|(to, msg)| {
            *to == NodeId(2) && matches!(msg, Message::Commit { slot, .. } if *slot == from)
        })
        .expect("node 0 decided the slot and commits it to node 2");
    // Node 2 wins a higher ballot with node 1, and votes the slot again.
    let mut queue = matchmade(&mut nodes, 2, &mut mms);
    while let Some((to, msg)) = queue.pop() {
        if to == NodeId(0) || matches!(msg, Message::Accept { .. } | Message::Commit { .. }) {
            continue;
        }
        let n = node_at(&mut nodes, to);
        n.step(msg);
        queue.extend(drain(n));
    }
    let second = nodes[2].ballot();
    assert!(nodes[2].is_leader(), "node 2 won with node 1");
    assert!(second > first);
    assert_eq!(
        nodes[2].acceptor().record(from).map(|(ballot, _)| *ballot),
        Some(second),
        "node 2 voted the slot again at its own ballot"
    );
    assert!(!nodes[2].replica().is_chosen(from));
    // The late `Commit` arrives; node 2 folds the slot and routes it.
    nodes[2].step(late.1);
    let (_, routed) = drain_with(&mut nodes[2], |ready| {
        ready
            .messages()
            .iter()
            .filter_map(|(audience, message)| match (audience, message) {
                (Audience::ReplicasOf { .. }, Message::Commit { slot, ballot, .. }) => {
                    Some((*slot, *ballot))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(
        routed,
        [(from, first)],
        "the slot goes to its replicas at the ballot that chose it"
    );
}

/// A slot that the node learned chosen with no choosing ballot (as a
/// handoff's decided tail teaches it) and then learns again from a `Commit`
/// at a higher ballot is routed at that ballot, and the leader does not
/// panic: its record stays at the lower ballot, which a correct run allows.
#[test]
fn a_slot_learned_twice_is_routed_at_the_ballot_of_its_commit() {
    let set = ReplicaSet::new([NodeId(9)]);
    let (mut nodes, mut mms) = cluster_with_first(Some(&set), 3);
    let q = matchmade(&mut nodes, 0, &mut mms);
    deliver_all(&mut nodes, q);
    assert!(nodes[0].is_leader());
    let first = nodes[0].ballot();
    let s0 = nodes[0].replica().folded();
    let s1 = Slot(s0.0 + 1);
    nodes[0].propose(entry(1, 1, 10));
    nodes[0].propose(entry(1, 2, 10));
    let _ = drain(&mut nodes[0]);
    let command = |n: &ColocatedNode, slot| {
        n.proposer
            .rounds()
            .get(&slot)
            .expect("the leader opened a round at the slot")
            .command()
            .clone()
    };
    let (c0, c1) = (command(&nodes[0], s0), command(&nodes[0], s1));
    // As a handoff's decided tail: chosen above a hole, no ballot noted.
    nodes[0].mark_chosen(s1, &c1, first);
    let _ = drain(&mut nodes[0]);
    let higher = Ballot {
        round: first.round + 1,
        node: NodeId(1),
    };
    let mut routed = Vec::new();
    for (slot, command) in [(s1, c1), (s0, c0)] {
        nodes[0].step(Message::Commit {
            from: Party::Node(NodeId(1)),
            ballot: higher,
            slot,
            command,
        });
        let (_, mut more) = drain_with(&mut nodes[0], |ready| {
            ready
                .messages()
                .iter()
                .filter_map(|(audience, message)| match (audience, message) {
                    (Audience::ReplicasOf { .. }, Message::Commit { slot, ballot, .. }) => {
                        Some((*slot, *ballot))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        });
        routed.append(&mut more);
    }
    assert!(nodes[0].is_leader(), "a Commit alone deposes no leader");
    assert_eq!(
        nodes[0].acceptor().record(s1).map(|(ballot, _)| *ballot),
        Some(first),
        "the record of the slot learned first stays at its ballot"
    );
    routed.sort();
    assert_eq!(routed, [(s0, higher), (s1, higher)]);
}

/// A `SetReplicas` of the open segment (D10): the leader routes the slot
/// of the `SetReplicas` to the old set, and each later slot to the new
/// one, from its fold (`Route = "fold"` of `specs/tla/segment-reseat/`).
/// One that names a segment the tail left changes nothing.
#[test]
fn a_set_replicas_routes_each_later_slot_to_the_new_set() {
    use crate::journal::Outcome;
    use crate::node::SetReplicasResult;
    let old = ReplicaSet::new([NodeId(7), NodeId(8)]);
    let new = ReplicaSet::new([NodeId(8), NodeId(9)]);
    let (mut nodes, mut mms) = cluster_with_first(Some(&old), 3);
    let queue = matchmade(&mut nodes, 0, &mut mms);
    let mut routed = deliver_routed(&mut nodes, queue);
    assert!(nodes[0].is_leader());
    let SetReplicasResult::Accepted { slot: at, segment } = nodes[0].set_replicas(new.clone())
    else {
        panic!("the leader admits a SetReplicas");
    };
    assert_eq!(segment, SegmentId::FIRST, "it names the open tail");
    nodes[0].propose(entry(1, 1, 10));
    let queue = drain_routed(&mut nodes[0], &mut routed);
    routed.extend(deliver_routed(&mut nodes, queue));
    for n in &nodes {
        assert_eq!(
            n.replica().outcome_at(at),
            Some(&Outcome::Reseated(new.clone()))
        );
        assert_eq!(n.replica().journal().replicas.as_ref(), Some(&new));
    }
    let at_set: Vec<_> = routed.iter().filter(|(.., slot)| *slot == at).collect();
    assert_eq!(
        at_set,
        [&(NodeId(0), SegmentId::FIRST, old.clone(), at)],
        "the SetReplicas itself goes to the old set"
    );
    let after: Vec<_> = routed.iter().filter(|(.., slot)| *slot > at).collect();
    assert!(!after.is_empty(), "the write past it is routed");
    assert!(
        after.iter().all(|(_, _, set, _)| *set == new),
        "every later slot goes to the new set"
    );
    assert!(
        routed
            .iter()
            .filter(|(.., slot)| *slot < at)
            .all(|(_, _, set, _)| *set == old),
        "every earlier slot went to the old set"
    );

    // Past a `Roll`, a `SetReplicas` that the leader built on the old tail
    // is ignored: the fold judges the segment it names.
    let crate::node::RollResult::Accepted { slot: roll, .. } = nodes[0].roll(None) else {
        panic!("the leader admits a roll");
    };
    let queue = drain_routed(&mut nodes[0], &mut routed);
    routed.extend(deliver_routed(&mut nodes, queue));
    assert_eq!(nodes[0].replica().journal().segment.id, SegmentId(1));
    let stale = crate::journal::SetReplicas {
        segment: SegmentId::FIRST,
        replicas: old.clone(),
    };
    let ProposeResult::Accepted(late) =
        nodes[0].propose_command_at(Command::SetReplicas(stale), crate::node::Delegation::Auto)
    else {
        panic!("the leader admits the command");
    };
    let queue = drain_routed(&mut nodes[0], &mut routed);
    routed.extend(deliver_routed(&mut nodes, queue));
    assert!(late > roll);
    assert_eq!(
        nodes[0].replica().outcome_at(late),
        Some(&Outcome::ReseatIgnored(
            nodes[0].replica().journal().segment
        ))
    );
    assert_eq!(
        nodes[0].replica().journal().replicas.as_ref(),
        Some(&new),
        "an ignored SetReplicas keeps the set"
    );
}
