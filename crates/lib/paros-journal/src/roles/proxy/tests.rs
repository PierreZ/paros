use paros_consensus::{Ballot, NodeId, Slot};

use super::{JournalProxy, ProxyEffect, RETENTION};
use crate::command::Command;
use crate::roles::message::Message;
use crate::segment::SegmentId;

const LEADER: NodeId = NodeId(100);
const PROXY: NodeId = NodeId(200);

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: LEADER,
    }
}

fn column() -> Vec<NodeId> {
    vec![NodeId(1), NodeId(2), NodeId(3)]
}

fn replicas() -> Vec<NodeId> {
    vec![NodeId(11), NodeId(12), NodeId(13)]
}

fn delegate(round: u64, slot: u64) -> Message {
    Message::Delegate {
        ballot: ballot(round),
        slot: Slot(slot),
        command: Command::Noop,
        column: column(),
        segment: SegmentId(1),
        replicas: replicas(),
    }
}

fn accepted(round: u64, slot: u64) -> Message {
    Message::Accepted {
        ballot: ballot(round),
        slot: Slot(slot),
    }
}

fn to(effects: &[ProxyEffect]) -> Vec<NodeId> {
    effects.iter().map(|e| e.to).collect()
}

#[test]
fn a_delegation_sends_the_accept_to_its_column() {
    let mut proxy = JournalProxy::new(PROXY);
    let effects = proxy.step(LEADER, delegate(1, 4));
    assert_eq!(to(&effects), column());
    assert!(
        effects
            .iter()
            .all(|e| matches!(e.message, Message::Accept { slot: Slot(4), .. }))
    );
    assert_eq!(proxy.open(), 1);
}

#[test]
fn a_majority_of_the_column_commits_to_the_replicas_and_tells_the_leader() {
    let mut proxy = JournalProxy::new(PROXY);
    proxy.step(LEADER, delegate(1, 4));
    assert_eq!(proxy.step(NodeId(1), accepted(1, 4)), Vec::new());
    // A second vote of the same acceptor counts once.
    assert_eq!(proxy.step(NodeId(1), accepted(1, 4)), Vec::new());
    let effects = proxy.step(NodeId(3), accepted(1, 4));
    let mut expected = replicas();
    expected.push(LEADER);
    assert_eq!(to(&effects), expected);
    assert!(matches!(
        effects.last().map(|e| &e.message),
        Some(Message::Chosen { slot: Slot(4), .. })
    ));
    assert_eq!(proxy.open(), 0, "a decided round closes");
}

#[test]
fn a_vote_from_outside_the_column_or_at_another_ballot_is_not_counted() {
    let mut proxy = JournalProxy::new(PROXY);
    proxy.step(LEADER, delegate(2, 4));
    assert_eq!(proxy.step(NodeId(7), accepted(2, 4)), Vec::new());
    assert_eq!(proxy.step(NodeId(1), accepted(1, 4)), Vec::new());
    assert_eq!(proxy.step(NodeId(2), accepted(2, 4)), Vec::new());
    assert!(
        proxy.step(NodeId(9), accepted(2, 4)).is_empty(),
        "one vote of the column and two outside it are no majority"
    );
    assert_eq!(proxy.step(NodeId(3), accepted(2, 4)).len(), 4);
}

#[test]
fn a_nack_drops_the_ballot_and_goes_to_the_leader() {
    let mut proxy = JournalProxy::new(PROXY);
    proxy.step(LEADER, delegate(1, 4));
    proxy.step(LEADER, delegate(1, 5));
    let effects = proxy.step(
        NodeId(2),
        Message::Nack {
            ballot: ballot(1),
            promised: ballot(3),
        },
    );
    assert_eq!(to(&effects), vec![LEADER], "one Nack for each leader");
    assert_eq!(proxy.open(), 0);
    // A Nack that refuses nothing tells nobody.
    assert_eq!(
        proxy.step(
            NodeId(2),
            Message::Nack {
                ballot: ballot(1),
                promised: ballot(1),
            },
        ),
        Vec::new()
    );
}

#[test]
fn a_higher_ballot_drops_the_rounds_below_and_nacks_their_leader() {
    let mut proxy = JournalProxy::new(PROXY);
    let old = NodeId(102);
    proxy.step(old, delegate(1, 4));
    let effects = proxy.step(LEADER, delegate(2, 5));
    assert_eq!(proxy.open(), 1);
    assert_eq!(proxy.round(Slot(5)).map(|r| r.0), Some(ballot(2)));
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e.message, Message::Nack { .. }))
            .cloned()
            .collect::<Vec<_>>(),
        vec![ProxyEffect {
            to: old,
            message: Message::Nack {
                ballot: ballot(1),
                promised: ballot(2),
            },
        }],
        "the leader of the dropped round hears the higher ballot"
    );
    assert_eq!(
        proxy.step(old, delegate(1, 6)),
        vec![ProxyEffect {
            to: old,
            message: Message::Nack {
                ballot: ballot(1),
                promised: ballot(2),
            },
        }],
        "a lower delegation is not taken, and its leader hears why"
    );
    assert_eq!(proxy.open(), 1);
}

#[test]
fn a_column_that_is_not_two_f_plus_one_is_not_taken() {
    let mut proxy = JournalProxy::new(PROXY);
    let effects = proxy.step(
        LEADER,
        Message::Delegate {
            ballot: ballot(1),
            slot: Slot(1),
            command: Command::Noop,
            column: vec![NodeId(1), NodeId(2)],
            segment: SegmentId(1),
            replicas: replicas(),
        },
    );
    assert_eq!(effects, Vec::new());
    assert_eq!(proxy.open(), 0);
}

#[test]
fn a_decided_round_delegated_again_is_decided_again() {
    let mut proxy = JournalProxy::new(PROXY);
    proxy.step(LEADER, delegate(1, 4));
    proxy.step(NodeId(1), accepted(1, 4));
    assert_ne!(proxy.step(NodeId(2), accepted(1, 4)), Vec::new());
    // The leader lost the `Chosen` and delegates the round again.
    assert_eq!(proxy.step(LEADER, delegate(1, 4)).len(), 3);
    proxy.step(NodeId(2), accepted(1, 4));
    assert_eq!(proxy.step(NodeId(3), accepted(1, 4)).len(), 4);
}

#[test]
fn a_tick_sends_again_and_drops_a_stalled_round() {
    let mut proxy = JournalProxy::new(PROXY);
    proxy.step(LEADER, delegate(1, 4));
    for _ in 0..RETENTION {
        assert_eq!(to(&proxy.tick(4)), column());
    }
    assert!(proxy.tick(4).is_empty(), "a stalled round is dropped");
    assert_eq!(proxy.open(), 0);
}
