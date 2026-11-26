use super::{Audience, Party};
use crate::membership::ProxyId;
use crate::types::NodeId;

/// A party writes its namespace before its number. A node and a proxy with
/// the same number do not look the same.
#[test]
fn a_party_writes_its_namespace() {
    assert_eq!(Party::Node(NodeId(3)).to_string(), "node:3");
    assert_eq!(Party::Proxy(ProxyId(3)).to_string(), "proxy:3");
}

/// Only a node party is a node. A proxy has no node id. A reply to a party
/// goes to that same party.
#[test]
fn only_a_node_party_is_a_node() {
    assert_eq!(Party::Node(NodeId(5)).node(), Some(NodeId(5)));
    assert_eq!(Party::Proxy(ProxyId(5)).node(), None);
    assert_eq!(Party::Node(NodeId(5)).audience(), Audience::Node(NodeId(5)));
    assert_eq!(
        Party::Proxy(ProxyId(5)).audience(),
        Audience::Proxy(ProxyId(5))
    );
}

/// `kind` is the name of the variant, whatever the fields hold.
#[test]
fn a_message_names_its_variant() {
    use super::Message;
    use crate::types::{Ballot, Slot};

    let beat_ack = |round| Message::HeartbeatAck {
        from: NodeId(1),
        ballot: Ballot {
            round,
            node: NodeId(2),
        },
        chosen: None,
    };
    assert_eq!(beat_ack(1).kind(), "HeartbeatAck");
    assert_eq!(beat_ack(9).kind(), "HeartbeatAck");
    let pull = Message::CatchUpRequest {
        from: NodeId(1),
        from_slot: Slot(4),
    };
    assert_eq!(pull.kind(), "CatchUpRequest");
    let read = Message::PreRead {
        reply_to: NodeId(1),
        ctx: 7,
    };
    assert_eq!(read.kind(), "PreRead");
}

/// The replicas of a segment are addressed by their ids, in order, and a
/// sender that keeps the segment never addresses itself.
#[test]
fn the_replicas_of_a_segment_resolve_to_their_members_but_the_sender() {
    use crate::journal::{ReplicaSet, SegmentId};
    let pool = [NodeId(0), NodeId(1), NodeId(2)];
    let audience = Audience::ReplicasOf {
        segment: SegmentId(1),
        replicas: ReplicaSet::new([NodeId(9), NodeId(2), NodeId(7)]),
    };
    assert_eq!(
        audience.resolve(&pool, NodeId(0)),
        [NodeId(2), NodeId(7), NodeId(9)]
    );
    assert_eq!(audience.resolve(&pool, NodeId(2)), [NodeId(7), NodeId(9)]);
    assert_eq!(
        audience.resolve_from_proxy(&pool),
        [NodeId(2), NodeId(7), NodeId(9)]
    );
    assert_eq!(audience.proxy(), None);
}
