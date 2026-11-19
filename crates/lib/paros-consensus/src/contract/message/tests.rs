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
