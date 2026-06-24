//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`paros_core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `RawNode`.
//!
//! [`run_node`] is written once over moonpool's `P: Providers` abstraction, so
//! the *same* code runs in production (`TokioProviders`) and deterministic
//! simulation (`SimProviders`); the deterministic-simulation harness lives in
//! `paros-sim` and adapts a moonpool `Process` to [`run_node`]. The client API
//! and a `parosd` binary land here too, once the protocol stabilizes.

mod driver;
mod storage;

pub use driver::{
    EV_APPLIED, EV_CHOSEN, EV_LEADER, EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK, Paros,
    Propose, ProposeAck, WLTOKEN_PAROS, parse_addr, run_node,
};
pub use storage::{MemStorage, NodeStorage};

pub use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Config, Entry, HardState, Message, NodeId, NodeRole,
    ProposeResult, RawNode, Ready, Slot, Storage, Value,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use paros_core::{Ballot, ClientId, ClientSeq, Entry, Message, NodeId, Slot, Value};

    /// One representative of every `Message` variant.
    fn every_variant() -> Vec<Message> {
        let ballot = Ballot {
            round: 7,
            node: NodeId(3),
        };
        let entry = Entry {
            client: ClientId(1),
            seq: ClientSeq(2),
            value: Value(vec![1, 2, 3]),
        };
        let mut accepted = BTreeMap::new();
        accepted.insert(Slot(5), (ballot, entry.clone()));
        vec![
            Message::Prepare {
                from: NodeId(1),
                ballot,
                from_slot: Slot(5),
            },
            Message::Promise {
                from: NodeId(1),
                ballot,
                from_slot: Slot(5),
                accepted,
            },
            Message::Accept {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
                entry: entry.clone(),
            },
            Message::Accepted {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
            },
            Message::Nack {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
            },
            Message::Commit {
                from: NodeId(0),
                ballot,
                slot: Slot(6),
                entry,
            },
            Message::CheckLeader { from: NodeId(0) },
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: Slot(2),
            },
        ]
    }

    /// The driver puts `paros_core::Message` on the wire directly (no DTO): every
    /// variant must serde round-trip losslessly.
    #[test]
    fn message_serde_round_trips() {
        for msg in every_variant() {
            let json = serde_json::to_string(&msg).expect("serialize");
            let back: Message = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(msg, back, "serde round-trip must be lossless for {msg:?}");
        }
    }
}
