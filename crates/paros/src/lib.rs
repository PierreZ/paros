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

mod audit;
mod driver;
mod grpc;
mod hooks;
mod storage;

pub use audit::{Audit, NoAudit};
pub use driver::{
    EV_APPLIED, EV_BOOTED, EV_CHOSEN, EV_CHOSEN_GAP, EV_COMPACTED, EV_CRASHED,
    EV_ELECTION_TIMEOUT_EXTREME, EV_GAP_FILLED, EV_LEADER, EV_LEADERSHIP_RESIGNED, EV_MSG_RECV,
    EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK, EV_PERSIST, EV_PREPARE_BELOW_FLOOR,
    EV_PROPOSE_DEDUP_ACK, EV_RECOVERED, EV_RESEND_SKIPPED, EV_SEND_DROPPED, EV_SNAPSHOT_INSTALLED,
    EV_SNAPSHOT_MID_ELECTION, EV_SNAPSHOT_OFFERED, EV_SYNCED, command_hash, is_seam_crash,
    parse_addr, run_node,
};
pub use grpc::{
    Compact, CompactAck, InspectReply, InspectRequest, ParosClient, ParosInternalClient, Propose,
    ProposeAck, Read, ReadAck,
};
pub use hooks::{DriverHooks, NoHooks, Seam};
pub use storage::{MemStorage, NodeStorage, StorageError};

pub use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Config, Control, Entry, HardState, Message, MustSync,
    NodeId, NodeRole, ProposeResult, QuorumSystem, RawNode, ReadIndexResult, ReadState, Ready,
    Slot, Storage, Value, WriteOp,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use paros_core::{
        Ballot, ClientId, ClientSeq, Command, Control, Entry, Message, NodeId, Slot, Value,
    };
    use prost::Message as ProstMessage;

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
        let command = Command::User(entry.clone());
        // Control commands in the accepted suffix exercise both protobuf
        // control variants alongside the client-entry case.
        let control = Command::Control(Control::Truncate { up_to: Slot(3) });
        let mut accepted = BTreeMap::new();
        accepted.insert(Slot(5), (ballot, command.clone()));
        accepted.insert(Slot(6), (ballot, control));
        accepted.insert(Slot(7), (ballot, Command::Control(Control::Noop)));
        let mut catchup = BTreeMap::new();
        catchup.insert(Slot(4), (ballot, command.clone()));
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
                command: command.clone(),
            },
            Message::Accepted {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
            },
            Message::Nack {
                from: NodeId(2),
                ballot,
                promised: Ballot {
                    round: 9,
                    node: NodeId(4),
                },
                slot: Slot(6),
            },
            Message::Commit {
                from: NodeId(0),
                ballot,
                slot: Slot(6),
                command,
            },
            Message::CatchUpRequest {
                from: NodeId(1),
                from_slot: Slot(4),
            },
            Message::CatchUpResponse {
                from: NodeId(0),
                entries: catchup,
            },
            Message::InstallSnapshot {
                from: NodeId(0),
                ballot,
                chosen_index: Slot(5),
                snapshot: Value(vec![9, 9, 9]),
            },
            Message::CheckLeader { from: NodeId(0) },
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: Some(Slot(2)),
                seq: 9,
            },
            // The empty watermark is its own variant of the beat, and the one the
            // wire encoding used to be unable to say (#56): a leader that has
            // chosen nothing is not a leader that has chosen slot 0.
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: None,
                seq: 10,
            },
            Message::HeartbeatAck {
                from: NodeId(1),
                ballot,
                seq: 9,
            },
        ]
    }

    /// Every domain variant must round-trip through the typed protobuf contract
    /// losslessly before the driver is allowed to put it on the wire.
    #[test]
    fn message_protobuf_round_trips() {
        for msg in every_variant() {
            let wire = crate::grpc::message_to_proto(&msg).expect("encode protobuf DTO");
            let bytes = wire.encode_to_vec();
            let decoded = crate::grpc::internal::ConsensusMessage::decode(bytes.as_slice())
                .expect("decode protobuf bytes");
            let back = crate::grpc::message_from_proto(decoded).expect("decode protobuf DTO");
            assert_eq!(
                msg, back,
                "protobuf round-trip must be lossless for {msg:?}"
            );
        }
    }
}
