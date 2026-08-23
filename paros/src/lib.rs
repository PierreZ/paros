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

mod crash;
mod driver;
mod storage;

pub use crash::{CrashSeam, NoCrash, Seam};
pub use driver::{
    Compact, CompactAck, EV_APPLIED, EV_BOOTED, EV_CHOSEN, EV_CHOSEN_GAP, EV_COMPACTED, EV_CRASHED,
    EV_GAP_FILLED, EV_LEADER, EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE, EV_NODE_TICK, EV_PERSIST,
    EV_PREPARE_BELOW_FLOOR, EV_PROPOSE_DEDUP_ACK, EV_RECOVERED, EV_SNAPSHOT_INSTALLED, EV_SYNCED,
    Paros, Perturbations, Propose, ProposeAck, Read, ReadAck, WLTOKEN_PAROS, is_seam_crash,
    parse_addr, run_node,
};
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
        // A control command in the accepted suffix exercises `Command::Control`
        // serde alongside the client-entry case.
        let control = Command::Control(Control::Truncate { up_to: Slot(3) });
        let mut accepted = BTreeMap::new();
        accepted.insert(Slot(5), (ballot, command.clone()));
        accepted.insert(Slot(6), (ballot, control));
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
                commit: Slot(2),
                seq: 9,
            },
            Message::HeartbeatAck {
                from: NodeId(1),
                ballot,
                seq: 9,
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
