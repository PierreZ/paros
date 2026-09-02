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
//!
//! [`run_matchmaker`] is the same shape for the **matchmaker** role (the
//! per-ballot configuration registry of Matchmaker Paxos), driven over
//! [`MatchmakerStorage`]. It is opt-in: a deployment without matchmakers never
//! runs it, and [`run_node`] does not know it exists.

mod audit;
mod corruption;
mod driver;
mod grpc;
mod hooks;
mod matchmaker;
mod storage;

pub use audit::{Audit, NoAudit, StorageFaultDecision};
pub use corruption::{
    CorruptionVerdict, EntryEvidence, IntegrityFault, RecoveryCase, SlotRecord, WitnessStatus,
    classify_log, decide,
};
pub use driver::{
    DriverTunables, EV_APPLIED, EV_AUTHORITY_INSTALLED, EV_AUTHORITY_RELINQUISHED, EV_BOOTED,
    EV_CHOSEN, EV_CHOSEN_GAP, EV_CLIENT_REPLY_DROPPED, EV_COMPACTED, EV_CRASHED,
    EV_DUPLICATE_SUPPRESSED, EV_ELECTION_TIMEOUT_EXTREME, EV_GAP_FILLED, EV_HANDOFF_FENCE_EXPIRED,
    EV_HANDOFF_REFUSED, EV_LEADER, EV_LEADERSHIP_RESIGNED, EV_MSG_RECV, EV_MSG_SENT, EV_NODE_STATE,
    EV_NODE_TICK, EV_PERSIST, EV_PREPARE_BELOW_FLOOR, EV_PROPOSE_DEDUP_ACK, EV_QUORUM_LOST,
    EV_RECOVERED, EV_RESEND_SKIPPED, EV_SEND_DROPPED, EV_SEND_DUPLICATED, EV_SNAPSHOT_INSTALLED,
    EV_SNAPSHOT_MID_ELECTION, EV_SNAPSHOT_OFFERED, EV_STORAGE_FAULT, EV_SYNCED, RunError,
    command_hash, parse_addr, run_node,
};
pub use grpc::{
    Compact, CompactAck, EdgeRejection, InspectReply, InspectRequest, ParosClient,
    ParosInternalClient, ParosMatchmakerClient, Propose, ProposeAck, Read, ReadAck,
    WireGarbageCollect, WireGarbageCollectAck, WireMatchReply, WireMatchRequest,
    garbage_collect_ack_from_wire, garbage_collect_from_wire, match_reply_from_wire,
    match_request_from_wire, proposal_checksum, wire_garbage_collect, wire_garbage_collect_ack,
    wire_match_reply, wire_match_request,
};
pub use hooks::{DriverHooks, HandoffContext, NoHooks, Reply, Seam};
pub use matchmaker::{
    MatchmakerStorage, MemMatchmakerStorage, config_hash, matchmaker_storage_contract_suite,
    run_matchmaker,
};
pub use storage::{
    MemStorage, MetadataFault, NodeStorage, SNAP_CHUNK_BYTES, StorageError, StorageRecord,
    WriteOutcome, snap_chunk_count, storage_contract_suite,
};

pub use paros_core::{
    AcceptorConfig, Ballot, ClientId, ClientSeq, Command, Config, ConfigId, Control, Entry,
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, Handoff, HandoffCounters, HardState,
    LEADER_RECOVERY_BATCH, LeadershipOrigin, MatchOutcome, MatchRefusal, MatchReply, MatchRequest,
    Matchmaker, MatchmakerHardState, MatchmakerId, MatchmakerReady, MatchmakerWriteOp, Message,
    MustSync, NodeId, NodeRole, PROMISE_BATCH, ProposeResult, QuorumSystem, RawNode,
    ReadIndexResult, ReadState, Ready, RegistryStorage, SessionEntry, Slot, Storage, Value,
    WriteOp, command_fingerprint,
};

pub use paros_core::REPAIR_TIMEOUT_ELECTIONS;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use paros_core::{
        Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, Message, NodeId, Slot,
        Value,
    };
    use prost::Message as ProstMessage;

    /// One representative of every `Message` variant.
    #[allow(clippy::too_many_lines)] // Exhaustive wire fixture: one literal per variant.
    fn every_variant() -> Vec<Message> {
        let ballot = Ballot {
            round: 7,
            node: NodeId(3),
        };
        let config_id = ConfigId(42);
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
                config_id,
                from: NodeId(1),
                ballot,
                from_slot: Slot(5),
            },
            Message::Promise {
                config_id,
                from: NodeId(1),
                ballot,
                from_slot: Slot(5),
                accepted,
                faulty: BTreeMap::from([(Slot(6), ballot)]),
                next_from_slot: None,
            },
            Message::Accept {
                config_id,
                from: NodeId(2),
                ballot,
                slot: Slot(6),
                command: command.clone(),
            },
            Message::Accepted {
                config_id,
                from: NodeId(2),
                ballot,
                slot: Slot(6),
                vhash: 17,
            },
            Message::Nack {
                config_id,
                from: NodeId(2),
                ballot,
                promised: Ballot {
                    round: 9,
                    node: NodeId(4),
                },
                slot: Slot(6),
            },
            Message::Commit {
                config_id,
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
                config_id,
                from: NodeId(0),
                ballot,
                chosen_index: Slot(5),
                snapshot: Value(vec![9, 9, 9]),
                // The #94 session ledger rides beside the opaque bytes and must
                // survive the wire round trip record-for-record.
                sessions: vec![
                    (ClientId(1), ClientSeq(2), Slot(3)),
                    (ClientId(4), ClientSeq(0), Slot(5)),
                ],
            },
            Message::CheckLeader { from: NodeId(0) },
            Message::Heartbeat {
                config_id,
                from: NodeId(0),
                ballot,
                commit: Some(Slot(2)),
                seq: 9,
            },
            // The empty watermark is its own variant of the beat, and the one the
            // wire encoding used to be unable to say (#56): a leader that has
            // chosen nothing is not a leader that has chosen slot 0.
            Message::Heartbeat {
                config_id,
                from: NodeId(0),
                ballot,
                commit: None,
                seq: 10,
            },
            Message::HeartbeatAck {
                config_id,
                from: NodeId(1),
                ballot,
                seq: 9,
            },
            // The driver-terminal snap-repair trio carries the configuration
            // identity too (guarded by the driver on receipt, never asserted).
            Message::SnapAck {
                config_id,
                from: NodeId(2),
                at_index: Slot(4),
            },
            Message::SnapChunkRequest {
                config_id,
                from: NodeId(1),
                at_index: Slot(4),
                chunks: vec![0, 3],
            },
            Message::SnapChunkResponse {
                config_id,
                from: NodeId(0),
                at_index: Slot(4),
                chunks: vec![(0, Value(vec![1, 2])), (3, Value(vec![]))],
            },
            // Cooperative leader handoff: the intended successor, the
            // transferred allocator frontier, and both halves of the tail —
            // `pending` deliberately carries no per-slot ballot on the wire
            // (it is the transferred ballot by construction), so the round
            // trip is what pins that re-derivation.
            Message::Relinquish {
                config_id,
                from: NodeId(3),
                to: NodeId(1),
                ballot,
                from_slot: Slot(4),
                next_slot: Slot(7),
                decided: BTreeMap::from([(
                    Slot(4),
                    (
                        Ballot {
                            round: 6,
                            node: NodeId(2),
                        },
                        Command::Control(Control::Snap { at_index: Slot(4) }),
                    ),
                )]),
                pending: BTreeMap::from([
                    (
                        Slot(5),
                        Command::User(Entry {
                            client: ClientId(8),
                            seq: ClientSeq(3),
                            value: Value(vec![4, 5]),
                        }),
                    ),
                    (Slot(6), Command::Control(Control::Noop)),
                ]),
            },
            // The empty tail: a fully settled leader hands over the frontier
            // and nothing else.
            Message::Relinquish {
                config_id,
                from: NodeId(3),
                to: NodeId(2),
                ballot,
                from_slot: Slot(9),
                next_slot: Slot(9),
                decided: BTreeMap::new(),
                pending: BTreeMap::new(),
            },
        ]
    }

    /// The matchmaker contract: every outcome and refusal round-trips through
    /// its typed protobuf losslessly.
    #[test]
    fn matchmaker_contract_round_trips() {
        use paros_core::{
            AcceptorConfig, MatchOutcome, MatchRefusal, MatchReply, MatchRequest, MatchmakerId,
            QuorumSystem,
        };
        let ballot = |round: u64, node: u64| Ballot {
            round,
            node: NodeId(node),
        };
        let config = |members: &[u64]| {
            AcceptorConfig::new(
                members.iter().map(|n| NodeId(*n)).collect(),
                QuorumSystem::Majority,
            )
        };
        let request = MatchRequest::new(NodeId(4), ballot(7, 4), config(&[0, 1, 2]));
        let wire = crate::grpc::wire_match_request(&request);
        let bytes = wire.encode_to_vec();
        let decoded = crate::grpc::WireMatchRequest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(
            crate::grpc::match_request_from_wire(decoded).expect("request"),
            request
        );

        let replies = vec![
            MatchReply {
                matchmaker: MatchmakerId(1),
                to: NodeId(4),
                ballot: ballot(7, 4),
                outcome: MatchOutcome::Registered {
                    history: BTreeMap::from([
                        (ballot(2, 1), config(&[0, 1, 2])),
                        (ballot(5, 3), config(&[1, 2, 3, 4])),
                    ]),
                    gc_watermark: ballot(2, 1),
                },
            },
            MatchReply {
                matchmaker: MatchmakerId(2),
                to: NodeId(4),
                ballot: ballot(7, 4),
                outcome: MatchOutcome::Registered {
                    history: BTreeMap::new(),
                    gc_watermark: Ballot::zero(),
                },
            },
            MatchReply {
                matchmaker: MatchmakerId(0),
                to: NodeId(4),
                ballot: ballot(7, 4),
                outcome: MatchOutcome::Refused(MatchRefusal::Stale {
                    highest: ballot(9, 2),
                }),
            },
            MatchReply {
                matchmaker: MatchmakerId(0),
                to: NodeId(4),
                ballot: ballot(1, 4),
                outcome: MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                    watermark: ballot(3, 1),
                }),
            },
        ];
        for reply in replies {
            let wire = crate::grpc::wire_match_reply(&reply);
            let bytes = wire.encode_to_vec();
            let decoded = crate::grpc::WireMatchReply::decode(bytes.as_slice()).expect("decode");
            assert_eq!(
                crate::grpc::match_reply_from_wire(decoded).expect("reply"),
                reply,
                "matchmaker reply round-trip must be lossless for {reply:?}"
            );
        }

        let ack = crate::grpc::wire_garbage_collect_ack(MatchmakerId(2), ballot(3, 1));
        let decoded = crate::grpc::WireGarbageCollectAck::decode(ack.encode_to_vec().as_slice())
            .expect("decode");
        assert_eq!(
            crate::grpc::garbage_collect_ack_from_wire(decoded).expect("ack"),
            (MatchmakerId(2), ballot(3, 1))
        );
        let gc = crate::grpc::wire_garbage_collect(NodeId(4), ballot(3, 1));
        let decoded =
            crate::grpc::WireGarbageCollect::decode(gc.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(
            crate::grpc::garbage_collect_from_wire(decoded).expect("gc"),
            (NodeId(4), ballot(3, 1))
        );
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
