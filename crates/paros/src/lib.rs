//! `paros` — the Paxos node library.
//!
//! This is the user-facing entry point. It re-exports the sans-IO
//! [`paros_core`] state machine and adds the **driver** that owns it and
//! performs I/O — the etcd-raft `Node` layer to `paros_core`'s `ColocatedNode`.
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

pub use audit::{Audit, Deployment, HistoryPage, NoAudit, StorageFaultDecision};
pub use corruption::{
    CorruptionVerdict, EntryEvidence, IntegrityFault, RecoveryCase, SlotRecord, WitnessStatus,
    classify_log, decide,
};
pub use driver::{
    DriverTunables, RunError, command_hash, parse_addr, registration_history_hash, run_node,
};
pub use grpc::{
    Compact, CompactAck, EdgeRejection, InspectReply, InspectRequest, ParosClient,
    ParosInternalClient, ParosMatchmakerClient, Propose, ProposeAck, Read, ReadAck, Reconfigure,
    ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck, RetireRequest,
    WireGarbageCollect, WireGarbageCollectAck, WireMatchReply, WireMatchRequest,
    WireReconfigureReply, WireReconfigureRequest, garbage_collect_ack_from_wire,
    garbage_collect_from_wire, match_reply_from_wire, match_request_from_wire,
    reconfigure_reply_from_wire, reconfigure_request_from_wire, wire_garbage_collect,
    wire_garbage_collect_ack, wire_match_reply, wire_match_request, wire_reconfigure_reply,
    wire_reconfigure_request,
};
pub use hooks::{DriverHooks, HandoffContext, NoHooks, Reply, Seam};
pub use matchmaker::{
    MatchmakerStorage, MemMatchmakerStorage, config_hash, matchmaker_storage_contract_suite,
    reconfigure_kind, reconfigure_reply_kind, run_matchmaker,
};
pub use storage::{
    MemStorage, MetadataFault, NodeStorage, SNAP_CHUNK_BYTES, StorageError, StorageRecord,
    WriteOutcome, snap_chunk_count, storage_contract_suite,
};

pub use paros_core::acceptor::Acceptor;
pub use paros_core::proposer::Proposer;
pub use paros_core::replica::Replica;
pub use paros_core::{
    AcceptorConfig, AcceptorWrite, Audience, Ballot, ClientId, ClientSeq, ColocatedNode, Command,
    Config, Control, Decree, DecreeRecord, Entry, GcAck, GcOutcome, GcRequest, GcStep,
    HANDOFF_BATCH, HANDOFF_FENCE_ELECTIONS, HEARTBEAT_TICKS, Handoff, HandoffCounters, HardState,
    LEADER_RECOVERY_BATCH, LeadershipOrigin, MatchOutcome, MatchRefusal, MatchReply, MatchRequest,
    MatchStep, Matchmaker, MatchmakerConfig, MatchmakerGeneration, MatchmakerHardState,
    MatchmakerId, MatchmakerPhase, MatchmakerReady, MatchmakerReconfigurer, MatchmakerSet,
    MatchmakerWriteOp, Message, MustSync, NodeId, NodeRole, PROMISE_BATCH, PendingBootstrap,
    ProposeResult, QuorumSystem, REGISTRY_PAGE, ReadIndexResult, ReadState, Ready,
    ReconfigureRefusal, ReconfigureReply, ReconfigureRequest, ReconfigureResult, ReconfigurerPhase,
    ReconfigurerStep, Registration, RegistrationKind, RegistryStorage, SessionEntry, Slot,
    StartRefusal, Storage, Value, WriteOp, command_fingerprint,
};

pub use paros_core::REPAIR_TIMEOUT_ELECTIONS;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use paros_core::{
        Ballot, ClientId, ClientSeq, Command, Control, Entry, Message, NodeId, Slot, Value,
    };
    use prost::Message as ProstMessage;

    /// One representative of every `Message` variant.
    #[allow(clippy::too_many_lines)] // Exhaustive wire fixture: one literal per variant.
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
                reply_to: NodeId(1),
                leader: NodeId(1),
                ballot,
                from_slot: Slot(5),
                config: None,
            },
            // A matchmaker deployment's `Prepare` carries the registered
            // configuration; the plain one above carries none.
            Message::Prepare {
                reply_to: NodeId(1),
                leader: NodeId(1),
                ballot,
                from_slot: Slot(5),
                config: Some(paros_core::AcceptorConfig::new(
                    vec![NodeId(3), NodeId(1), NodeId(4)],
                    paros_core::QuorumSystem::Majority,
                )),
            },
            Message::Promise {
                from: NodeId(1),
                ballot,
                from_slot: Slot(5),
                accepted,
                faulty: BTreeMap::from([(Slot(6), ballot)]),
                next_from_slot: None,
            },
            Message::Accept {
                reply_to: NodeId(2),
                leader: NodeId(2),
                ballot,
                slot: Slot(6),
                command: command.clone(),
            },
            // A reply address that is not the leader: the shape a proxied
            // Phase 2 would put on the wire, and the only case that encodes
            // the optional `leader` field at all.
            Message::Accept {
                reply_to: NodeId(5),
                leader: NodeId(2),
                ballot,
                slot: Slot(6),
                command: command.clone(),
            },
            Message::Accepted {
                from: NodeId(2),
                ballot,
                slot: Slot(6),
                vhash: 17,
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
                // The #94 session ledger rides beside the opaque bytes and must
                // survive the wire round trip record-for-record.
                sessions: vec![
                    (ClientId(1), ClientSeq(2), Slot(3)),
                    (ClientId(4), ClientSeq(0), Slot(5)),
                ],
            },
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: Some(Slot(2)),
                seq: 9,
                config: None,
            },
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: Some(Slot(2)),
                seq: 11,
                config: Some(paros_core::AcceptorConfig::new(
                    vec![NodeId(0), NodeId(2)],
                    paros_core::QuorumSystem::Majority,
                )),
            },
            // The empty watermark is its own variant of the beat, and the one the
            // wire encoding used to be unable to say (#56): a leader that has
            // chosen nothing is not a leader that has chosen slot 0.
            Message::Heartbeat {
                from: NodeId(0),
                ballot,
                commit: None,
                seq: 10,
                config: None,
            },
            Message::HeartbeatAck {
                from: NodeId(1),
                ballot,
                seq: 9,
                chosen: Some(Slot(4)),
            },
            // The driver-terminal snap-repair trio carries the configuration
            // identity too (guarded by the driver on receipt, never asserted).
            Message::SnapAck {
                from: NodeId(2),
                at_index: Slot(4),
            },
            Message::SnapChunkRequest {
                from: NodeId(1),
                at_index: Slot(4),
                chunks: vec![0, 3],
            },
            Message::SnapChunkResponse {
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
                config: Some(paros_core::AcceptorConfig::new(
                    vec![NodeId(1), NodeId(2), NodeId(3)],
                    paros_core::QuorumSystem::Majority,
                )),
            },
            // The empty tail: a fully settled leader hands over the frontier
            // and nothing else.
            Message::Relinquish {
                from: NodeId(3),
                to: NodeId(2),
                ballot,
                from_slot: Slot(9),
                next_slot: Slot(9),
                decided: BTreeMap::new(),
                pending: BTreeMap::new(),
                config: None,
            },
        ]
    }

    /// The matchmaker contract: every outcome and refusal round-trips through
    /// its typed protobuf losslessly.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn matchmaker_contract_round_trips() {
        use paros_core::{
            AcceptorConfig, GcAck, GcRequest, MatchOutcome, MatchRefusal, MatchReply, MatchRequest,
            MatchmakerGeneration, MatchmakerId, MatchmakerPhase, MatchmakerSet, PendingBootstrap,
            QuorumSystem, ReconfigureReply, ReconfigureRequest, Registration,
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
        let g = MatchmakerGeneration;
        let set = |generation: u64, members: &[u64]| {
            MatchmakerSet::new(
                g(generation),
                members.iter().copied().map(MatchmakerId).collect(),
            )
        };
        for request in [
            MatchRequest::new(NodeId(4), ballot(7, 4), config(&[0, 1, 2]), g(0)),
            MatchRequest::reconfigure(NodeId(4), ballot(8, 4), config(&[1, 2, 3]), g(3)),
        ] {
            let wire = crate::grpc::wire_match_request(&request);
            let bytes = wire.encode_to_vec();
            let decoded = crate::grpc::WireMatchRequest::decode(bytes.as_slice()).expect("decode");
            assert_eq!(
                crate::grpc::match_request_from_wire(decoded).expect("request"),
                request
            );
        }

        let reply = |matchmaker: u64, ballot: Ballot, outcome: MatchOutcome| MatchReply {
            matchmaker: MatchmakerId(matchmaker),
            to: NodeId(4),
            ballot,
            generation: g(2),
            outcome,
        };
        let replies = vec![
            reply(
                1,
                ballot(7, 4),
                MatchOutcome::Registered {
                    history: BTreeMap::from([
                        (ballot(2, 1), Registration::belief(config(&[0, 1, 2]))),
                        (
                            ballot(5, 3),
                            Registration::reconfiguration(config(&[1, 2, 3, 4])),
                        ),
                    ]),
                    gc_watermark: ballot(2, 1),
                    effective: Some((ballot(5, 3), config(&[1, 2, 3, 4]))),
                    from_ballot: ballot(2, 1),
                    next_from_ballot: Some(ballot(6, 1)),
                },
            ),
            reply(
                2,
                ballot(7, 4),
                MatchOutcome::Registered {
                    history: BTreeMap::new(),
                    gc_watermark: Ballot::zero(),
                    effective: None,
                    from_ballot: Ballot::zero(),
                    next_from_ballot: None,
                },
            ),
            reply(
                0,
                ballot(7, 4),
                MatchOutcome::Refused(MatchRefusal::Stale {
                    highest: ballot(9, 2),
                }),
            ),
            reply(
                0,
                ballot(1, 4),
                MatchOutcome::Refused(MatchRefusal::BelowWatermark {
                    watermark: ballot(3, 1),
                }),
            ),
            reply(
                0,
                ballot(1, 4),
                MatchOutcome::Refused(MatchRefusal::Stopped { successor: None }),
            ),
            reply(
                0,
                ballot(1, 4),
                MatchOutcome::Refused(MatchRefusal::Stopped {
                    successor: Some(set(3, &[0, 4, 5])),
                }),
            ),
            reply(
                0,
                ballot(1, 4),
                MatchOutcome::Refused(MatchRefusal::Generation {
                    current: set(5, &[1, 2]),
                }),
            ),
            reply(
                0,
                ballot(1, 4),
                MatchOutcome::Refused(MatchRefusal::Inactive),
            ),
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

        let ack = GcAck {
            matchmaker: MatchmakerId(2),
            generation: g(1),
            applied: true,
            watermark: ballot(3, 1),
        };
        let wire = crate::grpc::wire_garbage_collect_ack(&ack);
        let decoded = crate::grpc::WireGarbageCollectAck::decode(wire.encode_to_vec().as_slice())
            .expect("decode");
        assert_eq!(
            crate::grpc::garbage_collect_ack_from_wire(decoded).expect("ack"),
            ack
        );
        let gc = GcRequest {
            from: NodeId(4),
            generation: g(1),
            watermark: ballot(3, 1),
        };
        let wire = crate::grpc::wire_garbage_collect(&gc);
        let decoded = crate::grpc::WireGarbageCollect::decode(wire.encode_to_vec().as_slice())
            .expect("decode");
        assert_eq!(
            crate::grpc::garbage_collect_from_wire(decoded).expect("gc"),
            gc
        );

        // The handover contract (#125): every request and reply kind.
        let bootstrap = PendingBootstrap {
            set: set(1, &[0, 1, 3]),
            gc_watermark: ballot(2, 1),
            history: BTreeMap::from([(ballot(5, 3), Registration::belief(config(&[1, 2])))]),
            effective: Some((ballot(4, 2), config(&[0, 1, 2]))),
        };
        for request in [
            ReconfigureRequest::Stop {
                from: NodeId(4),
                generation: g(0),
            },
            ReconfigureRequest::Bootstrap {
                from: NodeId(4),
                bootstrap: bootstrap.clone(),
            },
            ReconfigureRequest::DecreePrepare {
                from: NodeId(4),
                generation: g(0),
                ballot: ballot(1, 4),
            },
            ReconfigureRequest::DecreeAccept {
                from: NodeId(4),
                generation: g(0),
                ballot: ballot(1, 4),
                members: vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(3)],
            },
            ReconfigureRequest::Chosen {
                from: NodeId(4),
                generation: g(0),
                successor: set(1, &[0, 1, 3]),
            },
        ] {
            let wire = crate::grpc::wire_reconfigure_request(&request);
            let decoded =
                crate::grpc::WireReconfigureRequest::decode(wire.encode_to_vec().as_slice())
                    .expect("decode");
            assert_eq!(
                crate::grpc::reconfigure_request_from_wire(decoded).expect("request"),
                request
            );
        }
        for reply in [
            ReconfigureReply::Stopped {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                gc_watermark: ballot(2, 1),
                history: bootstrap.history.clone(),
                effective: bootstrap.effective.clone(),
                successor: Some(set(1, &[0, 1, 3])),
                decree_promised: ballot(3, 2),
            },
            ReconfigureReply::Bootstrapped {
                matchmaker: MatchmakerId(3),
                set: set(1, &[0, 1, 3]),
            },
            ReconfigureReply::Promised {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                ballot: ballot(1, 4),
                vote: Some((ballot(1, 2), vec![MatchmakerId(1), MatchmakerId(2)])),
            },
            ReconfigureReply::Promised {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                ballot: ballot(1, 4),
                vote: None,
            },
            ReconfigureReply::Accepted {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                ballot: ballot(1, 4),
            },
            ReconfigureReply::Nacked {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                ballot: ballot(1, 4),
                promised: ballot(2, 5),
            },
            ReconfigureReply::Learned {
                matchmaker: MatchmakerId(1),
                generation: g(0),
                activated: true,
                at: g(1),
            },
            ReconfigureReply::Refused {
                matchmaker: MatchmakerId(1),
                current: set(2, &[1, 5]),
                phase: MatchmakerPhase::Stopped,
                successor: Some(set(3, &[5, 6])),
            },
        ] {
            let wire = crate::grpc::wire_reconfigure_reply(&reply);
            let decoded =
                crate::grpc::WireReconfigureReply::decode(wire.encode_to_vec().as_slice())
                    .expect("decode");
            assert_eq!(
                crate::grpc::reconfigure_reply_from_wire(decoded).expect("reply"),
                reply
            );
        }
    }

    /// The `leader` field of a `Prepare`/`Accept` is absent from the wire
    /// whenever it would merely repeat the reply address — which is every
    /// message paros sends today, on a plain deployment and on a matchmaker
    /// one alike. This pins the encoding against the day a proxied Phase 2
    /// starts populating it.
    #[test]
    fn a_leader_that_is_the_reply_address_stays_off_the_wire() {
        use crate::grpc::internal::consensus_message::Kind;

        for msg in every_variant() {
            let wire = crate::grpc::message_to_proto(&msg).expect("encode protobuf DTO");
            match (msg, wire.kind) {
                (
                    Message::Prepare {
                        reply_to, leader, ..
                    },
                    Some(Kind::Prepare(wire)),
                ) => {
                    assert_eq!(wire.leader.is_none(), reply_to == leader);
                }
                (
                    Message::Accept {
                        reply_to, leader, ..
                    },
                    Some(Kind::Accept(wire)),
                ) => {
                    assert_eq!(wire.leader.is_none(), reply_to == leader);
                }
                _ => {}
            }
        }
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
