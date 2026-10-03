//! The wire contracts' round-trip tests: every domain value crosses its
//! typed protobuf losslessly, and malformed wire input is refused.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, Command, Control, Entry, Generation, JournalState, Message, NodeId, Party,
    ProxyId, Seq, Slot, Value,
};
use prost::Message as ProstMessage;

use super::consensus::message_from_proto;
use super::{common, internal};

#[test]
fn protobuf_rejects_a_missing_message_kind() {
    let result = message_from_proto(internal::ConsensusMessage {
        kind: None,
        journal: 0,
        tenant: 0,
    });
    assert!(matches!(result, Err("missing Paxos message kind")));
}

#[test]
fn protobuf_rejects_duplicate_slots_in_a_suffix() {
    let entry = internal::SlotCommand {
        slot: 4,
        ballot: Some(common::Ballot { round: 2, node: 1 }),
        command: Some(internal::Command {
            kind: Some(internal::command::Kind::Control(internal::ControlCommand {
                kind: Some(internal::control_command::Kind::Noop(internal::Noop {})),
            })),
        }),
    };
    let wire = internal::ConsensusMessage {
        kind: Some(internal::consensus_message::Kind::CatchUpResponse(
            internal::CatchUpResponse {
                from: 1,
                entries: vec![entry.clone(), entry],
            },
        )),
        journal: 0,
        tenant: 0,
    };

    assert!(matches!(
        message_from_proto(wire),
        Err("duplicate slot in message")
    ));
}

/// One representative of every `Message` variant.
#[allow(clippy::too_many_lines)] // Exhaustive wire fixture: one literal per variant.
fn every_variant() -> Vec<Message> {
    let ballot = Ballot {
        round: 7,
        node: NodeId(3),
    };
    let entry = Entry {
        generation: Generation(4),
        owner: ClientId(1),
        seq: Seq(2),
        records: vec![Value(vec![1, 2, 3]), Value(Vec::new())],
    };
    let command = Command::Write(entry.clone());
    // Control commands in the accepted suffix exercise every protobuf
    // control variant alongside the client-write case.
    let control = Command::Control(Control::Truncate {
        generation: Generation(2),
        owner: ClientId(9),
        up_to: Seq(3),
    });
    let claim = Command::Control(Control::SetLeader {
        expected: Generation(3),
        owner: ClientId(9),
    });
    let mut accepted = BTreeMap::new();
    accepted.insert(Slot(5), (ballot, command.clone()));
    accepted.insert(Slot(6), (ballot, control));
    accepted.insert(Slot(8), (ballot, claim));
    accepted.insert(Slot(7), (ballot, Command::Control(Control::Noop)));
    let mut catchup = BTreeMap::new();
    catchup.insert(Slot(4), (ballot, command.clone()));
    vec![
        Message::Prepare {
            reply_to: NodeId(1),
            ballot,
            from_slot: Slot(5),
            config: None,
        },
        // A matchmaker deployment's `Prepare` carries the registered
        // configuration; the plain one above carries none.
        Message::Prepare {
            reply_to: NodeId(1),
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
            reply_to: Party::Node(NodeId(2)),
            leader: NodeId(2),
            ballot,
            slot: Slot(6),
            command: command.clone(),
            config: None,
        },
        // A reply address that is not the leader: the shape a handoff
        // successor's re-send puts on the wire, and a case that encodes
        // the optional `leader` field.
        Message::Accept {
            reply_to: Party::Node(NodeId(5)),
            leader: NodeId(2),
            ballot,
            slot: Slot(6),
            command: command.clone(),
            config: None,
        },
        // A delegated round (#142): the reply party is a proxy, and on a
        // matchmaker deployment the delegation carries the configuration.
        Message::Accept {
            reply_to: Party::Proxy(ProxyId(1)),
            leader: NodeId(2),
            ballot,
            slot: Slot(6),
            command: command.clone(),
            config: Some(paros_core::AcceptorConfig::new(
                vec![NodeId(1), NodeId(2), NodeId(3)],
                paros_core::QuorumSystem::Majority,
            )),
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
            from: Party::Node(NodeId(0)),
            ballot,
            slot: Slot(6),
            command: command.clone(),
        },
        // A proxy leader's decision (#142).
        Message::Commit {
            from: Party::Proxy(ProxyId(0)),
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
        Message::TrimmedTo {
            from: NodeId(0),
            point: Slot(6),
            // The journal state rides with the trim point (#204) and must
            // survive the wire round trip scalar for scalar.
            state: JournalState {
                owner: Some(ClientId(4)),
                generation: Generation(2),
                next_seq: Seq(7),
                first_seq: Seq(3),
            },
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
                    Command::Control(Control::Truncate {
                        generation: Generation(2),
                        owner: ClientId(9),
                        up_to: Seq(2),
                    }),
                ),
            )]),
            pending: BTreeMap::from([
                (
                    Slot(5),
                    Command::Write(Entry {
                        generation: Generation(1),
                        owner: ClientId(8),
                        seq: Seq(3),
                        records: vec![Value(vec![4, 5])],
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
        MatchRequest::probe(NodeId(5), ballot(9, 5), config(&[0, 1, 2]), g(1)),
    ] {
        let wire = super::matchmaker_codec::wire_match_request(&request);
        let bytes = wire.encode_to_vec();
        let decoded = super::WireMatchRequest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(
            super::matchmaker_codec::match_request_from_wire(decoded).expect("request"),
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
            3,
            ballot(8, 5),
            MatchOutcome::Probed {
                effective: Some((ballot(5, 3), config(&[3, 4, 5]))),
            },
        ),
        reply(3, ballot(8, 5), MatchOutcome::Probed { effective: None }),
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
        let wire = super::matchmaker_codec::wire_match_reply(&reply);
        let bytes = wire.encode_to_vec();
        let decoded = super::WireMatchReply::decode(bytes.as_slice()).expect("decode");
        assert_eq!(
            super::matchmaker_codec::match_reply_from_wire(decoded).expect("reply"),
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
    let wire = super::matchmaker_codec::wire_garbage_collect_ack(&ack);
    let decoded =
        super::WireGarbageCollectAck::decode(wire.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(
        super::matchmaker_codec::garbage_collect_ack_from_wire(decoded).expect("ack"),
        ack
    );
    let gc = GcRequest {
        from: NodeId(4),
        generation: g(1),
        watermark: ballot(3, 1),
    };
    let wire = super::matchmaker_codec::wire_garbage_collect(&gc);
    let decoded =
        super::WireGarbageCollect::decode(wire.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(
        super::matchmaker_codec::garbage_collect_from_wire(decoded).expect("gc"),
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
        let wire = super::matchmaker_codec::wire_reconfigure_request(&request);
        let decoded =
            super::WireReconfigureRequest::decode(wire.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(
            super::matchmaker_codec::reconfigure_request_from_wire(decoded).expect("request"),
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
        let wire = super::matchmaker_codec::wire_reconfigure_reply(&reply);
        let decoded =
            super::WireReconfigureReply::decode(wire.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(
            super::matchmaker_codec::reconfigure_reply_from_wire(decoded).expect("reply"),
            reply
        );
    }
}

/// The `leader` field of an `Accept` is absent from the wire whenever it
/// would merely repeat the reply address — which is every message paros
/// sends today, on a plain deployment and on a matchmaker one alike. This
/// pins the encoding against the day a proxied Phase 2 starts populating
/// it. (A `Prepare` has no such field: Phase 1 is never proxied.)
#[test]
fn a_leader_that_is_the_reply_address_stays_off_the_wire() {
    use super::internal::consensus_message::Kind;

    for msg in every_variant() {
        let wire = super::message_to_proto(&msg).expect("encode protobuf DTO");
        if let (
            Message::Accept {
                reply_to, leader, ..
            },
            Some(Kind::Accept(wire)),
        ) = (msg, wire.kind)
        {
            assert_eq!(wire.leader.is_none(), reply_to == Party::Node(leader));
        }
    }
}

/// Every domain variant must round-trip through the typed protobuf contract
/// losslessly before the driver is allowed to put it on the wire.
#[test]
fn message_protobuf_round_trips() {
    for msg in every_variant() {
        let wire = super::message_to_proto(&msg).expect("encode protobuf DTO");
        let bytes = wire.encode_to_vec();
        let decoded = super::internal::ConsensusMessage::decode(bytes.as_slice())
            .expect("decode protobuf bytes");
        let back = message_from_proto(decoded).expect("decode protobuf DTO");
        assert_eq!(
            msg, back,
            "protobuf round-trip must be lossless for {msg:?}"
        );
    }
}
