//! The trace and audit fields, pinned through the public helpers: the
//! stable digests, the labels, and the route of every consensus message.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::contract::Party;
use paros_core::journal::{Command, Control, Entry, JournalState, LeaderUuid, Seq};
use paros_core::matchmaker::{
    BootstrapPage, MatchmakerPhase, ReconfigureReply, ReconfigureRequest, Registration,
    RegistrationKind,
};
use paros_core::membership::{
    AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem,
};
use paros_core::node::{ReconfigureRefusal, ReconfigureResult};
use paros_core::{Ballot, Message, NodeId, Slot, Value};
use paros_wire::{internal, message_to_proto};

use super::*;

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn words(words: &[u64]) -> Vec<u8> {
    words.iter().flat_map(|w| w.to_le_bytes()).collect()
}

fn ballot(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

// ---- value_hash ----

#[test]
fn value_hash_is_fnv1a_64() {
    // The published FNV-1a 64-bit test vectors.
    assert_eq!(value_hash(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(value_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(value_hash(b"foobar"), 0x8594_4171_f739_67e8);
}

// ---- config_hash ----

#[test]
fn config_hash_distinguishes_membership_and_is_order_independent() {
    let a = AcceptorConfig::new(nodes(&[0, 1, 2]), QuorumSystem::Majority);
    let b = AcceptorConfig::new(nodes(&[2, 1, 0]), QuorumSystem::Majority);
    let c = AcceptorConfig::new(nodes(&[0, 1]), QuorumSystem::Majority);
    assert_eq!(config_hash(&a), config_hash(&b));
    assert_ne!(config_hash(&a), config_hash(&c));
}

#[test]
fn config_hash_folds_the_length_the_members_and_the_quorum_tag() {
    let majority = AcceptorConfig::new(nodes(&[4, 5, 6]), QuorumSystem::Majority);
    let mut bytes = words(&[3, 4, 5, 6]);
    bytes.push(0);
    assert_eq!(config_hash(&majority), value_hash(&bytes));

    let flexible = AcceptorConfig::new(nodes(&[4, 5, 6]), QuorumSystem::Flexible { q1: 2, q2: 2 });
    let mut bytes = words(&[3, 4, 5, 6]);
    bytes.push(1);
    bytes.extend(words(&[2, 2]));
    assert_eq!(config_hash(&flexible), value_hash(&bytes));
}

#[test]
fn config_hash_folds_the_ack_count_of_a_partial_column_only() {
    let full = AcceptorConfig::new(
        nodes(&[0, 1, 2, 3]),
        QuorumSystem::Grid {
            rows: 2,
            cols: 2,
            acks: 2,
        },
    );
    let mut bytes = words(&[4, 0, 1, 2, 3]);
    bytes.push(2);
    bytes.extend(words(&[2, 2]));
    assert_eq!(config_hash(&full), value_hash(&bytes));

    let partial = AcceptorConfig::new(
        nodes(&[0, 1, 2, 3, 4, 5]),
        QuorumSystem::Grid {
            rows: 3,
            cols: 2,
            acks: 2,
        },
    );
    let mut bytes = words(&[6, 0, 1, 2, 3, 4, 5]);
    bytes.push(3);
    bytes.extend(words(&[3, 2, 2]));
    assert_eq!(config_hash(&partial), value_hash(&bytes));
}

// ---- command_hash ----

fn write(records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(7),
        seq: Seq(9),
        records: records.iter().map(|r| Value(r.to_vec())).collect(),
    })
}

#[test]
fn a_write_hashes_its_tag_writer_position_and_records() {
    let mut bytes = vec![0x00];
    bytes.extend_from_slice(&7_u128.to_le_bytes());
    bytes.extend(words(&[9, 2]));
    bytes.extend_from_slice(b"ab");
    bytes.extend(words(&[1]));
    bytes.extend_from_slice(b"c");
    assert_eq!(command_hash(&write(&[b"ab", b"c"])), value_hash(&bytes));
    assert_ne!(
        command_hash(&write(&[b"ab", b"c"])),
        command_hash(&write(&[b"a", b"bc"])),
        "the record lengths cut the bytes"
    );
}

#[test]
fn each_control_command_hashes_its_own_tagged_encoding() {
    let truncate = Command::Control(Control::Truncate {
        leader: LeaderUuid(3),
        up_to: Seq(5),
    });
    let mut bytes = vec![0xff];
    bytes.extend_from_slice(&3_u128.to_le_bytes());
    bytes.extend(words(&[5]));
    assert_eq!(command_hash(&truncate), value_hash(&bytes));

    assert_eq!(
        command_hash(&Command::Control(Control::Noop)),
        value_hash(&[0xfe])
    );

    let set_leader = |old| {
        Command::Control(Control::SetLeader {
            new: LeaderUuid(8),
            old,
        })
    };
    let mut bytes = vec![0xfd];
    bytes.extend_from_slice(&8_u128.to_le_bytes());
    bytes.extend_from_slice(&2_u128.to_le_bytes());
    assert_eq!(
        command_hash(&set_leader(Some(LeaderUuid(2)))),
        value_hash(&bytes)
    );
    let mut bytes = vec![0xfd];
    bytes.extend_from_slice(&8_u128.to_le_bytes());
    bytes.extend_from_slice(&0_u128.to_le_bytes());
    assert_eq!(command_hash(&set_leader(None)), value_hash(&bytes));
}

// ---- registration_history_hash ----

fn registration(members: &[u64], kind: RegistrationKind) -> Registration {
    Registration {
        config: AcceptorConfig::new(nodes(members), QuorumSystem::Majority),
        kind,
    }
}

#[test]
fn an_empty_history_hashes_to_the_offset_basis() {
    assert_eq!(registration_history_hash([]), value_hash(&[]));
}

#[test]
fn a_history_hashes_each_ballot_kind_membership_and_separator() {
    let b = ballot(4, 1);
    let r = registration(&[0, 2], RegistrationKind::Reconfiguration);
    let mut bytes = words(&[4, 1]);
    bytes.push(1);
    bytes.extend(words(&[0, 2]));
    bytes.push(0xff);
    assert_eq!(registration_history_hash([(&b, &r)]), value_hash(&bytes));
}

#[test]
fn a_history_hash_tells_order_kind_and_cut_apart() {
    let (b1, b2) = (ballot(1, 0), ballot(2, 0));
    let belief = registration(&[0, 1], RegistrationKind::Belief);
    let change = registration(&[0, 1], RegistrationKind::Reconfiguration);
    let hash = |pairs: &[(&Ballot, &Registration)]| registration_history_hash(pairs.to_vec());
    assert_ne!(
        hash(&[(&b1, &belief), (&b2, &change)]),
        hash(&[(&b2, &change), (&b1, &belief)])
    );
    assert_ne!(hash(&[(&b1, &belief)]), hash(&[(&b1, &change)]));
    let (left, right) = (
        registration(&[0, 1], RegistrationKind::Belief),
        registration(&[2], RegistrationKind::Belief),
    );
    let (left2, right2) = (
        registration(&[0], RegistrationKind::Belief),
        registration(&[1, 2], RegistrationKind::Belief),
    );
    assert_ne!(
        hash(&[(&b1, &left), (&b1, &right)]),
        hash(&[(&b1, &left2), (&b1, &right2)])
    );
}

// ---- reconfiguration labels ----

fn set() -> MatchmakerSet {
    MatchmakerSet::new(MatchmakerGeneration(1), vec![MatchmakerId(0)])
}

#[test]
fn each_reconfigure_request_has_its_label() {
    let from = NodeId(1);
    let generation = MatchmakerGeneration(1);
    let cases = [
        (
            ReconfigureRequest::Stop {
                from,
                generation,
                after: None,
            },
            "stop",
        ),
        (
            ReconfigureRequest::Bootstrap {
                from,
                bootstrap: BootstrapPage {
                    set: set(),
                    after: None,
                    registries: BTreeMap::new(),
                    last: true,
                },
            },
            "bootstrap",
        ),
        (
            ReconfigureRequest::DecreePrepare {
                from,
                generation,
                ballot: ballot(1, 1),
            },
            "decree_prepare",
        ),
        (
            ReconfigureRequest::DecreeAccept {
                from,
                generation,
                ballot: ballot(1, 1),
                members: vec![MatchmakerId(0)],
            },
            "decree_accept",
        ),
        (
            ReconfigureRequest::Chosen {
                from,
                generation,
                successor: set(),
            },
            "chosen",
        ),
    ];
    for (request, label) in &cases {
        assert_eq!(reconfigure_kind(request), *label);
    }
}

#[test]
fn each_reconfigure_reply_has_its_label() {
    let matchmaker = MatchmakerId(0);
    let generation = MatchmakerGeneration(1);
    let cases = [
        (
            ReconfigureReply::Stopped {
                matchmaker,
                generation,
                after: None,
                next: None,
                registries: BTreeMap::new(),
                successor: None,
                decree_promised: ballot(0, 0),
            },
            "stopped",
        ),
        (
            ReconfigureReply::Bootstrapped {
                matchmaker,
                set: set(),
                held: None,
                complete: true,
            },
            "bootstrapped",
        ),
        (
            ReconfigureReply::Promised {
                matchmaker,
                generation,
                ballot: ballot(1, 1),
                vote: None,
            },
            "promised",
        ),
        (
            ReconfigureReply::Accepted {
                matchmaker,
                generation,
                ballot: ballot(1, 1),
            },
            "accepted",
        ),
        (
            ReconfigureReply::Nacked {
                matchmaker,
                generation,
                ballot: ballot(1, 1),
                promised: ballot(2, 1),
            },
            "nacked",
        ),
        (
            ReconfigureReply::Learned {
                matchmaker,
                generation,
                activated: true,
                at: generation,
            },
            "learned",
        ),
        (
            ReconfigureReply::Refused {
                matchmaker,
                current: set(),
                phase: MatchmakerPhase::Active,
                successor: None,
            },
            "refused",
        ),
    ];
    for (reply, label) in &cases {
        assert_eq!(reconfigure_reply_kind(reply), *label);
    }
}

#[test]
fn a_started_reconfiguration_answers_its_round_and_a_refusal_its_label() {
    assert_eq!(
        reconfigure_outcome(ReconfigureResult::Started(ballot(12, 3))),
        (true, "", Some(12))
    );
    assert_eq!(
        reconfigure_outcome(ReconfigureResult::NotLeader(Some(NodeId(2)))),
        (false, "not_leader", None)
    );
    for (refusal, label) in [
        (ReconfigureRefusal::NoMatchmakers, "no_matchmakers"),
        (ReconfigureRefusal::Unchanged, "unchanged"),
        (ReconfigureRefusal::UnknownMember, "unknown_member"),
        (ReconfigureRefusal::Malformed, "malformed"),
        (ReconfigureRefusal::Unsettled, "unsettled"),
        (ReconfigureRefusal::RoundExhausted, "round_exhausted"),
    ] {
        assert_eq!(
            reconfigure_outcome(ReconfigureResult::Refused(refusal)),
            (false, label, None)
        );
    }
}

// ---- messages ----

/// A message, its label, its sender and its route's ballot and slot.
type Case = (Message, &'static str, Party, Option<(Ballot, Option<Slot>)>);

/// One message of every kind, with its label, its sender and its route.
#[allow(clippy::too_many_lines)]
fn every_message() -> Vec<Case> {
    let b = ballot(5, 1);
    let noop = Command::Control(Control::Noop);
    let n = |id| Party::Node(NodeId(id));
    let proxy = Party::Proxy(ProxyId(9));
    vec![
        (
            Message::Prepare {
                reply_to: NodeId(1),
                ballot: b,
                from_slot: Slot(3),
                config: None,
            },
            "prepare",
            n(1),
            Some((b, Some(Slot(3)))),
        ),
        (
            Message::Promise {
                from: NodeId(2),
                ballot: b,
                from_slot: Slot(4),
                accepted: BTreeMap::new(),
                faulty: BTreeMap::new(),
                next_from_slot: None,
            },
            "promise",
            n(2),
            Some((b, Some(Slot(4)))),
        ),
        (
            Message::Accept {
                reply_to: proxy,
                leader: NodeId(1),
                ballot: b,
                slot: Slot(6),
                command: noop.clone(),
                config: None,
            },
            "accept",
            proxy,
            Some((b, Some(Slot(6)))),
        ),
        (
            Message::Accepted {
                from: NodeId(3),
                ballot: b,
                slot: Slot(7),
                vhash: 1,
            },
            "accepted",
            n(3),
            Some((b, Some(Slot(7)))),
        ),
        (
            Message::Nack {
                from: NodeId(4),
                ballot: b,
                slot: Slot(8),
            },
            "nack",
            n(4),
            Some((b, Some(Slot(8)))),
        ),
        (
            Message::Commit {
                from: proxy,
                ballot: b,
                slot: Slot(9),
                command: noop.clone(),
            },
            "commit",
            proxy,
            Some((b, Some(Slot(9)))),
        ),
        (
            Message::CatchUpRequest {
                from: NodeId(5),
                from_slot: Slot(1),
            },
            "catchup_request",
            n(5),
            None,
        ),
        (
            Message::CatchUpResponse {
                from: NodeId(6),
                entries: BTreeMap::new(),
            },
            "catchup_response",
            n(6),
            None,
        ),
        (
            Message::TrimmedTo {
                from: NodeId(7),
                point: Slot(2),
                state: JournalState::default(),
            },
            "trimmed_to",
            n(7),
            None,
        ),
        (
            Message::Heartbeat {
                from: NodeId(8),
                ballot: b,
                commit: None,
                config: None,
                fence: None,
            },
            "heartbeat",
            n(8),
            Some((b, None)),
        ),
        (
            Message::HeartbeatAck {
                from: NodeId(9),
                ballot: b,
                chosen: Some(Slot(1)),
            },
            "heartbeat_ack",
            n(9),
            None,
        ),
        (
            Message::Relinquish {
                from: NodeId(10),
                to: NodeId(11),
                ballot: b,
                from_slot: Slot(1),
                next_slot: Slot(12),
                decided: BTreeMap::new(),
                pending: BTreeMap::new(),
                config: None,
                prior: Vec::new(),
            },
            "relinquish",
            n(10),
            Some((b, Some(Slot(12)))),
        ),
        (
            Message::PreRead {
                reply_to: NodeId(12),
                ctx: 1,
            },
            "pre_read",
            n(12),
            None,
        ),
        (
            Message::PreReadAck {
                from: NodeId(13),
                ctx: 1,
                watermark: None,
                config_since: None,
            },
            "pre_read_ack",
            n(13),
            None,
        ),
    ]
}

#[test]
fn each_message_has_its_label_sender_and_route() {
    for (message, label, sender, route) in every_message() {
        assert_eq!(message_kind(&message), label);
        assert_eq!(message_sender(&message), Some(sender), "{label}");
        assert_eq!(
            message_route(&message),
            route.map(|(ballot, slot)| (sender, ballot, slot)),
            "{label}"
        );
    }
}

#[test]
fn the_labels_cover_every_kind_once() {
    let labels: BTreeSet<_> = every_message().iter().map(|m| m.1).collect();
    assert_eq!(labels.len(), every_message().len());
}

/// The encoded label table and the decoded one are one rule: a message
/// keeps its label across the wire.
#[test]
fn an_encoded_message_keeps_its_label() {
    for (message, label, _, _) in every_message() {
        let proto = message_to_proto(&message).expect("every kind encodes");
        assert_eq!(proto_message_kind(&proto), label);
    }
    assert_eq!(
        proto_message_kind(&internal::ConsensusMessage::default()),
        "unknown"
    );
}
