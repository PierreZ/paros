//! The consensus wire: one domain [`Message`] to and from its typed protobuf.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Message, NodeId, Party, SessionEntry, Slot,
};

use super::codec::{
    ballot_from_proto, ballot_to_proto, command_from_proto, command_to_proto, config_from_proto,
    config_to_proto, party_from_proto, party_to_proto, unique_map,
};
use super::internal;

fn faulty_slots_to_proto(entries: &BTreeMap<Slot, Ballot>) -> Vec<internal::FaultySlot> {
    entries
        .iter()
        .map(|(slot, ballot)| internal::FaultySlot {
            slot: slot.0,
            ballot: Some(ballot_to_proto(*ballot)),
        })
        .collect()
}

fn faulty_slots_from_proto(
    entries: Vec<internal::FaultySlot>,
) -> Result<BTreeMap<Slot, Ballot>, &'static str> {
    unique_map(
        entries
            .into_iter()
            .map(|entry| Ok((Slot(entry.slot), ballot_from_proto(entry.ballot)?))),
        "duplicate faulty slot in message",
    )
}

fn slot_commands_to_proto(
    entries: &BTreeMap<Slot, (Ballot, Command)>,
) -> Vec<internal::SlotCommand> {
    entries
        .iter()
        .map(|(slot, (ballot, command))| internal::SlotCommand {
            slot: slot.0,
            ballot: Some(ballot_to_proto(*ballot)),
            command: Some(command_to_proto(command)),
        })
        .collect()
}

fn slot_commands_from_proto(
    entries: Vec<internal::SlotCommand>,
) -> Result<BTreeMap<Slot, (Ballot, Command)>, &'static str> {
    unique_map(
        entries.into_iter().map(|entry| {
            let value = (
                ballot_from_proto(entry.ballot)?,
                command_from_proto(entry.command)?,
            );
            Ok((Slot(entry.slot), value))
        }),
        "duplicate slot in message",
    )
}

/// Encode the `pending` half of a [`Message::Relinquish`] tail: each slot's
/// command runs at the transferred ballot by construction, so the per-slot
/// ballot field of `SlotCommand` is left unset on the wire and re-derived on
/// decode.
fn pending_commands_to_proto(entries: &BTreeMap<Slot, Command>) -> Vec<internal::SlotCommand> {
    entries
        .iter()
        .map(|(slot, command)| internal::SlotCommand {
            slot: slot.0,
            ballot: None,
            command: Some(command_to_proto(command)),
        })
        .collect()
}

fn pending_commands_from_proto(
    entries: Vec<internal::SlotCommand>,
) -> Result<BTreeMap<Slot, Command>, &'static str> {
    unique_map(
        entries
            .into_iter()
            .map(|entry| Ok((Slot(entry.slot), command_from_proto(entry.command)?))),
        "duplicate slot in message",
    )
}

fn sessions_to_proto(sessions: &[SessionEntry]) -> Vec<internal::SessionRecord> {
    sessions
        .iter()
        .map(|&(client, seq, slot)| internal::SessionRecord {
            client: client.0,
            seq: seq.0,
            slot: slot.0,
        })
        .collect()
}

/// Convert one domain message into its typed protobuf representation.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) fn message_to_proto(
    message: &Message,
) -> Result<internal::ConsensusMessage, &'static str> {
    use internal::consensus_message::Kind;

    let kind = match message {
        Message::Prepare {
            reply_to,
            ballot,
            from_slot,
            config,
        } => Kind::Prepare(internal::Prepare {
            reply_to: reply_to.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            config: config.as_ref().map(config_to_proto),
        }),
        Message::Promise {
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        } => Kind::Promise(internal::Promise {
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            accepted: slot_commands_to_proto(accepted),
            faulty: faulty_slots_to_proto(faulty),
            next_from_slot: next_from_slot.map(|slot| slot.0),
        }),
        Message::Accept {
            reply_to,
            leader,
            ballot,
            slot,
            command,
            config,
        } => {
            let (reply_to_node, reply_to_proxy) = party_to_proto(*reply_to);
            Kind::Accept(internal::Accept {
                reply_to: reply_to_node,
                // Absent when it would merely repeat the reply address, which
                // is every colocated round: the plain wire is unchanged.
                leader: (*reply_to != Party::Node(*leader)).then_some(leader.0),
                ballot: Some(ballot_to_proto(*ballot)),
                slot: slot.0,
                command: Some(command_to_proto(command)),
                reply_to_proxy,
                config: config.as_ref().map(config_to_proto),
            })
        }
        Message::Accepted {
            from,
            ballot,
            slot,
            vhash,
        } => Kind::Accepted(internal::Accepted {
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            slot: slot.0,
            vhash: *vhash,
        }),
        Message::Nack { from, ballot, slot } => Kind::Nack(internal::Nack {
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            slot: slot.0,
        }),
        Message::Commit {
            from,
            ballot,
            slot,
            command,
        } => {
            let (from_node, from_proxy) = party_to_proto(*from);
            Kind::Commit(internal::Commit {
                from: from_node,
                ballot: Some(ballot_to_proto(*ballot)),
                slot: slot.0,
                command: Some(command_to_proto(command)),
                from_proxy,
            })
        }
        Message::CatchUpRequest { from, from_slot } => {
            Kind::CatchUpRequest(internal::CatchUpRequest {
                from: from.0,
                from_slot: from_slot.0,
            })
        }
        Message::CatchUpResponse { from, entries } => {
            Kind::CatchUpResponse(internal::CatchUpResponse {
                from: from.0,
                entries: slot_commands_to_proto(entries),
            })
        }
        Message::TrimmedTo {
            from,
            point,
            sessions,
        } => Kind::TrimmedTo(internal::TrimmedTo {
            from: from.0,
            point: point.0,
            sessions: sessions_to_proto(sessions),
        }),
        Message::Heartbeat {
            from,
            ballot,
            commit,
            seq,
            config,
        } => Kind::Heartbeat(internal::Heartbeat {
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            commit: commit.map(|slot| slot.0),
            seq: *seq,
            config: config.as_ref().map(config_to_proto),
        }),
        Message::HeartbeatAck {
            from,
            ballot,
            seq,
            chosen,
        } => Kind::HeartbeatAck(internal::HeartbeatAck {
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            seq: *seq,
            chosen: chosen.map(|s| s.0),
        }),
        Message::Relinquish {
            from,
            to,
            ballot,
            from_slot,
            next_slot,
            decided,
            pending,
            config,
        } => Kind::Relinquish(internal::Relinquish {
            from: from.0,
            to: to.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            next_slot: next_slot.0,
            decided: slot_commands_to_proto(decided),
            pending: pending_commands_to_proto(pending),
            config: config.as_ref().map(config_to_proto),
        }),
        Message::PreRead { reply_to, ctx } => Kind::PreRead(internal::PreRead {
            reply_to: reply_to.0,
            ctx: *ctx,
        }),
        Message::PreReadAck {
            from,
            ctx,
            watermark,
            config_since,
        } => Kind::PreReadAck(internal::PreReadAck {
            from: from.0,
            ctx: *ctx,
            watermark: watermark.map(|slot| slot.0),
            config_since: config_since.map(ballot_to_proto),
        }),
        _ => return Err("unsupported Paxos message variant"),
    };
    Ok(internal::ConsensusMessage { kind: Some(kind) })
}

/// Validate and convert one typed protobuf message into the core domain type.
// One arm per wire variant; splitting the decode table would scatter it.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) fn message_from_proto(
    message: internal::ConsensusMessage,
) -> Result<Message, &'static str> {
    use internal::consensus_message::Kind;

    match message.kind.ok_or("missing Paxos message kind")? {
        Kind::Prepare(message) => Ok(Message::Prepare {
            reply_to: NodeId(message.reply_to),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            config: config_from_proto(message.config)?,
        }),
        Kind::Promise(message) => Ok(Message::Promise {
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            accepted: slot_commands_from_proto(message.accepted)?,
            faulty: faulty_slots_from_proto(message.faulty)?,
            next_from_slot: message.next_from_slot.map(Slot),
        }),
        Kind::Accept(message) => Ok(Message::Accept {
            reply_to: party_from_proto(message.reply_to, message.reply_to_proxy),
            // A delegated round always names its leader explicitly; a
            // colocated one may leave it to the reply address.
            leader: NodeId(message.leader.unwrap_or(message.reply_to)),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
            command: command_from_proto(message.command)?,
            config: config_from_proto(message.config)?,
        }),
        Kind::Accepted(message) => Ok(Message::Accepted {
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
            vhash: message.vhash,
        }),
        Kind::Nack(message) => Ok(Message::Nack {
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
        }),
        Kind::Commit(message) => Ok(Message::Commit {
            from: party_from_proto(message.from, message.from_proxy),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
            command: command_from_proto(message.command)?,
        }),
        Kind::CatchUpRequest(message) => Ok(Message::CatchUpRequest {
            from: NodeId(message.from),
            from_slot: Slot(message.from_slot),
        }),
        Kind::CatchUpResponse(message) => Ok(Message::CatchUpResponse {
            from: NodeId(message.from),
            entries: slot_commands_from_proto(message.entries)?,
        }),
        Kind::TrimmedTo(message) => Ok(Message::TrimmedTo {
            from: NodeId(message.from),
            point: Slot(message.point),
            sessions: message
                .sessions
                .into_iter()
                .map(|record| {
                    (
                        ClientId(record.client),
                        ClientSeq(record.seq),
                        Slot(record.slot),
                    )
                })
                .collect(),
        }),
        Kind::Heartbeat(message) => Ok(Message::Heartbeat {
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            commit: message.commit.map(Slot),
            seq: message.seq,
            config: config_from_proto(message.config)?,
        }),
        Kind::HeartbeatAck(message) => Ok(Message::HeartbeatAck {
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            seq: message.seq,
            chosen: message.chosen.map(Slot),
        }),
        Kind::Relinquish(message) => Ok(Message::Relinquish {
            from: NodeId(message.from),
            to: NodeId(message.to),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            next_slot: Slot(message.next_slot),
            decided: slot_commands_from_proto(message.decided)?,
            pending: pending_commands_from_proto(message.pending)?,
            config: config_from_proto(message.config)?,
        }),
        Kind::PreRead(message) => Ok(Message::PreRead {
            reply_to: NodeId(message.reply_to),
            ctx: message.ctx,
        }),
        Kind::PreReadAck(message) => Ok(Message::PreReadAck {
            from: NodeId(message.from),
            ctx: message.ctx,
            watermark: message.watermark.map(Slot),
            config_since: message.config_since.map(Ballot::from),
        }),
    }
}
