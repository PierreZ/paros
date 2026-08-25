//! Generated gRPC contract and the bridge into the single-owner node driver.

use std::collections::BTreeMap;

use paros_core::{
    Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, Message, NodeId, SessionEntry,
    Slot, Value,
};
use prost::Message as ProstMessage;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

/// Client-facing journal contract generated from `proto/paros.proto`.
pub mod public {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.v1");
}

/// Cluster-internal consensus contract generated from `proto/internal.proto`.
pub(crate) mod internal {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.internal.v1");
}

pub use internal::paros_internal_client::ParosInternalClient;
pub(crate) use internal::paros_internal_server::ParosInternalServer;
pub use internal::{InspectReply, InspectRequest};
pub use public::paros_client::ParosClient;
pub(crate) use public::paros_server::ParosServer;
pub use public::{Compact, CompactAck, Propose, ProposeAck, Read, ReadAck};

pub(crate) type ReplySender<T> = oneshot::Sender<T>;
type Call<T, U> = (T, ReplySender<U>);

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn checksum_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Stable FNV-1a integrity checksum for one encoded protobuf consensus message.
pub(crate) fn wire_checksum(bytes: &[u8]) -> u64 {
    checksum_extend(FNV_OFFSET, bytes)
}

/// Stable integrity checksum for one client proposal.
///
/// The identity, explicit command length, and opaque bytes are all covered so a
/// changed request is rejected before it can enter the consensus log. This is
/// an integrity check for transport damage, not an authentication primitive.
#[must_use]
pub fn proposal_checksum(client: u64, seq: u64, command: &[u8]) -> u64 {
    let command_len = u64::try_from(command.len()).unwrap_or(u64::MAX);
    let hash = checksum_extend(FNV_OFFSET, b"paros-propose-v1");
    let hash = checksum_extend(hash, &client.to_le_bytes());
    let hash = checksum_extend(hash, &seq.to_le_bytes());
    let hash = checksum_extend(hash, &command_len.to_le_bytes());
    checksum_extend(hash, command)
}

fn ballot_to_proto(ballot: Ballot) -> internal::Ballot {
    internal::Ballot {
        round: ballot.round,
        node: ballot.node.0,
    }
}

fn ballot_from_proto(ballot: Option<internal::Ballot>) -> Result<Ballot, &'static str> {
    let ballot = ballot.ok_or("missing ballot")?;
    Ok(Ballot {
        round: ballot.round,
        node: NodeId(ballot.node),
    })
}

fn command_to_proto(command: &Command) -> internal::Command {
    let kind = match command {
        Command::User(entry) => internal::command::Kind::User(internal::UserEntry {
            client: entry.client.0,
            seq: entry.seq.0,
            value: entry.value.0.clone(),
        }),
        Command::Control(control) => {
            let kind = match control {
                Control::Truncate { up_to } => {
                    internal::control_command::Kind::Truncate(internal::Truncate { up_to: up_to.0 })
                }
                Control::Noop => internal::control_command::Kind::Noop(internal::Noop {}),
            };
            internal::command::Kind::Control(internal::ControlCommand { kind: Some(kind) })
        }
    };
    internal::Command { kind: Some(kind) }
}

fn command_from_proto(command: Option<internal::Command>) -> Result<Command, &'static str> {
    match command
        .ok_or("missing command")?
        .kind
        .ok_or("missing command kind")?
    {
        internal::command::Kind::User(entry) => Ok(Command::User(Entry {
            client: ClientId(entry.client),
            seq: ClientSeq(entry.seq),
            value: Value(entry.value),
        })),
        internal::command::Kind::Control(control) => {
            let control = match control.kind.ok_or("missing control command kind")? {
                internal::control_command::Kind::Truncate(truncate) => Control::Truncate {
                    up_to: Slot(truncate.up_to),
                },
                internal::control_command::Kind::Noop(_) => Control::Noop,
            };
            Ok(Command::Control(control))
        }
    }
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
    let mut decoded = BTreeMap::new();
    for entry in entries {
        let slot = Slot(entry.slot);
        let value = (
            ballot_from_proto(entry.ballot)?,
            command_from_proto(entry.command)?,
        );
        if decoded.insert(slot, value).is_some() {
            return Err("duplicate slot in message");
        }
    }
    Ok(decoded)
}

fn snapshot_to_proto(
    config_id: ConfigId,
    from: NodeId,
    ballot: Ballot,
    chosen_index: Slot,
    snapshot: &Value,
    sessions: &[SessionEntry],
) -> internal::consensus_message::Kind {
    internal::consensus_message::Kind::InstallSnapshot(internal::InstallSnapshot {
        config_id: config_id.0,
        from: from.0,
        ballot: Some(ballot_to_proto(ballot)),
        chosen_index: chosen_index.0,
        snapshot: snapshot.0.clone(),
        sessions: sessions
            .iter()
            .map(|&(client, seq, slot)| internal::SessionRecord {
                client: client.0,
                seq: seq.0,
                slot: slot.0,
            })
            .collect(),
    })
}

/// Convert one domain message into its typed protobuf representation.
#[allow(clippy::too_many_lines)]
pub(crate) fn message_to_proto(
    message: &Message,
) -> Result<internal::ConsensusMessage, &'static str> {
    use internal::consensus_message::Kind;

    let kind = match message {
        Message::Prepare {
            config_id,
            from,
            ballot,
            from_slot,
        } => Kind::Prepare(internal::Prepare {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
        }),
        Message::Promise {
            config_id,
            from,
            ballot,
            from_slot,
            accepted,
            next_from_slot,
        } => Kind::Promise(internal::Promise {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            accepted: slot_commands_to_proto(accepted),
            next_from_slot: next_from_slot.map(|slot| slot.0),
        }),
        Message::Accept {
            config_id,
            from,
            ballot,
            slot,
            command,
        } => Kind::Accept(internal::Accept {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            slot: slot.0,
            command: Some(command_to_proto(command)),
        }),
        Message::Accepted {
            config_id,
            from,
            ballot,
            slot,
            vhash,
        } => Kind::Accepted(internal::Accepted {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            slot: slot.0,
            vhash: *vhash,
        }),
        Message::Nack {
            config_id,
            from,
            ballot,
            promised,
            slot,
        } => Kind::Nack(internal::Nack {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            promised: Some(ballot_to_proto(*promised)),
            slot: slot.0,
        }),
        Message::Commit {
            config_id,
            from,
            ballot,
            slot,
            command,
        } => Kind::Commit(internal::Commit {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            slot: slot.0,
            command: Some(command_to_proto(command)),
        }),
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
        Message::InstallSnapshot {
            config_id,
            from,
            ballot,
            chosen_index,
            snapshot,
            sessions,
        } => snapshot_to_proto(
            *config_id,
            *from,
            *ballot,
            *chosen_index,
            snapshot,
            sessions,
        ),
        Message::CheckLeader { from } => Kind::CheckLeader(internal::CheckLeader { from: from.0 }),
        Message::Heartbeat {
            config_id,
            from,
            ballot,
            commit,
            seq,
        } => Kind::Heartbeat(internal::Heartbeat {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            commit: commit.map(|slot| slot.0),
            seq: *seq,
        }),
        Message::HeartbeatAck {
            config_id,
            from,
            ballot,
            seq,
        } => Kind::HeartbeatAck(internal::HeartbeatAck {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            seq: *seq,
        }),
        _ => return Err("unsupported Paxos message variant"),
    };
    Ok(internal::ConsensusMessage { kind: Some(kind) })
}

/// Validate and convert one typed protobuf message into the core domain type.
pub(crate) fn message_from_proto(
    message: internal::ConsensusMessage,
) -> Result<Message, &'static str> {
    use internal::consensus_message::Kind;

    match message.kind.ok_or("missing Paxos message kind")? {
        Kind::Prepare(message) => Ok(Message::Prepare {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
        }),
        Kind::Promise(message) => Ok(Message::Promise {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            accepted: slot_commands_from_proto(message.accepted)?,
            next_from_slot: message.next_from_slot.map(Slot),
        }),
        Kind::Accept(message) => Ok(Message::Accept {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
            command: command_from_proto(message.command)?,
        }),
        Kind::Accepted(message) => Ok(Message::Accepted {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            slot: Slot(message.slot),
            vhash: message.vhash,
        }),
        Kind::Nack(message) => Ok(Message::Nack {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            promised: ballot_from_proto(message.promised)?,
            slot: Slot(message.slot),
        }),
        Kind::Commit(message) => Ok(Message::Commit {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
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
        Kind::InstallSnapshot(message) => Ok(Message::InstallSnapshot {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            chosen_index: Slot(message.chosen_index),
            snapshot: Value(message.snapshot),
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
        Kind::CheckLeader(message) => Ok(Message::CheckLeader {
            from: NodeId(message.from),
        }),
        Kind::Heartbeat(message) => Ok(Message::Heartbeat {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            commit: message.commit.map(Slot),
            seq: message.seq,
        }),
        Kind::HeartbeatAck(message) => Ok(Message::HeartbeatAck {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            seq: message.seq,
        }),
    }
}

/// Requests accepted concurrently by tonic and consumed serially by the node
/// driver, which exclusively owns the sans-IO core.
pub(crate) struct RpcInbox {
    pub(crate) propose: mpsc::Receiver<Call<Propose, ProposeAck>>,
    pub(crate) read: mpsc::Receiver<Call<Read, ReadAck>>,
    pub(crate) deliver: mpsc::Receiver<Call<Message, ()>>,
    pub(crate) compact: mpsc::Receiver<Call<Compact, CompactAck>>,
    pub(crate) inspect: mpsc::Receiver<Call<InspectRequest, InspectReply>>,
}

/// Cloneable tonic handler. Each method forwards to [`RpcInbox`] and holds the
/// HTTP/2 response open until the driver completes that request.
#[derive(Clone)]
pub(crate) struct RpcService {
    propose: mpsc::Sender<Call<Propose, ProposeAck>>,
    read: mpsc::Sender<Call<Read, ReadAck>>,
    deliver: mpsc::Sender<Call<Message, ()>>,
    compact: mpsc::Sender<Call<Compact, CompactAck>>,
    inspect: mpsc::Sender<Call<InspectRequest, InspectReply>>,
}

/// Construct a handler/inbox pair for one node incarnation.
pub(crate) fn rpc_channel() -> (RpcService, RpcInbox) {
    // Bounded queues make overload visible as backpressure while leaving ample
    // room for one simulation tick's peer-message fanout.
    let (propose_tx, propose_rx) = mpsc::channel(256);
    let (read_tx, read_rx) = mpsc::channel(256);
    let (deliver_tx, deliver_rx) = mpsc::channel(1024);
    let (compact_tx, compact_rx) = mpsc::channel(256);
    let (inspect_tx, inspect_rx) = mpsc::channel(256);
    (
        RpcService {
            propose: propose_tx,
            read: read_tx,
            deliver: deliver_tx,
            compact: compact_tx,
            inspect: inspect_tx,
        },
        RpcInbox {
            propose: propose_rx,
            read: read_rx,
            deliver: deliver_rx,
            compact: compact_rx,
            inspect: inspect_rx,
        },
    )
}

async fn dispatch<T, U>(sender: &mpsc::Sender<Call<T, U>>, value: T) -> Result<U, Status> {
    let (reply_tx, reply_rx) = oneshot::channel();
    sender
        .send((value, reply_tx))
        .await
        .map_err(|_| Status::unavailable("node driver stopped"))?;
    reply_rx
        .await
        .map_err(|_| Status::unavailable("node driver dropped the reply"))
}

#[tonic::async_trait]
impl public::paros_server::Paros for RpcService {
    async fn propose(&self, request: Request<Propose>) -> Result<Response<ProposeAck>, Status> {
        let request = request.into_inner();
        if proposal_checksum(request.client, request.seq, &request.command) != request.checksum {
            tracing::warn!(
                client = request.client,
                seq = request.seq,
                "proposal_checksum_rejected"
            );
            return Err(Status::data_loss("invalid proposal checksum"));
        }
        dispatch(&self.propose, request).await.map(Response::new)
    }

    async fn read(&self, request: Request<Read>) -> Result<Response<ReadAck>, Status> {
        dispatch(&self.read, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn compact(&self, request: Request<Compact>) -> Result<Response<CompactAck>, Status> {
        dispatch(&self.compact, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl internal::paros_internal_server::ParosInternal for RpcService {
    async fn deliver(
        &self,
        request: Request<internal::Deliver>,
    ) -> Result<Response<internal::DeliverAck>, Status> {
        for envelope in request.into_inner().messages {
            let message = envelope
                .message
                .ok_or_else(|| Status::invalid_argument("missing Paxos message"))?;
            if wire_checksum(&message.encode_to_vec()) != envelope.checksum {
                tracing::warn!("message_corruption_rejected");
                return Err(Status::data_loss("invalid Paxos message checksum"));
            }
            let message = message_from_proto(message).map_err(|error| {
                Status::invalid_argument(format!("invalid Paxos message: {error}"))
            })?;
            dispatch(&self.deliver, message).await?;
        }
        Ok(Response::new(internal::DeliverAck {}))
    }

    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectReply>, Status> {
        dispatch(&self.inspect, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::{internal, message_from_proto, proposal_checksum};

    #[test]
    fn proposal_checksum_covers_identity_length_and_command() {
        let command = [1_u8, 2, 3, 4];
        let checksum = proposal_checksum(7, 11, &command);

        assert_ne!(checksum, proposal_checksum(8, 11, &command));
        assert_ne!(checksum, proposal_checksum(7, 12, &command));
        assert_ne!(checksum, proposal_checksum(7, 11, &[1, 2, 3]));
        assert_ne!(checksum, proposal_checksum(7, 11, &[1, 2, 3, 5]));
    }

    #[test]
    fn protobuf_rejects_a_missing_message_kind() {
        let result = message_from_proto(internal::ConsensusMessage { kind: None });
        assert!(matches!(result, Err("missing Paxos message kind")));
    }

    #[test]
    fn protobuf_rejects_duplicate_slots_in_a_suffix() {
        let entry = internal::SlotCommand {
            slot: 4,
            ballot: Some(internal::Ballot { round: 2, node: 1 }),
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
        };

        assert!(matches!(
            message_from_proto(wire),
            Err("duplicate slot in message")
        ));
    }
}
