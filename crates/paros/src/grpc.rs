//! Generated gRPC contract and the bridge into the single-owner node driver.

use std::collections::BTreeMap;
use std::sync::Arc;

use paros_core::{
    AcceptorConfig, Ballot, ClientId, ClientSeq, Command, ConfigId, Control, Entry, GcAck,
    GcRequest, MatchOutcome, MatchRefusal, MatchReply, MatchRequest, MatchmakerGeneration,
    MatchmakerId, MatchmakerPhase, MatchmakerSet, Message, NodeId, PendingBootstrap, QuorumSystem,
    ReconfigureReply, ReconfigureRequest, Registration, SessionEntry, Slot, Value,
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

/// The matchmaker contract generated from `proto/matchmaker.proto`: a per-ballot
/// configuration registry, spoken only by a deployment that names matchmakers.
pub mod matchmaker {
    #![allow(missing_docs, clippy::pedantic)]
    tonic::include_proto!("paros.matchmaker.v1");
}

pub use internal::paros_internal_client::ParosInternalClient;
pub(crate) use internal::paros_internal_server::ParosInternalServer;
pub use internal::{InspectReply, InspectRequest, RetireAck, RetireRequest};
pub use matchmaker::paros_matchmaker_client::ParosMatchmakerClient;
pub(crate) use matchmaker::paros_matchmaker_server::ParosMatchmakerServer;
pub use matchmaker::{
    GarbageCollect as WireGarbageCollect, GarbageCollectAck as WireGarbageCollectAck,
    MatchReply as WireMatchReply, MatchRequest as WireMatchRequest,
    ReconfigureReply as WireReconfigureReply, ReconfigureRequest as WireReconfigureRequest,
};
pub use public::paros_client::ParosClient;
pub(crate) use public::paros_server::ParosServer;
pub use public::{
    Compact, CompactAck, Propose, ProposeAck, Read, ReadAck, Reconfigure, ReconfigureAck,
    ReconfigureMatchmakers, ReconfigureMatchmakersAck,
};

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

fn config_to_proto(config: &AcceptorConfig) -> internal::AcceptorConfig {
    internal::AcceptorConfig {
        members: config.members.iter().map(|n| n.0).collect(),
        quorum_system: match config.quorum_system {
            QuorumSystem::Majority => internal::QuorumSystem::Majority.into(),
        },
    }
}

fn config_from_proto(
    config: Option<internal::AcceptorConfig>,
) -> Result<Option<AcceptorConfig>, &'static str> {
    let Some(config) = config else {
        return Ok(None);
    };
    let quorum_system = match internal::QuorumSystem::try_from(config.quorum_system) {
        Ok(internal::QuorumSystem::Majority) => QuorumSystem::Majority,
        Err(_) => return Err("unknown quorum system"),
    };
    if config.members.is_empty() {
        return Err("empty acceptor configuration");
    }
    Ok(Some(AcceptorConfig::new(
        config.members.into_iter().map(NodeId).collect(),
        quorum_system,
    )))
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
                Control::Snap { at_index } => {
                    internal::control_command::Kind::Snap(internal::Snap {
                        at_index: at_index.0,
                    })
                }
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
                internal::control_command::Kind::Snap(snap) => Control::Snap {
                    at_index: Slot(snap.at_index),
                },
            };
            Ok(Command::Control(control))
        }
    }
}

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
    let mut decoded = BTreeMap::new();
    for entry in entries {
        if decoded
            .insert(Slot(entry.slot), ballot_from_proto(entry.ballot)?)
            .is_some()
        {
            return Err("duplicate faulty slot in message");
        }
    }
    Ok(decoded)
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
    let mut decoded = BTreeMap::new();
    for entry in entries {
        if decoded
            .insert(Slot(entry.slot), command_from_proto(entry.command)?)
            .is_some()
        {
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
#[tracing::instrument(level = "trace", skip_all)]
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
            config,
        } => Kind::Prepare(internal::Prepare {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            config: config.as_ref().map(config_to_proto),
        }),
        Message::Promise {
            config_id,
            from,
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
        } => Kind::Promise(internal::Promise {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            accepted: slot_commands_to_proto(accepted),
            faulty: faulty_slots_to_proto(faulty),
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
            config,
        } => Kind::Heartbeat(internal::Heartbeat {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            commit: commit.map(|slot| slot.0),
            seq: *seq,
            config: config.as_ref().map(config_to_proto),
        }),
        Message::HeartbeatAck {
            config_id,
            from,
            ballot,
            seq,
            chosen,
        } => Kind::HeartbeatAck(internal::HeartbeatAck {
            config_id: config_id.0,
            from: from.0,
            ballot: Some(ballot_to_proto(*ballot)),
            seq: *seq,
            chosen: chosen.map(|s| s.0),
        }),
        Message::SnapAck {
            config_id,
            from,
            at_index,
        } => Kind::SnapAck(internal::SnapAck {
            config_id: config_id.0,
            from: from.0,
            at_index: at_index.0,
        }),
        Message::SnapChunkRequest {
            config_id,
            from,
            at_index,
            chunks,
        } => Kind::SnapChunkRequest(internal::SnapChunkRequest {
            config_id: config_id.0,
            from: from.0,
            at_index: at_index.0,
            chunks: chunks.clone(),
        }),
        Message::SnapChunkResponse {
            config_id,
            from,
            at_index,
            chunks,
        } => Kind::SnapChunkResponse(internal::SnapChunkResponse {
            config_id: config_id.0,
            from: from.0,
            at_index: at_index.0,
            chunks: chunks
                .iter()
                .map(|(index, bytes)| internal::SnapChunk {
                    index: *index,
                    bytes: bytes.0.clone(),
                })
                .collect(),
        }),
        Message::Relinquish {
            config_id,
            from,
            to,
            ballot,
            from_slot,
            next_slot,
            decided,
            pending,
            config,
        } => Kind::Relinquish(internal::Relinquish {
            config_id: config_id.0,
            from: from.0,
            to: to.0,
            ballot: Some(ballot_to_proto(*ballot)),
            from_slot: from_slot.0,
            next_slot: next_slot.0,
            decided: slot_commands_to_proto(decided),
            pending: pending_commands_to_proto(pending),
            config: config.as_ref().map(config_to_proto),
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
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            config: config_from_proto(message.config)?,
        }),
        Kind::Promise(message) => Ok(Message::Promise {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            accepted: slot_commands_from_proto(message.accepted)?,
            faulty: faulty_slots_from_proto(message.faulty)?,
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
            config: config_from_proto(message.config)?,
        }),
        Kind::HeartbeatAck(message) => Ok(Message::HeartbeatAck {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            ballot: ballot_from_proto(message.ballot)?,
            seq: message.seq,
            chosen: message.chosen.map(Slot),
        }),
        Kind::SnapAck(message) => Ok(Message::SnapAck {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            at_index: Slot(message.at_index),
        }),
        Kind::SnapChunkRequest(message) => Ok(Message::SnapChunkRequest {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            at_index: Slot(message.at_index),
            chunks: message.chunks,
        }),
        Kind::SnapChunkResponse(message) => Ok(Message::SnapChunkResponse {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            at_index: Slot(message.at_index),
            chunks: message
                .chunks
                .into_iter()
                .map(|chunk| (chunk.index, Value(chunk.bytes)))
                .collect(),
        }),
        Kind::Relinquish(message) => Ok(Message::Relinquish {
            config_id: ConfigId(message.config_id),
            from: NodeId(message.from),
            to: NodeId(message.to),
            ballot: ballot_from_proto(message.ballot)?,
            from_slot: Slot(message.from_slot),
            next_slot: Slot(message.next_slot),
            decided: slot_commands_from_proto(message.decided)?,
            pending: pending_commands_from_proto(message.pending)?,
            config: config_from_proto(message.config)?,
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
    pub(crate) reconfigure: mpsc::Receiver<Call<Reconfigure, ReconfigureAck>>,
    pub(crate) reconfigure_matchmakers:
        mpsc::Receiver<Call<ReconfigureMatchmakers, ReconfigureMatchmakersAck>>,
    pub(crate) inspect: mpsc::Receiver<Call<InspectRequest, InspectReply>>,
    pub(crate) retire: mpsc::Receiver<Call<RetireRequest, RetireAck>>,
}

/// Why the gRPC edge refused an inbound request before the node loop saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeRejection {
    /// A client proposal whose `(client, seq, command)` checksum did not match.
    ProposalChecksum,
    /// A peer batch envelope whose per-message checksum did not match.
    MessageChecksum,
    /// A peer message that decoded from the wire but not into a `Message`.
    MessageDecode,
}

/// The edge's observation callback: the driver installs one that forwards to
/// its [`Audit`](crate::Audit), stamped with the node's identity.
pub(crate) type OnReject = Arc<dyn Fn(EdgeRejection) + Send + Sync>;

/// Cloneable tonic handler. Each method forwards to [`RpcInbox`] and holds the
/// HTTP/2 response open until the driver completes that request.
#[derive(Clone)]
pub(crate) struct RpcService {
    propose: mpsc::Sender<Call<Propose, ProposeAck>>,
    read: mpsc::Sender<Call<Read, ReadAck>>,
    deliver: mpsc::Sender<Call<Message, ()>>,
    compact: mpsc::Sender<Call<Compact, CompactAck>>,
    reconfigure: mpsc::Sender<Call<Reconfigure, ReconfigureAck>>,
    reconfigure_matchmakers: mpsc::Sender<Call<ReconfigureMatchmakers, ReconfigureMatchmakersAck>>,
    inspect: mpsc::Sender<Call<InspectRequest, InspectReply>>,
    retire: mpsc::Sender<Call<RetireRequest, RetireAck>>,
    on_reject: OnReject,
}

/// Construct a handler/inbox pair for one node incarnation. `client_inbox`
/// bounds each client-facing queue (propose, read, compact, inspect) and
/// `peer_inbox` the peer-message queue; both must be at least 1.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn rpc_channel(
    client_inbox: usize,
    peer_inbox: usize,
    on_reject: OnReject,
) -> (RpcService, RpcInbox) {
    // Bounded queues make overload visible as backpressure while leaving ample
    // room for one simulation tick's peer-message fanout.
    let (propose_tx, propose_rx) = mpsc::channel(client_inbox);
    let (read_tx, read_rx) = mpsc::channel(client_inbox);
    let (deliver_tx, deliver_rx) = mpsc::channel(peer_inbox);
    let (compact_tx, compact_rx) = mpsc::channel(client_inbox);
    let (reconfigure_tx, reconfigure_rx) = mpsc::channel(client_inbox);
    let (reconfigure_mm_tx, reconfigure_mm_rx) = mpsc::channel(client_inbox);
    let (inspect_tx, inspect_rx) = mpsc::channel(client_inbox);
    let (retire_tx, retire_rx) = mpsc::channel(client_inbox);
    (
        RpcService {
            propose: propose_tx,
            read: read_tx,
            deliver: deliver_tx,
            compact: compact_tx,
            reconfigure: reconfigure_tx,
            reconfigure_matchmakers: reconfigure_mm_tx,
            inspect: inspect_tx,
            retire: retire_tx,
            on_reject,
        },
        RpcInbox {
            propose: propose_rx,
            read: read_rx,
            deliver: deliver_rx,
            compact: compact_rx,
            reconfigure: reconfigure_rx,
            reconfigure_matchmakers: reconfigure_mm_rx,
            inspect: inspect_rx,
            retire: retire_rx,
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
    #[tracing::instrument(level = "debug", skip_all)]
    async fn propose(&self, request: Request<Propose>) -> Result<Response<ProposeAck>, Status> {
        let request = request.into_inner();
        if proposal_checksum(request.client, request.seq, &request.command) != request.checksum {
            tracing::warn!(
                client = request.client,
                seq = request.seq,
                "proposal_checksum_rejected"
            );
            (self.on_reject)(EdgeRejection::ProposalChecksum);
            return Err(Status::data_loss("invalid proposal checksum"));
        }
        dispatch(&self.propose, request).await.map(Response::new)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn read(&self, request: Request<Read>) -> Result<Response<ReadAck>, Status> {
        dispatch(&self.read, request.into_inner())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn compact(&self, request: Request<Compact>) -> Result<Response<CompactAck>, Status> {
        dispatch(&self.compact, request.into_inner())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure(
        &self,
        request: Request<Reconfigure>,
    ) -> Result<Response<ReconfigureAck>, Status> {
        dispatch(&self.reconfigure, request.into_inner())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure_matchmakers(
        &self,
        request: Request<ReconfigureMatchmakers>,
    ) -> Result<Response<ReconfigureMatchmakersAck>, Status> {
        dispatch(&self.reconfigure_matchmakers, request.into_inner())
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl internal::paros_internal_server::ParosInternal for RpcService {
    #[tracing::instrument(level = "trace", skip_all)]
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
                (self.on_reject)(EdgeRejection::MessageChecksum);
                return Err(Status::data_loss("invalid Paxos message checksum"));
            }
            let message = message_from_proto(message).map_err(|error| {
                (self.on_reject)(EdgeRejection::MessageDecode);
                Status::invalid_argument(format!("invalid Paxos message: {error}"))
            })?;
            dispatch(&self.deliver, message).await?;
        }
        Ok(Response::new(internal::DeliverAck {}))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectReply>, Status> {
        dispatch(&self.inspect, request.into_inner())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn retire(&self, request: Request<RetireRequest>) -> Result<Response<RetireAck>, Status> {
        dispatch(&self.retire, request.into_inner())
            .await
            .map(Response::new)
    }
}

// ---- the matchmaker contract ------------------------------------------------

fn mm_ballot_to_proto(ballot: Ballot) -> matchmaker::Ballot {
    matchmaker::Ballot {
        round: ballot.round,
        node: ballot.node.0,
    }
}

fn mm_ballot_from_proto(ballot: Option<matchmaker::Ballot>) -> Result<Ballot, &'static str> {
    let ballot = ballot.ok_or("missing ballot")?;
    Ok(Ballot {
        round: ballot.round,
        node: NodeId(ballot.node),
    })
}

fn acceptor_config_to_proto(config: &AcceptorConfig) -> matchmaker::AcceptorConfig {
    matchmaker::AcceptorConfig {
        members: config.members.iter().map(|n| n.0).collect(),
        quorum_system: match config.quorum_system {
            QuorumSystem::Majority => matchmaker::QuorumSystem::Majority.into(),
        },
    }
}

fn acceptor_config_from_proto(
    config: Option<matchmaker::AcceptorConfig>,
) -> Result<AcceptorConfig, &'static str> {
    let config = config.ok_or("missing acceptor configuration")?;
    let quorum_system = match matchmaker::QuorumSystem::try_from(config.quorum_system) {
        Ok(matchmaker::QuorumSystem::Majority) => QuorumSystem::Majority,
        Err(_) => return Err("unknown quorum system"),
    };
    if config.members.is_empty() {
        return Err("empty acceptor configuration");
    }
    Ok(AcceptorConfig::new(
        config.members.into_iter().map(NodeId).collect(),
        quorum_system,
    ))
}

fn mm_set_to_proto(set: &MatchmakerSet) -> matchmaker::MatchmakerSet {
    matchmaker::MatchmakerSet {
        generation: set.generation.0,
        members: set.members.iter().map(|m| m.0).collect(),
    }
}

fn mm_set_from_proto(
    set: Option<matchmaker::MatchmakerSet>,
) -> Result<MatchmakerSet, &'static str> {
    let set = set.ok_or("missing matchmaker set")?;
    if set.members.is_empty() {
        return Err("empty matchmaker set");
    }
    Ok(MatchmakerSet::new(
        MatchmakerGeneration(set.generation),
        set.members.into_iter().map(MatchmakerId).collect(),
    ))
}

fn registrations_to_proto(
    history: &BTreeMap<Ballot, Registration>,
) -> Vec<matchmaker::Registration> {
    history
        .iter()
        .map(|(ballot, registration)| matchmaker::Registration {
            ballot: Some(mm_ballot_to_proto(*ballot)),
            config: Some(acceptor_config_to_proto(&registration.config)),
            reconfiguration: registration.reconfiguration,
        })
        .collect()
}

fn registrations_from_proto(
    entries: Vec<matchmaker::Registration>,
) -> Result<BTreeMap<Ballot, Registration>, &'static str> {
    let mut history = BTreeMap::new();
    for entry in entries {
        let ballot = mm_ballot_from_proto(entry.ballot)?;
        let registration = Registration {
            config: acceptor_config_from_proto(entry.config)?,
            reconfiguration: entry.reconfiguration,
        };
        if history.insert(ballot, registration).is_some() {
            return Err("duplicate ballot in history");
        }
    }
    Ok(history)
}

/// Encode a matchmaking request for the wire.
#[must_use]
pub fn wire_match_request(request: &MatchRequest) -> WireMatchRequest {
    WireMatchRequest {
        from: request.from.0,
        ballot: Some(mm_ballot_to_proto(request.ballot)),
        config: Some(acceptor_config_to_proto(&request.config)),
        reconfiguration: request.reconfiguration,
        generation: request.generation.0,
    }
}

/// Validate and decode a matchmaking request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn match_request_from_wire(request: WireMatchRequest) -> Result<MatchRequest, &'static str> {
    let from = NodeId(request.from);
    let ballot = mm_ballot_from_proto(request.ballot)?;
    let config = acceptor_config_from_proto(request.config)?;
    let generation = MatchmakerGeneration(request.generation);
    Ok(if request.reconfiguration {
        MatchRequest::reconfigure(from, ballot, config, generation)
    } else {
        MatchRequest::new(from, ballot, config, generation)
    })
}

/// Encode a matchmaker's reply for the wire.
#[must_use]
pub fn wire_match_reply(reply: &MatchReply) -> WireMatchReply {
    let outcome = match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
        } => matchmaker::match_reply::Outcome::Registered(matchmaker::Registered {
            history: registrations_to_proto(history),
            gc_watermark: Some(mm_ballot_to_proto(*gc_watermark)),
        }),
        MatchOutcome::Refused(refusal) => {
            let reason = match refusal {
                MatchRefusal::Stale { highest } => {
                    matchmaker::refused::Reason::StaleHighest(mm_ballot_to_proto(*highest))
                }
                MatchRefusal::BelowWatermark { watermark } => {
                    matchmaker::refused::Reason::BelowWatermark(mm_ballot_to_proto(*watermark))
                }
                MatchRefusal::Stopped { successor } => {
                    matchmaker::refused::Reason::Stopped(matchmaker::RefusedStopped {
                        successor: successor.as_ref().map(mm_set_to_proto),
                    })
                }
                MatchRefusal::Generation { current } => {
                    matchmaker::refused::Reason::Generation(mm_set_to_proto(current))
                }
                MatchRefusal::Inactive => {
                    matchmaker::refused::Reason::Inactive(matchmaker::RefusedInactive {})
                }
            };
            matchmaker::match_reply::Outcome::Refused(matchmaker::Refused {
                reason: Some(reason),
            })
        }
    };
    WireMatchReply {
        matchmaker: reply.matchmaker.0,
        to: reply.to.0,
        ballot: Some(mm_ballot_to_proto(reply.ballot)),
        outcome: Some(outcome),
        generation: reply.generation.0,
    }
}

/// Validate and decode a matchmaker's reply from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn match_reply_from_wire(reply: WireMatchReply) -> Result<MatchReply, &'static str> {
    let outcome = match reply.outcome.ok_or("missing match outcome")? {
        matchmaker::match_reply::Outcome::Registered(registered) => MatchOutcome::Registered {
            history: registrations_from_proto(registered.history)?,
            gc_watermark: mm_ballot_from_proto(registered.gc_watermark)?,
        },
        matchmaker::match_reply::Outcome::Refused(refused) => {
            MatchOutcome::Refused(match refused.reason.ok_or("missing refusal reason")? {
                matchmaker::refused::Reason::StaleHighest(highest) => MatchRefusal::Stale {
                    highest: mm_ballot_from_proto(Some(highest))?,
                },
                matchmaker::refused::Reason::BelowWatermark(watermark) => {
                    MatchRefusal::BelowWatermark {
                        watermark: mm_ballot_from_proto(Some(watermark))?,
                    }
                }
                matchmaker::refused::Reason::Stopped(stopped) => MatchRefusal::Stopped {
                    successor: stopped
                        .successor
                        .map(|s| mm_set_from_proto(Some(s)))
                        .transpose()?,
                },
                matchmaker::refused::Reason::Generation(current) => MatchRefusal::Generation {
                    current: mm_set_from_proto(Some(current))?,
                },
                matchmaker::refused::Reason::Inactive(_) => MatchRefusal::Inactive,
            })
        }
    };
    Ok(MatchReply {
        matchmaker: MatchmakerId(reply.matchmaker),
        to: NodeId(reply.to),
        ballot: mm_ballot_from_proto(reply.ballot)?,
        generation: MatchmakerGeneration(reply.generation),
        outcome,
    })
}

/// Encode a garbage-collection request for the wire.
#[must_use]
pub fn wire_garbage_collect(request: &GcRequest) -> WireGarbageCollect {
    WireGarbageCollect {
        from: request.from.0,
        watermark: Some(mm_ballot_to_proto(request.watermark)),
        generation: request.generation.0,
    }
}

/// Decode a garbage-collection request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn garbage_collect_from_wire(request: WireGarbageCollect) -> Result<GcRequest, &'static str> {
    Ok(GcRequest {
        from: NodeId(request.from),
        generation: MatchmakerGeneration(request.generation),
        watermark: mm_ballot_from_proto(request.watermark)?,
    })
}

/// Encode a garbage-collection acknowledgement for the wire.
#[must_use]
pub fn wire_garbage_collect_ack(ack: &GcAck) -> WireGarbageCollectAck {
    WireGarbageCollectAck {
        matchmaker: ack.matchmaker.0,
        watermark: Some(mm_ballot_to_proto(ack.watermark)),
        generation: ack.generation.0,
        applied: ack.applied,
    }
}

/// Decode a garbage-collection acknowledgement.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn garbage_collect_ack_from_wire(ack: WireGarbageCollectAck) -> Result<GcAck, &'static str> {
    Ok(GcAck {
        matchmaker: MatchmakerId(ack.matchmaker),
        generation: MatchmakerGeneration(ack.generation),
        applied: ack.applied,
        watermark: mm_ballot_from_proto(ack.watermark)?,
    })
}

fn bootstrap_to_proto(bootstrap: &PendingBootstrap) -> matchmaker::Bootstrap {
    matchmaker::Bootstrap {
        set: Some(mm_set_to_proto(&bootstrap.set)),
        gc_watermark: Some(mm_ballot_to_proto(bootstrap.gc_watermark)),
        history: registrations_to_proto(&bootstrap.history),
    }
}

fn bootstrap_from_proto(
    bootstrap: matchmaker::Bootstrap,
) -> Result<PendingBootstrap, &'static str> {
    Ok(PendingBootstrap {
        set: mm_set_from_proto(bootstrap.set)?,
        gc_watermark: mm_ballot_from_proto(bootstrap.gc_watermark)?,
        history: registrations_from_proto(bootstrap.history)?,
    })
}

fn phase_to_proto(phase: MatchmakerPhase) -> i32 {
    match phase {
        MatchmakerPhase::Fresh => matchmaker::MatchmakerPhase::Fresh,
        MatchmakerPhase::Inactive => matchmaker::MatchmakerPhase::Inactive,
        MatchmakerPhase::Active => matchmaker::MatchmakerPhase::Active,
        MatchmakerPhase::Stopped => matchmaker::MatchmakerPhase::Stopped,
    }
    .into()
}

fn phase_from_proto(phase: i32) -> Result<MatchmakerPhase, &'static str> {
    match matchmaker::MatchmakerPhase::try_from(phase) {
        Ok(matchmaker::MatchmakerPhase::Fresh) => Ok(MatchmakerPhase::Fresh),
        Ok(matchmaker::MatchmakerPhase::Inactive) => Ok(MatchmakerPhase::Inactive),
        Ok(matchmaker::MatchmakerPhase::Active) => Ok(MatchmakerPhase::Active),
        Ok(matchmaker::MatchmakerPhase::Stopped) => Ok(MatchmakerPhase::Stopped),
        Err(_) => Err("unknown matchmaker phase"),
    }
}

/// Encode a reconfigurer's request for the wire.
#[must_use]
pub fn wire_reconfigure_request(request: &ReconfigureRequest) -> WireReconfigureRequest {
    use matchmaker::reconfigure_request::Kind;
    let kind = match request {
        ReconfigureRequest::Stop { generation, .. } => Kind::Stop(matchmaker::Stop {
            generation: generation.0,
        }),
        ReconfigureRequest::Bootstrap { bootstrap, .. } => {
            Kind::Bootstrap(bootstrap_to_proto(bootstrap))
        }
        ReconfigureRequest::DecreePrepare {
            generation, ballot, ..
        } => Kind::DecreePrepare(matchmaker::DecreePrepare {
            generation: generation.0,
            ballot: Some(mm_ballot_to_proto(*ballot)),
        }),
        ReconfigureRequest::DecreeAccept {
            generation,
            ballot,
            members,
            ..
        } => Kind::DecreeAccept(matchmaker::DecreeAccept {
            generation: generation.0,
            ballot: Some(mm_ballot_to_proto(*ballot)),
            members: members.iter().map(|m| m.0).collect(),
        }),
        ReconfigureRequest::Chosen {
            generation,
            successor,
            ..
        } => Kind::Chosen(matchmaker::Chosen {
            generation: generation.0,
            successor: Some(mm_set_to_proto(successor)),
        }),
    };
    WireReconfigureRequest {
        from: request.from().0,
        kind: Some(kind),
    }
}

/// Validate and decode a reconfigurer's request from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn reconfigure_request_from_wire(
    request: WireReconfigureRequest,
) -> Result<ReconfigureRequest, &'static str> {
    use matchmaker::reconfigure_request::Kind;
    let from = NodeId(request.from);
    Ok(
        match request.kind.ok_or("missing reconfigure request kind")? {
            Kind::Stop(stop) => ReconfigureRequest::Stop {
                from,
                generation: MatchmakerGeneration(stop.generation),
            },
            Kind::Bootstrap(bootstrap) => ReconfigureRequest::Bootstrap {
                from,
                bootstrap: bootstrap_from_proto(bootstrap)?,
            },
            Kind::DecreePrepare(prepare) => ReconfigureRequest::DecreePrepare {
                from,
                generation: MatchmakerGeneration(prepare.generation),
                ballot: mm_ballot_from_proto(prepare.ballot)?,
            },
            Kind::DecreeAccept(accept) => {
                if accept.members.is_empty() {
                    return Err("empty decree proposal");
                }
                ReconfigureRequest::DecreeAccept {
                    from,
                    generation: MatchmakerGeneration(accept.generation),
                    ballot: mm_ballot_from_proto(accept.ballot)?,
                    members: accept.members.into_iter().map(MatchmakerId).collect(),
                }
            }
            Kind::Chosen(chosen) => ReconfigureRequest::Chosen {
                from,
                generation: MatchmakerGeneration(chosen.generation),
                successor: mm_set_from_proto(chosen.successor)?,
            },
        },
    )
}

/// Encode a matchmaker's reconfiguration reply for the wire.
#[must_use]
pub fn wire_reconfigure_reply(reply: &ReconfigureReply) -> WireReconfigureReply {
    use matchmaker::reconfigure_reply::Kind;
    let kind = match reply {
        ReconfigureReply::Stopped {
            generation,
            gc_watermark,
            history,
            successor,
            decree_promised,
            ..
        } => Kind::Stopped(matchmaker::StopAck {
            generation: generation.0,
            gc_watermark: Some(mm_ballot_to_proto(*gc_watermark)),
            history: registrations_to_proto(history),
            successor: successor.as_ref().map(mm_set_to_proto),
            decree_promised: Some(mm_ballot_to_proto(*decree_promised)),
        }),
        ReconfigureReply::Bootstrapped { set, .. } => {
            Kind::Bootstrapped(matchmaker::BootstrapAck {
                set: Some(mm_set_to_proto(set)),
            })
        }
        ReconfigureReply::Promised {
            generation,
            ballot,
            vote,
            ..
        } => Kind::Promised(matchmaker::DecreePromise {
            generation: generation.0,
            ballot: Some(mm_ballot_to_proto(*ballot)),
            vote: vote.as_ref().map(|(b, members)| matchmaker::DecreeVote {
                ballot: Some(mm_ballot_to_proto(*b)),
                members: members.iter().map(|m| m.0).collect(),
            }),
        }),
        ReconfigureReply::Accepted {
            generation, ballot, ..
        } => Kind::Accepted(matchmaker::DecreeAccepted {
            generation: generation.0,
            ballot: Some(mm_ballot_to_proto(*ballot)),
        }),
        ReconfigureReply::Nacked {
            generation,
            ballot,
            promised,
            ..
        } => Kind::Nacked(matchmaker::DecreeNack {
            generation: generation.0,
            ballot: Some(mm_ballot_to_proto(*ballot)),
            promised: Some(mm_ballot_to_proto(*promised)),
        }),
        ReconfigureReply::Learned {
            generation,
            activated,
            ..
        } => Kind::Learned(matchmaker::Learned {
            generation: generation.0,
            activated: *activated,
        }),
        ReconfigureReply::Refused {
            current,
            phase,
            successor,
            ..
        } => Kind::Refused(matchmaker::ReconfigureRefused {
            current: Some(mm_set_to_proto(current)),
            phase: phase_to_proto(*phase),
            successor: successor.as_ref().map(mm_set_to_proto),
        }),
    };
    WireReconfigureReply {
        matchmaker: reply.matchmaker().0,
        kind: Some(kind),
    }
}

/// Validate and decode a matchmaker's reconfiguration reply from the wire.
///
/// # Errors
/// Returns a static description of the first malformed field.
pub fn reconfigure_reply_from_wire(
    reply: WireReconfigureReply,
) -> Result<ReconfigureReply, &'static str> {
    use matchmaker::reconfigure_reply::Kind;
    let matchmaker = MatchmakerId(reply.matchmaker);
    Ok(match reply.kind.ok_or("missing reconfigure reply kind")? {
        Kind::Stopped(ack) => ReconfigureReply::Stopped {
            matchmaker,
            generation: MatchmakerGeneration(ack.generation),
            gc_watermark: mm_ballot_from_proto(ack.gc_watermark)?,
            history: registrations_from_proto(ack.history)?,
            successor: ack
                .successor
                .map(|s| mm_set_from_proto(Some(s)))
                .transpose()?,
            decree_promised: mm_ballot_from_proto(ack.decree_promised)?,
        },
        Kind::Bootstrapped(ack) => ReconfigureReply::Bootstrapped {
            matchmaker,
            set: mm_set_from_proto(ack.set)?,
        },
        Kind::Promised(promise) => ReconfigureReply::Promised {
            matchmaker,
            generation: MatchmakerGeneration(promise.generation),
            ballot: mm_ballot_from_proto(promise.ballot)?,
            vote: promise
                .vote
                .map(|vote| {
                    if vote.members.is_empty() {
                        return Err("empty decree vote");
                    }
                    Ok((
                        mm_ballot_from_proto(vote.ballot)?,
                        vote.members.into_iter().map(MatchmakerId).collect(),
                    ))
                })
                .transpose()?,
        },
        Kind::Accepted(accepted) => ReconfigureReply::Accepted {
            matchmaker,
            generation: MatchmakerGeneration(accepted.generation),
            ballot: mm_ballot_from_proto(accepted.ballot)?,
        },
        Kind::Nacked(nack) => ReconfigureReply::Nacked {
            matchmaker,
            generation: MatchmakerGeneration(nack.generation),
            ballot: mm_ballot_from_proto(nack.ballot)?,
            promised: mm_ballot_from_proto(nack.promised)?,
        },
        Kind::Learned(learned) => ReconfigureReply::Learned {
            matchmaker,
            generation: MatchmakerGeneration(learned.generation),
            activated: learned.activated,
        },
        Kind::Refused(refused) => ReconfigureReply::Refused {
            matchmaker,
            current: mm_set_from_proto(refused.current)?,
            phase: phase_from_proto(refused.phase)?,
            successor: refused
                .successor
                .map(|s| mm_set_from_proto(Some(s)))
                .transpose()?,
        },
    })
}

/// Matchmaker requests accepted concurrently by tonic and consumed serially by
/// the matchmaker driver, which exclusively owns the sans-IO core.
pub(crate) struct MatchmakerInbox {
    pub(crate) requests: mpsc::Receiver<Call<MatchRequest, MatchReply>>,
    pub(crate) collects: mpsc::Receiver<Call<GcRequest, GcAck>>,
    pub(crate) reconfigures: mpsc::Receiver<Call<ReconfigureRequest, ReconfigureReply>>,
}

/// Cloneable tonic handler for the matchmaker contract; each method forwards
/// into [`MatchmakerInbox`] and holds the response open until the driver
/// answers.
#[derive(Clone)]
pub(crate) struct MatchmakerService {
    requests: mpsc::Sender<Call<MatchRequest, MatchReply>>,
    collects: mpsc::Sender<Call<GcRequest, GcAck>>,
    reconfigures: mpsc::Sender<Call<ReconfigureRequest, ReconfigureReply>>,
}

/// Construct a matchmaker handler/inbox pair; `capacity` bounds each queue
/// (at least 1).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn matchmaker_channel(capacity: usize) -> (MatchmakerService, MatchmakerInbox) {
    let (requests_tx, requests_rx) = mpsc::channel(capacity);
    let (collects_tx, collects_rx) = mpsc::channel(capacity);
    let (reconfigures_tx, reconfigures_rx) = mpsc::channel(capacity);
    (
        MatchmakerService {
            requests: requests_tx,
            collects: collects_tx,
            reconfigures: reconfigures_tx,
        },
        MatchmakerInbox {
            requests: requests_rx,
            collects: collects_rx,
            reconfigures: reconfigures_rx,
        },
    )
}

#[tonic::async_trait]
impl matchmaker::paros_matchmaker_server::ParosMatchmaker for MatchmakerService {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn matchmake(
        &self,
        request: Request<WireMatchRequest>,
    ) -> Result<Response<WireMatchReply>, Status> {
        let request = match_request_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(format!("invalid match request: {error}")))?;
        dispatch(&self.requests, request)
            .await
            .map(|reply| Response::new(wire_match_reply(&reply)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn garbage_collect(
        &self,
        request: Request<WireGarbageCollect>,
    ) -> Result<Response<WireGarbageCollectAck>, Status> {
        let request = garbage_collect_from_wire(request.into_inner()).map_err(|error| {
            Status::invalid_argument(format!("invalid garbage-collect request: {error}"))
        })?;
        dispatch(&self.collects, request)
            .await
            .map(|ack| Response::new(wire_garbage_collect_ack(&ack)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure(
        &self,
        request: Request<WireReconfigureRequest>,
    ) -> Result<Response<WireReconfigureReply>, Status> {
        let request = reconfigure_request_from_wire(request.into_inner()).map_err(|error| {
            Status::invalid_argument(format!("invalid reconfigure request: {error}"))
        })?;
        dispatch(&self.reconfigures, request)
            .await
            .map(|reply| Response::new(wire_reconfigure_reply(&reply)))
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
