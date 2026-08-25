//! The provider-generic node driver — the `Node` layer that owns the sans-IO
//! [`paros_core::RawNode`] and performs all I/O.
//!
//! Written once over moonpool's `P: Providers` abstraction, so the *same* loop
//! runs in production (`TokioProviders`) and deterministic simulation
//! (`SimProviders`). The sim harness (`paros-sim`) adapts a moonpool `Process`
//! to it; a future `parosd` binary will adapt a tokio `main`.
//!
//! The loop `select`s over {client request, peer message, tick timer, shutdown},
//! feeds the core via `step`/`tick`, and drains every [`paros_core::Ready`] in
//! persist → send → apply → advance order (durable-before-send). It also draws
//! the randomized election timeout from the provider RNG (the core stays
//! dependency-free) and holds each client reply until its slot commits
//! (ack-on-commit), redirecting non-leader proposals.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{
    Detach, NetworkProvider, Providers, RandomProvider, SimulationError, SimulationResult,
    TaskProvider, TcpListenerTrait, TimeProvider,
};
use moonpool_hyper::{ChannelConfig, H2Server, H2ServerConfig, KeepAlive, ReconnectingChannel};
use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Control, Message, NodeId, NodeRole, ProposeResult,
    RawNode, ReadIndexResult, ReadState, SessionEntry, Slot, Value, WriteOp,
};
use prost::Message as ProstMessage;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::grpc::{
    CompactAck, InspectReply, ParosInternalClient, ParosInternalServer, ParosServer, ProposeAck,
    ReadAck, ReplySender, RpcInbox, internal, message_to_proto, rpc_channel, wire_checksum,
};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::storage::{NodeStorage, StorageError};

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

/// h2 liveness detection for peer streams. Both values use provider time, so a
/// half-open connection is replaced deterministically during the settle tail.
const GRPC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(2);
const GRPC_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(1);
const GRPC_DELIVERY_TIMEOUT: Duration = Duration::from_secs(1);
/// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this is
/// deliberately bounded and lossy: the consensus driver never waits for
/// network I/O, and current heartbeats/resends repair anything dropped here.
const GRPC_PEER_QUEUE_CAPACITY: usize = 4096;
/// Snapshot offers use an independent h2 request lane so their opaque bytes
/// cannot sit in front of heartbeats and normal replication.
const GRPC_SNAPSHOT_QUEUE_CAPACITY: usize = 4;
/// Leave headroom below tonic's default 4 MiB decoded-message limit for the
/// protobuf envelope. The retired transport capped a complete payload at 1 MiB;
/// this preserves that per-message envelope while allowing compact batches.
const GRPC_DELIVERY_BATCH_BYTES: usize = 3 * 1024 * 1024;
/// Maximum Paxos messages packed into one protobuf/gRPC request. This keeps
/// a chatty heartbeat/catch-up round from creating one h2 frame per message.
const GRPC_DELIVERY_BATCH: usize = 64;

/// Run one synchronous cleanup action on every exit path from its scope.
struct OnDrop<F: FnOnce()> {
    action: Option<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    fn new(action: F) -> Self {
        Self {
            action: Some(action),
        }
    }
}

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

fn grpc_keep_alive() -> KeepAlive {
    KeepAlive {
        interval: GRPC_KEEP_ALIVE_INTERVAL,
        timeout: GRPC_KEEP_ALIVE_TIMEOUT,
        while_idle: false,
    }
}

fn grpc_channel_config() -> ChannelConfig {
    ChannelConfig {
        connection_timeout: GRPC_DELIVERY_TIMEOUT,
        keep_alive: Some(grpc_keep_alive()),
        ..ChannelConfig::default()
    }
}

/// Ticks a parked read reply may wait for its read-index confirmation before
/// the driver answers a retry redirect (500 ms — well inside the sim client's
/// 1000 ms deadline, and inside the core's own round TTL, so a late core
/// confirmation just finds the ctx gone and is ignored).
const READ_RETRY_TICKS: u64 = 10;

/// Base election timeout, in ticks. Each node's actual timeout is drawn
/// uniformly from `[T, 2T)` (jitter from the [`RandomProvider`], in the driver,
/// never the zero-dep core) to break the dueling-proposer livelock. `T`
/// dominates the core's heartbeat interval, so a live leader always beats before
/// a follower's election clock fires.
const ELECTION_TIMEOUT_BASE: u64 = 5;

/// Tracing event name for a node logical-clock tick. Emitters use the string
/// literal (tracing requires one); readers (oracles) match on this constant.
pub const EV_NODE_TICK: &str = "node_tick";

/// Tracing event: this node raised its durable promised ballot. Carries `node`
/// (id) and the promised ballot (`pround`/`pbnode`). The safety oracle reads it
/// for the monotonic-promise invariant. (Per-slot accepted state is surfaced
/// separately by [`EV_PERSIST`], so never-accept-below-promise is checked across
/// the whole log, not just slot 0.)
pub const EV_NODE_STATE: &str = "node_state";

/// Tracing event: this node durably persisted an accepted `(ballot, entry)` for a
/// slot (a `WriteOp::AppendAccepted`). Carries `node`, `slot`, the node's current
/// promised ballot (`pround`/`pbnode`), the accepted ballot (`around`/`abnode`),
/// and the value hash (`vhash`). The safety oracle reads it for the
/// never-accept-below-promise invariant per slot; the recovery oracle reads it to
/// check a pre-crash accepted `(slot -> value)` is stable across a restart.
pub const EV_PERSIST: &str = "persist";

/// Tracing event: on (re)boot, this node recovered an accepted record from
/// durable storage. Carries `node`, `slot`, the accepted ballot (`around`/
/// `abnode`), and the value hash (`vhash`). The recovery oracle reads it to check
/// a restart never changes a pre-crash accepted `(slot -> value)`.
pub const EV_RECOVERED: &str = "recovered";

/// Tracing event: this node started an incarnation, having rebuilt its volatile
/// state from durable storage. Carries `node`. Fires on the initial boot and on
/// every restart (a seam-crash recovery re-run or an attrition process kill), so
/// it is the reliable "this node came (back) up" marker. Purely observational: no
/// oracle asserts on it; the recovery recorder derives per-node *restarts* from it
/// (every `booted` after the first). The crash/restart animation reads it.
pub const EV_BOOTED: &str = "booted";

/// Tracing event: this node crashed at a durability seam inside a `Ready` batch
/// (a `buggify`-injected [`Seam`] crash). Carries `node` and `seam`
/// (`"before_sync"` — the whole un-synced batch is lost — or
/// `"after_sync_before_send"` — the writes are durable but the batch's messages
/// never left). After-sync events also carry `snapshot_offers`, the number of
/// snapshot transfers dropped with the batch. Provider-generic but inert in production, where
/// [`NoHooks`](crate::NoHooks) never fires. Purely observational; the crash
/// animation reads it to mark the persist/send seam a node died on.
pub const EV_CRASHED: &str = "crashed";

/// Tracing event: the driver deliberately skipped re-sending one or more
/// pending `Accept`s on this beat.
pub const EV_RESEND_SKIPPED: &str = "accept_resend_skipped";

/// Tracing event: the driver deliberately asked the current leader to resign.
pub const EV_LEADERSHIP_RESIGNED: &str = "leadership_resigned";

/// Tracing event: a snapshot install persisted while this node was a live
/// Candidate (`role == Candidate`, election open). This is the #88 window —
/// `on_install_snapshot` may raise the candidate's promise above the ballot it
/// is campaigning at — surfaced so the sweep can prove the interleaving is
/// actually visited. Carries `node`.
pub const EV_SNAPSHOT_MID_ELECTION: &str = "snapshot_mid_election";

/// Tracing event: the driver deliberately dropped one outbound protocol message
/// at the send seam (after durability, before the transport). Carries `node`,
/// `to`, `kind`, and for an `Accept` the `slot`. Indistinguishable from network
/// loss to the peers; emitted so the sweep can prove the per-message-loss
/// BUGGIFY location is active and so a trace shows why a message never arrived.
pub const EV_SEND_DROPPED: &str = "msg_dropped_at_send";

/// Tracing event: the driver deliberately sent one outbound protocol message
/// twice at the send seam. Carries `node`, `to`, `kind`. Retransmission is
/// legal transport behavior; the sweep uses it to prove set-based quorum
/// counting tolerates duplicates.
pub const EV_SEND_DUPLICATED: &str = "msg_duplicated_at_send";

/// Tracing event: the driver deliberately dropped one client-facing reply
/// after the server state advanced. Carries `node` and `reply`
/// (`propose`/`propose_dedup`/`read`). The client's retry takes the
/// `(client, seq)` dedup path, which is the edge this exists to exercise.
pub const EV_CLIENT_REPLY_DROPPED: &str = "client_reply_dropped";

/// Tracing event: the driver selected the shortest valid election timeout.
/// Carries `node` and `ticks`. The driver-hook oracle uses it to prove the
/// timeout-jitter BUGGIFY location is active.
pub const EV_ELECTION_TIMEOUT_EXTREME: &str = "election_timeout_extreme";

/// Tracing event: this node flushed a `Ready` batch's durable writes. Carries
/// `node`, `sync` (whether the batch required an fsync-before-send —
/// [`MustSync::Sync`] — or a relaxed write), and `writes` (op count). Emitted once
/// per non-empty batch, right after the flush. Purely observational; it is the
/// "was this batch fsync'd?" marker the persist/send-seam animation renders.
pub const EV_SYNCED: &str = "synced";

/// Tracing event: this node applied a chosen value. Carries `node`, `slot`, and
/// the value hash (`vhash`). The safety oracle reads it for the
/// at-most-one-value-chosen invariant.
pub const EV_CHOSEN: &str = "value_chosen";

/// Tracing event: this node sent a protocol message. Carries `node` (sender),
/// `to` (destination), and `kind`; for the six ballot-carrying Paxos kinds it
/// also carries the ballot (`bround`/`bnode`) and `slot`. An `accept` additionally
/// carries the proposed command's `vhash` — the only message that proposes a
/// value, and so the only one whose hash the safety oracle needs to check that one
/// ballot proposes at most one command per slot. The wasm demo pairs it with
/// [`EV_MSG_RECV`] to draw the protocol timeline.
pub const EV_MSG_SENT: &str = "msg_sent";

/// Tracing event: this node received a protocol message (the mirror of
/// [`EV_MSG_SENT`]). Carries `node` (receiver), `from` (sender), and `kind`; for
/// the six ballot-carrying Paxos kinds it also carries `bround`/`bnode`/`slot`. A
/// sent message with no matching receive is one the network dropped.
pub const EV_MSG_RECV: &str = "msg_received";

/// Tracing event: this node became leader. Carries `node`, the won ballot
/// (`round`/`bnode`) and the promise it held at the instant of victory
/// (`pround`/`pbnode`). The leadership oracle asserts per-node ballot
/// monotonicity, and — the #67 check — that a fresh leader's promise never sits
/// above the ballot it just won.
pub const EV_LEADER: &str = "leader_elected";

/// Tracing event: this node advanced its applied (contiguous chosen) prefix.
/// Carries `node`, `slot` (the slot just applied), and `applied_index` (the new
/// high-water mark). The no-gaps oracle asserts the prefix grows by one without
/// skipping.
pub const EV_APPLIED: &str = "log_applied";

/// Tracing event: this node durably truncated its log prefix (a
/// `WriteOp::Truncate`). Carries `node` and `first` (the new compaction floor:
/// the first slot still retained). Emitted only after the fsync, like
/// [`EV_PERSIST`], so it never claims a truncation a `BeforeSync` crash discards.
/// The truncation oracle reads it to check the log stays bounded and nothing
/// below the floor is ever persisted or recovered again.
pub const EV_COMPACTED: &str = "compacted";

/// Tracing event: this node installed an opaque application snapshot from a peer
/// (a `WriteOp::InstallSnapshot`), jumping its chosen prefix. Carries `node`,
/// `chosen_index` (the commit index the snapshot brought it up to), and `first`
/// (the new compaction floor). Emitted only after the install is fsync'd. The
/// snapshot oracle reads it to confirm a below-floor node recovered, and the
/// no-gaps oracle reads it to admit the applied-index jump the install performs.
pub const EV_SNAPSHOT_INSTALLED: &str = "snapshot_installed";

/// Tracing event: the driver materialized one or more opaque snapshot offers as
/// outbound protocol messages. Carries `node` and `snapshot_offers`. Emitted
/// before the after-sync-before-send seam, so the driver-hook oracle can prove
/// snapshot transfers use the common outbound path.
pub const EV_SNAPSHOT_OFFERED: &str = "snapshot_offered";

/// Tracing event: this node, on winning an election, filled at least one undecided
/// hole in the recovered suffix with a [`Control::Noop`]. Carries `node`, `round`
/// (the ballot round it now leads at) and `gaps` (how many slots it filled). The
/// gap-fill oracle reads it as the reachability gate proving the fill path is
/// genuinely exercised, not merely present.
pub const EV_GAP_FILLED: &str = "election_gap_filled";

/// Tracing event: this node holds a **chosen gap** — a slot it knows is chosen
/// sitting above its contiguous applied prefix (see [`RawNode::chosen_gap`]).
/// Carries `node`, `hole` (the first slot missing from the prefix) and `above`
/// (the highest chosen slot past it). Emitted once per tick while the gap exists,
/// so its *persistence* is what the trace records, not a single instant. A gap is
/// an ordinary transient (pipelining, a missed `Commit`); one that outlives
/// quiescence is a wedged cluster, which is what the gap-fill oracle asserts
/// against. Purely observational.
pub const EV_CHOSEN_GAP: &str = "chosen_gap";

/// Tracing event: this node received a `Prepare` whose `from_slot` is below its
/// own compaction floor. Carries `node`, `from_slot`, and `floor`. Purely
/// observational: it marks that the dangerous "campaign against a truncated
/// acceptor" interleaving was reached, so the sweep can assert it stays reachable
/// once the acceptor floor guard is in place.
pub const EV_PREPARE_BELOW_FLOOR: &str = "prepare_below_floor";

/// Tracing event: this node's apply seam executed a chosen slot as a no-op
/// because its `(client, seq)` identity had already applied at a lower slot —
/// the #94 double-apply, suppressed. Carries `node` and `count` (suppressions in
/// the batch). Rare and mechanism-specific: it is the only outside evidence the
/// at-most-once suppression path ran.
pub const EV_DUPLICATE_SUPPRESSED: &str = "duplicate_suppressed";

/// Tracing event: this node, as Leader, spent a full election-timeout window
/// without hearing an ack quorum and demoted itself (CheckQuorum, #95). Carries
/// `node` and `count` (step-downs in the batch — in practice 1). The zombie
/// leader this bounds is the feeder of #94's stale-suffix interleaving.
pub const EV_QUORUM_LOST: &str = "leader_quorum_lost";

/// Tracing event: a client proposal was answered by the **dedup fast path** —
/// the `(client, seq)` was already applied here, so the reply fired immediately
/// instead of being parked on a slot ([`ProposeResult::Chosen`]). Carries `node`
/// and the `slot` the ack names. Purely observational: this is the one committed
/// ack that does not come out of the apply loop, so the sweep needs evidence it
/// is genuinely reached (and the ack oracle needs it named, not hidden).
pub const EV_PROPOSE_DEDUP_ACK: &str = "propose_dedup_ack";

/// Parse an IP (which may lack a port) into a socket-address string, defaulting to
/// port 4500 (the moonpool sim convention; production supplies a full address).
///
/// # Errors
///
/// Returns an error if `ip` is not a parseable network address.
pub fn parse_addr(ip: &str) -> SimulationResult<String> {
    let addr_str = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4500")
    };
    addr_str
        .parse::<SocketAddr>()
        .map(|addr| addr.to_string())
        .map_err(|e| SimulationError::InvalidState(format!("bad addr: {e}")))
}

/// A stable `u64` digest of a value's bytes (FNV-1a), emitted on observability
/// events so the safety oracle can compare chosen values by equality without
/// carrying the raw payload through the trace.
fn value_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The value hash for a decided [`Command`], for observability. A client entry
/// hashes its opaque value bytes; a control command hashes a stable, distinct
/// encoding of its metadata, so every node agrees on the per-slot hash the safety
/// oracle compares (a control command decided for a slot is the same on all
/// nodes).
///
/// Public so an [`Audit`] implementation can hash a `Command` it observes on the
/// wire ([`Audit::sent`]) with the *same* function the driver uses for the
/// durable-write and apply callbacks.
#[must_use]
pub fn command_hash(command: &Command) -> u64 {
    match command {
        Command::User(entry) => value_hash(&entry.value.0),
        Command::Control(Control::Truncate { up_to }) => {
            let mut bytes = vec![0xff_u8];
            bytes.extend_from_slice(&up_to.0.to_le_bytes());
            value_hash(&bytes)
        }
        // A distinct one-byte tag: no `Truncate` encoding can collide with it (they
        // are nine bytes and start `0xff`), and every node hashes the same no-op to
        // the same digest, so per-slot prefix agreement stays checkable.
        Command::Control(Control::Noop) => value_hash(&[0xfe_u8]),
    }
}

/// A short, stable label for a [`Message`] variant, for observability: the `kind`
/// field on the `msg_sent` / `msg_received` events.
fn message_kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Nack { .. } => "nack",
        Message::Commit { .. } => "commit",
        Message::CatchUpRequest { .. } => "catchup_request",
        Message::CatchUpResponse { .. } => "catchup_response",
        Message::InstallSnapshot { .. } => "install_snapshot",
        Message::CheckLeader { .. } => "check_leader",
        Message::Heartbeat { .. } => "heartbeat",
        Message::HeartbeatAck { .. } => "heartbeat_ack",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. Every ballot-carrying kind returns `Some`, `Heartbeat`
/// included — its "slot" is the commit watermark it advertises, which is
/// `None` on a leader that has chosen nothing (an empty prefix is not slot 0;
/// see [`paros_core::Message::Heartbeat`]). The kinds with no ballot at all
/// (`CheckLeader`, the catch-up pair) return `None` outright.
fn message_route(m: &Message) -> Option<(NodeId, Ballot, Option<Slot>)> {
    match m {
        // Phase 1 is per-ballot: report `from_slot` as the slot for the timeline.
        Message::Prepare {
            from,
            ballot,
            from_slot,
        }
        | Message::Promise {
            from,
            ballot,
            from_slot,
            ..
        } => Some((*from, *ballot, Some(*from_slot))),
        Message::Accept {
            from, ballot, slot, ..
        }
        | Message::Accepted { from, ballot, slot }
        | Message::Nack {
            from, ballot, slot, ..
        }
        | Message::Commit {
            from, ballot, slot, ..
        } => Some((*from, *ballot, Some(*slot))),
        Message::Heartbeat {
            from,
            ballot,
            commit,
            ..
        } => Some((*from, *ballot, *commit)),
        Message::InstallSnapshot {
            from,
            ballot,
            chosen_index,
            ..
        } => Some((*from, *ballot, Some(*chosen_index))),
        _ => None,
    }
}

/// The client replies this node is holding open: proposals wait on their
/// slot's commit (ack-on-commit), reads wait on their read-index round's
/// confirmation `(client seq, tick parked at, the held reply)`, keyed by the
/// core's `ctx` token.
#[derive(Default)]
struct ClientWaiters {
    /// `(client id, client seq, the held reply)` per slot.
    pending: BTreeMap<Slot, Vec<(u64, u64, ReplySender<ProposeAck>)>>,
    pending_reads: BTreeMap<u64, (u64, u64, ReplySender<ReadAck>)>,
}

/// The driver's outbound side: everything needed to put one message on the wire —
/// the gRPC clients, the task provider, and this node's id for observability
/// events. Bundled so `drain_ready` takes one parameter instead of three.
struct PeerQueues {
    regular: tokio::sync::mpsc::Sender<internal::ConsensusMessage>,
    snapshot: tokio::sync::mpsc::Sender<internal::ConsensusMessage>,
}

struct Outbound {
    peer_queues: BTreeMap<NodeId, PeerQueues>,
    /// This node's id, for the observability events.
    self_id: u64,
}

impl Outbound {
    /// Hand `msg` to the lossy per-peer transport and surface the protocol send.
    /// `msg_sent` deliberately records the core's outbound decision even when
    /// the bounded mailbox or network later drops it; safety oracles inspect the
    /// messages a proposer attempted, independently of delivery.
    fn transmit<A: Audit>(&self, audit: &A, to: NodeId, msg: &Message) {
        audit.sent(NodeId(self.self_id), to, msg);
        let kind = message_kind(msg);
        // An `Accept` is the only message that carries a *proposal*, so it is the
        // only one whose command hash the trace needs: it is what lets an oracle
        // check the Phase-2 half of P2b — one ballot proposes at most one command
        // per slot — a claim no other event can show, because the anomaly it
        // guards against (#67) puts two commands for one `(ballot, slot)` on the
        // wire without either ever being accepted or chosen.
        match msg {
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => tracing::info!(
                node = self.self_id,
                to = to.0,
                kind,
                bround = ballot.round,
                bnode = ballot.node.0,
                slot = slot.0,
                vhash = command_hash(command),
                "msg_sent"
            ),
            _ => match message_route(msg) {
                Some((_, ballot, Some(slot))) => tracing::info!(
                    node = self.self_id,
                    to = to.0,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    "msg_sent"
                ),
                // A beat from a leader whose chosen prefix is still empty: there is
                // no slot to report, and reporting a bare `0` would put back on the
                // trace exactly the sentinel #56 took off the wire.
                Some((_, ballot, None)) => tracing::info!(
                    node = self.self_id,
                    to = to.0,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    "msg_sent"
                ),
                None => tracing::info!(node = self.self_id, to = to.0, kind, "msg_sent"),
            },
        }
        if let Some(queues) = self.peer_queues.get(&to) {
            let Ok(message) = message_to_proto(msg) else {
                tracing::warn!(
                    node = self.self_id,
                    to = to.0,
                    "failed to encode Paxos message"
                );
                return;
            };
            let queue = if matches!(msg, Message::InstallSnapshot { .. }) {
                &queues.snapshot
            } else {
                &queues.regular
            };
            if queue.try_send(message).is_err() {
                tracing::debug!(
                    node = self.self_id,
                    to = to.0,
                    "dropped Paxos message because peer gRPC mailbox is unavailable"
                );
            }
        }
    }
}

/// Feed bounded unary batches over one reconnecting h2 channel per peer. While
/// a batch is in flight, new protocol messages accumulate for the next batch;
/// on failure Paxos heartbeats/resends repair anything lost with that RPC.
async fn run_peer_delivery<P: Providers>(
    client: ParosInternalClient<ReconnectingChannel<P, tonic::body::Body>>,
    time: P::Time,
    shutdown: CancellationToken,
    mut messages: tokio::sync::mpsc::Receiver<internal::ConsensusMessage>,
) {
    let mut carried = None;
    loop {
        let first = if let Some(message) = carried.take() {
            message
        } else {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                message = messages.recv() => {
                    let Some(message) = message else {
                        return;
                    };
                    message
                }
            }
        };
        let mut attempt_client = client.clone();
        let (batch, next) = delivery_batch(first, &mut messages);
        carried = next;
        let outcome = moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = time.timeout(GRPC_DELIVERY_TIMEOUT, attempt_client.deliver(batch)) => result,
        };
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(status)) => tracing::debug!(%status, "peer gRPC delivery failed"),
            Err(_) => tracing::debug!("peer gRPC delivery timed out"),
        }
    }
}

fn delivery_batch(
    mut first: internal::ConsensusMessage,
    messages: &mut tokio::sync::mpsc::Receiver<internal::ConsensusMessage>,
) -> (internal::Deliver, Option<internal::ConsensusMessage>) {
    // Do not spend the eventual-synchrony tail replaying a bounded but stale
    // stale chaos-era traffic. Peer delivery is allowed to lose messages; the
    // protocol's current heartbeat, Accept resend, and catch-up paths repair
    // them. Keep the newest batch so recovery signals can overtake old ballots.
    while messages.len() >= GRPC_DELIVERY_BATCH {
        let Ok(newer) = messages.try_recv() else {
            break;
        };
        first = newer;
    }
    let mut batch = Vec::with_capacity(GRPC_DELIVERY_BATCH);
    let mut batch_bytes = first.encoded_len();
    batch.push(wire_message(first));
    let mut carried = None;
    while batch.len() < GRPC_DELIVERY_BATCH {
        let Ok(message) = messages.try_recv() else {
            break;
        };
        if batch_bytes.saturating_add(message.encoded_len()) > GRPC_DELIVERY_BATCH_BYTES {
            carried = Some(message);
            break;
        }
        batch_bytes += message.encoded_len();
        batch.push(wire_message(message));
    }
    (internal::Deliver { messages: batch }, carried)
}

fn wire_message(message: internal::ConsensusMessage) -> internal::WireMessage {
    let checksum = wire_checksum(&message.encode_to_vec());
    internal::WireMessage {
        message: Some(message),
        checksum,
    }
}

/// Materialize and send this batch's snapshot offers. An offered snapshot must
/// describe exactly the application prefix named by the protocol message, so
/// this runs only after the batch's committed entries are durably applied.
fn send_snapshot_offers<S, H, A>(
    storage: &mut S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    snapshot_offers: &[(NodeId, Slot, Ballot)],
    sessions: &[SessionEntry],
) -> SimulationResult<()>
where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    for &(to, offered_index, ballot) in snapshot_offers {
        if storage.applied_slot() != Some(offered_index) {
            return Err(SimulationError::InvalidState(
                "application snapshot does not match offered chosen index".into(),
            ));
        }
        let message = Message::InstallSnapshot {
            from: NodeId(out.self_id),
            ballot,
            chosen_index: offered_index,
            snapshot: Value(storage.snapshot()),
            // The at-most-once ledger travels beside the opaque bytes (#94):
            // the receiver seals it so its duplicate-suppression decisions for
            // the folded prefix match every peer's.
            sessions: sessions.to_vec(),
        };
        if hooks.drop_outgoing(to, &message) {
            trace_send_drop(audit, out.self_id, to, &message);
            continue;
        }
        out.transmit(audit, to, &message);
        if hooks.duplicate_outgoing(to, &message) {
            audit.duplicated_at_send(NodeId(out.self_id), to, &message);
            tracing::info!(
                node = out.self_id,
                to = to.0,
                kind = message_kind(&message),
                "msg_duplicated_at_send"
            );
            out.transmit(audit, to, &message);
        }
    }
    Ok(())
}

/// Surface a hook-decided send drop ([`EV_SEND_DROPPED`]). An `Accept` names
/// its slot so a trace shows exactly which round the loss isolated.
fn trace_send_drop<A: Audit>(audit: &A, self_id: u64, to: NodeId, msg: &Message) {
    audit.dropped_at_send(NodeId(self_id), to, msg);
    let kind = message_kind(msg);
    if let Message::Accept { slot, .. } = msg {
        tracing::info!(
            node = self_id,
            to = to.0,
            kind,
            slot = slot.0,
            "msg_dropped_at_send"
        );
    } else {
        tracing::info!(node = self_id, to = to.0, kind, "msg_dropped_at_send");
    }
}

/// Surface the #88 window: a snapshot install persisted while this node's own
/// campaign is open (`on_install_snapshot` deliberately does not touch the
/// election), so the sweep can prove the interleaving is visited.
fn note_mid_election_snapshot<A: Audit>(
    node: &RawNode,
    writes: &[WriteOp],
    self_id: u64,
    audit: &A,
) {
    if node.role() == NodeRole::Candidate
        && writes
            .iter()
            .any(|w| matches!(w, WriteOp::InstallSnapshot { .. }))
    {
        audit.snapshot_mid_election(NodeId(self_id));
        tracing::info!(node = self_id, "snapshot_mid_election");
    }
}

/// Ack-on-commit: only now can a client learn success — both the chosen index
/// and the application transition are durable. Controls have no proposal
/// waiter. The reply may be deliberately dropped at the reply seam
/// ([`DriverHooks::drop_client_reply`]): the server state has advanced either
/// way, and the client's retry takes the `(client, seq)` dedup path.
fn ack_committed_waiters<S, H, A>(
    storage: &S,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
    self_id: u64,
    committed: &[(Slot, Command)],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    for (slot, command) in committed {
        if matches!(command, Command::Control(_)) {
            continue;
        }
        if let Some(replies) = waiters.pending.remove(slot) {
            for (client, seq, waiter) in replies {
                audit.client_acked(
                    NodeId(self_id),
                    client,
                    seq,
                    *slot,
                    storage.applied_slot(),
                    false,
                );
                if hooks.drop_client_reply(Reply::Propose) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Propose);
                    tracing::info!(node = self_id, reply = "propose", "client_reply_dropped");
                    continue;
                }
                let _ = waiter.send(ProposeAck {
                    seq,
                    leader: Some(self_id),
                    committed: true,
                    slot: Some(slot.0),
                });
            }
        }
    }
}

/// Send one batch's addressed messages (fire-and-forget). The core addresses
/// each one; the driver maps `NodeId` → address. Each message may be dropped
/// at this seam — per-message loss the network layer cannot produce on its own
/// (a TCP stream loses intervals, never one isolated message), with
/// `resend_pending` re-deriving what matters — or sent twice (retransmission
/// is legal transport behavior; set-based quorum counting must tolerate it).
fn send_messages<H, A>(out: &Outbound, hooks: &H, audit: &A, messages: Vec<(NodeId, Message)>)
where
    H: DriverHooks,
    A: Audit,
{
    let self_id = out.self_id;
    for (to, msg) in messages {
        if hooks.drop_outgoing(to, &msg) {
            trace_send_drop(audit, self_id, to, &msg);
            continue;
        }
        out.transmit(audit, to, &msg);
        if hooks.duplicate_outgoing(to, &msg) {
            audit.duplicated_at_send(NodeId(self_id), to, &msg);
            tracing::info!(
                node = self_id,
                to = to.0,
                kind = message_kind(&msg),
                "msg_duplicated_at_send"
            );
            out.transmit(audit, to, &msg);
        }
    }
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
fn drain_ready<S, H, A>(
    node: &mut RawNode,
    storage: &mut S,
    out: &Outbound,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
) -> SimulationResult<()>
where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let self_id = out.self_id;
    // Copy the batch out of the borrow guard, advance to release the gate, then
    // perform I/O — persist → send → apply. Advancing before the I/O is the
    // documented async pattern; persist-before-send still holds because the
    // persist loop below precedes the send loop.
    let ready = node.ready();
    // A durable compaction floor must never outrun the durable *application*
    // state covering the slots it drops: flushing a `Truncate` in step 1
    // discards the accepted records, and a crash at the `AfterApplyBeforeSync`
    // seam then lands a node whose application prefix is behind a floor nothing
    // can replay — its apply stream stays shifted forever (network-axis seed
    // 8398193358524544360). Split the truncates out of the batch and flush them
    // only after the application fsync below. A truncate lost to a crash in
    // that window is safe: the floor is pure space reclamation, re-raised by
    // the next decided `Truncate`.
    let (truncates, writes): (Vec<WriteOp>, Vec<WriteOp>) = ready
        .writes()
        .to_vec()
        .into_iter()
        .partition(|w| matches!(w, WriteOp::Truncate { .. }));
    let must_sync = if writes.iter().any(WriteOp::needs_sync) {
        paros_core::MustSync::Sync
    } else {
        paros_core::MustSync::Relaxed
    };
    let messages: Vec<(NodeId, Message)> = ready.messages().to_vec();
    let committed: Vec<(Slot, Command)> = ready.committed().to_vec();
    let snapshot_offers: Vec<(NodeId, Slot, Ballot)> = ready.snapshot_offers().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    ready.advance();

    // 1. Persist durable writes FIRST, each op in order, flush per MustSync, and
    //    surface the persisted state for the safety + recovery oracles. The
    //    `BeforeSync` crash seam lives inside `persist_writes`.
    let promised = node.hard_state().max_promised_ballot;
    persist_writes(storage, &writes, must_sync, promised, self_id, hooks, audit)?;

    note_mid_election_snapshot(node, &writes, self_id, audit);

    // Snapshot offers are outbound protocol messages too. Count them before the
    // after-sync seam so a crash can drop an offer-only batch just as it can any
    // other outbound batch. Their bytes are materialized after application below:
    // an application snapshot must cover exactly the boundary it advertises.
    let snapshot_offer_count = snapshot_offers.len();
    if snapshot_offer_count > 0 {
        audit.snapshot_offered(
            NodeId(self_id),
            u64::try_from(snapshot_offer_count).unwrap_or(u64::MAX),
        );
        tracing::info!(
            node = self_id,
            snapshot_offers = snapshot_offer_count as u64,
            "snapshot_offered"
        );
    }

    // Crash seam: after the batch is durable but before its messages leave. The
    // durable writes survive; the batch's messages are dropped (never sent), so a
    // recovered node must re-derive them. Only meaningful when there is durable
    // work or a message to lose.
    if (!writes.is_empty() || !messages.is_empty() || snapshot_offer_count > 0)
        && hooks.crash_at(Seam::AfterSyncBeforeSend)
    {
        audit.crashed(NodeId(self_id), Seam::AfterSyncBeforeSend);
        tracing::info!(
            node = self_id,
            seam = "after_sync_before_send",
            snapshot_offers = snapshot_offer_count as u64,
            "crashed"
        );
        return Err(seam_crash());
    }

    // 2. Send messages — only after (1) is durable.
    send_messages(out, hooks, audit, messages);

    // 3. Apply newly chosen entries (already durable, in contiguous order) —
    //    surface them to the oracles and ack any clients waiting on each slot
    //    (ack-on-commit: a held reply fires only now that its slot is chosen).
    let chosen_index = node.hard_state().chosen_index;
    for (slot, command) in &committed {
        let chosen_index = chosen_index.ok_or_else(|| {
            SimulationError::InvalidState("committed command without chosen prefix".into())
        })?;
        storage
            .apply(chosen_index, *slot, command)
            .map_err(|e| storage_err(&e))?;
        let vhash = command_hash(command);
        audit.applied(
            NodeId(self_id),
            *slot,
            vhash,
            match command {
                Command::User(e) => Some((e.client.0, e.seq.0)),
                Command::Control(_) => None,
            },
        );
        tracing::info!(node = self_id, slot = slot.0, vhash, "value_chosen");
        tracing::info!(
            node = self_id,
            slot = slot.0,
            applied_index = slot.0,
            "log_applied"
        );
    }

    // Application state is part of the durable replica contract. Flush all
    // staged transitions before an acknowledgement can escape this batch. This
    // also makes a chosen-index-only Ready durable before its application effect,
    // so reboot replay can never observe an application prefix ahead of consensus.
    if !committed.is_empty() {
        // Crash seam: the consensus prefix is durable and the application
        // transitions are staged, but their fsync has not happened. A crash
        // here is the only way to land "consensus ahead of application" on
        // disk — the state the boot replay's idempotent re-apply heals.
        if hooks.crash_at(Seam::AfterApplyBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterApplyBeforeSync);
            tracing::info!(node = self_id, seam = "after_apply_before_sync", "crashed");
            return Err(seam_crash());
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_err(&e))?;
    }

    // Only now that the application state covering the dropped slots is
    // fsync-durable may the compaction floor become durable (see the batch
    // split above). Runs through the same persist path, so the truncate keeps
    // its `BeforeSync` crash location and its after-fsync audit report.
    if !truncates.is_empty() {
        persist_writes(
            storage,
            &truncates,
            paros_core::MustSync::Sync,
            promised,
            self_id,
            hooks,
            audit,
        )?;
    }

    if !snapshot_offers.is_empty() {
        let sessions = node.session_ledger();
        send_snapshot_offers(storage, out, hooks, audit, &snapshot_offers, &sessions)?;
    }

    ack_committed_waiters(storage, waiters, hooks, audit, self_id, &committed);

    // 3b. Answer confirmed reads — after the apply loop, so the applied prefix
    //     this same batch carried is covered by what the read observes. The ack
    //     reports the *serve-time* chosen index (at or past the confirmed read
    //     index): that is the local state actually served.
    for state in &read_states {
        if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&state.ctx) {
            let read_index = node.hard_state().chosen_index;
            audit.read_confirmed(NodeId(self_id), read_index);
            if hooks.drop_client_reply(Reply::Read) {
                audit.client_reply_dropped(NodeId(self_id), Reply::Read);
                tracing::info!(node = self_id, reply = "read", "client_reply_dropped");
                continue;
            }
            let _ = waiter.send(ReadAck {
                seq,
                leader: Some(self_id),
                committed: true,
                read_index: read_index.map(|s| s.0),
            });
        }
    }

    Ok(())
}

/// Persist a batch's [`WriteOp`]s in order (persist-before-send step 1), flush per
/// [`MustSync`], and surface the persisted state for the safety + recovery
/// oracles: a `node_state` event when the promised ballot rose, and a per-slot
/// `persist` event for each accepted append. `promised` is the node's post-batch
/// promise (`>=` any accept ballot in the batch).
///
/// The observability events are emitted only **after** the fsync, so they never
/// claim a write the `BeforeSync` crash seam then discards: a crash before the
/// fsync loses the whole un-synced batch and emits nothing, exactly as a real
/// crash-before-flush would.
fn persist_writes<S: NodeStorage, H: DriverHooks, A: Audit>(
    storage: &mut S,
    writes: &[WriteOp],
    must_sync: paros_core::MustSync,
    promised: Ballot,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> SimulationResult<()> {
    let mut promise_changed = false;
    for op in writes {
        match op {
            WriteOp::SetPromise(ballot) => {
                storage
                    .persist_ballot(*ballot)
                    .map_err(|e| storage_err(&e))?;
                promise_changed = true;
            }
            WriteOp::AppendAccepted {
                slot,
                ballot,
                command,
            } => {
                storage
                    .append_accepted(*slot, *ballot, command.clone())
                    .map_err(|e| storage_err(&e))?;
            }
            WriteOp::SetChosenIndex(slot) => {
                storage
                    .set_chosen_index(*slot)
                    .map_err(|e| storage_err(&e))?;
            }
            WriteOp::Truncate { first, sealed } => {
                storage
                    .truncate(*first, sealed)
                    .map_err(|e| storage_err(&e))?;
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                snapshot,
                sessions,
            } => {
                storage
                    .install_snapshot(*chosen_index, *ballot, snapshot.0.clone(), sessions)
                    .map_err(|e| storage_err(&e))?;
            }
        }
    }

    // Crash seam: the batch is staged but not yet flushed. A crash here loses the
    // whole un-synced batch (and no message has been sent), so surface nothing but
    // the crash marker itself. Only meaningful when the batch actually staged
    // something.
    if !writes.is_empty() && hooks.crash_at(Seam::BeforeSync) {
        audit.crashed(NodeId(self_id), Seam::BeforeSync);
        tracing::info!(node = self_id, seam = "before_sync", "crashed");
        return Err(seam_crash());
    }

    if !writes.is_empty() {
        storage.sync(must_sync).map_err(|e| storage_err(&e))?;
        // Durability marker: whether this batch was fsync'd (a promise-raise or
        // accept — `MustSync::Sync`) or a relaxed write (a chosen-index-only
        // advance). The persist/send-seam animation renders it as a filled vs
        // hollow tick.
        tracing::info!(
            node = self_id,
            sync = (must_sync == paros_core::MustSync::Sync),
            writes = u64::try_from(writes.len()).unwrap_or(u64::MAX),
            "synced"
        );
    }

    surface_persisted(writes, promised, promise_changed, self_id, audit);
    Ok(())
}

/// Report a flushed batch's durable state — one audit callback and one tracing
/// event per op. Split out of [`persist_writes`] so the staging half and the
/// reporting half each stay readable; both loops walk `writes` in order.
fn surface_persisted<A: Audit>(
    writes: &[WriteOp],
    promised: Ballot,
    promise_changed: bool,
    self_id: u64,
    audit: &A,
) {
    // Durable now — emit the truthful persisted state for the oracles.
    if promise_changed {
        audit.promised(NodeId(self_id), promised);
        tracing::info!(
            node = self_id,
            pround = promised.round,
            pbnode = promised.node.0,
            "node_state"
        );
    }
    for op in writes {
        match op {
            WriteOp::AppendAccepted {
                slot,
                ballot,
                command,
            } => {
                let vhash = command_hash(command);
                audit.accepted(NodeId(self_id), *slot, *ballot, promised, vhash);
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    pround = promised.round,
                    pbnode = promised.node.0,
                    around = ballot.round,
                    abnode = ballot.node.0,
                    vhash,
                    "persist"
                );
            }
            WriteOp::SetChosenIndex(slot) => {
                audit.chosen_index(NodeId(self_id), *slot);
            }
            WriteOp::Truncate { first, .. } => {
                audit.truncated(NodeId(self_id), *first);
                tracing::info!(node = self_id, first = first.0, "compacted");
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                ..
            } => {
                let first = chosen_index.0 + 1;
                // The install jumps the applied prefix to `chosen_index` without
                // replaying entries (snapshot-xor-entries); the audit callback
                // reports both the install and that jump.
                audit.snapshot_installed(NodeId(self_id), *chosen_index, *ballot);
                tracing::info!(
                    node = self_id,
                    chosen_index = chosen_index.0,
                    first,
                    "snapshot_installed"
                );
                // Surface the jump so the no-gaps oracle (which admits it as a
                // snapshot jump) and the convergence oracle see the node reach the
                // cluster prefix.
                tracing::info!(
                    node = self_id,
                    slot = chosen_index.0,
                    applied_index = chosen_index.0,
                    "log_applied"
                );
            }
            WriteOp::SetPromise(_) => {}
        }
    }
}

/// Map a [`StorageError`] into a driver [`SimulationError`] so a durable-write
/// fault propagates out of the node loop.
fn storage_err(e: &StorageError) -> SimulationError {
    SimulationError::InvalidState(format!("storage: {e}"))
}

/// Marker payload of the error [`run_node`] returns when a [`Seam`] crash fires.
/// Distinguishes a *simulated crash* (the caller should recover and re-run) from
/// a genuine failure (which should propagate).
const SEAM_CRASH_MARKER: &str = "paros:seam-crash";

/// The error a crash seam raises to unwind the current node incarnation.
fn seam_crash() -> SimulationError {
    SimulationError::InvalidState(SEAM_CRASH_MARKER.to_string())
}

/// Whether `e` is the marker [`run_node`] returns on a simulated seam crash (as
/// opposed to a real failure). The node loop's owner re-runs `run_node` — which
/// rebuilds volatile state from durable storage — to recover.
#[must_use]
pub fn is_seam_crash(e: &SimulationError) -> bool {
    matches!(e, SimulationError::InvalidState(s) if s == SEAM_CRASH_MARKER)
}

/// Draw a randomized election timeout in `[T, 2T)` ticks from the provider's
/// seeded RNG. Drawn here, never in the zero-dep core, so the core stays
/// deterministic and dependency-free while a seed still replays bit-identically.
fn draw_election_timeout<P: Providers, H: DriverHooks, A: Audit>(
    providers: &P,
    hooks: &H,
    audit: &A,
    self_id: u64,
) -> u64 {
    if hooks.shortest_election_timeout() {
        audit.election_timeout_extreme(NodeId(self_id), ELECTION_TIMEOUT_BASE);
        tracing::info!(
            node = self_id,
            ticks = ELECTION_TIMEOUT_BASE,
            "election_timeout_extreme"
        );
        ELECTION_TIMEOUT_BASE
    } else {
        providers
            .random()
            .random_range(ELECTION_TIMEOUT_BASE..ELECTION_TIMEOUT_BASE * 2)
    }
}

/// Post-batch upkeep: feed the core a fresh randomized election timeout whenever
/// its election clock reset, emit `leader_elected` on the transition to Leader,
/// and drop held client replies on step-down (so clients time out and retry the
/// new leader).
fn maintain<P: Providers, H: DriverHooks, A: Audit>(
    node: &mut RawNode,
    providers: &P,
    last_role: &mut NodeRole,
    last_duplicates: &mut u64,
    last_quorum_lost: &mut u64,
    waiters: &mut ClientWaiters,
    self_id: u64,
    hooks: &H,
    audit: &A,
) {
    if node.needs_election_timeout() {
        node.set_election_timeout(draw_election_timeout(providers, hooks, audit, self_id));
    }
    // Surface any #94 duplicate suppressions the batch's contiguous walk
    // performed (the counter is monotone per incarnation).
    let duplicates = node.duplicates_suppressed();
    if duplicates > *last_duplicates {
        let count = duplicates - *last_duplicates;
        *last_duplicates = duplicates;
        audit.duplicate_suppressed(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "duplicate_suppressed");
    }
    // Surface any CheckQuorum step-down the batch's tick performed (#95).
    let quorum_lost = node.quorum_lost_step_downs();
    if quorum_lost > *last_quorum_lost {
        let count = quorum_lost - *last_quorum_lost;
        *last_quorum_lost = quorum_lost;
        audit.quorum_lost(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "leader_quorum_lost");
    }
    let role = node.role();
    if role == NodeRole::Leader && *last_role != NodeRole::Leader {
        // The won ballot *and* the promise held at the instant of victory. They
        // are normally the same ballot — winning means having promised your own
        // campaign ballot and heard nothing higher — and the oracle asserts
        // exactly that: a leader never holds a promise above the ballot it just
        // won (#67). Emitting both here, on the transition, is what makes the
        // stale win visible; a tick later the leader may legitimately learn a
        // higher-ballot commit and the state is no longer distinguishable.
        let ballot = node.ballot();
        let promised = node.hard_state().max_promised_ballot;
        let gaps = node.election_gap_fills();
        audit.elected(NodeId(self_id), ballot, promised, gaps);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            bnode = ballot.node.0,
            pround = promised.round,
            pbnode = promised.node.0,
            "leader_elected"
        );
        // This election found holes the promise quorum reported nothing for and
        // filled them with no-ops. Rare and mechanism-specific, so surface it: it is
        // the only outside evidence the fill path ran.
        if gaps > 0 {
            tracing::info!(
                node = self_id,
                round = node.ballot().round,
                gaps,
                "election_gap_filled"
            );
        }
    } else if *last_role == NodeRole::Leader && role != NodeRole::Leader {
        waiters.pending.clear();
        // Parked reads have no slot whose commit could ever answer them:
        // redirect explicitly so the client retries the new leader now rather
        // than burning its deadline (writes time out instead, on purpose —
        // their slot may still commit under the new leader).
        for (_, (seq, _, waiter)) in std::mem::take(&mut waiters.pending_reads) {
            let _ = waiter.send(ReadAck {
                seq,
                leader: node.leader().map(|n| n.0),
                committed: false,
                read_index: None,
            });
        }
    }
    *last_role = role;
}

/// On (re)boot the core rebuilt its volatile state from durable storage. Re-emit
/// that recovered state so the oracles see this node's post-restart belief: the
/// recovered promised ballot (`node_state`, feeding the monotonic-promise check
/// across the restart seam), each recovered accepted record (`recovered`, feeding
/// the recovery oracle's "a restart never changes a pre-crash accepted value"
/// check), and each rebuilt chosen entry (`value_chosen`, feeding
/// at-most-one-value-chosen). The apply replay (`log_applied`) covers a crash
/// between "`chosen_index` durable" and "apply side-effects done"; it is
/// idempotent (the chosen index is the applied index). A compacted node's
/// accepted log starts at its floor, so the replay naturally covers only the
/// retained prefix. A clean first boot has empty scalars/log, so this is a near
/// no-op.
fn replay_boot_state<S: NodeStorage, A: Audit>(
    node: &RawNode,
    storage: &mut S,
    self_id: u64,
    audit: &A,
) -> SimulationResult<()> {
    // Mark this incarnation coming up. The recovery recorder turns every `booted`
    // after a node's first into a *restart* event for the animation.
    tracing::info!(node = self_id, "booted");

    let promised = node.hard_state().max_promised_ballot;
    tracing::info!(
        node = self_id,
        pround = promised.round,
        pbnode = promised.node.0,
        "node_state"
    );
    // One typed report of the whole recovered belief: the promise plus every
    // durable accepted record read back. Built once so the audit sees the boot
    // as a single transition, matching the `recovered` trace stream.
    let mut records: Vec<(Slot, Ballot, u64)> = Vec::with_capacity(node.accepted().len());
    for (slot, (ballot, command)) in node.accepted() {
        let vhash = command_hash(command);
        records.push((*slot, *ballot, vhash));
        tracing::info!(
            node = self_id,
            slot = slot.0,
            around = ballot.round,
            abnode = ballot.node.0,
            vhash,
            "recovered"
        );
    }
    audit.recovered(NodeId(self_id), promised, &records);
    let mut replayed_application = false;
    if let Some(ci) = node.hard_state().chosen_index {
        let applied_slot = storage.applied_slot();
        for (slot, (_b, stored)) in node.accepted().range(..=ci) {
            // A #94 duplicate slot replays exactly as the live walk applied it:
            // a no-op. The core re-derived `duplicate_slots` from the sealed
            // sessions + the retained log in `RawNode::new`, so the substitution
            // is deterministic across the restart.
            let noop = Command::Control(Control::Noop);
            let command = if node.duplicate_slots().contains(slot) {
                &noop
            } else {
                stored
            };
            if applied_slot.is_none_or(|applied| *slot > applied) {
                storage
                    .apply(ci, *slot, command)
                    .map_err(|e| storage_err(&e))?;
                replayed_application = true;
            }
            let vhash = command_hash(command);
            audit.applied(
                NodeId(self_id),
                *slot,
                vhash,
                match command {
                    Command::User(e) => Some((e.client.0, e.seq.0)),
                    Command::Control(_) => None,
                },
            );
            tracing::info!(node = self_id, slot = slot.0, vhash, "value_chosen");
            tracing::info!(
                node = self_id,
                slot = slot.0,
                applied_index = slot.0,
                "log_applied"
            );
        }
    }
    if replayed_application {
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_err(&e))?;
    }
    Ok(())
}

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation — only the providers
/// differ) and `S: NodeStorage` (the injected durable storage). The loop owns a
/// [`RawNode`], serves the Paros gRPC interface, feeds client proposals and
/// peer messages into the core, sends the core's outbound messages to the peers
/// named in `members`, and ticks until `shutdown` fires.
///
/// `members` is the full cluster membership (`NodeId` → address, *including*
/// this node): the core addresses each outbound message by `NodeId`, and the
/// driver resolves it here. It must be consistent across the cluster and agree
/// with the `Config` the node read from `storage`.
///
/// `hooks` controls the driver-level crash seams and rare-but-valid policy
/// alternatives. Production passes [`NoHooks`](crate::NoHooks), whose default
/// methods are inert.
///
/// `audit` is the pure-observation mirror of `hooks`: the driver reports every
/// externally meaningful transition to it, and nothing it does can change the
/// run. Production passes [`NoAudit`](crate::NoAudit).
///
/// # Errors
///
/// Returns an error if the gRPC server fails to bind or listen on `local_addr`. May
/// also return a simulated crash marker ([`is_seam_crash`]) if `hooks` fires at a
/// durability seam — the caller recovers by re-running `run_node` with fresh
/// storage.
#[tracing::instrument(skip_all)]
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state.
#[allow(clippy::too_many_lines)]
pub async fn run_node<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> SimulationResult<()>
where
    P: Providers,
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    // Every task spawned by this incarnation must stop when `run_node` exits,
    // including a durability-seam error that immediately starts a replacement
    // incarnation. This drop guard covers every `?` and return path.
    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let listener = providers
        .network()
        .bind(&local_addr)
        .await
        .map_err(|e| SimulationError::InvalidState(format!("node gRPC listener: {e}")))?;

    // Tonic handlers run as h2 request tasks and forward into these typed
    // queues. The loop remains the sole owner of RawNode.
    let (rpc_service, mut rpc): (_, RpcInbox) = rpc_channel();
    let grpc_service = tonic::service::Routes::new(ParosServer::new(rpc_service.clone()))
        .add_service(ParosInternalServer::new(rpc_service))
        .prepare();
    let grpc_server = H2Server::new(&providers).with_config(H2ServerConfig {
        keep_alive: Some(grpc_keep_alive()),
        vectored_writes: true,
    });

    // The sans-IO core, bootstrapped from durable storage.
    let mut node = RawNode::new(&storage);
    let self_id = node.config().id.0;

    replay_boot_state(&node, &mut storage, self_id, audit)?;

    // Validate every origin before starting reconnecting-channel tasks. Once
    // channels exist, there are no fallible setup steps before their drop guard
    // is installed.
    let peers = members
        .into_iter()
        .map(|(id, addr)| {
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            Ok((id, addr, origin))
        })
        .collect::<SimulationResult<Vec<_>>>()?;

    let mut peer_channels = Vec::with_capacity(peers.len());
    let peer_queues = peers
        .into_iter()
        .map(|(id, addr, origin)| {
            let channel = ReconnectingChannel::new(&providers, addr, grpc_channel_config());
            peer_channels.push(channel.clone());
            let client = ParosInternalClient::with_origin(channel, origin);
            let (regular_tx, regular_rx) = tokio::sync::mpsc::channel(GRPC_PEER_QUEUE_CAPACITY);
            let (snapshot_tx, snapshot_rx) =
                tokio::sync::mpsc::channel(GRPC_SNAPSHOT_QUEUE_CAPACITY);
            providers
                .task()
                .spawn_task(
                    "paros-grpc-peer-delivery",
                    run_peer_delivery(
                        client.clone(),
                        providers.time().clone(),
                        incarnation_shutdown.clone(),
                        regular_rx,
                    ),
                )
                .detach();
            providers
                .task()
                .spawn_task(
                    "paros-grpc-snapshot-delivery",
                    run_peer_delivery(
                        client,
                        providers.time().clone(),
                        incarnation_shutdown.clone(),
                        snapshot_rx,
                    ),
                )
                .detach();
            (
                id,
                PeerQueues {
                    regular: regular_tx,
                    snapshot: snapshot_tx,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    // `close` is terminal and shared by every clone held by tonic clients. It
    // cancels connect/backoff/keepalive work immediately when this incarnation
    // exits, including simulated durability crashes that return via `?`.
    let _peer_channel_guard = OnDrop::new(move || {
        for channel in peer_channels {
            channel.close();
        }
    });

    // One reconnecting h2 channel per peer; cloned generated clients multiplex
    // concurrent RPCs over that shared connection.
    let out = Outbound {
        peer_queues,
        self_id,
    };

    // The held client replies: proposals keyed by slot (ack-on-commit), reads
    // keyed by their read-index ctx.
    let mut waiters = ClientWaiters::default();
    let mut next_read_ctx: u64 = 0;
    // Seed the first randomized election timeout (jitter from the driver's RNG).
    node.set_election_timeout(draw_election_timeout(&providers, hooks, audit, self_id));
    let mut last_role = node.role();
    let mut last_duplicates = node.duplicates_suppressed();
    let mut last_quorum_lost = node.quorum_lost_step_downs();

    let time = providers.time().clone();
    let mut ticks: u64 = 0;
    // The tick deadline is ABSOLUTE, not a fresh relative sleep per loop
    // iteration: `select!` drops and re-creates its futures every pass, so a
    // relative `sleep(TICK_INTERVAL)` resets whenever any other branch is
    // ready. Under sustained sub-interval traffic (a singleton absorbing every
    // client retry, a reconnect storm) the protocol clock then never advances —
    // no election, no ack, clients retry harder: a self-sustaining starvation
    // loop (seed 3847608256092482294 ticked twice in 81 simulated seconds).
    // With an absolute deadline the sleep is zero-length once the deadline
    // passes and fires regardless of load.
    let mut next_tick = time.now() + TICK_INTERVAL;

    loop {
        moonpool_core::select! {
            accepted = listener.accept() => {
                let (stream, addr) = accepted
                    .map_err(|e| SimulationError::InvalidState(format!("gRPC accept: {e}")))?;
                let connection = grpc_server.serve_connection_with_shutdown(
                    stream,
                    grpc_service.clone(),
                    incarnation_shutdown.clone().cancelled_owned(),
                );
                providers.task().spawn_task("paros-grpc-server", async move {
                    if let Err(error) = connection.await {
                        tracing::warn!(%addr, %error, "gRPC connection ended");
                    }
                }).detach();
            }
            Some((req, reply)) = rpc.propose.recv() => {
                // A client value → the leader (deduplicated by (client, seq)). The
                // reply is held until the slot commits (ack-on-commit); a non-leader
                // redirects immediately.
                let seq = req.seq;
                let client = req.client;
                match node.propose(ClientId(req.client), ClientSeq(req.seq), Value(req.command)) {
                    ProposeResult::NotLeader(hint) => {
                        let _ = reply.send(ProposeAck { seq, leader: hint.map(|n| n.0), committed: false, slot: None });
                    }
                    ProposeResult::Accepted(slot) | ProposeResult::Duplicate(slot) => {
                        waiters.pending.entry(slot).or_default().push((client, seq, reply));
                    }
                    ProposeResult::Chosen(slot) => {
                        // Already inside this node's applied prefix before this
                        // call, so the ack fires immediately — and it *names* the
                        // slot, exactly like the ack-on-commit path. A committed
                        // ack that named nothing was unfalsifiable: the client was
                        // told "applied" with no way for an oracle to check the
                        // claim against the applied prefix.
                        audit.client_acked(NodeId(self_id), client, seq, slot, storage.applied_slot(), true);
                        tracing::info!(node = self_id, slot = slot.0, "propose_dedup_ack");
                        if hooks.drop_client_reply(Reply::ProposeDedup) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ProposeDedup);
                            tracing::info!(node = self_id, reply = "propose_dedup", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ProposeAck { seq, leader: Some(self_id), committed: true, slot: Some(slot.0) });
                        }
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                maintain(&mut node, &providers, &mut last_role, &mut last_duplicates, &mut last_quorum_lost, &mut waiters, self_id, hooks, audit);
            }
            Some((req, reply)) = rpc.read.recv() => {
                // A client read via read-index: the leader captures its applied
                // watermark, confirms it is still leader with a heartbeat-ack
                // quorum round (no log write), and the reply is parked until the
                // confirmed `ReadState` surfaces after apply — a deposed or
                // freshly elected leader can no longer serve a stale watermark.
                // A non-leader redirects immediately.
                let seq = req.seq;
                match node.read_index(next_read_ctx) {
                    ReadIndexResult::NotLeader(hint) => {
                        let _ = reply.send(ReadAck { seq, leader: hint.map(|n| n.0), committed: false, read_index: None });
                    }
                    ReadIndexResult::Pending => {
                        waiters.pending_reads.insert(next_read_ctx, (seq, ticks, reply));
                        next_read_ctx += 1;
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                maintain(&mut node, &providers, &mut last_role, &mut last_duplicates, &mut last_quorum_lost, &mut waiters, self_id, hooks, audit);
            }
            Some((msg, reply)) = rpc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO). Surface the
                // arrival (mirror of `msg_sent`) so the demo can pair sends with
                // receives and mark the unmatched ones as network drops.
                let kind = message_kind(&msg);
                match message_route(&msg) {
                    Some((from, ballot, Some(slot))) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        slot = slot.0,
                        "msg_received"
                    ),
                    // The empty-prefix beat: no slot field, mirroring `msg_sent`.
                    Some((from, ballot, None)) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        "msg_received"
                    ),
                    None => tracing::info!(node = self_id, kind, "msg_received"),
                }
                // Canary: a Prepare whose from_slot is below our floor is the
                // dangerous "campaign against a truncated acceptor" case. Record it
                // so the sweep can assert the interleaving stays reachable once the
                // acceptor floor guard is in place.
                if let Message::Prepare { from_slot, .. } = &msg
                    && *from_slot < node.first_slot()
                {
                    audit.prepare_below_floor(NodeId(self_id), *from_slot, node.first_slot());
                    tracing::info!(
                        node = self_id,
                        from_slot = from_slot.0,
                        floor = node.first_slot().0,
                        "prepare_below_floor"
                    );
                }
                node.step(msg);
                drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                maintain(&mut node, &providers, &mut last_role, &mut last_duplicates, &mut last_quorum_lost, &mut waiters, self_id, hooks, audit);
                let _ = reply.send(());
            }
            Some((req, reply)) = rpc.compact.recv() => {
                // The application permits dropping the log prefix up to `up_to`.
                // Only the leader admits it: it proposes a `Truncate` control
                // command into the next slot, decided by ordinary Paxos and
                // forwarded to every node, each of which truncates lazily when it
                // applies that slot. A non-leader redirects (like `propose`).
                let ack = match node.propose_control(Control::Truncate { up_to: Slot(req.up_to) }) {
                    ProposeResult::NotLeader(hint) => CompactAck {
                        leader: hint.map(|n| n.0),
                        accepted: false,
                        first_slot: node.first_slot().0,
                    },
                    _ => CompactAck {
                        leader: Some(self_id),
                        accepted: true,
                        first_slot: node.first_slot().0,
                    },
                };
                drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                maintain(&mut node, &providers, &mut last_role, &mut last_duplicates, &mut last_quorum_lost, &mut waiters, self_id, hooks, audit);
                let _ = reply.send(ack);
            }
            Some((_req, reply)) = rpc.inspect.recv() => {
                let _ = reply.send(InspectReply {
                    chosen_index: node.hard_state().chosen_index.map(|slot| slot.0),
                    first_slot: node.first_slot().0,
                    snapshot: storage.snapshot(),
                });
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                next_tick = time.now() + TICK_INTERVAL;
                node.tick();
                // Consult each hook only when its decision can have an effect.
                // Production's hooks are false; simulation gives each decision
                // an independent BUGGIFY location.
                if node.has_pending_accepts() {
                    if hooks.skip_accept_resend() {
                        audit.resend_skipped(NodeId(self_id));
                        tracing::info!(node = self_id, "accept_resend_skipped");
                    } else {
                        node.resend_pending();
                    }
                }
                if node.role() == NodeRole::Leader && hooks.resign_leadership() {
                    audit.stepped_down(NodeId(self_id));
                    tracing::info!(node = self_id, "leadership_resigned");
                    node.step_down();
                }
                ticks += 1;
                // Expire parked reads whose confirmation is overdue (lost acks, a
                // minority-partitioned leader that never steps down): answer a
                // retry redirect while the client still has deadline left. A late
                // core confirmation finds the ctx gone and is ignored.
                let overdue: Vec<u64> = waiters.pending_reads
                    .iter()
                    .filter(|(_, (_, parked_at, _))| ticks.saturating_sub(*parked_at) > READ_RETRY_TICKS)
                    .map(|(ctx, _)| *ctx)
                    .collect();
                for ctx in overdue {
                    if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&ctx) {
                        let _ = waiter.send(ReadAck { seq, leader: Some(self_id), committed: false, read_index: None });
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                maintain(&mut node, &providers, &mut last_role, &mut last_duplicates, &mut last_quorum_lost, &mut waiters, self_id, hooks, audit);
                // Surface a chosen slot stranded above the applied prefix. The
                // `Ready` handshake only ever hands out the *contiguous* prefix, so
                // a hole below a chosen slot is otherwise invisible from outside the
                // core. Re-emitted every tick while it lasts: the oracle reads its
                // persistence past quiescence, not a single instant.
                if let Some((hole, above)) = node.chosen_gap() {
                    audit.chosen_gap(NodeId(self_id), hole, above);
                    tracing::info!(node = self_id, hole = hole.0, above = above.0, "chosen_gap");
                }
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
