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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_core::{
    Detach, NetworkProvider, Providers, RandomProvider, SimulationError, SimulationResult,
    TaskProvider, TcpListenerTrait, TimeProvider,
};
use moonpool_hyper::{ChannelConfig, H2Server, H2ServerConfig, KeepAlive, ReconnectingChannel};
use paros_core::{
    AcceptorConfig, Ballot, ClientId, ClientSeq, Command, ConfigId, Control, HandoffCounters,
    LeadershipOrigin, MatchReply, MatchRequest, MatchStep, MatchmakerId, Message, NodeId, NodeRole,
    ProposeResult, QuorumSystem, RawNode, ReadIndexResult, ReadState, ReconfigureRefusal,
    ReconfigureResult, SessionEntry, Slot, Value, WriteOp,
};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::{Audit, Deployment, StorageFaultDecision};
use crate::grpc::{
    CompactAck, InspectReply, ParosInternalClient, ParosInternalServer, ParosMatchmakerClient,
    ParosServer, ProposeAck, ReadAck, ReconfigureAck, ReplySender, RpcInbox, internal,
    match_reply_from_wire, message_to_proto, rpc_channel, wire_checksum, wire_match_request,
};
use crate::hooks::{DriverHooks, HandoffContext, Reply, Seam};
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
/// Overflow evicts the *oldest* undelivered message (see [`PeerMailbox`]).
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
/// Bounded inboxes between the tonic handlers and the node loop: overload is
/// visible as backpressure, with ample room for one tick's peer fanout.
const GRPC_CLIENT_INBOX_CAPACITY: usize = 256;
const GRPC_PEER_INBOX_CAPACITY: usize = 1024;

/// Per-node driver tunables — **born workload-buggified config** (AGENTS.md
/// prong 2): plain data the harness layer randomizes per seed, FDB knob style,
/// while production takes [`DriverTunables::default()`] and is bit-identical
/// to the constants above. Every field documents its floor: a capacity must be
/// at least 1 (a zero-capacity mpsc channel panics at construction), a
/// duration at least non-zero, and the election base at least
/// `2 * HEARTBEAT_TICKS` so a live leader always beats before a follower's
/// election clock fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DriverTunables {
    /// How often the node advances its logical clock. Pacing, not a protocol
    /// bound: every timeout the core owns is counted in ticks, so a slower
    /// tick is a slower node, which the cluster already tolerates. Floor: any
    /// non-zero duration.
    pub tick_interval: Duration,
    /// Base election timeout `T`, in ticks; the actual timeout is drawn from
    /// `[T, 2T)` to break dueling proposers. Two floors: `2 * HEARTBEAT_TICKS`
    /// (see `paros_core`), below which a live leader's beat can lose the race
    /// against its followers' election clocks every round; and, in wall-clock
    /// terms, `T × tick_interval` must exceed a Phase-1 round trip, or a
    /// candidate abandons its own round before its promises return and no
    /// leader is ever elected.
    pub election_timeout_base: u64,
    /// h2 PING interval on peer streams (provider time, so a half-open
    /// connection is replaced deterministically). Floor: non-zero.
    pub keep_alive_interval: Duration,
    /// How long a PING may go unanswered before the stream is replaced.
    /// Floor: non-zero.
    pub keep_alive_timeout: Duration,
    /// How long a peer connect attempt may take before it is retried. Floor:
    /// non-zero (a reconnecting channel retries forever).
    pub connection_timeout: Duration,
    /// How long one peer-delivery RPC may take before its batch is written
    /// off as lost (the mailbox is lossy by contract; resends repair it).
    /// Floor: non-zero.
    pub delivery_timeout: Duration,
    /// Ticks a parked read may wait for its read-index confirmation before
    /// the driver answers a retry redirect. Floor: the confirmation is one
    /// heartbeat-ack round trip, so `read_retry_ticks × tick_interval` must
    /// exceed it or no read ever confirms. A client whose deadline is shorter
    /// than the wait simply times out (ambiguous, never wrong).
    pub read_retry_ticks: u64,
    /// Capacity of the snapshot offers' independent h2 request lane. Floor 1.
    pub snapshot_queue_capacity: usize,
    /// Capacity of each client-facing inbox (propose, read, compact, inspect)
    /// between the tonic handlers and the node loop. Floor 1: overload is
    /// visible as backpressure, never as a lost request.
    pub client_inbox_capacity: usize,
    /// Capacity of the peer-message inbox. Floor 1, same contract.
    pub peer_inbox_capacity: usize,
    /// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this
    /// is deliberately bounded and lossy: the consensus driver never waits for
    /// network I/O, and current heartbeats/resends repair anything dropped
    /// here (overflow evicts the oldest message, keep-newest). The extreme (a
    /// handful of slots) makes mailbox overflow — [`Audit::dropped_at_mailbox`]
    /// — a likely event instead of a rare one.
    pub peer_queue_capacity: usize,
    /// Maximum Paxos messages packed into one protobuf/gRPC request. The
    /// extreme (one per request) maximizes h2 framing pressure and the
    /// batcher's keep-the-newest overflow shedding.
    pub delivery_batch: usize,
    /// Ticks between re-sends of an open matchmaking request
    /// (`RawNode::resend_matchmaking`), on a deployment with matchmakers.
    /// Floor 1: a re-send per tick is a request per tick per matchmaker, which
    /// the registry answers idempotently. The default is one election-timeout
    /// base, so a lost reply costs about one round trip before the retry.
    pub match_resend_ticks: u64,
}

impl Default for DriverTunables {
    fn default() -> Self {
        Self {
            tick_interval: TICK_INTERVAL,
            election_timeout_base: ELECTION_TIMEOUT_BASE,
            keep_alive_interval: GRPC_KEEP_ALIVE_INTERVAL,
            keep_alive_timeout: GRPC_KEEP_ALIVE_TIMEOUT,
            connection_timeout: GRPC_DELIVERY_TIMEOUT,
            delivery_timeout: GRPC_DELIVERY_TIMEOUT,
            read_retry_ticks: READ_RETRY_TICKS,
            snapshot_queue_capacity: GRPC_SNAPSHOT_QUEUE_CAPACITY,
            client_inbox_capacity: GRPC_CLIENT_INBOX_CAPACITY,
            peer_inbox_capacity: GRPC_PEER_INBOX_CAPACITY,
            peer_queue_capacity: GRPC_PEER_QUEUE_CAPACITY,
            delivery_batch: GRPC_DELIVERY_BATCH,
            match_resend_ticks: ELECTION_TIMEOUT_BASE,
        }
    }
}

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

fn grpc_keep_alive(tunables: &DriverTunables) -> KeepAlive {
    KeepAlive {
        interval: tunables.keep_alive_interval,
        timeout: tunables.keep_alive_timeout,
        while_idle: false,
    }
}

fn grpc_channel_config(tunables: &DriverTunables) -> ChannelConfig {
    ChannelConfig {
        connection_timeout: tunables.connection_timeout,
        keep_alive: Some(grpc_keep_alive(tunables)),
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
/// or the chunk-repair pipeline (a `buggify`-injected [`Seam`] crash). Carries
/// `node` and `seam` (`"before_sync"` — the whole un-synced batch is lost —
/// `"after_sync_before_send"` — the writes are durable but the batch's messages
/// never left — `"after_apply_before_sync"`, `"before_chunk_sync"`, or
/// `"after_chunk_restore_before_sync"`). After-sync events also carry `snapshot_offers`, the number of
/// snapshot transfers dropped with the batch. Provider-generic but inert in production, where
/// [`NoHooks`](crate::NoHooks) never fires. Purely observational; the crash
/// animation reads it to mark the persist/send seam a node died on.
pub const EV_CRASHED: &str = "crashed";

/// Tracing event: the driver deliberately skipped re-sending one or more
/// pending `Accept`s on this beat.
pub const EV_RESEND_SKIPPED: &str = "accept_resend_skipped";

/// Tracing event: the driver deliberately asked the current leader to resign.
pub const EV_LEADERSHIP_RESIGNED: &str = "leadership_resigned";

/// Tracing event: this leader cooperatively relinquished its Phase-2 authority to
/// a named successor and demoted itself in the same core call (`DPaxos` leader
/// handoff). Fields: `node`, `to`, `round`/`bnode` (the transferred authority),
/// `next_slot`, `decided`, `pending`.
pub const EV_AUTHORITY_RELINQUISHED: &str = "authority_relinquished";

/// Tracing event: this node installed a predecessor's transferred authority and
/// continues Phase 2 under it with **no** Phase 1 of its own. Fields: `node`,
/// `from`, `round`/`bnode`, `next_slot`, `tail`.
pub const EV_AUTHORITY_INSTALLED: &str = "authority_installed";

/// Tracing event: this node refused an incoming transfer. Fields: `node` plus the
/// monotone per-reason totals `target`, `stale`, `shape`, `unfit`.
pub const EV_HANDOFF_REFUSED: &str = "handoff_refused";

/// Tracing event: a handoff-installed leadership resigned because its inherited
/// read fence stayed uncovered — the deliberate fallback to ordinary Phase 1.
/// Fields: `node`, `count`.
pub const EV_HANDOFF_FENCE_EXPIRED: &str = "handoff_fence_expired";

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

/// Tracing event: a [`NodeStorage`] call failed and the driver took its
/// deliberate crash decision (see [`RunError::Storage`]). Carries `node`, the
/// human-readable `error` (the typed [`StorageError`] travels on the
/// [`Audit::storage_fault`] callback), and `decision` (`"crash"` — Stage 6's
/// only reaction). Emitted at the instant of the decision, before the crash
/// unwinds the incarnation.
pub const EV_STORAGE_FAULT: &str = "storage_fault";

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
/// without hearing an ack quorum and demoted itself (`CheckQuorum`, #95). Carries
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
        // Nine bytes starting 0xfd: disjoint from both encodings above.
        Command::Control(Control::Snap { at_index }) => {
            let mut bytes = vec![0xfd_u8];
            bytes.extend_from_slice(&at_index.0.to_le_bytes());
            value_hash(&bytes)
        }
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
        Message::SnapAck { .. } => "snap_ack",
        Message::SnapChunkRequest { .. } => "snap_chunk_request",
        Message::SnapChunkResponse { .. } => "snap_chunk_response",
        Message::Relinquish { .. } => "relinquish",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. Every ballot-carrying kind returns `Some`, `Heartbeat`
/// included — its "slot" is the commit watermark it advertises, which is
/// `None` on a leader that has chosen nothing (an empty prefix is not slot 0;
/// see [`paros_core::Message::Heartbeat`]). The kinds with no ballot at all
/// (`CheckLeader`, the catch-up pair) return `None` outright.
fn message_route(m: &Message) -> Option<(NodeId, ConfigId, Ballot, Option<Slot>)> {
    match m {
        // Phase 1 is per-ballot: report `from_slot` as the slot for the timeline.
        Message::Prepare {
            config_id,
            from,
            ballot,
            from_slot,
            ..
        }
        | Message::Promise {
            config_id,
            from,
            ballot,
            from_slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*from_slot))),
        Message::Accept {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Accepted {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Nack {
            config_id,
            from,
            ballot,
            slot,
            ..
        }
        | Message::Commit {
            config_id,
            from,
            ballot,
            slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*slot))),
        Message::Heartbeat {
            config_id,
            from,
            ballot,
            commit,
            ..
        } => Some((*from, *config_id, *ballot, *commit)),
        Message::InstallSnapshot {
            config_id,
            from,
            ballot,
            chosen_index,
            ..
        } => Some((*from, *config_id, *ballot, Some(*chosen_index))),
        // A handoff's "slot" is the allocator frontier it transfers — the
        // field that carries its meaning on a timeline.
        Message::Relinquish {
            config_id,
            from,
            ballot,
            next_slot,
            ..
        } => Some((*from, *config_id, *ballot, Some(*next_slot))),
        _ => None,
    }
}

/// What a handoff would transfer right now, from the core's public read views:
/// the span between this leader's contiguous chosen prefix and its allocator
/// frontier, plus whether it is itself still healing a hole.
///
/// Pure observation — it exists only so [`DriverHooks::initiate_handoff`] can be
/// biased toward the interesting shapes instead of firing uniformly.
fn handoff_context(node: &RawNode, candidates: usize) -> HandoffContext {
    let first_unchosen = node.hard_state().chosen_index.map_or(0, |ci| ci.0 + 1);
    let tail =
        usize::try_from(node.next_slot().0.saturating_sub(first_unchosen)).unwrap_or(usize::MAX);
    HandoffContext {
        tail,
        next_slot: node.next_slot(),
        settled: tail == 0,
        healing: node.chosen_gap().is_some(),
        candidates,
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

/// The driver's **snapshot-point repair layer** (#101, CTRL §3.5). Volatile
/// per-incarnation state beside the sans-IO core: consensus never depends on
/// snapshot custody, so all of this lives in the driver.
///
/// - Every node advertises its latest recorded decided snapshot point to the
///   leader once per tick ([`Message::SnapAck`]); the leader's set-based tally
///   is what gates the `Truncate` coupling rule (truncation is proposed only
///   once a quorum holds the covering point).
/// - A node whose boot scan reported rotted chunks of its retained point
///   pulls them from peers once per tick ([`Message::SnapChunkRequest`]);
///   peers answer chunks they hold clean, stay silent about what they lack,
///   and answer a point they have advanced past with the whole-blob
///   [`Message::InstallSnapshot`] fallback.
#[derive(Default)]
struct SnapRepair {
    /// Leader tally: decided snapshot point → nodes advertising custody of
    /// it. Points only ever advance, so a stale entry is still a sound
    /// coupling witness (any retained point at or past `up_to` covers a
    /// `Truncate{up_to}`); the map grows one entry per decided marker.
    acks: BTreeMap<u64, BTreeSet<u64>>,
    /// This node's rotted chunks of its retained point, awaiting peer repair.
    pending: BTreeMap<u64, BTreeSet<u32>>,
    /// A `Snap` marker this leadership proposed and is still waiting to see
    /// quorum custody for — dedupes marker proposals across compact retries.
    marker_pending: Option<Slot>,
}

/// Answer one peer's chunk request (see [`SnapRepair`]): chunks of the shared
/// point, silence for what this node lacks, or the whole-blob advanced
/// fallback — guarded exactly like a snapshot offer (the served state must
/// cover the advertised boundary).
// The repair layer's full context is exactly these handles; bundling them
// would only rename the same eight things.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0, to = to.0, at = at.0, chunks = chunks.len()))]
fn handle_snap_chunk_request<S, H, A>(
    node: &RawNode,
    storage: &S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    to: NodeId,
    at: Slot,
    chunks: &[u32],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let me = NodeId(out.self_id);
    match storage.latest_snap_point() {
        Some(point) if point == at => {
            let served: Vec<(u32, Value)> = chunks
                .iter()
                .filter_map(|chunk| {
                    let bytes = storage.read_snap_chunk(at, *chunk)?;
                    // Silence about a chunk this node holds is the same
                    // answer as silence about one it lacks; the requester
                    // re-asks every tick. Consulted only for a chunk that
                    // would otherwise be served.
                    if hooks.withhold_snap_chunk(to) {
                        audit.snap_chunk_withheld(me, to);
                        tracing::info!(node = me.0, at = at.0, chunk, "snap_chunk_withheld");
                        return None;
                    }
                    Some((*chunk, Value(bytes)))
                })
                .collect();
            if !served.is_empty() {
                send_messages(
                    out,
                    hooks,
                    audit,
                    vec![(
                        to,
                        Message::SnapChunkResponse {
                            config_id: node.hard_state().config_id,
                            from: me,
                            at_index: at,
                            chunks: served,
                        },
                    )],
                );
            }
        }
        Some(point) if point > at => {
            // The advanced whole-blob fallback: this node no longer retains
            // the requested point, so it serves its current snapshot instead,
            // under the same guard as any snapshot offer — the opaque bytes
            // must describe exactly the boundary the message names.
            let Some(ci) = node.hard_state().chosen_index else {
                return;
            };
            if node.app_repair().is_some() || storage.applied_slot() != Some(ci) {
                return;
            }
            let ballot = node
                .accepted()
                .get(&ci)
                .map_or(node.hard_state().max_promised_ballot, |(b, _)| *b);
            audit.snap_advanced_fallback(me, to);
            tracing::info!(node = out.self_id, to = to.0, "snap_advanced_fallback");
            send_messages(
                out,
                hooks,
                audit,
                vec![(
                    to,
                    Message::InstallSnapshot {
                        config_id: node.hard_state().config_id,
                        from: me,
                        ballot,
                        chosen_index: ci,
                        snapshot: Value(storage.snapshot()),
                        sessions: node.session_ledger(),
                    },
                )],
            );
        }
        // A point this node does not hold answers nothing: absence carries no
        // information (CTRL Figure 6 Box B), and a node *behind* the requested
        // point has nothing sound to say about it either.
        _ => {}
    }
}

/// Install repaired chunks from one peer's response, flush them durably, and
/// — once the point is whole again — restore the application from it if the
/// live state was lost below the floor (re-pointing the core's repair pump at
/// the floor so the retained suffix re-emits in order).
// The repair layer's full context is exactly these handles (see
// `handle_snap_chunk_request`); bundling them would only rename them.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, at = at.0, chunks = chunks.len()))]
fn handle_snap_chunk_response<S, H, A>(
    node: &mut RawNode,
    storage: &mut S,
    snap: &mut SnapRepair,
    hooks: &H,
    audit: &A,
    self_id: u64,
    at: Slot,
    chunks: &[(u32, Value)],
) -> Result<(), RunError>
where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let Some(pending) = snap.pending.get_mut(&at.0) else {
        return Ok(());
    };
    let mut installed = 0_u64;
    let mut bytes = 0_u64;
    for (chunk, payload) in chunks {
        if !pending.contains(chunk) {
            continue;
        }
        let clean = storage
            .write_snap_chunk(at, *chunk, &payload.0)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        pending.remove(chunk);
        installed += 1;
        bytes += u64::try_from(payload.0.len()).unwrap_or(u64::MAX);
        let _ = clean;
    }
    if installed == 0 {
        return Ok(());
    }
    let complete = pending.is_empty();
    if complete {
        snap.pending.remove(&at.0);
    }
    // Crash seam: the repaired chunks are staged but not yet flushed — the
    // only durable-write pipeline outside `drain_ready`'s seam machinery. A
    // crash here loses the staged installs whole; the reboot's scan still
    // reports the chunks faulty and the per-tick pull re-runs the repair.
    if hooks.crash_at(Seam::BeforeChunkSync) {
        audit.crashed(NodeId(self_id), Seam::BeforeChunkSync);
        tracing::info!(node = self_id, seam = "before_chunk_sync", "crashed");
        return Err(RunError::SeamCrash(Seam::BeforeChunkSync));
    }
    // Flush the chunk installs durably before reporting them (and before the
    // restore below stages the recovered application state).
    storage
        .sync(paros_core::MustSync::Sync)
        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    let blob_bytes = storage.snap_chunk_count(at).map_or(0, |count| {
        u64::from(count) * crate::storage::SNAP_CHUNK_BYTES as u64
    });
    audit.snap_chunk_repaired(NodeId(self_id), at, installed, bytes, blob_bytes);
    tracing::info!(
        node = self_id,
        at = at.0,
        chunks = installed,
        bytes,
        blob_bytes,
        "snap_chunk_repaired"
    );
    if complete
        && let Some(point) = storage
            .restore_from_snap_point()
            .map_err(|e| storage_fault_crash(audit, self_id, e))?
    {
        // Crash seam: the application restore is staged (the chunks above are
        // already durable) but its fsync has not happened. A crash here loses
        // the staged restore only; the reboot lands below the floor with a
        // clean point and recovers through a peer's `InstallSnapshot` instead.
        if hooks.crash_at(Seam::AfterChunkRestoreBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterChunkRestoreBeforeSync);
            tracing::info!(
                node = self_id,
                seam = "after_chunk_restore_before_sync",
                "crashed"
            );
            return Err(RunError::SeamCrash(Seam::AfterChunkRestoreBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        audit.snap_point_restored(NodeId(self_id), point);
        tracing::info!(node = self_id, at = point.0, "snap_point_restored");
        // The application jumped to the point (= floor - 1); re-point the
        // core's repair pump at the floor so the retained decided suffix
        // re-emits in order through the ordinary committed seam.
        if node.app_repair().is_some() {
            node.open_app_repair(node.first_slot());
        }
    }
    Ok(())
}

/// Per-tick snapshot-repair upkeep (see [`SnapRepair`]): custody
/// advertisement toward the leader, the leader's own tally and marker
/// bookkeeping, and the chunk-repair pull.
#[tracing::instrument(level = "trace", skip_all, fields(node = node.config().id.0))]
fn snap_repair_tick<S, H, A>(
    node: &RawNode,
    storage: &S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    snap: &mut SnapRepair,
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let me = NodeId(out.self_id);
    let latest = storage.latest_snap_point();
    if node.is_leader() {
        // The leader is its own first custodian, and a marker stops being
        // outstanding once a quorum advertises the point it created.
        if let Some(point) = latest {
            snap.acks.entry(point.0).or_default().insert(out.self_id);
        }
        let quorum = node.acceptors().quorum_size();
        if let Some(marker) = snap.marker_pending
            && snap
                .acks
                .get(&marker.0)
                .is_some_and(|holders| holders.len() >= quorum)
        {
            snap.marker_pending = None;
        }
    } else {
        snap.marker_pending = None;
        if let (Some(point), Some(leader)) = (latest, node.leader())
            && leader != me
        {
            // The advertisement is due: consult the pacing hook only now, when
            // skipping has an observable effect (a lost beat of the leader's
            // custody tally, re-sent next tick).
            if hooks.skip_snap_advertisement() {
                tracing::info!(node = out.self_id, "snap_advertisement_skipped");
            } else {
                send_messages(
                    out,
                    hooks,
                    audit,
                    vec![(
                        leader,
                        Message::SnapAck {
                            config_id: node.hard_state().config_id,
                            from: me,
                            at_index: point,
                        },
                    )],
                );
            }
        }
    }
    // The chunk pull: once per tick, ask every peer for the still-missing
    // chunks of the retained point. Pending chunks of a point this store has
    // advanced past are obsolete — the newer point covers everything.
    snap.pending
        .retain(|at, chunks| latest == Some(Slot(*at)) && !chunks.is_empty());
    if let Some((&at, chunks)) = snap.pending.iter().next() {
        // The pull is due: consult the pacing hook only now (skipping delays
        // the repair one beat; the pull re-issues every tick it is due).
        if hooks.skip_chunk_pull() {
            tracing::info!(node = out.self_id, "chunk_pull_skipped");
            return;
        }
        let wanted: Vec<u32> = chunks.iter().copied().collect();
        // Every pooled node is a replica that may hold the point.
        let requests: Vec<(NodeId, Message)> = node
            .config()
            .pool()
            .iter()
            .filter(|peer| **peer != me)
            .map(|peer| {
                (
                    *peer,
                    Message::SnapChunkRequest {
                        config_id: node.hard_state().config_id,
                        from: me,
                        at_index: Slot(at),
                        chunks: wanted.clone(),
                    },
                )
            })
            .collect();
        send_messages(out, hooks, audit, requests);
    }
}

/// The driver's outbound side: everything needed to put one message on the wire —
/// the gRPC clients, the task provider, and this node's id for observability
/// events. Bundled so `drain_ready` takes one parameter instead of three.
struct PeerQueues {
    regular: PeerMailbox,
    snapshot: PeerMailbox,
}

/// One peer's bounded, lossy, **keep-newest** outbound mailbox (the etcd
/// stream-mailbox shape). The consensus driver never waits for network I/O:
/// `push` always returns at once, and when the mailbox is full it evicts the
/// *oldest* undelivered message to make room for the new one.
///
/// Keep-newest is the whole point, not a detail. Every outbound class is
/// repaired by its *latest* instance — the current heartbeat, the current
/// `Accept` re-send, the current catch-up page — so under sustained overload
/// the messages worth delivering are the newest ones. The alternative,
/// refusing the new message while stale ones drain (what a bounded mpsc
/// `try_send` does), starves whole classes deterministically once a peer link
/// is slow: with a handful of slots and a delivery round trip of several
/// ticks, the slots free up once per round trip and refill with whatever is
/// enqueued first afterward, so a class that is always enqueued a beat later
/// than the heartbeat is dropped on every round trip, forever. That is an
/// adversary dropping every message of one kind, which defeats eventual
/// synchrony, and it is precisely how a lagging follower's catch-up responses
/// were lost for an entire quiet tail (sim seeds `14371623759479170018`,
/// `13938523914823716398`: 983 of 983 evicted at a 5-slot mailbox behind a
/// ~277 ms link).
///
/// Recency alone is not enough either: a leader re-sends *every* pending
/// `Accept` on every beat, so one beat's burst can fill a small mailbox by
/// itself and evict the single catch-up response enqueued just before it, on
/// every round trip (seeds `12153861921929631187`, `9558440018523712995`,
/// `1336557888375411500`, red on a plain keep-newest mailbox). So eviction is
/// **per kind**: a new message displaces the oldest queued message *of its own
/// kind* when one exists, and the oldest overall only when none does. A class
/// can then only be crowded out by itself — an `Accept` burst churns the
/// queued accepts, the current heartbeat replaces the stale one, the current
/// catch-up page replaces the previous — which is the same separation etcd
/// gets from carrying heartbeats and appends on distinct streams. The scan is
/// linear in the mailbox and runs only on overflow.
///
/// The mailbox also carries the two **drain-side** hook decisions
/// ([`DriverHooks::hold_peer_delivery`], [`DriverHooks::reverse_delivery_batch`]).
/// They are taken here, at enqueue time on the node loop, and merely *read* by
/// the delivery task, because a decision is a randomness draw and the delivery
/// task is `spawn_task(..).detach()`ed: a detached task's poll schedule is not
/// part of the simulation's deterministic step order the way the node loop is,
/// so drawing inside one lets a task that outlives its simulation shift the
/// *next* run's draw sequence. That is not a theoretical hazard — consulting
/// these two hooks from inside the delivery task broke
/// `same_seed_replays_identically` on CI (seed 42's first in-process replay
/// diverged from its second, on a run that was clean locally). Deciding on the
/// node loop restores the invariant every other hook already had: **simulation
/// randomness is drawn only where the simulation is stepping deterministically.**
#[derive(Clone)]
struct PeerMailbox {
    inner: Arc<Mutex<VecDeque<internal::ConsensusMessage>>>,
    capacity: usize,
    wake: Arc<tokio::sync::Notify>,
    /// Set by the enqueue side: the next drained batch waits one tick before
    /// it goes on the wire. Read-and-cleared by the delivery task.
    hold_next: Arc<AtomicBool>,
    /// Set by the enqueue side: the next drained batch is handed to the peer
    /// in reverse order. Read-and-cleared by the delivery task.
    reverse_next: Arc<AtomicBool>,
}

impl PeerMailbox {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "a peer mailbox holds at least one message");
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            wake: Arc::new(tokio::sync::Notify::new()),
            hold_next: Arc::new(AtomicBool::new(false)),
            reverse_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<internal::ConsensusMessage>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn is_full(&self) -> bool {
        self.lock().len() >= self.capacity
    }

    /// Enqueue `message`, evicting and returning one undelivered message when
    /// the mailbox is full: the oldest of the *same kind* as `message` if one
    /// is queued, else the oldest overall — unless `evict_across_kinds`, which
    /// takes the oldest overall outright. `overtake` enqueues at the front
    /// instead of the back. Both are the driver-hook perturbations
    /// ([`DriverHooks::overtake_in_mailbox`], [`DriverHooks::evict_across_kinds`]);
    /// production passes `false` for both. Never blocks.
    #[tracing::instrument(level = "trace", skip_all, fields(overtake, evict_across_kinds))]
    fn push(
        &self,
        message: internal::ConsensusMessage,
        overtake: bool,
        evict_across_kinds: bool,
    ) -> Option<internal::ConsensusMessage> {
        let evicted = {
            let mut queue = self.lock();
            let evicted = if queue.len() >= self.capacity {
                let kind = proto_message_kind(&message);
                let victim = if evict_across_kinds {
                    0
                } else {
                    queue
                        .iter()
                        .position(|queued| proto_message_kind(queued) == kind)
                        .unwrap_or(0)
                };
                queue.remove(victim)
            } else {
                None
            };
            if overtake {
                queue.push_front(message);
            } else {
                queue.push_back(message);
            }
            assert!(
                queue.len() <= self.capacity,
                "a peer mailbox never exceeds its capacity"
            );
            evicted
        };
        self.wake.notify_one();
        evicted
    }

    fn try_pop(&self) -> Option<internal::ConsensusMessage> {
        self.lock().pop_front()
    }

    /// Take the enqueue side's "hold the next drain" decision, clearing it.
    fn take_hold(&self) -> bool {
        self.hold_next.swap(false, Ordering::Relaxed)
    }

    /// Take the enqueue side's "reverse the next batch" decision, clearing it.
    fn take_reverse(&self) -> bool {
        self.reverse_next.swap(false, Ordering::Relaxed)
    }

    fn len(&self) -> usize {
        self.lock().len()
    }

    /// Wait for the next message. A `Notify` permit is stored when nobody is
    /// waiting, so a push that lands between the empty check and the await is
    /// never lost.
    async fn recv(&self) -> internal::ConsensusMessage {
        loop {
            if let Some(message) = self.try_pop() {
                return message;
            }
            self.wake.notified().await;
        }
    }
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
    #[tracing::instrument(level = "trace", skip_all, fields(node = self.self_id, to = to.0, kind = message_kind(msg)))]
    fn transmit<H: DriverHooks, A: Audit>(&self, hooks: &H, audit: &A, to: NodeId, msg: &Message) {
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
                config_id,
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
                config_id = config_id.0,
                "msg_sent"
            ),
            _ => match message_route(msg) {
                Some((_, config_id, ballot, Some(slot))) => tracing::info!(
                    node = self.self_id,
                    to = to.0,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    config_id = config_id.0,
                    "msg_sent"
                ),
                // A beat from a leader whose chosen prefix is still empty: there is
                // no slot to report, and reporting a bare `0` would put back on the
                // trace exactly the sentinel #56 took off the wire.
                Some((_, config_id, ballot, None)) => tracing::info!(
                    node = self.self_id,
                    to = to.0,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    config_id = config_id.0,
                    "msg_sent"
                ),
                None => {
                    if let Some(config_id) = msg.config_id() {
                        tracing::info!(
                            node = self.self_id,
                            to = to.0,
                            kind,
                            config_id = config_id.0,
                            "msg_sent"
                        );
                    } else {
                        tracing::info!(node = self.self_id, to = to.0, kind, "msg_sent");
                    }
                }
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
            // The mailbox's four decisions, all taken here on the node loop,
            // each consulted only where it can have an observable effect.
            //
            // Two act on this enqueue: overtake needs something already queued
            // to jump, evicting across kinds needs a full queue to evict from.
            let overtake = !queue.is_empty() && hooks.overtake_in_mailbox(to, msg);
            let evict_across_kinds = queue.is_full() && hooks.evict_across_kinds(to, msg);
            // Two arm the *drain*: this message's arrival is what makes the
            // next batch worth holding or reversing. Holding needs a queue that
            // is already non-empty (parking a drain of nothing changes
            // nothing); reversing needs at least two messages, since this one
            // plus a queued one is the smallest reorderable batch. Decided
            // here, applied there — see [`PeerMailbox`] for why the delivery
            // task must not draw.
            if !queue.is_empty() && hooks.hold_peer_delivery(to) {
                queue.hold_next.store(true, Ordering::Relaxed);
            }
            if !queue.is_empty() && hooks.reverse_delivery_batch(to) {
                queue.reverse_next.store(true, Ordering::Relaxed);
            }
            if let Some(evicted) = queue.push(message, overtake, evict_across_kinds) {
                // Deliberately lossy (etcd-style bounded mailbox, keep-newest),
                // but never silent: the audit sees the drop the moment it
                // happens, naming the *evicted* message, not the one that
                // displaced it.
                let evicted_kind = proto_message_kind(&evicted);
                audit.dropped_at_mailbox(NodeId(self.self_id), to, evicted_kind);
                tracing::debug!(
                    node = self.self_id,
                    to = to.0,
                    kind = evicted_kind,
                    "evicted oldest Paxos message from a full peer gRPC mailbox"
                );
            }
        }
    }
}

/// A short, stable label for an encoded [`internal::ConsensusMessage`], for
/// the mailbox-drop audit report (mirrors [`message_kind`], which needs the
/// decoded domain [`Message`] the delivery task no longer has).
fn proto_message_kind(m: &internal::ConsensusMessage) -> &'static str {
    use internal::consensus_message::Kind;
    match &m.kind {
        Some(Kind::Prepare(_)) => "prepare",
        Some(Kind::Promise(_)) => "promise",
        Some(Kind::Accept(_)) => "accept",
        Some(Kind::Accepted(_)) => "accepted",
        Some(Kind::Nack(_)) => "nack",
        Some(Kind::Commit(_)) => "commit",
        Some(Kind::CatchUpRequest(_)) => "catchup_request",
        Some(Kind::CatchUpResponse(_)) => "catchup_response",
        Some(Kind::InstallSnapshot(_)) => "install_snapshot",
        Some(Kind::CheckLeader(_)) => "check_leader",
        Some(Kind::Heartbeat(_)) => "heartbeat",
        Some(Kind::HeartbeatAck(_)) => "heartbeat_ack",
        Some(Kind::SnapAck(_)) => "snap_ack",
        Some(Kind::Relinquish(_)) => "relinquish",
        Some(Kind::SnapChunkRequest(_)) => "snap_chunk_request",
        Some(Kind::SnapChunkResponse(_)) => "snap_chunk_response",
        None => "unknown",
    }
}

/// Feed bounded unary batches over one reconnecting h2 channel per peer. While
/// a batch is in flight, new protocol messages accumulate for the next batch;
/// on failure Paxos heartbeats/resends repair anything lost with that RPC.
// The parameters are one delivery lane's complete wiring (client, clocks,
// lifecycle, queue, batch shape, and the audit identity for drop reports);
// a bundle would only rename the same eight things.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, to = to.0))]
async fn run_peer_delivery<P: Providers, A: Audit>(
    client: ParosInternalClient<ReconnectingChannel<P, tonic::body::Body>>,
    time: P::Time,
    shutdown: CancellationToken,
    messages: PeerMailbox,
    tunables: DriverTunables,
    audit: A,
    self_id: u64,
    to: NodeId,
) {
    let batch_limit = tunables.delivery_batch;
    let mut carried = None;
    loop {
        let first = if let Some(message) = carried.take() {
            message
        } else {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                message = messages.recv() => message,
            }
        };
        // Park the drain for one tick before batching. The mailbox keeps
        // filling while we wait, so the batcher below meets a real backlog
        // rather than the one-or-two messages a promptly drained queue holds —
        // the concurrency window an enqueue-time-only perturbation cannot
        // reach. The *decision* was taken on the node loop (see [`PeerMailbox`]
        // for why); this task only reads it.
        if messages.take_hold() {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                _ = time.sleep(tunables.tick_interval) => {}
            }
        }
        let mut attempt_client = client.clone();
        let (batch, next) = delivery_batch(first, &messages, batch_limit, &audit, self_id, to);
        carried = next;
        let outcome = moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = time.timeout(tunables.delivery_timeout, attempt_client.deliver(batch)) => result,
        };
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(status)) => {
                audit.delivery_failed(NodeId(self_id), to);
                tracing::debug!(%status, "peer gRPC delivery failed");
            }
            Err(_) => {
                audit.delivery_failed(NodeId(self_id), to);
                tracing::debug!("peer gRPC delivery timed out");
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, to = to.0))]
fn delivery_batch<A: Audit>(
    mut first: internal::ConsensusMessage,
    messages: &PeerMailbox,
    batch_limit: usize,
    audit: &A,
    self_id: u64,
    to: NodeId,
) -> (internal::Deliver, Option<internal::ConsensusMessage>) {
    // Do not spend the eventual-synchrony tail replaying a bounded but stale
    // stale chaos-era traffic. Peer delivery is allowed to lose messages; the
    // protocol's current heartbeat, Accept resend, and catch-up paths repair
    // them. Keep the newest batch so recovery signals can overtake old ballots
    // — the drain-side half of the mailbox's keep-newest policy (see
    // [`PeerMailbox`] for the enqueue-side half, which is what keeps a small
    // mailbox from starving a message class). The shed threshold stays at the
    // *default* batch depth even when the buggified `batch_limit` is smaller:
    // shedding detects a stale backlog, and tying it to a one-message batch
    // turns "drop stale traffic" into "drop everything but the newest message
    // on every drain" — a deterministic starvation of whole message classes
    // that no repair path can outrun (an adversary dropping every message of
    // one kind forever defeats eventual synchrony, which the knob's extreme
    // must not do).
    while messages.len() >= batch_limit.max(GRPC_DELIVERY_BATCH) {
        let Some(newer) = messages.try_pop() else {
            break;
        };
        // The stale head of the backlog is discarded, never silently: report
        // it at the instant of the drop, like the enqueue-side overflow.
        audit.dropped_at_mailbox(NodeId(self_id), to, proto_message_kind(&first));
        tracing::debug!(
            node = self_id,
            to = to.0,
            "dropped stale Paxos message from delivery backlog"
        );
        first = newer;
    }
    let mut batch = Vec::with_capacity(batch_limit);
    let mut batch_bytes = first.encoded_len();
    batch.push(wire_message(first));
    let mut carried = None;
    while batch.len() < batch_limit {
        let Some(message) = messages.try_pop() else {
            break;
        };
        if batch_bytes.saturating_add(message.encoded_len()) > GRPC_DELIVERY_BATCH_BYTES {
            carried = Some(message);
            break;
        }
        batch_bytes += message.encoded_len();
        batch.push(wire_message(message));
    }
    // The drain-side reorder: the peer transport never promised ordering, and
    // reversing a whole batch is the shape a retried RPC or a re-established
    // stream produces. Decided on the node loop (see [`PeerMailbox`]), applied
    // only where it can have an effect.
    if batch.len() > 1 && messages.take_reverse() {
        batch.reverse();
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
#[tracing::instrument(level = "debug", skip_all, fields(offers = snapshot_offers.len()))]
fn send_snapshot_offers<S, H, A>(
    storage: &mut S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    snapshot_offers: &[(NodeId, Slot, Ballot, ConfigId)],
    sessions: &[SessionEntry],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    for &(to, offered_index, ballot, config_id) in snapshot_offers {
        // The mismatch skip below, taken spuriously: the requester re-asks
        // every tick and any other custodian may answer, so an unserved beat
        // is always safe — and this reaches the "nobody served me this round"
        // state without needing an application repair to be open.
        //
        // Deliberately *not* reported through `Audit::snapshot_offer_skipped`:
        // that channel's coverage gate claims a **mismatched** offer was
        // withheld, and a hook that can fire on a perfectly matched offer would
        // satisfy it trivially. The hook's own BUGGIFY pairing proves this
        // location fires; the trace field says which of the two skips a reader
        // is looking at.
        if hooks.skip_snapshot_offer(to) {
            tracing::info!(
                node = out.self_id,
                offered = offered_index.0,
                reason = "hook",
                "snapshot_offer_skipped"
            );
            continue;
        }
        if storage.applied_slot() != Some(offered_index) {
            // An offered snapshot must describe exactly the application prefix
            // the protocol message names. Stage 8 makes a mismatch a
            // legitimate transient — an open application repair holds the
            // applied prefix behind the chosen index — so the offer is
            // *skipped*, never sent wrong and never fatal: the requester
            // re-asks each beat and another peer (or this one, once healed)
            // serves it. The core already withholds offers while its own
            // repair is open; this driver-side guard covers any other
            // application lag the core cannot see.
            audit.snapshot_offer_skipped(NodeId(out.self_id), offered_index);
            tracing::info!(
                node = out.self_id,
                offered = offered_index.0,
                reason = "mismatch",
                "snapshot_offer_skipped"
            );
            continue;
        }
        let message = Message::InstallSnapshot {
            config_id,
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
        out.transmit(hooks, audit, to, &message);
        if hooks.duplicate_outgoing(to, &message) {
            audit.duplicated_at_send(NodeId(out.self_id), to, &message);
            tracing::info!(
                node = out.self_id,
                to = to.0,
                kind = message_kind(&message),
                "msg_duplicated_at_send"
            );
            out.transmit(hooks, audit, to, &message);
        }
    }
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
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, committed = committed.len()))]
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
        let Some(replies) = waiters.pending.remove(slot) else {
            continue;
        };
        // The slot's decided identity. A reply may only claim `committed: true`
        // if the slot decided *this waiter's* command: a stale leader can park
        // a proposal on a slot the majority then decides differently (it
        // learns the decision by `Commit`/catch-up while still believing
        // itself leader — nothing in `on_commit` demotes it), and acking by
        // slot number alone then told a client its write was committed while
        // no node ever applied it (network-axis seed 12491191414293127136).
        // A control command — including a #94 duplicate suppressed to a Noop —
        // matches no waiter.
        let decided = command.user().map(|e| (e.client.0, e.seq.0));
        for (client, seq, waiter) in replies {
            if decided != Some((client, seq)) {
                // Not this proposal's commit: its fate is unknown here (the
                // core's dedup tables track it if it is still in flight
                // anywhere). Answer a retry-now redirect instead of holding
                // the reply to the client's deadline; the retry goes through
                // the honest `(client, seq)` dedup path.
                audit.waiter_superseded(NodeId(self_id), *slot);
                tracing::info!(node = self_id, slot = slot.0, "propose_waiter_superseded");
                let _ = waiter.send(ProposeAck {
                    seq,
                    leader: Some(self_id),
                    committed: false,
                    slot: None,
                });
                continue;
            }
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

/// Send one batch's addressed messages (fire-and-forget). The core addresses
/// each one; the driver maps `NodeId` → address. Each message may be dropped
/// at this seam — per-message loss the network layer cannot produce on its own
/// (a TCP stream loses intervals, never one isolated message), with
/// `resend_pending` re-deriving what matters — or sent twice (retransmission
/// is legal transport behavior; set-based quorum counting must tolerate it).
#[tracing::instrument(level = "trace", skip_all, fields(node = out.self_id, messages = messages.len()))]
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
        out.transmit(hooks, audit, to, &msg);
        if hooks.duplicate_outgoing(to, &msg) {
            audit.duplicated_at_send(NodeId(self_id), to, &msg);
            tracing::info!(
                node = self_id,
                to = to.0,
                kind = message_kind(&msg),
                "msg_duplicated_at_send"
            );
            out.transmit(hooks, audit, to, &msg);
        }
    }
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
// One linear durability pipeline: every step is ordered against its neighbors
// (persist → send → apply → app-fsync → truncate → offers → acks), so slicing
// it into helpers would scatter the ordering contract this function *is*.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all, fields(node = node.config().id.0))]
fn drain_ready<S, H, A>(
    node: &mut RawNode,
    storage: &mut S,
    out: &Outbound,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
) -> Result<Vec<(MatchmakerId, MatchRequest)>, RunError>
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
    let snapshot_offers: Vec<(NodeId, Slot, Ballot, ConfigId)> = ready.snapshot_offers().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    let recovery_batch = ready.recovery_batch();
    // The matchmaking requests ride the same persist-before-send edge as the
    // peer messages (the candidate's promise raise is in this batch) and are
    // handed back to the loop, which owns the matchmaker links.
    let match_requests: Vec<(MatchmakerId, MatchRequest)> = ready.match_requests().to_vec();
    ready.advance();

    // 1. Persist durable writes FIRST, each op in order, flush per MustSync, and
    //    surface the persisted state for the safety + recovery oracles. The
    //    `BeforeSync` crash seam lives inside `persist_writes`.
    let promised = node.hard_state().max_promised_ballot;
    persist_writes(storage, &writes, must_sync, promised, self_id, hooks, audit)?;

    if let Some((started, gap_fills, remaining)) = recovery_batch {
        let started = u64::try_from(started).unwrap_or(u64::MAX);
        let gap_fills = u64::try_from(gap_fills).unwrap_or(u64::MAX);
        let remaining = u64::try_from(remaining).unwrap_or(u64::MAX);
        audit.recovery_batch(NodeId(self_id), started, gap_fills, remaining);
        tracing::info!(
            node = self_id,
            started,
            gap_fills,
            remaining,
            "leader_recovery_batch"
        );
        if gap_fills > 0 {
            tracing::info!(node = self_id, gaps = gap_fills, "election_gap_filled");
        }
    }

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
    if (!writes.is_empty()
        || !messages.is_empty()
        || !match_requests.is_empty()
        || snapshot_offer_count > 0)
        && hooks.crash_at(Seam::AfterSyncBeforeSend)
    {
        audit.crashed(NodeId(self_id), Seam::AfterSyncBeforeSend);
        tracing::info!(
            node = self_id,
            seam = "after_sync_before_send",
            snapshot_offers = snapshot_offer_count as u64,
            "crashed"
        );
        return Err(RunError::SeamCrash(Seam::AfterSyncBeforeSend));
    }

    // 2. Send messages — only after (1) is durable.
    send_messages(out, hooks, audit, messages);

    // 3. Apply newly chosen entries (already durable, in contiguous order) —
    //    surface them to the oracles and ack any clients waiting on each slot
    //    (ack-on-commit: a held reply fires only now that its slot is chosen).
    let chosen_index = node.hard_state().chosen_index;
    let mut snap_markers: Vec<Slot> = Vec::new();
    for (slot, command) in &committed {
        let chosen_index = chosen_index.ok_or_else(|| {
            SimulationError::InvalidState("committed command without chosen prefix".into())
        })?;
        storage
            .apply(chosen_index, *slot, command)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        // A decided snapshot point (#101): the marker's boundary state is the
        // application state at exactly this instant of the contiguous walk,
        // so the point is captured here, mid-loop, and flushed with the
        // batch's application fsync below. The point is recorded at the
        // marker's *own slot* — a marker minted by `propose_snap_marker`
        // carries the identical `at_index`, and a hand-built mismatch is
        // external input (never asserted, only noted).
        if let Command::Control(Control::Snap { at_index }) = command {
            if at_index != slot {
                tracing::warn!(
                    node = self_id,
                    slot = slot.0,
                    at_index = at_index.0,
                    "snap_marker_index_mismatch"
                );
            }
            storage
                .record_snapshot(*slot)
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            snap_markers.push(*slot);
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
            return Err(RunError::SeamCrash(Seam::AfterApplyBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }

    // The decided snapshot points captured above are durable with the
    // application fsync; only now are they reported (never claiming a point a
    // crash-before-sync would discard).
    for at in &snap_markers {
        audit.snap_recorded(NodeId(self_id), *at);
        tracing::info!(node = self_id, at = at.0, "snap_recorded");
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
        send_snapshot_offers(storage, out, hooks, audit, &snapshot_offers, &sessions);
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

    // The previous recovery page is now fully durable, sent, and applied. Only
    // at this boundary may the core materialize the next bounded Ready page;
    // doing it inside `Ready::advance` would move single-node state ahead of the
    // I/O the async driver is still performing.
    node.advance_recovery();

    Ok(match_requests)
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
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, writes = writes.len(), must_sync = ?must_sync))]
fn persist_writes<S: NodeStorage, H: DriverHooks, A: Audit>(
    storage: &mut S,
    writes: &[WriteOp],
    must_sync: paros_core::MustSync,
    promised: Ballot,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError> {
    let mut promise_changed = false;
    for op in writes {
        match op {
            WriteOp::SetPromise(ballot) => {
                storage
                    .persist_ballot(*ballot)
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                promise_changed = true;
            }
            WriteOp::AppendAccepted {
                slot,
                ballot,
                command,
            } => {
                storage
                    .append_accepted(*slot, *ballot, command.clone())
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::SetChosenIndex(slot) => {
                storage
                    .set_chosen_index(*slot)
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::Truncate { first, sealed } => {
                storage
                    .truncate(*first, sealed)
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                snapshot,
                sessions,
            } => {
                storage
                    .install_snapshot(*chosen_index, *ballot, snapshot.0.clone(), sessions)
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
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
        return Err(RunError::SeamCrash(Seam::BeforeSync));
    }

    if !writes.is_empty() {
        storage
            .sync(must_sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
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
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
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

/// Map a [`StorageError`] into the driver's **deliberate crash decision**: a
/// storage fault never lets the node keep running on state it does not durably
/// have. The decision is reported through [`Audit::storage_fault`] (typed, at
/// the instant it is made) and surfaced as [`EV_STORAGE_FAULT`], then
/// [`RunError::Storage`] unwinds the incarnation. Production semantics: a
/// storage fault is a process exit (crash-only); the sim's node loop matches
/// the variant and routes to the crash/restart path instead.
fn storage_fault_crash<A: Audit>(audit: &A, self_id: u64, e: StorageError) -> RunError {
    audit.storage_fault(NodeId(self_id), &e, StorageFaultDecision::Crash);
    tracing::warn!(node = self_id, error = %e, decision = "crash", "storage_fault");
    RunError::Storage(e)
}

/// Why [`run_node`] stopped, typed. The driver's *domain* outcomes — a crash it
/// decided to take — are first-class variants a caller matches on; a moonpool
/// [`SimulationError`] appears only wrapped in [`RunError::Infra`], for genuine
/// provider/infrastructure failures. The simulation's error type never carries
/// a protocol-layer decision.
#[derive(Debug)]
pub enum RunError {
    /// A hook-injected crash at a durability [`Seam`] inside a `Ready` batch
    /// (simulation only: production's `NoHooks` never fires). The caller
    /// recovers by re-running [`run_node`], which rebuilds volatile state from
    /// durable storage.
    SeamCrash(Seam),
    /// A [`NodeStorage`] call failed and the driver took its fail-stop crash
    /// decision — never an incidental error propagation. In **production**
    /// this is a crash-only process exit; recovery is the next boot. In
    /// simulation the node loop recovers exactly like a seam crash: re-run
    /// [`run_node`] against whatever the disk *actually* holds (the recovery
    /// path must be correct for both outcomes of an ambiguous write; see
    /// [`crate::WriteOutcome`]).
    Storage(StorageError),
    /// A provider/infrastructure failure (bind, listen, address parsing): the
    /// only place a [`SimulationError`] escapes the driver, and a genuine
    /// failure — never a recovery signal.
    Infra(SimulationError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::SeamCrash(seam) => write!(f, "injected crash at durability seam {seam:?}"),
            RunError::Storage(e) => write!(f, "storage fault, crashing: {e}"),
            RunError::Infra(e) => write!(f, "infrastructure failure: {e}"),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunError::Storage(e) => Some(e),
            RunError::Infra(e) => Some(e),
            RunError::SeamCrash(_) => None,
        }
    }
}

impl From<SimulationError> for RunError {
    fn from(e: SimulationError) -> Self {
        RunError::Infra(e)
    }
}

/// Draw a randomized election timeout in `[T, 2T)` ticks from the provider's
/// seeded RNG. Drawn here, never in the zero-dep core, so the core stays
/// deterministic and dependency-free while a seed still replays bit-identically.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, base))]
fn draw_election_timeout<P: Providers, H: DriverHooks, A: Audit>(
    providers: &P,
    hooks: &H,
    audit: &A,
    self_id: u64,
    base: u64,
) -> u64 {
    if hooks.shortest_election_timeout() {
        audit.election_timeout_extreme(NodeId(self_id), base);
        tracing::info!(node = self_id, ticks = base, "election_timeout_extreme");
        base
    } else if hooks.longest_election_timeout() {
        // The other jitter extreme: the highest value the honest draw below
        // could produce. Consulted only when the shortest hook stayed quiet,
        // so the two extremes remain independent locations. Its BUGGIFY
        // pairing gate fires in the sim hook implementation (the audit's
        // `election_timeout_extreme` reach gate is the shortest extreme's).
        let ticks = base * 2 - 1;
        tracing::info!(node = self_id, ticks, "election_timeout_extreme");
        ticks
    } else {
        providers.random().random_range(base..base * 2)
    }
}

/// Report this batch's cooperative-handoff transitions and return whether an
/// authority was **installed** in it.
///
/// Three channels, each a different fact: the install of a predecessor's
/// authority (a leadership acquired with *no* Phase 1, so it is deliberately
/// not reported through [`Audit::elected`], whose "leadership ballots strictly
/// increase" reading is about a node's own campaigns), the per-reason refusal
/// totals the wire guards accumulated, and the inherited-fence resignations.
/// The relinquish half is reported at its own call site, at the instant the
/// authority changes hands.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_handoff<A: Audit>(
    node: &RawNode,
    last: &mut HandoffCounters,
    self_id: u64,
    audit: &A,
) -> bool {
    let handoff = node.handoff_counters();
    let installed_now = handoff.installed != last.installed;
    if handoff.rejected_target != last.rejected_target
        || handoff.rejected_stale != last.rejected_stale
        || handoff.rejected_shape != last.rejected_shape
        || handoff.rejected_unfit != last.rejected_unfit
    {
        audit.handoff_refused(
            NodeId(self_id),
            handoff.rejected_target,
            handoff.rejected_stale,
            handoff.rejected_shape,
            handoff.rejected_unfit,
        );
        tracing::info!(
            node = self_id,
            target = handoff.rejected_target,
            stale = handoff.rejected_stale,
            shape = handoff.rejected_shape,
            unfit = handoff.rejected_unfit,
            "handoff_refused"
        );
    }
    if handoff.fence_step_downs != last.fence_step_downs {
        audit.handoff_fence_expired(NodeId(self_id), handoff.fence_step_downs);
        tracing::info!(
            node = self_id,
            count = handoff.fence_step_downs,
            "handoff_fence_expired"
        );
    }
    // Keyed on the install counter rather than a role transition: an install
    // can also replace a leadership this node already held at a lower ballot.
    if installed_now && let LeadershipOrigin::Handoff { from } = node.leadership_origin() {
        let ballot = node.ballot();
        let next_slot = node.next_slot();
        let tail = u64::try_from(handoff_context(node, 0).tail).unwrap_or(u64::MAX);
        audit.authority_installed(NodeId(self_id), from, ballot, next_slot, tail);
        tracing::info!(
            node = self_id,
            from = from.0,
            round = ballot.round,
            bnode = ballot.node.0,
            next_slot = next_slot.0,
            tail,
            "authority_installed"
        );
    }
    *last = handoff;
    installed_now
}

/// The loop's cross-batch delta trackers, so `maintain` reports each monotone
/// core counter exactly once per change.
struct Deltas {
    role: NodeRole,
    duplicates: u64,
    quorum_lost: u64,
    repair: (u64, u64, u64, u64, u64),
    handoff: HandoffCounters,
    membership: (u64, u64),
    matchmaking: Option<Ballot>,
}

/// The driver's **matchmaker links** (#120): one reconnecting channel per
/// matchmaker of the deployment, and the inbox the answers come back through.
/// Empty on plain Multi-Paxos, whose driver never speaks the matchmaker
/// contract.
struct MatchmakerLinks<P: Providers> {
    clients:
        BTreeMap<MatchmakerId, ParosMatchmakerClient<ReconnectingChannel<P, tonic::body::Body>>>,
    replies: mpsc::Sender<MatchReply>,
    timeout: Duration,
    shutdown: CancellationToken,
}

/// Surface a matchmaking phase the batch just opened (#120): once per
/// campaign, keyed on its ballot, and *before* the batch's requests leave —
/// the audit folds the campaign's opening ahead of its first request.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn surface_matchmaking<A: Audit>(
    node: &RawNode,
    last_matchmaking: &mut Option<Ballot>,
    audit: &A,
    self_id: u64,
) {
    if let Some((ballot, config, reconfiguration)) = node.matchmaking()
        && *last_matchmaking != Some(ballot)
    {
        *last_matchmaking = Some(ballot);
        audit.matchmaking_started(NodeId(self_id), ballot, config, reconfiguration);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            members = config.members.len() as u64,
            reconfiguration,
            "matchmaking_started"
        );
    }
}

/// Send one batch of matchmaking requests, each as its own RPC task whose
/// answer (if any) is fed back into the node loop through the reply inbox.
/// The task draws no randomness and consults no hook — a lost or late reply
/// is exactly what [`RawNode::resend_matchmaking`] exists for.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, requests = requests.len()))]
fn send_match_requests<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    requests: Vec<(MatchmakerId, MatchRequest)>,
) {
    for (matchmaker, request) in requests {
        let Some(client) = links.clients.get(&matchmaker) else {
            tracing::warn!(
                node = self_id,
                matchmaker = matchmaker.0,
                "unknown matchmaker"
            );
            continue;
        };
        audit.match_request_sent(NodeId(self_id), matchmaker, request.ballot);
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            round = request.ballot.round,
            "match_request_sent"
        );
        let mut client = client.clone();
        let replies = links.replies.clone();
        let time = providers.time().clone();
        let timeout = links.timeout;
        let shutdown = links.shutdown.clone();
        let wire = wire_match_request(&request);
        providers
            .task()
            .spawn_task("paros-matchmaking-request", async move {
                let answer = moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    result = time.timeout(timeout, client.matchmake(wire)) => result,
                };
                match answer {
                    Ok(Ok(response)) => match match_reply_from_wire(response.into_inner()) {
                        Ok(reply) => {
                            let _ = replies.send(reply).await;
                        }
                        Err(error) => tracing::warn!(node = self_id, error, "bad match reply"),
                    },
                    Ok(Err(status)) => {
                        tracing::debug!(node = self_id, %status, "matchmaking RPC failed");
                    }
                    Err(_) => tracing::debug!(node = self_id, "matchmaking RPC timed out"),
                }
            })
            .detach();
    }
}

/// Report what one matchmaker reply did to the open campaign.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_match_step<A: Audit>(
    node: &RawNode,
    audit: &A,
    self_id: u64,
    matchmaker: MatchmakerId,
    ballot: Ballot,
    step: &MatchStep,
) {
    match step {
        MatchStep::Ignored => {}
        MatchStep::Registered { remaining } => {
            audit.match_registered_by(NodeId(self_id), matchmaker, ballot, *remaining);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                remaining = *remaining as u64,
                "match_registered_by"
            );
        }
        MatchStep::Completed {
            prior,
            watermark,
            registered_by,
        } => {
            // The closing reply is a registration too: fold it before the
            // completion so the audit's registering set is the full quorum.
            audit.match_registered_by(NodeId(self_id), matchmaker, ballot, 0);
            audit.matchmaking_completed(
                NodeId(self_id),
                ballot,
                prior,
                *watermark,
                *registered_by,
                node.matchmaking_disagreements(),
            );
            tracing::info!(
                node = self_id,
                round = ballot.round,
                prior = prior.len() as u64,
                watermark_round = watermark.round,
                registered_by = *registered_by as u64,
                "matchmaking_completed"
            );
        }
        MatchStep::Refused(refusal) => {
            audit.matchmaking_refused(NodeId(self_id), matchmaker, ballot, *refusal);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                reason = ?refusal,
                "matchmaking_refused"
            );
        }
    }
}

/// Surface the campaign-membership transitions (#122): a campaign this node
/// declined as a non-member, and a leadership it resigned once its own
/// reconfiguration removed it.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn report_membership<A: Audit>(
    node: &RawNode,
    last_membership: &mut (u64, u64),
    self_id: u64,
    audit: &A,
) {
    let membership = node.membership_counters();
    if membership.0 != last_membership.0 {
        audit.campaign_skipped_non_member(NodeId(self_id), membership.0);
        tracing::info!(
            node = self_id,
            count = membership.0,
            "campaign_skipped_non_member"
        );
    }
    if membership.1 != last_membership.1 {
        audit.non_member_leader_resigned(NodeId(self_id), membership.1);
        tracing::info!(
            node = self_id,
            count = membership.1,
            "non_member_leader_resigned"
        );
    }
    *last_membership = membership;
}

/// Post-batch upkeep: feed the core a fresh randomized election timeout whenever
/// its election clock reset, emit `leader_elected` on the transition to Leader,
/// and drop held client replies on step-down (so clients time out and retry the
/// new leader).
// The `last_*` parameters are the loop's cross-batch delta trackers (role,
// #94 suppressions, `CheckQuorum` step-downs); bundling them into a struct
// would only rename the same nine things.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn maintain<P: Providers, H: DriverHooks, A: Audit>(
    node: &mut RawNode,
    providers: &P,
    last: &mut Deltas,
    waiters: &mut ClientWaiters,
    self_id: u64,
    election_base: u64,
    hooks: &H,
    audit: &A,
) {
    let Deltas {
        role: last_role,
        duplicates: last_duplicates,
        quorum_lost: last_quorum_lost,
        repair: last_repair,
        handoff: last_handoff,
        membership: last_membership,
        matchmaking: _,
    } = last;
    if node.needs_election_timeout() {
        node.set_election_timeout(draw_election_timeout(
            providers,
            hooks,
            audit,
            self_id,
            election_base,
        ));
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
    // Surface any repair progress (Stage 8): in-place heals, straggler
    // resolutions, recovery-timeout resignations, and the repair-cost bytes.
    let repair = node.repair_counters();
    if repair != *last_repair {
        *last_repair = repair;
        let (repaired, case1, case2, step_downs, bytes) = repair;
        audit.repair_progress(NodeId(self_id), repaired, case1, case2, step_downs, bytes);
        tracing::info!(
            node = self_id,
            repaired,
            case1,
            case2,
            step_downs,
            bytes,
            "repair_progress"
        );
    }
    let installed_now = report_handoff(node, last_handoff, self_id, audit);
    report_membership(node, last_membership, self_id, audit);
    // Surface any CheckQuorum step-down the batch's tick performed (#95).
    let quorum_lost = node.quorum_lost_step_downs();
    if quorum_lost > *last_quorum_lost {
        let count = quorum_lost - *last_quorum_lost;
        *last_quorum_lost = quorum_lost;
        audit.quorum_lost(NodeId(self_id), count);
        tracing::info!(node = self_id, count, "leader_quorum_lost");
    }
    let role = node.role();
    if role == NodeRole::Leader && *last_role != NodeRole::Leader && !installed_now {
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
        audit.elected(NodeId(self_id), ballot, promised, gaps, node.acceptors());
        tracing::info!(
            node = self_id,
            round = ballot.round,
            bnode = ballot.node.0,
            pround = promised.round,
            pbnode = promised.node.0,
            members = node.acceptors().members.len() as u64,
            "leader_elected"
        );
    } else if *last_role == NodeRole::Leader && role != NodeRole::Leader {
        let writes = waiters.pending.values().map(Vec::len).sum::<usize>();
        let reads = waiters.pending_reads.len();
        if writes + reads > 0 {
            audit.waiters_cleared(
                NodeId(self_id),
                u64::try_from(writes).unwrap_or(u64::MAX),
                u64::try_from(reads).unwrap_or(u64::MAX),
            );
            tracing::info!(node = self_id, writes, reads, "waiters_cleared");
        }
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
// One linear boot replay: report → walk → repair; splitting it would scatter
// the ordering contract between the three.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
fn replay_boot_state<S: NodeStorage, H: DriverHooks, A: Audit>(
    node: &mut RawNode,
    storage: &mut S,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError> {
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
    // Stage 8: surface the scan's recoverable classification *before* the
    // recovered-state report — the audit's explained-divergence rule keys on
    // it (a recovered log may omit a persisted record only after a detected
    // corruption crash or a reported-faulty event).
    let faulty: Vec<(Slot, Ballot)> = node
        .faulty_entries()
        .iter()
        .map(|(slot, ballot)| (*slot, *ballot))
        .collect();
    if !faulty.is_empty() {
        audit.faulty_reported(NodeId(self_id), &faulty);
        for (slot, ballot) in &faulty {
            tracing::info!(
                node = self_id,
                slot = slot.0,
                around = ballot.round,
                abnode = ballot.node.0,
                "faulty_reported"
            );
        }
    }
    // The recovered chosen index and the configured cluster size travel with
    // the boot report: the index anchors the cross-restart chosen-prefix
    // checks, the size lets a checker do quorum arithmetic without guessing
    // the topology from partial boot observations.
    let deployment = Deployment {
        bootstrap: AcceptorConfig::new(node.config().peers.clone(), node.config().quorum_system),
        pool: node.config().pool().to_vec(),
        matchmakers: node.config().matchmakers.clone(),
    };
    audit.recovered(
        NodeId(self_id),
        promised,
        node.hard_state().chosen_index,
        &deployment,
        &records,
    );
    let mut replayed_application = false;
    let mut replayed_snap_points: Vec<Slot> = Vec::new();
    let mut repair_from: Option<Slot> = None;
    if let Some(ci) = node.hard_state().chosen_index {
        let applied_slot = storage.applied_slot();
        let floor = node.first_slot();
        let resume = applied_slot.map_or(Slot(0), |a| Slot(a.0.saturating_add(1)));
        if resume < floor {
            // The application prefix stops below the compaction floor (the
            // snapshot state was lost): the log cannot replay the missing
            // range — only a peer's InstallSnapshot can. Open the repair and
            // apply nothing; consensus keeps serving every slot it can read.
            repair_from = Some(resume);
        } else {
            for s in floor.0..=ci.0 {
                let slot = Slot(s);
                let record = node.accepted().get(&slot);
                let Some((_b, stored)) = record else {
                    if applied_slot.is_some_and(|applied| slot <= applied) {
                        // The record rotted but its effect is already durable
                        // in the application state, which is the authority for
                        // its own prefix; the reported-faulty event explains
                        // the emission gap to the oracles.
                        continue;
                    }
                    // A chosen record this node cannot read, not yet applied:
                    // the replay stops here — contiguity is the contract — and
                    // the repair pump re-emits the healed range via catch-up.
                    repair_from = Some(slot);
                    break;
                };
                // A #94 duplicate slot replays exactly as the live walk applied
                // it: a no-op. The core re-derived `duplicate_slots` from the
                // sealed sessions + the retained log in `RawNode::new`, so the
                // substitution is deterministic across the restart.
                let noop = Command::Control(Control::Noop);
                let command = if node.duplicate_slots().contains(&slot) {
                    &noop
                } else {
                    stored
                };
                if applied_slot.is_none_or(|applied| slot > applied) {
                    storage
                        .apply(ci, slot, command)
                        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                    // A freshly replayed `Snap` marker re-captures its decided
                    // point (#101): the application state at this walk instant
                    // is the boundary state, exactly as in the live apply
                    // loop. An already-applied marker is skipped — its point
                    // flushed with the same batch as its apply.
                    if let Command::Control(Control::Snap { .. }) = command {
                        storage
                            .record_snapshot(slot)
                            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                        replayed_snap_points.push(slot);
                    }
                    replayed_application = true;
                }
                let vhash = command_hash(command);
                audit.applied(
                    NodeId(self_id),
                    slot,
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
    }
    if replayed_application {
        // Crash seam: the replayed prefix is staged but not yet flushed. The
        // next incarnation replays the same prefix from the same durable
        // state, so this is the idempotence of the boot replay itself under
        // test — the one seam a crash *between* batches can never reach,
        // because it sits before the first batch.
        if hooks.crash_at(Seam::AfterBootReplayBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterBootReplayBeforeSync);
            tracing::info!(
                node = self_id,
                seam = "after_boot_replay_before_sync",
                "crashed"
            );
            return Err(RunError::SeamCrash(Seam::AfterBootReplayBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }
    for at in &replayed_snap_points {
        audit.snap_recorded(NodeId(self_id), *at);
        tracing::info!(node = self_id, at = at.0, "snap_recorded");
    }
    if let Some(from) = repair_from {
        let below_floor = from < node.first_slot();
        node.open_app_repair(from);
        audit.app_repair_started(NodeId(self_id), from, below_floor);
        tracing::info!(
            node = self_id,
            from = from.0,
            below_floor,
            "app_repair_started"
        );
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
/// `members` is the full **node pool** (`NodeId` → address, *including* this
/// node): every node the core may ever address — the bootstrap membership on
/// plain Multi-Paxos, and every spare a reconfiguration could add on a
/// matchmaker deployment. The core addresses each outbound message by
/// `NodeId`, and the driver resolves it here. It must be consistent across
/// the cluster and agree with the `Config` the node read from `storage`.
///
/// `matchmakers` is the matchmaker set (`MatchmakerId` → address), empty on
/// plain Multi-Paxos; it must agree with the `Config`'s matchmaker set. The
/// driver speaks the matchmaker contract only when it is non-empty.
///
/// `tunables` is the driver's per-node transport shape ([`DriverTunables`]):
/// production passes [`DriverTunables::default()`] (the historical constants);
/// the sim harness buggifies it per seed, FDB knob style.
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
/// The exit is typed ([`RunError`]): [`RunError::SeamCrash`] when `hooks` fires
/// at a durability seam (the caller recovers by re-running `run_node` with
/// fresh storage); [`RunError::Storage`] when a [`NodeStorage`] call failed and
/// the driver took its fail-stop crash decision — production treats it as a
/// process exit (crash-only), the sim node loop recovers through the same
/// restart path as a seam crash; [`RunError::Infra`] for genuine
/// provider/infrastructure failures (bind, listen), the only exit that is not
/// a deliberate crash and must propagate.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len()))]
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state. The parameters are the node's complete wiring (providers,
// storage, addressing, tunables, lifecycle, hooks, audit) — a bundle would
// only rename the same eight things.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn run_node<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: NodeStorage,
    // Deliberately *not* `Send + 'static`, unlike the audit below. Every hook
    // is consulted from the node loop, never from a spawned task, and keeping
    // the bound this narrow is what *enforces* that: `hooks` arrives as a
    // borrow, so it cannot be captured by a `spawn_task` future, and a future
    // attempt to consult a hook from a detached task is a compile error rather
    // than a determinism bug found on CI months later. See [`PeerMailbox`].
    H: DriverHooks,
    // `Clone + Send + Sync + 'static` because each peer-delivery task carries
    // its own handle to the audit: the bounded-mailbox drops happen inside
    // those tasks, and reporting them (`Audit::dropped_at_mailbox`) is part of
    // the observation contract. Pure observation still holds — the clone
    // shares the same underlying sink.
    A: Audit + Clone + Send + Sync + 'static,
{
    // Stage 7: verify and classify every durable record BEFORE anything else —
    // in particular before `RawNode::new` reads the store — so no corrupted
    // bytes ever cross into protocol logic. A detected mismatch is the same
    // deliberate crash decision as any other storage fault: typed on the
    // audit, then `RunError::Storage` unwinds the incarnation. The scan itself
    // may only discard a crash-truncatable tail or repair a `HardState` copy
    // from its twin (see [`NodeStorage::boot_scan`]); it never truncates on a
    // corruption verdict.
    let boot_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .map_err(|e| storage_fault_crash(audit, boot_id, e))?;

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

    // The sans-IO core, bootstrapped from durable storage.
    let mut node = RawNode::new(&storage);
    let self_id = node.config().id.0;

    replay_boot_state(&mut node, &mut storage, self_id, hooks, audit)?;

    // Tonic handlers run as h2 request tasks and forward into these typed
    // queues. The loop remains the sole owner of RawNode. The edge's
    // integrity rejections are reported through the audit like every other
    // externally meaningful transition (observation only: the closure
    // returns nothing and the edge's answer does not depend on it).
    let on_reject: crate::grpc::OnReject = {
        let audit = audit.clone();
        Arc::new(move |kind| audit.edge_rejected(NodeId(self_id), kind))
    };
    let (rpc_service, mut rpc): (_, RpcInbox) = rpc_channel(
        tunables.client_inbox_capacity,
        tunables.peer_inbox_capacity,
        on_reject,
    );
    let grpc_service = tonic::service::Routes::new(ParosServer::new(rpc_service.clone()))
        .add_service(ParosInternalServer::new(rpc_service))
        .prepare();
    let grpc_server = H2Server::new(&providers).with_config(H2ServerConfig {
        keep_alive: Some(grpc_keep_alive(&tunables)),
        vectored_writes: true,
    });

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
            let channel =
                ReconnectingChannel::new(&providers, addr, grpc_channel_config(&tunables));
            peer_channels.push(channel.clone());
            let client = ParosInternalClient::with_origin(channel, origin);
            let regular = PeerMailbox::new(tunables.peer_queue_capacity);
            let snapshot = PeerMailbox::new(tunables.snapshot_queue_capacity);
            providers
                .task()
                .spawn_task(
                    "paros-grpc-peer-delivery",
                    run_peer_delivery(
                        client.clone(),
                        providers.time().clone(),
                        incarnation_shutdown.clone(),
                        regular.clone(),
                        tunables,
                        audit.clone(),
                        self_id,
                        id,
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
                        snapshot.clone(),
                        tunables,
                        audit.clone(),
                        self_id,
                        id,
                    ),
                )
                .detach();
            (id, PeerQueues { regular, snapshot })
        })
        .collect::<BTreeMap<_, _>>();

    // The matchmaker links (#120): one reconnecting channel per matchmaker,
    // and the inbox their answers come back through. Empty on plain
    // Multi-Paxos.
    let (match_reply_tx, mut match_replies) =
        mpsc::channel::<MatchReply>(tunables.peer_inbox_capacity);
    let matchmaker_clients = matchmakers
        .into_iter()
        .map(|(id, addr)| {
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            let channel =
                ReconnectingChannel::new(&providers, addr, grpc_channel_config(&tunables));
            peer_channels.push(channel.clone());
            Ok((id, ParosMatchmakerClient::with_origin(channel, origin)))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let links = MatchmakerLinks {
        clients: matchmaker_clients,
        replies: match_reply_tx,
        timeout: tunables.delivery_timeout,
        shutdown: incarnation_shutdown.clone(),
    };

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
    // The snapshot-point repair layer (#101): the boot scan's rotted-chunk
    // classification arms the chunk pull; everything else fills in per tick.
    let mut snap = SnapRepair::default();
    for (at, chunk) in storage.faulty_snap_chunks() {
        snap.pending.entry(at.0).or_default().insert(chunk);
    }
    for (&at, chunks) in &snap.pending {
        let count = u64::try_from(chunks.len()).unwrap_or(u64::MAX);
        audit.snap_chunks_reported(NodeId(self_id), Slot(at), count);
        tracing::info!(node = self_id, at, chunks = count, "snap_chunks_reported");
    }
    // Seed the first randomized election timeout (jitter from the driver's RNG).
    node.set_election_timeout(draw_election_timeout(
        &providers,
        hooks,
        audit,
        self_id,
        tunables.election_timeout_base,
    ));
    let mut last = Deltas {
        role: node.role(),
        duplicates: node.duplicates_suppressed(),
        quorum_lost: node.quorum_lost_step_downs(),
        repair: node.repair_counters(),
        handoff: node.handoff_counters(),
        membership: node.membership_counters(),
        matchmaking: None,
    };
    // Ticks since the open matchmaking request was last (re-)sent.
    let mut match_resend_elapsed: u64 = 0;

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
    let mut next_tick = time.now() + tunables.tick_interval;

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
                        // A lost redirect is a legal outcome: the client's
                        // deadline turns it into a retry elsewhere.
                        if hooks.drop_client_reply(Reply::ProposeRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ProposeRedirect);
                            tracing::info!(node = self_id, reply = "propose_redirect", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ProposeAck { seq, leader: hint.map(|n| n.0), committed: false, slot: None });
                        }
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
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
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
                        if hooks.drop_client_reply(Reply::ReadRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ReadRedirect);
                            tracing::info!(node = self_id, reply = "read_redirect", "client_reply_dropped");
                        } else {
                            let _ = reply.send(ReadAck { seq, leader: hint.map(|n| n.0), committed: false, read_index: None });
                        }
                    }
                    ReadIndexResult::Pending => {
                        waiters.pending_reads.insert(next_read_ctx, (seq, ticks, reply));
                        next_read_ctx += 1;
                    }
                }
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
            }
            Some((msg, reply)) = rpc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO). Surface the
                // arrival (mirror of `msg_sent`) so the demo can pair sends with
                // receives and mark the unmatched ones as network drops.
                let kind = message_kind(&msg);
                match message_route(&msg) {
                    Some((from, config_id, ballot, Some(slot))) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        slot = slot.0,
                        config_id = config_id.0,
                        "msg_received"
                    ),
                    // The empty-prefix beat: no slot field, mirroring `msg_sent`.
                    Some((from, config_id, ballot, None)) => tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        config_id = config_id.0,
                        "msg_received"
                    ),
                    None => {
                        if let Some(config_id) = msg.config_id() {
                            tracing::info!(
                                node = self_id,
                                kind,
                                config_id = config_id.0,
                                "msg_received"
                            );
                        } else {
                            tracing::info!(node = self_id, kind, "msg_received");
                        }
                    }
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
                // Snapshot-repair traffic is driver-terminal (#101): handled
                // here, never stepped into the core — consensus state must
                // not depend on snapshot custody.
                // A snap-repair message naming a foreign configuration is
                // ignored (guarded, never asserted — wire input): custody and
                // chunk bytes are only meaningful within one configuration.
                let snap_handled = match &msg {
                    Message::SnapAck {
                        config_id,
                        from,
                        at_index,
                    } => {
                        // Custody counts toward the coupling quorum only from
                        // members of the active configuration.
                        if *config_id == node.hard_state().config_id
                            && node.is_leader()
                            && node.acceptors().contains(*from)
                        {
                            snap.acks.entry(at_index.0).or_default().insert(from.0);
                        }
                        true
                    }
                    Message::SnapChunkRequest {
                        config_id,
                        from,
                        at_index,
                        chunks,
                    } => {
                        if *config_id == node.hard_state().config_id {
                            handle_snap_chunk_request(
                                &node, &storage, &out, hooks, audit, *from, *at_index, chunks,
                            );
                        }
                        true
                    }
                    Message::SnapChunkResponse {
                        config_id,
                        from,
                        at_index,
                        chunks,
                    } => {
                        // Pool-checked: only a pooled node's chunk bytes are
                        // installed (a replica outside the active
                        // configuration holds the same decided point).
                        if *config_id == node.hard_state().config_id
                            && node.config().pool().contains(from)
                        {
                            let at = *at_index;
                            let chunks = chunks.clone();
                            handle_snap_chunk_response(
                                &mut node,
                                &mut storage,
                                &mut snap,
                                hooks,
                                audit,
                                self_id,
                                at,
                                &chunks,
                            )?;
                        }
                        true
                    }
                    _ => false,
                };
                if !snap_handled {
                    node.step(msg);
                }
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
                let _ = reply.send(());
            }
            Some(reply) = match_replies.recv() => {
                // A matchmaker's answer to this candidate's registration (#120):
                // fold it into the open matchmaking phase; a quorum closes the
                // phase and opens Phase 1 in the same step.
                let (matchmaker, ballot) = (reply.matchmaker, reply.ballot);
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    round = ballot.round,
                    registered = matches!(reply.outcome, paros_core::MatchOutcome::Registered { .. }),
                    "match_reply_received"
                );
                let step = node.on_match_reply(reply);
                report_match_step(&node, audit, self_id, matchmaker, ballot, &step);
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
            }
            Some((req, reply)) = rpc.reconfigure.recv() => {
                // An online reconfiguration (#122): the leader moves to a fresh
                // ballot registered with the new acceptor set. Refusable like
                // `Compact` — a non-leader redirects, a plain deployment
                // refuses outright, and an unsettled leadership asks the
                // client to retry.
                let members: Vec<NodeId> = req.members.iter().copied().map(NodeId).collect();
                let result = if members.is_empty() {
                    ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember)
                } else {
                    node.reconfigure(&AcceptorConfig::new(members.clone(), QuorumSystem::Majority))
                };
                audit.reconfigure_acked(NodeId(self_id), &members, result);
                let (accepted, refusal, round) = match result {
                    ReconfigureResult::Started(ballot) => (true, "", Some(ballot.round)),
                    ReconfigureResult::NotLeader(_) => (false, "not_leader", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers) => (false, "no_matchmakers", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::Unchanged) => (false, "unchanged", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember) => (false, "unknown_member", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::Unsettled) => (false, "unsettled", None),
                    ReconfigureResult::Refused(ReconfigureRefusal::RoundExhausted) => (false, "round_exhausted", None),
                };
                let leader = match result {
                    ReconfigureResult::NotLeader(hint) => hint.map(|n| n.0),
                    _ => Some(self_id),
                };
                tracing::info!(
                    node = self_id,
                    members = members.len() as u64,
                    accepted,
                    refusal,
                    round = round.unwrap_or(0),
                    "reconfigure_acked"
                );
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
                // A lost reconfiguration ack is ambiguous to the client, which
                // re-asks; a started reconfiguration stands (a retry is refused
                // as `not_leader` while it runs, then `unchanged` once done).
                if hooks.drop_client_reply(Reply::Reconfigure) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Reconfigure);
                    tracing::info!(node = self_id, reply = "reconfigure", "client_reply_dropped");
                } else {
                    let _ = reply.send(ReconfigureAck { leader, accepted, refusal: refusal.to_string(), round });
                }
            }
            Some((req, reply)) = rpc.compact.recv() => {
                // The application permits dropping the log prefix up to `up_to`.
                // Only the leader admits it: it proposes a `Truncate` control
                // command into the next slot, decided by ordinary Paxos and
                // forwarded to every node, each of which truncates lazily when it
                // applies that slot. A non-leader redirects (like `propose`).
                //
                // The coupling rule (#101, CTRL §3.5): a `Truncate{up_to}` is
                // proposed only once a quorum holds the decided snapshot at
                // (or past) `up_to` — that is what makes chunk repair sound
                // once the log below the floor is gone. A request no decided
                // point covers first seeds a `Snap` marker and answers
                // `accepted: false`; the client's retry finds the point once
                // the quorum's custody advertisements land. Proposal-side
                // policy only — the acceptor paths stay fully opaque.
                let ack = if node.is_leader() {
                    let quorum = node.acceptors().quorum_size();
                    let covered = snap
                        .acks
                        .iter()
                        .filter(|(_, holders)| holders.len() >= quorum)
                        .map(|(&point, _)| point)
                        .max();
                    let propose_marker = |node: &mut RawNode, snap: &mut SnapRepair| {
                        if snap.marker_pending.is_none()
                            && let ProposeResult::Accepted(slot) = node.propose_snap_marker()
                        {
                            snap.marker_pending = Some(slot);
                            tracing::info!(node = self_id, at = slot.0, "snap_marker_proposed");
                        }
                    };
                    if let Some(point) = covered {
                        let up_to = Slot(req.up_to.min(point));
                        // Honest ack: `accepted: true` only when the Truncate
                        // proposal was actually admitted. `propose_control`
                        // can refuse (a step-down raced this request), and the
                        // client's retry handles `accepted: false` exactly
                        // like the coupling refusal below.
                        let proposed = matches!(
                            node.propose_control(Control::Truncate { up_to }),
                            ProposeResult::Accepted(_)
                        );
                        tracing::info!(
                            node = self_id,
                            requested = req.up_to,
                            up_to = up_to.0,
                            point,
                            accepted = proposed,
                            "truncate_coupled_to_snap_point"
                        );
                        if req.up_to > point {
                            // The request outruns the covered prefix: seed the
                            // next point so a later compact can go further.
                            propose_marker(&mut node, &mut snap);
                        }
                        CompactAck {
                            leader: Some(self_id),
                            accepted: proposed,
                            first_slot: node.first_slot().0,
                        }
                    } else {
                        propose_marker(&mut node, &mut snap);
                        CompactAck {
                            leader: Some(self_id),
                            accepted: false,
                            first_slot: node.first_slot().0,
                        }
                    }
                } else {
                    CompactAck {
                        leader: node.leader().map(|n| n.0),
                        accepted: false,
                        first_slot: node.first_slot().0,
                    }
                };
                audit.compact_acked(NodeId(self_id), ack.accepted);
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
                // A lost compaction ack is ambiguous to the client, which
                // re-asks; the marker/Truncate it may have seeded stands.
                if hooks.drop_client_reply(Reply::Compact) {
                    audit.client_reply_dropped(NodeId(self_id), Reply::Compact);
                    tracing::info!(node = self_id, reply = "compact", "client_reply_dropped");
                } else {
                    let _ = reply.send(ack);
                }
            }
            Some((_req, reply)) = rpc.inspect.recv() => {
                let since = node.acceptors_since();
                let _ = reply.send(InspectReply {
                    chosen_index: node.hard_state().chosen_index.map(|slot| slot.0),
                    first_slot: node.first_slot().0,
                    snapshot: storage.snapshot(),
                    members: node.acceptors().members.iter().map(|n| n.0).collect(),
                    config_ballot: Some(internal::Ballot { round: since.round, node: since.node.0 }),
                    leader: node.is_leader(),
                });
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                // Pacing, not a protocol bound: every timeout the core owns is
                // counted in ticks, so a node that waits twice as long between
                // ticks is exactly a slow node. Stretching desynchronizes the
                // cluster's protocol clocks — a shape moonpool's clock skew
                // reaches only for the wall clock, never for the tick counter
                // the election and read-round timers actually run on.
                next_tick = time.now()
                    + if hooks.stretch_tick_interval() { tunables.tick_interval * 2 } else { tunables.tick_interval };
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
                // The open matchmaking request's re-send (#120): paced by
                // `match_resend_ticks`, and its own BUGGIFY location — consulted
                // only when a re-send is due, so a skip always costs a beat.
                if node.matchmaking_pending() {
                    match_resend_elapsed += 1;
                    if match_resend_elapsed >= tunables.match_resend_ticks.max(1) {
                        match_resend_elapsed = 0;
                        if hooks.skip_matchmaking_resend() {
                            audit.matchmaking_resend_skipped(NodeId(self_id));
                            tracing::info!(node = self_id, "matchmaking_resend_skipped");
                        } else {
                            node.resend_matchmaking();
                        }
                    }
                } else {
                    match_resend_elapsed = 0;
                }
                // Cooperative leader handoff (`DPaxos`): move the existing
                // Phase-2 authority to another physical node instead of letting
                // an election destroy it and make the successor rediscover the
                // log through Phase 1. Consulted only when the core says the
                // leadership is transferable, so a `true` always has an effect;
                // answering `false` is always safe (a handoff is an
                // optimization, never a requirement). Offered *before* the
                // resignation hook: both give up the leadership, and the
                // cooperative one is strictly the more interesting outcome.
                let mut handed_off = false;
                if node.can_relinquish() {
                    let candidates = node.handoff_candidates();
                    if !candidates.is_empty() {
                        let ctx = handoff_context(&node, candidates.len());
                        if hooks.initiate_handoff(ctx) {
                            let fallback = providers.random().random_range(0..candidates.len());
                            let target = hooks
                                .handoff_target(&candidates)
                                .filter(|t| candidates.contains(t))
                                .unwrap_or(candidates[fallback]);
                            if let Some(handoff) = node.relinquish_to(target) {
                                handed_off = true;
                                audit.authority_relinquished(NodeId(self_id), handoff);
                                tracing::info!(
                                    node = self_id,
                                    to = handoff.to.0,
                                    round = handoff.ballot.round,
                                    bnode = handoff.ballot.node.0,
                                    next_slot = handoff.next_slot.0,
                                    decided = handoff.decided,
                                    pending = handoff.pending,
                                    "authority_relinquished"
                                );
                            }
                        }
                    }
                }
                if !handed_off && node.role() == NodeRole::Leader && hooks.resign_leadership() {
                    audit.stepped_down(NodeId(self_id));
                    tracing::info!(node = self_id, "leadership_resigned");
                    node.step_down();
                }
                // Snapshot-point repair upkeep (#101): custody advertisement,
                // the leader's coupling tally, and the chunk-repair pull.
                snap_repair_tick(&node, &storage, &out, hooks, audit, &mut snap);
                ticks += 1;
                // Expire parked reads whose confirmation is overdue (lost acks, a
                // minority-partitioned leader that never steps down): answer a
                // retry redirect while the client still has deadline left. A late
                // core confirmation finds the ctx gone and is ignored. The
                // early-expiry hook (consulted only while reads are parked)
                // takes the same exit before the deadline.
                let expire_all = !waiters.pending_reads.is_empty() && hooks.expire_parked_read_early();
                // `(ctx, early)`: `early` marks a read the hook expired while
                // its deadline still had ticks left — the audit keeps the two
                // exits apart.
                let overdue: Vec<(u64, bool)> = waiters.pending_reads
                    .iter()
                    .filter_map(|(ctx, (_, parked_at, _))| {
                        let by_deadline = ticks.saturating_sub(*parked_at) > tunables.read_retry_ticks;
                        (expire_all || by_deadline).then_some((*ctx, !by_deadline))
                    })
                    .collect();
                for (ctx, early) in overdue {
                    if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&ctx) {
                        audit.read_expired(NodeId(self_id), early);
                        if hooks.drop_client_reply(Reply::ReadRedirect) {
                            audit.client_reply_dropped(NodeId(self_id), Reply::ReadRedirect);
                            tracing::info!(node = self_id, reply = "read_redirect", "client_reply_dropped");
                        } else {
                            let _ = waiter.send(ReadAck { seq, leader: Some(self_id), committed: false, read_index: None });
                        }
                    }
                }
                let requests = drain_ready(&mut node, &mut storage, &out, &mut waiters, hooks, audit)?;
                surface_matchmaking(&node, &mut last.matchmaking, audit, self_id);
                send_match_requests(&providers, &links, audit, self_id, requests);
                maintain(&mut node, &providers, &mut last, &mut waiters, self_id, tunables.election_timeout_base, hooks, audit);
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
