//! Driver configuration and the wiring both drivers share: the per-node
//! tunables, the transport constants they default to, the gRPC keep-alive /
//! channel shapes, the accepted-connection server, the scope guard, the
//! address parser, and the driver's typed exit ([`RunError`]).

use std::fmt::Display;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Detach, Providers, SimulationError, SimulationResult, TaskProvider};
use moonpool_hyper::{ChannelConfig, KeepAlive};

use crate::hooks::Seam;
use crate::storage::StorageError;

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
pub(crate) const GRPC_DELIVERY_BATCH_BYTES: usize = 3 * 1024 * 1024;
/// Maximum Paxos messages packed into one protobuf/gRPC request. This keeps
/// a chatty heartbeat/catch-up round from creating one h2 frame per message.
pub(crate) const GRPC_DELIVERY_BATCH: usize = 64;
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
    /// (`ColocatedNode::resend_matchmaking`), on a deployment with matchmakers.
    /// Floor 1: a re-send per tick is a request per tick per matchmaker, which
    /// the registry answers idempotently. The default is one election-timeout
    /// base, so a lost reply costs about one round trip before the retry.
    pub match_resend_ticks: u64,
    /// Ticks between re-sends of an open GC request (`ColocatedNode::resend_gc`),
    /// on a deployment with matchmakers. Its own cadence, not matchmaking's:
    /// the two pace unrelated round trips and a seed should be able to be
    /// extreme in one and ordinary in the other. Floor 1 (a request per tick,
    /// answered idempotently); the ceiling is unbounded and still safe — a
    /// watermark that is never raised costs the matchmakers their retained
    /// histories, never safety.
    pub gc_resend_ticks: u64,
    /// Ticks between re-sends of the running matchmaker-set handover's step
    /// (`HandoverDriver::resend_due`). Floor 1; bounded above by the stall
    /// budget below — a cadence longer than
    /// `election_timeout * reconfigure_timeout_elections` would let the phase
    /// be abandoned before it is ever re-sent, which is not a slower retry
    /// but no retry at all.
    pub reconfigurer_resend_ticks: u64,
    /// How many election timeouts a matchmaker-set handover may make no
    /// progress before the driver abandons it
    /// (`MatchmakerReconfigurer::abandon`). Driver policy, never a constant
    /// inside the state machine: the core only reports the stall
    /// (`stalled_for`). Floor 1 election timeout — long enough for a slow
    /// matchmaker to answer one re-sent request; below that a healthy
    /// handover could not finish, which is not an extreme configuration but
    /// a broken one.
    pub reconfigure_timeout_elections: u64,
    /// Upper bound on the jittered backoff a preempted successor decree waits
    /// before it reopens at a higher ballot — the symmetry break between
    /// dueling reconfigurers, drawn from `1..=reconfigure_backoff_max_ticks`.
    /// Floor 1 (draw exactly one tick: no jitter, so two reconfigurers may
    /// duel for a while — liveness, and the stall budget ends it). Its own
    /// knob rather than a multiple of `election_timeout_base`, so a seed can
    /// push the election clock and the decree's symmetry break independently.
    pub reconfigure_backoff_max_ticks: u64,
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
            gc_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigurer_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigure_timeout_elections: RECONFIGURE_TIMEOUT_ELECTIONS,
            reconfigure_backoff_max_ticks: ELECTION_TIMEOUT_BASE * 2,
        }
    }
}

/// Run one synchronous cleanup action on every exit path from its scope.
pub(crate) struct OnDrop<F: FnOnce()> {
    action: Option<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    pub(crate) fn new(action: F) -> Self {
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

pub(crate) fn grpc_keep_alive(tunables: &DriverTunables) -> KeepAlive {
    KeepAlive {
        interval: tunables.keep_alive_interval,
        timeout: tunables.keep_alive_timeout,
        while_idle: false,
    }
}

pub(crate) fn grpc_channel_config(tunables: &DriverTunables) -> ChannelConfig {
    ChannelConfig {
        connection_timeout: tunables.connection_timeout,
        keep_alive: Some(grpc_keep_alive(tunables)),
        ..ChannelConfig::default()
    }
}

/// Serve one accepted gRPC connection on its own detached task, ending when
/// the incarnation does. Shared by both drivers in this crate — the node loop
/// and the matchmaker loop differ only in the task's name and the `role` their
/// connection errors carry.
pub(crate) fn accept_and_serve<P, F, E>(
    providers: &P,
    task: &'static str,
    role: &'static str,
    addr: impl Display + Send + 'static,
    connection: F,
) where
    P: Providers,
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Display + Send + 'static,
{
    providers
        .task()
        .spawn_task(task, async move {
            if let Err(error) = connection.await {
                tracing::warn!(%addr, %error, role, "gRPC connection ended");
            }
        })
        .detach();
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

/// Default stall budget for a matchmaker-set handover, in election timeouts:
/// long enough for a slow matchmaker to answer a re-sent request, short enough
/// that a dead one does not hold the `Busy` refusal for the rest of a run.
/// Driver policy, not a protocol bound — the core reports the stall
/// (`MatchmakerReconfigurer::stalled_for`), the driver decides — and a
/// [`DriverTunables`] field rather than a constant the harness cannot move.
const RECONFIGURE_TIMEOUT_ELECTIONS: u64 = 4;

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

/// Why a driver loop stopped, typed — the shared exit of every provider-generic
/// driver in this crate ([`crate::run_node`] and [`crate::run_matchmaker`]).
/// The driver's *domain* outcomes — a crash it
/// decided to take — are first-class variants a caller matches on; a moonpool
/// [`SimulationError`] appears only wrapped in [`RunError::Infra`], for genuine
/// provider/infrastructure failures. The simulation's error type never carries
/// a protocol-layer decision.
#[derive(Debug)]
pub enum RunError {
    /// A hook-injected crash at a durability [`Seam`] inside a `Ready` batch
    /// (simulation only: production's `NoHooks` never fires). The caller
    /// recovers by re-running the driver loop, which rebuilds volatile state
    /// from durable storage.
    SeamCrash(Seam),
    /// A [`crate::NodeStorage`] (or [`crate::MatchmakerStorage`]) call failed
    /// and the driver took its fail-stop crash
    /// decision — never an incidental error propagation. In **production**
    /// this is a crash-only process exit; recovery is the next boot. In
    /// simulation the loop recovers exactly like a seam crash: re-run the
    /// driver against whatever the disk *actually* holds (the recovery
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
