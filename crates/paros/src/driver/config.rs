//! Driver configuration and the wiring both drivers share: the per-node
//! tunables, the transport constants they default to, the address parser, and
//! the driver's typed exit ([`RunError`]).

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{SimulationError, SimulationResult};

use crate::hooks::Seam;
use crate::storage::StorageError;

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

/// RPC liveness pings on peer connections. Both values use provider time, so a
/// half-open connection is failed deterministically during the settle tail.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(2);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(1);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(1);
/// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this is
/// deliberately bounded and lossy: the consensus driver never waits for
/// network I/O, and current heartbeats/resends repair anything dropped here.
/// Overflow evicts the *oldest* undelivered message (see [`PeerMailbox`]).
const PEER_QUEUE_CAPACITY: usize = 4096;
/// Leave headroom below the RPC runtime's 4 MiB frame limit
/// (`rpc::inbound`'s `MAX_FRAME_BYTES`) for the protobuf and RPC envelopes.
/// An earlier transport capped a complete payload at 1 MiB; this preserves
/// that per-message envelope while allowing compact batches.
pub(crate) const DELIVERY_BATCH_BYTES: usize = 3 * 1024 * 1024;
/// Maximum Paxos messages packed into one `Deliver` request. This keeps a
/// chatty heartbeat/catch-up round from creating one RPC frame per message.
pub(crate) const DELIVERY_BATCH: usize = 64;
/// Bounded inboxes between the RPC edge and the node loop: overload is
/// visible (a refused call, or backpressure on the peer lane), with ample
/// room for one tick's peer fanout.
const CLIENT_INBOX_CAPACITY: usize = 256;
const PEER_INBOX_CAPACITY: usize = 1024;

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
    /// Liveness ping interval on the RPC runtime's connections
    /// (`PeerPolicy::ping_interval`, provider time, so a half-open
    /// connection is failed deterministically). Floor: non-zero.
    pub keep_alive_interval: Duration,
    /// How long a connection may stay silent after a ping before it is
    /// failed (`PeerPolicy::ping_timeout`). Floor: non-zero.
    pub keep_alive_timeout: Duration,
    /// How long a connect (and its session handshake) may take before the
    /// attempt fails (`RpcConfig::connect_timeout`); the runtime re-dials with
    /// backoff for as long as calls need the peer. Floor: non-zero.
    pub connection_timeout: Duration,
    /// How long one peer-delivery RPC may take to get its batch *into the
    /// peer's inbox* before the batch is written off as lost (the mailbox is
    /// lossy by contract; resends repair it). The peer acks on enqueue, not
    /// after processing, so this races the connection and the peer's
    /// `peer_inbox_capacity`, never its loop. Floor: non-zero.
    pub delivery_timeout: Duration,
    /// Ticks a parked read may wait for its read-index confirmation before
    /// the driver answers a retry redirect. Floor: the confirmation is one
    /// heartbeat-ack round trip, so `read_retry_ticks × tick_interval` must
    /// exceed it or no read ever confirms. A client whose deadline is shorter
    /// than the wait simply times out (ambiguous, never wrong).
    pub read_retry_ticks: u64,
    /// Ticks a journal `Read` at or past the serving node's end may wait
    /// (the long-poll, #185) for something to be chosen before the driver
    /// answers an empty page. Floor 0: a zero wait answers every such read
    /// at once, empty, and the client simply re-asks — a busier client,
    /// never a wrong one. A client whose deadline is shorter than the wait
    /// times out (ambiguous, never wrong).
    pub read_poll_ticks: u64,
    /// Ticks a journal quarantined by a storage fault (#188) stays down on
    /// this node before the driver re-opens it from its store — the
    /// per-journal twin of a crashed process's restart delay. Floor 1: a
    /// re-open the same tick is a restart loop with no room for the rest of
    /// the node; a long quarantine is a node that is slow to heal one
    /// journal, never a wrong one.
    pub quarantine_ticks: u64,
    /// How many times a node's election timeout base may double across
    /// consecutive failed rounds (an election clock reset with no leader
    /// known: a failed campaign, or a leadership a rival deposed): the
    /// `k`-th consecutive one draws from `[T·2^j, 2·T·2^j)` with
    /// `j = min(k - 1, election_backoff_doublings)`, and hearing another
    /// node lead resets it (leading does not). A fixed timeout below a Phase-1 round trip
    /// livelocks for good: a sole candidate whose slowest promise always
    /// lands one round late abandons every round it opens (witness
    /// 2881076808784637484: `q1 = n` over a degraded link, 180 rounds in
    /// 63 s and never a leader). Floor 2: `T × 4` outruns any round trip the
    /// base's own floor is sized against; a larger ceiling is a slower
    /// recovery after a leader dies behind a partition, never a wrong one.
    pub election_backoff_doublings: u32,
    /// Capacity of each client-facing endpoint queue (propose, read, compact,
    /// inspect, …) between the RPC runtime and the node loop
    /// (`RpcConfig::endpoint_queue_capacity`). Floor 1: overload is visible as
    /// a refused call (`Overloaded`, never admitted), never as a lost request.
    pub client_inbox_capacity: usize,
    /// Capacity of the peer-message inbox the `Deliver` lane feeds. Floor 1:
    /// a full inbox backpressures the lane, never loses a message.
    pub peer_inbox_capacity: usize,
    /// Per-peer in-memory handoff capacity. Like etcd's stream mailbox, this
    /// is deliberately bounded and lossy: the consensus driver never waits for
    /// network I/O, and current heartbeats/resends repair anything dropped
    /// here (overflow evicts the oldest message, keep-newest). The extreme (a
    /// handful of slots) makes mailbox overflow —
    /// [`Audit::dropped_at_mailbox`](crate::Audit::dropped_at_mailbox)
    /// — a likely event instead of a rare one.
    pub peer_queue_capacity: usize,
    /// Maximum Paxos messages packed into one `Deliver` request. A small
    /// batch raises RPC framing pressure and the batcher's keep-the-newest
    /// overflow shedding; its floor is a throughput floor — a link carries
    /// every journal its two ends serve, so a batch too small for their
    /// combined rate is a permanent partition (the harness keeps it at 24
    /// or more, `paros_sim::shape`).
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
    /// How many times a delegated round may be re-delegated (once per beat,
    /// by `resend_pending`) without the proxy's `Commit` arriving before the
    /// leader **takes it back** and runs it colocated
    /// (`ColocatedNode::take_back_delegated`, #142). Driver policy, never a
    /// constant of the state machine: liveness under a dead proxy is the
    /// leader's, and this is its whole budget. Floor 1 — taking a round back
    /// after a single re-delegation is always safe (two fan-outs of one
    /// `(slot, ballot, command)` are P2b-idempotent), it merely runs more of
    /// the log colocated; the ceiling is unbounded and still winnable, a
    /// round a dead proxy holds forever being recovered by the next
    /// leadership's Phase 1. Meaningless on a deployment without proxies.
    pub proxy_take_back_resends: u64,
    /// How many times a proxy leader may re-fan-out an open round (once per
    /// beat, by `ProxyLeader::resend_pending`) without an answer before it
    /// **evicts** it (`ProxyLeader::expire_stale`, #142) — the proxy's
    /// bounded retention. A round nobody answers is a real state, not a
    /// slow one: an `Accept` for a slot every acceptor compacted past is
    /// ignored without `Accepted` or `Nack`, so a delegation delayed until
    /// after the leader took the round back and the cluster truncated the
    /// slot would otherwise be re-fanned-out for the rest of the process's
    /// life. Eviction is never a decision — the leader's re-delegation
    /// reopens a round it still needs and its take-back
    /// (`proxy_take_back_resends`) stays the liveness — so the floor is 1
    /// and the ceiling is unbounded and still winnable. The default is twice
    /// the take-back budget, so in the default configuration the leader has
    /// taken a stalled round back before its proxy evicts it and the
    /// eviction reclaims only rounds the leader is done with. Meaningless
    /// on a proxy-less deployment, and read only by `run_proxy`.
    pub proxy_round_resends: u64,
}

impl Default for DriverTunables {
    fn default() -> Self {
        Self {
            tick_interval: TICK_INTERVAL,
            election_timeout_base: ELECTION_TIMEOUT_BASE,
            keep_alive_interval: KEEP_ALIVE_INTERVAL,
            keep_alive_timeout: KEEP_ALIVE_TIMEOUT,
            connection_timeout: DELIVERY_TIMEOUT,
            delivery_timeout: DELIVERY_TIMEOUT,
            read_retry_ticks: READ_RETRY_TICKS,
            read_poll_ticks: READ_POLL_TICKS,
            quarantine_ticks: QUARANTINE_TICKS,
            election_backoff_doublings: ELECTION_BACKOFF_DOUBLINGS,
            client_inbox_capacity: CLIENT_INBOX_CAPACITY,
            peer_inbox_capacity: PEER_INBOX_CAPACITY,
            peer_queue_capacity: PEER_QUEUE_CAPACITY,
            delivery_batch: DELIVERY_BATCH,
            match_resend_ticks: ELECTION_TIMEOUT_BASE,
            gc_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigurer_resend_ticks: ELECTION_TIMEOUT_BASE,
            reconfigure_timeout_elections: RECONFIGURE_TIMEOUT_ELECTIONS,
            reconfigure_backoff_max_ticks: ELECTION_TIMEOUT_BASE * 2,
            proxy_take_back_resends: PROXY_TAKE_BACK_RESENDS,
            proxy_round_resends: PROXY_ROUND_RESENDS,
        }
    }
}

/// Ticks a parked read reply may wait for its read-index confirmation before
/// the driver answers a retry redirect (500 ms — well inside the sim client's
/// 1000 ms deadline, and inside the core's own round TTL, so a late core
/// confirmation just finds the ctx gone and is ignored).
const READ_RETRY_TICKS: u64 = 10;

/// Ticks a journal `Read` above the end long-polls before an empty answer
/// (#185): 400 ms at the default tick, inside the sim client's 1000 ms
/// deadline.
const READ_POLL_TICKS: u64 = 8;
/// Default [`DriverTunables::quarantine_ticks`]: eight election timeouts —
/// long enough that a journal's re-open is not a restart loop against a
/// still-faulty device, short enough that the node rejoins the journal well
/// inside a recovery tail.
const QUARANTINE_TICKS: u64 = 8 * ELECTION_TIMEOUT_BASE;

/// Base election timeout, in ticks. Each node's actual timeout is drawn
/// uniformly from `[T, 2T)` (jitter from the [`RandomProvider`], in the driver,
/// never the zero-dep core) to break the dueling-proposer livelock. `T`
/// dominates the core's heartbeat interval, so a live leader always beats before
/// a follower's election clock fires.
const ELECTION_TIMEOUT_BASE: u64 = 5;

/// Default [`DriverTunables::election_backoff_doublings`]: up to `8 × T`
/// (two to four seconds at the default tick) after three failed campaigns.
const ELECTION_BACKOFF_DOUBLINGS: u32 = 3;

/// Default take-back budget for a delegated round (#142), in re-delegations
/// — one per beat, so two election-timeout bases of ticks: long enough for a
/// proxy's fan-out, fold and `Commit` to complete over a slow link, short
/// enough that a dead proxy costs a slot a fraction of a second rather than
/// an election. Driver policy (the core only counts), and a
/// [`DriverTunables`] field so the harness can push it to its floor.
const PROXY_TAKE_BACK_RESENDS: u64 = ELECTION_TIMEOUT_BASE * 2;

/// Default retention budget of a proxy leader's open round (#142), in
/// unanswered re-fan-outs — one per beat: twice the take-back budget, so the
/// leader reclaims a stalled round first and the proxy's eviction is the
/// backstop for rounds nobody will ever answer (a compacted slot, a round
/// the leader already decided colocated). Driver policy (the core only
/// counts), and a [`DriverTunables`] field so the harness can push it to
/// its floor.
const PROXY_ROUND_RESENDS: u64 = PROXY_TAKE_BACK_RESENDS * 2;

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

/// What the operator claims about the store [`crate::run_node`] is handed
/// (#147): configuration data, never inferred from the store's contents.
///
/// The claim is judged against the store's **format marker**
/// ([`crate::LogStorage::is_formatted`]): a store that has ever belonged to
/// a member carries one, written by the driver on the identity's first boot
/// and never removed. An existing member whose store carries no marker has
/// lost its disk — *amnesia*, not a clean crash — and its durable promise
/// with it; booting it would answer a Phase 1 with "nothing accepted" for
/// slots it once voted on, so the driver refuses
/// ([`BootRefusal::Amnesia`]). A first boot on a store that already carries
/// a marker is two identities on one disk, refused as well
/// ([`BootRefusal::AlreadyFormatted`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootKind {
    /// This identity has never been provisioned: the store is empty and the
    /// driver formats it (writes the marker, durably) before the core reads
    /// anything.
    FirstBoot,
    /// This identity was provisioned before: the store must carry the marker.
    ExistingMember,
}

/// Why [`crate::run_node`] refused to boot (#147): the operator's
/// [`BootKind`] claim and the store's format marker disagree. An operating
/// error — a result value, never an assert — because the claim is external
/// input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootRefusal {
    /// An existing member's store carries no format marker: the disk was
    /// lost, and the durable promise with it. The identity never rejoins;
    /// the cluster replaces it by reconfiguration.
    Amnesia,
    /// A first boot found a store already formatted: another identity's
    /// disk, or a provisioning mistake. Nothing is written.
    AlreadyFormatted,
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
    /// A [`crate::LogStorage`] (or [`crate::MatchmakerStorage`]) call failed
    /// and the driver took its fail-stop crash
    /// decision — never an incidental error propagation. In **production**
    /// this is a crash-only process exit; recovery is the next boot. In
    /// simulation the loop recovers exactly like a seam crash: re-run the
    /// driver against whatever the disk *actually* holds (the recovery
    /// path must be correct for both outcomes of an ambiguous write; see
    /// [`crate::WriteOutcome`]).
    Storage(StorageError),
    /// The driver refused to boot this identity on this store (#147): the
    /// operator's [`BootKind`] claim and the store's format marker disagree.
    /// Nothing was written and no message left; the identity stays down
    /// until the operator resolves the claim (an amnesiac member is replaced
    /// by reconfiguration, never rebooted).
    Refused(BootRefusal),
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
            RunError::Refused(BootRefusal::Amnesia) => write!(
                f,
                "boot refused: an existing member's store carries no format marker (amnesia)"
            ),
            RunError::Refused(BootRefusal::AlreadyFormatted) => {
                write!(
                    f,
                    "boot refused: a first boot on a store that is already formatted"
                )
            }
            RunError::Infra(e) => write!(f, "infrastructure failure: {e}"),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RunError::Storage(e) => Some(e),
            RunError::Infra(e) => Some(e),
            RunError::SeamCrash(_) | RunError::Refused(_) => None,
        }
    }
}

impl From<SimulationError> for RunError {
    fn from(e: SimulationError) -> Self {
        RunError::Infra(e)
    }
}
