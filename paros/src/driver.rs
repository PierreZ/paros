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
use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{
    NetworkAddress, Providers, RandomProvider, SimulationError, SimulationResult, TimeProvider,
};
use moonpool_transport::{NetTransport, NetTransportBuilder, ReplyPromise, RpcError, service};
use paros_core::{
    Ballot, ClientId, ClientSeq, Command, Control, Message, NodeId, NodeRole, ProposeResult,
    RawNode, ReadIndexResult, ReadState, Slot, Value, WriteOp,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::crash::{CrashSeam, Seam};
use crate::storage::{NodeStorage, StorageError};

/// Well-known RPC token the paros node service is registered at. Every node
/// serves [`Paros`] here, and clients address it by `(node address, this token)`
/// — no service discovery. Must be `> WELL_KNOWN_RESERVED_COUNT` (3).
pub const WLTOKEN_PAROS: u32 = 4;

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

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
/// never left). Provider-generic but inert in production, where
/// [`NoCrash`](crate::NoCrash) never fires. Purely observational; the crash
/// animation reads it to mark the persist/send seam a node died on.
pub const EV_CRASHED: &str = "crashed";

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

/// Tracing event: a client proposal was answered by the **dedup fast path** —
/// the `(client, seq)` was already applied here, so the reply fired immediately
/// instead of being parked on a slot ([`ProposeResult::Chosen`]). Carries `node`
/// and the `slot` the ack names. Purely observational: this is the one committed
/// ack that does not come out of the apply loop, so the sweep needs evidence it
/// is genuinely reached (and the ack oracle needs it named, not hidden).
pub const EV_PROPOSE_DEDUP_ACK: &str = "propose_dedup_ack";

/// How often the driver takes the **rare-but-valid decisions** the core exposes
/// as methods, instead of the helpful default.
///
/// Neither knob is a fault: both name a choice a Paxos leader is always free to
/// make, and the core is correct whichever way each goes (see
/// [`RawNode::resend_pending`] and [`RawNode::step_down`]). What they buy is
/// *reachability* — with the helpful default taken every single beat, the states
/// those choices lead to (an undecided slot below a decided one, and a leader
/// that walks away from it) are essentially unreachable, which is what left #54
/// invisible to the sweep for 1500 seeds.
///
/// **Production passes [`Perturbations::NONE`]**, and with it the driver behaves
/// exactly as it did before this existed: it re-sends on every beat and never
/// resigns. The random draws are skipped entirely in that case, so production
/// does not even consume RNG values and a seeded replay is unaffected.
///
/// The deterministic simulation is where non-zero values come from, and it does
/// **not** hard-code them: `paros-sim` draws each magnitude per seed with
/// moonpool's `buggify!`, so activation-per-seed in the harness × firing-per-beat
/// here reproduces `FoundationDB`'s two-level BUGGIFY model across the layer
/// boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Perturbations {
    /// Probability, per beat, of *skipping*
    /// [`RawNode::resend_pending`](paros_core::RawNode::resend_pending) — the
    /// leader lets its pending `Accept`s wait another beat. `0.0` re-sends every
    /// beat (production).
    pub skip_resend: f64,
    /// Probability, per beat, that a leader calls
    /// [`RawNode::step_down`](paros_core::RawNode::step_down) and resigns. `0.0`
    /// never resigns (production). Keep it small: at one beat per
    /// [`TICK_INTERVAL`] a generous value is a leaderless storm, and progress
    /// stops being observable.
    pub step_down: f64,
}

impl Perturbations {
    /// The production setting: re-send every beat, never resign. Behaviourally
    /// identical to a driver with no perturbation code at all.
    pub const NONE: Self = Self {
        skip_resend: 0.0,
        step_down: 0.0,
    };

    /// Whether this is [`Perturbations::NONE`] — the fast path that draws nothing.
    fn is_none(self) -> bool {
        self.skip_resend <= 0.0 && self.step_down <= 0.0
    }
}

/// Apply one beat's worth of [`Perturbations`] to the core, right after
/// [`RawNode::tick`].
///
/// Order matters and is the honest one: the beat's re-send (if it happens) goes
/// out *before* a resignation, so a step-down never silently swallows work the
/// driver already decided to do.
fn perturb<P: Providers>(node: &mut RawNode, providers: &P, p: Perturbations) {
    if p.is_none() {
        node.resend_pending();
        return;
    }
    let rng = providers.random();
    if !rng.random_bool(p.skip_resend) {
        node.resend_pending();
    }
    if rng.random_bool(p.step_down) {
        node.step_down();
    }
}

/// A client proposal, deduplicated by `(client, seq)` for at-most-once execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Propose {
    /// Client identity.
    pub client: u64,
    /// Per-client request sequence number (the `ClientSeq`).
    pub seq: u64,
    /// Opaque command bytes.
    pub command: Vec<u8>,
}

/// The node's acknowledgement of a [`Propose`]. The node acks on commit: a
/// `committed` ack is only sent once the command is durably chosen; otherwise it
/// is a redirect to `leader`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeAck {
    /// Echoed request sequence number.
    pub seq: u64,
    /// The node to (re)try: `Some(self)` when this node admitted or had already
    /// chosen the request; `Some(other)` to redirect; `None` when the leader is
    /// unknown.
    pub leader: Option<u64>,
    /// Whether the command is durably chosen. `false` is a redirect: retry
    /// `leader`.
    pub committed: bool,
    /// The slot the command committed at, when `committed` is `true`. `None` for a
    /// redirect. Lets the application track the chosen prefix so it can drive
    /// compaction (see [`Compact`]).
    pub slot: Option<u64>,
}

/// A client read request. Reads are idempotent, so there is no dedup; `seq` is
/// echoed for client-side matching only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Read {
    /// Client identity.
    pub client: u64,
    /// Per-client request sequence number.
    pub seq: u64,
}

/// The node's acknowledgement of a [`Read`]. A `committed` ack observes the
/// node's applied log prefix: `read_index` is the highest contiguously applied
/// slot (`None` when the prefix is empty — entries are opaque, so the watermark
/// *is* the local state a read serves). Otherwise it is a redirect to `leader`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadAck {
    /// Echoed request sequence number.
    pub seq: u64,
    /// The node to (re)try: `Some(self)` when this node served the read,
    /// `Some(other)` to redirect, `None` when the leader is unknown.
    pub leader: Option<u64>,
    /// Whether the read was served. `false` is a redirect: retry `leader`.
    pub committed: bool,
    /// The applied watermark observed, when `committed` is `true`: the highest
    /// contiguously applied slot, `None` for an empty prefix.
    pub read_index: Option<u64>,
}

/// An application request to truncate the log: drop every slot at or below
/// `up_to` across the cluster. The application owns compaction of its own state
/// and tells the **leader** how far the log may be truncated; the leader decides
/// a [`paros_core::Control::Truncate`] control command into the log, and every
/// node truncates lazily when it applies that slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Compact {
    /// The last slot the application permits dropping (inclusive). Each node
    /// clamps to its own chosen prefix, so nothing undecided is ever dropped.
    pub up_to: u64,
}

/// The node's acknowledgement of a [`Compact`]. Because truncation is now decided
/// through Paxos and applied lazily, the ack reports admission (did the leader
/// propose the control command?) plus the node's current floor — not a
/// synchronously-updated floor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactAck {
    /// The node to (re)try: `Some(self)` when the leader admitted the request,
    /// `Some(other)` to redirect, `None` when leadership is unknown.
    pub leader: Option<u64>,
    /// Whether the leader admitted the truncate (proposed the control command).
    /// `false` is a redirect: retry `leader`.
    pub accepted: bool,
    /// The node's current durable compaction floor (best-effort; the decided
    /// truncation reaches this node's floor once it applies the control command).
    pub first_slot: u64,
}

/// The paros node RPC interface. The `#[service]` macro renames this trait to
/// `ParosHandler` and generates a [`Paros`] struct that works in both server
/// (`Paros::well_known`) and client (`Paros::client_well_known`) modes — replacing
/// hand-rolled `register_handler_at` calls and magic interface/method ids.
#[service]
pub trait Paros {
    /// A client proposes a command; the node acknowledges it.
    async fn propose(&self, req: Propose) -> Result<ProposeAck, RpcError>;
    /// A client reads the applied log prefix; the node acknowledges with the
    /// observed watermark or redirects to the leader.
    async fn read(&self, req: Read) -> Result<ReadAck, RpcError>;
    /// A peer delivers a Paxos protocol message into this node's `step()` inbox.
    /// One-way: the reply is empty (peers use fire-and-forget `send`).
    async fn deliver(&self, msg: Message) -> Result<(), RpcError>;
    /// The application asks the node to truncate its log prefix; the node replies
    /// with the new durable compaction floor.
    async fn compact(&self, req: Compact) -> Result<CompactAck, RpcError>;
}

/// Parse an IP (which may lack a port) into a [`NetworkAddress`], defaulting to
/// port 4500 (the moonpool sim convention; production supplies a full address).
///
/// # Errors
///
/// Returns an error if `ip` is not a parseable network address.
pub fn parse_addr(ip: &str) -> SimulationResult<NetworkAddress> {
    let addr_str = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4500")
    };
    NetworkAddress::parse(&addr_str)
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
fn command_hash(command: &Command) -> u64 {
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
    pending: BTreeMap<Slot, Vec<(u64, ReplyPromise<ProposeAck>)>>,
    pending_reads: BTreeMap<u64, (u64, u64, ReplyPromise<ReadAck>)>,
}

/// The driver's outbound side: everything needed to put one message on the wire —
/// the transport, the membership map, and this node's id for the observability
/// events. Bundled so `drain_ready` takes one parameter instead of three.
struct Outbound<'a, P: Providers> {
    transport: &'a Arc<NetTransport<P>>,
    addrs: &'a BTreeMap<NodeId, NetworkAddress>,
    /// This node's id, for the observability events.
    self_id: u64,
}

impl<P: Providers> Outbound<'_, P> {
    /// Put `msg` on the wire (fire-and-forget) and surface the send. `msg_sent`
    /// records what genuinely left the node, so a `msg_sent` with no matching
    /// `msg_received` means exactly "the network lost it".
    fn transmit(&self, to: NodeId, msg: &Message) {
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
        if let Some(addr) = self.addrs.get(&to) {
            let client = Paros::client_well_known(addr.clone(), WLTOKEN_PAROS, self.transport);
            let _ = client.deliver.send(msg.clone());
        }
    }
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
fn drain_ready<P, S, C>(
    node: &mut RawNode,
    storage: &mut S,
    out: &Outbound<'_, P>,
    waiters: &mut ClientWaiters,
    crash: &C,
) -> SimulationResult<()>
where
    P: Providers,
    S: NodeStorage,
    C: CrashSeam,
{
    let self_id = out.self_id;
    // Copy the batch out of the borrow guard, advance to release the gate, then
    // perform I/O — persist → send → apply. Advancing before the I/O is the
    // documented async pattern; persist-before-send still holds because the
    // persist loop below precedes the send loop.
    let ready = node.ready();
    let writes: Vec<WriteOp> = ready.writes().to_vec();
    let must_sync = ready.must_sync();
    let messages: Vec<(NodeId, Message)> = ready.messages().to_vec();
    let committed: Vec<(Slot, Command)> = ready.committed().to_vec();
    let snapshot_offers: Vec<(NodeId, Slot, Ballot)> = ready.snapshot_offers().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    ready.advance();

    // 1. Persist durable writes FIRST, each op in order, flush per MustSync, and
    //    surface the persisted state for the safety + recovery oracles. The
    //    `BeforeSync` crash seam lives inside `persist_writes`.
    let promised = node.hard_state().max_promised_ballot;
    persist_writes(storage, &writes, must_sync, promised, self_id, crash)?;

    // Crash seam: after the batch is durable but before its messages leave. The
    // durable writes survive; the batch's messages are dropped (never sent), so a
    // recovered node must re-derive them. Only meaningful when there is durable
    // work or a message to lose.
    if (!writes.is_empty() || !messages.is_empty()) && crash.crash_at(Seam::AfterSyncBeforeSend) {
        tracing::info!(node = self_id, seam = "after_sync_before_send", "crashed");
        return Err(seam_crash());
    }

    // 2. Send messages — only after (1) is durable. The core addresses each one;
    //    the driver maps NodeId → address and fires (fire-and-forget).
    for (to, msg) in messages {
        out.transmit(to, &msg);
    }

    // 2b. Serve snapshot offers: the core decided a peer needs a snapshot (it
    //     asked for a prefix below our floor). Attach the opaque application bytes
    //     from storage and send the InstallSnapshot — the driver holds the state
    //     the core does not.
    for (to, chosen_index, ballot) in &snapshot_offers {
        let msg = Message::InstallSnapshot {
            from: NodeId(self_id),
            ballot: *ballot,
            chosen_index: *chosen_index,
            snapshot: Value(storage.snapshot()),
        };
        out.transmit(*to, &msg);
    }

    // 3. Apply newly chosen entries (already durable, in contiguous order) —
    //    surface them to the oracles and ack any clients waiting on each slot
    //    (ack-on-commit: a held reply fires only now that its slot is chosen).
    for (slot, command) in &committed {
        tracing::info!(
            node = self_id,
            slot = slot.0,
            vhash = command_hash(command),
            "value_chosen"
        );
        tracing::info!(
            node = self_id,
            slot = slot.0,
            applied_index = slot.0,
            "log_applied"
        );
        // A control command carries no client waiter (a `Truncate`'s effect is the
        // durable floor the core's `WriteOp::Truncate` already persisted this
        // batch); only a client entry acks a proposer.
        if matches!(command, Command::Control(_)) {
            continue;
        }
        if let Some(replies) = waiters.pending.remove(slot) {
            for (seq, w) in replies {
                w.send(ProposeAck {
                    seq,
                    leader: Some(self_id),
                    committed: true,
                    slot: Some(slot.0),
                });
            }
        }
    }

    // 3b. Answer confirmed reads — after the apply loop, so the applied prefix
    //     this same batch carried is covered by what the read observes. The ack
    //     reports the *serve-time* chosen index (at or past the confirmed read
    //     index): that is the local state actually served.
    for state in &read_states {
        if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&state.ctx) {
            waiter.send(ReadAck {
                seq,
                leader: Some(self_id),
                committed: true,
                read_index: node.hard_state().chosen_index.map(|s| s.0),
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
fn persist_writes<S: NodeStorage, C: CrashSeam>(
    storage: &mut S,
    writes: &[WriteOp],
    must_sync: paros_core::MustSync,
    promised: Ballot,
    self_id: u64,
    crash: &C,
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
            WriteOp::Truncate { first } => {
                storage.truncate(*first).map_err(|e| storage_err(&e))?;
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                snapshot,
            } => {
                storage
                    .install_snapshot(*chosen_index, *ballot, snapshot.0.clone())
                    .map_err(|e| storage_err(&e))?;
            }
        }
    }

    // Crash seam: the batch is staged but not yet flushed. A crash here loses the
    // whole un-synced batch (and no message has been sent), so surface nothing but
    // the crash marker itself. Only meaningful when the batch actually staged
    // something.
    if !writes.is_empty() && crash.crash_at(Seam::BeforeSync) {
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

    // Durable now — emit the truthful persisted state for the oracles.
    if promise_changed {
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
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    pround = promised.round,
                    pbnode = promised.node.0,
                    around = ballot.round,
                    abnode = ballot.node.0,
                    vhash = command_hash(command),
                    "persist"
                );
            }
            WriteOp::Truncate { first } => {
                tracing::info!(node = self_id, first = first.0, "compacted");
            }
            WriteOp::InstallSnapshot { chosen_index, .. } => {
                let first = chosen_index.0 + 1;
                tracing::info!(
                    node = self_id,
                    chosen_index = chosen_index.0,
                    first,
                    "snapshot_installed"
                );
                // The install jumps the applied prefix to `chosen_index` without
                // replaying entries (snapshot-xor-entries); surface the jump so the
                // no-gaps oracle (which admits it as a snapshot jump) and the
                // convergence oracle see the node reach the cluster prefix.
                tracing::info!(
                    node = self_id,
                    slot = chosen_index.0,
                    applied_index = chosen_index.0,
                    "log_applied"
                );
            }
            WriteOp::SetPromise(_) | WriteOp::SetChosenIndex(_) => {}
        }
    }
    Ok(())
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
fn draw_election_timeout<P: Providers>(providers: &P) -> u64 {
    providers
        .random()
        .random_range(ELECTION_TIMEOUT_BASE..ELECTION_TIMEOUT_BASE * 2)
}

/// Post-batch upkeep: feed the core a fresh randomized election timeout whenever
/// its election clock reset, emit `leader_elected` on the transition to Leader,
/// and drop held client replies on step-down (so clients time out and retry the
/// new leader).
fn maintain<P: Providers>(
    node: &mut RawNode,
    providers: &P,
    last_role: &mut NodeRole,
    waiters: &mut ClientWaiters,
    self_id: u64,
) {
    if node.needs_election_timeout() {
        node.set_election_timeout(draw_election_timeout(providers));
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
        let gaps = node.election_gap_fills();
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
            waiter.send(ReadAck {
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
fn replay_boot_state(node: &RawNode, self_id: u64) {
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
    for (slot, (ballot, command)) in node.accepted() {
        tracing::info!(
            node = self_id,
            slot = slot.0,
            around = ballot.round,
            abnode = ballot.node.0,
            vhash = command_hash(command),
            "recovered"
        );
    }
    if let Some(ci) = node.hard_state().chosen_index {
        for (slot, (_b, command)) in node.accepted().range(..=ci) {
            tracing::info!(
                node = self_id,
                slot = slot.0,
                vhash = command_hash(command),
                "value_chosen"
            );
            tracing::info!(
                node = self_id,
                slot = slot.0,
                applied_index = slot.0,
                "log_applied"
            );
        }
    }
}

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation — only the providers
/// differ) and `S: NodeStorage` (the injected durable storage). The loop owns a
/// [`RawNode`], serves the [`Paros`] RPC interface, feeds client proposals and
/// peer messages into the core, sends the core's outbound messages to the peers
/// named in `members`, and ticks until `shutdown` fires.
///
/// `members` is the full cluster membership (`NodeId` → address, *including*
/// this node): the core addresses each outbound message by `NodeId`, and the
/// driver resolves it here. It must be consistent across the cluster and agree
/// with the `Config` the node read from `storage`.
///
/// `perturbations` decides how often the driver takes the rare-but-valid
/// alternatives to its helpful defaults (see [`Perturbations`]). Production
/// passes [`Perturbations::NONE`], which re-sends every beat, never resigns, and
/// consumes no randomness.
///
/// # Errors
///
/// Returns an error if the transport fails to bind or listen on `local_addr`. May
/// also return a simulated crash marker ([`is_seam_crash`]) if `crash` fires at a
/// durability seam — the caller recovers by re-running `run_node` with fresh
/// storage. Production passes [`NoCrash`](crate::NoCrash), which never fires.
#[tracing::instrument(skip_all)]
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state.
#[allow(clippy::too_many_lines)]
pub async fn run_node<P, S, C>(
    providers: P,
    mut storage: S,
    local_addr: NetworkAddress,
    members: Vec<(NodeId, NetworkAddress)>,
    shutdown: CancellationToken,
    crash: &C,
    perturbations: Perturbations,
) -> SimulationResult<()>
where
    P: Providers,
    S: NodeStorage,
    C: CrashSeam,
{
    let transport = NetTransportBuilder::new(providers.clone())
        .local_address(local_addr)
        .build_listening()
        .await
        .map_err(|e| SimulationError::InvalidState(format!("node transport: {e}")))?;

    // Serve the Paros interface at the well-known token. `svc.propose` /
    // `svc.deliver` are typed receive streams the loop selects over.
    let svc = Paros::well_known(&transport, WLTOKEN_PAROS);

    // The sans-IO core, bootstrapped from durable storage.
    let mut node = RawNode::new(&storage);
    let self_id = node.config().id.0;

    replay_boot_state(&node, self_id);

    let addrs: BTreeMap<NodeId, NetworkAddress> = members.into_iter().collect();

    // The outbound side: transport + membership. Every message this incarnation
    // sends goes through it.
    let out = Outbound {
        transport: &transport,
        addrs: &addrs,
        self_id,
    };

    // The held client replies: proposals keyed by slot (ack-on-commit), reads
    // keyed by their read-index ctx.
    let mut waiters = ClientWaiters::default();
    let mut next_read_ctx: u64 = 0;
    // Seed the first randomized election timeout (jitter from the driver's RNG).
    node.set_election_timeout(draw_election_timeout(&providers));
    let mut last_role = node.role();

    let time = providers.time().clone();
    let mut ticks: u64 = 0;

    loop {
        moonpool_core::select! {
            Some((req, reply)) = svc.propose.recv() => {
                // A client value → the leader (deduplicated by (client, seq)). The
                // reply is held until the slot commits (ack-on-commit); a non-leader
                // redirects immediately.
                let seq = req.seq;
                match node.propose(ClientId(req.client), ClientSeq(req.seq), Value(req.command)) {
                    ProposeResult::NotLeader(hint) => {
                        reply.send(ProposeAck { seq, leader: hint.map(|n| n.0), committed: false, slot: None });
                    }
                    ProposeResult::Accepted(slot) | ProposeResult::Duplicate(slot) => {
                        waiters.pending.entry(slot).or_default().push((seq, reply));
                    }
                    ProposeResult::Chosen(slot) => {
                        // Already inside this node's applied prefix before this
                        // call, so the ack fires immediately — and it *names* the
                        // slot, exactly like the ack-on-commit path. A committed
                        // ack that named nothing was unfalsifiable: the client was
                        // told "applied" with no way for an oracle to check the
                        // claim against the applied prefix.
                        tracing::info!(node = self_id, slot = slot.0, "propose_dedup_ack");
                        reply.send(ProposeAck { seq, leader: Some(self_id), committed: true, slot: Some(slot.0) });
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, crash)?;
                maintain(&mut node, &providers, &mut last_role, &mut waiters, self_id);
            }
            Some((req, reply)) = svc.read.recv() => {
                // A client read via read-index: the leader captures its applied
                // watermark, confirms it is still leader with a heartbeat-ack
                // quorum round (no log write), and the reply is parked until the
                // confirmed `ReadState` surfaces after apply — a deposed or
                // freshly elected leader can no longer serve a stale watermark.
                // A non-leader redirects immediately.
                let seq = req.seq;
                match node.read_index(next_read_ctx) {
                    ReadIndexResult::NotLeader(hint) => {
                        reply.send(ReadAck { seq, leader: hint.map(|n| n.0), committed: false, read_index: None });
                    }
                    ReadIndexResult::Pending => {
                        waiters.pending_reads.insert(next_read_ctx, (seq, ticks, reply));
                        next_read_ctx += 1;
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, crash)?;
                maintain(&mut node, &providers, &mut last_role, &mut waiters, self_id);
            }
            Some((msg, reply)) = svc.deliver.recv() => {
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
                    tracing::info!(
                        node = self_id,
                        from_slot = from_slot.0,
                        floor = node.first_slot().0,
                        "prepare_below_floor"
                    );
                }
                node.step(msg);
                drain_ready(&mut node, &mut storage, &out, &mut waiters, crash)?;
                maintain(&mut node, &providers, &mut last_role, &mut waiters, self_id);
                reply.send(());
            }
            Some((req, reply)) = svc.compact.recv() => {
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
                drain_ready(&mut node, &mut storage, &out, &mut waiters, crash)?;
                maintain(&mut node, &providers, &mut last_role, &mut waiters, self_id);
                reply.send(ack);
            }
            _ = time.sleep(TICK_INTERVAL) => {
                node.tick();
                // The beat's two discretionary decisions: re-send the leader's
                // still-pending `Accept`s, and (far more rarely) resign. With
                // `Perturbations::NONE` — production — this is exactly "re-send
                // every beat", with no draw taken.
                perturb(&mut node, &providers, perturbations);
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
                        waiter.send(ReadAck { seq, leader: Some(self_id), committed: false, read_index: None });
                    }
                }
                drain_ready(&mut node, &mut storage, &out, &mut waiters, crash)?;
                maintain(&mut node, &providers, &mut last_role, &mut waiters, self_id);
                // Surface a chosen slot stranded above the applied prefix. The
                // `Ready` handshake only ever hands out the *contiguous* prefix, so
                // a hole below a chosen slot is otherwise invisible from outside the
                // core. Re-emitted every tick while it lasts: the oracle reads its
                // persistence past quiescence, not a single instant.
                if let Some((hole, above)) = node.chosen_gap() {
                    tracing::info!(node = self_id, hole = hole.0, above = above.0, "chosen_gap");
                }
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
