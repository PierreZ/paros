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
    Ballot, ClientId, ClientSeq, Message, NodeId, NodeRole, ProposeResult, RawNode, Slot, Value,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::storage::NodeStorage;

/// Well-known RPC token the paros node service is registered at. Every node
/// serves [`Paros`] here, and clients address it by `(node address, this token)`
/// — no service discovery. Must be `> WELL_KNOWN_RESERVED_COUNT` (3).
pub const WLTOKEN_PAROS: u32 = 4;

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

/// Base election timeout, in ticks. Each node's actual timeout is drawn
/// uniformly from `[T, 2T)` (jitter from the [`RandomProvider`], in the driver,
/// never the zero-dep core) to break the dueling-proposer livelock. `T`
/// dominates the core's heartbeat interval, so a live leader always beats before
/// a follower's election clock fires.
const ELECTION_TIMEOUT_BASE: u64 = 5;

/// Tracing event name for a node logical-clock tick. Emitters use the string
/// literal (tracing requires one); readers (oracles) match on this constant.
pub const EV_NODE_TICK: &str = "node_tick";

/// Tracing event: this node's durable state changed. Carries `node` (id), the
/// promised ballot (`pround`/`pbnode`), and — when `has_accepted` — the slot-0
/// accepted ballot (`around`/`abnode`) + value hash (`vhash`). The safety oracle
/// reads it for the monotonic-promise and never-accept-below-promise invariants.
pub const EV_NODE_STATE: &str = "node_state";

/// Tracing event: this node applied a chosen value. Carries `node`, `slot`, and
/// the value hash (`vhash`). The safety oracle reads it for the
/// at-most-one-value-chosen invariant.
pub const EV_CHOSEN: &str = "value_chosen";

/// Tracing event: this node sent a protocol message. Carries `node` (sender),
/// `to` (destination), and `kind`; for the six ballot-carrying Paxos kinds it
/// also carries the ballot (`bround`/`bnode`) and `slot`. The wasm demo pairs it
/// with [`EV_MSG_RECV`] to draw the protocol timeline.
pub const EV_MSG_SENT: &str = "msg_sent";

/// Tracing event: this node received a protocol message (the mirror of
/// [`EV_MSG_SENT`]). Carries `node` (receiver), `from` (sender), and `kind`; for
/// the six ballot-carrying Paxos kinds it also carries `bround`/`bnode`/`slot`. A
/// sent message with no matching receive is one the network dropped.
pub const EV_MSG_RECV: &str = "msg_received";

/// Tracing event: this node became leader. Carries `node` and `round` (its
/// ballot round). The leader-uniqueness oracle asserts at most one leader per
/// round across the cluster.
pub const EV_LEADER: &str = "leader_elected";

/// Tracing event: this node advanced its applied (contiguous chosen) prefix.
/// Carries `node`, `slot` (the slot just applied), and `applied_index` (the new
/// high-water mark). The no-gaps oracle asserts the prefix grows by one without
/// skipping.
pub const EV_APPLIED: &str = "log_applied";

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
}

/// The paros node RPC interface. The `#[service]` macro renames this trait to
/// `ParosHandler` and generates a [`Paros`] struct that works in both server
/// (`Paros::well_known`) and client (`Paros::client_well_known`) modes — replacing
/// hand-rolled `register_handler_at` calls and magic interface/method ids.
#[service]
pub trait Paros {
    /// A client proposes a command; the node acknowledges it.
    async fn propose(&self, req: Propose) -> Result<ProposeAck, RpcError>;
    /// A peer delivers a Paxos protocol message into this node's `step()` inbox.
    /// One-way: the reply is empty (peers use fire-and-forget `send`).
    async fn deliver(&self, msg: Message) -> Result<(), RpcError>;
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

/// A short, stable label for a [`Message`] variant, for observability.
fn message_kind(m: &Message) -> &'static str {
    match m {
        Message::Prepare { .. } => "prepare",
        Message::Promise { .. } => "promise",
        Message::Accept { .. } => "accept",
        Message::Accepted { .. } => "accepted",
        Message::Nack { .. } => "nack",
        Message::Commit { .. } => "commit",
        Message::CheckLeader { .. } => "check_leader",
        Message::Heartbeat { .. } => "heartbeat",
        _ => "unknown",
    }
}

/// The `(sender, ballot, slot)` triple a ballot-carrying Paxos message routes on,
/// for observability. The six consensus kinds return `Some`; the tick-injected
/// `CheckLeader`/`Heartbeat` self-events (no ballot/slot) return `None`.
fn message_route(m: &Message) -> Option<(NodeId, Ballot, Slot)> {
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
        } => Some((*from, *ballot, *from_slot)),
        Message::Accept {
            from, ballot, slot, ..
        }
        | Message::Accepted { from, ballot, slot }
        | Message::Nack { from, ballot, slot }
        | Message::Commit {
            from, ballot, slot, ..
        } => Some((*from, *ballot, *slot)),
        Message::Heartbeat {
            from,
            ballot,
            commit,
        } => Some((*from, *ballot, *commit)),
        _ => None,
    }
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
fn drain_ready<P, S>(
    node: &mut RawNode,
    storage: &mut S,
    transport: &Arc<NetTransport<P>>,
    addrs: &BTreeMap<NodeId, NetworkAddress>,
    self_id: u64,
    pending: &mut BTreeMap<Slot, Vec<(u64, ReplyPromise<ProposeAck>)>>,
) where
    P: Providers,
    S: NodeStorage,
{
    let ready = node.ready();

    // 1. Persist durable state FIRST, and surface it for the safety oracles.
    if let Some(hard_state) = ready.hard_state() {
        storage.set_hard_state(hard_state.clone());
        let pb = hard_state.max_promised_ballot;
        if let Some((ab, e)) = hard_state.accepted.get(&Slot(0)) {
            tracing::info!(
                node = self_id,
                pround = pb.round,
                pbnode = pb.node.0,
                has_accepted = true,
                around = ab.round,
                abnode = ab.node.0,
                vhash = value_hash(&e.value.0),
                "node_state"
            );
        } else {
            tracing::info!(
                node = self_id,
                pround = pb.round,
                pbnode = pb.node.0,
                has_accepted = false,
                "node_state"
            );
        }
    }

    // 2. Send messages — only after (1) is durable. The core addresses each one;
    //    the driver just maps NodeId → address and fires (fire-and-forget).
    for (to, msg) in ready.messages() {
        let kind = message_kind(msg);
        if let Some((_, ballot, slot)) = message_route(msg) {
            tracing::info!(
                node = self_id,
                to = to.0,
                kind,
                bround = ballot.round,
                bnode = ballot.node.0,
                slot = slot.0,
                "msg_sent"
            );
        } else {
            tracing::info!(node = self_id, to = to.0, kind, "msg_sent");
        }
        if let Some(addr) = addrs.get(to) {
            let client = Paros::client_well_known(addr.clone(), WLTOKEN_PAROS, transport);
            let _ = client.deliver.send(msg.clone());
        }
    }

    // 3. Apply newly chosen entries (already durable, in contiguous order) —
    //    surface them to the oracles and ack any clients waiting on each slot
    //    (ack-on-commit: a held reply fires only now that its slot is chosen).
    for (slot, entry) in ready.committed() {
        tracing::info!(
            node = self_id,
            slot = slot.0,
            vhash = value_hash(&entry.value.0),
            "value_chosen"
        );
        tracing::info!(
            node = self_id,
            slot = slot.0,
            applied_index = slot.0,
            "log_applied"
        );
        if let Some(waiters) = pending.remove(slot) {
            for (seq, w) in waiters {
                w.send(ProposeAck {
                    seq,
                    leader: Some(self_id),
                    committed: true,
                });
            }
        }
    }

    // 4. Release the gate.
    ready.advance();
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
    pending: &mut BTreeMap<Slot, Vec<(u64, ReplyPromise<ProposeAck>)>>,
    self_id: u64,
) {
    if node.needs_election_timeout() {
        node.set_election_timeout(draw_election_timeout(providers));
    }
    let role = node.role();
    if role == NodeRole::Leader && *last_role != NodeRole::Leader {
        tracing::info!(
            node = self_id,
            round = node.ballot().round,
            "leader_elected"
        );
    } else if *last_role == NodeRole::Leader && role != NodeRole::Leader {
        pending.clear();
    }
    *last_role = role;
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
/// # Errors
///
/// Returns an error if the transport fails to bind or listen on `local_addr`.
#[tracing::instrument(skip_all)]
pub async fn run_node<P, S>(
    providers: P,
    mut storage: S,
    local_addr: NetworkAddress,
    members: Vec<(NodeId, NetworkAddress)>,
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: Providers,
    S: NodeStorage,
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

    // On restart the core rebuilt its chosen log from durable `HardState`. Re-emit
    // each rebuilt chosen entry as `value_chosen` so the safety oracle sees this
    // node's post-restart belief and catches any divergence from the value the
    // cluster actually chose for that slot. A clean first boot has an empty log, so
    // this is a no-op. (This is what makes a "stale chosen value resurrected on
    // restart" bug observable in the simulation.)
    {
        let hs = node.hard_state();
        if let Some(ci) = hs.chosen_index {
            for (slot, (_b, entry)) in hs.accepted.range(..=ci) {
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    vhash = value_hash(&entry.value.0),
                    "value_chosen"
                );
            }
        }
    }

    let addrs: BTreeMap<NodeId, NetworkAddress> = members.into_iter().collect();

    // Client replies held until their slot commits (ack-on-commit), keyed by slot.
    let mut pending: BTreeMap<Slot, Vec<(u64, ReplyPromise<ProposeAck>)>> = BTreeMap::new();
    // Seed the first randomized election timeout (jitter from the driver's RNG).
    node.set_election_timeout(draw_election_timeout(&providers));
    let mut last_role = node.role();

    let time = providers.time().clone();
    let mut ticks: u64 = 0;

    loop {
        tokio::select! {
            Some((req, reply)) = svc.propose.recv() => {
                // A client value → the leader (deduplicated by (client, seq)). The
                // reply is held until the slot commits (ack-on-commit); a non-leader
                // redirects immediately.
                let seq = req.seq;
                match node.propose(ClientId(req.client), ClientSeq(req.seq), Value(req.command)) {
                    ProposeResult::NotLeader(hint) => {
                        reply.send(ProposeAck { seq, leader: hint.map(|n| n.0), committed: false });
                    }
                    ProposeResult::Accepted(slot) | ProposeResult::Duplicate(slot) => {
                        pending.entry(slot).or_default().push((seq, reply));
                    }
                    ProposeResult::Chosen => {
                        reply.send(ProposeAck { seq, leader: Some(self_id), committed: true });
                    }
                }
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id, &mut pending);
                maintain(&mut node, &providers, &mut last_role, &mut pending, self_id);
            }
            Some((msg, reply)) = svc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO). Surface the
                // arrival (mirror of `msg_sent`) so the demo can pair sends with
                // receives and mark the unmatched ones as network drops.
                let kind = message_kind(&msg);
                if let Some((from, ballot, slot)) = message_route(&msg) {
                    tracing::info!(
                        node = self_id,
                        from = from.0,
                        kind,
                        bround = ballot.round,
                        bnode = ballot.node.0,
                        slot = slot.0,
                        "msg_received"
                    );
                } else {
                    tracing::info!(node = self_id, kind, "msg_received");
                }
                node.step(msg);
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id, &mut pending);
                maintain(&mut node, &providers, &mut last_role, &mut pending, self_id);
                reply.send(());
            }
            _ = time.sleep(TICK_INTERVAL) => {
                node.tick();
                ticks += 1;
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id, &mut pending);
                maintain(&mut node, &providers, &mut last_role, &mut pending, self_id);
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
