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
//! persist → send → apply → advance order (durable-before-send). Stage 1's core
//! is a no-op, so the batches are empty — the ordering is wired for Stage 2.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{NetworkAddress, Providers, SimulationError, SimulationResult, TimeProvider};
use moonpool_transport::{NetTransport, NetTransportBuilder, RpcError, service};
use paros_core::{Message, NodeId, RawNode, Slot, Value};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::storage::NodeStorage;

/// Well-known RPC token the paros node service is registered at. Every node
/// serves [`Paros`] here, and clients address it by `(node address, this token)`
/// — no service discovery. Must be `> WELL_KNOWN_RESERVED_COUNT` (3).
pub const WLTOKEN_PAROS: u32 = 4;

/// How often a node advances its logical clock.
const TICK_INTERVAL: Duration = Duration::from_millis(50);

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

/// Tracing event: this node sent a protocol message. Carries `node`, `to`, and
/// `kind` — timeline/debug only.
pub const EV_MSG_SENT: &str = "msg_sent";

/// A client proposal: a sequence number plus an opaque command payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Propose {
    /// Client request sequence number.
    pub seq: u64,
    /// Opaque command bytes (uninterpreted in Stage 1).
    pub command: Vec<u8>,
}

/// The node's acknowledgement of a [`Propose`]: the echoed sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeAck {
    /// Echoed request sequence number.
    pub seq: u64,
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

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
fn drain_ready<P, S>(
    node: &mut RawNode,
    storage: &mut S,
    transport: &Arc<NetTransport<P>>,
    addrs: &BTreeMap<NodeId, NetworkAddress>,
    self_id: u64,
) where
    P: Providers,
    S: NodeStorage,
{
    let ready = node.ready();

    // 1. Persist durable state FIRST, and surface it for the safety oracles.
    if let Some(hard_state) = ready.hard_state() {
        storage.set_hard_state(hard_state.clone());
        let pb = hard_state.max_promised_ballot;
        if let Some((ab, v)) = hard_state.accepted.get(&Slot(0)) {
            tracing::info!(
                node = self_id,
                pround = pb.round,
                pbnode = pb.node.0,
                has_accepted = true,
                around = ab.round,
                abnode = ab.node.0,
                vhash = value_hash(&v.0),
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
        tracing::info!(
            node = self_id,
            to = to.0,
            kind = message_kind(msg),
            "msg_sent"
        );
        if let Some(addr) = addrs.get(to) {
            let client = Paros::client_well_known(addr.clone(), WLTOKEN_PAROS, transport);
            let _ = client.deliver.send(msg.clone());
        }
    }

    // 3. Apply newly chosen entries (already durable) — surfaced to the oracle.
    for (slot, value) in ready.committed() {
        tracing::info!(
            node = self_id,
            slot = slot.0,
            vhash = value_hash(&value.0),
            "value_chosen"
        );
    }

    // 4. Release the gate.
    ready.advance();
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
    let addrs: BTreeMap<NodeId, NetworkAddress> = members.into_iter().collect();

    let time = providers.time().clone();
    let mut ticks: u64 = 0;

    loop {
        tokio::select! {
            Some((req, reply)) = svc.propose.recv() => {
                // A client value → the proposer. Ack means accepted-for-processing
                // (consensus runs in the background), not yet chosen.
                let seq = req.seq;
                node.propose(Value(req.command));
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id);
                reply.send(ProposeAck { seq });
            }
            Some((msg, reply)) = svc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO).
                tracing::info!(kind = message_kind(&msg), "peer_message_received");
                node.step(msg);
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id);
                reply.send(());
            }
            _ = time.sleep(TICK_INTERVAL) => {
                node.tick();
                ticks += 1;
                drain_ready(&mut node, &mut storage, &transport, &addrs, self_id);
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
