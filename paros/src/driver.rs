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

use std::time::Duration;

use moonpool_core::{NetworkAddress, Providers, SimulationError, SimulationResult, TimeProvider};
use moonpool_transport::{NetTransportBuilder, RpcError, service};
use paros_core::{Message, RawNode};
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

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send.
/// Stage 1's no-op core produces empty batches; the ordering is wired for
/// Stage 2.
fn drain_ready<S: NodeStorage>(node: &mut RawNode, storage: &mut S) {
    let ready = node.ready();
    // 1. Persist durable state FIRST.
    if let Some(hard_state) = ready.hard_state() {
        storage.set_hard_state(hard_state.clone());
    }
    // 2. Send messages — only after (1) is durable. 3. Apply committed entries.
    // Both empty under the Stage-1 core; this asserts that invariant so a future
    // stage cannot silently leak output before the driver ships it.
    debug_assert!(
        ready.messages().is_empty(),
        "Stage 1 core emits no messages"
    );
    debug_assert!(ready.committed().is_empty(), "Stage 1 core commits nothing");
    // 4. Release the gate.
    ready.advance();
}

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation — only the providers
/// differ) and `S: NodeStorage` (the injected durable storage). The loop owns a
/// [`RawNode`], serves the [`Paros`] RPC interface, and ticks until `shutdown`
/// fires.
///
/// # Errors
///
/// Returns an error if the transport fails to bind or listen on `local_addr`.
pub async fn run_node<P, S>(
    providers: P,
    mut storage: S,
    local_addr: NetworkAddress,
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

    // The sans-IO core: a fresh, empty node (no protocol logic in Stage 1).
    let mut node = RawNode::new(&storage);

    let time = providers.time().clone();
    let mut ticks: u64 = 0;

    loop {
        tokio::select! {
            Some((req, reply)) = svc.propose.recv() => {
                // Stage 1: no consensus yet — run the handshake to exercise the
                // durable-before-send path, then acknowledge.
                let seq = req.seq;
                drain_ready(&mut node, &mut storage);
                reply.send(ProposeAck { seq });
            }
            Some((msg, reply)) = svc.deliver.recv() => {
                // A peer Paxos message → the core's single input router. The same
                // `paros_core::Message` is sent and received (no DTO).
                tracing::info!(kind = message_kind(&msg), "peer_message_received");
                node.step(msg);
                drain_ready(&mut node, &mut storage);
                reply.send(());
            }
            _ = time.sleep(TICK_INTERVAL) => {
                node.tick();
                ticks += 1;
                drain_ready(&mut node, &mut storage);
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}
