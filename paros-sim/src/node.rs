//! The async driver: a moonpool [`Process`] that wraps the sans-IO
//! [`paros_core::RawNode`].
//!
//! The loop `select`s over {client request, peer message, tick timer, shutdown},
//! feeds the core via `step`/`tick`, and drains every [`paros_core::Ready`] in
//! the persist → send → apply → advance order. Stage 1's core is a no-op, so the
//! batches are empty — but the durable-before-send code path runs on every
//! stimulus, ready for Stage 2's protocol to fill it.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    NetworkAddress, Process, SimContext, SimulationError, SimulationResult, TimeProvider, UID,
};
use moonpool_transport::{NetTransport, NetTransportBuilder};
use paros_core::{Config, Message, RawNode};
use serde::{Deserialize, Serialize};

use crate::TICK_INTERVAL_MS;
use crate::storage::MemStorage;

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

/// RPC interface id for the paros node service.
const PAROS_INTERFACE: u64 = 0x9A05_0001;
/// Method index for a client propose on [`PAROS_INTERFACE`].
const METHOD_PROPOSE: u64 = 1;
/// Method index for an inbound peer Paxos message on [`PAROS_INTERFACE`].
const METHOD_PEER: u64 = 2;

/// The well-known [`UID`] a client targets to send a [`Propose`].
#[must_use]
pub(crate) fn propose_method_uid() -> UID {
    UID::new(PAROS_INTERFACE, METHOD_PROPOSE)
}

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

/// Parse a sim IP (which may lack a port) into a [`NetworkAddress`], defaulting
/// to port 4500 (the sim convention).
pub(crate) fn parse_sim_addr(ip: &str) -> SimulationResult<NetworkAddress> {
    let addr_str = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4500")
    };
    NetworkAddress::parse(&addr_str)
        .map_err(|e| SimulationError::InvalidState(format!("bad addr: {e}")))
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send.
/// Stage 1's no-op core produces empty batches; the ordering is wired for
/// Stage 2.
fn drain_ready(node: &mut RawNode, storage: &mut MemStorage) {
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

/// A paros node: owns a [`RawNode`] + its [`MemStorage`] and drives them inside
/// moonpool. Stage 1 nodes are interchangeable (no protocol, no membership yet).
pub struct NodeProcess;

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let addr = parse_sim_addr(ctx.my_ip())?;
        let transport = NetTransportBuilder::new(ctx.providers().clone())
            .local_address(addr)
            .build_listening()
            .await
            .map_err(|e| SimulationError::InvalidState(format!("node transport: {e}")))?;

        let (propose_stream, _) = NetTransport::register_handler_at::<Propose, ProposeAck>(
            &transport,
            PAROS_INTERFACE,
            METHOD_PROPOSE,
        );
        let (peer_stream, _) = NetTransport::register_handler_at::<Message, ()>(
            &transport,
            PAROS_INTERFACE,
            METHOD_PEER,
        );

        // The sans-IO core: a fresh, empty node (no protocol logic in Stage 1).
        let mut storage = MemStorage::new(Config::default());
        let mut node = RawNode::new(&storage);

        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let mut ticks: u64 = 0;

        loop {
            tokio::select! {
                Some((req, reply)) = propose_stream.recv() => {
                    // Stage 1: no consensus yet — run the handshake to exercise
                    // the durable-before-send path, then acknowledge.
                    let seq = req.seq;
                    drain_ready(&mut node, &mut storage);
                    reply.send(ProposeAck { seq });
                }
                Some((msg, reply)) = peer_stream.recv() => {
                    // A peer Paxos message → the core's single input router. The
                    // same `paros_core::Message` is sent and received (no DTO).
                    tracing::info!(kind = message_kind(&msg), "peer_message_received");
                    node.step(msg);
                    drain_ready(&mut node, &mut storage);
                    reply.send(());
                }
                _ = time.sleep(Duration::from_millis(TICK_INTERVAL_MS)) => {
                    node.tick();
                    ticks += 1;
                    drain_ready(&mut node, &mut storage);
                    tracing::info!(tick = ticks, "node_tick");
                }
                () = shutdown.cancelled() => return Ok(()),
            }
        }
    }
}
