//! The one RPC edge every driver in this crate serves from: a moonpool-rpc
//! runtime listening on the process's address, the well-known endpoints the
//! role registers on it, and the typed queues its loop selects on. The node,
//! the proxy leader, the replica and the matchmaker differ only in which
//! methods they register.
//!
//! The runtime is owned here and **polled by the loop** ([`RpcEdge::run`] is
//! one of its `select!` arms), never spawned: when an incarnation ends —
//! returning, or dropped mid-await by a crash — the listener, every
//! connection and every pending call go with it on the spot, so a restart
//! at the same address never meets its predecessor's socket.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use moonpool_core::{Providers, SimulationError, SimulationResult};
use moonpool_rpc::{RpcDriver, RpcHandle};
use paros_core::{
    GcAck, GcRequest, MatchReply, MatchRequest, Message, Party, ReconfigureReply,
    ReconfigureRequest,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::DriverTunables;
use crate::audit::Audit;
use crate::rpc::methods::{
    CompactRpc, GarbageCollectRpc, InspectRpc, MatchmakeRpc, MatchmakerReconfigureRpc, ProposeRpc,
    QuorumReadRpc, ReadRpc, ReconfigureMatchmakersRpc, ReconfigureRpc, RetireRpc,
};
use crate::rpc::{
    Inbound, OnReject, garbage_collect_from_wire, match_request_from_wire,
    reconfigure_request_from_wire, rpc_config, serve_deliveries, serve_well_known,
    wire_garbage_collect_ack, wire_match_reply, wire_reconfigure_reply,
};

/// The edge's rejection callback for the driver serving as `me`: each
/// rejection is reported to `audit` as [`Audit::edge_rejected`], stamped with
/// that identity. Pure construction; the edge's answer does not depend on it.
pub(crate) fn edge_reporter<A: Audit + Clone + Send + Sync + 'static>(
    audit: &A,
    me: Party,
) -> OnReject {
    let audit = audit.clone();
    Arc::new(move |kind| audit.edge_rejected(me, kind))
}

/// A method whose bodies the loop consumes exactly as they arrive.
pub(crate) type Plain<M> =
    Inbound<M, <M as moonpool_rpc::RpcMethod>::Request, <M as moonpool_rpc::RpcMethod>::Reply>;

/// A driver's inbound edge: the listening runtime and its driving future.
pub(crate) struct RpcEdge<P: Providers> {
    handle: RpcHandle<P>,
    run: Pin<Box<dyn Future<Output = io::Error> + Send>>,
    role: &'static str,
}

impl<P: Providers> RpcEdge<P> {
    /// Bind `local_addr` and start a runtime shaped by `tunables`. Nothing
    /// is served until the loop polls [`RpcEdge::run`].
    ///
    /// # Errors
    ///
    /// A configuration the runtime refuses or a failed bind, as
    /// [`RunError::Infra`](super::RunError::Infra).
    pub(crate) async fn listen(
        providers: &P,
        local_addr: &str,
        role: &'static str,
        tunables: &DriverTunables,
    ) -> SimulationResult<Self> {
        let (driver, handle) =
            RpcDriver::listen(providers.clone(), local_addr, rpc_config(tunables))
                .await
                .map_err(|e| SimulationError::InvalidState(format!("{role} RPC listener: {e}")))?;
        Ok(Self {
            handle,
            run: Box::pin(driver.run()),
            role,
        })
    }

    /// The runtime every endpoint of this incarnation registers on, and
    /// every outbound client binds to.
    pub(crate) fn handle(&self) -> &RpcHandle<P> {
        &self.handle
    }

    /// Drive the runtime; resolves only if it fails for good, with the
    /// error the loop exits on. Cancel-safe: the runtime's future is kept
    /// across `select!` passes.
    pub(crate) async fn run(&mut self) -> SimulationError {
        let error = (&mut self.run).await;
        SimulationError::IoError(format!("{} RPC runtime: {error}", self.role))
    }
}

/// A node's inbound queues: the public journal, the operator calls, and the
/// peer lane.
pub(crate) struct NodeInbox {
    pub(crate) propose: Plain<ProposeRpc>,
    pub(crate) read: Plain<ReadRpc>,
    pub(crate) quorum_read: Plain<QuorumReadRpc>,
    pub(crate) compact: Plain<CompactRpc>,
    pub(crate) reconfigure: Plain<ReconfigureRpc>,
    pub(crate) reconfigure_matchmakers: Plain<ReconfigureMatchmakersRpc>,
    pub(crate) inspect: Plain<InspectRpc>,
    pub(crate) retire: Plain<RetireRpc>,
    pub(crate) deliver: mpsc::Receiver<Message>,
}

impl NodeInbox {
    /// Register a node's endpoints on `edge`; the peer lane holds
    /// `peer_inbox_capacity` messages and ends with `shutdown`.
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    pub(crate) fn serve<P: Providers>(
        providers: &P,
        edge: &RpcEdge<P>,
        tunables: &DriverTunables,
        on_reject: OnReject,
        shutdown: CancellationToken,
    ) -> SimulationResult<Self> {
        let rpc = edge.handle();
        Ok(Self {
            propose: Inbound::plain(serve_well_known(rpc)?),
            read: Inbound::plain(serve_well_known(rpc)?),
            quorum_read: Inbound::plain(serve_well_known(rpc)?),
            compact: Inbound::plain(serve_well_known(rpc)?),
            reconfigure: Inbound::plain(serve_well_known(rpc)?),
            reconfigure_matchmakers: Inbound::plain(serve_well_known(rpc)?),
            inspect: Inbound::plain(serve_well_known(rpc)?),
            retire: Inbound::plain(serve_well_known(rpc)?),
            deliver: serve_deliveries(
                providers,
                rpc,
                tunables.peer_inbox_capacity,
                on_reject,
                shutdown,
            )?,
        })
    }
}

/// A replica's inbound queues (#144): the lane, the `Inspect` a probe reads
/// its application through, and the public `QuorumRead` (§3.4: a client
/// reads from a replica). Nothing else a node serves is a replica's.
pub(crate) struct ReplicaInbox {
    pub(crate) inspect: Plain<InspectRpc>,
    pub(crate) quorum_read: Plain<QuorumReadRpc>,
    pub(crate) deliver: mpsc::Receiver<Message>,
}

impl ReplicaInbox {
    /// Register a replica's endpoints on `edge`.
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    pub(crate) fn serve<P: Providers>(
        providers: &P,
        edge: &RpcEdge<P>,
        tunables: &DriverTunables,
        on_reject: OnReject,
        shutdown: CancellationToken,
    ) -> SimulationResult<Self> {
        let rpc = edge.handle();
        Ok(Self {
            inspect: Inbound::plain(serve_well_known(rpc)?),
            quorum_read: Inbound::plain(serve_well_known(rpc)?),
            deliver: serve_deliveries(
                providers,
                rpc,
                tunables.peer_inbox_capacity,
                on_reject,
                shutdown,
            )?,
        })
    }
}

/// A matchmaker's inbound queues: the contract's three methods, decoded
/// into the core's types (a malformed request never reaches the loop).
pub(crate) struct MatchmakerInbox {
    pub(crate) requests: Inbound<MatchmakeRpc, MatchRequest, MatchReply>,
    pub(crate) collects: Inbound<GarbageCollectRpc, GcRequest, GcAck>,
    pub(crate) reconfigures:
        Inbound<MatchmakerReconfigureRpc, ReconfigureRequest, ReconfigureReply>,
}

impl MatchmakerInbox {
    /// Register a matchmaker's endpoints on `edge`.
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    pub(crate) fn serve<P: Providers>(edge: &RpcEdge<P>) -> SimulationResult<Self> {
        let rpc = edge.handle();
        Ok(Self {
            requests: Inbound::new(serve_well_known(rpc)?, match_request_from_wire, |reply| {
                wire_match_reply(&reply)
            }),
            collects: Inbound::new(serve_well_known(rpc)?, garbage_collect_from_wire, |ack| {
                wire_garbage_collect_ack(&ack)
            }),
            reconfigures: Inbound::new(
                serve_well_known(rpc)?,
                reconfigure_request_from_wire,
                |reply| wire_reconfigure_reply(&reply),
            ),
        })
    }
}
