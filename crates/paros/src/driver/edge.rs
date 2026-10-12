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
//!
//! **The trade.** Polled by the loop, the runtime makes no network progress
//! while an arm body awaits — above all the durability pipeline's storage
//! calls in `drain_ready`. Outbound lanes, replies, pings and dials all wait
//! for the arm to finish. The simulation cannot see this (its disks complete
//! every operation on the poll that started it), and it is acceptable while
//! an fsync is short next to the liveness ping (`keep_alive_timeout`): a
//! stalled persist delays traffic, it reorders or loses nothing, and every
//! class the lanes carry is lossy by contract. It stops being acceptable
//! once device latency is modelled or a production disk can stall past the
//! ping timeout — then the runtime moves to its own task, owned by the
//! incarnation, and the crash-cleanliness this buys has to be re-earned
//! (the listener must be released before a restart binds the address).

use std::sync::Arc;

use moonpool_core::{Providers, SimulationError, SimulationResult};
use moonpool_rpc::{RpcHandle, ServiceGroup};
use paros_core::{
    GcAck, GcRequest, JournalIdentifier, MatchReply, MatchRequest, MatchmakerId, Message, Party,
    ReconfigureReply, ReconfigureRequest, TenantId,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::DriverTunables;
use crate::audit::Audit;
use crate::rpc::methods::{
    GarbageCollectRpc, InspectRpc, MachineInterface, MatchmakeRpc, MatchmakerReconfigureRpc,
    ReadRpc, ReconfigureMatchmakersRpc, ReconfigureRpc, RetireRpc, SetLeaderRpc, TruncateRpc,
    WriteRpc,
};
use crate::rpc::{
    Inbound, OnReject, garbage_collect_from_wire, match_request_from_wire,
    reconfigure_request_from_wire, rpc_config, serve_deliveries, serve_well_known,
    wire_garbage_collect_ack, wire_match_reply, wire_reconfigure_reply,
};
use crate::transport::{RuntimeFuture, Transport};

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
    run: RuntimeFuture,
    role: &'static str,
}

impl<P: Providers> RpcEdge<P> {
    /// Bind `local_addr` and start a runtime shaped by `tunables`, its
    /// sessions protected by `transport` (it dials its peers the same way).
    /// Nothing is served until the loop polls [`RpcEdge::run`].
    ///
    /// # Errors
    ///
    /// A configuration the runtime refuses or a failed bind, as
    /// [`RunError::Infra`](super::RunError::Infra).
    #[tracing::instrument(level = "debug", skip_all, fields(role, local_addr = %local_addr, transport = transport.name()))]
    pub(crate) async fn listen(
        providers: &P,
        local_addr: &str,
        role: &'static str,
        tunables: &DriverTunables,
        transport: &Transport,
    ) -> SimulationResult<Self> {
        let (run, handle) = transport
            .listen(providers.clone(), local_addr, rpc_config(tunables))
            .await
            .map_err(|e| SimulationError::InvalidState(format!("{role} RPC listener: {e}")))?;
        Ok(Self { handle, run, role })
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
    pub(crate) write: Plain<WriteRpc>,
    pub(crate) log_read: Plain<ReadRpc>,
    pub(crate) truncate: Plain<TruncateRpc>,
    pub(crate) set_leader: Plain<SetLeaderRpc>,
    pub(crate) reconfigure: Plain<ReconfigureRpc>,
    pub(crate) reconfigure_matchmakers: Plain<ReconfigureMatchmakersRpc>,
    pub(crate) inspect: Plain<InspectRpc>,
    pub(crate) retire: Plain<RetireRpc>,
    pub(crate) deliver: mpsc::Receiver<(JournalIdentifier, Message)>,
}

impl NodeInbox {
    /// Register a node's endpoints on `edge`; the peer lane holds
    /// `peer_inbox_capacity` messages of cell `cell_id` and ends with
    /// `shutdown`. A machine's lane and its data plane are also served in
    /// its `machine` group (#404 (static vs dynamic endpoints)).
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    #[tracing::instrument(level = "debug", skip_all)]
    // The node's edge, its shape, its identity, its observer, its lifetime
    // and the machine's group.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn serve<P: Providers>(
        providers: &P,
        edge: &RpcEdge<P>,
        tunables: &DriverTunables,
        me: Party,
        cell_id: u64,
        on_reject: OnReject,
        shutdown: CancellationToken,
        machine: Option<&ServiceGroup<MachineInterface>>,
    ) -> SimulationResult<Self> {
        let rpc = edge.handle();
        Ok(Self {
            write: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            log_read: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            truncate: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            set_leader: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            reconfigure: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            reconfigure_matchmakers: Inbound::plain(serve_well_known(rpc)?).in_group(machine)?,
            inspect: Inbound::plain(serve_well_known(rpc)?),
            retire: Inbound::plain(serve_well_known(rpc)?),
            deliver: serve_deliveries(
                providers,
                rpc,
                tunables.peer_inbox_capacity,
                me,
                cell_id,
                on_reject,
                shutdown,
                machine,
            )?,
        })
    }
}

/// A replica's inbound queues (#144): the lane, the `Inspect` a probe reads
/// it through, and the journal `Read` (#204: read replicas serve the
/// leaderless read, §3.4, so read load leaves the acceptors). Nothing else a
/// node serves is a replica's.
pub(crate) struct ReplicaInbox {
    pub(crate) inspect: Plain<InspectRpc>,
    pub(crate) log_read: Plain<ReadRpc>,
    pub(crate) deliver: mpsc::Receiver<(JournalIdentifier, Message)>,
}

impl ReplicaInbox {
    /// Register a replica's endpoints on `edge`; the peer lane takes
    /// messages of cell `cell_id`.
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn serve<P: Providers>(
        providers: &P,
        edge: &RpcEdge<P>,
        tunables: &DriverTunables,
        me: Party,
        cell_id: u64,
        on_reject: OnReject,
        shutdown: CancellationToken,
    ) -> SimulationResult<Self> {
        let rpc = edge.handle();
        Ok(Self {
            inspect: Inbound::plain(serve_well_known(rpc)?),
            log_read: Inbound::plain(serve_well_known(rpc)?),
            deliver: serve_deliveries(
                providers,
                rpc,
                tunables.peer_inbox_capacity,
                me,
                cell_id,
                on_reject,
                shutdown,
                None,
            )?,
        })
    }
}

/// A matchmaker's inbound queues: the contract's three methods, decoded
/// into the core's types (a malformed request never reaches the loop).
pub(crate) struct MatchmakerInbox {
    pub(crate) requests: Inbound<MatchmakeRpc, (TenantId, MatchmakerId, MatchRequest), MatchReply>,
    pub(crate) collects: Inbound<GarbageCollectRpc, (TenantId, MatchmakerId, GcRequest), GcAck>,
    pub(crate) reconfigures: Inbound<
        MatchmakerReconfigureRpc,
        (TenantId, MatchmakerId, ReconfigureRequest),
        ReconfigureReply,
    >,
}

impl MatchmakerInbox {
    /// Register a matchmaker's endpoints on the runtime `rpc` (its own
    /// edge, or a machine's shared one, #190), and in the `machine` group
    /// of a founding member that hosts it (#404 (static vs dynamic
    /// endpoints)).
    ///
    /// # Errors
    ///
    /// A registration the runtime refuses.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn serve<P: Providers>(
        rpc: &RpcHandle<P>,
        machine: Option<&ServiceGroup<MachineInterface>>,
    ) -> SimulationResult<Self> {
        Ok(Self {
            requests: Inbound::new(serve_well_known(rpc)?, match_request_from_wire, |reply| {
                wire_match_reply(&reply)
            })
            .in_group(machine)?,
            collects: Inbound::new(serve_well_known(rpc)?, garbage_collect_from_wire, |ack| {
                wire_garbage_collect_ack(&ack)
            })
            .in_group(machine)?,
            reconfigures: Inbound::new(
                serve_well_known(rpc)?,
                reconfigure_request_from_wire,
                |reply| wire_reconfigure_reply(&reply),
            )
            .in_group(machine)?,
        })
    }
}
