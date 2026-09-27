//! The node's and the proxy leader's tonic handlers and the bridge into
//! their single-owner driver loops.

use std::sync::Arc;

use paros_core::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use super::consensus::message_from_proto;
use super::{
    Call, Compact, CompactAck, InspectReply, InspectRequest, Propose, ProposeAck, Read, ReadAck,
    Reconfigure, ReconfigureAck, ReconfigureMatchmakers, ReconfigureMatchmakersAck, RetireAck,
    RetireRequest, internal, public,
};

/// Requests accepted concurrently by tonic and consumed serially by the node
/// driver, which exclusively owns the sans-IO core. Every client-facing lane
/// carries a reply channel; `deliver` is fire-and-forget — a peer message is
/// acknowledged to its sender the moment it is *enqueued* here, never after
/// the loop has stepped it (see [`RpcService`]).
pub(crate) struct RpcInbox {
    pub(crate) propose: mpsc::Receiver<Call<Propose, ProposeAck>>,
    pub(crate) read: mpsc::Receiver<Call<Read, ReadAck>>,
    pub(crate) quorum_read: mpsc::Receiver<Call<Read, ReadAck>>,
    pub(crate) deliver: mpsc::Receiver<Message>,
    pub(crate) compact: mpsc::Receiver<Call<Compact, CompactAck>>,
    pub(crate) reconfigure: mpsc::Receiver<Call<Reconfigure, ReconfigureAck>>,
    pub(crate) reconfigure_matchmakers:
        mpsc::Receiver<Call<ReconfigureMatchmakers, ReconfigureMatchmakersAck>>,
    pub(crate) inspect: mpsc::Receiver<Call<InspectRequest, InspectReply>>,
    pub(crate) retire: mpsc::Receiver<Call<RetireRequest, RetireAck>>,
}

/// Why the gRPC edge refused an inbound request before the node loop saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeRejection {
    /// A peer message that decoded from the wire but not into a `Message`.
    MessageDecode,
}

/// The edge's observation callback: the driver installs one that forwards to
/// its [`Audit`](crate::Audit), stamped with the node's identity.
pub(crate) type OnReject = Arc<dyn Fn(EdgeRejection) + Send + Sync>;

/// Cloneable tonic handler. Each client-facing method forwards to [`RpcInbox`]
/// and holds the HTTP/2 response open until the driver completes that request.
/// `deliver` is the exception: it holds the response open only until every
/// message of the batch is *in the inbox* (the bounded `send().await` is the
/// backpressure), so the sender's `delivery_timeout` races the peer's inbox
/// capacity, not the time its loop takes to persist and step the batch.
#[derive(Clone)]
pub(crate) struct RpcService {
    propose: mpsc::Sender<Call<Propose, ProposeAck>>,
    read: mpsc::Sender<Call<Read, ReadAck>>,
    quorum_read: mpsc::Sender<Call<Read, ReadAck>>,
    deliver: mpsc::Sender<Message>,
    compact: mpsc::Sender<Call<Compact, CompactAck>>,
    reconfigure: mpsc::Sender<Call<Reconfigure, ReconfigureAck>>,
    reconfigure_matchmakers: mpsc::Sender<Call<ReconfigureMatchmakers, ReconfigureMatchmakersAck>>,
    inspect: mpsc::Sender<Call<InspectRequest, InspectReply>>,
    retire: mpsc::Sender<Call<RetireRequest, RetireAck>>,
    on_reject: OnReject,
}

/// Construct a handler/inbox pair for one node incarnation. `client_inbox`
/// bounds each client-facing queue (propose, both reads, compact, reconfigure,
/// reconfigure-matchmakers, inspect, retire) and
/// `peer_inbox` the peer-message queue; both must be at least 1.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn rpc_channel(
    client_inbox: usize,
    peer_inbox: usize,
    on_reject: OnReject,
) -> (RpcService, RpcInbox) {
    // Bounded queues make overload visible as backpressure while leaving ample
    // room for one simulation tick's peer-message fanout.
    let (propose_tx, propose_rx) = mpsc::channel(client_inbox);
    let (read_tx, read_rx) = mpsc::channel(client_inbox);
    let (quorum_read_tx, quorum_read_rx) = mpsc::channel(client_inbox);
    let (deliver_tx, deliver_rx) = mpsc::channel(peer_inbox);
    let (compact_tx, compact_rx) = mpsc::channel(client_inbox);
    let (reconfigure_tx, reconfigure_rx) = mpsc::channel(client_inbox);
    let (reconfigure_mm_tx, reconfigure_mm_rx) = mpsc::channel(client_inbox);
    let (inspect_tx, inspect_rx) = mpsc::channel(client_inbox);
    let (retire_tx, retire_rx) = mpsc::channel(client_inbox);
    (
        RpcService {
            propose: propose_tx,
            read: read_tx,
            quorum_read: quorum_read_tx,
            deliver: deliver_tx,
            compact: compact_tx,
            reconfigure: reconfigure_tx,
            reconfigure_matchmakers: reconfigure_mm_tx,
            inspect: inspect_tx,
            retire: retire_tx,
            on_reject,
        },
        RpcInbox {
            propose: propose_rx,
            read: read_rx,
            quorum_read: quorum_read_rx,
            deliver: deliver_rx,
            compact: compact_rx,
            reconfigure: reconfigure_rx,
            reconfigure_matchmakers: reconfigure_mm_rx,
            inspect: inspect_rx,
            retire: retire_rx,
        },
    )
}

/// Hand one decoded request to the driver's inbox and wait for its answer.
pub(super) async fn call<T, U>(sender: &mpsc::Sender<Call<T, U>>, value: T) -> Result<U, Status> {
    let (reply_tx, reply_rx) = oneshot::channel();
    sender
        .send((value, reply_tx))
        .await
        .map_err(|_| Status::unavailable("node driver stopped"))?;
    reply_rx
        .await
        .map_err(|_| Status::unavailable("node driver dropped the reply"))
}

/// [`call`] for a request the driver consumes exactly as it arrived.
async fn dispatch<T, U>(
    sender: &mpsc::Sender<Call<T, U>>,
    request: Request<T>,
) -> Result<Response<U>, Status> {
    call(sender, request.into_inner()).await.map(Response::new)
}

/// Decode one matchmaker-wire request, refusing a malformed one as an invalid
/// argument that names `what` was being decoded.
pub(super) fn decode_wire<W, T>(
    request: Request<W>,
    decode: fn(W) -> Result<T, &'static str>,
    what: &str,
) -> Result<T, Status> {
    decode(request.into_inner())
        .map_err(|error| Status::invalid_argument(format!("invalid {what}: {error}")))
}

#[tonic::async_trait]
impl public::paros_server::Paros for RpcService {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn propose(&self, request: Request<Propose>) -> Result<Response<ProposeAck>, Status> {
        dispatch(&self.propose, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn read(&self, request: Request<Read>) -> Result<Response<ReadAck>, Status> {
        dispatch(&self.read, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn quorum_read(&self, request: Request<Read>) -> Result<Response<ReadAck>, Status> {
        dispatch(&self.quorum_read, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn compact(&self, request: Request<Compact>) -> Result<Response<CompactAck>, Status> {
        dispatch(&self.compact, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure(
        &self,
        request: Request<Reconfigure>,
    ) -> Result<Response<ReconfigureAck>, Status> {
        dispatch(&self.reconfigure, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure_matchmakers(
        &self,
        request: Request<ReconfigureMatchmakers>,
    ) -> Result<Response<ReconfigureMatchmakersAck>, Status> {
        dispatch(&self.reconfigure_matchmakers, request).await
    }
}

/// Decode one `Deliver` batch and enqueue it, message by message, into the
/// loop's inbox. The ack means "in the inbox", not "processed" — the bounded
/// send is the only wait — and a message that decodes from the wire but not
/// into a [`Message`] is refused at the edge and reported through
/// `on_reject`. Shared by the node's handler and the proxy leader's.
async fn enqueue_delivery(
    inbox: &mpsc::Sender<Message>,
    on_reject: &OnReject,
    request: Request<internal::Deliver>,
) -> Result<Response<internal::DeliverAck>, Status> {
    for message in request.into_inner().messages {
        let message = message_from_proto(message).map_err(|error| {
            on_reject(EdgeRejection::MessageDecode);
            Status::invalid_argument(format!("invalid Paxos message: {error}"))
        })?;
        inbox
            .send(message)
            .await
            .map_err(|_| Status::unavailable("driver stopped"))?;
    }
    Ok(Response::new(internal::DeliverAck {}))
}

#[tonic::async_trait]
impl internal::paros_internal_server::ParosInternal for RpcService {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn deliver(
        &self,
        request: Request<internal::Deliver>,
    ) -> Result<Response<internal::DeliverAck>, Status> {
        enqueue_delivery(&self.deliver, &self.on_reject, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectReply>, Status> {
        dispatch(&self.inspect, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn retire(&self, request: Request<RetireRequest>) -> Result<Response<RetireAck>, Status> {
        dispatch(&self.retire, request).await
    }
}

// ---- the deliver-only roles: the proxy leader (#142), the replica (#144) ----

/// Which deliver-only role a [`LaneService`] serves — the reason its two
/// operator methods are refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LaneRole {
    /// A proxy leader: holds no replica to inspect, retired by stopping it.
    Proxy,
    /// A replica that is not an acceptor: it answers `Inspect` with its
    /// chosen prefix and its application's state — what a probe comparing
    /// every applier reads — and has no configuration to retire from.
    Replica,
}

/// The tonic handler of a role that hears the **`Deliver` lane** and
/// little else: a proxy leader receives delegated `Accept`s, `Accepted`s and
/// `Nack`s, a replica receives `Commit`s, beats and catch-up answers, both
/// through the same lane a node does. A replica also answers `Inspect` (its
/// application is what a probe reads); nothing else a client or an operator
/// asks a node is theirs, so the rest is refused as unimplemented.
#[derive(Clone)]
pub(crate) struct LaneService {
    deliver: mpsc::Sender<Message>,
    /// The replica's `Inspect` queue; `None` on a proxy leader.
    inspect: Option<mpsc::Sender<Call<InspectRequest, InspectReply>>>,
    on_reject: OnReject,
    role: LaneRole,
}

/// The loop-side queues of a [`LaneService`]: the lane, and — on a replica —
/// the `Inspect` calls.
pub(crate) struct LaneInbox {
    pub(crate) deliver: mpsc::Receiver<Message>,
    pub(crate) inspect: Option<mpsc::Receiver<Call<InspectRequest, InspectReply>>>,
}

/// Construct a lane role's handler/inbox pair; `peer_inbox` bounds the lane
/// and the replica's `Inspect` queue (each at least 1).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lane_channel(
    role: LaneRole,
    peer_inbox: usize,
    on_reject: OnReject,
) -> (LaneService, LaneInbox) {
    let (deliver_tx, deliver_rx) = mpsc::channel(peer_inbox);
    let (inspect_tx, inspect_rx) = match role {
        LaneRole::Proxy => (None, None),
        LaneRole::Replica => {
            let (tx, rx) = mpsc::channel(peer_inbox);
            (Some(tx), Some(rx))
        }
    };
    (
        LaneService {
            deliver: deliver_tx,
            inspect: inspect_tx,
            on_reject,
            role,
        },
        LaneInbox {
            deliver: deliver_rx,
            inspect: inspect_rx,
        },
    )
}

#[tonic::async_trait]
impl internal::paros_internal_server::ParosInternal for LaneService {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn deliver(
        &self,
        request: Request<internal::Deliver>,
    ) -> Result<Response<internal::DeliverAck>, Status> {
        enqueue_delivery(&self.deliver, &self.on_reject, request).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectReply>, Status> {
        match &self.inspect {
            Some(inspect) => dispatch(inspect, request).await,
            None => Err(Status::unimplemented(
                "a proxy leader holds no replica to inspect",
            )),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn retire(
        &self,
        _request: Request<RetireRequest>,
    ) -> Result<Response<RetireAck>, Status> {
        Err(Status::unimplemented(match self.role {
            LaneRole::Proxy => "a proxy leader is retired by stopping its process",
            LaneRole::Replica => "a replica is retired by stopping its process",
        }))
    }
}
