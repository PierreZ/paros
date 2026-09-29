//! The inbound edge: the RPC runtime every driver serves from, the typed
//! request queues its loop selects on, the one-shot reply seam, and the
//! `Deliver` lane that hands peer messages to the loop.

use std::num::NonZeroUsize;
use std::sync::Arc;

use moonpool_core::{Detach, Providers, SimulationError, SimulationResult, TaskProvider};
use moonpool_rpc::{
    AccessClass, IncomingRequest, ReplyHandle, RequestStream, RpcConfig, RpcHandle, RpcMethod,
};
use paros_core::{JournalId, Message, Party};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::internal;
use super::message_from_proto;
use super::methods::{DeliverRpc, WellKnownMethod};
use crate::driver::DriverTunables;

/// Largest frame a paros runtime accepts or produces: a full delivery batch
/// (the driver's 3 MiB batch cap) plus envelope headroom. A client runtime
/// takes the same limit, or a large `Read` page would fail `ReplyTooLarge`
/// on its side.
pub const MAX_FRAME_BYTES: u32 = 4 << 20;

/// The runtime configuration every paros role serves and calls with, shaped
/// by the driver's tunables: the default per-endpoint queue is the client
/// inbox (a full queue refuses `Overloaded`, never admitted, which a client
/// sees as a failed call; the `Deliver` lane registers its own, see
/// [`serve_deliveries`]), the connect budget is `connection_timeout`, and the
/// liveness ping is the keep-alive pair (provider time, so a half-open
/// connection is failed deterministically).
pub(crate) fn rpc_config(tunables: &DriverTunables) -> RpcConfig {
    let mut config = RpcConfig {
        max_frame_bytes: MAX_FRAME_BYTES,
        endpoint_queue_capacity: tunables.client_inbox_capacity,
        connect_timeout: tunables.connection_timeout,
        ..RpcConfig::default()
    };
    config.peer.ping_interval = tunables.keep_alive_interval;
    config.peer.ping_timeout = tunables.keep_alive_timeout;
    config
}

/// Register `M` at its well-known id on `rpc`, public (paros authenticates
/// nobody yet: the deployment is a trusted network).
///
/// # Errors
///
/// A registration the runtime refuses (never on a fresh listening runtime).
pub(crate) fn serve_well_known<P: Providers, M: WellKnownMethod>(
    rpc: &RpcHandle<P>,
) -> SimulationResult<RequestStream<M>> {
    rpc.register_well_known::<M>(M::ID, AccessClass::Public)
        .map(|(_, stream)| stream)
        .map_err(|e| SimulationError::InvalidState(format!("register {}: {e}", M::NAME)))
}

/// The one-shot answer to a request a driver loop holds: sending consumes
/// it, dropping it is a broken promise (the caller's call fails at once,
/// possibly executed) — the reply seam's drop.
pub(crate) struct ReplySender<U>(Box<dyn FnOnce(U) -> bool + Send>);

impl<U: 'static> ReplySender<U> {
    fn new<M: RpcMethod>(handle: ReplyHandle<M>, encode: fn(U) -> M::Reply) -> Self {
        Self(Box::new(move |reply| handle.send(&encode(reply))))
    }
}

impl<U> ReplySender<U> {
    /// Answer; `false` when the session the request came on is gone.
    pub(crate) fn send(self, reply: U) -> bool {
        (self.0)(reply)
    }
}

/// One well-known endpoint's requests, decoded into what the loop steps
/// (`T`) with a reply seam for what it answers (`U`). A request that does
/// not decode is refused by dropping its reply handle (the caller sees a
/// broken promise) and never reaches the loop.
pub(crate) struct Inbound<M: RpcMethod, T, U> {
    stream: RequestStream<M>,
    decode: fn(M::Request) -> Result<T, &'static str>,
    encode: fn(U) -> M::Reply,
}

impl<M: RpcMethod, T, U: 'static> Inbound<M, T, U> {
    /// Wrap `stream` with the body codecs.
    pub(crate) fn new(
        stream: RequestStream<M>,
        decode: fn(M::Request) -> Result<T, &'static str>,
        encode: fn(U) -> M::Reply,
    ) -> Self {
        Self {
            stream,
            decode,
            encode,
        }
    }

    /// The next decodable request, `None` once the runtime is gone.
    /// Cancel-safe: the only await is the stream's.
    #[tracing::instrument(level = "trace", skip_all, fields(method = M::NAME))]
    pub(crate) async fn recv(&mut self) -> Option<(T, ReplySender<U>)> {
        loop {
            let IncomingRequest { request, reply } = self.stream.recv().await?;
            match (self.decode)(request) {
                Ok(request) => return Some((request, ReplySender::new(reply, self.encode))),
                Err(error) => tracing::warn!(method = M::NAME, error, "undecodable request"),
            }
        }
    }
}

impl<M: RpcMethod> Inbound<M, M::Request, M::Reply> {
    /// A method whose bodies the loop consumes exactly as they arrive.
    pub(crate) fn plain(stream: RequestStream<M>) -> Self {
        Self::new(stream, Ok, std::convert::identity)
    }
}

/// Why the edge refused an inbound request before the loop saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeRejection {
    /// A peer message that decoded from the wire but not into a `Message`.
    MessageDecode,
    /// A peer message whose envelope names no journal (`0`, #188).
    UnsetJournal,
}

/// The edge's observation callback: the driver installs one that forwards to
/// its [`Audit`](crate::Audit), stamped with the node's identity.
pub(crate) type OnReject = Arc<dyn Fn(EdgeRejection) + Send + Sync>;

/// Serve the `Deliver` lane on its own task until the runtime ends or
/// `shutdown` fires, and return the loop's side: a bounded queue of
/// `capacity` decoded messages.
///
/// Each batch is decoded and enqueued message by message; the ack is sent
/// once every message is *in the queue* (the bounded send is the only wait,
/// the backpressure), so the sender's `delivery_timeout` races the
/// connection and this queue, never the time the loop takes to persist and
/// step the batch. A message that decodes from the wire but not into a
/// [`Message`] is refused — reported through `on_reject`, the batch's reply
/// dropped — after the messages before it were enqueued.
///
/// The task draws no randomness and consults no hook (a hook answer is a
/// randomness draw, and a detached task is not where the simulation steps
/// deterministically).
#[tracing::instrument(level = "debug", skip_all, fields(at = %me, capacity))]
pub(crate) fn serve_deliveries<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    capacity: usize,
    me: Party,
    on_reject: OnReject,
    shutdown: CancellationToken,
) -> SimulationResult<mpsc::Receiver<(JournalId, Message)>> {
    // The lane's own endpoint queue (moonpool-rpc's per-endpoint queues):
    // `capacity` batches, never the client inbox's depth. While this task
    // waits on a full inbox the queue absorbs one batch from every peer
    // lane, so the peer knob alone decides the lane's shape and the client
    // knob can be extreme without refusing peer traffic.
    let queue = RpcConfig::default()
        .endpoint_queue()
        .ok_or_else(|| SimulationError::InvalidState("default endpoint queue is empty".into()))?
        .with_requests(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN));
    let mut stream = rpc
        .register_well_known_with::<DeliverRpc>(DeliverRpc::ID, AccessClass::Public, queue)
        .map(|(_, stream)| stream)
        .map_err(|e| {
            SimulationError::InvalidState(format!("register {}: {e}", DeliverRpc::NAME))
        })?;
    let (inbox, messages) = mpsc::channel(capacity);
    providers
        .task()
        .spawn_task("paros-deliver-edge", async move {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                () = deliver_all(&mut stream, &inbox, me, &on_reject) => {}
            }
        })
        .detach();
    Ok(messages)
}

/// The `Deliver` edge task's body: one batch at a time, acked on enqueue.
#[tracing::instrument(level = "debug", skip_all, fields(at = %me))]
async fn deliver_all(
    stream: &mut RequestStream<DeliverRpc>,
    inbox: &mpsc::Sender<(JournalId, Message)>,
    me: Party,
    on_reject: &OnReject,
) {
    while let Some(IncomingRequest { request, reply }) = stream.recv().await {
        if enqueue_batch(request, inbox, me, on_reject).await {
            reply.send(&internal::DeliverAck {});
        }
    }
}

/// Enqueue one batch; `false` when a message was refused or the loop is
/// gone (the reply is then dropped).
#[tracing::instrument(level = "trace", skip_all, fields(at = %me, messages = batch.messages.len()))]
async fn enqueue_batch(
    batch: internal::Deliver,
    inbox: &mpsc::Sender<(JournalId, Message)>,
    me: Party,
    on_reject: &OnReject,
) -> bool {
    for message in batch.messages {
        let journal = JournalId(message.journal);
        if !journal.is_set() {
            on_reject(EdgeRejection::UnsetJournal);
            tracing::warn!("a Paxos message names no journal");
            return false;
        }
        let message = match message_from_proto(message) {
            Ok(message) => message,
            Err(error) => {
                on_reject(EdgeRejection::MessageDecode);
                tracing::warn!(error, "invalid Paxos message");
                return false;
            }
        };
        if inbox.send((journal, message)).await.is_err() {
            return false;
        }
    }
    true
}
