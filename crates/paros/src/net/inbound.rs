//! The inbound edge: the RPC runtime every driver serves from, the typed
//! request queues its loop selects on, the one-shot reply seam, and the
//! `Deliver` lane that hands peer messages to the loop.

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::net::runtime::ParosProviders;
use moonpool_core::{Detach, LocalTaskProvider, SimulationError, SimulationResult};
use moonpool_rpc::{
    AccessClass, IncomingRequest, InterfaceMethod, ReplyHandle, RequestStream, RpcConfig,
    RpcHandle, RpcMethod, ServiceGroup,
};
use paros_core::{JournalId, JournalIdentifier, Message, Party, TenantId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::tunables::DriverTunables;
use crate::wire::internal;
use crate::wire::message_from_proto;
use crate::wire::methods::{DeliverRpc, MachineInterface, WellKnownMethod};

pub use crate::tunables::MAX_FRAME_BYTES;

/// The runtime configuration every paros role serves and calls with, shaped
/// by the driver's tunables: the default per-endpoint queue is the client
/// inbox (a full queue refuses `Overloaded`, never admitted, which a client
/// sees as a failed call; the `Deliver` lane registers its own, see
/// [`serve_deliveries`]), the connect budget is `connection_timeout`, and the
/// liveness ping is the keep-alive pair (provider time, so a half-open
/// connection is failed deterministically).
pub fn rpc_config(tunables: &DriverTunables) -> RpcConfig {
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
pub fn serve_well_known<P: ParosProviders, M: WellKnownMethod>(
    rpc: &RpcHandle<P>,
) -> SimulationResult<RequestStream<M>> {
    rpc.register_well_known::<M>(M::ID, AccessClass::Public)
        .map(|(_, stream)| stream)
        .map_err(|e| SimulationError::InvalidState(format!("register {}: {e}", M::NAME)))
}

/// The one-shot answer to a request a driver loop holds: sending consumes
/// it, dropping it is a broken promise (the caller's call fails at once,
/// possibly executed) — the reply seam's drop.
pub struct ReplySender<U>(Box<dyn FnOnce(U) -> bool>);

impl<U: 'static> ReplySender<U> {
    fn new<M: RpcMethod>(handle: ReplyHandle<M>, encode: fn(U) -> M::Reply) -> Self {
        Self(Box::new(move |reply| handle.send(&encode(reply))))
    }
}

impl<U> ReplySender<U> {
    /// Answer; `false` when the session the request came on is gone.
    pub fn send(self, reply: U) -> bool {
        (self.0)(reply)
    }
}

impl<U: 'static> ReplySender<U> {
    /// The same answer, with `observe` shown the reply just before it
    /// leaves (#366 (metrics)): an observer only, it cannot change the
    /// reply. A reply dropped unsent is never observed.
    pub fn observed(self, observe: impl FnOnce(&U) + 'static) -> Self {
        Self(Box::new(move |reply| {
            observe(&reply);
            (self.0)(reply)
        }))
    }
}

/// One well-known endpoint's requests, decoded into what the loop steps
/// (`T`) with a reply seam for what it answers (`U`). A request that does
/// not decode is refused by dropping its reply handle (the caller sees a
/// broken promise) and never reaches the loop.
///
/// A machine also serves the method in its machine group (#404 (static vs
/// dynamic endpoints), [`Inbound::in_group`]): both streams feed the same
/// queue, under the same rules.
pub struct Inbound<M: RpcMethod, T, U> {
    stream: RequestStream<M>,
    /// The same method in the machine's group, while the group serves it.
    group: Option<RequestStream<M>>,
    decode: fn(M::Request) -> Result<T, &'static str>,
    encode: fn(U) -> M::Reply,
}

/// Which stream one [`Inbound::recv`] heard.
enum Heard<M: RpcMethod> {
    Request(IncomingRequest<M>, bool),
    /// The well-known endpoint ended: the runtime is gone.
    Gone,
    /// The group ended: it was dropped, the well-known endpoint stays.
    GroupGone,
}

impl<M: RpcMethod, T, U: 'static> Inbound<M, T, U> {
    /// Wrap `stream` with the body codecs.
    pub fn new(
        stream: RequestStream<M>,
        decode: fn(M::Request) -> Result<T, &'static str>,
        encode: fn(U) -> M::Reply,
    ) -> Self {
        Self {
            stream,
            group: None,
            decode,
            encode,
        }
    }

    /// The next decodable request, `None` once the runtime is gone.
    /// Cancel-safe: the only awaits are the streams'.
    #[tracing::instrument(level = "trace", skip_all, fields(method = M::NAME))]
    pub async fn recv(&mut self) -> Option<(T, ReplySender<U>)> {
        loop {
            let heard = match self.group.as_mut() {
                None => self
                    .stream
                    .recv()
                    .await
                    .map_or(Heard::Gone, |r| Heard::Request(r, false)),
                Some(group) => moonpool_core::select! {
                    biased;
                    r = self.stream.recv() => r.map_or(Heard::Gone, |r| Heard::Request(r, false)),
                    r = group.recv() => r.map_or(Heard::GroupGone, |r| Heard::Request(r, true)),
                },
            };
            let IncomingRequest { request, reply } = match heard {
                Heard::Request(incoming, dynamic) => {
                    if dynamic {
                        moonpool_assertions::reachable!(
                            "edge: a call arrives through a machine interface"
                        );
                    }
                    incoming
                }
                Heard::Gone => return None,
                Heard::GroupGone => {
                    self.group = None;
                    continue;
                }
            };
            match (self.decode)(request) {
                Ok(request) => return Some((request, ReplySender::new(reply, self.encode))),
                Err(error) => tracing::warn!(method = M::NAME, error, "undecodable request"),
            }
        }
    }

    /// A request already queued, without waiting: `None` when none is, or
    /// the runtime is gone. One poll of [`Inbound::recv`], which is
    /// cancel-safe, so a request it does not return stays queued.
    pub fn try_recv(&mut self) -> Option<(T, ReplySender<U>)> {
        futures::FutureExt::now_or_never(self.recv()).flatten()
    }
}

impl<M: InterfaceMethod<MachineInterface>, T, U> Inbound<M, T, U> {
    /// Serve the method in the machine's `group` too (#404 (static vs
    /// dynamic endpoints)); unchanged without one (a static deployment, a
    /// matchmaker or a replica process).
    ///
    /// # Errors
    ///
    /// A registration the group refuses.
    pub fn in_group(
        mut self,
        group: Option<&ServiceGroup<MachineInterface>>,
    ) -> SimulationResult<Self> {
        assert!(self.group.is_none(), "a method is served once per group");
        self.group = group
            .map(ServiceGroup::serve::<M>)
            .transpose()
            .map_err(|e| {
                SimulationError::InvalidState(format!("serve {} in its group: {e}", M::NAME))
            })?;
        Ok(self)
    }
}

impl<M: RpcMethod> Inbound<M, M::Request, M::Reply> {
    /// A method whose bodies the loop consumes exactly as they arrive.
    pub fn plain(stream: RequestStream<M>) -> Self {
        Self::new(stream, Ok, std::convert::identity)
    }
}

/// Why the edge refused an inbound request before the loop saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeRejection {
    /// A peer message that decoded from the wire but not into a `Message`.
    MessageDecode,
    /// A peer message whose envelope names no journal: an unset tenant or
    /// journal (`0`, #188, #235).
    UnsetJournal,
    /// A peer batch from another cell (#216): its `cell_id` is not the
    /// receiver's. The whole batch is refused.
    ForeignCell,
}

/// The edge's observation callback: the driver installs one that forwards to
/// its [`Audit`](crate::observe::audit::Audit), stamped with the node's identity.
pub type OnReject = Arc<dyn Fn(EdgeRejection) + Send + Sync>;

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
/// dropped — after the messages before it were enqueued. A batch from
/// another cell than `cell_id` is refused whole, before any of its messages
/// is decoded (#216): a machine never steps a peer of another cell.
///
/// A machine also serves `Deliver` in its `group` (#404 (static vs dynamic
/// endpoints)), the dynamic endpoint its peers dial through the reference
/// the registry holds: both endpoints feed the same queue, each from a
/// task of its own, under the same rules.
///
/// The tasks draw no randomness and consult no hook (a hook answer is a
/// randomness draw, and a detached task is not where the simulation steps
/// deterministically).
#[tracing::instrument(level = "debug", skip_all, fields(at = %me, capacity))]
// One lane's wiring: the runtime, its shape, the receiver's identity, its
// observer, its lifetime and the machine's group.
#[allow(clippy::too_many_arguments)]
pub fn serve_deliveries<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    capacity: usize,
    me: Party,
    cell_id: u64,
    on_reject: OnReject,
    shutdown: CancellationToken,
    group: Option<&ServiceGroup<MachineInterface>>,
) -> SimulationResult<mpsc::Receiver<(JournalIdentifier, Message)>> {
    // The lane's own endpoint queue (moonpool-rpc's per-endpoint queues):
    // `capacity` batches, never the client inbox's depth. While this task
    // waits on a full inbox the queue absorbs one batch from every peer
    // lane, so the peer knob alone decides the lane's shape and the client
    // knob can be extreme without refusing peer traffic.
    let queue = RpcConfig::default()
        .endpoint_queue()
        .ok_or_else(|| SimulationError::InvalidState("default endpoint queue is empty".into()))?
        .with_requests(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN));
    let stream = rpc
        .register_well_known_with::<DeliverRpc>(DeliverRpc::ID, AccessClass::Public, queue)
        .map(|(_, stream)| stream)
        .map_err(|e| {
            SimulationError::InvalidState(format!("register {}: {e}", DeliverRpc::NAME))
        })?;
    let dynamic = group
        .map(|group| group.serve_with::<DeliverRpc>(queue))
        .transpose()
        .map_err(|e| {
            SimulationError::InvalidState(format!("serve {} in its group: {e}", DeliverRpc::NAME))
        })?;
    let (inbox, messages) = mpsc::channel(capacity);
    let lane = |mut stream: RequestStream<DeliverRpc>,
                inbox: mpsc::Sender<(JournalIdentifier, Message)>,
                on_reject: OnReject,
                shutdown: CancellationToken,
                dynamic: bool| async move {
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => {}
            () = deliver_all(&mut stream, &inbox, (me, cell_id, dynamic), &on_reject) => {}
        }
    };
    if let Some(stream) = dynamic {
        providers
            .local_task()
            .spawn_local_task(
                "paros-deliver-edge-group",
                lane(
                    stream,
                    inbox.clone(),
                    on_reject.clone(),
                    shutdown.clone(),
                    true,
                ),
            )
            .detach();
    }
    providers
        .local_task()
        .spawn_local_task(
            "paros-deliver-edge",
            lane(stream, inbox, on_reject, shutdown, false),
        )
        .detach();
    Ok(messages)
}

/// The `Deliver` edge task's body: one batch at a time, acked on enqueue.
/// `dynamic` says the stream is the machine group's (#404).
#[tracing::instrument(level = "debug", skip_all, fields(at = %me, cell = cell_id, dynamic))]
async fn deliver_all(
    stream: &mut RequestStream<DeliverRpc>,
    inbox: &mpsc::Sender<(JournalIdentifier, Message)>,
    (me, cell_id, dynamic): (Party, u64, bool),
    on_reject: &OnReject,
) {
    while let Some(IncomingRequest { request, reply }) = stream.recv().await {
        if dynamic {
            moonpool_assertions::reachable!(
                "edge: a peer batch arrives through a machine interface"
            );
        }
        if request.cell_id != cell_id {
            // Another cell's peer (#216, ScyllaDB's cluster id in gossip):
            // after a bad configuration, or at an address a wiped machine
            // left and another cell took. Refused whole, never acked.
            on_reject(EdgeRejection::ForeignCell);
            moonpool_assertions::reachable!("edge: a peer batch from another cell is refused");
            tracing::warn!(from = request.cell_id, "a peer batch names another cell");
            continue;
        }
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
    inbox: &mpsc::Sender<(JournalIdentifier, Message)>,
    me: Party,
    on_reject: &OnReject,
) -> bool {
    for message in batch.messages {
        let journal = JournalIdentifier::new(TenantId(message.tenant), JournalId(message.journal));
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
