//! The inbound edge: the RPC runtime every driver serves from, the typed
//! request queues its loop selects on, the one-shot reply seam, and the
//! `Deliver` lane that hands peer messages to the loop.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use crate::runtime::ParosProviders;
use moonpool_core::{Detach, LocalTaskProvider, SimulationError, SimulationResult};
use moonpool_rpc::{
    AccessClass, IncomingRequest, InterfaceMethod, ReplyHandle, RequestStream, RpcConfig,
    RpcHandle, RpcMethod, ServiceGroup,
};
use paros_core::Message;
use paros_core::contract::Party;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::fence::CellFence;

use paros_tunables::DriverTunables;
use paros_wire::internal;
use paros_wire::message_from_proto;
use paros_wire::methods::{DeliverRpc, MachineInterface, WellKnownMethod};

pub use paros_tunables::MAX_FRAME_BYTES;

/// The runtime configuration every paros role serves and calls with, shaped
/// by the driver's tunables: the default per-endpoint queue is the client
/// inbox (a full queue refuses `Overloaded`, never admitted, which a client
/// sees as a failed call; the `Deliver` lane registers its own, see
/// [`serve_deliveries`]), the connect budget is `connection_timeout`, and the
/// liveness ping is the keep-alive pair (provider time, so a half-open
/// connection is failed deterministically).
#[must_use]
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
    #[must_use]
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error, never
    /// an operating condition.
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

/// The address a peer machine advertises, as its own peer batch names it:
/// its address, its machine interface (empty: the well-known `Deliver`) and
/// its transport incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SenderAdvert {
    pub addr: String,
    pub interface: Vec<u8>,
    pub incarnation: u128,
}

impl SenderAdvert {
    /// The advert a batch carries: `None` when its sender advertises
    /// nothing (no node id or no address, a role that runs on no machine).
    #[must_use]
    pub fn of(batch: &internal::Deliver) -> Option<(u64, Self)> {
        if batch.from == 0 || batch.from_addr.is_empty() {
            return None;
        }
        let incarnation = (u128::from(batch.from_incarnation_high) << 64)
            | u128::from(batch.from_incarnation_low);
        Some((
            batch.from,
            Self {
                addr: batch.from_addr.clone(),
                interface: batch.from_interface.clone(),
                incarnation,
            },
        ))
    }
}

/// The newest advert each peer machine's batches named since the node loop
/// last took them. The `Deliver` edge writes it; the node loop takes it at
/// its tick, so a lane is readdressed only where the simulation steps
/// deterministically (a `BTreeMap`: the order is part of determinism).
///
/// A peer that moved is dialed at its new address once it sends to this
/// node, before the registry names it there: the registry's write may need
/// this node's answer to that peer (a lone founding member whose plan
/// journals grew past it, #851 (grow the cell plan journals)).
#[derive(Clone, Default)]
pub struct SenderAdverts(Arc<Mutex<BTreeMap<u64, SenderAdvert>>>);

impl SenderAdverts {
    /// Note `advert` as `from`'s newest.
    ///
    /// # Panics
    ///
    /// If `from` is `0`: an advert always names its sender.
    pub fn note(&self, from: u64, advert: SenderAdvert) {
        assert!(from != 0, "an advert names its sender");
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(from, advert);
    }

    /// The adverts noted since the last call, each peer's newest.
    #[must_use]
    pub fn take(&self) -> BTreeMap<u64, SenderAdvert> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// The edge's observation callback: the driver installs one that forwards to
/// its [`Audit`](paros_observe::audit::Audit), stamped with the node's identity.
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
/// # Errors
///
/// When the call or the decoding fails: the error says why.
#[allow(clippy::too_many_arguments)]
pub fn serve_deliveries<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    capacity: usize,
    me: Party,
    fence: &CellFence,
    (on_reject, adverts): (OnReject, Option<SenderAdverts>),
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
                adverts: Option<SenderAdverts>,
                shutdown: CancellationToken,
                dynamic: bool| {
        let fence = fence.clone();
        async move {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                () = deliver_all(&mut stream, &inbox, (me, &fence, dynamic), (&on_reject, adverts.as_ref())) => {}
            }
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
                    adverts.clone(),
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
            lane(stream, inbox, on_reject, adverts, shutdown, false),
        )
        .detach();
    Ok(messages)
}

/// The `Deliver` edge task's body: one batch at a time, acked on enqueue.
/// `dynamic` says the stream is the machine group's (#404).
#[tracing::instrument(level = "debug", skip_all, fields(at = %me, cell = fence.own(), dynamic))]
async fn deliver_all(
    stream: &mut RequestStream<DeliverRpc>,
    inbox: &mpsc::Sender<(JournalIdentifier, Message)>,
    (me, fence, dynamic): (Party, &CellFence, bool),
    (on_reject, adverts): (&OnReject, Option<&SenderAdverts>),
) {
    while let Some(IncomingRequest { request, reply }) = stream.recv().await {
        if dynamic {
            moonpool_assertions::reachable!(
                "edge: a peer batch arrives through a machine interface"
            );
        }
        let foreign = request.cell_id != fence.own();
        // Another cell's peer passes only for a tenant whose move names
        // that cell's machines (#783 (cross the cell fence)): every message
        // of the batch must be one.
        let admitted = !foreign
            || (!request.messages.is_empty()
                && request
                    .messages
                    .iter()
                    .all(|m| fence.admits(request.cell_id, TenantId(m.tenant))));
        if foreign && admitted {
            moonpool_assertions::reachable!(
                "edge: a peer batch of another cell passes for a moving tenant"
            );
        }
        if !admitted {
            // Another cell's peer (#216, ScyllaDB's cluster id in gossip):
            // after a bad configuration, or at an address a wiped machine
            // left and another cell took. Refused whole, never acked.
            on_reject(EdgeRejection::ForeignCell);
            moonpool_assertions::reachable!("edge: a peer batch from another cell is refused");
            tracing::warn!(from = request.cell_id, "a peer batch names another cell");
            continue;
        }
        // The sender's advert, from a peer of this cell only: the node
        // loop follows it at its tick.
        if let (Some(adverts), Some((from, advert))) = (adverts, SenderAdvert::of(&request)) {
            adverts.note(from, advert);
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

#[cfg(test)]
mod advert_tests {
    use super::{SenderAdvert, SenderAdverts};

    fn advert(addr: &str, incarnation: u128) -> SenderAdvert {
        SenderAdvert {
            addr: addr.into(),
            interface: Vec::new(),
            incarnation,
        }
    }

    #[test]
    fn the_loop_takes_each_peers_newest_advert_once() {
        let adverts = SenderAdverts::default();
        adverts.note(2, advert("a:1", 1));
        adverts.note(2, advert("b:1", 2));
        adverts.note(1, advert("c:1", 3));
        let taken = adverts.take();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[&2], advert("b:1", 2));
        assert_eq!(taken[&1], advert("c:1", 3));
        assert!(adverts.take().is_empty(), "taken once");
    }

    #[test]
    fn a_batch_without_a_sender_or_an_address_carries_no_advert() {
        let mut batch = paros_wire::internal::Deliver {
            from: 3,
            ..Default::default()
        };
        assert!(SenderAdvert::of(&batch).is_none());
        batch.from = 0;
        batch.from_addr = "a:1".into();
        assert!(SenderAdvert::of(&batch).is_none());
        batch.from = 3;
        batch.from_incarnation_high = 1;
        batch.from_incarnation_low = 2;
        let (from, advert) = SenderAdvert::of(&batch).expect("an advert");
        assert_eq!(from, 3);
        assert_eq!(advert.incarnation, (1_u128 << 64) | 2);
    }
}
