//! The driver's outbound peer transport: the bounded, lossy, keep-newest
//! per-peer mailboxes, the [`Outbound`] handle the rest of the driver sends
//! through, and the detached delivery task that feeds bounded batches over one
//! reconnecting h2 channel per peer.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_hyper::ReconnectingChannel;
use paros_core::{Message, NodeId, Party, ProxyId};
use prost::Message as ProstMessage;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::grpc::{ParosInternalClient, internal, message_to_proto};
use crate::hooks::DriverHooks;

use super::config::{DriverTunables, GRPC_DELIVERY_BATCH, GRPC_DELIVERY_BATCH_BYTES};
use super::events::{command_hash, message_kind, message_route, proto_message_kind};

/// One peer's outbound mailboxes: `regular` for ordinary protocol traffic
/// and `snapshot` for the bulky `InstallSnapshot` class. Separate bounded
/// queues, so a snapshot push can never evict the beats and `Accept`s queued
/// beside it (and vice versa). A proxy leader opens no snapshot lane: it
/// never serves one.
pub(crate) struct PeerQueues {
    pub(crate) regular: PeerMailbox,
    pub(crate) snapshot: Option<PeerMailbox>,
}

/// One peer's bounded, lossy, **keep-newest** outbound mailbox (the etcd
/// stream-mailbox shape). The consensus driver never waits for network I/O:
/// `push` always returns at once, and when the mailbox is full it evicts the
/// *oldest* undelivered message to make room for the new one.
///
/// Keep-newest is the whole point, not a detail. Every outbound class is
/// repaired by its *latest* instance — the current heartbeat, the current
/// `Accept` re-send, the current catch-up page — so under sustained overload
/// the messages worth delivering are the newest ones. The alternative,
/// refusing the new message while stale ones drain (what a bounded mpsc
/// `try_send` does), starves whole classes deterministically once a peer link
/// is slow: with a handful of slots and a delivery round trip of several
/// ticks, the slots free up once per round trip and refill with whatever is
/// enqueued first afterward, so a class that is always enqueued a beat later
/// than the heartbeat is dropped on every round trip, forever. That is an
/// adversary dropping every message of one kind, which defeats eventual
/// synchrony, and it is precisely how a lagging follower's catch-up responses
/// were lost for an entire quiet tail (sim seeds `14371623759479170018`,
/// `13938523914823716398`: 983 of 983 evicted at a 5-slot mailbox behind a
/// ~277 ms link).
///
/// Recency alone is not enough either: a leader re-sends *every* pending
/// `Accept` on every beat, so one beat's burst can fill a small mailbox by
/// itself and evict the single catch-up response enqueued just before it, on
/// every round trip (seeds `12153861921929631187`, `9558440018523712995`,
/// `1336557888375411500`, red on a plain keep-newest mailbox). So eviction is
/// **per kind**: a new message displaces the oldest queued message *of its own
/// kind* when one exists, and the oldest overall only when none does. A class
/// can then only be crowded out by itself — an `Accept` burst churns the
/// queued accepts, the current heartbeat replaces the stale one, the current
/// catch-up page replaces the previous — which is the same separation etcd
/// gets from carrying heartbeats and appends on distinct streams. The scan is
/// linear in the mailbox and runs only on overflow.
///
/// The mailbox also carries the two **drain-side** hook decisions
/// ([`DriverHooks::hold_peer_delivery`], [`DriverHooks::reverse_delivery_batch`]).
/// They are taken here, at enqueue time on the node loop, and merely *read* by
/// the delivery task, because a decision is a randomness draw and the delivery
/// task is `spawn_task(..).detach()`ed: a detached task's poll schedule is not
/// part of the simulation's deterministic step order the way the node loop is,
/// so drawing inside one lets a task that outlives its simulation shift the
/// *next* run's draw sequence. That is not a theoretical hazard — consulting
/// these two hooks from inside the delivery task broke
/// `same_seed_replays_identically` on CI (seed 42's first in-process replay
/// diverged from its second, on a run that was clean locally). Deciding on the
/// node loop restores the invariant every other hook already had: **simulation
/// randomness is drawn only where the simulation is stepping deterministically.**
#[derive(Clone)]
pub(crate) struct PeerMailbox {
    inner: Arc<Mutex<VecDeque<internal::ConsensusMessage>>>,
    capacity: usize,
    wake: Arc<tokio::sync::Notify>,
    /// Set by the enqueue side: the next drained batch waits one tick before
    /// it goes on the wire. Read-and-cleared by the delivery task.
    hold_next: Arc<AtomicBool>,
    /// Set by the enqueue side: the next drained batch is handed to the peer
    /// in reverse order. Read-and-cleared by the delivery task.
    reverse_next: Arc<AtomicBool>,
}

impl PeerMailbox {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "a peer mailbox holds at least one message");
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            wake: Arc::new(tokio::sync::Notify::new()),
            hold_next: Arc::new(AtomicBool::new(false)),
            reverse_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<internal::ConsensusMessage>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn is_full(&self) -> bool {
        self.lock().len() >= self.capacity
    }

    /// Enqueue `message`, evicting and returning one undelivered message when
    /// the mailbox is full: the oldest of the *same kind* as `message` if one
    /// is queued, else the oldest overall — unless `evict_across_kinds`, which
    /// takes the oldest overall outright. `overtake` enqueues at the front
    /// instead of the back. Both are the driver-hook perturbations
    /// ([`DriverHooks::overtake_in_mailbox`], [`DriverHooks::evict_across_kinds`]);
    /// production passes `false` for both. Never blocks.
    #[tracing::instrument(level = "trace", skip_all, fields(overtake, evict_across_kinds))]
    fn push(
        &self,
        message: internal::ConsensusMessage,
        overtake: bool,
        evict_across_kinds: bool,
    ) -> Option<internal::ConsensusMessage> {
        let evicted = {
            let mut queue = self.lock();
            let evicted = if queue.len() >= self.capacity {
                let kind = proto_message_kind(&message);
                let victim = if evict_across_kinds {
                    0
                } else {
                    queue
                        .iter()
                        .position(|queued| proto_message_kind(queued) == kind)
                        .unwrap_or(0)
                };
                queue.remove(victim)
            } else {
                None
            };
            if overtake {
                queue.push_front(message);
            } else {
                queue.push_back(message);
            }
            assert!(
                queue.len() <= self.capacity,
                "a peer mailbox never exceeds its capacity"
            );
            evicted
        };
        self.wake.notify_one();
        evicted
    }

    fn try_pop(&self) -> Option<internal::ConsensusMessage> {
        self.lock().pop_front()
    }

    /// Take the enqueue side's "hold the next drain" decision, clearing it.
    fn take_hold(&self) -> bool {
        self.hold_next.swap(false, Ordering::Relaxed)
    }

    /// Take the enqueue side's "reverse the next batch" decision, clearing it.
    fn take_reverse(&self) -> bool {
        self.reverse_next.swap(false, Ordering::Relaxed)
    }

    fn len(&self) -> usize {
        self.lock().len()
    }

    /// Wait for the next message. A `Notify` permit is stored when nobody is
    /// waiting, so a push that lands between the empty check and the await is
    /// never lost.
    async fn recv(&self) -> internal::ConsensusMessage {
        loop {
            if let Some(message) = self.try_pop() {
                return message;
            }
            self.wake.notified().await;
        }
    }
}

/// The driver's outbound side: every peer's mailboxes, every proxy leader's
/// mailbox (#142; empty on a deployment without proxies), and the sender's
/// identity for the observability events — a node, or a proxy leader running
/// [`run_proxy`](crate::run_proxy), which sends through exactly this handle.
/// Bundled so `drain_ready` takes one handle.
pub(crate) struct Outbound {
    pub(crate) peer_queues: BTreeMap<NodeId, PeerQueues>,
    /// The proxy leaders' regular mailboxes: a node delegates through them,
    /// and nothing bulky ever goes to a proxy (a proxy holds no snapshot).
    pub(crate) proxy_queues: BTreeMap<ProxyId, PeerMailbox>,
    /// Who is sending, for the observability events and the audit.
    pub(crate) sender: Party,
}

impl Outbound {
    /// The node this handle sends as. The node driver's own paths (the
    /// `Ready` drain, the snapshot repair plane) are the only callers, and
    /// they never run on a proxy.
    ///
    /// # Panics
    ///
    /// If the sender is a proxy leader — a programmer error, never an
    /// operating condition.
    pub(crate) fn self_node(&self) -> NodeId {
        self.sender
            .node()
            .expect("the node driver's paths send as a node, never as a proxy")
    }

    /// Report one send through the audit port: the callback is chosen by
    /// **who** sends to **whom**, so each keeps its own checks — a node's
    /// send, a node's delegation to a proxy, a proxy's fan-out or `Commit`.
    /// A proxy never addresses a proxy.
    fn report_sent<A: Audit>(&self, audit: &A, to: Party, msg: &Message) {
        match (self.sender, to) {
            (Party::Node(from), Party::Node(to)) => audit.sent(from, to, msg),
            (Party::Node(from), Party::Proxy(proxy)) => audit.delegation_sent(from, proxy, msg),
            (Party::Proxy(proxy), Party::Node(to)) => audit.proxy_sent(proxy, to, msg),
            (Party::Proxy(_), Party::Proxy(_)) => {
                unreachable!("a proxy leader never addresses a proxy leader")
            }
        }
    }

    /// The mailbox `to`'s copy of `msg` goes into, if the deployment map
    /// names `to` at all.
    fn mailbox_for(&self, to: Party, msg: &Message) -> Option<&PeerMailbox> {
        match to {
            Party::Node(node) => self.peer_queues.get(&node).map(|queues| {
                if matches!(msg, Message::InstallSnapshot { .. }) {
                    queues.snapshot.as_ref().unwrap_or(&queues.regular)
                } else {
                    &queues.regular
                }
            }),
            Party::Proxy(proxy) => self.proxy_queues.get(&proxy),
        }
    }

    /// Hand `msg` to the lossy per-peer transport and surface the protocol send.
    /// `msg_sent` deliberately records the core's outbound decision even when
    /// the bounded mailbox or network later drops it; safety oracles inspect the
    /// messages a proposer attempted, independently of delivery.
    #[tracing::instrument(level = "trace", skip_all, fields(from = %self.sender, to = %to, kind = message_kind(msg)))]
    pub(crate) fn transmit<H: DriverHooks, A: Audit>(
        &self,
        hooks: &H,
        audit: &A,
        to: Party,
        msg: &Message,
    ) {
        self.report_sent(audit, to, msg);
        let kind = message_kind(msg);
        // An `Accept` is the only message that carries a *proposal*, so it is the
        // only one whose command hash the trace needs: it is what lets an oracle
        // check the Phase-2 half of P2b — one ballot proposes at most one command
        // per slot — a claim no other event can show, because the anomaly it
        // guards against (#67) puts two commands for one `(ballot, slot)` on the
        // wire without either ever being accepted or chosen.
        match msg {
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => tracing::info!(
                from = %self.sender,
                to = %to,
                kind,
                bround = ballot.round,
                bnode = ballot.node.0,
                slot = slot.0,
                vhash = command_hash(command),
                "msg_sent"
            ),
            _ => match message_route(msg) {
                Some((_, ballot, Some(slot))) => tracing::info!(
                    from = %self.sender,
                    to = %to,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    slot = slot.0,
                    "msg_sent"
                ),
                // A beat from a leader whose chosen prefix is still empty: there is
                // no slot to report, and reporting a bare `0` would put back on the
                // trace exactly the sentinel #56 took off the wire.
                Some((_, ballot, None)) => tracing::info!(
                    from = %self.sender,
                    to = %to,
                    kind,
                    bround = ballot.round,
                    bnode = ballot.node.0,
                    "msg_sent"
                ),
                None => tracing::info!(from = %self.sender, to = %to, kind, "msg_sent"),
            },
        }
        if let Some(queue) = self.mailbox_for(to, msg) {
            let Ok(message) = message_to_proto(msg) else {
                tracing::warn!(
                    from = %self.sender,
                    to = %to,
                    "failed to encode Paxos message"
                );
                return;
            };
            // The mailbox's four decisions, all taken here on the node loop,
            // each consulted only where it can have an observable effect.
            //
            // Two act on this enqueue: overtake needs something already queued
            // to jump, evicting across kinds needs a full queue to evict from.
            let overtake = !queue.is_empty() && hooks.overtake_in_mailbox(to, msg);
            let evict_across_kinds = queue.is_full() && hooks.evict_across_kinds(to, msg);
            // Two arm the *drain*: this message's arrival is what makes the
            // next batch worth holding or reversing. Holding needs a queue that
            // is already non-empty (parking a drain of nothing changes
            // nothing); reversing needs at least two messages, since this one
            // plus a queued one is the smallest reorderable batch. Decided
            // here, applied there — see [`PeerMailbox`] for why the delivery
            // task must not draw.
            if !queue.is_empty() && hooks.hold_peer_delivery(to) {
                queue.hold_next.store(true, Ordering::Relaxed);
            }
            if !queue.is_empty() && hooks.reverse_delivery_batch(to) {
                queue.reverse_next.store(true, Ordering::Relaxed);
            }
            if let Some(evicted) = queue.push(message, overtake, evict_across_kinds) {
                // Deliberately lossy (etcd-style bounded mailbox, keep-newest),
                // but never silent: the audit sees the drop the moment it
                // happens, naming the *evicted* message, not the one that
                // displaced it.
                let evicted_kind = proto_message_kind(&evicted);
                audit.dropped_at_mailbox(self.sender, to, evicted_kind);
                tracing::debug!(
                    from = %self.sender,
                    to = %to,
                    kind = evicted_kind,
                    "evicted oldest Paxos message from a full peer gRPC mailbox"
                );
            }
        }
    }
}

/// Open one outbound lane toward `to`: a bounded keep-newest mailbox of
/// `capacity`, drained by a detached delivery task over `client`'s
/// reconnecting channel until `shutdown` fires. Shared by the node driver
/// (two lanes per peer, one per proxy) and the proxy driver (one per
/// acceptor): the lane is the same whoever sends through it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn open_lane<P: Providers, A: Audit + Clone + Send + Sync + 'static>(
    providers: &P,
    task: &'static str,
    client: ParosInternalClient<ReconnectingChannel<P, tonic::body::Body>>,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    audit: &A,
    from: Party,
    to: Party,
    capacity: usize,
) -> PeerMailbox {
    let mailbox = PeerMailbox::new(capacity);
    providers
        .task()
        .spawn_task(
            task,
            run_peer_delivery(
                client,
                providers.time().clone(),
                shutdown,
                mailbox.clone(),
                tunables,
                audit.clone(),
                from,
                to,
            ),
        )
        .detach();
    mailbox
}

/// Feed bounded unary batches over one reconnecting h2 channel per peer. While
/// a batch is in flight, new protocol messages accumulate for the next batch;
/// on failure Paxos heartbeats/resends repair anything lost with that RPC. A
/// batch is "in flight" only until the peer has *enqueued* it (its `Deliver`
/// acks on inbox entry, not after its loop stepped the messages), so the
/// `delivery_timeout` below races the connection and the peer's inbox
/// capacity — never the peer's persist-and-step time.
// The parameters are one delivery lane's complete wiring (client, clocks,
// lifecycle, queue, batch shape, and the audit identity for drop reports);
// a bundle would only rename the same eight things.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all, fields(from = %from, to = %to))]
pub(crate) async fn run_peer_delivery<P: Providers, A: Audit>(
    client: ParosInternalClient<ReconnectingChannel<P, tonic::body::Body>>,
    time: P::Time,
    shutdown: CancellationToken,
    messages: PeerMailbox,
    tunables: DriverTunables,
    audit: A,
    from: Party,
    to: Party,
) {
    let batch_limit = tunables.delivery_batch;
    let mut carried = None;
    loop {
        let first = if let Some(message) = carried.take() {
            message
        } else {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                message = messages.recv() => message,
            }
        };
        // Park the drain for one tick before batching. The mailbox keeps
        // filling while we wait, so the batcher below meets a real backlog
        // rather than the one-or-two messages a promptly drained queue holds —
        // the concurrency window an enqueue-time-only perturbation cannot
        // reach. The *decision* was taken on the node loop (see [`PeerMailbox`]
        // for why); this task only reads it.
        if messages.take_hold() {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                _ = time.sleep(tunables.tick_interval) => {}
            }
        }
        let mut attempt_client = client.clone();
        let (batch, next) = delivery_batch(first, &messages, batch_limit, &audit, from, to);
        carried = next;
        let outcome = moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = time.timeout(tunables.delivery_timeout, attempt_client.deliver(batch)) => result,
        };
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(status)) => {
                audit.delivery_failed(from, to);
                tracing::debug!(%status, "peer gRPC delivery failed");
            }
            Err(_) => {
                audit.delivery_failed(from, to);
                tracing::debug!("peer gRPC delivery timed out");
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip_all, fields(from = %from, to = %to))]
fn delivery_batch<A: Audit>(
    mut first: internal::ConsensusMessage,
    messages: &PeerMailbox,
    batch_limit: usize,
    audit: &A,
    from: Party,
    to: Party,
) -> (internal::Deliver, Option<internal::ConsensusMessage>) {
    // Do not spend the eventual-synchrony tail replaying a bounded but stale
    // stale chaos-era traffic. Peer delivery is allowed to lose messages; the
    // protocol's current heartbeat, Accept resend, and catch-up paths repair
    // them. Keep the newest batch so recovery signals can overtake old ballots
    // — the drain-side half of the mailbox's keep-newest policy (see
    // [`PeerMailbox`] for the enqueue-side half, which is what keeps a small
    // mailbox from starving a message class). The shed threshold stays at the
    // *default* batch depth even when the buggified `batch_limit` is smaller:
    // shedding detects a stale backlog, and tying it to a one-message batch
    // turns "drop stale traffic" into "drop everything but the newest message
    // on every drain" — a deterministic starvation of whole message classes
    // that no repair path can outrun (an adversary dropping every message of
    // one kind forever defeats eventual synchrony, which the knob's extreme
    // must not do).
    while messages.len() >= batch_limit.max(GRPC_DELIVERY_BATCH) {
        let Some(newer) = messages.try_pop() else {
            break;
        };
        // The stale head of the backlog is discarded, never silently: report
        // it at the instant of the drop, like the enqueue-side overflow.
        audit.dropped_at_mailbox(from, to, proto_message_kind(&first));
        tracing::debug!(
            from = %from,
            to = %to,
            "dropped stale Paxos message from delivery backlog"
        );
        first = newer;
    }
    let mut batch = Vec::with_capacity(batch_limit);
    let mut batch_bytes = first.encoded_len();
    batch.push(first);
    let mut carried = None;
    while batch.len() < batch_limit {
        let Some(message) = messages.try_pop() else {
            break;
        };
        if batch_bytes.saturating_add(message.encoded_len()) > GRPC_DELIVERY_BATCH_BYTES {
            carried = Some(message);
            break;
        }
        batch_bytes += message.encoded_len();
        batch.push(message);
    }
    // The drain-side reorder: the peer transport never promised ordering, and
    // reversing a whole batch is the shape a retried RPC or a re-established
    // stream produces. Decided on the node loop (see [`PeerMailbox`]), applied
    // only where it can have an effect.
    if batch.len() > 1 && messages.take_reverse() {
        batch.reverse();
    }
    (internal::Deliver { messages: batch }, carried)
}

/// Surface a hook-decided send drop (the `msg_dropped_at_send` trace and
/// [`Audit::dropped_at_send`]). An `Accept` names its slot so a trace shows
/// exactly which round the loss isolated.
pub(crate) fn trace_send_drop<A: Audit>(audit: &A, from: Party, to: Party, msg: &Message) {
    audit.dropped_at_send(from, to, msg);
    let kind = message_kind(msg);
    if let Message::Accept { slot, .. } = msg {
        tracing::info!(
            from = %from,
            to = %to,
            kind,
            slot = slot.0,
            "msg_dropped_at_send"
        );
    } else {
        tracing::info!(from = %from, to = %to, kind, "msg_dropped_at_send");
    }
}

/// Send one batch's addressed messages (fire-and-forget). The core addresses
/// each one; the driver maps a [`Party`] → address. Each message may be dropped
/// at this seam — per-message loss the network layer cannot produce on its own
/// (a TCP stream loses intervals, never one isolated message), with
/// `resend_pending` re-deriving what matters — or sent twice (retransmission
/// is legal transport behavior; set-based quorum counting must tolerate it).
#[tracing::instrument(level = "trace", skip_all, fields(from = %out.sender, messages = messages.len()))]
pub(crate) fn send_messages<H, A>(
    out: &Outbound,
    hooks: &H,
    audit: &A,
    messages: Vec<(Party, Message)>,
) where
    H: DriverHooks,
    A: Audit,
{
    let from = out.sender;
    for (to, msg) in messages {
        if hooks.drop_outgoing(to, &msg) {
            trace_send_drop(audit, from, to, &msg);
            continue;
        }
        out.transmit(hooks, audit, to, &msg);
        if hooks.duplicate_outgoing(to, &msg) {
            audit.duplicated_at_send(from, to, &msg);
            tracing::info!(
                from = %from,
                to = %to,
                kind = message_kind(&msg),
                "msg_duplicated_at_send"
            );
            out.transmit(hooks, audit, to, &msg);
        }
    }
}
