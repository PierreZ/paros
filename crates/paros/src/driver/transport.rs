//! The driver's outbound peer transport: the bounded, lossy, keep-newest
//! per-peer mailboxes, the [`Outbound`] handle the rest of the driver sends
//! through, and the detached delivery task that feeds bounded batches to each
//! peer's well-known `Deliver` endpoint.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_core::{
    Detach, Providers, SimulationError, SimulationResult, TaskProvider, TimeProvider,
};
use moonpool_rpc::{RpcHandle, ServiceClient};
use paros_core::{Audience, JournalIdentifier, Message, NodeId, Party, ProxyId};
use prost::Message as ProstMessage;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::rpc::methods::DeliverRpc;
use crate::rpc::{internal, message_to_proto, well_known};
use crate::{Address, Names};

use super::config::{DELIVERY_BATCH, DELIVERY_BATCH_BYTES, DriverTunables};
use super::events::{command_hash, message_kind, message_route, proto_message_kind};

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
/// **One lane per journal (#188).** A node runs several journals over one
/// mailbox per peer, and a busy journal must not evict another's messages:
/// each journal (the message's envelope id) gets its own keep-newest lane of
/// `capacity` messages, and the drain takes lanes **round-robin**, one
/// message per non-empty lane in journal order. The per-kind eviction rule
/// holds inside a lane; a journal is never crowded out by another.
///
/// The mailbox also carries the two **drain-side** BUGGIFY decisions (hold
/// the next batch a tick, reverse it), drawn in [`Outbound::transmit`].
/// They are taken here, at enqueue time on the node loop, and merely *read* by
/// the delivery task, because a decision is a randomness draw and the delivery
/// task is `spawn_task(..).detach()`ed: a detached task's poll schedule is not
/// part of the simulation's deterministic step order the way the node loop is,
/// so drawing inside one lets a task that outlives its simulation shift the
/// *next* run's draw sequence. That is not a theoretical hazard — drawing
/// these two decisions inside the delivery task broke
/// `same_seed_replays_identically` on CI (seed 42's first in-process replay
/// diverged from its second, on a run that was clean locally). Deciding on the
/// node loop restores the invariant every other site already has: **simulation
/// randomness is drawn only where the simulation is stepping deterministically.**
#[derive(Clone)]
pub(crate) struct PeerMailbox {
    inner: Arc<Mutex<Lanes>>,
    /// Each lane's capacity.
    capacity: usize,
    wake: Arc<tokio::sync::Notify>,
    /// Set by the enqueue side: the next drained batch waits one tick before
    /// it goes on the wire. Read-and-cleared by the delivery task.
    hold_next: Arc<AtomicBool>,
    /// Set by the enqueue side: the next drained batch is handed to the peer
    /// in reverse order. Read-and-cleared by the delivery task.
    reverse_next: Arc<AtomicBool>,
}

/// A mailbox's journal lanes and the round-robin cursor over them.
#[derive(Default)]
struct Lanes {
    /// The envelope's identifier `(tenant, journal)` (#235) → its keep-newest
    /// lane (`BTreeMap`: the drain order is part of determinism).
    by_journal: BTreeMap<(u64, u64), VecDeque<internal::ConsensusMessage>>,
    /// The identifier the next drain starts looking from.
    cursor: (u64, u64),
    /// Messages queued over every lane.
    total: usize,
}

impl Lanes {
    /// The running total is the sum of the lanes, and the round-robin cursor
    /// is a journal key.
    fn assert_invariants(&self) {
        assert!(
            self.total == self.by_journal.values().map(VecDeque::len).sum::<usize>(),
            "a mailbox's total counts every queued message once"
        );
    }

    /// Pop one message round-robin: the first non-empty lane at or after the
    /// cursor, wrapping; the cursor then moves past that lane.
    fn pop(&mut self) -> Option<internal::ConsensusMessage> {
        let journal = self
            .by_journal
            .range(self.cursor..)
            .chain(self.by_journal.range(..self.cursor))
            .find(|(_, lane)| !lane.is_empty())
            .map(|(journal, _)| *journal)?;
        let total = self.total;
        let message = self.by_journal.get_mut(&journal)?.pop_front()?;
        self.total -= 1;
        self.cursor = (journal.0, journal.1.saturating_add(1));
        // Fairness: the cursor moves strictly past the lane it served.
        assert!(
            self.cursor > journal,
            "the round-robin cursor moves past the served lane"
        );
        assert!(self.total + 1 == total, "a pop takes exactly one message");
        // Pair of the push-side keying: a lane holds only its own journal.
        assert!(
            (message.tenant, message.journal) == journal,
            "a lane holds only its own journal's messages"
        );
        self.assert_invariants();
        Some(message)
    }

    fn lane_len(&self, journal: (u64, u64)) -> usize {
        self.by_journal.get(&journal).map_or(0, VecDeque::len)
    }

    /// Shed every lane's stale head, **per lane** (#299): a lane that holds
    /// `threshold` messages or more loses its oldest until it holds
    /// `threshold - 1`. `held` is the lane of the message the drain already
    /// took; that message counts in its lane as the oldest, so it goes first.
    /// Returns the kinds shed from the lanes, and whether the held message
    /// is stale too. A lane under the threshold loses nothing, so a sparse
    /// journal's message survives however dense its neighbours are.
    fn shed_stale(&mut self, threshold: usize, held: (u64, u64)) -> (Vec<&'static str>, bool) {
        assert!(threshold > 1, "a shed lane keeps at least one message");
        let total = self.total;
        let mut shed = Vec::new();
        let mut held_stale = false;
        for (journal, lane) in &mut self.by_journal {
            let before = lane.len();
            let mut count = before + usize::from(*journal == held);
            if count < threshold {
                continue;
            }
            if *journal == held {
                held_stale = true;
                count -= 1;
            }
            while count >= threshold {
                let message = lane
                    .pop_front()
                    .expect("a lane over the threshold is not empty");
                shed.push(proto_message_kind(&message));
                count -= 1;
            }
            // A shed lane keeps exactly its newest `threshold - 1` messages.
            assert!(
                lane.len() == threshold - 1,
                "a shed lane keeps its newest messages"
            );
            // A shed lane loses its oldest message: the held one, or its
            // own front (the held one alone when the lane held exactly
            // `threshold - 1` behind it).
            assert!(
                lane.len() < before || *journal == held,
                "a shed lane loses its oldest message"
            );
        }
        self.total -= shed.len();
        assert!(
            self.total + shed.len() == total,
            "shedding counts every dropped message"
        );
        // Negative space: no lane is left at or over the threshold.
        assert!(
            self.by_journal.values().all(|lane| lane.len() < threshold),
            "no lane stays at or over the shed threshold"
        );
        // A stale held message leaves its lane non-empty: the drain has a
        // newer message of the same journal to send instead.
        assert!(
            !held_stale || self.lane_len(held) > 0,
            "a stale held message has a newer one behind it"
        );
        self.assert_invariants();
        (shed, held_stale)
    }
}

impl PeerMailbox {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "a peer mailbox holds at least one message");
        Self {
            inner: Arc::new(Mutex::new(Lanes::default())),
            capacity,
            wake: Arc::new(tokio::sync::Notify::new()),
            hold_next: Arc::new(AtomicBool::new(false)),
            reverse_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lanes> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_empty(&self) -> bool {
        self.lock().total == 0
    }

    /// Whether `journal`'s lane is full (the next push into it evicts).
    fn is_full(&self, journal: (u64, u64)) -> bool {
        let len = self.lock().lane_len(journal);
        assert!(len <= self.capacity, "a lane never exceeds its capacity");
        len == self.capacity
    }

    /// Enqueue `message`, evicting and returning one undelivered message when
    /// the mailbox is full: the oldest of the *same kind* as `message` if one
    /// is queued, else the oldest overall — unless `evict_across_kinds`, which
    /// takes the oldest overall outright. `overtake` enqueues at the front
    /// instead of the back. Both are BUGGIFY perturbations drawn in
    /// [`Outbound::transmit`]; production passes `false` for both. Never
    /// blocks.
    #[tracing::instrument(level = "trace", skip_all, fields(overtake, evict_across_kinds))]
    fn push(
        &self,
        message: internal::ConsensusMessage,
        overtake: bool,
        evict_across_kinds: bool,
    ) -> Option<internal::ConsensusMessage> {
        let evicted = {
            let mut lanes = self.lock();
            let journal = (message.tenant, message.journal);
            let lanes = &mut *lanes;
            let queue = lanes.by_journal.entry(journal).or_default();
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
            if evicted.is_none() {
                lanes.total += 1;
            }
            // Eviction is per lane (#188): only a full lane evicts, and only
            // its own journal's message — never another journal's.
            if let Some(victim) = &evicted {
                assert!(
                    lanes.lane_len(journal) == self.capacity,
                    "only a full lane evicts"
                );
                assert!(
                    (victim.tenant, victim.journal) == journal,
                    "a lane evicts only its own journal's message"
                );
            }
            lanes.assert_invariants();
            evicted
        };
        self.wake.notify_one();
        evicted
    }

    fn try_pop(&self) -> Option<internal::ConsensusMessage> {
        self.lock().pop()
    }

    /// Take the enqueue side's "hold the next drain" decision, clearing it.
    fn take_hold(&self) -> bool {
        self.hold_next.swap(false, Ordering::Relaxed)
    }

    /// Take the enqueue side's "reverse the next batch" decision, clearing it.
    fn take_reverse(&self) -> bool {
        self.reverse_next.swap(false, Ordering::Relaxed)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().total
    }

    /// See [`Lanes::shed_stale`].
    fn shed_stale(&self, threshold: usize, held: (u64, u64)) -> (Vec<&'static str>, bool) {
        self.lock().shed_stale(threshold, held)
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
    /// Every peer's lane, by node: one lane carries every class, since
    /// nothing bulky travels between peers (#186: a laggard below the trim
    /// point gets a `TrimmedTo`, never bytes). Behind a lock only so a lane
    /// can be added while the loop holds the handle shared (#189: a node the
    /// registry admits at runtime); the loop is the only writer and never
    /// holds it across an await.
    peer_queues: Mutex<BTreeMap<NodeId, PeerMailbox>>,
    /// The proxy leaders' mailboxes: a node delegates through them.
    pub(crate) proxy_queues: BTreeMap<ProxyId, PeerMailbox>,
    /// The deployment's replicas (#144): learners that are not in the node
    /// pool, reached by every [`Audience::Learners`] message beside the pool
    /// and addressed by their own `NodeId` (a catch-up answer, a trim
    /// point). Their lanes are in `peer_queues`. Empty on a deployment
    /// without a replica tier, and on a replica itself — a replica sends to
    /// no learner.
    pub(crate) learners: Vec<NodeId>,
    /// Who is sending, for the observability events and the audit.
    pub(crate) sender: Party,
}

impl Outbound {
    /// A handle over `peer_queues`, sending as `sender`.
    pub(crate) fn new(
        peer_queues: BTreeMap<NodeId, PeerMailbox>,
        proxy_queues: BTreeMap<ProxyId, PeerMailbox>,
        learners: Vec<NodeId>,
        sender: Party,
    ) -> Self {
        Self {
            peer_queues: Mutex::new(peer_queues),
            proxy_queues,
            learners,
            sender,
        }
    }

    /// Whether a lane to `node` exists.
    pub(crate) fn has_peer(&self, node: NodeId) -> bool {
        self.peers().contains_key(&node)
    }

    /// Add a lane to `node` (#189: a node the registry admitted at runtime).
    /// A node already reachable keeps its lane.
    pub(crate) fn add_peer(&self, node: NodeId, lane: PeerMailbox) {
        // A node never opens a lane to itself.
        if let Party::Node(me) = self.sender {
            assert!(node != me, "a node opens no lane to itself");
        }
        self.peers().entry(node).or_insert(lane);
    }

    fn peers(&self) -> std::sync::MutexGuard<'_, BTreeMap<NodeId, PeerMailbox>> {
        self.peer_queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Resolve `audience` as this handle sends it: the core's own resolution
    /// against the node `pool` ([`Audience::resolve`] for a node, which never
    /// addresses itself; [`Audience::resolve_from_proxy`] for a proxy, which
    /// is nobody's peer), then — for [`Audience::Learners`] — the
    /// deployment's replicas, which the pool does not name.
    pub(crate) fn resolve(&self, audience: &Audience, pool: &[NodeId]) -> Vec<NodeId> {
        let mut nodes = match self.sender {
            Party::Node(me) => audience.resolve(pool, me),
            Party::Proxy(_) => audience.resolve_from_proxy(pool),
        };
        if *audience == Audience::Learners {
            nodes.extend(self.learners.iter().copied());
        }
        // A fan-out never loops back to its sender.
        if let (Party::Node(me), false) = (self.sender, matches!(audience, Audience::Node(_))) {
            assert!(!nodes.contains(&me), "a fan-out never addresses its sender");
        }
        if audience.proxy().is_some() {
            assert!(
                nodes.is_empty(),
                "a proxy audience resolves through the proxy lanes"
            );
        }
        nodes
    }

    /// The node this handle sends as. The node driver's own paths (the
    /// `Ready` drain) are the only callers, and they never run on a proxy.
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
    /// send to a node, a node's send to a proxy (a leader's delegation *or*
    /// an acceptor's `Accepted` / `Nack` reply to a delegated round), a
    /// proxy's fan-out or `Commit`. A proxy never addresses a proxy.
    fn report_sent<A: Audit>(&self, audit: &A, to: Party, msg: &Message) {
        match (self.sender, to) {
            (Party::Node(from), Party::Node(to)) => audit.sent(from, to, msg),
            (Party::Node(from), Party::Proxy(proxy)) => audit.sent_to_proxy(from, proxy, msg),
            (Party::Proxy(proxy), Party::Node(to)) => audit.proxy_sent(proxy, to, msg),
            (Party::Proxy(_), Party::Proxy(_)) => {
                unreachable!("a proxy leader never addresses a proxy leader")
            }
        }
    }

    /// The mailbox `to`'s copy of `msg` goes into, if the deployment map
    /// names `to` at all.
    fn mailbox_for(&self, to: Party) -> Option<PeerMailbox> {
        match to {
            Party::Node(node) => self.peers().get(&node).cloned(),
            Party::Proxy(proxy) => self.proxy_queues.get(&proxy).cloned(),
        }
    }

    /// Hand `msg` to the lossy per-peer transport and surface the protocol send.
    /// The send is reported through the audit port first
    /// (`report_sent` → [`Audit::sent`] and its siblings),
    /// so it records the core's outbound decision even when the bounded mailbox
    /// or network later drops it: the safety oracles fold the messages a
    /// proposer attempted, independently of delivery. The `msg_sent` trace
    /// event is the human-readable mirror; nothing reads it back.
    #[tracing::instrument(level = "trace", skip_all, fields(from = %self.sender, to = %to, kind = message_kind(msg)))]
    pub(crate) fn transmit<A: Audit>(
        &self,
        audit: &A,
        journal: JournalIdentifier,
        to: Party,
        msg: &Message,
    ) {
        self.report_sent(audit, to, msg);
        let kind = message_kind(msg);
        // An `Accept` is the only message that carries a *proposal*, so it is the
        // only one whose command hash the trace shows: it lets a human reading
        // the trace see the Phase-2 half of P2b — one ballot proposes at most one
        // command per slot — which the audit checks from the report above,
        // because the anomaly it guards against (#67) puts two commands for one
        // `(ballot, slot)` on the wire without either ever being accepted or
        // chosen.
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
        if let Some(queue) = self.mailbox_for(to) {
            let Ok(mut message) = message_to_proto(msg) else {
                tracing::warn!(
                    from = %self.sender,
                    to = %to,
                    "failed to encode Paxos message"
                );
                return;
            };
            // The envelope (#188), named with its tenant (#235): the
            // receiver demuxes on the pair.
            message.tenant = journal.tenant.0;
            message.journal = journal.journal.0;
            // The mailbox's four decisions, each its own BUGGIFY location,
            // all drawn here on the node loop, each only where it can have an
            // observable effect, and silent in the recovery tail.
            //
            // Two act on this enqueue. Overtake needs something already
            // queued to jump: a per-peer stream is otherwise delivered in
            // enqueue order, so this is the only in-stream reorder. Evicting
            // across kinds needs a full queue to evict from; kept occasional,
            // because a systematic cross-kind eviction is the starvation the
            // per-kind default exists to prevent.
            let overtake = !queue.is_empty() && moonpool_buggify::buggify_fault_with_prob!(0.02);
            if overtake {
                moonpool_assertions::reachable!("mailbox: a message overtakes its peer queue");
            }
            let evict_across_kinds = queue.is_full((journal.tenant.0, journal.journal.0))
                && moonpool_buggify::buggify_fault_with_prob!(0.10);
            if evict_across_kinds {
                moonpool_assertions::reachable!("mailbox: overflow evicts across kinds");
            }
            // Two arm the *drain*: this message's arrival is what makes the
            // next batch worth holding or reversing. Holding needs a queue that
            // is already non-empty (parking a drain of nothing changes
            // nothing); reversing needs at least two messages, since this one
            // plus a queued one is the smallest reorderable batch. Decided
            // here, applied there — see [`PeerMailbox`] for why the delivery
            // task must not draw.
            //
            // Both arms are latches, so a node that enqueues a dozen messages
            // in one tick draws a dozen times and the arms collapse into one:
            // the per-drain rate is far above the per-call one. Holding most
            // drains would halve per-peer throughput (a partition, moonpool's
            // job), and reversing most batches would make the stream
            // systematically backwards. One tick per hold bounds the backlog
            // one hold builds to one tick's traffic: enough to cross the shed
            // threshold, never enough to wedge a link.
            if !queue.is_empty() && moonpool_buggify::buggify_fault_with_prob!(0.01) {
                moonpool_assertions::reachable!("mailbox: a peer drain is held for a tick");
                queue.hold_next.store(true, Ordering::Relaxed);
            }
            if !queue.is_empty() && moonpool_buggify::buggify_fault_with_prob!(0.01) {
                moonpool_assertions::reachable!("mailbox: a delivery batch is reversed");
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
                    "evicted oldest Paxos message from a full peer mailbox"
                );
            }
        }
    }
}

/// A peer's address of a deployment-map entry: `HOST:PORT`, a literal or a
/// name resolved at dial time (#257).
///
/// # Errors
///
/// An entry that is not `HOST:PORT`.
pub(crate) fn peer_target(addr: &str) -> SimulationResult<Address> {
    Address::parse(addr)
        .map_err(|e| SimulationError::InvalidState(format!("bad peer address {addr}: {e}")))
}

/// How one lane reaches its peer (#257): the peer's address, resolved at
/// dial time through the lane's [`Names`], and the client of the address it
/// resolved to. A failed delivery to a name forgets that address, so the
/// next batch resolves the name again (FDB's `removeCachedDNS`): a peer
/// whose IP changed behind its name is reached again with no write.
struct Dialer<P: Providers> {
    rpc: RpcHandle<P>,
    names: Names,
    address: Address,
    /// The address the name resolved to last, and its client.
    resolved: Option<(SocketAddr, ServiceClient<P, DeliverRpc>)>,
    /// The last address the name resolved to, kept across a forget: a
    /// re-resolution that lands elsewhere is a peer that moved.
    last: Option<SocketAddr>,
    /// The name resolved elsewhere and no delivery reached the new address
    /// yet.
    moved: bool,
}

impl<P: Providers> Dialer<P> {
    fn new(rpc: &RpcHandle<P>, names: Names, address: Address) -> Self {
        Self {
            rpc: rpc.clone(),
            names,
            address,
            resolved: None,
            last: None,
            moved: false,
        }
    }

    /// The client of the peer's current address: the one resolved last, or
    /// a fresh resolution. `None` while the name does not resolve.
    async fn client(&mut self) -> Option<ServiceClient<P, DeliverRpc>> {
        if let Some((_, client)) = &self.resolved {
            return Some(client.clone());
        }
        let addr = match self.names.resolve(&self.address).await {
            Ok(addr) => addr,
            Err(error) => {
                tracing::debug!(address = %self.address, %error, "peer_name_unresolved");
                return None;
            }
        };
        if self.last.is_some_and(|last| last != addr) {
            moonpool_assertions::reachable!(
                "transport: a peer re-resolved a name to a new address"
            );
            tracing::info!(address = %self.address, %addr, "peer_address_moved");
            self.moved = true;
        }
        self.last = Some(addr);
        let client: ServiceClient<P, DeliverRpc> = well_known(&self.rpc, addr);
        self.resolved = Some((addr, client.clone()));
        Some(client)
    }

    /// A delivery succeeded: a peer that moved is reached at its new
    /// address.
    fn delivered(&mut self) {
        assert!(
            self.resolved.is_some(),
            "a delivery goes to a resolved address"
        );
        if self.moved {
            self.moved = false;
            moonpool_assertions::reachable!(
                "transport: a peer reached a moved peer at its new address"
            );
        }
    }

    /// A delivery failed: a name is resolved again before the next batch;
    /// a literal address stays as it is.
    fn failed(&mut self) {
        if self.address.literal().is_none() {
            self.resolved = None;
        }
    }
}

/// The resolved address of a deployment-map entry (`ip:port`).
///
/// # Errors
///
/// An entry that is not a numeric socket address.
pub(crate) fn peer_address(addr: &str) -> SimulationResult<SocketAddr> {
    addr.parse()
        .map_err(|e| SimulationError::InvalidState(format!("bad peer address {addr}: {e}")))
}

/// What every outbound lane a driver opens shares: the providers it spawns
/// on, the tunables that shape it, the incarnation's shutdown, the audit the
/// delivery task reports drops to, and who is sending. Shared by the node
/// driver (one lane per peer and per proxy), the proxy driver (one per
/// acceptor and replica) and the replica driver (one per acceptor): the lane
/// is the same whoever sends through it.
pub(crate) struct LaneOpener<'a, P: Providers, A: Audit> {
    pub(crate) providers: &'a P,
    pub(crate) tunables: DriverTunables,
    pub(crate) shutdown: CancellationToken,
    pub(crate) audit: &'a A,
    pub(crate) from: Party,
    /// The sender's cell, stamped on every batch (`0` without a cell plan).
    pub(crate) cell_id: u64,
    /// How the lanes resolve their peers' addresses (#257):
    /// [`Names::literal`] where every address is literal.
    pub(crate) names: Names,
}

impl<P: Providers, A: Audit + Clone + Send + Sync + 'static> LaneOpener<'_, P, A> {
    /// Open one outbound lane toward `to` at `addr`: a bounded keep-newest
    /// mailbox of the tunables' `peer_queue_capacity`, drained by a detached
    /// delivery task (named `task`) over a reconnecting channel on `rpc`
    /// until the incarnation's shutdown fires. A name in `addr` is resolved
    /// at dial time, and again after a failed delivery (#257).
    pub(crate) fn open(
        &self,
        rpc: &RpcHandle<P>,
        task: &'static str,
        addr: Address,
        to: Party,
    ) -> PeerMailbox {
        let dialer = Dialer::new(rpc, self.names.clone(), addr);
        let mailbox = PeerMailbox::new(self.tunables.peer_queue_capacity);
        self.providers
            .task()
            .spawn_task(
                task,
                run_peer_delivery(
                    dialer,
                    self.providers.time().clone(),
                    self.shutdown.clone(),
                    mailbox.clone(),
                    self.tunables,
                    self.audit.clone(),
                    self.from,
                    self.cell_id,
                    to,
                ),
            )
            .detach();
        mailbox
    }

    /// Open one lane per `(id, address)` of `peers` ([`LaneOpener::open`]),
    /// each addressed as `party(id)` — the per-peer lanes a driver builds at
    /// boot. An address that is not `HOST:PORT` fails the whole set.
    pub(crate) fn open_all<I: Copy + Ord>(
        &self,
        rpc: &RpcHandle<P>,
        task: &'static str,
        peers: impl IntoIterator<Item = (I, String)>,
        party: impl Fn(I) -> Party,
    ) -> SimulationResult<BTreeMap<I, PeerMailbox>> {
        peers
            .into_iter()
            .map(|(id, addr)| Ok((id, self.open(rpc, task, peer_target(&addr)?, party(id)))))
            .collect()
    }
}

/// Feed bounded batches to one peer's `Deliver` endpoint, one at-most-once
/// attempt each (the runtime re-dials a lost connection on its own). While
/// a batch is in flight, new protocol messages accumulate for the next batch;
/// on failure Paxos heartbeats/resends repair anything lost with that RPC. A
/// batch is "in flight" only until the peer has *enqueued* it (its `Deliver`
/// acks on inbox entry, not after its loop stepped the messages), so the
/// `delivery_timeout` below races the connection and the peer's inbox
/// capacity — never the peer's persist-and-step time.
// The parameters are one delivery lane's complete wiring (client, clocks,
// lifecycle, queue, batch shape, and the sender's identity: its party for
// drop reports and its cell for the batch); a bundle would only rename the
// same nine things.
// The lane is a task of its own: it owns its copy of the tunables.
#[allow(clippy::too_many_arguments, clippy::large_types_passed_by_value)]
#[tracing::instrument(level = "debug", skip_all, fields(from = %from, to = %to))]
async fn run_peer_delivery<P: Providers, A: Audit>(
    mut dialer: Dialer<P>,
    time: P::Time,
    shutdown: CancellationToken,
    messages: PeerMailbox,
    tunables: DriverTunables,
    audit: A,
    from: Party,
    cell_id: u64,
    to: Party,
) {
    let batch_limit = tunables.delivery_batch;
    assert!(
        batch_limit > 0,
        "a delivery batch carries at least one message"
    );
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
        let (mut batch, next) = delivery_batch(first, &messages, batch_limit, &audit, from, to);
        carried = next;
        assert_eq!(batch.cell_id, 0, "a batch is stamped once, here");
        batch.cell_id = cell_id;
        let client = moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            client = dialer.client() => client,
        };
        let Some(client) = client else {
            // The peer's name does not resolve (yet): the batch is lost, as
            // on a failed delivery, and Paxos resends what matters.
            audit.delivery_failed(from, to);
            continue;
        };
        let outcome = moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = client.try_get_reply_within(&batch, tunables.delivery_timeout) => result,
        };
        match outcome {
            Ok(_) => dialer.delivered(),
            Err(error) => {
                audit.delivery_failed(from, to);
                dialer.failed();
                tracing::debug!(%error, "peer delivery failed");
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
    // Do not spend the eventual-synchrony tail replaying a bounded but
    // stale chaos-era backlog. Peer delivery is allowed to lose messages; the
    // protocol's current heartbeat, Accept resend, and catch-up paths repair
    // them. Keep the newest messages so recovery signals can overtake old
    // ballots — the drain-side half of the mailbox's keep-newest policy (see
    // [`PeerMailbox`] for the enqueue-side half, which is what keeps a small
    // mailbox from starving a message class). The shed threshold stays at the
    // *default* batch depth even when the buggified `batch_limit` is smaller:
    // shedding detects a stale backlog, and tying it to a one-message batch
    // turns "drop stale traffic" into "drop everything but the newest message
    // on every drain" — a deterministic starvation of whole message classes
    // that no repair path can outrun (an adversary dropping every message of
    // one kind forever defeats eventual synchrony, which the knob's extreme
    // must not do).
    //
    // The shed is **per lane** (#299): a backlog is stale per journal. A shed
    // judged on the total over every lane let two dense heartbeat lanes keep
    // the total over the threshold, and the round-robin throw-away then
    // reached a sparse lane within its first pops and dropped its one message
    // on every drain: a follower's every `CatchUpRequest` died at the mailbox
    // and it never caught up (witness seed 1791023425762573528 on 9371ab2:
    // 663 sent on link 2→0, every one dropped, node 2 stuck two slots behind
    // through a 60 s tail).
    let threshold = batch_limit.max(DELIVERY_BATCH);
    let held = (first.tenant, first.journal);
    let (shed, held_stale) = messages.shed_stale(threshold, held);
    // The stale heads are discarded, never silently: report each at the
    // instant of the drop, oldest first, like the enqueue-side overflow.
    if held_stale {
        audit.dropped_at_mailbox(from, to, proto_message_kind(&first));
        first = messages
            .try_pop()
            .expect("a stale held message has a newer one behind it");
    }
    for kind in shed {
        audit.dropped_at_mailbox(from, to, kind);
        tracing::debug!(
            from = %from,
            to = %to,
            kind,
            "dropped stale Paxos message from delivery backlog"
        );
    }
    let mut batch = Vec::with_capacity(batch_limit);
    let mut batch_bytes = first.encoded_len();
    batch.push(first);
    let mut carried = None;
    while batch.len() < batch_limit {
        let Some(message) = messages.try_pop() else {
            break;
        };
        if batch_bytes.saturating_add(message.encoded_len()) > DELIVERY_BATCH_BYTES {
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
    // A batch is never empty, never longer than its limit, and fits a frame
    // unless it is one oversized message on its own.
    assert!(!batch.is_empty(), "a delivery batch carries a message");
    assert!(
        batch.len() <= batch_limit.max(1),
        "a delivery batch honours its limit"
    );
    if batch.len() > 1 {
        assert!(
            batch_bytes <= DELIVERY_BATCH_BYTES,
            "a multi-message batch fits its byte budget"
        );
    }
    // A carried message is the one that would have overflowed the budget:
    // the batch stopped short of its limit for bytes, not for count.
    if let Some(next) = &carried {
        assert!(
            batch.len() < batch_limit,
            "a carried message left room by count"
        );
        assert!(
            batch_bytes.saturating_add(next.encoded_len()) > DELIVERY_BATCH_BYTES,
            "a message is carried only for bytes"
        );
    }
    (
        internal::Deliver {
            messages: batch,
            cell_id: 0,
        },
        carried,
    )
}

/// Whether to drop this one outbound protocol message after it is durable
/// but before it reaches the transport. One location per kind family, one
/// macro line per arm, silent in the recovery tail.
///
/// Always safe: the network could lose the same message, and every protocol
/// path tolerates that loss (`resend_pending` re-derives what still
/// matters). Unlike moonpool's connection-level faults, this reaches
/// per-message loss: for example, one isolated `Accept` for an earlier slot
/// vanishes while later slots land, the interleaving behind a stranded
/// chosen-gap wedge.
fn drop_at_send(msg: &Message) -> bool {
    match msg {
        // An isolated `Accept` loss is the interleaving behind a stranded
        // chosen-gap wedge (#80): one earlier slot's Accept vanishes while
        // later slots land.
        Message::Accept { .. } => moonpool_buggify::buggify_fault_with_prob!(0.05),
        // A lost `Promise`/`Prepare` stretches an election open.
        Message::Prepare { .. } | Message::Promise { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.10)
        }
        // A lost `Nack` keeps a below-floor candidate's campaign alive long
        // enough for the answering trim point to land mid-election (the #88
        // window).
        Message::Nack { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        // A dropped `Commit` delays a follower's floor raise, widening the
        // mixed-floor window the #88 mid-election trim jump needs, and
        // leaves the hole commit-replay catch-up must heal (#80).
        Message::Commit { .. } => moonpool_buggify::buggify_fault_with_prob!(0.05),
        // The lost ack: a slot durably accepted by a quorum whose proposer
        // never learns it, which forces a re-propose under a new ballot.
        Message::Accepted { .. } => moonpool_buggify::buggify_fault_with_prob!(0.05),
        // Starve the `CheckQuorum` window and the catch-up push direction.
        // Kept low: these fire per tick per peer, and a high rate is a
        // partition, which is moonpool's job.
        Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.02)
        }
        // Repair traffic for a node that is already behind: a lost response
        // costs one beat and re-derives on the next.
        Message::TrimmedTo { .. } | Message::CatchUpResponse { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.10)
        }
        // The pull direction of catch-up: the next tick re-asks.
        Message::CatchUpRequest { .. } => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // The whole handoff, lost in one message. It must cost availability
        // only: the outgoing leader already stepped down, so an ordinary
        // Phase 1 elects the next one. Aggressive, because that fallback must
        // always work.
        Message::Relinquish { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        _ => false,
    }
}

/// Whether to send this one outbound protocol message twice. One location
/// per kind family, one macro line per arm, silent in the recovery tail.
///
/// Always safe: retransmission is legal transport behavior on any
/// reconnecting link, and every quorum in the core is set-based, so a
/// duplicate must be harmless. These locations keep it that way: a quorum
/// counter "optimized" into an integer would let a duplicated `Accepted`
/// fabricate a quorum. Moonpool has no message-duplication fault.
fn duplicate_at_send(msg: &Message) -> bool {
    match msg {
        // The quorum-counting kinds are the point of the location.
        Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.05)
        }
        Message::Commit { .. } => moonpool_buggify::buggify_fault_with_prob!(0.05),
        // A duplicated trim point must be a no-op the second time (the jump
        // refuses a point at or below its floor).
        Message::TrimmedTo { .. } | Message::CatchUpResponse { .. } => {
            moonpool_buggify::buggify_fault_with_prob!(0.10)
        }
        // A duplicated catch-up request must only cost a redundant reply.
        Message::CatchUpRequest { .. } => moonpool_buggify::buggify_fault_with_prob!(0.10),
        // A re-delivered handoff must be a no-op at its addressee (never an
        // allocator rewind) and refused everywhere else.
        Message::Relinquish { .. } => moonpool_buggify::buggify_fault_with_prob!(0.25),
        _ => false,
    }
}

/// Surface a send drop (the `msg_dropped_at_send` trace and
/// [`Audit::dropped_at_send`]). An `Accept` names its slot so a trace shows
/// exactly which round the loss isolated.
fn trace_send_drop<A: Audit>(audit: &A, from: Party, to: Party, msg: &Message) {
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
/// at this seam ([`drop_at_send`]) — per-message loss the network layer cannot
/// produce on its own (a TCP stream loses intervals, never one isolated
/// message), with `resend_pending` re-deriving what matters — or sent twice
/// ([`duplicate_at_send`]: retransmission is legal transport behavior;
/// set-based quorum counting must tolerate it). Both are drawn here, on the
/// node loop.
#[tracing::instrument(level = "trace", skip_all, fields(from = %out.sender, messages = messages.len()))]
pub(crate) fn send_messages<A: Audit>(
    out: &Outbound,
    audit: &A,
    journal: JournalIdentifier,
    messages: Vec<(Party, Message)>,
) {
    let from = out.sender;
    for (to, msg) in messages {
        if drop_at_send(&msg) {
            trace_send_drop(audit, from, to, &msg);
            continue;
        }
        out.transmit(audit, journal, to, &msg);
        if duplicate_at_send(&msg) {
            audit.duplicated_at_send(from, to, &msg);
            tracing::info!(
                from = %from,
                to = %to,
                kind = message_kind(&msg),
                "msg_duplicated_at_send"
            );
            out.transmit(audit, journal, to, &msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A heartbeat of `journal`, as the wire carries it.
    fn beat(journal: u64) -> internal::ConsensusMessage {
        let mut message = message_to_proto(&Message::HeartbeatAck {
            from: NodeId(1),
            ballot: paros_core::Ballot::zero(),
            chosen: None,
        })
        .expect("a heartbeat ack encodes");
        message.tenant = 256;
        message.journal = journal;
        message
    }

    /// #188's fair lanes, pinned at the mechanism: a journal that floods its
    /// lane evicts only its own messages, and the drain alternates between
    /// journals instead of emptying the busy one first.
    #[test]
    fn a_busy_journal_never_evicts_another_journals_messages() {
        let mailbox = PeerMailbox::new(2);
        assert!(mailbox.push(beat(129), false, false).is_none());
        for _ in 0..10 {
            let evicted = mailbox.push(beat(128), false, false);
            if let Some(evicted) = evicted {
                assert_eq!(evicted.journal, 128, "only the busy journal's lane evicts");
            }
        }
        assert!(mailbox.is_full((256, 128)));
        assert!(!mailbox.is_full((256, 129)));
        assert_eq!(mailbox.len(), 3);
        let order: Vec<u64> = std::iter::from_fn(|| mailbox.try_pop())
            .map(|m| m.journal)
            .collect();
        assert_eq!(
            order,
            vec![128, 129, 128],
            "the drain takes lanes round-robin"
        );
        assert!(mailbox.is_empty());
    }

    /// #299's per-lane shed, pinned at the mechanism: two dense lanes over
    /// the threshold and one single-message lane. The dense lanes keep
    /// exactly their newest messages; the single message survives.
    #[test]
    fn the_shed_never_drops_a_sparse_journals_message() {
        let threshold = 4;
        let mailbox = PeerMailbox::new(16);
        for _ in 0..10 {
            assert!(mailbox.push(beat(128), false, false).is_none());
            assert!(mailbox.push(beat(130), false, false).is_none());
        }
        assert!(mailbox.push(beat(129), false, false).is_none());
        // The drain holds a message of a dense lane: it is the oldest there.
        let (shed, held_stale) = mailbox.shed_stale(threshold, (256, 128));
        assert!(held_stale, "the held message is the oldest of a dense lane");
        assert_eq!(shed.len(), (10 + 1 - 3) - 1 + (10 - 3));
        assert_eq!(mailbox.lock().lane_len((256, 128)), threshold - 1);
        assert_eq!(mailbox.lock().lane_len((256, 130)), threshold - 1);
        assert_eq!(
            mailbox.lock().lane_len((256, 129)),
            1,
            "a lane under the threshold loses nothing"
        );
        // A second shed finds nothing stale.
        let (shed, held_stale) = mailbox.shed_stale(threshold, (256, 129));
        assert!(shed.is_empty() && !held_stale);
        // A lane one short of the threshold plus the held message: only the
        // held message is stale, and the lane keeps every message.
        let (shed, held_stale) = mailbox.shed_stale(threshold, (256, 130));
        assert!(shed.is_empty() && held_stale);
        assert_eq!(mailbox.lock().lane_len((256, 130)), threshold - 1);
        let order: Vec<u64> = std::iter::from_fn(|| mailbox.try_pop())
            .map(|m| m.journal)
            .collect();
        assert_eq!(order.iter().filter(|&&j| j == 129).count(), 1);
        assert_eq!(order.len(), 2 * (threshold - 1) + 1);
    }
}
