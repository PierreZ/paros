//! **The batcher** (#145, `docs/architecture/02-data-plane.md` §2.4,
//! `docs/architecture/04-roles.md`): the stateless role that merges many
//! writers' `Write` calls of one multi-writer journal into one slot, and
//! hands each write its own `seq` range back.
//!
//! A batcher serves `BatchWrite` (`0x5041_0501`): the plain `Write` request
//! and answer, sent to a batcher instead of a node. It queues each unfenced
//! write in a [`Batcher`] lane of its journal and, when the lane flushes
//! (its flush bound, the leader's limits, or the tick), sends the lane as
//! one write to the journal's leader, following the leader hints itself
//! (a re-sent batch never splits a write). The leader answers once the slot
//! applies; the [`Unbatcher`] scatters that answer, one `seq` range per
//! write. The unbatcher runs in the batcher's process: in paros the leader
//! answers a held write itself, so the answer comes back to the batcher
//! that sent it, and a separate unbatcher would be one more hop (decided on
//! 2026-10-19, #145).
//!
//! A write shaped for a single-writer journal (it carries a leader uuid) is
//! never merged: it goes to the leader alone, and the journal judges it. A
//! merged write the leader refuses as too large teaches the batcher the
//! leader's limits, and each of its writes is sent again alone. Any other
//! answer is every write's answer: within the limits a multi-writer write
//! is never refused, and a merged write that may have run with no answer is
//! answered unanswered (an ambiguity), never sent again by the batcher.
//!
//! Where it runs (#604): every machine of a cell serves the endpoint over
//! the cell's address book (`paros_machine`), and the tenant coordinator
//! books each tenant's batchers, by count, on the cell's `stateless`
//! machines; `Resolve` names a tenant's booked batchers, so its clients
//! send multi-writer writes there. [`run_batcher`] runs one on its own
//! address, in front of a fixed list of nodes.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider, SimulationResult};
use moonpool_rpc::{Execution, RpcHandle};
use paros_core::{
    Batch, BatchLimits, Batcher, JournalId, JournalIdentifier, NodeId, TenantId, Unbatcher,
};
use paros_net::runtime::ParosProviders;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::node::edge::RpcEdge;
use paros_client::{CallObserver, Client, ClientTunables, Server};
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::machine::CachedRegistry;
use paros_net::NodeClient;
use paros_net::inbound::{Inbound, ReplySender, serve_well_known};
use paros_net::transport::Transport;
use paros_tunables::DriverTunables;
use paros_wire::methods::BatchWriteRpc;
use paros_wire::public::WriteOutcome as WireWriteOutcome;
use paros_wire::{FrontendVerdict, Write, WriteAck, common};

/// What a batcher is given to run.
#[derive(Clone, Copy, Debug)]
pub struct BatcherSettings {
    /// The flush rule: the leader's limits, and the records a lane holds
    /// before it flushes on its own.
    pub limits: BatchLimits,
    /// The longest a write waits in a lane: the driver's tick of the flush
    /// rule. At least 1 ms.
    pub tick: Duration,
    /// How it calls the nodes: each attempt's timeout, the redirects it
    /// follows.
    pub client: ClientTunables,
}

impl BatcherSettings {
    /// The settings `tunables` give: the node's `max_batch_records` and
    /// `max_batch_bytes`, a lane flushed at `batch_flush_records` or every
    /// `batch_tick`.
    #[must_use]
    pub fn from_tunables(tunables: &DriverTunables) -> Self {
        let max_records = usize::try_from(tunables.max_batch_records)
            .unwrap_or(usize::MAX)
            .max(1);
        let max_bytes = usize::try_from(tunables.max_batch_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let flush_records = usize::try_from(tunables.batch_flush_records)
            .unwrap_or(usize::MAX)
            .clamp(1, max_records);
        Self {
            limits: BatchLimits {
                max_records,
                max_bytes,
                flush_records,
            },
            tick: tunables.batch_tick.max(Duration::from_millis(1)),
            client: tunables.internal_client(),
        }
    }

    /// These settings with four times the record and byte limits, and a
    /// lane flushed only at its record limit or its tick: a batcher
    /// configured above the leader's limits.
    ///
    /// # Panics
    ///
    /// Never: the assert checks that the raised limits keep their floors.
    #[must_use]
    pub fn above_limits(self) -> Self {
        let max_records = self.limits.max_records.saturating_mul(4);
        let limits = BatchLimits {
            max_records,
            max_bytes: self.limits.max_bytes.saturating_mul(4),
            flush_records: max_records,
        };
        assert!(limits.valid(), "raised limits keep their floors");
        Self { limits, ..self }
    }

    /// These settings with at most [`TIGHT_RECORDS`] records a write: a
    /// batcher configured below the leader's limits, which sends a write
    /// of more records alone.
    ///
    /// # Panics
    ///
    /// Never: the asserts check that the lowered limits keep their floors.
    #[must_use]
    pub fn below_limits(self) -> Self {
        let max_records = self.limits.max_records.min(TIGHT_RECORDS);
        let limits = BatchLimits {
            max_records,
            flush_records: self.limits.flush_records.min(max_records),
            ..self.limits
        };
        assert!(limits.valid(), "lowered limits keep their floors");
        assert!(
            limits.max_records <= self.limits.max_records,
            "a limit only goes down"
        );
        Self { limits, ..self }
    }
}

/// The record limit of a batcher configured below the leader's limits.
/// Floor 1: a one-record write still fits.
pub const TIGHT_RECORDS: usize = 2;
const _: () = assert!(TIGHT_RECORDS >= 1);

/// The nodes a batcher sends its writes to: a fixed list, or a cell's
/// address book, followed as the machine's cached registry fold moves.
pub struct BatcherBook {
    /// The nodes it starts with.
    pub nodes: Vec<(NodeId, Address)>,
    /// The machine's cached registry folds: each new book replaces
    /// `nodes`. `None` for a fixed list.
    pub updates: Option<watch::Receiver<Option<CachedRegistry>>>,
    /// A node it never sends to: the machine it runs on, when that machine
    /// serves no journal.
    pub skip: Option<NodeId>,
    /// Told of each merged write as it leaves (a harness's history); `None`
    /// in production.
    pub observer: Option<Arc<dyn CallObserver>>,
}

/// Run a batcher on its own address until `shutdown`: bind `listen` and
/// serve `BatchWrite` in front of `book`'s nodes, dialed through `names`.
///
/// # Errors
///
/// A failed bind, a registration the runtime refuses, or a runtime that
/// fails for good.
///
/// # Panics
///
/// If `book` names no node, or `settings` breaks a floor.
#[tracing::instrument(level = "debug", skip_all, fields(listen = %listen))]
pub async fn run_batcher<P: ParosProviders>(
    providers: P,
    (listen, tunables, transport): (SocketAddr, &DriverTunables, &Transport),
    settings: BatcherSettings,
    (book, names): (BatcherBook, Names),
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    assert!(
        !book.nodes.is_empty(),
        "a batcher sends to at least one node"
    );
    let edge = RpcEdge::listen(
        &providers,
        &listen.to_string(),
        "batcher",
        tunables,
        transport,
    )
    .await?;
    let rpc = edge.handle().clone();
    let requests = Inbound::plain(serve_well_known::<P, BatchWriteRpc>(&rpc)?);
    tracing::info!(listen = %listen, nodes = book.nodes.len(), "batcher_starting");
    moonpool_core::select! {
        error = edge.run() => Err(error),
        () = serve(providers, rpc.clone(), requests, settings, (book, names), shutdown) => Ok(()),
    }
}

/// Serve `BatchWrite` on a machine's runtime `rpc`, in a task of its own,
/// until `shutdown`.
///
/// # Errors
///
/// The endpoint could not be registered.
pub fn spawn<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    settings: BatcherSettings,
    (book, names): (BatcherBook, Names),
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    let requests = Inbound::plain(serve_well_known::<P, BatchWriteRpc>(rpc)?);
    // A batcher's limits above the leader's, on a BUGGIFY decision drawn on
    // the machine's start path: a machine whose tunables differ from the
    // leader's merges past what the leader takes, and learns its limits
    // from the refusal.
    // Below them on another decision: a write of more records than the
    // batcher takes goes alone.
    let settings = if moonpool_buggify::buggify_with_prob!(0.25) {
        moonpool_assertions::reachable!("batcher: a batcher starts above the leader's limits");
        settings.above_limits()
    } else if moonpool_buggify::buggify_with_prob!(0.25) {
        moonpool_assertions::reachable!("batcher: a batcher starts below the leader's limits");
        settings.below_limits()
    } else {
        settings
    };
    let task = serve(
        providers.clone(),
        rpc.clone(),
        requests,
        settings,
        (book, names),
        shutdown,
    );
    providers
        .local_task()
        .spawn_local_task("paros-batcher", task)
        .detach();
    Ok(())
}

/// The batcher's inbound queue.
type Requests = Inbound<BatchWriteRpc, Write, WriteAck>;

/// What the batcher's loop and its flushes share.
struct Shared<P: ParosProviders> {
    providers: P,
    settings: BatcherSettings,
    /// The limits a leader taught a flush, for the loop's next push.
    learned: Mutex<Option<(usize, usize)>>,
    /// Told of each merged write as it leaves.
    observer: Option<Arc<dyn CallObserver>>,
}

/// The nodes a batcher sends to, as clients: one per journal, so each
/// keeps its own leader hint.
struct Route<P: ParosProviders> {
    rpc: RpcHandle<P>,
    names: Names,
    skip: Option<NodeId>,
    nodes: Vec<(NodeId, Address)>,
    clients: BTreeMap<JournalIdentifier, Client<P>>,
}

impl<P: ParosProviders> Route<P> {
    /// The client of `journal`, `None` while the book names no node to send
    /// to.
    fn client(
        &mut self,
        providers: &P,
        tunables: ClientTunables,
        journal: JournalIdentifier,
    ) -> Option<Client<P>> {
        if let Some(client) = self.clients.get(&journal) {
            return Some(client.clone());
        }
        let servers: Vec<Server<P>> = self
            .nodes
            .iter()
            .filter(|(id, _)| Some(*id) != self.skip)
            .map(|(id, addr)| Server {
                id: id.0,
                node: NodeClient::named(&self.rpc, self.names.clone(), addr.clone()),
            })
            .collect();
        if servers.is_empty() {
            return None;
        }
        let client = Client::new(providers, servers, tunables);
        self.clients.insert(journal, client.clone());
        Some(client)
    }

    /// Take `book`'s machines as the nodes: every client is built again.
    fn follow(&mut self, book: &CachedRegistry) {
        if book.machines.is_empty() || book.machines == self.nodes {
            return;
        }
        self.nodes.clone_from(&book.machines);
        self.clients.clear();
        moonpool_assertions::reachable!("batcher: a batcher follows the cell's address book");
    }
}

/// Take the machine's newest cached book, if one came since the last look.
/// Read without waiting: a task parked on a watch channel beside the
/// cache's other readers would be woken in an order the seed does not
/// decide (tokio spreads a channel's waiters over several notifiers), so
/// the loop looks at the book only when it is about to send.
fn follow_book<P: ParosProviders>(
    updates: &mut Option<watch::Receiver<Option<CachedRegistry>>>,
    route: &mut Route<P>,
) {
    let Some(receiver) = updates.as_mut() else {
        return;
    };
    match receiver.has_changed() {
        Ok(true) => {
            if let Some(book) = receiver.borrow_and_update().clone() {
                route.follow(&book);
            }
        }
        Ok(false) => {}
        // The cache's writer is gone: the book stays as it is.
        Err(_) => *updates = None,
    }
}

/// One write the loop holds: its reply, until its batch is answered.
type Held = ReplySender<WriteAck>;

/// The batcher's loop.
#[tracing::instrument(level = "debug", skip_all)]
async fn serve<P: ParosProviders>(
    providers: P,
    rpc: RpcHandle<P>,
    mut requests: Requests,
    settings: BatcherSettings,
    (book, names): (BatcherBook, Names),
    shutdown: CancellationToken,
) {
    assert!(
        settings.limits.valid(),
        "a batcher's limits keep their floors"
    );
    assert!(
        !settings.tick.is_zero(),
        "a batcher's tick is at least 1 ms"
    );
    let BatcherBook {
        nodes,
        mut updates,
        skip,
        observer,
    } = book;
    let mut route = Route {
        rpc,
        names,
        skip,
        nodes,
        clients: BTreeMap::new(),
    };
    let shared = Arc::new(Shared {
        providers: providers.clone(),
        settings,
        learned: Mutex::new(None),
        observer,
    });
    let mut batcher: Batcher<u64> = Batcher::new(settings.limits);
    let mut held: BTreeMap<u64, Held> = BTreeMap::new();
    let mut next_token: u64 = 0;
    // When the lanes flush: set when a write enters an empty batcher, so a
    // steady stream of writes never pushes the tick back.
    let mut deadline: Option<Duration> = None;
    let time = providers.time().clone();
    loop {
        let wait = deadline.map(|at| at.saturating_sub(time.now()));
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            () = async {
                match wait {
                    Some(wait) => {
                        let _ = time.sleep(wait).await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                deadline = None;
                follow_book(&mut updates, &mut route);
                for batch in batcher.tick() {
                    flush(&shared, &mut route, &mut held, batch, &shutdown);
                }
            }
            Some((mut write, reply)) = requests.recv() => {
                follow_book(&mut updates, &mut route);
                if let Some((max_records, max_bytes)) = shared
                    .learned
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                {
                    batcher.lower_limits(max_records, max_bytes);
                }
                let journal = JournalIdentifier::new(TenantId(write.tenant), JournalId(write.journal));
                next_token += 1;
                let token = next_token;
                held.insert(token, reply);
                let records = std::mem::take(&mut write.records);
                if let Some(uuid) = write.leader.take() {
                    // A single-writer journal's write: never merged, sent
                    // with its own fence and position.
                    let fence = Fence { uuid, seq: write.seq };
                    moonpool_assertions::reachable!("batcher: a fenced write passes through alone");
                    let alone = Batch { journal, writes: vec![(token, records.len())], records };
                    flush_with(&shared, &mut route, &mut held, alone, Some(fence), &shutdown);
                } else {
                    if batcher.pending() == 0 {
                        deadline = Some(time.now().saturating_add(settings.tick));
                    }
                    for batch in batcher.push(journal, token, records) {
                        flush(&shared, &mut route, &mut held, batch, &shutdown);
                    }
                    if batcher.pending() == 0 {
                        deadline = None;
                    }
                }
            }
            else => return,
        }
    }
}

/// Send `batch` (unfenced) in a task of its own.
fn flush<P: ParosProviders>(
    shared: &Arc<Shared<P>>,
    route: &mut Route<P>,
    held: &mut BTreeMap<u64, Held>,
    batch: Batch<u64>,
    shutdown: &CancellationToken,
) {
    flush_with(shared, route, held, batch, None, shutdown);
}

/// Send `batch` as one write carrying `fence`, in a task of its own, and
/// answer each of its writes from the leader's answer.
fn flush_with<P: ParosProviders>(
    shared: &Arc<Shared<P>>,
    route: &mut Route<P>,
    held: &mut BTreeMap<u64, Held>,
    batch: Batch<u64>,
    fence: Option<Fence>,
    shutdown: &CancellationToken,
) {
    assert!(!batch.writes.is_empty(), "a flush sends a write");
    let replies: Vec<(u64, usize, Held)> = batch
        .writes
        .iter()
        .filter_map(|(token, count)| held.remove(token).map(|reply| (*token, *count, reply)))
        .collect();
    assert_eq!(
        replies.len(),
        batch.writes.len(),
        "every write of a batch is held until its flush"
    );
    let Some(client) = route.client(&shared.providers, shared.settings.client, batch.journal)
    else {
        // A book with no node but the one skipped: nothing serves the
        // journal from here, and every write is answered unavailable.
        tracing::warn!(journal = %batch.journal, "batcher_no_node");
        for (_, _, reply) in replies {
            let _ = reply.send(WriteAck::default());
        }
        return;
    };
    let shared = shared.clone();
    let shutdown = shutdown.clone();
    shared
        .providers
        .clone()
        .local_task()
        .spawn_local_task("paros-batcher-flush", async move {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                () = answer(&shared, &client, batch, fence, replies) => {}
            }
        })
        .detach();
}

/// A single-writer journal's write: its leader uuid and its position.
#[derive(Clone, Copy, Debug)]
struct Fence {
    uuid: common::LeaderUuid,
    seq: u64,
}

/// The write a batch sends: unfenced at `seq` 0, or with its `fence`.
fn write_of(batch: &Batch<u64>, records: Vec<Vec<u8>>, fence: Option<Fence>) -> Write {
    Write {
        journal: batch.journal.journal.0,
        tenant: batch.journal.tenant.0,
        leader: fence.map(|f| f.uuid),
        seq: fence.map_or(0, |f| f.seq),
        records,
    }
}

/// Send `batch`, then answer every write of it.
async fn answer<P: ParosProviders>(
    shared: &Shared<P>,
    client: &Client<P>,
    batch: Batch<u64>,
    fence: Option<Fence>,
    replies: Vec<(u64, usize, Held)>,
) {
    let merged = batch.merged();
    let write = write_of(&batch, batch.records.clone(), fence);
    if let (true, Some(observer)) = (merged, &shared.observer) {
        observer.merged(&write);
    }
    let ack = send(shared, client, &write).await;
    if merged && ack.outcome() == WireWriteOutcome::TooLarge {
        // The leader's limits are lower than the batcher's: learn them, and
        // send each write again alone, its records in the order it sent them.
        moonpool_assertions::reachable!(
            "batcher: a merged write over the leader's limits is split"
        );
        let learned = (
            usize::try_from(ack.max_records).unwrap_or(usize::MAX),
            usize::try_from(ack.max_bytes).unwrap_or(usize::MAX),
        );
        *shared
            .learned
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(learned);
        let mut records = batch.records.into_iter();
        for (_, count, reply) in replies {
            let own: Vec<Vec<u8>> = records.by_ref().take(count).collect();
            assert_eq!(own.len(), count, "a write takes back its own records");
            let alone = write_of(
                &Batch {
                    journal: batch.journal,
                    writes: Vec::new(),
                    records: Vec::new(),
                },
                own,
                fence,
            );
            let ack = send(shared, client, &alone).await;
            let _ = reply.send(ack);
        }
        return;
    }
    match ack.outcome() {
        WireWriteOutcome::Accepted | WireWriteOutcome::Duplicate => {
            let writes: Vec<(u64, usize)> = replies
                .iter()
                .map(|(token, count, _)| (*token, *count))
                .collect();
            let ranges = Unbatcher::scatter(&writes, ack.seq, ack.count);
            if merged {
                moonpool_assertions::sometimes!(true, "batcher: a slot held more than one write");
            }
            for ((token, seq, count), (held_token, _, reply)) in ranges.into_iter().zip(replies) {
                assert_eq!(token, held_token, "a range answers its own write");
                let _ = reply.send(WriteAck { seq, count, ..ack });
            }
        }
        _ => {
            for (_, _, reply) in replies {
                let _ = reply.send(ack);
            }
        }
    }
}

/// One write to the journal's leader: the hints followed here, as a
/// frontend follows them, and stripped from the answer. A write that may
/// have run with no answer is answered unanswered, never sent again.
async fn send<P: ParosProviders>(
    shared: &Shared<P>,
    client: &Client<P>,
    write: &Write,
) -> WriteAck {
    let count = client.server_count();
    assert!(count > 0, "a batcher sends to at least one node");
    let tunables = &shared.settings.client;
    let mut target = client.leader().unwrap_or(0);
    let budget = count + usize::try_from(tunables.redirect_limit).unwrap_or(usize::MAX);
    let mut last: Option<WriteAck> = None;
    let mut redirected = false;
    let time = shared.providers.time();
    for _ in 0..budget {
        let attempt = time
            .timeout(tunables.request_timeout, client.node(target).write(write))
            .await;
        let next = (target + 1) % count;
        match attempt {
            Ok(Ok(ack)) if ack.unknown_journal => {
                target = next;
                last = Some(ack);
            }
            Ok(Ok(ack)) if ack.outcome() == WireWriteOutcome::None => {
                client.observe_leader(ack.leader);
                target = client.leader().filter(|l| *l != target).unwrap_or(next);
                redirected = true;
                last = Some(ack);
            }
            Ok(Ok(mut ack)) => {
                client.observe_leader_at(target);
                if redirected {
                    moonpool_assertions::reachable!(
                        "batcher: a batch is re-sent across a leader change"
                    );
                }
                ack.leader = None;
                return ack;
            }
            Ok(Err(error)) if error.execution() == Execution::NotAdmitted => target = next,
            Ok(Err(_)) | Err(_) => {
                moonpool_assertions::reachable!(
                    "batcher: a merged write goes unanswered and is answered ambiguous"
                );
                let mut ack = WriteAck::default();
                ack.set_frontend(FrontendVerdict::Unanswered);
                return ack;
            }
        }
    }
    last.map_or_else(WriteAck::default, |mut ack| {
        ack.leader = None;
        ack
    })
}
