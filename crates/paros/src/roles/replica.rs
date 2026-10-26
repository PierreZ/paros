//! The provider-generic **replica driver** (#144, Compartmentalized Paxos
//! §3.3) — the I/O layer that owns a sans-IO [`paros_core::ReplicaNode`],
//! the fourth driver beside [`run_node`](crate::roles::node::run_node),
//! [`run_matchmaker`](crate::roles::matchmaker::run_matchmaker) and [`run_proxy`](crate::roles::proxy_leader::run_proxy).
//!
//! Written once over `P: ParosProviders`, so the *same* loop runs in
//! production and deterministic simulation. The loop serves the node
//! contract's **learner subset** — it receives the `Commit`s a leader or a
//! proxy leader sends to every learner, the leader's beats, and the
//! `CatchUpResponse` / `TrimmedTo` answers its own requests draw, through
//! the same `Deliver` lane a node does — and drains every batch the core
//! produces in the node's own order: persist (the learned records, the
//! chosen index, the floor) → send (catch-up requests and pre-reads) →
//! report the walk. It runs no application (#186).
//!
//! **Durable, and never a vote.** A replica keeps its chosen log on the same
//! [`LogStorage`] a node uses — the boot scan, the format marker (#147) and
//! the durability seams all apply — but it writes a learned record
//! ([`paros_core::WriteOp::Learned`]) where a node writes an accepted one, it
//! answers no `Prepare` and no `Accept`, and it is in no configuration, so no
//! quorum ever counts it. It serves no peer either: a lagging replica is
//! healed from the acceptors, never from another replica, so it answers no
//! catch-up.
//!
//! **It serves clients reads, and only reads.** The journal `Read` (#204)
//! is the leaderless read (§3.4): it opens a quorum read in the core (the
//! grid row is the node's own row override), parks the reply exactly as the
//! node driver does, serves the page from this replica's own journal fold
//! after the walk that covers the confirmed watermark — read replicas take
//! read load off the acceptors — long-polls at the tail, and answers
//! `served: false` when the confirmation does not come within
//! `read_retry_ticks`. Every other public call is refused as unimplemented.
//!
//! **A replica below the floor jumps.** A replica left below a trim point
//! the acceptors decided while it was away asks for catch-up from where it
//! stopped, and an acceptor answers `TrimmedTo` (#186): it jumps its floor
//! and chosen index to the point and catches up from there.
//!
//! A deployment whose `Config::replica_count` is zero runs no replica; the
//! node driver then sends its learner traffic to the pool alone, exactly the
//! plain deployment's messages.

use std::collections::{BTreeMap, BTreeSet};

use crate::net::runtime::ParosProviders;
use moonpool_buggify::hint::Strike;
use moonpool_core::{LocalTimeProvider, RandomProvider, SimulationError, SimulationResult};
use paros_core::{
    Ballot, Command, JournalId, JournalIdentifier, MustSync, NodeId, Outcome, Party, QuorumSystem,
    ReadState, ReplicaNode, Slot, TenantId, WriteOp,
};

use crate::net::transport::Transport;
use crate::roles::node::JournalStores;
use crate::roles::node::log_reads::{JournalReads, ReadLimits};
use tokio_util::sync::CancellationToken;

use crate::net::ReplySender;
use crate::observe::audit::Audit;
use crate::observe::events::{message_kind, message_route};
use crate::roles::node::boot::check_format_marker;
use crate::roles::node::edge::{ReplicaInbox, RpcEdge, edge_reporter};
use crate::roles::node::ready::{persist_writes, report_applied, storage_fault_crash};
use crate::roles::node::reply::{Reply, answer};
use crate::roles::node::transport::{LaneOpener, Outbound, send_messages};
use crate::storage::LogStorage;
use crate::storage::boot::{BootKind, RunError};
use crate::tunables::DriverTunables;
use crate::wire::{
    InspectRefusal, InspectReply, InspectTarget, Read, ReadAck, journal_state_to_proto,
    quorum_system_to_proto,
};

/// Hand a booted replica the driver's tunables: it keeps a quorum read as
/// long as the driver waits (#386), and walks the driver's apply page,
/// capped at the core's ceiling (#338; an operator's override is external
/// input, never a panic).
fn tune(replica: &mut ReplicaNode, tunables: &DriverTunables) {
    replica.set_read_window(tunables.read_retry_ticks);
    let apply_page = tunables.apply_page.clamp(1, paros_core::APPLY_BATCH);
    replica.set_apply_page(apply_page);
    assert!(
        replica.replica().apply_page() == apply_page,
        "a booted replica runs the driver's apply page"
    );
}

/// Report a (re)boot: the chosen index this replica rebuilt from its log
/// and its floor. Nothing is replayed — a replica runs no application
/// (#186); its chosen prefix is its whole state.
#[tracing::instrument(level = "debug", skip_all, fields(replica = self_id))]
fn report_boot<A: Audit>(replica: &ReplicaNode, self_id: u64, audit: &A) {
    let chosen_index = replica.replica().chosen_index();
    let floor = replica.first_slot();
    audit.replica_booted(NodeId(self_id), chosen_index, floor);
    tracing::info!(
        replica = self_id,
        chosen = chosen_index.map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
        floor = floor.0,
        "replica_booted"
    );
}

/// One drained batch: the quorum reads it confirmed and the fold head it
/// reported. The reads are served from exactly that fold, before the next
/// page of a deferred walk is released ([`serve_and_advance`]).
struct Drained {
    reads: Vec<ReadState>,
    head: Option<Slot>,
}

/// Run the [`ReplicaReady`](paros_core::ReplicaReady) handshake once, in the
/// node driver's order and with its seams: persist the learned records, the
/// chosen index and a decided floor (the `BeforeSync` seam inside), send the
/// catch-up requests (after the `AfterSyncBeforeSend` seam), report the
/// slots the walk moved over, and hand back the quorum reads this batch
/// confirmed with the fold head it reported. It does not release the next
/// page of a deferred walk: [`serve_and_advance`] does, after the reads are
/// answered from the fold this batch reported (the node's order,
/// `finish_ready` step 3b, then `advance_recovery`; #550 (replica serves an
/// unreported fold page)).
#[tracing::instrument(level = "trace", skip_all, fields(replica = self_id))]
async fn drain<S: LogStorage, A: Audit>(
    replica: &mut ReplicaNode,
    storage: &mut S,
    out: &Outbound,
    self_id: u64,
    audit: &A,
) -> Result<Drained, RunError> {
    let ready = replica.ready();
    let writes: Vec<WriteOp> = ready.writes().to_vec();
    let messages: Vec<(Party, paros_core::Message)> = ready
        .messages()
        .iter()
        .filter_map(|(audience, msg)| match audience {
            paros_core::Audience::Node(to) => Some((Party::Node(*to), msg.clone())),
            _ => None,
        })
        .collect();
    let committed: Vec<(Slot, Ballot, Command, Outcome)> = ready.committed().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    ready.advance();

    // A replica holds no promise; the ballot the persist report is handed
    // only ever labels an accepted record, which a replica never writes.
    let must_sync = MustSync::for_batch(&writes);
    persist_writes(storage, &writes, must_sync, Ballot::zero(), self_id, audit).await?;
    // Hint: the replica's batch is durable, its messages have not left.
    if !writes.is_empty() || !messages.is_empty() {
        let hinted = moonpool_buggify::hint!("replica batch durable, not sent");
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!("a replica crashes after sync and before sending");
        }
        hinted.await;
    }
    send_messages(out, audit, replica.config().journal, messages);
    for (slot, _, command, outcome) in &committed {
        let outcome = (*outcome != Outcome::Noop).then_some(outcome);
        report_applied(audit, self_id, *slot, command, outcome);
    }
    let head = fold_head(replica);
    // Every slot this batch reported is in the fold it hands back.
    if let Some((last, _, _, _)) = committed.last() {
        assert!(
            head.is_some_and(|head| head >= *last),
            "a drained batch's fold covers every slot it reported"
        );
    }
    Ok(Drained {
        reads: read_states,
        head,
    })
}

/// Serve the reads a drained batch confirmed from the fold that batch
/// reported, then release the next page of a deferred walk (#550 (replica
/// serves an unreported fold page)). The order is the node driver's
/// (`finish_ready`: step 3b, then `advance_recovery`): released first, the
/// core folds the next page at once, while its writes and its `committed`
/// report wait for the next batch, and a read would be served from slots the
/// replica has neither persisted nor reported. Witness 147148122662988833
/// (with a local rename amplification), `log_read_served`: a rebooted
/// replica paging its catch-up served `next_seq` 11 with slot 42 unreported.
///
/// On a tick, `expire` carries the tick count and the confirmation window:
/// the long-polls whose wait ran out are answered here too, from the same
/// reported fold, before the release.
fn serve_and_advance<A: Audit>(
    reads: &mut JournalReads,
    drained: &Drained,
    replica: &mut ReplicaNode,
    expire: Option<(u64, u64)>,
    me: NodeId,
    audit: &A,
) {
    assert!(
        fold_head(replica) == drained.head,
        "a replica serves reads from the fold its last batch reported"
    );
    serve_reads(reads, &drained.reads, replica, me, audit);
    if let Some((ticks, retry_ticks)) = expire {
        reads.expire(
            |from, limit, bytes| replica.read_log(from, limit, bytes),
            ticks,
            retry_ticks,
            false,
            me,
            audit,
        );
    }
    assert!(
        fold_head(replica) == drained.head,
        "serving reads never moves the fold"
    );
    replica.advance_recovery();
    assert!(
        fold_head(replica) >= drained.head,
        "releasing the next page never rewinds the fold"
    );
    if !drained.reads.is_empty() && fold_head(replica) > drained.head {
        // The path #550 (replica serves an unreported fold page) broke: a
        // read confirmed in the batch that ends a page, answered before the
        // next page folds.
        moonpool_assertions::reachable!("a replica serves a read before its next apply page");
    }
}

/// The last slot of this replica's journal fold — what a read is served
/// from.
fn fold_head(replica: &ReplicaNode) -> Option<Slot> {
    replica.replica().folded().0.checked_sub(1).map(Slot)
}

/// Serve the reads this batch confirmed and re-serve the ones waiting at the
/// tail: the state the client reads is this replica's own fold.
fn serve_reads<A: Audit>(
    reads: &mut JournalReads,
    served: &[ReadState],
    replica: &ReplicaNode,
    me: NodeId,
    audit: &A,
) {
    let read = |from, limit, bytes| replica.read_log(from, limit, bytes);
    reads.confirmed(served, read, fold_head(replica), false, me, audit);
    reads.wake(read, me, audit);
}

/// Surface an inbound message's arrival for a human reading the trace, the
/// mirror of the sender's `msg_sent`.
fn trace_received(self_id: u64, msg: &paros_core::Message) {
    let kind = message_kind(msg);
    if let Some((from, ballot, slot)) = message_route(msg) {
        tracing::info!(
            replica = self_id,
            from = %from,
            kind,
            bround = ballot.round,
            bnode = ballot.node.0,
            slot = slot.map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
            "msg_received"
        );
    } else {
        tracing::info!(replica = self_id, kind, "msg_received");
    }
}

/// Open one journal `Read` (#204) on the replica: a leaderless read of its
/// own fold (§3.4) — the core asks a row of the configuration it believes in
/// force, and the read is served once the row answered whole and this
/// replica folded the maximum watermark — parked until then. The row
/// override is the node's hook, asked only under a grid, from the loop.
fn open_read<A: Audit>(
    replica: &mut ReplicaNode,
    reads: &mut JournalReads,
    req: Read,
    reply: ReplySender<ReadAck>,
    tunables: &DriverTunables,
    audit: &A,
) {
    let ctx = reads.next_ctx();
    // The replica reads over its basis (#260); without one it opens nothing,
    // and neither the row hook nor the audit hears of the read.
    let basis = replica.read_basis();
    let row = basis.as_ref().and_then(|b| match b.config.quorum_system() {
        QuorumSystem::Grid { rows, acks, .. }
            if rows >= 2 && acks == rows && moonpool_buggify::buggify_fault_with_prob!(0.10) =>
        {
            moonpool_assertions::reachable!("grid: the driver overrides a quorum read's row");
            Some(crate::roles::node::log_reads::next_row(ctx, rows))
        }
        _ => None,
    });
    let row = basis.as_ref().and_then(|b| b.config.read_row(ctx, row));
    let opened = fold_head(replica);
    if basis.is_some() {
        audit.quorum_read_opened(replica.config().id, ctx);
    }
    replica.quorum_read_in(ctx, row);
    reads.park(vec![(req, reply)], ReadLimits::of(tunables), row, opened);
}

/// Scan the replica's store and check its format marker against `boot`;
/// the replica's id.
async fn boot_store<S, A>(storage: &mut S, boot: BootKind, audit: &A) -> Result<u64, RunError>
where
    S: LogStorage,
    A: Audit + Clone + Send + Sync + 'static,
{
    let self_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .await
        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    check_format_marker(storage, boot, self_id, audit).await?;
    Ok(self_id)
}

/// One journal this replica serves (#193): its own `ReplicaNode`, store,
/// reads and audit port. Nothing crosses journals.
struct ReplicaRt<S, A> {
    replica: ReplicaNode,
    storage: S,
    reads: JournalReads,
    audit: A,
}

impl<S: LogStorage, A: Audit + Clone + Send + Sync + 'static> ReplicaRt<S, A> {
    /// Drain this journal's batch and serve the reads it confirmed, `expire`
    /// as [`serve_and_advance`] takes it; whether a read was confirmed.
    async fn settle(
        &mut self,
        out: &Outbound,
        self_id: u64,
        expire: Option<(u64, u64)>,
    ) -> Result<bool, RunError> {
        let drained = drain(
            &mut self.replica,
            &mut self.storage,
            out,
            self_id,
            &self.audit,
        )
        .await?;
        let confirmed = !drained.reads.is_empty();
        serve_and_advance(
            &mut self.reads,
            &drained,
            &mut self.replica,
            expire,
            NodeId(self_id),
            &self.audit,
        );
        Ok(confirmed)
    }
}

/// Note that `journal`'s replica confirmed a read: one replica serving reads
/// for two journals is the shared process #193 makes of it.
fn note_read(read: &mut BTreeSet<JournalIdentifier>, journal: JournalIdentifier) {
    if read.insert(journal) && read.len() >= 2 {
        moonpool_assertions::reachable!("replica: one replica serves reads for two journals");
    }
    assert!(read.contains(&journal), "a journal that read is remembered");
}

/// Open and boot every journal `stores` lists: each store scanned and its
/// format marker judged against the operator's claim, its `ReplicaNode`
/// rebuilt from it and tuned. A journal whose store stays down for good is
/// left out. Every store names one replica id; a store naming another is
/// caller input, refused.
async fn boot_journals<P: ParosProviders, J: JournalStores>(
    providers: &P,
    stores: &mut J,
    tunables: &DriverTunables,
) -> Result<
    (
        u64,
        BTreeMap<JournalIdentifier, ReplicaRt<J::Store, J::Audit>>,
    ),
    RunError,
> {
    let mut journals = BTreeMap::new();
    let mut self_id: Option<u64> = None;
    for journal in stores.journals() {
        let Some((mut storage, boot)) = stores.open(journal).await else {
            tracing::info!(journal = %journal, "replica_journal_down");
            continue;
        };
        let audit = stores.audit(journal);
        let id = boot_store(&mut storage, boot, &audit).await?;
        stores.opened(journal);
        if self_id.is_some_and(|known| known != id) {
            return Err(RunError::Infra(SimulationError::InvalidState(
                "a replica's stores name one replica".into(),
            )));
        }
        self_id = Some(id);
        let mut replica = ReplicaNode::new(&storage);
        if replica.config().journal != journal {
            return Err(RunError::Infra(SimulationError::InvalidState(
                "a replica's store names its own journal".into(),
            )));
        }
        tune(&mut replica, tunables);
        report_boot(&replica, id, &audit);
        journals.insert(
            journal,
            ReplicaRt {
                replica,
                storage,
                // A fresh random token space per incarnation (see
                // `JournalReads`): a dead incarnation's acks never match.
                reads: JournalReads::starting_at(providers.random().random()),
                audit,
            },
        );
    }
    let Some(self_id) = self_id else {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "a replica serves at least one journal".into(),
        )));
    };
    assert!(!journals.is_empty(), "a booted replica serves a journal");
    assert!(
        journals
            .values()
            .all(|rt| rt.replica.config().id.0 == self_id),
        "every journal of a replica runs under its one id"
    );
    Ok((self_id, journals))
}

/// Drive a paros replica to completion over the given providers.
///
/// Generic over `P: ParosProviders` (production *or* simulation) and
/// `J: JournalStores` (the journals this replica serves and their injected
/// durable stores, the same seam a node runs on). A replica is a shared
/// process (#193, §3.4): one [`ReplicaNode`] and one store per journal,
/// demuxed by the [`JournalIdentifier`] every message and call names, and
/// nothing crosses journals. The loop serves the node contract's learner
/// subset on `local_addr` and sends its catch-up requests to the acceptors
/// named in `members` — the full **node pool** (`NodeId` → address), the
/// same list every node is given. The replica's own id, read from the
/// stores' `Config`s (one id for every journal), is outside that pool.
///
/// Each store's boot claim ([`BootKind`], #147) comes from `stores` and is
/// judged against the store's format marker before the core reads a byte,
/// exactly as on a node: a replica that lost its disk comes back as a first
/// boot and relearns the log from the acceptors, because a replica holds no
/// promise — there is nothing an amnesiac replica could regress. A journal
/// `stores` cannot open stays down on this replica for good.
///
/// `tunables` is the tick cadence and the transport shape; each journal
/// reports to its own audit port (`JournalStores::audit`), the replica's
/// own facts to `JournalStores::node_audit`. The send seam's drops and
/// duplicates and the row override are inline BUGGIFY locations. The
/// durability moments are `hint!`s inline.
///
/// # Errors
///
/// [`RunError::Storage`] when a storage call failed, on any journal
/// (recovered by re-running against the surviving stores: a replica is
/// fail-stop, as it was with one journal), [`RunError::Refused`] when a
/// boot claim and its format marker disagree, [`RunError::Infra`] for a
/// genuine provider failure or stores that name no journal or several
/// replica ids.
///
/// # Panics
///
/// If an internal invariant is broken (a boot batch that confirms a read,
/// a fold that moved under a served read): a programmer error, never an
/// operating condition.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len()))]
// The parameters are the replica's complete wiring; the loop is one select
// over the lane, the reads, the beat, the inspections and the shutdown.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_replica<P, J>(
    providers: P,
    mut stores: J,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    tunables: DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
) -> Result<(), RunError>
where
    P: ParosProviders,
    J: JournalStores,
{
    let (self_id, mut journals) = boot_journals(&providers, &mut stores, &tunables).await?;
    let node_audit = stores.node_audit();
    if journals.len() >= 2 {
        moonpool_assertions::reachable!("replica: a replica serves several journals");
    }

    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let me_id = NodeId(self_id);
    let me = Party::Node(me_id);
    let edge = RpcEdge::listen(&providers, &local_addr, "replica", &tunables, &transport).await?;
    let ReplicaInbox {
        inspect: mut inspects,
        log_read: mut log_reads,
        deliver: mut inbox,
    } = ReplicaInbox::serve(
        &providers,
        &edge,
        &tunables,
        me,
        // No cell plan reaches a replica yet: it runs on no machine.
        0,
        edge_reporter(&node_audit, me),
        incarnation_shutdown.clone(),
    )?;

    let out = acceptor_lanes(
        &providers,
        &edge,
        &tunables,
        &incarnation_shutdown,
        &node_audit,
        me,
        members,
    )?;

    // The boot may already carry a batch per journal: records learned above
    // the prefix before the crash complete it on the way back up. Nothing is
    // parked yet, so a boot batch confirms no read.
    for rt in journals.values_mut() {
        let confirmed = rt.settle(&out, self_id, None).await?;
        assert!(!confirmed, "a boot batch confirms no read");
    }

    // The journals that confirmed a read in this incarnation.
    let mut read: BTreeSet<JournalIdentifier> = BTreeSet::new();
    let mut ticks: u64 = 0;
    let time = providers.time().clone();
    let mut next_tick = time.now() + tunables.tick_interval;
    loop {
        moonpool_core::select! {
            error = edge.run() => return Err(error.into()),
            Some((to, msg)) = inbox.recv() => {
                let Some(rt) = journals.get_mut(&to) else {
                    tracing::info!(node = self_id, journal = %to, "foreign_journal_dropped");
                    continue;
                };
                trace_received(self_id, &msg);
                rt.replica.step(msg);
                if rt.settle(&out, self_id, None).await? {
                    note_read(&mut read, to);
                }
            }
            Some((req, reply)) = log_reads.recv() => {
                // A journal `Read` (#204): a leaderless read served from this
                // replica's own fold of the journal it names (§3.4) — the
                // core asks a row of the configuration it believes in force,
                // and the read is served once the row answered whole and
                // this replica folded the maximum watermark. The row
                // override is the node's hook, asked only under a grid, from
                // the loop. A journal this replica does not serve is refused.
                let asked = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.get_mut(&asked).filter(|_| asked.is_set()) else {
                    node_audit.journal_refused(me_id, asked, "read");
                    tracing::info!(node = self_id, journal = %asked, call = "read", "journal_refused");
                    let refused = ReadAck { unknown_journal: true, ..ReadAck::default() };
                    answer(&node_audit, me_id, Reply::LogRead, reply, refused);
                    continue;
                };
                open_read(&mut rt.replica, &mut rt.reads, req, reply, &tunables, &rt.audit);
                if rt.settle(&out, self_id, None).await? {
                    note_read(&mut read, asked);
                }
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                next_tick = time.now() + tunables.tick_interval;
                ticks += 1;
                let expire = Some((ticks, tunables.read_retry_ticks));
                // Each journal's beat, in identifier order (iteration order
                // is part of determinism).
                for (journal, rt) in &mut journals {
                    rt.replica.tick();
                    if rt.settle(&out, self_id, expire).await? {
                        note_read(&mut read, *journal);
                    }
                    if let Some((hole, above)) = rt.replica.replica().chosen_gap() {
                        rt.audit.chosen_gap(me_id, hole, above);
                        tracing::info!(replica = self_id, journal = %journal, hole = hole.0, above = above.0, "chosen_gap");
                    }
                }
                tracing::info!(replica = self_id, "replica_tick");
            }
            Some((req, reply)) = inspects.recv() => {
                // No batch: an inspect reads the replica and its store. It
                // names a journal this replica serves or asks for the node
                // alone; an unset identifier or another journal is refused
                // (#243).
                let first = journals.values().next().map(|rt| inspect(&rt.replica));
                let Some(first) = first else {
                    continue;
                };
                let answer = match req.target() {
                    Ok(InspectTarget::Node) => first.node_facts(),
                    Ok(InspectTarget::Journal(journal)) => journals.get(&journal).map_or_else(
                        || first.refused(InspectRefusal::UnknownJournal),
                        |rt| inspect(&rt.replica),
                    ),
                    Err(refusal) => first.refused(refusal),
                };
                let _ = reply.send(answer);
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

/// Open one lane per node of the pool — a replica only ever asks an
/// acceptor — and the [`Outbound`] over them, riding `edge`'s runtime.
fn acceptor_lanes<P: ParosProviders, A: Audit + Clone + Send + Sync + 'static>(
    providers: &P,
    edge: &RpcEdge<P>,
    tunables: &DriverTunables,
    shutdown: &CancellationToken,
    audit: &A,
    me: Party,
    members: Vec<(NodeId, String)>,
) -> SimulationResult<Outbound> {
    let lanes = LaneOpener {
        providers,
        tunables: *tunables,
        shutdown: shutdown.clone(),
        audit,
        from: me,
        // No cell plan reaches this role yet: it runs on no machine.
        cell_id: 0,
        names: crate::model::address::Names::literal(),
    };
    let peer_queues = lanes.open_all(
        edge.handle(),
        "paros-replica-catch-up",
        members,
        Party::Node,
    )?;
    let out = Outbound::new(peer_queues, BTreeMap::new(), Vec::new(), me);
    Ok(out)
}

/// Answer an operator's or a probe's `Inspect`: the replica's chosen prefix
/// and its floor. The configuration fields name the
/// bootstrap acceptors this replica learns from; a replica never leads and
/// holds no matchmaker belief or GC floor.
fn inspect(replica: &ReplicaNode) -> InspectReply {
    let (quorum_system, phase1_quorum, phase2_quorum, rows, cols) =
        quorum_system_to_proto(replica.config().quorum_system).into_parts();
    InspectReply {
        chosen_index: replica.replica().chosen_index().map(|slot| slot.0),
        first_slot: replica.first_slot().0,
        members: replica.config().peers.iter().map(|n| n.0).collect(),
        quorum_system,
        phase1_quorum,
        phase2_quorum,
        rows,
        cols,
        folded: replica.replica().folded().0,
        journal: Some(journal_state_to_proto(replica.replica().journal())),
        node: replica.config().id.0,
        ..InspectReply::default()
    }
}
