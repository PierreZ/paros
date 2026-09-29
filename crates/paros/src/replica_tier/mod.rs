//! The provider-generic **replica driver** (#144, Compartmentalized Paxos
//! §3.3) — the I/O layer that owns a sans-IO [`paros_core::ReplicaNode`],
//! the fourth driver beside [`run_node`](crate::run_node),
//! [`run_matchmaker`](crate::run_matchmaker) and [`run_proxy`](crate::run_proxy).
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
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
//! **It serves clients reads, and only reads.** The journal `Read` (#185)
//! is served from its own chosen prefix — read replicas take read load off
//! the acceptors — and long-polls at its end exactly as a node's does. A
//! `CheckTail` on the quorum path is the leaderless read (§3.4): it opens a
//! quorum read in the core (the grid row is the node's `read_row` hook),
//! parks the reply exactly as the node driver does, answers it after the
//! walk that covers the confirmed index, and expires it on
//! `read_retry_ticks`; on the leader path it redirects, since a replica
//! confirms no leadership. Every other public call is refused as
//! unimplemented.
//!
//! **A replica below the floor jumps.** A replica left below a trim point
//! the acceptors decided while it was away asks for catch-up from where it
//! stopped, and an acceptor answers `TrimmedTo` (#186): it jumps its floor
//! and chosen index to the point and catches up from there.
//!
//! A deployment whose `Config::replica_count` is zero runs no replica; the
//! node driver then sends its learner traffic to the pool alone, exactly the
//! plain deployment's messages.

use std::collections::BTreeMap;

use moonpool_core::{Providers, SimulationResult, TimeProvider};
use paros_core::{
    Ballot, Command, MustSync, NodeId, Party, QuorumSystem, ReadState, ReplicaNode, Slot, WriteOp,
};
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::driver::boot::check_format_marker;
use crate::driver::edge::{ReplicaInbox, RpcEdge, edge_reporter};
use crate::driver::events::{message_kind, message_route};
use crate::driver::log_reads::{LogReads, refuse_journal};
use crate::driver::ready::committed_end;
use crate::driver::ready::{
    ParkedRead, ReadPath, crash_if, persist_writes, report_applied, storage_fault_crash,
};
use crate::driver::reply::answer;
use crate::driver::transport::{LaneOpener, Outbound, PeerQueues, peer_address, send_messages};
use crate::driver::{BootKind, DriverTunables, RunError};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::rpc::{
    CheckTail, CheckTailAck, InspectReply, ReplySender, TailPath, quorum_system_to_proto,
    well_known,
};
use crate::storage::LogStorage;

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

/// Run the [`ReplicaReady`](paros_core::ReplicaReady) handshake once, in the
/// node driver's order and with its seams: persist the learned records, the
/// chosen index and a decided floor (the `BeforeSync` seam inside), send the
/// catch-up requests (after the `AfterSyncBeforeSend` seam), report the
/// slots the walk moved over, release the next page of a deferred walk, and
/// hand back the quorum reads this batch confirmed: answered only now, after
/// the walk that covers them (the node's order, `drain_ready` step 3b).
#[tracing::instrument(level = "trace", skip_all, fields(replica = self_id))]
async fn drain<S: LogStorage, H: DriverHooks, A: Audit>(
    replica: &mut ReplicaNode,
    storage: &mut S,
    out: &Outbound,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<Vec<ReadState>, RunError> {
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
    let committed: Vec<(Slot, Command)> = ready.committed().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    ready.advance();

    // A replica holds no promise; the ballot the persist report is handed
    // only ever labels an accepted record, which a replica never writes.
    let must_sync = if writes.iter().any(WriteOp::needs_sync) {
        MustSync::Sync
    } else {
        MustSync::Relaxed
    };
    persist_writes(
        storage,
        &writes,
        must_sync,
        Ballot::zero(),
        self_id,
        hooks,
        audit,
    )
    .await?;
    crash_if(
        !writes.is_empty() || !messages.is_empty(),
        hooks,
        audit,
        NodeId(self_id),
        Seam::AfterSyncBeforeSend,
    )?;
    send_messages(out, hooks, audit, messages);
    for (slot, command) in &committed {
        report_applied(audit, self_id, *slot, command);
    }
    replica.advance_recovery();
    Ok(read_states)
}

/// The replica's held client reads: the tails, keyed by the core's `ctx`
/// token, and the journal reads long-polling at its end (#185).
struct ParkedReads {
    reads: BTreeMap<u64, ParkedRead>,
    next_ctx: u64,
    journal: LogReads,
}

impl ParkedReads {
    /// A leaderless read served from this replica's own state (§3.4): the
    /// core asks a row of the configuration it believes in force, and the
    /// reply is parked until the row answered whole and this replica applied
    /// the maximum watermark. The row override is the node's hook, asked only
    /// under a grid, from the loop.
    fn open<H: DriverHooks>(
        &mut self,
        replica: &mut ReplicaNode,
        seq: u64,
        reply: ReplySender<CheckTailAck>,
        ticks: u64,
        hooks: &H,
    ) {
        let ctx = self.next_ctx;
        self.next_ctx += 1;
        let row = match replica.acceptors().quorum_system() {
            QuorumSystem::Grid { rows, .. } => hooks.read_row(ctx, rows).filter(|r| *r < rows),
            _ => None,
        };
        let row = replica.acceptors().read_row(ctx, row);
        let opened = replica.replica().chosen_index();
        replica.quorum_read_in(ctx, row);
        let path = ReadPath::Quorum { row, opened };
        self.reads.insert(
            ctx,
            ParkedRead {
                seq,
                parked_at: ticks,
                path,
                reply,
            },
        );
    }

    /// Answer every read the core confirmed and this replica's applied
    /// prefix covers — the state the client reads is this replica's own —
    /// reporting each to the audit as the node driver does.
    fn answer_served<H: DriverHooks, A: Audit>(
        &mut self,
        served: &[ReadState],
        replica: &ReplicaNode,
        self_id: u64,
        hooks: &H,
        audit: &A,
    ) {
        for state in served {
            let Some(parked) = self.reads.remove(&state.ctx) else {
                continue;
            };
            let read_index = replica.replica().chosen_index();
            if let ReadPath::Quorum { row, opened } = parked.path {
                audit.quorum_read_served(
                    NodeId(self_id),
                    row,
                    state.index,
                    read_index,
                    opened,
                    false,
                );
            }
            tracing::info!(
                replica = self_id,
                ctx = state.ctx,
                watermark = state
                    .index
                    .map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
                "quorum_read_served"
            );
            answer(
                hooks,
                audit,
                NodeId(self_id),
                Reply::Read,
                parked.reply,
                CheckTailAck {
                    seq: parked.seq,
                    leader: replica.leader().map(|n| n.0),
                    committed: true,
                    committed_end: Some(committed_end(read_index)),
                    unknown_journal: false,
                },
            );
        }
        // The chosen prefix only grows inside a batch: a journal read at the
        // end is re-served after each one.
        self.journal.wake(
            |from, max| replica.read_log(from, max),
            NodeId(self_id),
            hooks,
            audit,
        );
    }

    /// Answer a retry redirect to every read whose confirmation is overdue
    /// (a row that never answered whole, a watermark this replica has not
    /// reached): the client records it ambiguous and asks again. A journal
    /// read whose long-poll ran out is answered empty.
    fn expire<H: DriverHooks, A: Audit>(
        &mut self,
        ticks: u64,
        tunables: &DriverTunables,
        replica: &ReplicaNode,
        self_id: u64,
        hooks: &H,
        audit: &A,
    ) {
        let retry_ticks = tunables.read_retry_ticks;
        self.journal.expire(
            |from, max| replica.read_log(from, max),
            ticks,
            tunables.read_poll_ticks,
            NodeId(self_id),
            hooks,
            audit,
        );
        let overdue: Vec<u64> = self
            .reads
            .iter()
            .filter(|(_, parked)| ticks.saturating_sub(parked.parked_at) > retry_ticks)
            .map(|(ctx, _)| *ctx)
            .collect();
        for ctx in overdue {
            if let Some(parked) = self.reads.remove(&ctx) {
                audit.read_expired(NodeId(self_id), false);
                answer(
                    hooks,
                    audit,
                    NodeId(self_id),
                    Reply::ReadRedirect,
                    parked.reply,
                    CheckTailAck {
                        seq: parked.seq,
                        leader: replica.leader().map(|n| n.0),
                        ..CheckTailAck::default()
                    },
                );
            }
        }
    }
}

/// The refusal a replica answers a `CheckTail` with before opening
/// anything: a journal it does not serve, or the leader path — a replica
/// holds no leadership to confirm, so it redirects to the leader it heard
/// last. `None` when it serves the read (a quorum read, §3.4).
fn refused_tail<A: Audit>(
    req: &CheckTail,
    journal: paros_core::JournalId,
    replica: &ReplicaNode,
    me: NodeId,
    audit: &A,
) -> Option<CheckTailAck> {
    if refuse_journal(journal, req.journal, "check_tail", me, audit) {
        return Some(CheckTailAck {
            seq: req.seq,
            unknown_journal: true,
            ..CheckTailAck::default()
        });
    }
    (req.path() == TailPath::Leader).then(|| CheckTailAck {
        seq: req.seq,
        leader: replica.leader().map(|n| n.0),
        ..CheckTailAck::default()
    })
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

/// Drive a paros replica to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation) and
/// `S: LogStorage` (the injected durable storage, the same trait a node
/// runs on). The loop owns a [`ReplicaNode`] rebuilt from `storage`, serves
/// the node contract's learner subset on `local_addr`, and sends its
/// catch-up requests to the acceptors named in `members` — the full **node
/// pool** (`NodeId` → address), the same list every node is given. The
/// replica's own id, read from the store's `Config`, is outside that pool.
///
/// `boot` is the operator's claim about `storage` ([`BootKind`], #147),
/// judged against the store's format marker before the core reads a byte,
/// exactly as on a node: a replica that lost its disk comes back as a first
/// boot and relearns the log from the acceptors, because a replica holds no
/// promise — there is nothing an amnesiac replica could regress.
///
/// `tunables`, `hooks` and `audit` are the seams every driver in this crate
/// takes: the tick cadence and the transport shape, the durability seams
/// and the send seam's drop and duplicate locations, and the observation
/// port.
///
/// # Errors
///
/// [`RunError::SeamCrash`] when `hooks` fires at a durability seam and
/// [`RunError::Storage`] when a storage call failed (both recovered by
/// re-running against the surviving store), [`RunError::Refused`] when
/// `boot` and the format marker disagree, [`RunError::Infra`] for a genuine
/// provider failure.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len()))]
// The parameters are the replica's complete wiring; the loop is one select
// over the lane, the beat and the shutdown.
#[allow(clippy::too_many_arguments)]
pub async fn run_replica<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    boot: BootKind,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: LogStorage,
    // Not `Send + 'static`, as on `run_node`: a hook is consulted from this
    // loop and never from a spawned task.
    H: DriverHooks,
    A: Audit + Clone + Send + Sync + 'static,
{
    let self_id = storage.initial_state().1.id.0;
    storage
        .boot_scan()
        .await
        .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    check_format_marker(&mut storage, boot, self_id, audit).await?;

    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let me_id = NodeId(self_id);
    let me = Party::Node(me_id);
    let mut edge = RpcEdge::listen(&providers, &local_addr, "replica", &tunables).await?;
    let ReplicaInbox {
        inspect: mut inspects,
        log_read: mut log_reads,
        check_tail: mut tails,
        deliver: mut inbox,
    } = ReplicaInbox::serve(
        &providers,
        &edge,
        &tunables,
        me,
        edge_reporter(audit, me),
        incarnation_shutdown.clone(),
    )?;

    let mut replica = ReplicaNode::new(&storage);
    report_boot(&replica, self_id, audit);

    let out = acceptor_lanes(
        &providers,
        &edge,
        tunables,
        &incarnation_shutdown,
        audit,
        me,
        members,
    )?;

    // The boot may already carry a batch: records learned above the prefix
    // before the crash complete it on the way back up.
    drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;

    let mut parked = ParkedReads {
        reads: BTreeMap::new(),
        next_ctx: 0,
        journal: LogReads::default(),
    };
    let journal = replica.config().journal;
    let mut ticks: u64 = 0;
    let time = providers.time().clone();
    let mut next_tick = time.now() + tunables.tick_interval;
    loop {
        moonpool_core::select! {
            error = edge.run() => return Err(error.into()),
            Some(msg) = inbox.recv() => {
                trace_received(self_id, &msg);
                replica.step(msg);
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
            }
            Some((req, reply)) = tails.recv() => {
                if let Some(refused) = refused_tail(&req, journal, &replica, me_id, audit) {
                    answer(hooks, audit, me_id, Reply::ReadRedirect, reply, refused);
                    continue;
                }
                parked.open(&mut replica, req.seq, reply, ticks, hooks);
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
            }
            Some((req, reply)) = log_reads.recv() => {
                // A journal read (#185), served from this replica's own
                // chosen prefix — read replicas take read load off the
                // acceptors — or parked at its end.
                parked.journal.serve(
                    |from, max| replica.read_log(from, max),
                    journal,
                    &req,
                    reply,
                    ticks,
                    me_id,
                    hooks,
                    audit,
                );
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                next_tick = time.now() + tunables.tick_interval;
                ticks += 1;
                replica.tick();
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
                parked.expire(ticks, &tunables, &replica, self_id, hooks, audit);
                if let Some((hole, above)) = replica.replica().chosen_gap() {
                    audit.chosen_gap(NodeId(self_id), hole, above);
                    tracing::info!(replica = self_id, hole = hole.0, above = above.0, "chosen_gap");
                }
                tracing::info!(replica = self_id, "replica_tick");
            }
            Some((_req, reply)) = inspects.recv() => {
                // No batch: an inspect reads the replica and its store.
                let _ = reply.send(inspect(&replica));
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

/// Open one lane per node of the pool — a replica only ever asks an
/// acceptor — and the [`Outbound`] over them, riding `edge`'s runtime.
fn acceptor_lanes<P: Providers, A: Audit + Clone + Send + Sync + 'static>(
    providers: &P,
    edge: &RpcEdge<P>,
    tunables: DriverTunables,
    shutdown: &CancellationToken,
    audit: &A,
    me: Party,
    members: Vec<(NodeId, String)>,
) -> SimulationResult<Outbound> {
    let lanes = LaneOpener {
        providers,
        tunables,
        shutdown: shutdown.clone(),
        audit,
        from: me,
    };
    let peer_queues = members
        .into_iter()
        .map(|(node, addr)| {
            let client = well_known(edge.handle(), peer_address(&addr)?);
            let regular = lanes.open(
                "paros-replica-catch-up",
                client,
                Party::Node(node),
                tunables.peer_queue_capacity,
            );
            Ok((node, PeerQueues { regular }))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let out = Outbound {
        peer_queues,
        proxy_queues: BTreeMap::new(),
        learners: Vec::new(),
        sender: me,
    };
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
        ..InspectReply::default()
    }
}
