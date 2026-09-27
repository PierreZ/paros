//! The provider-generic **replica driver** (#144, Compartmentalized Paxos
//! §3.3) — the I/O layer that owns a sans-IO [`paros_core::ReplicaNode`],
//! the fourth driver beside [`run_node`](crate::run_node),
//! [`run_matchmaker`](crate::run_matchmaker) and [`run_proxy`](crate::run_proxy).
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
//! production and deterministic simulation. The loop serves the node
//! contract's **learner subset** — it receives the `Commit`s a leader or a
//! proxy leader sends to every learner, the leader's beats, and the
//! `CatchUpResponse` / `InstallSnapshot` answers its own requests draw,
//! through the same `Deliver` lane a node does — and drains every batch the
//! core produces in the node's own order: persist (the learned records, the
//! chosen index) → send (catch-up requests and pre-reads) → apply the contiguous
//! prefix to the application → application fsync → the compaction floor.
//!
//! **Durable, and never a vote.** A replica keeps its chosen log on the same
//! [`NodeStorage`] a node uses — the boot scan, the format marker (#147) and
//! the durability seams all apply — but it writes a learned record
//! ([`paros_core::WriteOp::Learned`]) where a node writes an accepted one, it
//! answers no `Prepare` and no `Accept`, and it is in no configuration, so no
//! quorum ever counts it. It serves no peer either: a lagging replica is
//! healed from the acceptors, never from another replica, so it offers no
//! snapshot and answers no catch-up.
//!
//! **It serves clients one thing: the leaderless read** (§3.4). The public
//! `QuorumRead` RPC opens a quorum read in the core (the grid row is the
//! node's `read_row` hook), parks the reply exactly as the node driver does,
//! answers it after the apply that covers the confirmed index, and expires
//! it on `read_retry_ticks`. Every other public call is refused as
//! unimplemented.
//!
//! **No snapshot custody, by decision.** A replica records the decided
//! snapshot points it applies, like a node, but it neither advertises
//! custody nor runs the chunk repair plane (`SnapAck` /
//! `SnapChunkRequest` / `SnapChunkResponse` reach the core, which ignores
//! them). The `Truncate` precondition — a quorum advertises custody of a
//! decided snapshot point — exists so that a node left below the new floor
//! can be served a snapshot; the only processes that serve one are
//! acceptors, so the custodians it counts are acceptors, and re-derived with
//! a replica tier it stays exactly that. A replica below the floor is healed
//! by an acceptor's `InstallSnapshot`. A deployment of bare acceptors holds
//! no custody at all, so its leader never truncates: the log grows and no
//! floor forms, the price of an acceptor that keeps no application bytes.
//!
//! A deployment whose `Config::replica_count` is zero runs no replica; the
//! node driver then sends its learner traffic to the pool alone, exactly the
//! plain deployment's messages.

use std::collections::BTreeMap;

use moonpool_core::{Providers, SimulationError, SimulationResult, TimeProvider};
use paros_core::{
    Ballot, Command, Control, MustSync, NodeId, Party, QuorumSystem, ReadState, ReplicaNode, Slot,
    WriteOp,
};
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::driver::boot::check_format_marker;
use crate::driver::edge::{GrpcEdge, edge_reporter};
use crate::driver::events::{message_kind, message_route};
use crate::driver::ready::{
    ParkedRead, ReadPath, crash_if, persist_writes, report_applied, report_snap_recorded,
    storage_fault_crash,
};
use crate::driver::reply::answer;
use crate::driver::transport::{Channels, LaneOpener, Outbound, PeerQueues, send_messages};
use crate::driver::{BootKind, DriverTunables, RunError};
use crate::grpc::{
    InspectReply, LaneRole, ParosInternalClient, ParosInternalServer, ParosServer, ReadAck,
    ReplySender, lane_channel, quorum_system_to_proto,
};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::storage::NodeStorage;

/// Walk the retained chosen prefix back through the application on a
/// (re)boot, exactly as the node driver's boot replay does: from one past the
/// application's durable prefix to the chosen index, a #94 duplicate slot as
/// the `Noop` the live walk applied, a `Snap` marker re-capturing its point.
/// A prefix that cannot be walked — the application behind the floor, or a
/// chosen record this replica cannot read — opens the application repair
/// instead, and the core pulls the missing range from the acceptors.
#[tracing::instrument(level = "debug", skip_all, fields(replica = self_id))]
async fn replay_boot_state<S: NodeStorage, H: DriverHooks, A: Audit>(
    replica: &mut ReplicaNode,
    storage: &mut S,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError> {
    let chosen_index = replica.replica().chosen_index();
    let floor = replica.first_slot();
    audit.replica_booted(NodeId(self_id), chosen_index, floor);
    tracing::info!(
        replica = self_id,
        chosen = chosen_index.map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
        floor = floor.0,
        "replica_booted"
    );
    let Some(ci) = chosen_index else {
        return Ok(());
    };
    let applied_slot = storage.applied_slot();
    let resume = applied_slot.map_or(Slot(0), |a| Slot(a.0.saturating_add(1)));
    let mut repair_from: Option<Slot> = None;
    let mut replayed = false;
    let mut snap_points: Vec<Slot> = Vec::new();
    if resume < floor {
        repair_from = Some(resume);
    } else {
        for s in resume.0..=ci.0 {
            let slot = Slot(s);
            let Some(stored) = replica.replica().chosen_at(slot).cloned() else {
                // A record the boot scan could not read, not yet applied:
                // contiguity is the contract, so the replay stops and the
                // repair pump re-emits the healed range.
                repair_from = Some(slot);
                break;
            };
            let command = if replica.replica().duplicate_slots().contains(&slot) {
                Command::Control(Control::Noop)
            } else {
                stored
            };
            storage
                .apply(ci, slot, &command)
                .await
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            if let Command::Control(Control::Snap { .. }) = command {
                storage
                    .record_snapshot(slot)
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                snap_points.push(slot);
            }
            replayed = true;
            report_applied(audit, self_id, slot, &command);
        }
    }
    if replayed {
        crash_if(
            true,
            hooks,
            audit,
            NodeId(self_id),
            Seam::AfterBootReplayBeforeSync,
        )?;
        storage
            .sync(MustSync::Sync)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }
    report_snap_recorded(audit, self_id, &snap_points);
    if let Some(from) = repair_from {
        let below_floor = from < floor;
        replica.open_app_repair(from);
        audit.app_repair_started(NodeId(self_id), from, below_floor);
        tracing::info!(
            replica = self_id,
            from = from.0,
            below_floor,
            "app_repair_started"
        );
    }
    Ok(())
}

/// Run the [`ReplicaReady`](paros_core::ReplicaReady) handshake once, in the
/// node driver's order and with its seams: persist the learned records and
/// the chosen index (the `BeforeSync` seam inside), send the catch-up
/// requests (after the `AfterSyncBeforeSend` seam), apply the contiguous
/// prefix to the application and fsync it (the `AfterApplyBeforeSync` seam
/// between), and only then make a decided compaction floor durable — a
/// floor never outruns the application state covering the slots it drops.
/// Then release the next page of a deferred walk, and hand back the quorum
/// reads this batch confirmed: answered only now, after the apply that
/// covers them (the node's order, `drain_ready` step 3b).
#[tracing::instrument(level = "trace", skip_all, fields(replica = self_id))]
async fn drain<S: NodeStorage, H: DriverHooks, A: Audit>(
    replica: &mut ReplicaNode,
    storage: &mut S,
    out: &Outbound,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<Vec<ReadState>, RunError> {
    let ready = replica.ready();
    let (truncates, writes): (Vec<WriteOp>, Vec<WriteOp>) = ready
        .writes()
        .iter()
        .cloned()
        .partition(|w| matches!(w, WriteOp::Truncate { .. }));
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

    let chosen_index = replica.replica().chosen_index();
    let mut snap_points: Vec<Slot> = Vec::new();
    for (slot, command) in &committed {
        let chosen_index = chosen_index.ok_or_else(|| {
            SimulationError::InvalidState("committed command without chosen prefix".into())
        })?;
        storage
            .apply(chosen_index, *slot, command)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        if let Command::Control(Control::Snap { .. }) = command {
            storage
                .record_snapshot(*slot)
                .await
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            snap_points.push(*slot);
        }
        report_applied(audit, self_id, *slot, command);
    }
    if !committed.is_empty() {
        crash_if(
            true,
            hooks,
            audit,
            NodeId(self_id),
            Seam::AfterApplyBeforeSync,
        )?;
        storage
            .sync(MustSync::Sync)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }
    report_snap_recorded(audit, self_id, &snap_points);
    if !truncates.is_empty() {
        persist_writes(
            storage,
            &truncates,
            MustSync::Sync,
            Ballot::zero(),
            self_id,
            hooks,
            audit,
        )
        .await?;
    }
    replica.advance_recovery();
    Ok(read_states)
}

/// The replica's held client reads, keyed by the core's `ctx` token.
struct ParkedReads {
    reads: BTreeMap<u64, ParkedRead>,
    next_ctx: u64,
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
        reply: ReplySender<ReadAck>,
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
                ReadAck {
                    seq: parked.seq,
                    leader: replica.leader().map(|n| n.0),
                    committed: true,
                    read_index: read_index.map(|s| s.0),
                },
            );
        }
    }

    /// Answer a retry redirect to every read whose confirmation is overdue
    /// (a row that never answered whole, a watermark this replica has not
    /// reached): the client records it ambiguous and asks again.
    fn expire<H: DriverHooks, A: Audit>(
        &mut self,
        ticks: u64,
        retry_ticks: u64,
        replica: &ReplicaNode,
        self_id: u64,
        hooks: &H,
        audit: &A,
    ) {
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
                    ReadAck {
                        seq: parked.seq,
                        leader: replica.leader().map(|n| n.0),
                        committed: false,
                        read_index: None,
                    },
                );
            }
        }
    }
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
/// `S: NodeStorage` (the injected durable storage, the same trait a node
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
    S: NodeStorage,
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

    let me = Party::Node(NodeId(self_id));
    let on_reject = edge_reporter(audit, me);
    let (service, lane) = lane_channel(LaneRole::Replica, tunables.peer_inbox_capacity, on_reject);
    let mut inbox = lane.deliver;
    let mut inspects = lane.inspect.ok_or_else(|| {
        SimulationError::InvalidState("a replica's lane carries an inspect queue".into())
    })?;
    let mut quorum_reads = lane.quorum_read.ok_or_else(|| {
        SimulationError::InvalidState("a replica's lane carries a quorum-read queue".into())
    })?;
    let grpc_service = tonic::service::Routes::new(ParosServer::new(service.clone()))
        .add_service(ParosInternalServer::new(service))
        .prepare();
    let mut edge = GrpcEdge::bind(
        &providers,
        &local_addr,
        "paros-replica-grpc-server",
        "replica",
        &tunables,
        grpc_service,
        incarnation_shutdown.clone(),
    )
    .await?;

    let mut replica = ReplicaNode::new(&storage);
    replay_boot_state(&mut replica, &mut storage, self_id, hooks, audit).await?;

    let (_channels, out) = acceptor_lanes(
        &providers,
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
    };
    let mut ticks: u64 = 0;
    let time = providers.time().clone();
    let mut next_tick = time.now() + tunables.tick_interval;
    loop {
        moonpool_core::select! {
            accepted = edge.serve_next(&providers) => accepted?,
            Some(msg) = inbox.recv() => {
                trace_received(self_id, &msg);
                replica.step(msg);
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
            }
            Some((req, reply)) = quorum_reads.recv() => {
                parked.open(&mut replica, req.seq, reply, ticks, hooks);
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                next_tick = time.now() + tunables.tick_interval;
                ticks += 1;
                replica.tick();
                let served = drain(&mut replica, &mut storage, &out, self_id, hooks, audit).await?;
                parked.answer_served(&served, &replica, self_id, hooks, audit);
                parked.expire(ticks, tunables.read_retry_ticks, &replica, self_id, hooks, audit);
                if let Some((hole, above)) = replica.replica().chosen_gap() {
                    audit.chosen_gap(NodeId(self_id), hole, above);
                    tracing::info!(replica = self_id, hole = hole.0, above = above.0, "chosen_gap");
                }
                tracing::info!(replica = self_id, "replica_tick");
            }
            Some((_req, reply)) = inspects.recv() => {
                // No batch: an inspect reads the replica and its store.
                let _ = reply.send(inspect(&replica, &storage).await);
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

/// Open one lane per node of the pool — a replica only ever asks an
/// acceptor — and the [`Outbound`] over them. The channels are returned so
/// the caller keeps them alive for the incarnation.
fn acceptor_lanes<P: Providers, A: Audit + Clone + Send + Sync + 'static>(
    providers: &P,
    tunables: DriverTunables,
    shutdown: &CancellationToken,
    audit: &A,
    me: Party,
    members: Vec<(NodeId, String)>,
) -> SimulationResult<(Channels<P>, Outbound)> {
    let mut channels = Channels::with_capacity(members.len());
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
            let client =
                channels.connect(providers, &tunables, addr, ParosInternalClient::with_origin)?;
            let regular = lanes.open(
                "paros-grpc-replica-catch-up",
                client,
                Party::Node(node),
                tunables.peer_queue_capacity,
            );
            Ok((
                node,
                PeerQueues {
                    regular,
                    snapshot: None,
                },
            ))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let out = Outbound {
        peer_queues,
        proxy_queues: BTreeMap::new(),
        learners: Vec::new(),
        sender: me,
    };
    Ok((channels, out))
}

/// Answer an operator's or a probe's `Inspect`: the replica's chosen prefix,
/// its floor and its application's state — the opaque snapshot a probe
/// compares across every applier. The configuration fields name the
/// bootstrap acceptors this replica learns from; a replica never leads and
/// holds no matchmaker belief or GC floor.
async fn inspect<S: NodeStorage>(replica: &ReplicaNode, storage: &S) -> InspectReply {
    let (quorum_system, phase1_quorum, phase2_quorum, rows, cols) =
        quorum_system_to_proto(replica.config().quorum_system).into_parts();
    InspectReply {
        chosen_index: replica.replica().chosen_index().map(|slot| slot.0),
        first_slot: replica.first_slot().0,
        snapshot: storage.snapshot().await,
        members: replica.config().peers.iter().map(|n| n.0).collect(),
        quorum_system,
        phase1_quorum,
        phase2_quorum,
        rows,
        cols,
        ..InspectReply::default()
    }
}
