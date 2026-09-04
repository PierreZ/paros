//! The [`paros_core::Ready`] handshake's I/O side: one linear durability
//! pipeline (persist → send → apply → app-fsync → truncate → offers → acks),
//! the durable-write staging and reporting it splits into, the held client
//! replies it answers, and the driver's fail-stop storage-fault decision.

use std::collections::BTreeMap;

use moonpool_core::SimulationError;
use paros_core::{
    AcceptorWrite, Ballot, ColocatedNode, Command, ConfigId, Control, GcRequest, MatchRequest,
    MatchmakerId, Message, NodeId, NodeRole, ReadState, SessionEntry, Slot, Value, WriteOp,
};

use crate::audit::{Audit, StorageFaultDecision};
use crate::grpc::{ProposeAck, ReadAck, ReplySender};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::storage::{NodeStorage, StorageError};

use super::config::RunError;
use super::events::{command_hash, message_kind};
use super::transport::{Outbound, send_messages, trace_send_drop};

/// The client replies this node is holding open: proposals wait on their
/// slot's commit (ack-on-commit), reads wait on their read-index round's
/// confirmation `(client seq, tick parked at, the held reply)`, keyed by the
/// core's `ctx` token.
#[derive(Default)]
pub(crate) struct ClientWaiters {
    /// `(client id, client seq, the held reply)` per slot.
    pub(crate) pending: BTreeMap<Slot, Vec<(u64, u64, ReplySender<ProposeAck>)>>,
    pub(crate) pending_reads: BTreeMap<u64, (u64, u64, ReplySender<ReadAck>)>,
}

/// Materialize and send this batch's snapshot offers. An offered snapshot must
/// describe exactly the application prefix named by the protocol message, so
/// this runs only after the batch's committed entries are durably applied.
#[tracing::instrument(level = "debug", skip_all, fields(offers = snapshot_offers.len()))]
async fn send_snapshot_offers<S, H, A>(
    storage: &mut S,
    out: &Outbound,
    hooks: &H,
    audit: &A,
    snapshot_offers: &[(NodeId, Slot, Ballot, ConfigId)],
    sessions: &[SessionEntry],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    for &(to, offered_index, ballot, config_id) in snapshot_offers {
        // The mismatch skip below, taken spuriously: the requester re-asks
        // every tick and any other custodian may answer, so an unserved beat
        // is always safe — and this reaches the "nobody served me this round"
        // state without needing an application repair to be open.
        //
        // Deliberately *not* reported through `Audit::snapshot_offer_skipped`:
        // that channel's coverage gate claims a **mismatched** offer was
        // withheld, and a hook that can fire on a perfectly matched offer would
        // satisfy it trivially. The hook's own BUGGIFY pairing proves this
        // location fires; the trace field says which of the two skips a reader
        // is looking at.
        if hooks.skip_snapshot_offer(to) {
            tracing::info!(
                node = out.self_id,
                offered = offered_index.0,
                reason = "hook",
                "snapshot_offer_skipped"
            );
            continue;
        }
        if storage.applied_slot() != Some(offered_index) {
            // An offered snapshot must describe exactly the application prefix
            // the protocol message names. Stage 8 makes a mismatch a
            // legitimate transient — an open application repair holds the
            // applied prefix behind the chosen index — so the offer is
            // *skipped*, never sent wrong and never fatal: the requester
            // re-asks each beat and another peer (or this one, once healed)
            // serves it. The core already withholds offers while its own
            // repair is open; this driver-side guard covers any other
            // application lag the core cannot see.
            audit.snapshot_offer_skipped(NodeId(out.self_id), offered_index);
            tracing::info!(
                node = out.self_id,
                offered = offered_index.0,
                reason = "mismatch",
                "snapshot_offer_skipped"
            );
            continue;
        }
        let message = Message::InstallSnapshot {
            config_id,
            from: NodeId(out.self_id),
            ballot,
            chosen_index: offered_index,
            snapshot: Value(storage.snapshot().await),
            // The at-most-once ledger travels beside the opaque bytes (#94):
            // the receiver seals it so its duplicate-suppression decisions for
            // the folded prefix match every peer's.
            sessions: sessions.to_vec(),
        };
        if hooks.drop_outgoing(to, &message) {
            trace_send_drop(audit, out.self_id, to, &message);
            continue;
        }
        out.transmit(hooks, audit, to, &message);
        if hooks.duplicate_outgoing(to, &message) {
            audit.duplicated_at_send(NodeId(out.self_id), to, &message);
            tracing::info!(
                node = out.self_id,
                to = to.0,
                kind = message_kind(&message),
                "msg_duplicated_at_send"
            );
            out.transmit(hooks, audit, to, &message);
        }
    }
}

/// Surface the #88 window: a snapshot install persisted while this node's own
/// campaign is open (`on_install_snapshot` deliberately does not touch the
/// election), so the sweep can prove the interleaving is visited.
fn note_mid_election_snapshot<A: Audit>(
    node: &ColocatedNode,
    writes: &[WriteOp],
    self_id: u64,
    audit: &A,
) {
    if node.role() == NodeRole::Candidate
        && writes
            .iter()
            .any(|w| matches!(w, WriteOp::InstallSnapshot { .. }))
    {
        audit.snapshot_mid_election(NodeId(self_id));
        tracing::info!(node = self_id, "snapshot_mid_election");
    }
}

/// Ack-on-commit: only now can a client learn success — both the chosen index
/// and the application transition are durable. Controls have no proposal
/// waiter. The reply may be deliberately dropped at the reply seam
/// ([`DriverHooks::drop_client_reply`]): the server state has advanced either
/// way, and the client's retry takes the `(client, seq)` dedup path.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, committed = committed.len()))]
fn ack_committed_waiters<S, H, A>(
    storage: &S,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
    self_id: u64,
    committed: &[(Slot, Command)],
) where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    for (slot, command) in committed {
        let Some(replies) = waiters.pending.remove(slot) else {
            continue;
        };
        // The slot's decided identity. A reply may only claim `committed: true`
        // if the slot decided *this waiter's* command: a stale leader can park
        // a proposal on a slot the majority then decides differently (it
        // learns the decision by `Commit`/catch-up while still believing
        // itself leader — nothing in `on_commit` demotes it), and acking by
        // slot number alone then told a client its write was committed while
        // no node ever applied it (network-axis seed 12491191414293127136).
        // A control command — including a #94 duplicate suppressed to a Noop —
        // matches no waiter.
        let decided = command.user().map(|e| (e.client.0, e.seq.0));
        for (client, seq, waiter) in replies {
            if decided != Some((client, seq)) {
                // Not this proposal's commit: its fate is unknown here (the
                // core's dedup tables track it if it is still in flight
                // anywhere). Answer a retry-now redirect instead of holding
                // the reply to the client's deadline; the retry goes through
                // the honest `(client, seq)` dedup path.
                audit.waiter_superseded(NodeId(self_id), *slot);
                tracing::info!(node = self_id, slot = slot.0, "propose_waiter_superseded");
                let _ = waiter.send(ProposeAck {
                    seq,
                    leader: Some(self_id),
                    committed: false,
                    slot: None,
                });
                continue;
            }
            audit.client_acked(
                NodeId(self_id),
                client,
                seq,
                *slot,
                storage.applied_slot(),
                false,
            );
            if hooks.drop_client_reply(Reply::Propose) {
                audit.client_reply_dropped(NodeId(self_id), Reply::Propose);
                tracing::info!(node = self_id, reply = "propose", "client_reply_dropped");
                continue;
            }
            let _ = waiter.send(ProposeAck {
                seq,
                leader: Some(self_id),
                committed: true,
                slot: Some(slot.0),
            });
        }
    }
}

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
// One linear durability pipeline: every step is ordered against its neighbors
// (persist → send → apply → app-fsync → truncate → offers → acks), so slicing
// it into helpers would scatter the ordering contract this function *is*.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all, fields(node = node.config().id.0))]
pub(crate) async fn drain_ready<S, H, A>(
    node: &mut ColocatedNode,
    storage: &mut S,
    out: &Outbound,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
) -> Result<Outbox, RunError>
where
    S: NodeStorage,
    H: DriverHooks,
    A: Audit,
{
    let self_id = out.self_id;
    // The deployment map an `Audience` is resolved against, read before the
    // batch takes the node's borrow.
    let pool: Vec<NodeId> = node.config().pool().to_vec();
    // Copy the batch out of the borrow guard, advance to release the gate, then
    // perform I/O — persist → send → apply. Advancing before the I/O is the
    // documented async pattern; persist-before-send still holds because the
    // persist loop below precedes the send loop.
    let ready = node.ready();
    // A durable compaction floor must never outrun the durable *application*
    // state covering the slots it drops: flushing a `Truncate` in step 1
    // discards the accepted records, and a crash at the `AfterApplyBeforeSync`
    // seam then lands a node whose application prefix is behind a floor nothing
    // can replay — its apply stream stays shifted forever (network-axis seed
    // 8398193358524544360). Split the truncates out of the batch and flush them
    // only after the application fsync below. A truncate lost to a crash in
    // that window is safe: the floor is pure space reclamation, re-raised by
    // the next decided `Truncate`.
    let (truncates, writes): (Vec<WriteOp>, Vec<WriteOp>) = ready
        .writes()
        .to_vec()
        .into_iter()
        .partition(|w| matches!(w, WriteOp::Truncate { .. }));
    let must_sync = if writes.iter().any(WriteOp::needs_sync) {
        paros_core::MustSync::Sync
    } else {
        paros_core::MustSync::Relaxed
    };
    // The deployment map, applied: the core hands out audiences (one entry
    // per fan-out), the driver turns each into the node ids its own pool
    // names, in order, and sends. The bytes and their order are exactly what
    // an enumerated batch carried.
    let messages: Vec<(NodeId, Message)> = ready
        .messages()
        .iter()
        .flat_map(|(audience, msg)| {
            audience
                .resolve(&pool, NodeId(self_id))
                .into_iter()
                .map(move |to| (to, msg.clone()))
        })
        .collect();
    let committed: Vec<(Slot, Command)> = ready.committed().to_vec();
    let snapshot_offers: Vec<(NodeId, Slot, Ballot, ConfigId)> = ready.snapshot_offers().to_vec();
    let read_states: Vec<ReadState> = ready.read_states().to_vec();
    let recovery_batch = ready.recovery_batch();
    // The matchmaking requests ride the same persist-before-send edge as the
    // peer messages (the candidate's promise raise is in this batch) and are
    // handed back to the loop, which owns the matchmaker links.
    let match_requests: Vec<(MatchmakerId, MatchRequest)> = ready.match_requests().to_vec();
    // The GC requests (#123) ride the same edge: the leader's own fence
    // tally decided them, and they leave only with the batch.
    let gc_requests: Vec<(MatchmakerId, GcRequest)> = ready.gc_requests().to_vec();
    ready.advance();
    let gc_fence = node.gc_fence();

    // 1. Persist durable writes FIRST, each op in order, flush per MustSync, and
    //    surface the persisted state for the safety + recovery oracles. The
    //    `BeforeSync` crash seam lives inside `persist_writes`.
    let promised = node.hard_state().max_promised_ballot;
    persist_writes(storage, &writes, must_sync, promised, self_id, hooks, audit).await?;

    if let Some((started, gap_fills, remaining)) = recovery_batch {
        let started = u64::try_from(started).unwrap_or(u64::MAX);
        let gap_fills = u64::try_from(gap_fills).unwrap_or(u64::MAX);
        let remaining = u64::try_from(remaining).unwrap_or(u64::MAX);
        audit.recovery_batch(NodeId(self_id), started, gap_fills, remaining);
        tracing::info!(
            node = self_id,
            started,
            gap_fills,
            remaining,
            "leader_recovery_batch"
        );
        if gap_fills > 0 {
            tracing::info!(node = self_id, gaps = gap_fills, "election_gap_filled");
        }
    }

    note_mid_election_snapshot(node, &writes, self_id, audit);

    // Snapshot offers are outbound protocol messages too. Count them before the
    // after-sync seam so a crash can drop an offer-only batch just as it can any
    // other outbound batch. Their bytes are materialized after application below:
    // an application snapshot must cover exactly the boundary it advertises.
    let snapshot_offer_count = snapshot_offers.len();
    if snapshot_offer_count > 0 {
        audit.snapshot_offered(
            NodeId(self_id),
            u64::try_from(snapshot_offer_count).unwrap_or(u64::MAX),
        );
        tracing::info!(
            node = self_id,
            snapshot_offers = snapshot_offer_count as u64,
            "snapshot_offered"
        );
    }

    // Crash seam: after the batch is durable but before its messages leave. The
    // durable writes survive; the batch's messages are dropped (never sent), so a
    // recovered node must re-derive them. Only meaningful when there is durable
    // work or a message to lose.
    if (!writes.is_empty()
        || !messages.is_empty()
        || !match_requests.is_empty()
        || !gc_requests.is_empty()
        || snapshot_offer_count > 0)
        && hooks.crash_at(Seam::AfterSyncBeforeSend)
    {
        audit.crashed(NodeId(self_id), Seam::AfterSyncBeforeSend);
        tracing::info!(
            node = self_id,
            seam = "after_sync_before_send",
            snapshot_offers = snapshot_offer_count as u64,
            "crashed"
        );
        return Err(RunError::SeamCrash(Seam::AfterSyncBeforeSend));
    }

    // 2. Send messages — only after (1) is durable.
    send_messages(out, hooks, audit, messages);

    // 3. Apply newly chosen entries (already durable, in contiguous order) —
    //    surface them to the oracles and ack any clients waiting on each slot
    //    (ack-on-commit: a held reply fires only now that its slot is chosen).
    let chosen_index = node.hard_state().chosen_index;
    let mut snap_markers: Vec<Slot> = Vec::new();
    for (slot, command) in &committed {
        let chosen_index = chosen_index.ok_or_else(|| {
            SimulationError::InvalidState("committed command without chosen prefix".into())
        })?;
        storage
            .apply(chosen_index, *slot, command)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        // A decided snapshot point (#101): the marker's boundary state is the
        // application state at exactly this instant of the contiguous walk,
        // so the point is captured here, mid-loop, and flushed with the
        // batch's application fsync below. The point is recorded at the
        // marker's *own slot* — a marker minted by `propose_snap_marker`
        // carries the identical `at_index`, and a hand-built mismatch is
        // external input (never asserted, only noted).
        if let Command::Control(Control::Snap { at_index }) = command {
            if at_index != slot {
                tracing::warn!(
                    node = self_id,
                    slot = slot.0,
                    at_index = at_index.0,
                    "snap_marker_index_mismatch"
                );
            }
            storage
                .record_snapshot(*slot)
                .await
                .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            snap_markers.push(*slot);
        }
        let vhash = command_hash(command);
        audit.applied(
            NodeId(self_id),
            *slot,
            vhash,
            match command {
                Command::User(e) => Some((e.client.0, e.seq.0)),
                Command::Control(_) => None,
            },
        );
        tracing::info!(node = self_id, slot = slot.0, vhash, "value_chosen");
        tracing::info!(
            node = self_id,
            slot = slot.0,
            applied_index = slot.0,
            "log_applied"
        );
    }

    // Application state is part of the durable replica contract. Flush all
    // staged transitions before an acknowledgement can escape this batch. This
    // also makes a chosen-index-only Ready durable before its application effect,
    // so reboot replay can never observe an application prefix ahead of consensus.
    if !committed.is_empty() {
        // Crash seam: the consensus prefix is durable and the application
        // transitions are staged, but their fsync has not happened. A crash
        // here is the only way to land "consensus ahead of application" on
        // disk — the state the boot replay's idempotent re-apply heals.
        if hooks.crash_at(Seam::AfterApplyBeforeSync) {
            audit.crashed(NodeId(self_id), Seam::AfterApplyBeforeSync);
            tracing::info!(node = self_id, seam = "after_apply_before_sync", "crashed");
            return Err(RunError::SeamCrash(Seam::AfterApplyBeforeSync));
        }
        storage
            .sync(paros_core::MustSync::Sync)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }

    // The decided snapshot points captured above are durable with the
    // application fsync; only now are they reported (never claiming a point a
    // crash-before-sync would discard).
    for at in &snap_markers {
        audit.snap_recorded(NodeId(self_id), *at);
        tracing::info!(node = self_id, at = at.0, "snap_recorded");
    }

    // Only now that the application state covering the dropped slots is
    // fsync-durable may the compaction floor become durable (see the batch
    // split above). Runs through the same persist path, so the truncate keeps
    // its `BeforeSync` crash location and its after-fsync audit report.
    if !truncates.is_empty() {
        persist_writes(
            storage,
            &truncates,
            paros_core::MustSync::Sync,
            promised,
            self_id,
            hooks,
            audit,
        )
        .await?;
    }

    if !snapshot_offers.is_empty() {
        let sessions = node.session_ledger();
        send_snapshot_offers(storage, out, hooks, audit, &snapshot_offers, &sessions).await;
    }

    ack_committed_waiters(storage, waiters, hooks, audit, self_id, &committed);

    // 3b. Answer confirmed reads — after the apply loop, so the applied prefix
    //     this same batch carried is covered by what the read observes. The ack
    //     reports the *serve-time* chosen index (at or past the confirmed read
    //     index): that is the local state actually served.
    for state in &read_states {
        if let Some((seq, _, waiter)) = waiters.pending_reads.remove(&state.ctx) {
            let read_index = node.hard_state().chosen_index;
            audit.read_confirmed(NodeId(self_id), read_index);
            if hooks.drop_client_reply(Reply::Read) {
                audit.client_reply_dropped(NodeId(self_id), Reply::Read);
                tracing::info!(node = self_id, reply = "read", "client_reply_dropped");
                continue;
            }
            let _ = waiter.send(ReadAck {
                seq,
                leader: Some(self_id),
                committed: true,
                read_index: read_index.map(|s| s.0),
            });
        }
    }

    // The previous recovery page is now fully durable, sent, and applied. Only
    // at this boundary may the core materialize the next bounded Ready page;
    // doing it inside `Ready::advance` would move single-node state ahead of the
    // I/O the async driver is still performing.
    node.advance_recovery();

    Ok(Outbox {
        match_requests,
        gc_requests,
        gc_fence,
    })
}

/// Persist a batch's [`WriteOp`]s in order (persist-before-send step 1), flush per
/// [`MustSync`], and surface the persisted state for the safety + recovery
/// oracles: a `node_state` event when the promised ballot rose, and a per-slot
/// `persist` event for each accepted append. `promised` is the node's post-batch
/// promise (`>=` any accept ballot in the batch).
///
/// The observability events are emitted only **after** the fsync, so they never
/// claim a write the `BeforeSync` crash seam then discards: a crash before the
/// fsync loses the whole un-synced batch and emits nothing, exactly as a real
/// crash-before-flush would.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, writes = writes.len(), must_sync = ?must_sync))]
async fn persist_writes<S: NodeStorage, H: DriverHooks, A: Audit>(
    storage: &mut S,
    writes: &[WriteOp],
    must_sync: paros_core::MustSync,
    promised: Ballot,
    self_id: u64,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError> {
    let mut promise_changed = false;
    for op in writes {
        match op {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => {
                storage
                    .persist_ballot(*ballot)
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
                promise_changed = true;
            }
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            }) => {
                storage
                    .append_accepted(*slot, *ballot, command.clone())
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::SetChosenIndex(slot) => {
                storage
                    .set_chosen_index(*slot)
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::Truncate { first, sealed } => {
                storage
                    .truncate(*first, sealed)
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                snapshot,
                sessions,
            } => {
                storage
                    .install_snapshot(*chosen_index, *ballot, snapshot.0.clone(), sessions)
                    .await
                    .map_err(|e| storage_fault_crash(audit, self_id, e))?;
            }
        }
    }

    // Crash seam: the batch is staged but not yet flushed. A crash here loses the
    // whole un-synced batch (and no message has been sent), so surface nothing but
    // the crash marker itself. Only meaningful when the batch actually staged
    // something.
    if !writes.is_empty() && hooks.crash_at(Seam::BeforeSync) {
        audit.crashed(NodeId(self_id), Seam::BeforeSync);
        tracing::info!(node = self_id, seam = "before_sync", "crashed");
        return Err(RunError::SeamCrash(Seam::BeforeSync));
    }

    if !writes.is_empty() {
        storage
            .sync(must_sync)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        // Durability marker: whether this batch was fsync'd (a promise-raise or
        // accept — `MustSync::Sync`) or a relaxed write (a chosen-index-only
        // advance). The persist/send-seam animation renders it as a filled vs
        // hollow tick.
        tracing::info!(
            node = self_id,
            sync = (must_sync == paros_core::MustSync::Sync),
            writes = u64::try_from(writes.len()).unwrap_or(u64::MAX),
            "synced"
        );
    }

    surface_persisted(writes, promised, promise_changed, self_id, audit);
    Ok(())
}

/// Report a flushed batch's durable state — one audit callback and one tracing
/// event per op. Split out of [`persist_writes`] so the staging half and the
/// reporting half each stay readable; both loops walk `writes` in order.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn surface_persisted<A: Audit>(
    writes: &[WriteOp],
    promised: Ballot,
    promise_changed: bool,
    self_id: u64,
    audit: &A,
) {
    // Durable now — emit the truthful persisted state for the oracles.
    if promise_changed {
        audit.promised(NodeId(self_id), promised);
        tracing::info!(
            node = self_id,
            pround = promised.round,
            pbnode = promised.node.0,
            "node_state"
        );
    }
    for op in writes {
        match op {
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            }) => {
                let vhash = command_hash(command);
                audit.accepted(NodeId(self_id), *slot, *ballot, promised, vhash);
                tracing::info!(
                    node = self_id,
                    slot = slot.0,
                    pround = promised.round,
                    pbnode = promised.node.0,
                    around = ballot.round,
                    abnode = ballot.node.0,
                    vhash,
                    "persist"
                );
            }
            WriteOp::SetChosenIndex(slot) => {
                audit.chosen_index(NodeId(self_id), *slot);
            }
            WriteOp::Truncate { first, .. } => {
                audit.truncated(NodeId(self_id), *first);
                tracing::info!(node = self_id, first = first.0, "compacted");
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                ..
            } => {
                let first = chosen_index.0 + 1;
                // The install jumps the applied prefix to `chosen_index` without
                // replaying entries (snapshot-xor-entries); the audit callback
                // reports both the install and that jump.
                audit.snapshot_installed(NodeId(self_id), *chosen_index, *ballot);
                tracing::info!(
                    node = self_id,
                    chosen_index = chosen_index.0,
                    first,
                    "snapshot_installed"
                );
                // Surface the jump so the no-gaps oracle (which admits it as a
                // snapshot jump) and the convergence oracle see the node reach the
                // cluster prefix.
                tracing::info!(
                    node = self_id,
                    slot = chosen_index.0,
                    applied_index = chosen_index.0,
                    "log_applied"
                );
            }
            WriteOp::Acceptor(AcceptorWrite::SetPromise(_)) => {}
        }
    }
}

/// Map a [`StorageError`] into the driver's **deliberate crash decision**: a
/// storage fault never lets the node keep running on state it does not durably
/// have. The decision is reported through [`Audit::storage_fault`] (typed, at
/// the instant it is made) and surfaced as [`EV_STORAGE_FAULT`], then
/// [`RunError::Storage`] unwinds the incarnation. Production semantics: a
/// storage fault is a process exit (crash-only); the sim's node loop matches
/// the variant and routes to the crash/restart path instead.
pub(crate) fn storage_fault_crash<A: Audit>(audit: &A, self_id: u64, e: StorageError) -> RunError {
    audit.storage_fault(NodeId(self_id), &e, StorageFaultDecision::Crash);
    tracing::warn!(node = self_id, error = %e, decision = "crash", "storage_fault");
    RunError::Storage(e)
}

/// What one drained batch hands the loop to send over the matchmaker wire
/// (the loop owns the links, the drain owns the persist-before-send edge).
pub(crate) struct Outbox {
    pub(crate) match_requests: Vec<(MatchmakerId, MatchRequest)>,
    pub(crate) gc_requests: Vec<(MatchmakerId, GcRequest)>,
    /// The election fence the GC requests were licensed by (audit context).
    pub(crate) gc_fence: Option<Slot>,
}
