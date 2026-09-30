//! The [`paros_core::Ready`] handshake's I/O side: one linear durability
//! pipeline (persist → send → learn → acks), the durable-write staging and
//! reporting it splits into, the held client replies it answers, and the
//! driver's fail-stop storage-fault decision.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorWrite, Ballot, ColocatedNode, Command, GcRequest, MatchRequest, MatchmakerId, Message,
    NodeId, NodeRole, Party, ReadState, Slot, WriteOp,
};

use crate::audit::{Audit, StorageFaultDecision};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::rpc::{AppendAck, CheckTailAck, ReplySender};
use crate::storage::{LogStorage, StorageError};

use super::config::RunError;
use super::events::command_hash;
use super::reply::answer;
use super::transport::{Outbound, send_messages};

/// The client replies this node is holding open: proposals wait on their
/// slot's commit (ack-on-commit), reads wait on their confirmation — a
/// read-index round or a quorum read — keyed by the core's `ctx` token (one
/// counter for both tallies, so a token names one read whichever served it).
#[derive(Default)]
pub(crate) struct ClientWaiters {
    /// `(client id, client seq, the held reply)` per slot.
    pub(crate) pending: BTreeMap<Slot, Vec<(u64, u64, ReplySender<AppendAck>)>>,
    pub(crate) pending_reads: BTreeMap<u64, ParkedRead>,
    /// Journal reads long-polling at the end (#185).
    pub(crate) log_reads: super::log_reads::LogReads,
}

/// Which of the two read tallies a parked read waits on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadPath {
    /// The leader's read-index round (`ColocatedNode::read_index`): bound to
    /// the leadership that opened it, redirected when it ends.
    Index,
    /// A leaderless quorum read (`ColocatedNode::quorum_read_in`, #143):
    /// bound to no role, so a leadership change never touches it.
    Quorum {
        /// The grid row the read asked (`None`: the whole configuration).
        row: Option<usize>,
        /// This node's chosen index when the read opened — what a local,
        /// unconfirmed read would have served.
        opened: Option<Slot>,
    },
}

/// One held client read.
pub(crate) struct ParkedRead {
    /// The client's seq, echoed in the answer.
    pub(crate) seq: u64,
    /// The driver tick the read was parked at (its confirmation deadline).
    pub(crate) parked_at: u64,
    /// The tally it waits on.
    pub(crate) path: ReadPath,
    /// The held reply.
    pub(crate) reply: ReplySender<CheckTailAck>,
}

/// The prefix this node's acks and reads are answered from: its contiguous
/// chosen prefix. paros runs no application (#186), so the journal a client
/// reads *is* the chosen log.
pub(crate) fn served_prefix(node: &ColocatedNode) -> Option<Slot> {
    node.replica().chosen_index()
}

/// The acks a batch can answer: every parked proposal whose slot is inside
/// the contiguous chosen prefix, paired with the command chosen there (a #94
/// duplicate as the `Noop` the walk treats it as, a slot no longer retained
/// — trimmed, or jumped over below a peer's trim point — as a `Noop` too;
/// neither matches a waiter, so the client retries through the dedup path).
/// The ack path then judges the identity. Swept over the whole prefix, not
/// only the slots this batch walked, so a jump that chose slots without
/// walking them still answers the proposals parked there.
fn chosen_waiters(node: &ColocatedNode, waiters: &ClientWaiters) -> Vec<(Slot, Command)> {
    let Some(ci) = node.replica().chosen_index() else {
        return Vec::new();
    };
    waiters
        .pending
        .range(..=ci)
        .map(|(slot, _)| {
            let chosen = node
                .replica()
                .chosen_at(*slot)
                .filter(|_| !node.replica().duplicate_slots().contains(slot))
                .cloned()
                .unwrap_or(Command::Control(paros_core::Control::Noop));
            (*slot, chosen)
        })
        .collect()
}

/// Ack-on-commit: only now can a client learn success — the chosen index is
/// durable. Controls have no proposal waiter. The reply may be deliberately dropped at the reply seam
/// ([`DriverHooks::drop_client_reply`]): the server state has advanced either
/// way, and the client's retry takes the `(client, seq)` dedup path.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, committed = committed.len()))]
fn ack_committed_waiters<H, A>(
    applied: Option<Slot>,
    waiters: &mut ClientWaiters,
    hooks: &H,
    audit: &A,
    self_id: u64,
    committed: &[(Slot, Command)],
) where
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
                let _ = waiter.send(AppendAck {
                    seq,
                    leader: Some(self_id),
                    committed: false,
                    first_lsn: None,
                    unknown_journal: false,
                });
                continue;
            }
            audit.client_acked(NodeId(self_id), client, seq, *slot, applied, false);
            answer(
                hooks,
                audit,
                NodeId(self_id),
                Reply::Propose,
                waiter,
                AppendAck {
                    seq,
                    leader: Some(self_id),
                    committed: true,
                    first_lsn: Some(slot.0),
                    unknown_journal: false,
                },
            );
        }
    }
}

/// Report one leader-recovery batch this `Ready` carried: how many recovered
/// slots it started, how many of them were gap fills, and how many remain.
fn report_recovery_batch<A: Audit>(audit: &A, self_id: u64, batch: (usize, usize, usize)) {
    let (started, gap_fills, remaining) = batch;
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

/// Run the [`paros_core::Ready`] handshake once, honoring persist-before-send:
/// persist `hard_state`, *then* send the addressed messages, *then* surface the
/// chosen entries — and emit the observability events the safety oracle reads.
// One linear durability pipeline: every step is ordered against its neighbors
// (persist → send → learn → acks), so slicing it into helpers would scatter
// the ordering contract this function *is*.
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
    S: LogStorage,
    H: DriverHooks,
    A: Audit,
{
    let self_id = out.self_node().0;
    // The journal every message of this batch is framed by (#188).
    let journal = node.config().journal;
    // The replica tier serves one journal (#188): a journal with no replica
    // in its configuration never addresses one.
    let with_learners = node.config().replica_count > 0;
    // The deployment map an `Audience` is resolved against, read before the
    // batch takes the node's borrow.
    let pool: Vec<NodeId> = node.pool().to_vec();
    // Copy the batch out of the borrow guard, advance to release the gate, then
    // perform I/O — persist → send → apply. Advancing before the I/O is the
    // documented async pattern; persist-before-send still holds because the
    // persist loop below precedes the send loop.
    let ready = node.ready();
    // One flush for the whole batch, truncates included: the split that held
    // a `Truncate` behind the application fsync protected an application
    // prefix, and there is none (#186) — a floor only ever moves inside the
    // durable chosen prefix, which the same flush carries.
    let writes: Vec<WriteOp> = ready.writes().to_vec();
    let must_sync = if writes.iter().any(WriteOp::needs_sync) {
        paros_core::MustSync::Sync
    } else {
        paros_core::MustSync::Relaxed
    };
    // The deployment map, applied: the core hands out audiences (one entry
    // per fan-out), the driver turns each into the parties its own map
    // names — the node ids of the pool, or the one proxy leader a
    // delegation is for (#142) — in order, and sends. The bytes and their
    // order are exactly what an enumerated batch carried.
    let messages: Vec<(Party, Message)> = ready
        .messages()
        .iter()
        .flat_map(|(audience, msg)| {
            let proxy = audience.proxy().map(Party::Proxy);
            let nodes = out
                .resolve(audience, &pool)
                .into_iter()
                .filter(|node| with_learners || !out.learners.contains(node))
                .map(Party::Node);
            proxy
                .into_iter()
                .chain(nodes)
                .map(move |to| (to, msg.clone()))
        })
        .collect();
    let committed: Vec<(Slot, Command)> = ready.committed().to_vec();
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

    if let Some(batch) = recovery_batch {
        report_recovery_batch(audit, self_id, batch);
    }

    // Crash seam: after the batch is durable but before its messages leave. The
    // durable writes survive; the batch's messages are dropped (never sent), so a
    // recovered node must re-derive them. Only meaningful when there is durable
    // work or a message to lose.
    if (!writes.is_empty()
        || !messages.is_empty()
        || !match_requests.is_empty()
        || !gc_requests.is_empty())
        && hooks.crash_at(Seam::AfterSyncBeforeSend)
    {
        audit.crashed(NodeId(self_id), Seam::AfterSyncBeforeSend);
        tracing::info!(
            node = self_id,
            seam = Seam::AfterSyncBeforeSend.label(),
            "crashed"
        );
        return Err(RunError::SeamCrash(Seam::AfterSyncBeforeSend));
    }

    // 2. Send messages — only after (1) is durable.
    send_messages(out, hooks, audit, journal, messages);

    // 3. Learn the entries the chosen prefix walked over (already durable, in
    //    contiguous order) — surface them to the oracles and ack any clients
    //    waiting on a slot the prefix now covers (ack-on-commit: a held reply
    //    fires only now that its slot is chosen).
    for (slot, command) in &committed {
        report_applied(audit, self_id, *slot, command);
    }
    let applied = served_prefix(node);
    let decided = chosen_waiters(node, waiters);
    ack_committed_waiters(applied, waiters, hooks, audit, self_id, &decided);

    // 3b. Answer confirmed reads — after the learn step, so the prefix this
    //     same batch carried is covered by what the read observes. The ack
    //     reports the *serve-time* chosen index (at or past the confirmed read
    //     index): that is the local state actually served.
    answer_confirmed_reads(node, waiters, &read_states, hooks, audit, self_id);

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

/// Answer the reads this batch confirmed: a parked read-index or quorum read
/// whose `ctx` came back in `Ready::read_states`, answered with the
/// serve-time chosen index (at or past the confirmed read index — the local
/// state actually served).
fn answer_confirmed_reads<H: DriverHooks, A: Audit>(
    node: &ColocatedNode,
    waiters: &mut ClientWaiters,
    read_states: &[ReadState],
    hooks: &H,
    audit: &A,
    self_id: u64,
) {
    for state in read_states {
        if let Some(parked) = waiters.pending_reads.remove(&state.ctx) {
            let read_index = node.hard_state().chosen_index;
            // The leader hint: a read-index answer comes from the leader
            // itself; a quorum read's server may be anyone, so it names the
            // leader it believes in.
            let leader = match parked.path {
                ReadPath::Index => Some(self_id),
                ReadPath::Quorum { .. } => node.leader().map(|n| n.0),
            };
            match parked.path {
                ReadPath::Index => audit.read_confirmed(NodeId(self_id), read_index),
                ReadPath::Quorum { row, opened } => {
                    let is_leader = node.role() == NodeRole::Leader;
                    audit.quorum_read_served(
                        NodeId(self_id),
                        row,
                        state.index,
                        read_index,
                        opened,
                        is_leader,
                    );
                    tracing::info!(
                        node = self_id,
                        ctx = state.ctx,
                        watermark = state
                            .index
                            .map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
                        is_leader,
                        "quorum_read_served"
                    );
                }
            }
            answer(
                hooks,
                audit,
                NodeId(self_id),
                Reply::Read,
                parked.reply,
                CheckTailAck {
                    seq: parked.seq,
                    leader,
                    committed: true,
                    committed_end: Some(committed_end(read_index)),
                    unknown_journal: false,
                },
            );
        }
    }
}

/// The exclusive end of a chosen prefix whose last slot is `index` — what
/// a `CheckTail` answers (#185): every LSN below it is chosen.
pub(crate) fn committed_end(index: Option<Slot>) -> u64 {
    index.map_or(0, |slot| slot.0 + 1)
}

/// Report one slot the contiguous walk moved over — "applied" names the walk,
/// there is no application behind it (#186): the audit's apply callback, then
/// the `value_chosen` and `log_applied` traces.
pub(crate) fn report_applied<A: Audit>(audit: &A, self_id: u64, slot: Slot, command: &Command) {
    let vhash = command_hash(command);
    audit.applied(
        NodeId(self_id),
        slot,
        vhash,
        command.user().map(|e| (e.client.0, e.seq.0)),
    );
    tracing::info!(node = self_id, slot = slot.0, vhash, "value_chosen");
    tracing::info!(
        node = self_id,
        slot = slot.0,
        applied_index = slot.0,
        "log_applied"
    );
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
pub(crate) async fn persist_writes<S: LogStorage, H: DriverHooks, A: Audit>(
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
        let staged = match op {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => {
                promise_changed = true;
                storage.persist_ballot(*ballot).await
            }
            // A replica's learned record (#144) is the accepted record's
            // durable shape; only a `ReplicaNode` emits it, and it is never a
            // vote, so `surface_persisted` reports it as nothing accepted.
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            })
            | WriteOp::Learned {
                slot,
                ballot,
                command,
            } => {
                storage
                    .append_accepted(*slot, *ballot, command.clone())
                    .await
            }
            WriteOp::SetChosenIndex(slot) => storage.set_chosen_index(*slot).await,
            WriteOp::Truncate { first, sealed } => storage.truncate(*first, sealed).await,
            WriteOp::TrimmedTo { point, sessions } => storage.trimmed_to(*point, sessions).await,
        };
        staged.map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }

    // Crash seam: the batch is staged but not yet flushed. A crash here loses the
    // whole un-synced batch (and no message has been sent), so surface nothing but
    // the crash marker itself. Only meaningful when the batch actually staged
    // something.
    crash_if(
        !writes.is_empty(),
        hooks,
        audit,
        NodeId(self_id),
        Seam::BeforeSync,
    )?;

    if !writes.is_empty() {
        storage
            .sync(must_sync)
            .await
            .map_err(|e| storage_fault_crash(audit, self_id, e))?;
        // Durability marker: whether this batch was fsync'd (a promise-raise or
        // accept — `MustSync::Sync`) or a relaxed write (a chosen-index-only
        // advance).
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
            WriteOp::TrimmedTo { point, .. } => {
                // The jump moves the prefix to `point - 1` without walking the
                // slots below it (they are trimmed cluster-wide); the audit
                // callback reports the jump so the no-gaps oracle admits it
                // and the convergence oracle sees the node reach the point.
                audit.trimmed_to(NodeId(self_id), *point);
                tracing::info!(node = self_id, point = point.0, "trimmed_to");
            }
            // A learned record is not an accept: no quorum oracle may fold it.
            WriteOp::Acceptor(AcceptorWrite::SetPromise(_)) | WriteOp::Learned { .. } => {}
        }
    }
}

/// The crash seam: when `armed` (the seam has something to lose — an
/// unarmed site never consults the hook, so it spends no draw) and the hook
/// fires, report the crash where it happens and unwind the incarnation with
/// [`RunError::SeamCrash`].
///
/// # Errors
///
/// [`RunError::SeamCrash`] when the hook fires.
pub(crate) fn crash_if<H: DriverHooks, A: Audit>(
    armed: bool,
    hooks: &H,
    audit: &A,
    node: NodeId,
    seam: Seam,
) -> Result<(), RunError> {
    if armed && hooks.crash_at(seam) {
        audit.crashed(node, seam);
        tracing::info!(node = node.0, seam = seam.label(), "crashed");
        return Err(RunError::SeamCrash(seam));
    }
    Ok(())
}

/// Map a [`StorageError`] into the driver's **deliberate crash decision**: a
/// storage fault never lets the node keep running on state it does not durably
/// have. The decision is reported through [`Audit::storage_fault`] (typed, at
/// the instant it is made) and traced as `storage_fault`, then
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
