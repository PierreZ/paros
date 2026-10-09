//! The [`paros_core::Ready`] handshake's I/O side: one linear durability
//! pipeline (persist → send → learn → acks), the durable-write staging and
//! reporting it splits into, the held client replies it answers, and the
//! driver's fail-stop storage-fault decision.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorWrite, Ballot, ColocatedNode, Command, GcRequest, MatchRequest, MatchmakerId, Message,
    NodeId, NodeRole, Outcome, Party, ReadState, Slot, WriteOp,
};

use crate::audit::{Audit, StorageFaultDecision};
use moonpool_buggify::hint::Strike;

use crate::storage::{LogStorage, StorageError};

use super::calls::Call;
use super::config::RunError;
use super::events::command_hash;
use super::transport::{Outbound, send_messages};

/// The client replies this node is holding open: the journal calls that
/// wait on their slot's verdict (#204), and the journal reads that wait on
/// their quorum read or at the tail.
#[derive(Default)]
pub(crate) struct ClientWaiters {
    /// The calls proposed at each slot, answered once it applies.
    pub(crate) pending: BTreeMap<Slot, Vec<Call>>,
    /// Journal reads (#204).
    pub(crate) reads: super::log_reads::JournalReads,
}

/// The last slot of the node's journal fold — what a read is served from.
pub(crate) fn fold_head(node: &ColocatedNode) -> Option<Slot> {
    let head = node.replica().folded().0.checked_sub(1).map(Slot);
    // The fold's head is a folded slot, inside the chosen prefix.
    assert!(
        head.is_none_or(|h| h < node.replica().folded()),
        "the fold's head lies below the first unfolded slot"
    );
    assert!(
        head <= node.replica().chosen_index(),
        "the fold's head lies inside the chosen prefix"
    );
    head
}

/// Answer the calls parked on every slot the journal fold now covers
/// (#204): a call whose slot decided its own command gets the verdict the
/// journal state machine gave there; one whose slot decided another command
/// (a stale leader's admission, superseded) or was dropped below the floor
/// before it applied here gets no verdict — ambiguous, never false. Swept
/// over the whole fold, not only the slots this batch walked, so a jump
/// that chose slots without walking them still answers the calls parked
/// there. A slot this batch walked is judged from the batch (`walked`):
/// a `Truncate` folded later in the same walk may already have compacted
/// it. The reply may be deliberately dropped at the reply seam
/// (`reply::answer`, #318): the journal moved either way, and
/// the client's retry is answered from the log.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
fn answer_applied_calls<A>(
    node: &ColocatedNode,
    walked: &[(Slot, Command, Outcome)],
    waiters: &mut ClientWaiters,
    audit: &A,
    self_id: u64,
) where
    A: Audit,
{
    let folded = node.replica().folded();
    let slots: Vec<Slot> = waiters.pending.range(..folded).map(|(s, _)| *s).collect();
    for slot in slots {
        let Some(calls) = waiters.pending.remove(&slot) else {
            continue;
        };
        let batch = walked
            .binary_search_by_key(&slot, |(at, _, _)| *at)
            .ok()
            .map(|i| &walked[i]);
        let decided = batch
            .map(|(_, command, _)| command)
            .or_else(|| node.replica().chosen_at(slot));
        let outcome = batch
            .map(|(_, _, outcome)| outcome)
            .filter(|outcome| **outcome != Outcome::Noop)
            .or_else(|| node.replica().outcome_at(slot));
        for call in calls {
            match (decided, outcome) {
                (Some(command), Some(outcome)) if *command == call.command() => {
                    audit.answered(NodeId(self_id), slot, command, outcome);
                    tracing::info!(node = self_id, slot = slot.0, ?outcome, "call_answered");
                    call.answer(outcome, self_id, audit);
                }
                _ => {
                    audit.waiter_superseded(NodeId(self_id), slot);
                    tracing::info!(node = self_id, slot = slot.0, "call_superseded");
                    call.no_verdict(Some(self_id), audit, self_id);
                }
            }
        }
    }
    // Every call whose slot the fold covers has been answered, one way or
    // the other: nothing waits below the fold.
    assert!(
        waiters.pending.range(..folded).next().is_none(),
        "no call waits on a slot the fold already covers"
    );
}

/// Report one leader-recovery batch this `Ready` carried: how many recovered
/// slots it started, how many of them were gap fills, and how many remain.
fn report_recovery_batch<A: Audit>(audit: &A, self_id: u64, batch: (usize, usize, usize)) {
    let (started, gap_fills, remaining) = batch;
    assert!(
        gap_fills <= started,
        "a recovery page fills only rounds it started"
    );
    moonpool_assertions::sometimes!(
        remaining > 0,
        "recovery: a leader's recovery spans more than one bounded page"
    );
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
pub(crate) async fn drain_ready<S, A>(
    node: &mut ColocatedNode,
    storage: &mut S,
    out: &Outbound,
    waiters: &mut ClientWaiters,
    audit: &A,
) -> Result<Outbox, RunError>
where
    S: LogStorage,
    A: Audit,
{
    let self_id = out.self_node().0;
    assert!(
        self_id == node.config().id.0,
        "a batch is drained by the node that queued it"
    );
    let chosen_before = node.hard_state().chosen_index;
    // The journal every message of this batch belongs to (#188).
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
    let must_sync = paros_core::MustSync::for_batch(&writes);
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
    let committed: Vec<(Slot, Command, Outcome)> = ready.committed().to_vec();
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
    //    staged-not-synced hint lives inside `persist_writes`.
    let promised = node.hard_state().max_promised_ballot;
    persist_writes(storage, &writes, must_sync, promised, self_id, audit).await?;

    if let Some(batch) = recovery_batch {
        report_recovery_batch(audit, self_id, batch);
    }

    // Hint: the batch is durable, its messages have not left. A crash here
    // keeps the durable writes and drops the batch's messages, so a
    // restarted node must re-derive them. Only meaningful when there is
    // durable work or a message to lose.
    if !writes.is_empty()
        || !messages.is_empty()
        || !match_requests.is_empty()
        || !gc_requests.is_empty()
    {
        let hinted = moonpool_buggify::hint!("batch durable, not sent");
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "the driver crashes after sync and before sending a batch"
            );
        }
        hinted.await;
    }

    // 2. Send messages — only after (1) is durable.
    // A reconfiguring campaign's `Prepare` carries a configuration other
    // than the one this node believes in force (its leadership adopts it only
    // on winning): the campaign whose death the #260 seam is about.
    let sent_prepare = messages.iter().any(|(_, msg)| {
        matches!(msg, Message::Prepare { config: Some(config), .. } if config != node.acceptors())
    });
    send_messages(out, audit, journal, messages);

    // 3. Learn the entries the chosen prefix walked over (already durable, in
    //    contiguous order) — surface them and the journal state machine's
    //    verdicts to the oracles and answer every call waiting on a slot the
    //    fold now covers (a held reply fires only now that its slot applied).
    for (slot, command, outcome) in &committed {
        let outcome = (*outcome != Outcome::Noop).then_some(outcome);
        report_applied(audit, self_id, *slot, command, outcome);
    }
    answer_applied_calls(node, &committed, waiters, audit, self_id);

    // 3b. Answer confirmed reads — after the learn step, so the fold this
    //     same batch carried is covered by what the read observes: the page
    //     is served from the *serve-time* fold, at or past the confirmed
    //     watermark.
    let leader = node.role() == NodeRole::Leader;
    waiters.reads.confirmed(
        &read_states,
        |from, limit, bytes| node.read_log(from, limit, bytes),
        fold_head(node),
        leader,
        NodeId(self_id),
        audit,
    );

    // Hint (#260): a reconfiguring candidate dies with its `Prepare`s in
    // flight, after the batch is durable, sent, applied and its reads
    // answered, so all it loses is the campaign itself. Armed only by a
    // reconfiguring campaign's `Prepare`, a few per run, so the rate is far
    // above the write hints': the state worth reaching is the campaign that
    // never finishes. The witness of #260 was a reconfiguring leader dying
    // here: an acceptor that promised its `Prepare` served reads over a
    // configuration the slot the dead leader had just chosen was never
    // voted in.
    if sent_prepare {
        let hinted = moonpool_buggify::hint!("reconfiguring prepare sent", 0.5);
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "the driver crashes with a campaign's Prepares in flight"
            );
        }
        hinted.await;
    }

    // The previous recovery page is now fully durable, sent, and applied. Only
    // at this boundary may the core materialize the next bounded Ready page;
    // doing it inside `Ready::advance` would move single-node state ahead of the
    // I/O the async driver is still performing.
    node.advance_recovery();
    // The batch only ever moved the prefix forward.
    assert!(
        node.hard_state().chosen_index >= chosen_before,
        "a drained batch never rewinds the chosen index"
    );

    Ok(Outbox {
        match_requests,
        gc_requests,
        gc_fence,
    })
}

/// Report one slot the contiguous walk moved over — "applied" names the walk,
/// there is no application behind it (#186): the audit's apply callback, then
/// the `value_chosen` and `log_applied` traces.
pub(crate) fn report_applied<A: Audit>(
    audit: &A,
    self_id: u64,
    slot: Slot,
    command: &Command,
    outcome: Option<&Outcome>,
) {
    // A slot's verdict is of its command's kind (pair of `JournalState::apply`).
    if let Some(outcome) = outcome {
        let write_verdict = matches!(
            outcome,
            Outcome::Accepted { .. }
                | Outcome::Duplicate { .. }
                | Outcome::Refused(_)
                | Outcome::Truncated(_)
        );
        // A wrong-mode refusal (#241) answers a write, a truncation or a
        // claim alike; every other verdict is of its command's kind.
        if matches!(outcome, Outcome::WrongMode(_)) {
            assert!(
                !matches!(command, Command::Control(paros_core::Control::Noop)),
                "a wrong-mode refusal answers a call, never a noop"
            );
        } else {
            assert!(
                write_verdict == command.write().is_some(),
                "a write's slot folds to a write's verdict"
            );
        }
        assert!(
            matches!(outcome, Outcome::Noop)
                == matches!(command, Command::Control(paros_core::Control::Noop)),
            "only a Noop folds to Noop"
        );
    }
    let vhash = command_hash(command);
    audit.applied(NodeId(self_id), slot, vhash, command, outcome);
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
/// claim a write the staged-not-synced hint then discards: a crash before the
/// fsync loses the whole un-synced batch and emits nothing, exactly as a real
/// crash-before-flush would.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, writes = writes.len(), must_sync = ?must_sync))]
pub(crate) async fn persist_writes<S: LogStorage, A: Audit>(
    storage: &mut S,
    writes: &[WriteOp],
    must_sync: paros_core::MustSync,
    promised: Ballot,
    self_id: u64,
    audit: &A,
) -> Result<(), RunError> {
    // The write half of the pairs the boot scan re-asserts
    // (`read_back_log`, `ColocatedNode::new`): nothing reaches the disk
    // above the promise the batch carries, and the scalars it persists only
    // rise, in the order the core queued them.
    assert!(
        writes.iter().all(|op| match op {
            WriteOp::Acceptor(
                AcceptorWrite::SetPromise(ballot) | AcceptorWrite::AppendAccepted { ballot, .. },
            ) => *ballot <= promised,
            _ => true,
        }),
        "a batch persists no promise or vote above the promise it carries"
    );
    assert!(
        writes
            .iter()
            .filter_map(|op| match op {
                WriteOp::Truncate { first, .. } => Some(*first),
                WriteOp::TrimmedTo { point, .. } => Some(*point),
                _ => None,
            })
            .collect::<Vec<Slot>>()
            .windows(2)
            .all(|w| w[0] < w[1]),
        "a batch persists a strictly rising floor"
    );
    if writes.iter().any(WriteOp::needs_sync) {
        assert!(
            must_sync == paros_core::MustSync::Sync,
            "a promise, a vote or a floor is always flushed with an fsync"
        );
    }
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
            WriteOp::Truncate { first, sealed } => {
                audit.floor_requested(NodeId(self_id), *first);
                storage.truncate(*first, *sealed).await
            }
            WriteOp::TrimmedTo { point, state } => {
                audit.floor_requested(NodeId(self_id), *point);
                storage.trimmed_to(*point, *state).await
            }
        };
        staged.map_err(|e| storage_fault_crash(audit, self_id, e))?;
    }

    // Hint: the batch is staged, not yet flushed. A crash here loses the
    // whole un-synced batch, and no message was sent. Only meaningful when
    // the batch staged something.
    if !writes.is_empty() {
        let hinted = moonpool_buggify::hint!("batch staged, not synced");
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!("the driver crashes before syncing a staged batch");
        }
        hinted.await;
    }

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

    assert!(
        promise_changed
            == writes
                .iter()
                .any(|op| matches!(op, WriteOp::Acceptor(AcceptorWrite::SetPromise(_)))),
        "the promise is reported changed exactly when the batch raised it"
    );
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
