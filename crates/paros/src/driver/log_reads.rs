//! Journal reads (#204): the `Read` answer, the leaderless confirmation it
//! waits on, and the **long-poll** of a read that starts at the tail.
//!
//! A `Read` is served through the leaderless read of Compartmentalized
//! Paxos §3.4 (paros's quorum read, #143): the serving process — a node or a
//! replica — opens a quorum read in its core, which asks a Phase-1 quorum of
//! the acceptors for their vote watermarks, and the read is parked here,
//! keyed by the core's `ctx` token, until the core surfaces it confirmed
//! ([`paros_core::ReadState`]) — the row answered whole and this process's
//! journal fold covers the maximum. Only then is the page served, from the
//! fold ([`paros_core::LogRead`], a pure read): every write acknowledged
//! before the read opened is in it. No read goes through the leader, and no
//! read-index round is ever asked.
//!
//! A confirmed read with nothing to return yet — it starts at or past the
//! journal's `next_seq` — waits here for as long as the client asked
//! (`wait_ms`, capped by `read_poll_ticks`), re-served after every batch (the
//! fold only grows inside one), and is answered empty when its wait runs
//! out. A read whose confirmation does not arrive within `read_retry_ticks`
//! is answered `served: false`: the client retries, here or elsewhere.
//!
//! Every driver that serves `Read` (the node's and the replica's) holds one
//! of these; the page itself is a closure over its core, so the wait never
//! knows which role is serving.

use std::collections::BTreeMap;
use std::time::Duration;

use paros_core::{JournalId, LogRead, NodeId, ReadState, Seq, Slot};

use crate::audit::{Audit, LogReadAnswer, LogReadReport};
use crate::hooks::{DriverHooks, Reply};
use crate::rpc::{Read, ReadAck, ReplySender, journal_state_to_proto};

use super::reply::answer;

/// The records one page carries when the client names no limit.
pub(crate) const READ_PAGE_RECORDS: usize = 256;

/// The byte budget of one page: far below the frame limit. A page that can
/// hold a record always holds one, whatever its size.
pub(crate) const READ_PAGE_BYTES: usize = 64 * 1024;

/// One journal read, from its arrival to its answer.
struct PendingRead {
    from: Seq,
    limit: usize,
    /// The ticks the client lets the read wait at the tail.
    wait_ticks: u64,
    /// The driver tick it arrived at (its confirmation deadline), then the
    /// tick it started waiting at the tail.
    parked_at: u64,
    /// The grid row its quorum read asked (`None`: the whole configuration).
    row: Option<usize>,
    /// The serving process's fold head when the read opened: what an
    /// unconfirmed local read would have served.
    opened: Option<Slot>,
    reply: ReplySender<ReadAck>,
}

/// The journal reads a driver is holding open.
#[derive(Default)]
pub(crate) struct JournalReads {
    /// Reads waiting on their quorum read, by the core's `ctx`.
    confirming: BTreeMap<u64, PendingRead>,
    /// Confirmed reads waiting at the tail, oldest first.
    parked: Vec<PendingRead>,
    next_ctx: u64,
    /// The driver tick of the last upkeep ([`JournalReads::expire`]): the
    /// clock every deadline here is counted on.
    now: u64,
}

/// Whether `read` is an empty page at (or past) the tail — the one answer a
/// long-poll waits out rather than sends.
fn at_end(read: &LogRead) -> bool {
    matches!(read, LogRead::Page(page)
        if page.records.is_empty() && page.from >= page.state.next_seq)
}

/// The wire answer for a core read page.
fn read_ack(read: &LogRead) -> ReadAck {
    match read {
        LogRead::Truncated(state) => ReadAck {
            served: true,
            truncated: true,
            state: Some(journal_state_to_proto(*state)),
            ..ReadAck::default()
        },
        LogRead::Page(page) => ReadAck {
            served: true,
            from_seq: page.from.0,
            records: page.records.iter().map(|r| r.0.clone()).collect(),
            state: Some(journal_state_to_proto(page.state)),
            ..ReadAck::default()
        },
    }
}

/// Hand one read's answer to the reply seam, reporting it first.
fn send<H: DriverHooks, A: Audit>(
    read: &LogRead,
    from: Seq,
    how: LogReadAnswer,
    reply: ReplySender<ReadAck>,
    node: NodeId,
    hooks: &H,
    audit: &A,
) {
    let report = LogReadReport::of(read, from, how);
    audit.log_read_served(node, &report);
    tracing::info!(
        node = node.0,
        from = from.0,
        records = report.records.len(),
        next_seq = report.state.next_seq.0,
        truncated = report.truncated,
        answer = ?how,
        "log_read_served"
    );
    answer(hooks, audit, node, Reply::LogRead, reply, read_ack(read));
}

/// The refusal a call naming a journal this process does not serve gets
/// (#185: `0`, or anything but its own), reported through
/// [`Audit::journal_refused`]. `true` when the call was refused.
pub(crate) fn refuse_journal<A: Audit>(
    served: JournalId,
    asked: u64,
    call: &'static str,
    node: NodeId,
    audit: &A,
) -> bool {
    let asked = JournalId(asked);
    if asked.is_set() && asked == served {
        return false;
    }
    audit.journal_refused(node, asked, call);
    tracing::info!(node = node.0, journal = asked.0, call, "journal_refused");
    true
}

/// How many ticks `wait_ms` is at `tick`, capped at `cap`.
pub(crate) fn wait_ticks(wait_ms: u64, tick: Duration, cap: u64) -> u64 {
    let tick_ms = u64::try_from(tick.as_millis()).unwrap_or(u64::MAX).max(1);
    wait_ms.div_ceil(tick_ms).min(cap)
}

impl JournalReads {
    /// The `ctx` the next read's quorum read opens with.
    pub(crate) fn next_ctx(&self) -> u64 {
        self.next_ctx
    }

    /// Park `req` on the quorum read the caller just opened at
    /// [`JournalReads::next_ctx`] (asking `row`, with the fold head at
    /// `opened`), until the core confirms it.
    pub(crate) fn park(
        &mut self,
        req: &Read,
        reply: ReplySender<ReadAck>,
        wait_ticks: u64,
        row: Option<usize>,
        opened: Option<Slot>,
    ) {
        let ctx = self.next_ctx;
        self.next_ctx += 1;
        let limit = match usize::try_from(req.limit).unwrap_or(usize::MAX) {
            0 => READ_PAGE_RECORDS,
            limit => limit.min(READ_PAGE_RECORDS),
        };
        self.confirming.insert(
            ctx,
            PendingRead {
                from: Seq(req.from_seq),
                limit,
                wait_ticks,
                parked_at: self.now,
                row,
                opened,
                reply,
            },
        );
    }

    /// Serve every read the core confirmed in this batch (`served`): report
    /// the quorum read, then answer the page — or, at the tail with time
    /// left, start the long-poll. `fold` is the serving process's fold head
    /// now and `leader` whether it leads.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn confirmed<H: DriverHooks, A: Audit>(
        &mut self,
        served: &[ReadState],
        read: impl Fn(Seq, usize, usize) -> LogRead,
        fold: Option<Slot>,
        leader: bool,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        for state in served {
            let Some(mut pending) = self.confirming.remove(&state.ctx) else {
                continue;
            };
            audit.quorum_read_served(node, pending.row, state.index, fold, pending.opened, leader);
            tracing::info!(
                node = node.0,
                ctx = state.ctx,
                watermark = state
                    .index
                    .map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
                leader,
                "quorum_read_served"
            );
            let page = read(pending.from, pending.limit, READ_PAGE_BYTES);
            if at_end(&page) && pending.wait_ticks > 0 {
                pending.parked_at = self.now;
                self.parked.push(pending);
            } else {
                send(
                    &page,
                    pending.from,
                    LogReadAnswer::Immediate,
                    pending.reply,
                    node,
                    hooks,
                    audit,
                );
            }
        }
    }

    /// Re-serve every read waiting at the tail after a batch: a read the
    /// fold (or a truncation) moved past is answered; the rest keep waiting.
    pub(crate) fn wake<H: DriverHooks, A: Audit>(
        &mut self,
        read: impl Fn(Seq, usize, usize) -> LogRead,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        if self.parked.is_empty() {
            return;
        }
        let mut still = Vec::with_capacity(self.parked.len());
        for parked in std::mem::take(&mut self.parked) {
            let page = read(parked.from, parked.limit, READ_PAGE_BYTES);
            if at_end(&page) {
                still.push(parked);
            } else {
                send(
                    &page,
                    parked.from,
                    LogReadAnswer::Woke,
                    parked.reply,
                    node,
                    hooks,
                    audit,
                );
            }
        }
        self.parked = still;
    }

    /// Per-tick upkeep: a read whose confirmation is overdue
    /// (`retry_ticks`, or every one of them when `expire_all` — the
    /// driver's early-expiry hook) is answered `served: false`, and a read
    /// whose wait at the tail ran out is answered with the empty page.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn expire<H: DriverHooks, A: Audit>(
        &mut self,
        read: impl Fn(Seq, usize, usize) -> LogRead,
        ticks: u64,
        retry_ticks: u64,
        expire_all: bool,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        self.now = ticks;
        let overdue: Vec<(u64, bool)> = self
            .confirming
            .iter()
            .filter_map(|(ctx, pending)| {
                let by_deadline = ticks.saturating_sub(pending.parked_at) > retry_ticks;
                (expire_all || by_deadline).then_some((*ctx, !by_deadline))
            })
            .collect();
        for (ctx, early) in overdue {
            if let Some(pending) = self.confirming.remove(&ctx) {
                audit.read_expired(node, early);
                answer(
                    hooks,
                    audit,
                    node,
                    Reply::ReadUnserved,
                    pending.reply,
                    ReadAck::default(),
                );
            }
        }
        if self.parked.is_empty() {
            return;
        }
        let (overdue, still): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parked)
            .into_iter()
            .partition(|parked| ticks.saturating_sub(parked.parked_at) >= parked.wait_ticks);
        self.parked = still;
        for parked in overdue {
            let page = read(parked.from, parked.limit, READ_PAGE_BYTES);
            send(
                &page,
                parked.from,
                LogReadAnswer::Expired,
                parked.reply,
                node,
                hooks,
                audit,
            );
        }
    }

    /// Whether any read is still waiting on its confirmation.
    pub(crate) fn has_confirming(&self) -> bool {
        !self.confirming.is_empty()
    }
}
