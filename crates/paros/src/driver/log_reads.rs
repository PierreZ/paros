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
//! before the read opened is in it. No read goes through the leader.
//!
//! A confirmed read with nothing to return yet — it starts at or past the
//! journal's `next_seq` — waits here for as long as the client asked
//! (`wait_ms`, capped at `max_wait_ms` and a non-zero one raised to
//! `min_wait_ms`, #241), re-served after every batch (the fold only grows
//! inside one), and is answered empty when its wait runs out. A page holds
//! at most `max_read_records` records and `max_read_bytes` record bytes
//! ([`ReadLimits`], `docs/architecture.md` §2.7). A read whose confirmation does not arrive within `read_retry_ticks`
//! is answered `served: false`: the client retries, here or elsewhere.
//!
//! Every driver that serves `Read` (the node's and the replica's) holds one
//! of these; the page itself is a closure over its core, so the wait never
//! knows which role is serving.

use std::collections::BTreeMap;
use std::time::Duration;

use paros_core::{JournalIdentifier, LogRead, NodeId, ReadState, Seq, Slot};

use crate::audit::{Audit, LogReadAnswer, LogReadReport};
use crate::rpc::{Read, ReadAck, ReplySender, journal_view_to_proto};

use super::DriverTunables;
use super::reply::{Reply, answer};

/// The limits a serving process puts on a `Read` (#241,
/// `docs/architecture.md` §2.7), taken from its [`DriverTunables`]: the page
/// (records and bytes) and the tail wait (a maximum and a minimum).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadLimits {
    records: usize,
    bytes: usize,
    min_wait_ms: u64,
    max_wait_ms: u64,
    tick: Duration,
}

impl ReadLimits {
    /// The limits `tunables` name.
    pub(crate) fn of(tunables: &DriverTunables) -> Self {
        let limits = Self {
            records: usize::try_from(tunables.max_read_records).unwrap_or(usize::MAX),
            bytes: usize::try_from(tunables.max_read_bytes).unwrap_or(usize::MAX),
            min_wait_ms: tunables.min_wait_ms,
            max_wait_ms: tunables.max_wait_ms,
            tick: tunables.tick_interval,
        };
        // The floors `check_floors` names: a page always moves a reader.
        assert!(limits.records >= 1, "a page holds at least one record");
        assert!(limits.bytes >= 1, "a page holds at least one byte");
        limits
    }

    /// The records a page asked with `limit` carries at most: the maximum
    /// when the client names none (0) or a larger one.
    pub(crate) fn page(&self, limit: u64) -> usize {
        let page = match usize::try_from(limit).unwrap_or(usize::MAX) {
            0 => self.records,
            limit => limit.min(self.records),
        };
        assert!(page >= 1, "a page holds at least one record");
        assert!(page <= self.records, "a page never passes the maximum");
        page
    }

    /// The tail wait `wait_ms` gets, in milliseconds: 0 stays 0 (answer at
    /// once), a non-zero one is raised to the minimum, and the maximum caps
    /// it (the cap wins when the two cross).
    pub(crate) fn wait_ms(&self, wait_ms: u64) -> u64 {
        if wait_ms == 0 {
            return 0;
        }
        let wait = wait_ms.max(self.min_wait_ms).min(self.max_wait_ms);
        assert!(
            wait <= self.max_wait_ms,
            "a read never outwaits the maximum"
        );
        assert!(
            wait >= self.min_wait_ms.min(self.max_wait_ms),
            "a read that waits waits at least the minimum"
        );
        wait
    }

    /// The tail wait `wait_ms` gets, in driver ticks (rounded up).
    pub(crate) fn wait_ticks(&self, wait_ms: u64) -> u64 {
        let wait = self.wait_ms(wait_ms);
        let tick_ms = u64::try_from(self.tick.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let ticks = wait.div_ceil(tick_ms);
        if wait == 0 {
            assert!(ticks == 0, "a read that asks no wait never waits");
        } else {
            assert!(ticks >= 1, "a read that waits waits at least a tick");
        }
        ticks
    }

    /// The longest tail wait, as a duration: what a follower's deadline
    /// must cover (one tick of rounding included).
    pub(crate) fn longest_wait(&self) -> Duration {
        Duration::from_millis(self.max_wait_ms).saturating_add(self.tick)
    }
}

/// One journal read, from its arrival to its answer.
struct PendingRead {
    from: Seq,
    /// The records its page may carry: the client's limit, cut to the
    /// maximum.
    limit: usize,
    /// The record bytes its page may carry.
    bytes: usize,
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
    let ack = read_ack_unchecked(read);
    if matches!(read, LogRead::NotHeld) {
        // Not held here: unserved, carrying neither state nor records.
        assert!(!ack.served, "a read not held here is unserved");
        assert!(ack.state.is_none(), "an unserved read names no state");
        assert!(
            ack.records.is_empty(),
            "an unserved read carries no records"
        );
        return ack;
    }
    // A served answer always names the state it was read from, and only a
    // truncated read says so.
    assert!(ack.served, "a page or a truncation is a served read");
    assert!(ack.state.is_some(), "a served read names the journal state");
    assert!(
        ack.truncated == matches!(read, LogRead::Truncated(_)),
        "only a truncated read is answered truncated"
    );
    ack
}

/// [`read_ack`] before its postconditions.
fn read_ack_unchecked(read: &LogRead) -> ReadAck {
    match read {
        // Unserved: the client asks another server (`ReadOutcome::Unserved`).
        LogRead::NotHeld => ReadAck::default(),
        LogRead::Truncated(state) => ReadAck {
            served: true,
            truncated: true,
            state: Some(journal_view_to_proto(*state)),
            ..ReadAck::default()
        },
        LogRead::Page(page) => ReadAck {
            served: true,
            from_seq: page.from.0,
            records: page.records.iter().map(|r| r.0.clone()).collect(),
            state: Some(journal_view_to_proto(page.state)),
            ..ReadAck::default()
        },
    }
}

/// The page `pending` gets from `read` now, inside its limits.
fn page_for(pending: &PendingRead, read: &impl Fn(Seq, usize, usize) -> LogRead) -> LogRead {
    let page = read(pending.from, pending.limit, pending.bytes);
    // Paired with the cut at `park`: what the fold serves stays inside the
    // page limit the read was parked with.
    if let LogRead::Page(p) = &page {
        assert!(
            p.records.len() <= pending.limit,
            "a page never carries more records than its limit"
        );
        assert!(
            p.records.len() <= 1
                || p.records.iter().map(|r| r.0.len()).sum::<usize>() <= pending.bytes,
            "a page of several records stays inside its byte budget"
        );
    }
    page
}

/// Hand one read's answer to the reply seam, reporting it first.
fn send<A: Audit>(
    read: &LogRead,
    from: Seq,
    how: LogReadAnswer,
    reply: ReplySender<ReadAck>,
    node: NodeId,
    audit: &A,
) {
    let Some(report) = LogReadReport::of(read, from, how) else {
        // The fold's floor rose ahead of it (a trim-point jump): the record
        // is in the journal but not here, so the read is answered unserved
        // and the client asks elsewhere (`LogRead::NotHeld`).
        tracing::info!(node = node.0, from = from.0, answer = ?how, "log_read_not_held");
        let ack = read_ack(read);
        assert!(!ack.served, "a read this process does not hold is unserved");
        answer(audit, node, Reply::ReadUnserved, reply, ack);
        return;
    };
    // A woken read is one the fold moved past: never an empty tail page.
    if how == LogReadAnswer::Woke {
        assert!(!at_end(read), "a woken read has something to serve");
    }
    let ack = read_ack(read);
    // The audit's view and the wire answer are two encodings of one read.
    assert!(
        ack.truncated == report.truncated,
        "the audit and the wire agree on truncation"
    );
    assert!(
        ack.records.len() == report.records.len(),
        "the audit and the wire carry the same records"
    );
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
    answer(audit, node, Reply::LogRead, reply, ack);
}

/// The refusal a call naming a journal this process does not serve gets
/// (#185: an unset half, or any identifier but its own, #235), reported through
/// [`Audit::journal_refused`]. `true` when the call was refused.
pub(crate) fn refuse_journal<A: Audit>(
    served: JournalIdentifier,
    asked: JournalIdentifier,
    call: &'static str,
    node: NodeId,
    audit: &A,
) -> bool {
    if asked.is_set() && asked == served {
        return false;
    }
    // Negative space: a refusal is never for the named journal this serves.
    if asked.is_set() {
        assert!(
            asked != served,
            "a named journal this process serves is never refused"
        );
    }
    audit.journal_refused(node, asked, call);
    tracing::info!(node = node.0, journal = %asked, call, "journal_refused");
    true
}

/// The grid row a quorum read asks instead of the core's `ctx % rows`:
/// `(ctx + 1) % rows`, the next read's row, so consecutive reads land on one
/// row and the reader is asked about a row it may not sit in. Every row is a
/// Phase-1 quorum that meets every column, so the choice is always valid.
/// The draw is the caller's own location (the node and the replica tier).
pub(crate) fn next_row(ctx: u64, rows: usize) -> usize {
    assert!(rows >= 2, "a row override needs another row");
    let modulus = u64::try_from(rows).unwrap_or(u64::MAX);
    let row = usize::try_from(ctx.wrapping_add(1) % modulus).unwrap_or(0);
    assert!(row < rows, "an overridden row is one the grid has");
    row
}

impl JournalReads {
    /// The read tally's own invariants: every token was minted here, every
    /// page is bounded, and only a read that may wait is parked.
    fn assert_invariants(&self) {
        assert!(
            self.confirming.keys().all(|ctx| *ctx < self.next_ctx),
            "every confirming read carries a token minted here"
        );
        assert!(
            self.confirming
                .values()
                .chain(&self.parked)
                .all(|p| p.limit > 0 && p.bytes > 0),
            "every pending read's page is bounded"
        );
        assert!(
            self.parked.iter().all(|p| p.wait_ticks > 0),
            "only a read that asked to wait is parked"
        );
        assert!(
            self.confirming
                .values()
                .chain(&self.parked)
                .all(|p| p.parked_at <= self.now),
            "no read was parked in the future"
        );
    }

    /// The `ctx` the next read's quorum read opens with.
    pub(crate) fn next_ctx(&self) -> u64 {
        // The next token is fresh: no read waits on it yet.
        assert!(
            !self.confirming.contains_key(&self.next_ctx),
            "the next read token is unused"
        );
        self.next_ctx
    }

    /// Park `req` on the quorum read the caller just opened at
    /// [`JournalReads::next_ctx`] (asking `row`, with the fold head at
    /// `opened`), until the core confirms it. Its page and its tail wait
    /// are cut to `limits`.
    pub(crate) fn park(
        &mut self,
        req: &Read,
        reply: ReplySender<ReadAck>,
        limits: ReadLimits,
        row: Option<usize>,
        opened: Option<Slot>,
    ) {
        let ctx = self.next_ctx;
        self.next_ctx += 1;
        self.confirming.insert(
            ctx,
            PendingRead {
                from: Seq(req.from_seq),
                limit: limits.page(req.limit),
                bytes: limits.bytes,
                wait_ticks: limits.wait_ticks(req.wait_ms),
                parked_at: self.now,
                row,
                opened,
                reply,
            },
        );
        assert!(self.next_ctx > ctx, "a read token is never reused");
        self.assert_invariants();
    }

    /// Serve every read the core confirmed in this batch (`served`): report
    /// the quorum read, then answer the page — or, at the tail with time
    /// left, start the long-poll. `fold` is the serving process's fold head
    /// now and `leader` whether it leads.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn confirmed<A: Audit>(
        &mut self,
        served: &[ReadState],
        read: impl Fn(Seq, usize, usize) -> LogRead,
        fold: Option<Slot>,
        leader: bool,
        node: NodeId,
        audit: &A,
    ) {
        for state in served {
            let Some(mut pending) = self.confirming.remove(&state.ctx) else {
                continue;
            };
            audit.quorum_read_served(
                node,
                state.ctx,
                pending.row,
                state.index,
                fold,
                pending.opened,
                leader,
            );
            tracing::info!(
                node = node.0,
                ctx = state.ctx,
                watermark = state
                    .index
                    .map_or(-1, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
                leader,
                "quorum_read_served"
            );
            let page = page_for(&pending, &read);
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
                    audit,
                );
            }
        }
        // A confirmed read leaves the confirming tally, answered or parked.
        assert!(
            served.iter().all(|s| !self.confirming.contains_key(&s.ctx)),
            "a confirmed read is no longer confirming"
        );
        self.assert_invariants();
    }

    /// Re-serve every read waiting at the tail after a batch: a read the
    /// fold (or a truncation) moved past is answered; the rest keep waiting.
    pub(crate) fn wake<A: Audit>(
        &mut self,
        read: impl Fn(Seq, usize, usize) -> LogRead,
        node: NodeId,
        audit: &A,
    ) {
        if self.parked.is_empty() {
            return;
        }
        let before = self.parked.len();
        let mut still = Vec::with_capacity(before);
        for parked in std::mem::take(&mut self.parked) {
            let page = page_for(&parked, &read);
            if at_end(&page) {
                still.push(parked);
            } else {
                send(
                    &page,
                    parked.from,
                    LogReadAnswer::Woke,
                    parked.reply,
                    node,
                    audit,
                );
            }
        }
        self.parked = still;
        // Waking only answers: it never parks a read.
        assert!(self.parked.len() <= before, "a wake never parks a read");
        self.assert_invariants();
    }

    /// Per-tick upkeep: a read whose confirmation is overdue
    /// (`retry_ticks`, or every one of them when `expire_all` — the
    /// driver's early-expiry hook) is answered `served: false`, and a read
    /// whose wait at the tail ran out is answered with the empty page.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn expire<A: Audit>(
        &mut self,
        read: impl Fn(Seq, usize, usize) -> LogRead,
        ticks: u64,
        retry_ticks: u64,
        expire_all: bool,
        node: NodeId,
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
                    audit,
                    node,
                    Reply::ReadUnserved,
                    pending.reply,
                    ReadAck::default(),
                );
            }
        }
        // What is still confirming is inside its deadline, and an early
        // expiry leaves nothing confirming at all.
        if expire_all {
            assert!(
                self.confirming.is_empty(),
                "an early expiry answers every read"
            );
        }
        assert!(
            self.confirming
                .values()
                .all(|p| ticks.saturating_sub(p.parked_at) <= retry_ticks),
            "no confirming read outlives its deadline"
        );
        if self.parked.is_empty() {
            return;
        }
        let (overdue, still): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parked)
            .into_iter()
            .partition(|parked| ticks.saturating_sub(parked.parked_at) >= parked.wait_ticks);
        self.parked = still;
        for parked in overdue {
            let page = page_for(&parked, &read);
            send(
                &page,
                parked.from,
                LogReadAnswer::Expired,
                parked.reply,
                node,
                audit,
            );
        }
        // Nothing parked outlives its wait.
        assert!(
            self.parked
                .iter()
                .all(|p| ticks.saturating_sub(p.parked_at) < p.wait_ticks),
            "no parked read outlives its wait"
        );
        self.assert_invariants();
    }

    /// Whether any read is still waiting on its confirmation.
    pub(crate) fn has_confirming(&self) -> bool {
        !self.confirming.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(records: u64, min_wait_ms: u64, max_wait_ms: u64) -> ReadLimits {
        ReadLimits::of(&DriverTunables {
            max_read_records: records,
            min_wait_ms,
            max_wait_ms,
            ..DriverTunables::default()
        })
    }

    #[test]
    fn a_page_is_cut_to_the_maximum() {
        let l = limits(16, 0, 400);
        assert_eq!(l.page(0), 16);
        assert_eq!(l.page(3), 3);
        assert_eq!(l.page(16), 16);
        assert_eq!(l.page(u64::MAX), 16);
    }

    #[test]
    fn a_wait_is_capped_and_floored_but_zero_stays_zero() {
        let l = limits(16, 120, 400);
        assert_eq!(l.wait_ms(0), 0);
        assert_eq!(l.wait_ms(1), 120);
        assert_eq!(l.wait_ms(200), 200);
        assert_eq!(l.wait_ms(10_000), 400);
        // At the default 50 ms tick, rounded up.
        assert_eq!(l.wait_ticks(0), 0);
        assert_eq!(l.wait_ticks(1), 3);
        assert_eq!(l.wait_ticks(10_000), 8);
    }

    #[test]
    fn the_maximum_wins_when_the_limits_cross() {
        let l = limits(16, 500, 100);
        assert_eq!(l.wait_ms(1), 100);
        assert_eq!(l.wait_ms(1000), 100);
        assert_eq!(limits(16, 0, 0).wait_ticks(1000), 0);
    }
}
