//! Journal reads (#185): the `Read` answer, and the **long-poll** of a read
//! that starts at or past the serving process's end.
//!
//! The core serves a page from its chosen prefix ([`paros_core::LogRead`],
//! a pure read); the driver owns the wait. A read with nothing to return
//! yet is parked here, keyed by nothing but its arrival order, and is
//! re-served after every batch (the chosen prefix only grows inside one) —
//! answered the moment a slot at or above its start is chosen, or a trim
//! overtakes it — and answered empty once its wait (`read_poll_ticks`) runs
//! out. Every driver that serves `Read` (the node's and the replica's) holds
//! one of these; the read itself is a closure over its core, so the wait
//! never knows which role is serving.

use paros_core::{JournalId, LogRead, NodeId, Slot};

use crate::audit::{Audit, LogReadAnswer, LogReadReport};
use crate::hooks::{DriverHooks, Reply};
use crate::rpc::{LogEntry, Read, ReadAck, ReplySender, decode_records};

use super::reply::answer;

/// One parked journal read.
struct ParkedLogRead {
    from: Slot,
    max_bytes: usize,
    parked_at: u64,
    reply: ReplySender<ReadAck>,
}

/// The journal reads a driver is holding open, oldest first.
#[derive(Default)]
pub(crate) struct LogReads {
    parked: Vec<ParkedLogRead>,
}

/// Whether `read` is an empty page at (or past) the end — the one answer a
/// long-poll waits out rather than sends.
fn at_end(read: &LogRead, from: Slot) -> bool {
    matches!(read, LogRead::Page(page)
        if page.entries.is_empty() && page.next == from && from >= page.committed_end)
}

/// The wire answer for a core read page.
fn read_ack(read: &LogRead) -> ReadAck {
    match read {
        LogRead::Trimmed { trim_point } => ReadAck {
            next_lsn: trim_point.0,
            trimmed_to: Some(trim_point.0),
            ..ReadAck::default()
        },
        LogRead::Page(page) => ReadAck {
            entries: page
                .entries
                .iter()
                .map(|(slot, entry)| LogEntry {
                    lsn: slot.0,
                    client: entry.client.0,
                    seq: entry.seq.0,
                    records: decode_records(&entry.value.0),
                })
                .collect(),
            next_lsn: page.next.0,
            committed_end: page.committed_end.0,
            trimmed_to: None,
            unknown_journal: false,
        },
    }
}

/// The audit's view of one answer.
fn report(read: &LogRead, from: Slot, answer: LogReadAnswer) -> LogReadReport {
    match read {
        LogRead::Trimmed { trim_point } => LogReadReport {
            from,
            trimmed_to: Some(*trim_point),
            next: *trim_point,
            committed_end: *trim_point,
            entries: 0,
            skipped: 0,
            answer,
        },
        LogRead::Page(page) => LogReadReport {
            from,
            trimmed_to: None,
            next: page.next,
            committed_end: page.committed_end,
            entries: page.entries.len() as u64,
            skipped: page.skipped,
            answer,
        },
    }
}

/// Hand one read's answer to the reply seam, reporting it first.
fn send<H: DriverHooks, A: Audit>(
    read: &LogRead,
    from: Slot,
    how: LogReadAnswer,
    reply: ReplySender<ReadAck>,
    node: NodeId,
    hooks: &H,
    audit: &A,
) {
    let served = report(read, from, how);
    audit.log_read_served(node, &served);
    tracing::info!(
        node = node.0,
        from = from.0,
        next = served.next.0,
        committed_end = served.committed_end.0,
        entries = served.entries,
        skipped = served.skipped,
        trimmed = served.trimmed_to.is_some(),
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

impl LogReads {
    /// Serve `req` through `read` (the serving core's page), or park it when
    /// it starts at or past the end. A request for another journal is
    /// refused.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn serve<H: DriverHooks, A: Audit>(
        &mut self,
        read: impl Fn(Slot, usize) -> LogRead,
        journal: JournalId,
        req: &Read,
        reply: ReplySender<ReadAck>,
        ticks: u64,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        if refuse_journal(journal, req.journal, "read", node, audit) {
            let refused = ReadAck {
                unknown_journal: true,
                ..ReadAck::default()
            };
            answer(hooks, audit, node, Reply::LogRead, reply, refused);
            return;
        }
        let from = Slot(req.from_lsn);
        let max_bytes = usize::try_from(req.max_bytes).unwrap_or(usize::MAX);
        let page = read(from, max_bytes);
        if at_end(&page, from) {
            self.parked.push(ParkedLogRead {
                from,
                max_bytes,
                parked_at: ticks,
                reply,
            });
        } else {
            send(
                &page,
                from,
                LogReadAnswer::Immediate,
                reply,
                node,
                hooks,
                audit,
            );
        }
    }

    /// Re-serve every parked read after a batch: a read the prefix (or a
    /// trim) moved past is answered; the rest keep waiting.
    pub(crate) fn wake<H: DriverHooks, A: Audit>(
        &mut self,
        read: impl Fn(Slot, usize) -> LogRead,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        if self.parked.is_empty() {
            return;
        }
        let mut still = Vec::with_capacity(self.parked.len());
        for parked in std::mem::take(&mut self.parked) {
            let page = read(parked.from, parked.max_bytes);
            if at_end(&page, parked.from) {
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

    /// Answer every read whose wait ran out (`poll_ticks` ticks parked) with
    /// the empty page at the end: the client re-asks.
    pub(crate) fn expire<H: DriverHooks, A: Audit>(
        &mut self,
        read: impl Fn(Slot, usize) -> LogRead,
        ticks: u64,
        poll_ticks: u64,
        node: NodeId,
        hooks: &H,
        audit: &A,
    ) {
        if self.parked.is_empty() {
            return;
        }
        let (overdue, still): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parked)
            .into_iter()
            .partition(|parked| ticks.saturating_sub(parked.parked_at) >= poll_ticks);
        self.parked = still;
        for parked in overdue {
            let page = read(parked.from, parked.max_bytes);
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
}
