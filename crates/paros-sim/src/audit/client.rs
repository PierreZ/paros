//! The client-history checker: what one client asked for, what it was told,
//! and linearizability of the merged history of every client of a journal
//! (#205: a search against the journal's sequential model, which replaced
//! the per-operation interval rules).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_sometimes};

use super::linearizability::Attempt;

// --- the client-history checker ---------------------------------------------

/// One committed operation's real-time span: first issue to first committed
/// ack, in simulated milliseconds — what the coverage gates read. Two spans
/// sharing a boundary millisecond are concurrent.
#[derive(Clone, Copy)]
pub(super) struct OpSpan {
    pub(super) inv: u64,
    pub(super) resp: u64,
}

impl OpSpan {
    pub(super) fn before(self, other: OpSpan) -> bool {
        self.resp < other.inv
    }
}

/// One client's own record of what it asked for and what came back. Owned by
/// the workload — the client is the only party that knows its own program order
/// — and merged into the shared [`LinHistory`] at `check()` time.
///
/// Two records. **Every attempt** at the four calls (`attempts`), logged at
/// the RPC seam, is what the linearizability search judges. **Every
/// operation**, keyed by the client's own operation number — a retry, a
/// duplicate re-send or a reconciled ambiguous attempt records one issue and
/// at most one terminal outcome, the first ack winning — is what the counts
/// and the coverage gates read: a write acked written pins at the last
/// position its batch occupies, a read at the last position its state
/// covered (`next_seq - 1`).
#[derive(Default)]
pub(crate) struct ClientHistory {
    pub(super) client: u64,
    /// Every attempt this client made at its journal (#205).
    pub(super) attempts: Vec<Attempt>,
    /// First issue time per write seq.
    pub(super) write_inv: BTreeMap<u64, u64>,
    /// First written ack per write op: `(time, last position)`.
    pub(super) write_resp: BTreeMap<u64, (u64, Option<u64>)>,
    /// Write seqs that ended without a committed ack (so far).
    pub(super) write_failed: BTreeSet<u64>,
    pub(super) read_inv: BTreeMap<u64, u64>,
    /// First committed ack per read seq: `(time, watermark)`.
    pub(super) read_resp: BTreeMap<u64, (u64, Option<u64>)>,
    pub(super) read_failed: BTreeSet<u64>,
    pub(super) read_retried: bool,
}

impl ClientHistory {
    pub(crate) fn set_client(&mut self, client: u64) {
        self.client = client;
    }

    /// Hand over the attempts the RPC seam logged (`chain_workload`'s
    /// `CallLog`).
    pub(crate) fn set_attempts(&mut self, attempts: Vec<Attempt>) {
        self.attempts = attempts;
    }

    pub(crate) fn record_write_issued(&mut self, seq: u64, now_ms: u64) {
        self.write_inv.entry(seq).or_insert(now_ms);
    }

    /// Record operation `seq` acked written, its batch ending at `slot`.
    pub(crate) fn record_write_ack(&mut self, seq: u64, slot: Option<u64>, now_ms: u64) {
        self.write_resp.entry(seq).or_insert((now_ms, slot));
        self.write_failed.remove(&seq);
    }

    pub(crate) fn record_write_failed(&mut self, seq: u64) {
        if !self.write_resp.contains_key(&seq) {
            self.write_failed.insert(seq);
        }
    }

    pub(crate) fn record_read_issued(&mut self, seq: u64, now_ms: u64) {
        self.read_inv.entry(seq).or_insert(now_ms);
    }

    pub(crate) fn record_read_ack(
        &mut self,
        seq: u64,
        watermark: Option<u64>,
        attempts: u64,
        now_ms: u64,
    ) {
        self.read_resp.entry(seq).or_insert((now_ms, watermark));
        self.read_failed.remove(&seq);
        self.read_retried |= attempts > 1;
    }

    pub(crate) fn record_read_failed(&mut self, seq: u64) {
        if !self.read_resp.contains_key(&seq) {
            self.read_failed.insert(seq);
        }
    }
}

/// The committed client history of one journal, merged from every client.
/// A watermark is `Option<u64>`: `None` is the *empty* journal, and
/// `None < Some(0)` is exactly the watermark order.
///
/// Its bools are independent per-run coverage flags (see [`AuditState`](crate::audit::state::AuditState)).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct LinHistory {
    /// Every client's attempts (#205), what the search judges.
    pub(super) attempts: Vec<Attempt>,
    /// How many clients merged so far.
    pub(super) merged: usize,
    /// Whether the search already ran (once, at the last merge).
    pub(super) searched: bool,
    /// Acked writes with a known last position, by `(client, op)`.
    pub(super) write_slot: BTreeMap<(u64, u64), u64>,
    /// Committed reads and their observed watermark.
    pub(super) read_wm: BTreeMap<(u64, u64), Option<u64>>,
    /// Committed writes as real-time spans.
    pub(super) writes: Vec<OpSpan>,
    /// Committed reads as real-time spans with their watermark.
    pub(super) reads: Vec<(OpSpan, Option<u64>)>,
    pub(super) issued: usize,
    pub(super) acked: usize,
    pub(super) failed: usize,
    pub(super) read_issued: usize,
    pub(super) read_acked: usize,
    pub(super) read_failed: usize,
    pub(super) read_ack_ms: Vec<u64>,
    pub(super) read_retried: bool,
}

impl LinHistory {
    /// Fold one client's record in. Called once per client, from its `check()`.
    pub(super) fn merge(&mut self, h: &ClientHistory) {
        let c = h.client;
        self.merged += 1;
        self.attempts.extend(h.attempts.iter().cloned());
        self.issued += h.write_inv.len();
        self.acked += h.write_resp.len();
        self.failed += h.write_failed.len();
        self.read_issued += h.read_inv.len();
        self.read_acked += h.read_resp.len();
        self.read_failed += h.read_failed.len();
        self.read_retried |= h.read_retried;
        for (&seq, &(resp, slot)) in &h.write_resp {
            if let Some(s) = slot {
                self.write_slot.insert((c, seq), s);
            }
            if let Some(&inv) = h.write_inv.get(&seq) {
                self.writes.push(OpSpan { inv, resp });
            }
        }
        for (&seq, &(resp, wm)) in &h.read_resp {
            self.read_wm.insert((c, seq), wm);
            self.read_ack_ms.push(resp);
            if let Some(&inv) = h.read_inv.get(&seq) {
                self.reads.push((OpSpan { inv, resp }, wm));
            }
        }
    }

    /// Coverage gates on the client-visible register (`UntilCoverageStable`
    /// only saturates once these fire).
    pub(super) fn check_coverage_gates(&self, leader_change_ms: Option<u64>) {
        let concurrent_read_write = self
            .reads
            .iter()
            .any(|&(r, _)| self.writes.iter().any(|&w| !w.before(r) && !r.before(w)));
        assert_sometimes!(
            concurrent_read_write,
            "a linearizable read commits concurrently with a conflicting write"
        );
        assert_sometimes!(!self.read_wm.is_empty(), "a linearizable read commits");
        let multi_slot = self.read_wm.values().any(|wm| *wm >= Some(1));
        assert_sometimes!(multi_slot, "a committed read observes a multi-slot prefix");
        // A read served after leadership changed hands — the window where a
        // naive local read goes stale.
        let read_after_change =
            leader_change_ms.is_some_and(|t| self.read_ack_ms.iter().any(|&ms| ms > t));
        assert_sometimes!(read_after_change, "a read commits after a leader change");
        assert_sometimes!(
            self.read_retried,
            "a read is retried across nodes before committing"
        );
    }
}

/// The search's step budget. The histories a campaign produces take a few
/// hundred thousand steps at most; the budget only bounds a pathological
/// one, and running out of it is reported, never mistaken for a verdict.
const LIN_SEARCH_BUDGET: u64 = 50_000_000;

/// The full checker (#205): the merged attempts of every client of one
/// journal, searched for a linearization against the journal's sequential
/// model ([`linearizability`](super::linearizability)). Run once, when the
/// last client of the journal merged: a sub-history of some clients is not a
/// history (a read shows records another client wrote).
pub(super) fn check_linearizable(h: &LinHistory) {
    let verdict = super::linearizability::check(&h.attempts, LIN_SEARCH_BUDGET);
    assert_always!(
        !verdict.exhausted,
        "the linearizability history stays within the checker's cap",
        { "attempts" => h.attempts.len(), "steps" => verdict.steps }
    );
    if !verdict.linearizable {
        let stuck = verdict
            .stuck
            .map(|(attempt, depth)| (h.attempts[attempt].clone(), depth));
        eprintln!(
            "journal history NOT LINEARIZABLE: {} attempts judged of {}, stuck at {stuck:?}",
            verdict.judged,
            h.attempts.len()
        );
        for attempt in &h.attempts {
            eprintln!("  {attempt:?}");
        }
    }
    assert_always!(
        verdict.linearizable,
        "journal: the four-call history is linearizable against the journal model",
        {
            "attempts" => h.attempts.len(),
            "judged" => verdict.judged,
            "stuck_client" => verdict.stuck.map_or(u64::MAX, |(a, _)| h.attempts[a].client),
            "linearized" => verdict.stuck.map_or(0, |(_, depth)| depth)
        }
    );
}
