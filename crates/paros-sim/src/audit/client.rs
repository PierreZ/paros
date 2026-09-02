//! The client-history checker: what one client asked for, what it was told,
//! and disclosed-order linearizability over the merged history of every client.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};

/// Cap on the committed-operation history the interval checker walks pairwise.
/// The current workloads stay far below it (a few dozen operations per client);
/// the cap only bounds the `O(n^2)` walk if a future workload explodes.
const LIN_HISTORY_CAP: usize = 512;

// --- the client-history checker ---------------------------------------------

/// One committed operation's real-time span: first issue to first committed
/// ack, in simulated milliseconds. Two spans sharing a boundary millisecond are
/// treated as *concurrent* (no precedence edge), which can only drop — never
/// fabricate — a real-time constraint, so the checker stays sound at
/// millisecond granularity.
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
/// Everything is keyed by `seq`, so a retry, a duplicate re-proposal, or an
/// ambiguous attempt that is later reconciled records one issue and at most
/// one terminal outcome per identity: the first ack wins, and an ack retires
/// an earlier failure of the same seq.
#[derive(Default)]
pub(crate) struct ClientHistory {
    pub(super) client: u64,
    /// First issue time per write seq.
    pub(super) write_inv: BTreeMap<u64, u64>,
    /// First committed ack per write seq: `(time, slot)`.
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

    pub(crate) fn record_write_issued(&mut self, seq: u64, now_ms: u64) {
        self.write_inv.entry(seq).or_insert(now_ms);
    }

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

/// The committed client history of the whole run, keyed by `(client_id, seq)`.
/// A watermark is `Option<u64>`: an absent `read_index` is the *empty* applied
/// prefix, and `None < Some(0)` is exactly the watermark order.
///
/// The register under check is the **applied log prefix**: an acked write is a
/// state transition at its committed `slot`, and a committed read observes the
/// watermark. Failed / timed-out operations enter no constraint — a timed-out
/// write may still commit later, so it is deliberately unconstrained.
///
/// Its bools are independent per-run coverage flags (see [`AuditState`]).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct LinHistory {
    /// Acked writes with a known slot (program order within one client).
    pub(super) write_slot: BTreeMap<(u64, u64), u64>,
    /// Committed reads and their observed watermark.
    pub(super) read_wm: BTreeMap<(u64, u64), Option<u64>>,
    /// Committed writes as real-time spans with their slot (`None` for a
    /// defensive slotless ack, which still forbids the empty prefix later).
    pub(super) writes: Vec<(OpSpan, Option<u64>)>,
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
    /// The highest slot any client was told was committed.
    pub(super) fn acked_max(&self) -> Option<u64> {
        self.write_slot.values().copied().max()
    }

    /// Fold one client's record in. Called once per client, from its `check()`.
    pub(super) fn merge(&mut self, h: &ClientHistory) {
        let c = h.client;
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
                self.writes.push((OpSpan { inv, resp }, slot));
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
    pub(super) fn check_coverage_gates(
        &self,
        committed_clients: &BTreeSet<u64>,
        leader_change_ms: Option<u64>,
    ) {
        let multi_client = committed_clients.len() > 1;
        assert_sometimes!(
            multi_client,
            "a run drives concurrent clients against one register"
        );
        if multi_client {
            assert_reachable!("a run drives concurrent clients against one register");
        }
        let concurrent_read_write = self.reads.iter().any(|&(r, _)| {
            self.writes
                .iter()
                .any(|&(w, _)| !w.before(r) && !r.before(w))
        });
        assert_sometimes!(
            concurrent_read_write,
            "a linearizable read commits concurrently with a conflicting write"
        );
        if concurrent_read_write {
            assert_reachable!("a linearizable read commits concurrently with a conflicting write");
        }
        assert_sometimes!(!self.read_wm.is_empty(), "a linearizable read commits");
        if !self.read_wm.is_empty() {
            assert_reachable!("a linearizable read commits");
        }
        let multi_slot = self.read_wm.values().any(|wm| *wm >= Some(1));
        assert_sometimes!(multi_slot, "a committed read observes a multi-slot prefix");
        if multi_slot {
            assert_reachable!("a committed read observes a multi-slot prefix");
        }
        // A read served after leadership changed hands — the window where a
        // naive local read goes stale.
        let read_after_change =
            leader_change_ms.is_some_and(|t| self.read_ack_ms.iter().any(|&ms| ms > t));
        assert_sometimes!(read_after_change, "a read commits after a leader change");
        if read_after_change {
            assert_reachable!("a read commits after a leader change");
        }
        assert_sometimes!(
            self.read_retried,
            "a read is retried across nodes before committing"
        );
        if self.read_retried {
            assert_reachable!("a read is retried across nodes before committing");
        }
    }
}

/// The full checker: disclosed-order linearizability over real time. Committed
/// writes pin to their slot, committed reads to their watermark; the induced
/// order is a valid linearization iff it agrees with every real-time precedence
/// edge. A Wing & Gong / Porcupine search backtracks over candidate
/// linearization orders; here the consensus log *discloses* every linearization
/// point, so the search collapses to its verification half — four pairwise
/// interval checks over committed operations, valid for any number of
/// concurrent clients and any per-client mode, bounded by [`LIN_HISTORY_CAP`].
pub(super) fn check_disclosed_order(h: &LinHistory) {
    // The pairwise walk is bounded by the cap; a workload that outgrows it
    // must raise it deliberately, never lose L1–L4 in silence.
    let ops = h.writes.len() + h.reads.len();
    assert_always!(
        ops <= LIN_HISTORY_CAP,
        "the linearizability history stays within the checker's cap",
        { "ops" => ops, "cap" => LIN_HISTORY_CAP }
    );
    if ops > LIN_HISTORY_CAP {
        return;
    }
    // L1 — the log order of two committed writes agrees with their real-time
    // order.
    for (i, &(w1, s1)) in h.writes.iter().enumerate() {
        for &(w2, s2) in &h.writes[i + 1..] {
            let (Some(s1), Some(s2)) = (s1, s2) else {
                continue;
            };
            if w1.before(w2) {
                assert_always!(
                    s1 < s2,
                    "two real-time-ordered committed writes land in log order"
                );
            } else if w2.before(w1) {
                assert_always!(
                    s2 < s1,
                    "two real-time-ordered committed writes land in log order"
                );
            }
        }
    }
    // L2 — a committed read observes every write that completed before it
    // began (a slotless committed ack still forbids the empty prefix). L3 — a
    // write invoked after a committed read lands above that read's watermark.
    for &(r, wm) in &h.reads {
        for &(w, slot) in &h.writes {
            if w.before(r) {
                let observed = match slot {
                    Some(s) => wm >= Some(s),
                    None => wm.is_some(),
                };
                assert_always!(
                    observed,
                    "a committed read observes every write completed before it began"
                );
            } else if r.before(w)
                && let Some(s) = slot
            {
                assert_always!(
                    Some(s) > wm,
                    "a write invoked after a committed read lands above its watermark"
                );
            }
        }
    }
    // L4 — watermarks of real-time-ordered committed reads never move
    // backwards.
    for (i, &(r1, wm1)) in h.reads.iter().enumerate() {
        for &(r2, wm2) in &h.reads[i + 1..] {
            if r1.before(r2) {
                assert_always!(
                    wm2 >= wm1,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            } else if r2.before(r1) {
                assert_always!(
                    wm1 >= wm2,
                    "real-time-ordered committed reads observe monotone watermarks"
                );
            }
        }
    }
}

/// The sequential fast path for one non-pipelined client: program order (seq)
/// is real-time order within the client even where timestamps tie, so C1-C3
/// are strictly stronger than the interval checks for its operations.
pub(super) fn check_sequential_client(client: u64, h: &LinHistory) {
    let span = (client, 0)..=(client, u64::MAX);
    // C1 — a committed read observes every write acked before it began: read
    // `k` starts after write `j`'s ack for every `j <= k`, so its watermark
    // covers the running max acked slot (two-pointer over seq).
    let mut max_acked_slot: Option<u64> = None;
    let mut writes = h.write_slot.range(span.clone()).peekable();
    for (&(_, rk), &wm) in h.read_wm.range(span.clone()) {
        while let Some(&(&(_, wj), &slot)) = writes.peek() {
            if wj > rk {
                break;
            }
            max_acked_slot = max_acked_slot.max(Some(slot));
            writes.next();
        }
        assert_always!(
            wm >= max_acked_slot,
            "a committed read's watermark covers every write acked before it began"
        );
    }

    // C2 — this client's reads do not overlap, so their watermarks never move
    // backwards.
    let mut prev: Option<u64> = None;
    for (_, &wm) in h.read_wm.range(span.clone()) {
        assert_always!(wm >= prev, "committed-read watermarks never move backwards");
        prev = prev.max(wm);
    }

    // C3 — a write issued after a committed read must land above that read's
    // watermark (a slot at or below it would place the write inside the prefix
    // the read already observed). Guards against an inflated / speculative
    // watermark.
    let mut max_read_wm: Option<u64> = None;
    let mut reads = h.read_wm.range(span.clone()).peekable();
    for (&(_, wj), &slot) in h.write_slot.range(span) {
        while let Some(&(&(_, rk), &wm)) = reads.peek() {
            if rk >= wj {
                break;
            }
            max_read_wm = max_read_wm.max(wm);
            reads.next();
        }
        if let Some(i) = max_read_wm {
            assert_always!(
                slot > i,
                "a write issued after a committed read lands above its watermark"
            );
        }
    }
}
