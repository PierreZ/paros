//! The client-history checker: what one client asked for, what it was told,
//! and linearizability of the merged history of every client of a journal
//! (#205: a search against the journal's sequential model, which replaced
//! the per-operation interval rules).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};

use super::journal_model::AckedWrite;
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
    /// First written ack per write op, with the records it sent (#651
    /// (acked writes judged by their bytes)).
    pub(super) write_acked: BTreeMap<u64, AckedWrite>,
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

    /// Hand over the attempts the RPC seam logged (`workload`'s
    /// `CallLog`).
    pub(crate) fn set_attempts(&mut self, attempts: Vec<Attempt>) {
        self.attempts = attempts;
    }

    pub(crate) fn record_write_issued(&mut self, seq: u64, now_ms: u64) {
        self.write_inv.entry(seq).or_insert(now_ms);
    }

    /// Record operation `seq` acked written as `write`: the batch's last
    /// position, and its records, which the audit judges by their bytes
    /// (#651 (acked writes judged by their bytes)).
    pub(crate) fn record_write_ack(&mut self, seq: u64, write: AckedWrite, now_ms: u64) {
        let slot = (write.seq + write.count).checked_sub(1);
        self.write_resp.entry(seq).or_insert((now_ms, slot));
        self.write_acked.entry(seq).or_insert(write);
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
    /// Acked writes with the records they sent, by `(client, op)` (#651).
    pub(super) write_acked: BTreeMap<(u64, u64), AckedWrite>,
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
        for (&seq, write) in &h.write_acked {
            self.write_acked.insert((c, seq), write.clone());
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

/// The control journals' histories (#247): every attempt any client's
/// library calls made at one of the fleet tenant's control journal, the
/// cell's control journal (the registry) or the directory — fleet steps,
/// checkpoints and truncations, system appends and their read-backs —
/// searched for a linearization against the same journal model as a tenant
/// journal's, under the same messages (the same invariant, on another
/// journal). Run once, after every
/// client's run: every one of them writes these journals.
#[tracing::instrument(level = "debug", skip_all, fields(attempts = attempts.len()))]
pub(crate) fn check_control_history(
    attempts: Vec<Attempt>,
    mode: paros::core::journal::WriterMode,
) {
    if attempts.is_empty() {
        return;
    }
    assert_reachable!("journal: a control journal's history is searched for a linearization");
    check_linearizable(
        &LinHistory {
            attempts,
            ..LinHistory::default()
        },
        mode,
    );
}

/// The search's step budget. The steps grow with the history (#392: the
/// 37 histories of 4,500 hunt seeds that took 200k to 1.2M steps before
/// take 4,120 at most, on 3,668 attempts); the
/// budget only bounds a pathological one — the memo grows with the steps,
/// so it bounds memory too — and running out of it is a violation, never
/// mistaken for a verdict.
const LIN_SEARCH_BUDGET: u64 = 5_000_000;

/// The full checker (#205): the merged attempts of every client of one
/// journal, searched for a linearization against the journal's sequential
/// model ([`linearizability`](super::linearizability)). Run once, when the
/// last client of the journal merged: a sub-history of some clients is not a
/// history (a read shows records another client wrote).
#[tracing::instrument(level = "debug", skip_all)]
pub(super) fn check_linearizable(h: &LinHistory, mode: paros::core::journal::WriterMode) {
    let verdict = super::linearizability::check(&h.attempts, mode, LIN_SEARCH_BUDGET);
    if verdict.exhausted {
        eprintln!(
            "journal history UNDECIDED after {} steps: {} attempts judged of {}",
            verdict.steps,
            verdict.judged,
            h.attempts.len()
        );
        for attempt in &h.attempts {
            eprintln!("  {attempt:?}");
        }
    }
    // A budget a history used up is no verdict (#443 (an undecided search
    // passed)): #392 measured 4,120 steps at most against the 5,000,000 the
    // budget allows, so a pass never needs it.
    assert_always!(
        !verdict.exhausted,
        "journal: the linearizability search decides every history",
        { "steps" => verdict.steps, "attempts" => h.attempts.len() }
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

#[cfg(test)]
mod tests {
    use crate::audit::testing::red;
    use paros::core::journal::{JournalView, LeaderUuid, Seq, WriterMode};

    use super::{ClientHistory, LinHistory, OpSpan, check_control_history, check_linearizable};
    use crate::audit::journal_model::AckedWrite;
    use crate::audit::linearizability::{Attempt, Call, Seen};

    fn acked(seq: u64, count: u64) -> AckedWrite {
        AckedWrite {
            seq,
            count,
            records: (0..count).collect(),
        }
    }

    fn won(at: u64) -> Attempt {
        Attempt {
            client: 0,
            inv: at,
            call: Call::SetLeader {
                new: LeaderUuid(1),
                old: None,
            },
            seen: Some((
                at + 1,
                Seen::Won(JournalView {
                    leader: Some(LeaderUuid(1)),
                    next_seq: Seq(0),
                    first_seq: Seq(0),
                }),
            )),
        }
    }

    fn read_page(at: u64, records: Vec<u64>, next: u64) -> Attempt {
        Attempt {
            client: 0,
            inv: at,
            call: Call::Read { from: 0, limit: 0 },
            seen: Some((
                at + 1,
                Seen::Page {
                    records,
                    state: JournalView {
                        leader: Some(LeaderUuid(1)),
                        next_seq: Seq(next),
                        first_seq: Seq(0),
                    },
                },
            )),
        }
    }

    #[test]
    fn a_span_is_before_another_only_when_it_ends_first() {
        let a = OpSpan { inv: 0, resp: 4 };
        let b = OpSpan { inv: 5, resp: 9 };
        let touching = OpSpan { inv: 4, resp: 6 };
        assert!(a.before(b));
        assert!(!b.before(a));
        assert!(!a.before(touching), "a shared millisecond is concurrent");
    }

    #[test]
    fn the_first_terminal_outcome_of_an_operation_wins() {
        let mut h = ClientHistory::default();
        h.set_client(3);
        h.record_write_issued(1, 10);
        h.record_write_issued(1, 20);
        h.record_write_failed(1);
        assert!(h.write_failed.contains(&1));
        h.record_write_ack(1, acked(4, 2), 30);
        h.record_write_ack(1, acked(9, 1), 40);
        h.record_write_failed(1);
        assert_eq!(h.write_inv[&1], 10);
        assert_eq!(h.write_resp[&1], (30, Some(5)), "the batch's last position");
        assert_eq!(h.write_acked[&1], acked(4, 2));
        assert!(!h.write_failed.contains(&1));
        h.record_write_ack(2, acked(0, 0), 50);
        assert_eq!(h.write_resp[&2], (50, None), "an empty batch pins nothing");

        h.record_read_issued(1, 10);
        h.record_read_issued(1, 11);
        h.record_read_failed(1);
        h.record_read_ack(1, Some(3), 1, 12);
        assert!(!h.read_retried);
        h.record_read_ack(1, Some(9), 2, 13);
        h.record_read_failed(1);
        assert_eq!(h.read_inv[&1], 10);
        assert_eq!(h.read_resp[&1], (12, Some(3)));
        assert!(h.read_failed.is_empty());
        assert!(h.read_retried, "a later ack after a retry still marks it");
        h.record_read_failed(2);
        assert!(h.read_failed.contains(&2));
    }

    #[test]
    fn a_merge_folds_every_clients_counts_spans_and_watermarks() {
        let mut one = ClientHistory::default();
        one.set_client(1);
        one.set_attempts(vec![won(0)]);
        one.record_write_issued(1, 0);
        one.record_write_ack(1, acked(0, 2), 5);
        one.record_write_issued(2, 6);
        one.record_write_failed(2);
        one.record_write_ack(3, acked(2, 1), 7);
        one.record_read_issued(1, 3);
        one.record_read_ack(1, Some(1), 2, 4);
        one.record_read_ack(2, None, 1, 9);
        one.record_read_issued(3, 10);
        one.record_read_failed(3);
        let mut two = ClientHistory::default();
        two.set_client(2);
        two.record_write_issued(1, 0);
        two.record_write_ack(1, acked(3, 1), 8);

        let mut lin = LinHistory::default();
        lin.merge(&one);
        lin.merge(&two);
        assert_eq!(lin.merged, 2);
        assert_eq!(lin.attempts.len(), 1);
        assert_eq!((lin.issued, lin.acked, lin.failed), (3, 3, 1));
        assert_eq!(
            (lin.read_issued, lin.read_acked, lin.read_failed),
            (2, 2, 1)
        );
        assert!(lin.read_retried);
        assert_eq!(lin.write_slot[&(1, 1)], 1);
        assert_eq!(lin.write_slot[&(1, 3)], 2);
        assert_eq!(lin.write_slot[&(2, 1)], 3);
        assert_eq!(lin.write_acked.len(), 3);
        assert_eq!(lin.writes.len(), 2, "an ack with no issue has no span");
        assert_eq!(lin.reads.len(), 1);
        assert_eq!(lin.read_wm[&(1, 1)], Some(1));
        assert_eq!(lin.read_wm[&(1, 2)], None);
        assert_eq!(lin.read_ack_ms, vec![4, 9]);
    }

    #[test]
    fn the_coverage_gates_never_go_red() {
        let mut lin = LinHistory::default();
        assert!(!red(|| lin.check_coverage_gates(None)));
        lin.writes.push(OpSpan { inv: 0, resp: 5 });
        lin.reads.push((OpSpan { inv: 3, resp: 4 }, Some(1)));
        lin.read_wm.insert((0, 0), Some(1));
        lin.read_ack_ms.push(4);
        assert!(!red(|| lin.check_coverage_gates(Some(2))));
    }

    #[test]
    fn a_control_history_is_searched_for_a_linearization() {
        assert!(!red(|| check_control_history(
            Vec::new(),
            WriterMode::Single
        )));
        assert!(!red(|| check_control_history(
            vec![won(0), read_page(2, vec![], 0)],
            WriterMode::Single
        )));
        // A read that shows a record no write ever sent.
        assert!(red(|| check_control_history(
            vec![won(0), read_page(2, vec![7], 1)],
            WriterMode::Single
        )));
    }

    #[test]
    fn check_linearizable_judges_the_merged_attempts() {
        let honest = LinHistory {
            attempts: vec![won(0)],
            ..LinHistory::default()
        };
        assert!(!red(|| check_linearizable(&honest, WriterMode::Single)));
        let broken = LinHistory {
            attempts: vec![won(0), read_page(2, vec![7], 1)],
            ..LinHistory::default()
        };
        assert!(red(|| check_linearizable(&broken, WriterMode::Single)));
    }
}
