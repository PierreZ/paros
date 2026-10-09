//! The journal state machine's oracles (#204): what every node's fold of
//! one journal owes every other, and what the journal owes its clients.
//!
//! The core judges every `Write`, `SetLeader` and `Truncate` at apply, in
//! slot order ([`paros::journal_state`]); each node reports the verdict it
//! reached at each slot it walked ([`paros::Audit::applied`]). This model
//! folds those reports — never a second implementation of the journal — and
//! asserts the invariants of `docs/architecture.md`, section 6:
//!
//! - **one verdict per slot**: every node judges a slot to the same outcome
//!   (the fold is a function of the log alone);
//! - **leaders born in the log** (#241): a uuid leads from a winning
//!   `SetLeader` that installed it over the leader it named; every verdict
//!   names the leader in force at its slot, and a write is accepted only
//!   under that uuid. A uuid that led may be reinstated by a misbehaving
//!   client (§2.3: the journal trusts its clients' fresh uuids), and these
//!   invariants hold all the same;
//! - **positions dense**: in slot order, every accepted batch starts where
//!   the last one ended, and a refusal names the position the journal stood
//!   at;
//! - **no re-accept with other bytes**: a position is accepted once, and a
//!   retry is acknowledged only with the bytes accepted there;
//! - **truncation monotone**: `first_seq` never moves backwards along the
//!   log, and never past `next_seq`;
//! - **truncation fenced** (#228): a `Truncate` is accepted only from the
//!   current leader, and refused only from a caller that is not it.
//!
//! The per-slot facts arrive in whatever order the nodes walk; the checks
//! that need slot order (density, the leader chain, truncation) run
//! over the whole record at the end of the run ([`JournalModel::check`]), so
//! no check depends on which node reported first.

use std::collections::BTreeMap;

use moonpool_sim::{assert_always, assert_sometimes};
use paros::{Command, Control, JournalView, LeaderUuid, Outcome};

/// The write a slot decided, as the model keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WriteFact {
    leader: LeaderUuid,
    seq: u64,
    count: u64,
    vhash: u64,
}

/// What one slot decided and the verdict every node reached there.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SlotFact {
    write: Option<WriteFact>,
    /// A `Truncate`'s fence, the leader uuid it names (#228).
    truncate: Option<LeaderUuid>,
    outcome: Outcome,
}

/// One journal's model, fed by every node's walk. Its bools are
/// independent per-run coverage flags.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct JournalModel {
    /// Per slot: the verdict every node must reach there (a `Noop` is kept
    /// out: it judges nothing).
    slots: BTreeMap<u64, SlotFact>,
    /// Per leader uuid: the slot its `SetLeader` won at.
    leaders: BTreeMap<LeaderUuid, u64>,
    /// Per accepted batch, by its first position: the slot and the write.
    batches: BTreeMap<u64, (u64, WriteFact)>,
    /// Per accepted position: the hash of the record there.
    records: BTreeMap<u64, u64>,
    /// The highest `next_seq` any verdict revealed.
    next_seq: u64,
    accepted_any: bool,
    duplicate_any: bool,
    fenced_any: bool,
    gap_refused_any: bool,
    won_any: bool,
    lost_any: bool,
    truncated_any: bool,
    trimmed_any: bool,
    superseded_write_answered: bool,
    reinstated_any: bool,
}

/// The hash a record is kept under (`user_command_hash` of its bytes).
pub(crate) fn record_hash(record: &[u8]) -> u64 {
    crate::chain::user_command_hash(record)
}

impl JournalModel {
    /// Fold one node's verdict at `slot`: `command` (hashed to `vhash`)
    /// judged `outcome` (`None` for a `Noop`).
    pub(super) fn applied(
        &mut self,
        node: u64,
        slot: u64,
        command: &Command,
        vhash: u64,
        outcome: Option<&Outcome>,
    ) {
        let Some(outcome) = outcome else {
            return;
        };
        let write = command.write().map(|entry| WriteFact {
            leader: entry.leader,
            seq: entry.seq.0,
            count: entry.count(),
            vhash,
        });
        let truncate = match command {
            Command::Control(Control::Truncate { leader, .. }) => Some(*leader),
            _ => None,
        };
        let fact = SlotFact {
            write,
            truncate,
            outcome: outcome.clone(),
        };
        if let Some(known) = self.slots.get(&slot) {
            assert_always!(
                *known == fact,
                "journal: every node judges a slot to the same outcome",
                { "node" => node, "slot" => slot }
            );
            return;
        }
        self.first_verdict(slot, command, &fact);
        self.slots.insert(slot, fact);
    }

    /// The order-independent checks a slot's first verdict runs.
    fn first_verdict(&mut self, slot: u64, command: &Command, fact: &SlotFact) {
        if let Some(state) = revealed(&fact.outcome) {
            self.next_seq = self.next_seq.max(state.next_seq.0);
        }
        match (&fact.outcome, fact.write) {
            (Outcome::Accepted { seq, count }, Some(write)) => {
                self.accepted_any = true;
                assert_always!(
                    write.seq == seq.0 && write.count == *count && *count > 0,
                    "journal: an accepted write takes the positions it asked for",
                    { "slot" => slot, "seq" => seq.0, "asked" => write.seq }
                );
                // No position is accepted twice: the batch below this one
                // ends at or before it, the one above starts at or after its
                // end.
                let below = self.batches.range(..=seq.0).next_back();
                let above = self.batches.range(seq.0..).next();
                assert_always!(
                    below.is_none_or(|(start, (_, w))| start + w.count <= seq.0)
                        && above.is_none_or(|(start, _)| *start >= seq.0 + count),
                    "journal: a position is accepted once",
                    { "slot" => slot, "seq" => seq.0, "count" => *count }
                );
                self.batches.insert(seq.0, (slot, write));
                self.next_seq = self.next_seq.max(seq.0 + count);
                if let Command::Write(entry) = command {
                    for (position, record) in (seq.0..).zip(&entry.records) {
                        self.records.insert(position, record_hash(&record.0));
                    }
                }
            }
            (Outcome::Duplicate { seq, .. }, Some(write)) => {
                self.duplicate_any = true;
                if let Some((_, original)) = self.batches.get(&seq.0) {
                    assert_always!(
                        original.vhash == write.vhash,
                        "journal: a retry is acked only with the bytes accepted there",
                        { "slot" => slot, "seq" => seq.0 }
                    );
                }
            }
            (Outcome::Refused(state), Some(write)) => {
                if state.leader != Some(write.leader) {
                    self.fenced_any = true;
                } else if write.seq != state.next_seq.0 {
                    self.gap_refused_any = true;
                }
            }
            (Outcome::Truncated(state), Some(write)) => {
                self.truncated_any = true;
                assert_always!(
                    write.seq < state.first_seq.0,
                    "journal: a write is answered truncated only below first_seq",
                    { "slot" => slot, "seq" => write.seq, "first_seq" => state.first_seq.0 }
                );
            }
            (Outcome::Leader(state), None) => {
                self.won_any = true;
                if let Command::Control(Control::SetLeader { new, .. }) = command {
                    assert_always!(
                        state.leader == Some(*new),
                        "journal: a won SetLeader installs its new uuid",
                        { "slot" => slot }
                    );
                    // A uuid that led before may lead again: the journal
                    // trusts its clients to draw fresh ones (decided on
                    // 2026-10-09, §2.3); a misbehaving one reinstates.
                    let earliest = self.leaders.entry(*new).or_insert(slot);
                    if *earliest != slot {
                        self.reinstated_any = true;
                    }
                    *earliest = (*earliest).min(slot);
                }
            }
            (Outcome::LeaderRefused(state), None) => {
                self.lost_any = true;
                if let Command::Control(Control::SetLeader { new, old }) = command {
                    assert_always!(
                        state.leader != *old || state.leader == Some(*new) || !new.is_set(),
                        "journal: a SetLeader loses only against another leader",
                        { "slot" => slot }
                    );
                }
            }
            (Outcome::Trimmed(state) | Outcome::TruncateRefused(state), None) => {
                self.truncate_verdict(slot, fact, state);
            }
            (outcome, write) => {
                assert_always!(
                    false,
                    "journal: a verdict matches the command it judged",
                    { "slot" => slot, "outcome" => format!("{outcome:?}"), "write" => write.is_some() }
                );
            }
        }
    }

    /// A `Truncate`'s verdict against its fence (#228): accepted only from
    /// the leader the state after it names, refused only from a caller that
    /// is not the leader it was judged against.
    fn truncate_verdict(&mut self, slot: u64, fact: &SlotFact, state: &JournalView) {
        let leads = fact.truncate.is_some() && state.leader == fact.truncate;
        if matches!(fact.outcome, Outcome::Trimmed(_)) {
            self.trimmed_any = true;
            assert_always!(
                leads,
                "journal: a Truncate is accepted only from the current owner",
                { "slot" => slot }
            );
        } else {
            assert_always!(
                !leads,
                "journal: a Truncate is refused only from a caller that is not the owner",
                { "slot" => slot }
            );
        }
    }

    /// A node answered the call it proposed at `slot` with `outcome`: the
    /// verdict its fold reached there, which every node shares.
    pub(super) fn answered(&mut self, node: u64, slot: u64, outcome: &Outcome) {
        if let Some(known) = self.slots.get(&slot) {
            assert_always!(
                known.outcome == *outcome,
                "journal: a call is answered with the verdict its slot applied to",
                { "node" => node, "slot" => slot }
            );
            if let (Some(write), Outcome::Refused(state)) = (known.write, outcome)
                && state.leader.is_some_and(|leader| leader != write.leader)
            {
                self.superseded_write_answered = true;
            }
        }
    }

    /// The hash of the record accepted at `position`, if the model saw it.
    pub(super) fn record_at(&self, position: u64) -> Option<u64> {
        self.records.get(&position).copied()
    }

    /// The highest `next_seq` any verdict revealed.
    pub(super) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The slot-ordered checks, over everything folded so far: positions
    /// dense, the leader chain in the log, truncation monotone. Then the
    /// journal's coverage gates.
    pub(super) fn check(&self) {
        let mut next: Option<u64> = None;
        let mut first = 0_u64;
        let mut current: Option<LeaderUuid> = None;
        let mut stale_truncate_refused = false;
        for (&slot, fact) in &self.slots {
            let (before, after) = match (&fact.outcome, fact.write) {
                (Outcome::Accepted { seq, count }, _) => (Some(seq.0), Some(seq.0 + count)),
                (outcome, _) => revealed(outcome).map_or((None, None), |state| {
                    (Some(state.next_seq.0), Some(state.next_seq.0))
                }),
            };
            if let (Some(expected), Some(before)) = (next, before) {
                assert_always!(
                    expected == before,
                    "journal: positions are dense in slot order",
                    { "slot" => slot, "expected" => expected, "observed" => before }
                );
            }
            next = after.or(next);
            if let Some(state) = revealed(&fact.outcome) {
                assert_always!(
                    state.first_seq.0 >= first && state.first_seq <= state.next_seq,
                    "journal: first_seq never moves backwards",
                    { "slot" => slot, "first_seq" => state.first_seq.0, "previous" => first }
                );
                first = state.first_seq.0;
                // A leader changes only at a won `SetLeader`: every other
                // verdict names the one in force (a verdict before the first
                // win this model saw is the chain's start).
                if let Some(current) = current
                    && !matches!(fact.outcome, Outcome::Leader(_))
                {
                    assert_always!(
                        state.leader == Some(current),
                        "journal: every verdict names the leader in force",
                        { "slot" => slot }
                    );
                }
            }
            if let Outcome::Leader(state) = &fact.outcome {
                current = state.leader;
            }
            if let (Outcome::TruncateRefused(_), Some(fence)) = (&fact.outcome, fact.truncate)
                && self.leaders.get(&fence).is_some_and(|at| *at < slot)
                && current != Some(fence)
            {
                stale_truncate_refused = true;
            }
            if let (Outcome::Trimmed(_), Some(fence), Some(current)) =
                (&fact.outcome, fact.truncate, current)
            {
                assert_always!(
                    fence == current,
                    "journal: a Truncate is accepted only from the writer in force",
                    { "slot" => slot }
                );
            }
            if let (Outcome::Accepted { .. }, Some(write)) = (&fact.outcome, fact.write) {
                // The uuid a write was accepted under won in the log, at a
                // lower slot.
                assert_always!(
                    self.leaders.get(&write.leader).is_some_and(|at| *at < slot),
                    "journal: a write is accepted only under a uuid won in the log",
                    { "slot" => slot }
                );
                if let Some(current) = current {
                    assert_always!(
                        current == write.leader,
                        "journal: a write is accepted only from the writer in force",
                        { "slot" => slot }
                    );
                }
            }
        }
        self.gates(stale_truncate_refused);
    }

    /// The journal's coverage gates, once the slot-ordered checks ran.
    fn gates(&self, stale_truncate_refused: bool) {
        assert_sometimes!(self.won_any, "journal: a SetLeader wins a generation");
        assert_sometimes!(self.accepted_any, "journal: a write is accepted");
        assert_sometimes!(
            self.duplicate_any,
            "journal: a retried write is acked from the log"
        );
        assert_sometimes!(
            self.fenced_any,
            "journal: a superseded writer's write is refused"
        );
        assert_sometimes!(
            self.gap_refused_any,
            "journal: a write at the wrong position is refused"
        );
        assert_sometimes!(
            self.lost_any,
            "journal: a SetLeader loses its compare-and-swap"
        );
        assert_sometimes!(self.trimmed_any, "journal: a truncation applies");
        assert_sometimes!(
            stale_truncate_refused,
            "journal: a truncate from a stale owner is refused"
        );
        if self.truncated_any {
            moonpool_sim::assert_reachable!(
                "journal: a write below first_seq is answered truncated"
            );
        }
        if self.reinstated_any {
            moonpool_sim::assert_reachable!("journal: a former leader uuid is reinstated");
        }
        if self.superseded_write_answered {
            moonpool_sim::assert_reachable!(
                "journal: a superseded writer is told the generation that fenced it"
            );
        }
    }
}

/// The state a verdict reveals (every verdict but an accept, a duplicate
/// and a `Noop` carries the whole state).
fn revealed(outcome: &Outcome) -> Option<JournalView> {
    match outcome {
        Outcome::Refused(state)
        | Outcome::Truncated(state)
        | Outcome::Leader(state)
        | Outcome::LeaderRefused(state)
        | Outcome::Trimmed(state)
        | Outcome::TruncateRefused(state) => Some(*state),
        Outcome::Accepted { .. } | Outcome::Duplicate { .. } | Outcome::Noop => None,
    }
}
