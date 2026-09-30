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
//! - **one owner per generation**, and **generations monotone and in the
//!   log**: a generation is born by exactly one winning `SetLeader`, one
//!   above the one before it, and a write is accepted only from the owner
//!   of the generation in force at its slot;
//! - **positions dense**: in slot order, every accepted batch starts where
//!   the last one ended, and a refusal names the position the journal stood
//!   at;
//! - **no re-accept with other bytes**: a position is accepted once, and a
//!   retry is acknowledged only with the bytes accepted there;
//! - **truncation monotone**: `first_seq` never moves backwards along the
//!   log, and never past `next_seq`.
//!
//! The per-slot facts arrive in whatever order the nodes walk; the checks
//! that need slot order (density, the generation chain, truncation) run
//! over the whole record at the end of the run ([`JournalModel::check`]), so
//! no check depends on which node reported first.

use std::collections::BTreeMap;

use moonpool_sim::{assert_always, assert_sometimes};
use paros::{Command, Control, JournalState, Outcome};

/// The write a slot decided, as the model keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WriteFact {
    generation: u64,
    owner: u64,
    seq: u64,
    count: u64,
    vhash: u64,
}

/// What one slot decided and the verdict every node reached there.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SlotFact {
    write: Option<WriteFact>,
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
    /// Per generation: its owner and the slot its `SetLeader` won at.
    owners: BTreeMap<u64, (u64, u64)>,
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
            generation: entry.generation.0,
            owner: entry.owner.0,
            seq: entry.seq.0,
            count: entry.count(),
            vhash,
        });
        let fact = SlotFact {
            write,
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
                if write.generation != state.generation.0
                    || state.owner.map(|o| o.0) != Some(write.owner)
                {
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
                let owner = state.owner.map_or(u64::MAX, |o| o.0);
                let known = *self
                    .owners
                    .entry(state.generation.0)
                    .or_insert((owner, slot));
                assert_always!(
                    known == (owner, slot),
                    "journal: a generation has one owner",
                    { "slot" => slot, "generation" => state.generation.0 }
                );
                if let Command::Control(Control::SetLeader {
                    expected,
                    owner: asked,
                }) = command
                {
                    assert_always!(
                        state.generation.0 == expected.0 + 1 && owner == asked.0,
                        "journal: a won SetLeader is the next generation, owned by its caller",
                        { "slot" => slot, "generation" => state.generation.0 }
                    );
                }
            }
            (Outcome::LeaderRefused(state), None) => {
                self.lost_any = true;
                if let Command::Control(Control::SetLeader { expected, .. }) = command {
                    assert_always!(
                        state.generation.0 != expected.0,
                        "journal: a SetLeader loses only against another generation",
                        { "slot" => slot, "generation" => state.generation.0 }
                    );
                }
            }
            (Outcome::Trimmed(_), None) => self.trimmed_any = true,
            (outcome, write) => {
                assert_always!(
                    false,
                    "journal: a verdict matches the command it judged",
                    { "slot" => slot, "outcome" => format!("{outcome:?}"), "write" => write.is_some() }
                );
            }
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
            if known.write.is_some()
                && matches!(outcome, Outcome::Refused(state) if state.generation.0 > known.write.map_or(0, |w| w.generation))
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
    /// dense, the generation chain in the log, truncation monotone. Then the
    /// journal's coverage gates.
    pub(super) fn check(&self) {
        let mut next: Option<u64> = None;
        let mut first = 0_u64;
        let mut current: Option<(u64, u64)> = None;
        let mut generation = 0_u64;
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
                assert_always!(
                    state.generation.0 >= generation,
                    "journal: generations never move backwards",
                    { "slot" => slot, "generation" => state.generation.0, "previous" => generation }
                );
                generation = state.generation.0;
            }
            if let Outcome::Leader(state) = &fact.outcome {
                current = Some((state.generation.0, state.owner.map_or(u64::MAX, |o| o.0)));
            }
            if let (Outcome::Accepted { .. }, Some(write)) = (&fact.outcome, fact.write) {
                // The generation a write was accepted under was born in the
                // log, at a lower slot, owned by its writer.
                let born = self.owners.get(&write.generation);
                assert_always!(
                    born.is_some_and(|(owner, at)| *owner == write.owner && *at < slot),
                    "journal: a write is accepted only under a generation its owner won in the log",
                    { "slot" => slot, "generation" => write.generation }
                );
                if let Some((generation, owner)) = current {
                    assert_always!(
                        (generation, owner) == (write.generation, write.owner),
                        "journal: a write is accepted only from the writer in force",
                        { "slot" => slot, "generation" => write.generation }
                    );
                }
            }
        }
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
        if self.truncated_any {
            moonpool_sim::assert_reachable!(
                "journal: a write below first_seq is answered truncated"
            );
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
fn revealed(outcome: &Outcome) -> Option<JournalState> {
    match outcome {
        Outcome::Refused(state)
        | Outcome::Truncated(state)
        | Outcome::Leader(state)
        | Outcome::LeaderRefused(state)
        | Outcome::Trimmed(state) => Some(*state),
        Outcome::Accepted { .. } | Outcome::Duplicate { .. } | Outcome::Noop => None,
    }
}
