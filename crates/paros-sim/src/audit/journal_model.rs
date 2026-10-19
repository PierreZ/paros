//! The journal state machine's oracles (#204): what every node's fold of
//! one journal owes every other, and what the journal owes its clients.
//!
//! The core judges every `Write`, `SetLeader` and `Truncate` at apply, in
//! slot order ([`paros::journal_state`]); each node reports the verdict it
//! reached at each slot it walked ([`paros::Audit::applied`]). This model
//! folds those reports — never a second implementation of the journal — and
//! asserts the invariants of `docs/architecture/06-verification.md`, section 6:
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
//!   current leader, and refused only from a caller that is not it;
//! - **the writer mode** (#241): a multi-writer journal never names a leader,
//!   accepts every write that carries records at the next position, lets
//!   anyone truncate, and refuses a fenced call or a `SetLeader` as of the
//!   wrong mode; a single-writer journal refuses an unfenced call so.
//!
//! The per-slot facts arrive in whatever order the nodes walk; the checks
//! that need slot order (density, the leader chain, truncation) run
//! over the whole record at the end of the run ([`JournalModel::check`]), so
//! no check depends on which node reported first.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{Command, Control, JournalView, LeaderUuid, Outcome, WriterMode};

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
    /// A `Truncate`'s fence, the leader uuid it names (#228), and the
    /// position it asked to trim to (#656 (verdicts no client received)).
    truncate: Option<(LeaderUuid, u64)>,
    /// A `SetLeader`'s `(new, old)` (#339: a won one is judged in slot
    /// order).
    set_leader: Option<(LeaderUuid, Option<LeaderUuid>)>,
    outcome: Outcome,
}

/// One journal's model, fed by every node's walk. Its bools are
/// independent per-run coverage flags.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct JournalModel {
    /// The journal's writer mode (#241), fixed at creation.
    mode: WriterMode,
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
    /// The highest `first_seq` any verdict revealed (#448): what the decided
    /// `Truncate`s released, the bound on what a compaction may drop.
    first_seq: u64,
    /// Per slot of an accepted batch: one past its last position (#448).
    /// Positions are dense in slot order, so the last batch below a floor
    /// ends highest.
    ends: BTreeMap<u64, u64>,
    accepted_any: bool,
    duplicate_any: bool,
    fenced_any: bool,
    gap_refused_any: bool,
    ahead_refused_any: bool,
    won_any: bool,
    lost_any: bool,
    truncated_any: bool,
    trimmed_any: bool,
    superseded_write_answered: bool,
    reinstated_any: bool,
    wrong_mode_any: bool,
    /// Every slot a node answered a client from (#656 (verdicts no client
    /// received)): the others' verdicts reach no client, so only this model
    /// judges them.
    answered: BTreeSet<u64>,
    /// Per record position of each `Duplicate`, by slot: the hashes of the
    /// records it sent, judged in [`JournalModel::check`] against the
    /// batches accepted (#656).
    duplicates: BTreeMap<u64, (u64, u64, Vec<u64>)>,
    /// Per record-batch hash, how many slots accepted it: a multi-writer
    /// write retried may land twice (#241, at-least-once).
    accepted_bytes: BTreeMap<u64, u64>,
    /// Every write a client of a created journal was told is written (#651
    /// (acked writes judged by their bytes)): its first position, its count
    /// and each record's hash, judged in [`JournalModel::check`]. A plan
    /// journal's acked writes come through its client history instead.
    acked: Vec<AckedWrite>,
}

/// A write a client was told is written at `[seq, seq + count)`, and the
/// hash of each record it sent (#651 (acked writes judged by their bytes)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AckedWrite {
    pub(crate) seq: u64,
    pub(crate) count: u64,
    pub(crate) records: Vec<u64>,
}

/// The hash a record is kept under (`user_command_hash` of its bytes).
pub(crate) fn record_hash(record: &[u8]) -> u64 {
    crate::chain::user_command_hash(record)
}

impl JournalModel {
    /// Learn the journal's writer mode (#241), before any verdict.
    pub(super) fn settle(&mut self, mode: WriterMode) {
        assert_always!(
            self.slots.is_empty(),
            "audit: a journal's mode is settled before any verdict"
        );
        self.mode = mode;
    }

    /// The journal's writer mode.
    pub(super) fn mode(&self) -> WriterMode {
        self.mode
    }

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
            Command::Control(Control::Truncate { leader, up_to }) => Some((*leader, up_to.0)),
            _ => None,
        };
        let set_leader = match command {
            Command::Control(Control::SetLeader { new, old }) => Some((*new, *old)),
            _ => None,
        };
        let fact = SlotFact {
            write,
            truncate,
            set_leader,
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
            self.first_seq = self.first_seq.max(state.first_seq.0);
        }
        // The mode check's other half (#339): a call shaped for the other
        // mode is refused as such, never judged as a call of this one.
        if self.of_wrong_mode(command) {
            assert_always!(
                matches!(fact.outcome, Outcome::WrongMode(_)),
                "journal: a call of the wrong mode is refused as such",
                { "slot" => slot, "multi" => self.mode == WriterMode::Multi }
            );
        }
        match (&fact.outcome, fact.write) {
            (Outcome::Accepted { seq, count }, Some(write)) => {
                self.accepted(slot, (seq.0, *count), write, command);
            }
            (Outcome::Duplicate { seq, count }, Some(write)) => {
                self.duplicate(slot, (seq.0, *count), write, command);
            }
            (Outcome::Refused(state), Some(write)) => self.refused(slot, state, write),
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
            (Outcome::WrongMode(_), _) => {
                self.wrong_mode_any = true;
                assert_always!(
                    self.of_wrong_mode(command),
                    "journal: a call is refused as of the wrong mode only when it is",
                    { "slot" => slot, "multi" => self.mode == WriterMode::Multi }
                );
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

    /// A `Duplicate`'s first verdict: acked at the position and count the
    /// retry names, with the bytes accepted there when the model saw them;
    /// its records are kept for [`JournalModel::check`] (#656 (verdicts no
    /// client received)).
    fn duplicate(
        &mut self,
        slot: u64,
        (seq, count): (u64, u64),
        write: WriteFact,
        command: &Command,
    ) {
        self.duplicate_any = true;
        assert_always!(
            write.seq == seq && write.count == count,
            "journal: a duplicate is acked at the position and count it names",
            { "slot" => slot, "seq" => seq, "asked" => write.seq }
        );
        if let Some((_, original)) = self.batches.get(&seq) {
            assert_always!(
                original.vhash == write.vhash,
                "journal: a retry is acked only with the bytes accepted there",
                { "slot" => slot, "seq" => seq }
            );
        }
        if let Command::Write(entry) = command {
            let hashes = entry.records.iter().map(|r| record_hash(&r.0)).collect();
            self.duplicates.insert(slot, (seq, count, hashes));
        }
    }

    /// A refused write's first verdict, against the view it was judged
    /// against.
    fn refused(&mut self, slot: u64, state: &JournalView, write: WriteFact) {
        if self.mode == WriterMode::Multi {
            assert_always!(
                write.count == 0,
                "journal: a multi-writer journal refuses only an empty write",
                { "slot" => slot, "count" => write.count }
            );
        } else if state.leader != Some(write.leader) {
            self.fenced_any = true;
        } else if write.seq > state.next_seq.0 {
            // Ahead of the journal (#339): its own gate, apart from a write
            // at a position another write took.
            self.ahead_refused_any = true;
        } else if write.seq != state.next_seq.0 {
            self.gap_refused_any = true;
        } else {
            // The leader in force, at the next position (#656 (verdicts no
            // client received)): only an empty batch is refused there.
            assert_always!(
                write.count == 0,
                "journal: an in-order write from the leader is refused only when empty",
                { "slot" => slot, "seq" => write.seq, "count" => write.count }
            );
        }
    }

    /// An accepted write's first verdict: the positions it takes, taken
    /// once, and the records there.
    fn accepted(
        &mut self,
        slot: u64,
        (seq, count): (u64, u64),
        write: WriteFact,
        command: &Command,
    ) {
        self.accepted_any = true;
        *self.accepted_bytes.entry(write.vhash).or_insert(0) += 1;
        if self.mode == WriterMode::Multi {
            assert_always!(
                !write.leader.is_set() && write.count == count && count > 0,
                "journal: a multi-writer write is accepted unfenced, records whole",
                { "slot" => slot, "seq" => seq }
            );
        } else {
            assert_always!(
                write.seq == seq && write.count == count && count > 0,
                "journal: an accepted write takes the positions it asked for",
                { "slot" => slot, "seq" => seq, "asked" => write.seq }
            );
        }
        // No position is accepted twice: the batch below this one
        // ends at or before it, the one above starts at or after its
        // end.
        let below = self.batches.range(..=seq).next_back();
        let above = self.batches.range(seq..).next();
        assert_always!(
            below.is_none_or(|(start, (_, w))| start + w.count <= seq)
                && above.is_none_or(|(start, _)| *start >= seq + count),
            "journal: a position is accepted once",
            { "slot" => slot, "seq" => seq, "count" => count }
        );
        self.batches.insert(seq, (slot, write));
        self.ends.insert(slot, seq + count);
        self.next_seq = self.next_seq.max(seq + count);
        if let Command::Write(entry) = command {
            for (position, record) in (seq..).zip(&entry.records) {
                self.records.insert(position, record_hash(&record.0));
            }
        }
    }

    /// Whether `command` is shaped for the other writer mode (#241): a
    /// leader uuid or a `SetLeader` on a multi-writer journal, the unset
    /// uuid on a single-writer one.
    fn of_wrong_mode(&self, command: &Command) -> bool {
        let fence = match command {
            Command::Write(entry) => entry.leader,
            Command::Control(Control::Truncate { leader, .. }) => *leader,
            Command::Control(Control::SetLeader { .. }) => {
                return self.mode == WriterMode::Multi;
            }
            Command::Control(Control::Noop) => return false,
        };
        match self.mode {
            WriterMode::Single => !fence.is_set(),
            WriterMode::Multi => fence.is_set(),
        }
    }

    /// A `Truncate`'s verdict against its fence (#228): accepted only from
    /// the leader the state after it names, refused only from a caller that
    /// is not the leader it was judged against. On a multi-writer journal
    /// (#241) anyone truncates, under the unset uuid, and nobody is refused.
    fn truncate_verdict(&mut self, slot: u64, fact: &SlotFact, state: &JournalView) {
        // An applied truncation trims at least to where it asked, clamped
        // to `next_seq` (#656 (verdicts no client received)); its upper
        // bound needs the floor before it, judged in slot order.
        if let (Outcome::Trimmed(_), Some((_, up_to))) = (&fact.outcome, fact.truncate) {
            assert_always!(
                state.first_seq.0 >= up_to.min(state.next_seq.0),
                "journal: a truncation trims at least to where it asked",
                { "slot" => slot, "up_to" => up_to, "first_seq" => state.first_seq.0 }
            );
        }
        if self.mode == WriterMode::Multi {
            self.trimmed_any = true;
            assert_always!(
                matches!(fact.outcome, Outcome::Trimmed(_))
                    && fact.truncate.is_some_and(|(fence, _)| !fence.is_set()),
                "journal: a multi-writer truncation applies from anyone",
                { "slot" => slot }
            );
            return;
        }
        let leads = fact
            .truncate
            .is_some_and(|(fence, _)| state.leader == Some(fence));
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
        self.answered.insert(slot);
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

    /// A client of this journal was told `write` is written (#651): judged
    /// at [`JournalModel::check`], once every verdict is in.
    pub(super) fn note_acked(&mut self, write: AckedWrite) {
        self.acked.push(write);
    }

    /// Judge one acknowledged write by its bytes (#651 (acked writes judged
    /// by their bytes)): it covers as many positions as it sent records, and
    /// every position the model saw accepted holds the record the client
    /// sent there. Returns whether the model knew one of its positions.
    pub(super) fn judge_acked(&self, write: &AckedWrite) -> bool {
        assert_always!(
            write.count == write.records.len() as u64,
            "chain: an acknowledged write covers as many positions as it sent records",
            { "seq" => write.seq, "count" => write.count, "records" => write.records.len() }
        );
        let mut judged = false;
        for (position, &sent) in (write.seq..).zip(&write.records) {
            if let Some(&held) = self.records.get(&position) {
                judged = true;
                assert_always!(
                    held == sent,
                    "chain: an acknowledged write holds its own bytes",
                    { "position" => position, "seq" => write.seq }
                );
            }
        }
        judged
    }

    /// The highest `next_seq` any verdict revealed.
    pub(super) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The highest `first_seq` any verdict revealed (#448): every position
    /// below it was released by a decided `Truncate`.
    pub(super) fn released(&self) -> u64 {
        self.first_seq
    }

    /// One past the last position of the accepted batches in slots below
    /// `floor`, `None` when the model knows none (#448). Positions are dense
    /// in slot order (checked in [`JournalModel::check`]), so this is the
    /// end of the last such batch.
    pub(super) fn end_below(&self, floor: u64) -> Option<u64> {
        self.ends.range(..floor).next_back().map(|(_, end)| *end)
    }

    /// The slot of the accepted batch that holds `position`, if the model
    /// saw it (#448).
    pub(super) fn slot_holding(&self, position: u64) -> Option<u64> {
        self.batches
            .range(..=position)
            .next_back()
            .filter(|(start, (_, write))| position < *start + write.count)
            .map(|(_, (slot, _))| *slot)
    }

    /// The slot-ordered checks, over everything folded so far: positions
    /// dense, the leader chain in the log, truncation monotone. Then the
    /// journal's coverage gates.
    /// Whether any node reported a verdict of this journal: the model has
    /// something to judge.
    /// Every `SetLeader` verdict in slot order (#653 (election term to
    /// installed leader)): its slot, the uuid it asked to install and
    /// whether it won.
    pub(super) fn set_leaders(&self) -> impl Iterator<Item = (u64, LeaderUuid, bool)> + '_ {
        self.slots.iter().filter_map(|(&slot, fact)| {
            fact.set_leader
                .map(|(new, _)| (slot, new, matches!(fact.outcome, Outcome::Leader(_))))
        })
    }

    pub(super) fn judged_any(&self) -> bool {
        !self.slots.is_empty()
    }

    pub(super) fn check(&self) {
        let mut next: Option<u64> = None;
        let mut first = 0_u64;
        let mut current: Option<LeaderUuid> = None;
        let mut stale_truncate_refused = false;
        // The floor the last verdict revealed, once one did (#656).
        let mut floor: Option<u64> = None;
        let mut unanswered_judged = false;
        for (&slot, fact) in &self.slots {
            unanswered_judged |= self.unanswered_verdict(slot, fact, &mut floor);
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
            if let (Some(state), WriterMode::Multi) = (revealed(&fact.outcome), self.mode) {
                assert_always!(
                    state.leader.is_none(),
                    "journal: a multi-writer journal never names a leader",
                    { "slot" => slot }
                );
            }
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
            if let (Outcome::Leader(_), Some((new, old))) = (&fact.outcome, fact.set_leader) {
                // A won `SetLeader` (#339) replaced the leader it named with
                // another uuid: the current leader never wins its own term
                // again.
                assert_always!(
                    old != Some(new),
                    "journal: a won SetLeader installs another uuid than the one it named",
                    { "slot" => slot }
                );
                if let Some(current) = current {
                    assert_always!(
                        old == Some(current) && new != current,
                        "journal: a SetLeader wins only over the leader in force",
                        { "slot" => slot }
                    );
                }
            }
            if let Outcome::Leader(state) = &fact.outcome {
                current = state.leader;
            }
            if let (Outcome::TruncateRefused(_), Some((fence, _))) = (&fact.outcome, fact.truncate)
                && self.leaders.get(&fence).is_some_and(|at| *at < slot)
                && current != Some(fence)
            {
                stale_truncate_refused = true;
            }
            if let (Outcome::Trimmed(_), Some((fence, _)), Some(current)) =
                (&fact.outcome, fact.truncate, current)
            {
                assert_always!(
                    fence == current,
                    "journal: a Truncate is accepted only from the writer in force",
                    { "slot" => slot }
                );
            }
            if let (Outcome::Accepted { .. }, Some(write), WriterMode::Single) =
                (&fact.outcome, fact.write, self.mode)
            {
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
        self.judge_bytes(unanswered_judged);
        self.gates(stale_truncate_refused);
    }

    /// The checks of the bytes, once every verdict is in: every
    /// `Duplicate` (#656 (verdicts no client received)) and every
    /// acknowledged write (#651 (acked writes judged by their bytes)).
    fn judge_bytes(&self, unanswered_judged: bool) {
        self.judge_duplicates();
        assert_sometimes!(
            unanswered_judged,
            "journal: a verdict no client received is judged"
        );
        let mut judged = false;
        for write in &self.acked {
            judged |= self.judge_acked(write);
        }
        if judged {
            assert_reachable!("created: an acknowledged write is judged by its bytes");
        }
    }

    /// The slot-ordered half of #656 (verdicts no client received): an
    /// applied truncation moves the floor `floor` (the one the last verdict
    /// revealed) to its clamped ask, or leaves it; `floor` moves on to this
    /// slot's. Returns whether this slot is a verdict no node answered a
    /// client from that the model's checks judge.
    fn unanswered_verdict(&self, slot: u64, fact: &SlotFact, floor: &mut Option<u64>) -> bool {
        if let (Outcome::Trimmed(state), Some((_, up_to)), Some(before)) =
            (&fact.outcome, fact.truncate, *floor)
        {
            // Every slot reaches the model (the density check relies on it
            // too), so the floor before this one is known.
            assert_always!(
                state.first_seq.0 == before.max(up_to.min(state.next_seq.0)),
                "journal: a truncation trims to its clamped ask and no further",
                { "slot" => slot, "up_to" => up_to, "first_seq" => state.first_seq.0, "before" => before }
            );
        }
        if let Some(state) = revealed(&fact.outcome) {
            *floor = Some(state.first_seq.0);
        }
        !self.answered.contains(&slot)
            && matches!(
                fact.outcome,
                Outcome::Refused(_) | Outcome::Trimmed(_) | Outcome::Duplicate { .. }
            )
    }

    /// Judge every `Duplicate` against the batches accepted (#656
    /// (verdicts no client received)), once every verdict is in: a retry is
    /// acked only where a batch starts, never mid-batch, and every position
    /// it covers holds the record it sent.
    fn judge_duplicates(&self) {
        for (&slot, (seq, count, hashes)) in &self.duplicates {
            let covering = self
                .batches
                .range(..=*seq)
                .next_back()
                .filter(|(start, (_, write))| *seq < *start + write.count);
            if let Some((start, (_, original))) = covering {
                assert_always!(
                    *start == *seq && original.count == *count,
                    "journal: a duplicate is acked only for a whole batch accepted there",
                    { "slot" => slot, "seq" => *seq, "start" => *start }
                );
            }
            for (position, sent) in (*seq..).zip(hashes) {
                if let Some(held) = self.records.get(&position) {
                    assert_always!(
                        held == sent,
                        "journal: a duplicate is acked only where its own bytes are",
                        { "slot" => slot, "position" => position }
                    );
                }
            }
        }
    }

    /// The journal's coverage gates, once the slot-ordered checks ran.
    fn gates(&self, stale_truncate_refused: bool) {
        if self.mode == WriterMode::Multi {
            assert_sometimes!(
                self.accepted_any,
                "journal: a multi-writer journal accepts a write"
            );
            assert_sometimes!(
                self.trimmed_any,
                "journal: a multi-writer truncation applies"
            );
            assert_sometimes!(
                self.wrong_mode_any,
                "journal: a multi-writer journal refuses a fenced call"
            );
            assert_sometimes!(
                self.accepted_bytes.values().any(|&n| n > 1),
                "journal: a multi-writer write lands twice"
            );
            return;
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
            self.ahead_refused_any,
            "journal: a write ahead of the journal is refused"
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
        | Outcome::TruncateRefused(state)
        | Outcome::WrongMode(state) => Some(*state),
        Outcome::Accepted { .. } | Outcome::Duplicate { .. } | Outcome::Noop => None,
    }
}
