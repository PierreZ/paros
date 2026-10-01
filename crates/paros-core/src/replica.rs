//! The **replica**: the chosen log, its application order, and the journal
//! state machine folded over it — and nothing else.
//!
//! A replica consumes one kind of fact — *slot `s` chose value `v`* — and
//! turns it into the contiguous applied prefix, never caring *why* a value
//! was chosen (an accept quorum this node counted, a `Commit` off the wire, a
//! catch-up replay, a handoff's decided tail). It owns:
//!
//! - the **chosen map** and the durable **chosen index** (the commit index:
//!   every slot at or below it is chosen and applied in order);
//! - the contiguous **walk** ([`Replica::advance`]) that surfaces newly
//!   applied entries, bounded per batch;
//! - the **journal fold** (#204): the [`JournalState`] of the journal the log
//!   carries, judged at apply in slot order
//!   ([`crate::journal_state`]) — the state at the retention floor (the
//!   `base`, sealed durably when truncation drops the slots it was folded
//!   from and carried by a trim-point jump), the state at the fold's head,
//!   the [`Outcome`] of every retained slot (what a driver answers the call
//!   that proposed it from), and the **positions index** (where each
//!   accepted write's records sit) a read and a retry are answered from;
//! - the **journal read** ([`Replica::read`]): the records from a position
//!   up, served from the fold's head.
//!
//! "Applied" here names the walk and the fold, not an application: paros runs
//! no user application (#186, #204), one journal-control state machine per
//! journal, and a slot is applied the moment the walk moves the prefix over
//! it.
//!
//! **The fold is contiguous or it stops.** Every slot's outcome depends on
//! every slot before it, so the fold never steps over a slot it does not
//! hold: a faulty record inside the chosen prefix (a boot read it back
//! damaged, CTRL Stage 8) stops the fold there, and the walk is *held* until
//! the record heals ([`Replica::learn`]) or a trim-point jump carries the
//! state past it ([`Replica::trim_to`]). Walking past the hole used to
//! decide a #94 duplicate of the missing identity as its first application
//! (seed 16921589310752617664); with the journal fold it would judge every
//! later write against the wrong next position.
//!
//! It knows nothing about ballots, promises, leadership, quorums or the
//! network. The one cross-component fact it consults is handed to it as
//! data: for the chosen/accepted coupling the walk asserts, a predicate over
//! the accepted log. Durable changes are emitted as [`WriteOp`]s into the
//! caller's batch.
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*).

use std::collections::BTreeMap;

use crate::journal_state::{JournalState, Outcome};
use crate::types::{Command, Control, Entry, Seq, Slot, Value};
use crate::write::WriteOp;

/// Maximum slots the contiguous apply walk releases in one batch — the
/// bound this role enforces in [`Replica::advance`], so one `Ready` never
/// hands the driver an unbounded run.
pub const APPLY_BATCH: usize = 64;

/// The answer to a journal read ([`Replica::read`], #204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogRead {
    /// The read started below `first_seq`: those records are gone, and the
    /// state names where the journal now starts.
    Truncated(JournalState),
    /// A page of records.
    Page(LogPage),
}

/// One page of a journal read ([`Replica::read`]): the records at
/// `[from, from + records.len())`, in order, and the state they were served
/// from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogPage {
    /// The first record's position (the read's `from_seq`).
    pub from: Seq,
    /// The records, dense from `from`.
    pub records: Vec<Value>,
    /// The journal state at the fold's head the page was served from: its
    /// `next_seq` is the committed end, its generation and owner the current
    /// writer — how every tailer learns the owner changed, in-band.
    pub state: JournalState,
}

impl LogPage {
    /// Where the next read starts.
    #[must_use]
    pub fn next(&self) -> Seq {
        Seq(self.from.0 + self.records.len() as u64)
    }
}

/// The replica: chosen log, applied prefix, journal fold. See the module doc.
#[derive(Clone, Debug)]
pub struct Replica {
    /// Every slot this node knows chosen, with its value — contiguous or not.
    chosen: BTreeMap<Slot, Command>,
    /// Highest contiguous chosen slot (the commit index), or `None` when
    /// nothing is chosen yet. Durable ([`WriteOp::SetChosenIndex`]).
    chosen_index: Option<Slot>,
    /// The walk released one bounded chunk and the next slot is already
    /// chosen: a deferred continuation the caller resumes after its batch.
    advance_pending: bool,
    /// The slot [`Replica::base`] was folded up to: the retention floor.
    floor: Slot,
    /// The journal state after every slot below `floor` — sealed durably
    /// with each truncation, carried by a trim-point jump.
    base: JournalState,
    /// The first slot the fold has not applied: at or below the first
    /// unchosen slot, and below it only at a hole (a slot of the prefix whose
    /// value this node does not hold).
    folded: Slot,
    /// The journal state after every slot below `folded`.
    state: JournalState,
    /// The state after each retained slot that moved it, so the state at any
    /// retained slot — a new floor's base — is one lookup.
    history: BTreeMap<Slot, JournalState>,
    /// The outcome of every retained folded slot that was not a `Noop`.
    outcomes: BTreeMap<Slot, Outcome>,
    /// The positions index: the slot of every retained accepted write, by
    /// its first record's position.
    positions: BTreeMap<Seq, Slot>,
    /// A `Truncate` was folded since the last walk: the caller compacts to
    /// [`Replica::compaction_target`] after it.
    truncate_due: bool,
    /// Newly applied `(slot, command, verdict)` triples, in order, for the
    /// caller's `Ready` batch. The verdict rides with the slot because a
    /// `Truncate` folded later in the same walk may compact the slot — and
    /// its [`Replica::outcome_at`] — before the caller reports it.
    committed: Vec<(Slot, Command, Outcome)>,
}

impl Replica {
    /// Rebuild the replica from what a boot scan read back: the durable
    /// chosen index, the retention floor and the journal state sealed at it,
    /// and the retained accepted log (every record at or below the chosen
    /// index carries the chosen value — the P2c chain).
    ///
    /// The fold starts from the sealed state at the floor and replays the
    /// retained chosen records in slot order, stopping at the first one the
    /// scan could not read (a faulty record): a restarted node reaches
    /// exactly the state a node that never restarted holds. A truncation the
    /// replay passes is not re-applied to the log: the store's floor is what
    /// it is, and a lower floor only retains more.
    #[must_use]
    pub fn from_boot(
        chosen_index: Option<Slot>,
        floor: Slot,
        sealed: JournalState,
        records: &BTreeMap<Slot, (crate::types::Ballot, Command)>,
    ) -> Self {
        let chosen: BTreeMap<Slot, Command> = records
            .iter()
            .filter(|(slot, _)| chosen_index.is_some_and(|ci| **slot <= ci))
            .map(|(slot, (_, command))| (*slot, command.clone()))
            .collect();
        let mut replica = Self {
            chosen,
            chosen_index,
            advance_pending: false,
            floor,
            base: sealed,
            folded: floor,
            state: sealed,
            history: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            positions: BTreeMap::new(),
            truncate_due: false,
            committed: Vec::new(),
        };
        replica.refold();
        replica.truncate_due = false;
        replica
    }

    /// The replica's own cross-field invariants against the retention floor
    /// `floor` (the caller's compaction floor, handed in as data).
    ///
    /// # Panics
    ///
    /// Panics when a replica invariant is broken: a programmer error, never
    /// an operating condition.
    pub fn assert_invariants(&self, floor: Slot) {
        // A chosen first-unchosen slot is legal only as the explicit bounded
        // continuation left by the walk — an iff, split into its two
        // directions so a violation names the side that broke.
        if self.chosen.contains_key(&self.first_unchosen()) {
            assert!(
                self.advance_pending,
                "a chosen first-unchosen slot has a deferred prefix continuation"
            );
        }
        if self.advance_pending {
            assert!(
                self.chosen.contains_key(&self.first_unchosen()),
                "a deferred prefix continuation names a chosen first-unchosen slot"
            );
        }
        assert!(
            self.chosen.keys().next().is_none_or(|s| *s >= floor),
            "no chosen record survives below the compaction floor"
        );
        assert!(
            self.floor == floor,
            "the journal fold's base sits at the compaction floor"
        );
        assert!(
            self.floor <= self.folded && self.folded <= self.first_unchosen(),
            "the journal fold lies between the floor and the first unchosen slot"
        );
        if self.folded < self.first_unchosen() {
            assert!(
                !self.chosen.contains_key(&self.folded),
                "the journal fold stops only at a slot it does not hold"
            );
        }
        assert!(
            self.outcomes.keys().next().is_none_or(|s| *s >= floor)
                && self.history.keys().next().is_none_or(|s| *s >= floor)
                && self
                    .outcomes
                    .keys()
                    .next_back()
                    .is_none_or(|s| *s < self.folded),
            "the journal fold's per-slot record lies between the floor and the fold"
        );
        self.base.assert_invariants();
        self.state.assert_invariants();
        assert!(
            self.base.next_seq <= self.state.next_seq
                && self.base.first_seq <= self.state.first_seq
                && self.base.generation <= self.state.generation,
            "the fold's head never lies behind its base"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// The durable chosen index (the commit index).
    #[must_use]
    pub fn chosen_index(&self) -> Option<Slot> {
        self.chosen_index
    }

    /// Whether the journal fold covers `index` — the replica's one answer to
    /// a quorum read (#143, Compartmentalized Paxos §3.4): a read at
    /// watermark `index` may be served once the replica has chosen *and
    /// folded* everything up to it. `None` is the empty watermark, covered by
    /// any prefix. The replica consumes "applied past `index`" and nothing
    /// else — it never sees the watermark tally that produced the index.
    #[must_use]
    pub fn covers(&self, index: Option<Slot>) -> bool {
        index.is_none_or(|i| i < self.folded)
    }

    /// First slot not in the contiguous chosen prefix.
    #[must_use]
    pub fn first_unchosen(&self) -> Slot {
        match self.chosen_index {
            Some(s) => Slot(s.0 + 1),
            None => Slot(0),
        }
    }

    /// Every slot known chosen, contiguous or not.
    #[must_use]
    pub fn chosen(&self) -> &BTreeMap<Slot, Command> {
        &self.chosen
    }

    /// Whether `slot` is known chosen here.
    #[must_use]
    pub fn is_chosen(&self, slot: Slot) -> bool {
        self.chosen.contains_key(&slot)
    }

    /// The value chosen at `slot`, if known.
    #[must_use]
    pub fn chosen_at(&self, slot: Slot) -> Option<&Command> {
        self.chosen.get(&slot)
    }

    /// The journal state at the fold's head.
    #[must_use]
    pub fn journal(&self) -> JournalState {
        self.state
    }

    /// The journal state at the retention floor (sealed with the last
    /// truncation, or carried by the last trim-point jump).
    #[must_use]
    pub fn journal_base(&self) -> JournalState {
        self.base
    }

    /// The first slot the fold has not applied.
    #[must_use]
    pub fn folded(&self) -> Slot {
        self.folded
    }

    /// The slot the fold is stopped at, when it is stopped below the first
    /// unchosen slot: a slot of the prefix whose value this node does not
    /// hold, that a catch-up must bring back before anything past it applies.
    #[must_use]
    pub fn fold_hole(&self) -> Option<Slot> {
        (self.folded < self.first_unchosen()).then_some(self.folded)
    }

    /// The outcome of the folded slot `slot`, if it is retained and was not
    /// a `Noop`.
    #[must_use]
    pub fn outcome_at(&self, slot: Slot) -> Option<&Outcome> {
        self.outcomes.get(&slot)
    }

    /// Newly applied entries this batch, in order, each with the verdict
    /// the journal state machine gave it.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command, Outcome)] {
        &self.committed
    }

    /// Drop the batch's applied entries (the caller consumed them).
    pub fn clear_committed(&mut self) {
        self.committed.clear();
    }

    /// The **chosen gap**, if this node holds one: `(hole, highest)` where
    /// `hole` is the first slot missing from the contiguous prefix and
    /// `highest` the highest slot above it already known chosen. `None` when
    /// the chosen set is contiguous.
    ///
    /// A read-only observability accessor: the core cannot trace, and the gap
    /// is invisible from outside because [`Ready::committed`](crate::Ready::committed)
    /// only ever surfaces the *contiguous* prefix. A gap is a normal transient
    /// (pipelining, a follower that missed one `Commit`); a gap that
    /// **survives quiescence** is the wedge this exists to make observable —
    /// the chosen index frozen at `hole - 1` cluster-wide while higher slots
    /// keep being chosen.
    #[must_use]
    pub fn chosen_gap(&self) -> Option<(Slot, Slot)> {
        let hole = self.first_unchosen();
        let highest = *self.chosen.range(hole..).next_back()?.0;
        Some((hole, highest))
    }

    /// The write accepted with its first record at `seq`, if it is retained
    /// — what the journal state machine compares a retry against.
    #[must_use]
    pub fn accepted_at(&self, seq: Seq) -> Option<&Entry> {
        self.positions
            .get(&seq)
            .and_then(|slot| self.chosen.get(slot))
            .and_then(Command::write)
    }

    /// The journal state after every slot below `slot` (a retained slot at
    /// or past the floor, at or below the fold).
    fn state_at(&self, slot: Slot) -> JournalState {
        assert!(
            slot >= self.floor && slot <= self.folded,
            "the journal state is asked at a folded slot"
        );
        self.history
            .range(..slot)
            .next_back()
            .map_or(self.base, |(_, state)| *state)
    }

    /// The highest slot a decided truncation lets this replica drop. `None`
    /// when nothing may go.
    ///
    /// A floor must keep every record a retained slot's verdict read, not
    /// only the journal's first retained one: a node that reboots (or jumps
    /// to a peer's trim point) refolds every slot from its floor, and a
    /// retry above it — a write at a position already inside the journal
    /// when it folded — is judged against the record at that position,
    /// which a `Truncate` above the retry may since have released. So the
    /// floor starts at the slot holding the current first record and walks
    /// down to the lowest slot holding a record some retained retry read,
    /// until that is a fixed point (or, when every record is truncated, it
    /// starts at the fold's head). Dropping only up to the current first
    /// record left such a retry nothing to compare against on the refold:
    /// `Refused` on the rebooted node where every other node had folded
    /// `Duplicate` (the audit's "every node judges a slot to the same
    /// outcome", witness seed 9212868850674249062 of the sweep that landed
    /// #204).
    ///
    /// # Panics
    ///
    /// If the positions index lost the write holding a retained record: a
    /// programmer error.
    #[must_use]
    pub fn compaction_target(&self) -> Option<Slot> {
        let mut first = self.slot_holding(self.state.first_seq);
        loop {
            let needed = self
                .chosen
                .range(first..self.folded)
                .filter_map(|(slot, command)| {
                    let entry = command.write()?;
                    let at = self.state_at(*slot);
                    (entry.seq >= at.first_seq && entry.seq < at.next_seq)
                        .then(|| self.slot_holding(entry.seq))
                })
                .min()
                .unwrap_or(first);
            if needed >= first {
                break;
            }
            first = needed;
        }
        assert!(
            first >= self.floor && first <= self.folded,
            "a compaction floor lies inside the retained fold"
        );
        first.0.checked_sub(1).map(Slot)
    }

    /// The slot holding the record at `position` — the write whose batch
    /// covers it — or the fold's head when `position` is at or past the
    /// journal's end (no record there to keep), or the floor when the record
    /// already lies below it (a node that jumped to a peer's trim point holds
    /// nothing below the jump, and a later `Truncate` may still name a
    /// position there). Never below the floor.
    fn slot_holding(&self, position: Seq) -> Slot {
        if position >= self.state.next_seq {
            return self.folded;
        }
        let Some((start, slot)) = self.positions.range(..=position).next_back() else {
            // Nothing indexed at or below it: the record sits in a slot below
            // the floor — every position the floor's base counts is there.
            // Red→green: canary seed 3961852116714272941 (a joiner jumped to
            // slot 15, base `next_seq` 9, then folded a `Truncate` to 8 and
            // panicked looking for the record at 8).
            assert!(
                position < self.base.next_seq,
                "a record the index lacks lies below the floor"
            );
            return self.floor;
        };
        let entry = self
            .chosen
            .get(slot)
            .and_then(Command::write)
            .expect("an indexed write is a retained chosen write");
        assert!(
            *start <= position && position.0 < start.0 + entry.count(),
            "a retained record lies inside the write indexed below it"
        );
        (*slot).max(self.floor)
    }

    /// One page of a **journal read** (#204, `Read(from_seq, limit)`): the
    /// records from position `from` up, served from the fold's head and
    /// never past it.
    ///
    /// A read that starts below `first_seq` is [`LogRead::Truncated`], and
    /// nothing else, since the records there are gone. `limit` bounds the
    /// page's record count and `max_bytes` the sum of its records' bytes,
    /// except that a page that could hold a record always holds at least one
    /// — a single record larger than the budget must still be readable. A
    /// read may start inside a batch: positions are per record.
    ///
    /// A `from` at or past `next_seq` returns an empty page: the caller (the
    /// driver) decides whether to wait for the journal to grow — the
    /// long-poll is not the core's.
    ///
    /// # Panics
    ///
    /// If the positions index does not cover a position below `next_seq`
    /// (a programmer error).
    #[must_use]
    pub fn read(&self, from: Seq, limit: usize, max_bytes: usize) -> LogRead {
        let state = self.state;
        if from < state.first_seq {
            return LogRead::Truncated(state);
        }
        let mut page = LogPage {
            from,
            records: Vec::new(),
            state,
        };
        let mut bytes = 0_usize;
        let mut at = from;
        'writes: while at < state.next_seq && page.records.len() < limit {
            let (start, slot) = self
                .positions
                .range(..=at)
                .next_back()
                .expect("every position below next_seq lies in a retained write");
            let entry = self
                .chosen
                .get(slot)
                .and_then(Command::write)
                .expect("an indexed write is a retained chosen write");
            let offset = usize::try_from(at.0 - start.0).expect("a batch fits in memory");
            assert!(
                offset < entry.records.len(),
                "a position below next_seq lies inside the write indexed below it"
            );
            for record in &entry.records[offset..] {
                let size = record.0.len();
                if page.records.len() >= limit
                    || (!page.records.is_empty() && bytes.saturating_add(size) > max_bytes)
                {
                    break 'writes;
                }
                bytes = bytes.saturating_add(size);
                page.records.push(record.clone());
                at = Seq(at.0 + 1);
            }
        }
        // Postcondition: the page never passes the fold's head.
        assert!(
            from >= state.next_seq || page.next() <= state.next_seq,
            "a read page ends at or below the journal's next position"
        );
        LogRead::Page(page)
    }

    // ---- learning -------------------------------------------------------------

    /// Learn `slot` chosen with `command`. The caller has already checked the
    /// slot is retained and not yet known chosen here. A slot that fills the
    /// hole the fold is stopped at resumes the fold at once.
    ///
    /// # Panics
    ///
    /// If `slot` is already known chosen.
    pub fn learn(&mut self, slot: Slot, command: &Command) {
        assert!(
            !self.chosen.contains_key(&slot),
            "a slot is learned chosen once"
        );
        assert!(slot >= self.folded, "a folded slot is never relearned");
        self.chosen.insert(slot, command.clone());
        if slot == self.folded && slot < self.first_unchosen() {
            self.refold();
        }
    }

    /// Fold one slot at the fold's head, and return its verdict.
    fn fold_one(&mut self, slot: Slot, command: &Command) -> Outcome {
        assert!(
            slot == self.folded,
            "the journal fold advances one slot at a time"
        );
        let before = self.state;
        let mut state = self.state;
        let outcome = state.apply(command, |seq| self.accepted_at(seq));
        self.state = state;
        if let (Outcome::Accepted { seq, .. }, Command::Write(_)) = (&outcome, command) {
            self.positions.insert(*seq, slot);
        }
        if matches!(command, Command::Control(Control::Truncate { .. })) {
            self.truncate_due = true;
        }
        if self.state != before {
            self.history.insert(slot, self.state);
        }
        if outcome != Outcome::Noop {
            self.outcomes.insert(slot, outcome.clone());
        }
        self.folded = Slot(slot.0 + 1);
        outcome
    }

    /// Resume the fold below the first unchosen slot, through every chosen
    /// slot it now holds.
    fn refold(&mut self) {
        let end = self.first_unchosen();
        while self.folded < end {
            let slot = self.folded;
            let Some(command) = self.chosen.get(&slot).cloned() else {
                break;
            };
            self.fold_one(slot, &command);
        }
    }

    /// Walk the contiguous chosen prefix forward, folding and surfacing each
    /// newly applied `(slot, command)` in order (no gaps), bounded per batch.
    /// Returns the compaction target ([`Replica::compaction_target`]) when a
    /// `Truncate` was folded, for the caller to compact *after* the walk.
    ///
    /// A fold stopped at a hole below the prefix **holds** the walk: nothing
    /// past the hole is judged until it heals.
    ///
    /// `records_agree(slot, command)` answers "does the authoritative
    /// accepted record for `slot` hold exactly this command?" — the coupling
    /// assertion, and the only thing the walk needs to know about the
    /// acceptor. A predicate, not the acceptor's map: the replica consumes
    /// "slot chosen, value" and must not acquire the accepted log merely
    /// because one deployment colocates the two roles.
    ///
    /// # Panics
    ///
    /// If the walk's contiguity or the chosen/accepted coupling is broken.
    pub fn advance(
        &mut self,
        records_agree: impl Fn(Slot, &Command) -> bool,
        writes: &mut Vec<WriteOp>,
    ) -> Option<Slot> {
        self.refold();
        if self.folded == self.first_unchosen() {
            let mut next = self.first_unchosen();
            let mut advanced = 0_usize;
            while advanced < APPLY_BATCH
                && let Some(command) = self.chosen.get(&next).cloned()
            {
                // The walk is the *only* writer of `chosen_index`, and it
                // advances exactly one slot per iteration — the contiguity the
                // apply seam and the boot rebuild are built on.
                assert!(
                    next == self.first_unchosen(),
                    "the chosen prefix advances one slot at a time"
                );
                // The chosen/accepted coupling, per applied slot.
                assert!(
                    records_agree(next, &command),
                    "an applied slot's accepted record carries the applied command"
                );
                self.chosen_index = Some(next);
                writes.push(WriteOp::SetChosenIndex(next));
                let outcome = self.fold_one(next, &command);
                self.committed.push((next, command, outcome));
                next = Slot(next.0 + 1);
                advanced += 1;
            }
            // Postcondition: either the walk consumed the entire contiguous
            // chosen prefix, or exactly one bounded chunk was released.
            assert!(
                advanced == APPLY_BATCH || !self.chosen.contains_key(&self.first_unchosen()),
                "the walk consumes or bounds the contiguous chosen prefix"
            );
        }
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        if std::mem::take(&mut self.truncate_due) {
            self.compaction_target()
        } else {
            None
        }
    }

    // ---- log prefix drops -----------------------------------------------------

    /// Drop everything below `first` after a decided truncation, and return
    /// the journal state there — the new base, which the caller seals
    /// durably with the truncation.
    ///
    /// # Panics
    ///
    /// If `first` lies outside `[floor, folded]`, or would drop a record at
    /// or past the journal's first retained position.
    pub fn truncate(&mut self, first: Slot) -> JournalState {
        assert!(
            self.compaction_target().is_some_and(|t| first.0 <= t.0 + 1),
            "a truncation never drops a retained record"
        );
        let base = self.state_at(first);
        self.base = base;
        self.floor = first;
        self.chosen = self.chosen.split_off(&first);
        self.history = self.history.split_off(&first);
        self.outcomes = self.outcomes.split_off(&first);
        self.positions.retain(|_, slot| *slot >= first);
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        base
    }

    /// Jump below a peer's trim point (#186, [`crate::Message::TrimmedTo`]):
    /// drop everything below `point`, move the chosen index to at least
    /// `point - 1` (everything below a trim point is chosen), and take the
    /// journal state the peer sealed at its floor when this node's own fold
    /// has not reached it. A fold already past the point keeps its own state
    /// (the prefixes agree cluster-wide) and only the prefix below the point
    /// goes. Returns the new base, for the caller to seal.
    ///
    /// # Panics
    ///
    /// If `point` is slot zero (nothing lies below it, and no honest peer
    /// trims nothing).
    pub fn trim_to(&mut self, point: Slot, sealed: JournalState) -> JournalState {
        assert!(point.0 > 0, "a trim point has a chosen slot below it");
        let boundary = Slot(point.0 - 1);
        if self.chosen_index.is_none_or(|ci| ci < boundary) {
            self.chosen_index = Some(boundary);
        }
        let base = if self.folded >= point {
            self.state_at(point)
        } else {
            self.history.clear();
            self.outcomes.clear();
            self.positions.clear();
            self.state = sealed;
            self.folded = point;
            sealed
        };
        self.base = base;
        self.floor = point;
        self.chosen = self.chosen.split_off(&point);
        self.history = self.history.split_off(&point);
        self.outcomes = self.outcomes.split_off(&point);
        self.positions.retain(|_, slot| *slot >= point);
        self.refold();
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        base
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{LogRead, Replica};
    use crate::journal_state::{JournalState, Outcome};
    use crate::types::{
        Ballot, ClientId, Command, Control, Entry, Generation, NodeId, Seq, Slot, Value,
    };

    fn write(seq: u64, records: &[&[u8]]) -> Command {
        Command::Write(Entry {
            generation: Generation(1),
            owner: ClientId(1),
            seq: Seq(seq),
            records: records.iter().map(|r| Value(r.to_vec())).collect(),
        })
    }

    fn claim() -> Command {
        Command::Control(Control::SetLeader {
            expected: Generation(0),
            owner: ClientId(1),
        })
    }

    fn records(commands: &[Command]) -> BTreeMap<Slot, (Ballot, Command)> {
        let ballot = Ballot {
            round: 1,
            node: NodeId(1),
        };
        commands
            .iter()
            .enumerate()
            .map(|(i, c)| (Slot(i as u64), (ballot, c.clone())))
            .collect()
    }

    /// A replica whose chosen prefix is `commands` from slot 0.
    fn replica(commands: &[Command]) -> Replica {
        let ci = commands.len().checked_sub(1).map(|i| Slot(i as u64));
        Replica::from_boot(ci, Slot(0), JournalState::default(), &records(commands))
    }

    fn page(read: LogRead) -> super::LogPage {
        match read {
            LogRead::Page(page) => page,
            LogRead::Truncated(_) => panic!("expected a page"),
        }
    }

    fn bytes(page: &super::LogPage) -> Vec<Vec<u8>> {
        page.records.iter().map(|v| v.0.clone()).collect()
    }

    /// A floor keeps every record a retained slot's verdict read, not only
    /// the journal's first retained one: a node that refolds from the floor
    /// judges a retry above it exactly as before.
    #[test]
    fn a_floor_keeps_every_record_a_retained_slot_compares_against() {
        let commands = [
            claim(),
            write(0, &[b"a"]),
            write(1, &[b"b"]),
            write(0, &[b"a"]), // a retry of position 0
            Command::Control(Control::Truncate { up_to: Seq(1) }),
        ];
        let mut r = replica(&commands);
        assert!(matches!(
            r.outcome_at(Slot(3)),
            Some(Outcome::Duplicate { .. })
        ));
        // The first retained record (position 1) sits at slot 2, but slot
        // 3's verdict read position 0 at slot 1: the floor stops at slot 1.
        assert_eq!(r.compaction_target(), Some(Slot(0)));
        let first = Slot(1);
        let sealed = r.truncate(first);
        let mut recs = records(&commands);
        recs.retain(|slot, _| *slot >= first);
        let rebooted = Replica::from_boot(Some(Slot(4)), first, sealed, &recs);
        assert!(matches!(
            rebooted.outcome_at(Slot(3)),
            Some(Outcome::Duplicate { .. })
        ));
        assert_eq!(rebooted.journal(), r.journal());
    }

    #[test]
    fn a_truncation_below_a_jumped_floor_keeps_the_floor() {
        // A node that jumped to slot 2 holds nothing below it: positions 0
        // and 1 lie in slots it never saw. A `Truncate` to position 1 folded
        // after the jump names a record that is already gone here.
        let base = JournalState {
            owner: Some(ClientId(1)),
            generation: Generation(1),
            next_seq: Seq(2),
            first_seq: Seq(0),
        };
        let commands = [
            claim(),
            write(0, &[b"a"]),
            write(2, &[b"c"]),
            Command::Control(Control::Truncate { up_to: Seq(1) }),
        ];
        let mut recs = records(&commands);
        recs.retain(|slot, _| *slot >= Slot(2));
        let r = Replica::from_boot(Some(Slot(3)), Slot(2), base, &recs);
        assert_eq!(r.journal().first_seq, Seq(1));
        assert_eq!(r.compaction_target(), Some(Slot(1)), "the floor stays");
    }

    #[test]
    fn a_read_is_dense_by_position_and_skips_every_hole() {
        let r = replica(&[
            claim(),
            write(0, &[b"a", b"b"]),
            Command::Control(Control::Noop),
            write(0, &[b"a", b"b"]), // a retry: acked, no position
            write(9, &[b"gap"]),     // refused: no position
            write(2, &[b"c"]),
        ]);
        let p = page(r.read(Seq(0), 64, 1024));
        assert_eq!(bytes(&p), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(p.next(), Seq(3));
        assert_eq!(p.state.next_seq, Seq(3));
        assert!(matches!(
            r.outcome_at(Slot(3)),
            Some(Outcome::Duplicate { .. })
        ));
        assert!(matches!(r.outcome_at(Slot(4)), Some(Outcome::Refused(_))));
    }

    #[test]
    fn a_read_may_start_inside_a_batch_and_is_bounded() {
        let r = replica(&[claim(), write(0, &[b"xxxx", b"yyyy", b"z"])]);
        let p = page(r.read(Seq(1), 64, 1024));
        assert_eq!(bytes(&p), vec![b"yyyy".to_vec(), b"z".to_vec()]);
        let p = page(r.read(Seq(0), 64, 1));
        assert_eq!(p.records.len(), 1, "a page always carries one record");
        let p = page(r.read(Seq(0), 2, 1024));
        assert_eq!(p.next(), Seq(2));
    }

    #[test]
    fn a_read_at_the_end_is_empty_and_below_first_seq_is_truncated() {
        let r = replica(&[
            claim(),
            write(0, &[b"a"]),
            write(1, &[b"b"]),
            Command::Control(Control::Truncate { up_to: Seq(1) }),
        ]);
        let p = page(r.read(Seq(2), 64, 64));
        assert!(p.records.is_empty());
        let p = page(r.read(Seq(7), 64, 64));
        assert_eq!(p.next(), Seq(7));
        assert!(matches!(r.read(Seq(0), 64, 64), LogRead::Truncated(s) if s.first_seq == Seq(1)));
    }

    #[test]
    fn truncation_keeps_the_write_holding_the_first_record_and_seals_its_base() {
        let mut r = replica(&[
            claim(),
            write(0, &[b"a"]),
            write(1, &[b"b", b"c"]),
            Command::Control(Control::Truncate { up_to: Seq(2) }),
        ]);
        // Slot 2 holds position 2: everything below it may go.
        assert_eq!(r.compaction_target(), Some(Slot(1)));
        let base = r.truncate(Slot(2));
        assert_eq!(base.next_seq, Seq(1));
        assert_eq!(base.generation, Generation(1));
        let p = page(r.read(Seq(2), 64, 64));
        assert_eq!(bytes(&p), vec![b"c".to_vec()]);
        // A reboot from the sealed base folds to the same head.
        let kept: Vec<Command> = r.chosen().values().cloned().collect();
        let mut recs = BTreeMap::new();
        for (i, c) in kept.iter().enumerate() {
            recs.insert(
                Slot(2 + i as u64),
                (
                    Ballot {
                        round: 1,
                        node: NodeId(1),
                    },
                    c.clone(),
                ),
            );
        }
        let rebooted = Replica::from_boot(Some(Slot(3)), Slot(2), base, &recs);
        assert_eq!(rebooted.journal(), r.journal());
    }

    #[test]
    fn a_hole_below_the_prefix_stops_the_fold_until_it_heals() {
        let mut recs = records(&[claim(), write(0, &[b"a"]), write(1, &[b"b"])]);
        let lost = recs.remove(&Slot(1)).expect("slot 1").1;
        let mut r = Replica::from_boot(Some(Slot(2)), Slot(0), JournalState::default(), &recs);
        assert_eq!(r.fold_hole(), Some(Slot(1)));
        assert!(!r.covers(Some(Slot(1))));
        r.learn(Slot(1), &lost);
        assert_eq!(r.fold_hole(), None);
        assert_eq!(r.journal().next_seq, Seq(2));
    }
}
