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

use crate::journal_state::{JournalState, JournalView, Outcome, WriterMode};
use crate::types::{Command, Entry, Seq, Slot, Value};
use crate::write::WriteOp;

/// Maximum slots the contiguous apply walk releases in one batch — the
/// bound this role enforces in [`Replica::advance`], so one `Ready` never
/// hands the driver an unbounded run.
pub const APPLY_BATCH: usize = 64;

// A walk that releases nothing per batch would never apply a slot.
const _: () = assert!(APPLY_BATCH > 0);

/// The answer to a journal read ([`Replica::read`], #204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogRead {
    /// The read started below `first_seq`: those records are gone, and the
    /// view names where the journal now starts.
    Truncated(JournalView),
    /// A page of records.
    Page(LogPage),
    /// The read starts at a position the journal at this fold's head still
    /// counts (at or past its `first_seq`) but that lies below this replica's
    /// floor: the record is gone from here, not from the journal. Only a
    /// replica whose floor rose before its fold reached the truncation that
    /// let it rise answers this — one that jumped to a peer's trim point
    /// ([`Replica::trim_to`]) or rebooted onto a fold stopped at a hole —
    /// typically only until its fold catches up. The reader asks elsewhere: the
    /// driver answers it unserved, never `Truncated` (the journal still has
    /// the record) and never a page (this replica cannot produce it).
    NotHeld,
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
    /// The journal view at the fold's head the page was served from: its
    /// `next_seq` is the committed end, its leader uuid the current leader —
    /// how every tailer learns the leader changed, in-band.
    pub state: JournalView,
}

impl LogPage {
    /// Where the next read starts.
    #[must_use]
    ///
    /// # Panics
    ///
    /// If the page runs past the position space (a programmer error).
    pub fn next(&self) -> Seq {
        let len = self.records.len() as u64;
        assert!(
            self.from.0.checked_add(len).is_some(),
            "a page ends inside the position space"
        );
        let next = Seq(self.from.0 + len);
        assert!(next >= self.from, "a page never ends before it starts");
        next
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
    /// The journal's writer mode (#241): configuration, fixed at creation,
    /// handed to every [`JournalState::apply`].
    mode: WriterMode,
}

impl Replica {
    /// Rebuild the replica from what a boot scan read back: the durable
    /// chosen index, the retention floor and the journal state sealed at it,
    /// and the retained accepted log (every record at or below the chosen
    /// index carries the chosen value — the P2c chain), under the journal's
    /// writer `mode`.
    ///
    /// The fold starts from the sealed state at the floor and replays the
    /// retained chosen records in slot order, stopping at the first one the
    /// scan could not read (a faulty record): a restarted node reaches
    /// exactly the state a node that never restarted holds. A truncation the
    /// replay passes is not re-applied to the log: the store's floor is what
    /// it is, and a lower floor only retains more.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn from_boot(
        mode: WriterMode,
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
            mode,
        };
        replica.refold();
        replica.truncate_due = false;
        // The boot read-back of the walk's durable writes: everything the
        // chosen index covers is chosen, and the fold lands between the
        // sealed floor and the first unchosen slot.
        assert!(
            replica.chosen.keys().next_back().copied() <= chosen_index,
            "a boot rebuild holds no chosen slot past the chosen index"
        );
        assert!(
            replica.folded >= floor,
            "a boot fold starts at the sealed floor"
        );
        assert!(
            replica.folded <= replica.first_unchosen(),
            "a boot fold never passes the chosen prefix"
        );
        assert!(
            replica.committed.is_empty(),
            "a boot rebuild surfaces nothing"
        );
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
            self.floor <= self.folded,
            "the journal fold lies at or above the floor"
        );
        assert!(
            self.folded <= self.first_unchosen(),
            "the journal fold lies at or below the first unchosen slot"
        );
        if self.folded < self.first_unchosen() {
            assert!(
                !self.chosen.contains_key(&self.folded),
                "the journal fold stops only at a slot it does not hold"
            );
        }
        assert!(
            self.outcomes.keys().next().is_none_or(|s| *s >= floor),
            "no folded outcome survives below the floor"
        );
        assert!(
            self.history.keys().next().is_none_or(|s| *s >= floor),
            "no folded state survives below the floor"
        );
        assert!(
            self.outcomes
                .keys()
                .next_back()
                .is_none_or(|s| *s < self.folded),
            "every folded outcome lies below the fold's head"
        );
        assert!(
            self.history
                .keys()
                .next_back()
                .is_none_or(|s| *s < self.folded),
            "every folded state lies below the fold's head"
        );
        self.base.assert_invariants();
        self.state.assert_invariants();
        assert!(
            self.base.next_seq <= self.state.next_seq,
            "the fold's head never lies behind its base: next_seq"
        );
        assert!(
            self.base.first_seq <= self.state.first_seq,
            "the fold's head never lies behind its base: first_seq"
        );
        assert!(
            self.base.term <= self.state.term,
            "the fold's head never lies behind its base: term"
        );
        // The positions index names only retained, folded, accepted writes.
        assert!(
            self.positions
                .values()
                .all(|s| *s >= self.floor && *s < self.folded),
            "the positions index lies between the floor and the fold"
        );
        assert!(
            self.positions
                .keys()
                .next_back()
                .is_none_or(|seq| *seq < self.state.next_seq),
            "every indexed write starts below the journal's next position"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// The durable chosen index (the commit index).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn chosen_index(&self) -> Option<Slot> {
        // The chosen index covers everything the fold applied, and the floor
        // never outruns it.
        assert!(
            self.folded <= self.first_unchosen(),
            "the fold lies inside the chosen prefix"
        );
        assert!(
            self.floor <= self.first_unchosen(),
            "the floor lies inside the chosen prefix"
        );
        self.chosen_index
    }

    /// Whether the journal fold covers `index` — the replica's one answer to
    /// a quorum read (#143, Compartmentalized Paxos §3.4): a read at
    /// watermark `index` may be served once the replica has chosen *and
    /// folded* everything up to it. `None` is the empty watermark, covered by
    /// any prefix. The replica consumes "applied past `index`" and nothing
    /// else — it never sees the watermark tally that produced the index.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn covers(&self, index: Option<Slot>) -> bool {
        let covered = index.is_none_or(|i| i < self.folded);
        // A covered index is inside the chosen prefix: folded is chosen.
        if let Some(index) = index.filter(|_| covered) {
            assert!(
                self.chosen_index.is_some_and(|ci| index <= ci),
                "a covered index lies inside the chosen prefix"
            );
        }
        covered
    }

    /// First slot not in the contiguous chosen prefix.
    ///
    /// # Panics
    ///
    /// If the chosen index sits at the end of the slot space.
    #[must_use]
    pub fn first_unchosen(&self) -> Slot {
        match self.chosen_index {
            Some(s) => {
                assert!(
                    s.0 < u64::MAX,
                    "the chosen index never reaches the end of the slots"
                );
                Slot(s.0 + 1)
            }
            None => Slot(0),
        }
    }

    /// Every slot known chosen, contiguous or not.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn chosen(&self) -> &BTreeMap<Slot, Command> {
        assert!(
            self.chosen.keys().next().is_none_or(|s| *s >= self.floor),
            "no chosen value survives below the floor"
        );
        &self.chosen
    }

    /// Whether `slot` is known chosen here.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn is_chosen(&self, slot: Slot) -> bool {
        let chosen = self.chosen.contains_key(&slot);
        if chosen {
            assert!(slot >= self.floor, "a held chosen slot is retained");
        }
        chosen
    }

    /// The value chosen at `slot`, if known.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn chosen_at(&self, slot: Slot) -> Option<&Command> {
        let command = self.chosen.get(&slot);
        if command.is_some() {
            assert!(slot >= self.floor, "a held chosen slot is retained");
        }
        command
    }

    /// The journal state at the fold's head.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn journal(&self) -> JournalState {
        assert!(
            self.state.next_seq >= self.base.next_seq,
            "the fold's head is past its base"
        );
        assert!(
            self.state.first_seq <= self.state.next_seq,
            "the head's positions are ordered"
        );
        self.state
    }

    /// The journal state at the retention floor (sealed with the last
    /// truncation, or carried by the last trim-point jump).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn journal_base(&self) -> JournalState {
        assert!(
            self.base.first_seq <= self.state.first_seq,
            "the base lies behind the head"
        );
        self.base
    }

    /// The first slot the fold has not applied.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn folded(&self) -> Slot {
        assert!(self.folded >= self.floor, "the fold starts at the floor");
        assert!(
            self.folded <= self.first_unchosen(),
            "the fold lies inside the chosen prefix"
        );
        self.folded
    }

    /// The slot the fold is stopped at, when it is stopped below the first
    /// unchosen slot: a slot of the prefix whose value this node does not
    /// hold, that a catch-up must bring back before anything past it applies.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn fold_hole(&self) -> Option<Slot> {
        let hole = (self.folded < self.first_unchosen()).then_some(self.folded);
        if let Some(hole) = hole {
            // The fold stops only at a slot of the prefix it does not hold.
            assert!(
                !self.chosen.contains_key(&hole),
                "a fold hole is a slot not held"
            );
            assert!(hole >= self.floor, "a fold hole lies at or above the floor");
            assert!(
                hole < self.first_unchosen(),
                "a fold hole lies below the first unchosen slot"
            );
        }
        hole
    }

    /// The outcome of the folded slot `slot`, if it is retained and was not
    /// a `Noop`.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn outcome_at(&self, slot: Slot) -> Option<&Outcome> {
        let outcome = self.outcomes.get(&slot);
        if outcome.is_some() {
            assert!(slot < self.folded, "only a folded slot has an outcome");
            assert!(slot >= self.floor, "only a retained slot has an outcome");
        }
        outcome
    }

    /// Newly applied entries this batch, in order, each with the verdict
    /// the journal state machine gave it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command, Outcome)] {
        assert!(
            self.committed.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "a batch's applied entries ascend by slot"
        );
        assert!(
            self.committed
                .last()
                .is_none_or(|(slot, _, _)| *slot < self.first_unchosen()),
            "an applied entry lies inside the chosen prefix"
        );
        &self.committed
    }

    /// Drop the batch's applied entries (the caller consumed them).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn clear_committed(&mut self) {
        self.committed.clear();
        assert!(
            self.committed.is_empty(),
            "a consumed batch leaves nothing applied"
        );
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn chosen_gap(&self) -> Option<(Slot, Slot)> {
        let hole = self.first_unchosen();
        let highest = *self.chosen.range(hole..).next_back()?.0;
        assert!(
            highest >= hole,
            "a gap's highest chosen slot lies at or past its hole"
        );
        Some((hole, highest))
    }

    /// The write accepted with its first record at `seq`, if it is retained
    /// — what the journal state machine compares a retry against.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn accepted_at(&self, seq: Seq) -> Option<&Entry> {
        let entry = self
            .positions
            .get(&seq)
            .and_then(|slot| self.chosen.get(slot))
            .and_then(Command::write);
        // Read-back of the index `fold_one` writes: a single-writer write
        // indexed at a position names it (a multi-writer write names none,
        // the journal assigns it).
        if let (Some(entry), WriterMode::Single) = (entry, self.mode) {
            assert!(entry.seq == seq, "an indexed write starts at its position");
        }
        entry
    }

    /// The journal state after every slot below `slot` (a retained slot at
    /// or past the floor, at or below the fold).
    fn state_at(&self, slot: Slot) -> JournalState {
        assert!(
            slot >= self.floor,
            "the journal state is asked at a retained slot"
        );
        assert!(
            slot <= self.folded,
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
                    // Only a single-writer retry reads a retained record: a
                    // multi-writer write is never compared with one.
                    if self.mode == WriterMode::Multi {
                        return None;
                    }
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
            first >= self.floor,
            "a compaction floor lies at or above the floor"
        );
        assert!(
            first <= self.folded,
            "a compaction floor lies at or below the fold"
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
            *start <= position,
            "a retained record lies at or past its write's start"
        );
        assert!(
            position.0 < start.0 + entry.count(),
            "a retained record lies inside the write indexed below it"
        );
        assert!(*slot < self.folded, "a record's write is a folded slot");
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
    /// A `from` at or past `first_seq` but below the base's `next_seq` is
    /// [`LogRead::NotHeld`]: every record the base counts sits in a slot
    /// below the floor, dropped here. A floor this replica's own truncation
    /// set keeps the slot holding `first_seq`, so this meets only a floor
    /// that rose ahead of the fold: a trim-point jump adopts the peer's base
    /// at its floor while the decided `Truncate` that let the peer's floor
    /// rise sits in a slot above it this fold has not reached yet (or a
    /// reboot's refold stopped at a hole below that slot). Serving such a
    /// read used to panic looking for the record in the positions index;
    /// red→green: main-hunt seed 2382785388621081873 (a node jumped to trim
    /// point 5 with base `first_seq` 0, `next_seq` 1, missed the truncation
    /// at slot 19, and a quorum read from 0 confirmed at its fold head).
    ///
    /// # Panics
    ///
    /// If the positions index does not cover a position at or past the
    /// base's `next_seq` and below `next_seq` (a programmer error).
    #[must_use]
    pub fn read(&self, from: Seq, limit: usize, max_bytes: usize) -> LogRead {
        let state = self.state.view();
        if from < state.first_seq {
            return LogRead::Truncated(state);
        }
        if from < self.base.next_seq {
            // The record exists in the journal, just not here.
            assert!(
                from < state.next_seq,
                "a position below the base's end lies below the journal's end"
            );
            return LogRead::NotHeld;
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
        // Postconditions: the page never passes the fold's head, and honours
        // its bounds — the byte budget may be exceeded only by a lone record.
        if from < state.next_seq {
            assert!(
                page.next() <= state.next_seq,
                "a read page ends at or below the journal's next position"
            );
        } else {
            assert!(
                page.records.is_empty(),
                "a read at the end returns no record"
            );
        }
        assert!(
            page.records.len() <= limit,
            "a read page honours its record limit"
        );
        if page.records.len() > 1 {
            assert!(bytes <= max_bytes, "a read page honours its byte budget");
        }
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
        let folded = self.folded;
        self.chosen.insert(slot, command.clone());
        if slot == self.folded && slot < self.first_unchosen() {
            self.refold();
        }
        assert!(
            self.chosen.contains_key(&slot),
            "a learned slot is held chosen"
        );
        assert!(self.folded >= folded, "learning never moves the fold back");
    }

    /// Fold one slot at the fold's head, and return its verdict.
    fn fold_one(&mut self, slot: Slot, command: &Command) -> Outcome {
        assert!(
            slot == self.folded,
            "the journal fold advances one slot at a time"
        );
        let before = self.state;
        let mut state = self.state;
        let outcome = state.apply(self.mode, command, |seq| self.accepted_at(seq));
        self.state = state;
        if let (Outcome::Accepted { seq, .. }, Command::Write(_)) = (&outcome, command) {
            self.positions.insert(*seq, slot);
        }
        if matches!(outcome, Outcome::Trimmed(_)) {
            self.truncate_due = true;
        }
        if self.state != before {
            self.history.insert(slot, self.state);
        }
        if outcome != Outcome::Noop {
            self.outcomes.insert(slot, outcome.clone());
        }
        self.folded = Slot(slot.0 + 1);
        // The write side of the positions pair `accepted_at` reads back.
        if let Outcome::Accepted { seq, .. } = &outcome {
            assert!(
                self.positions.get(seq) == Some(&slot),
                "an accepted write is indexed"
            );
        }
        assert!(self.folded > slot, "the fold moves past the slot it folded");
        // The write side of the outcomes pair `outcome_at` reads back: a
        // waiter's answer and a rebooted node's replay both read it there.
        assert!(
            self.outcome_at(slot) == (outcome != Outcome::Noop).then_some(&outcome),
            "a folded verdict reads back as its slot's outcome"
        );
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
        assert!(
            self.folded <= end,
            "a refold never passes the chosen prefix"
        );
        if self.folded < end {
            assert!(
                !self.chosen.contains_key(&self.folded),
                "a refold stops only at a hole"
            );
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
        let chosen_before = self.chosen_index;
        let writes_before = writes.len();
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
        // The chosen index only advances, and each advance is one durable
        // write: the walk is its only writer.
        assert!(
            self.chosen_index >= chosen_before,
            "the chosen index never decreases"
        );
        assert!(
            writes.len() - writes_before <= APPLY_BATCH,
            "one walk writes one bounded batch"
        );
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
        // Checked apart from `compaction_target` (#269): every record the
        // dropped prefix accepted lies below the journal's first position.
        assert!(
            base.next_seq <= self.state.first_seq,
            "a truncation drops no record at or past first_seq"
        );
        let chosen_index = self.chosen_index;
        self.drop_prefix(first, base);
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        assert!(
            self.floor == first,
            "a truncation lands the floor on its slot"
        );
        assert!(
            self.chosen_index == chosen_index,
            "a truncation never moves the chosen index"
        );
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
        self.drop_prefix(point, base);
        self.refold();
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        // Everything below a trim point is chosen and folded here now.
        assert!(
            self.chosen_index.is_some_and(|ci| ci >= boundary),
            "a trim jump covers the boundary with the chosen index"
        );
        assert!(self.folded >= point, "a trim jump folds past its point");
        assert!(
            self.floor == point,
            "a trim jump lands the floor on its point"
        );
        base
    }

    /// The prefix drop [`Replica::truncate`] and [`Replica::trim_to`] share:
    /// `first` becomes the floor with `base` the journal state there, and
    /// every chosen value, history entry, outcome and position below it
    /// goes.
    fn drop_prefix(&mut self, first: Slot, base: JournalState) {
        assert!(
            first >= self.floor,
            "the retention floor never moves backward"
        );
        assert!(first <= self.folded, "a prefix drop never passes the fold");
        base.assert_invariants();
        self.base = base;
        self.floor = first;
        self.chosen = self.chosen.split_off(&first);
        self.history = self.history.split_off(&first);
        self.outcomes = self.outcomes.split_off(&first);
        self.positions.retain(|_, slot| *slot >= first);
        assert!(
            self.chosen.keys().next().is_none_or(|s| *s >= first),
            "no chosen value survives below the floor"
        );
        assert!(
            self.positions.values().all(|s| *s >= first),
            "no position survives below the floor"
        );
        assert!(
            self.outcomes.keys().next().is_none_or(|s| *s >= first),
            "no outcome survives below the floor"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{LogRead, Replica};
    use crate::journal_state::{JournalState, Outcome, WriterMode};
    use crate::types::{Ballot, Command, Control, Entry, LeaderUuid, NodeId, Seq, Slot, Value};

    fn write(seq: u64, records: &[&[u8]]) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(seq),
            records: records.iter().map(|r| Value(r.to_vec())).collect(),
        })
    }

    fn claim() -> Command {
        Command::Control(Control::SetLeader {
            new: LeaderUuid(1),
            old: None,
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
        Replica::from_boot(
            WriterMode::Single,
            ci,
            Slot(0),
            JournalState::default(),
            &records(commands),
        )
    }

    fn page(read: LogRead) -> super::LogPage {
        match read {
            LogRead::Page(page) => page,
            LogRead::Truncated(_) | LogRead::NotHeld => panic!("expected a page"),
        }
    }

    /// A trim-point jump whose base still counts a record below the floor
    /// (the truncation that let the peer's floor rise lies above it, unfolded
    /// here) serves a read there as not held — never a page, never
    /// `Truncated` — and serves every position at or past the floor.
    #[test]
    fn a_jumped_fold_behind_its_truncation_does_not_hold_the_base_records() {
        let commands = [
            claim(),
            write(0, &[b"a"]),
            write(1, &[b"b"]),
            write(2, &[b"c"]),
            Command::Control(Control::Truncate {
                leader: LeaderUuid(1),
                up_to: Seq(1),
            }),
        ];
        let mut r = replica(&commands[..1]);
        let base = JournalState {
            leader: Some(LeaderUuid(1)),
            term: 1,
            next_seq: Seq(1),
            first_seq: Seq(0),
        };
        assert_eq!(r.trim_to(Slot(2), base), base);
        r.learn(Slot(2), &commands[2]);
        r.advance(|_, _| true, &mut Vec::new());
        assert_eq!(r.journal().first_seq, Seq(0));
        assert_eq!(r.read(Seq(0), 8, 64), LogRead::NotHeld);
        assert_eq!(bytes(&page(r.read(Seq(1), 8, 64))), vec![b"b".to_vec()]);
        // Once the fold passes the truncation, the gap is `Truncated`.
        r.learn(Slot(3), &commands[3]);
        r.learn(Slot(4), &commands[4]);
        r.advance(|_, _| true, &mut Vec::new());
        assert_eq!(r.journal().first_seq, Seq(1));
        assert!(matches!(r.read(Seq(0), 8, 64), LogRead::Truncated(_)));
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
            Command::Control(Control::Truncate {
                leader: LeaderUuid(1),
                up_to: Seq(1),
            }),
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
        let rebooted = Replica::from_boot(WriterMode::Single, Some(Slot(4)), first, sealed, &recs);
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
            leader: Some(LeaderUuid(1)),
            term: 1,
            next_seq: Seq(2),
            first_seq: Seq(0),
        };
        let commands = [
            claim(),
            write(0, &[b"a"]),
            write(2, &[b"c"]),
            Command::Control(Control::Truncate {
                leader: LeaderUuid(1),
                up_to: Seq(1),
            }),
        ];
        let mut recs = records(&commands);
        recs.retain(|slot, _| *slot >= Slot(2));
        let r = Replica::from_boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
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
            Command::Control(Control::Truncate {
                leader: LeaderUuid(1),
                up_to: Seq(1),
            }),
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
            Command::Control(Control::Truncate {
                leader: LeaderUuid(1),
                up_to: Seq(2),
            }),
        ]);
        // Slot 2 holds position 2: everything below it may go.
        assert_eq!(r.compaction_target(), Some(Slot(1)));
        let base = r.truncate(Slot(2));
        assert_eq!(base.next_seq, Seq(1));
        assert_eq!(base.term, 1);
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
        let rebooted = Replica::from_boot(WriterMode::Single, Some(Slot(3)), Slot(2), base, &recs);
        assert_eq!(rebooted.journal(), r.journal());
    }

    #[test]
    fn a_hole_below_the_prefix_stops_the_fold_until_it_heals() {
        let mut recs = records(&[claim(), write(0, &[b"a"]), write(1, &[b"b"])]);
        let lost = recs.remove(&Slot(1)).expect("slot 1").1;
        let mut r = Replica::from_boot(
            WriterMode::Single,
            Some(Slot(2)),
            Slot(0),
            JournalState::default(),
            &recs,
        );
        assert_eq!(r.fold_hole(), Some(Slot(1)));
        assert!(!r.covers(Some(Slot(1))));
        r.learn(Slot(1), &lost);
        assert_eq!(r.fold_hole(), None);
        assert_eq!(r.journal().next_seq, Seq(2));
    }
}
