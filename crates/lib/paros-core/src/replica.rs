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
//!   ([`crate::journal::state`]) — the state at the retention floor (the
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

use crate::contract::write::WriteOp;
use crate::journal::state::{JournalState, JournalView, Outcome, WriterMode};
use crate::journal::{Command, Entry, Seq};
use crate::types::{Ballot, Slot, Value};

pub use crate::contract::limits::APPLY_BATCH;

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
    /// The last walk handed a folded `Truncate` to the caller: the caller's
    /// compaction is judged by [`Replica::assert_compacted`] (#342).
    compaction_due: bool,
    /// Newly applied `(slot, ballot, command, verdict)` entries, in order,
    /// for the caller's `Ready` batch. The ballot is the one the caller's
    /// record of the slot held when the walk applied it. The ballot and the
    /// verdict ride with the slot because a `Truncate` folded later in the
    /// same walk may compact the slot — its record and its
    /// [`Replica::outcome_at`] — before the caller reports it.
    committed: Vec<(Slot, Ballot, Command, Outcome)>,
    /// The journal's writer mode (#241): configuration, fixed at creation,
    /// handed to every [`JournalState::apply`].
    mode: WriterMode,
    /// The most slots one walk releases (#338): a node tunable in
    /// `1..=APPLY_BATCH`, the constant by default
    /// ([`Replica::set_apply_page`]).
    apply_page: usize,
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
        records: &BTreeMap<Slot, (Ballot, Command)>,
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
            compaction_due: false,
            committed: Vec::new(),
            mode,
            apply_page: APPLY_BATCH,
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
        // The apply page stays inside its tunable range (#338).
        assert!(self.apply_page > 0, "an apply page is never empty");
        assert!(
            self.apply_page <= APPLY_BATCH,
            "an apply page never exceeds its ceiling"
        );
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
        // It is monotone (asserted at each insert in `fold_one`): a later
        // position is folded at a later slot. So its first and last slots
        // bound every other one, and the check stays O(1) however long the
        // log grows (#619 (O(n) invariants)).
        assert!(
            self.positions
                .first_key_value()
                .is_none_or(|(_, s)| *s >= self.floor),
            "the positions index lies between the floor and the fold"
        );
        assert!(
            self.positions
                .last_key_value()
                .is_none_or(|(_, s)| *s < self.folded),
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

    /// Newly applied entries this batch, in order, each with the ballot of
    /// its record when the walk applied it and the verdict the journal
    /// state machine gave it.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Ballot, Command, Outcome)] {
        assert!(
            self.committed.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "a batch's applied entries ascend by slot"
        );
        assert!(
            self.committed
                .last()
                .is_none_or(|(slot, _, _, _)| *slot < self.first_unchosen()),
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
            probe!(
                reachable,
                "compaction: a retained retry holds the floor down"
            );
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

    /// Set the apply page size (#338): the most slots one walk
    /// ([`Replica::advance`]) releases before it leaves a deferred
    /// continuation. A per-node size, never on the wire. A small page makes
    /// a burst of decisions span several walks.
    ///
    /// # Panics
    ///
    /// If `page` is zero or above [`APPLY_BATCH`]: a programmer error, never
    /// an operating condition.
    pub fn set_apply_page(&mut self, page: usize) {
        assert!(page > 0, "an apply page releases at least one slot");
        assert!(
            page <= APPLY_BATCH,
            "an apply page never exceeds its ceiling"
        );
        self.apply_page = page;
        assert!(self.apply_page == page, "the apply page is in force");
    }

    /// The most slots one walk releases (#338), in `1..=APPLY_BATCH`.
    #[must_use]
    pub fn apply_page(&self) -> usize {
        self.apply_page
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
            // The index stays monotone: the head's write starts past every
            // indexed one, at a slot past every indexed one. The O(1) bounds
            // in `assert_invariants` rest on it.
            assert!(
                self.positions.last_key_value().is_none_or(|(s, _)| s < seq),
                "the positions index grows at its last position"
            );
            assert!(
                self.positions
                    .last_key_value()
                    .is_none_or(|(_, at)| *at < slot),
                "the positions index grows at its last slot"
            );
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
    /// `record_ballot(slot, command)` answers "at which ballot does the
    /// authoritative record for `slot` hold exactly this command?" (`None`
    /// when it holds another one or none). It is the coupling assertion, and
    /// the only thing the walk needs to know about the acceptor. The walk
    /// hands the ballot on with the slot ([`Replica::committed`]). A
    /// function, not the acceptor's map: the replica consumes "slot chosen,
    /// value" and must not acquire the accepted log merely because one
    /// deployment colocates the two roles.
    ///
    /// # Panics
    ///
    /// If the walk's contiguity or the chosen/accepted coupling is broken.
    pub fn advance(
        &mut self,
        record_ballot: impl Fn(Slot, &Command) -> Option<Ballot>,
        writes: &mut Vec<WriteOp>,
    ) -> Option<Slot> {
        let chosen_before = self.chosen_index;
        let writes_before = writes.len();
        self.refold();
        if self.folded == self.first_unchosen() {
            let mut next = self.first_unchosen();
            let mut advanced = 0_usize;
            while advanced < self.apply_page
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
                let Some(ballot) = record_ballot(next, &command) else {
                    panic!("an applied slot's accepted record carries the applied command");
                };
                self.chosen_index = Some(next);
                writes.push(WriteOp::SetChosenIndex(next));
                let outcome = self.fold_one(next, &command);
                self.committed.push((next, ballot, command, outcome));
                next = Slot(next.0 + 1);
                advanced += 1;
            }
            // A small apply page (#338) makes a walk yield mid-burst.
            probe!(
                sometimes,
                advanced == self.apply_page && self.chosen.contains_key(&next),
                "apply: a walk yields with the next slot already chosen"
            );
            // Postcondition: either the walk consumed the entire contiguous
            // chosen prefix, or exactly one bounded chunk was released.
            assert!(
                advanced == self.apply_page || !self.chosen.contains_key(&self.first_unchosen()),
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
            writes.len() - writes_before <= self.apply_page,
            "one walk writes one bounded batch"
        );
        self.compaction_due = std::mem::take(&mut self.truncate_due);
        if self.compaction_due {
            self.compaction_target()
        } else {
            None
        }
    }

    /// Judge the compaction the caller ran after a walk that folded a
    /// `Truncate` (#342): call it after every [`Replica::advance`], once the
    /// caller compacted to the target it returned. A walk that folded no
    /// `Truncate` is not judged.
    ///
    /// Checked apart from [`Replica::compaction_target`], so a wrong target
    /// fails here and not only on a later refold:
    ///
    /// - **Safety.** Every retained single-writer retry still holds the
    ///   record it was judged against. A floor that drops it leaves a refold
    ///   (a reboot, a trim-point jump) nothing to compare with, the #204
    ///   shape.
    /// - **Progress.** The floor slot is needed: it is the fold's head, or
    ///   it holds the journal's first record, or it holds a record a
    ///   retained retry read. A floor that stays below the slots a decided
    ///   truncation released never compacts (the mutants
    ///   `compaction_target -> None`, `-> Some(Default)` and
    ///   `slot_holding -> Default` of the first mutation run, #269).
    ///
    /// # Panics
    ///
    /// If the compaction dropped a record a retained retry reads, or left an
    /// unneeded slot at the floor.
    pub fn assert_compacted(&mut self) {
        if !std::mem::take(&mut self.compaction_due) {
            return;
        }
        assert!(
            self.floor <= self.folded,
            "a judged compaction lies at or below the fold"
        );
        // The records the floor slot holds: those its own accepted write
        // holds, and every record below the floor's base (dropped with the
        // prefix, or never held on a node that jumped to a trim point).
        let held = match self.outcomes.get(&self.floor) {
            Some(Outcome::Accepted { seq, count }) => seq.0..seq.0 + count,
            _ => 0..0,
        };
        let at_floor = |position: Seq| position < self.base.next_seq || held.contains(&position.0);
        let mut pinned = self.floor == self.folded
            || (self.state.first_seq < self.state.next_seq && at_floor(self.state.first_seq));
        if self.mode == WriterMode::Single {
            for (slot, command) in self.chosen.range(self.floor..self.folded) {
                let Some(entry) = command.write() else {
                    continue;
                };
                let at = self.state_at(*slot);
                if entry.seq < at.first_seq || entry.seq >= at.next_seq {
                    continue;
                }
                assert!(
                    entry.seq >= self.base.next_seq,
                    "a compaction keeps every record a retained retry reads"
                );
                pinned |= at_floor(entry.seq);
            }
        }
        assert!(pinned, "a compaction leaves a needed slot at the floor");
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
mod tests;
