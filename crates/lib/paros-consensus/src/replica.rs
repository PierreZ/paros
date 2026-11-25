//! The **replica**: the chosen log, its application order, and the journal
//! state machine folded over it — and nothing else.
//!
//! A replica consumes one kind of fact — *slot `s` chose value `v`* — and
//! turns it into the contiguous applied prefix, never caring *why* a value
//! was chosen (an accept quorum this node counted, a `Commit` off the wire, a
//! catch-up replay, a handoff's decided tail). It owns:
//!
//! - the **chosen set** (the slots it knows chosen) and the durable
//!   **chosen index** (the commit index: every slot at or below it is chosen
//!   and applied in order);
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
//! **One copy of the retained log.** The replica holds no command. It
//! reads the command chosen at a slot through a port, [`ChosenLog`], that
//! the deployment hands it on each call: on a node the acceptor's records
//! (every record of a slot known chosen carries the chosen command, the P2c
//! chain), on a replica node the records it learned. A role must not read
//! another role, so the replica sees the port, never the acceptor. Before
//! this, a node held each retained command two times (#1101 (one copy of the
//! retained log)).
//!
//! It knows nothing about ballots, promises, leadership, quorums or the
//! network. Durable changes are emitted as [`WriteOp`]s into the caller's
//! batch.
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*).

use std::collections::{BTreeMap, BTreeSet};

use crate::contract::write::WriteOp;
use crate::journal::segment::{Segment, SegmentId};
use crate::journal::state::{JournalState, JournalView, Outcome, WriterMode};
use crate::journal::{Command, Entry, Seq};
use crate::types::{Ballot, Slot, Value};

use crate::contract::limits::APPLY_BATCH;

/// The port through which the [`Replica`] reads the command of a slot it
/// knows chosen (see the module doc): the store of records the deployment
/// keeps. A role must not read another role, so the replica sees this
/// trait, never the acceptor.
pub trait ChosenLog {
    /// The record this store holds at `slot`: the ballot it was accepted or
    /// learned at, and its command. At a slot the replica knows chosen, the
    /// command is the chosen one.
    fn record(&self, slot: Slot) -> Option<(Ballot, &Command)>;
}

impl ChosenLog for BTreeMap<Slot, (Ballot, Command)> {
    fn record(&self, slot: Slot) -> Option<(Ballot, &Command)> {
        self.get(&slot).map(|(ballot, command)| (*ballot, command))
    }
}

/// The command `log` holds at `slot`, a slot the replica knows chosen.
///
/// # Panics
///
/// If `log` holds no record there: the deployment broke the coupling of a
/// chosen slot and its record.
fn chosen_command<L: ChosenLog + ?Sized>(log: &L, slot: Slot) -> (Ballot, &Command) {
    log.record(slot)
        .expect("a slot known chosen holds its record in the chosen log")
}

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
    /// The read starts at or past the end of the segment this replica keeps
    /// (R6 of `docs/architecture/04-7-segments.md`): its walk applied the
    /// `Roll` that ends the segment, and every record of the segment lies
    /// below `end`. The reader asks a replica of `next` from the chain.
    /// Only a replica bound to a segment ([`Replica::bind_segment`])
    /// answers this.
    Sealed {
        /// The end of the segment: the journal's next position after the
        /// `Roll` (a `Roll` takes no position).
        end: Seq,
        /// The segment the `Roll` opened.
        next: Segment,
    },
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
    /// Every slot this node knows chosen, contiguous or not. The commands
    /// are in the deployment's [`ChosenLog`].
    chosen: BTreeSet<Slot>,
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
    /// The segment this replica keeps (R6), or `None` for a replica that
    /// keeps the whole log (a node's). A bound fold stops after the `Roll`
    /// that ends the segment ([`Replica::is_sealed`]).
    bound: Option<SegmentId>,
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
        // The pair of the seal (`WriteOp::Truncate`, `WriteOp::TrimmedTo`):
        // the sealed tail segment opened at a `Roll` below the floor, or is
        // the first segment.
        assert!(
            sealed.segment.start <= floor,
            "the sealed tail segment opens at or below the sealed floor"
        );
        let chosen: BTreeSet<Slot> = records
            .keys()
            .filter(|slot| chosen_index.is_some_and(|ci| **slot <= ci))
            .copied()
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
            bound: None,
        };
        replica.refold(records);
        replica.truncate_due = false;
        // The boot read-back of the walk's durable writes: everything the
        // chosen index covers is chosen, and the fold lands between the
        // sealed floor and the first unchosen slot.
        assert!(
            replica.chosen.last().copied() <= chosen_index,
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
        if self.chosen.contains(&self.first_unchosen()) {
            assert!(
                self.advance_pending,
                "a chosen first-unchosen slot has a deferred prefix continuation"
            );
        }
        if self.advance_pending {
            assert!(
                self.chosen.contains(&self.first_unchosen()),
                "a deferred prefix continuation names a chosen first-unchosen slot"
            );
        }
        assert!(
            self.chosen.first().is_none_or(|s| *s >= floor),
            "no chosen record survives below the compaction floor"
        );
        self.assert_segment_invariants();
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
                !self.chosen.contains(&self.folded),
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

    /// The invariants of the segment a bound replica keeps (R6): its base
    /// lies in it, its fold passes at most the `Roll` that ends it, and a
    /// sealed replica holds nothing past the end.
    fn assert_segment_invariants(&self) {
        // A bound replica keeps the segment of its base, and folds at most
        // the `Roll` that ends it.
        if let Some(kept) = self.bound {
            assert!(
                self.base.segment.id == kept,
                "a bound replica's base lies in the segment it keeps"
            );
            assert!(
                self.state.segment.id == kept || self.state.segment.id == kept.successor(),
                "a bound fold never passes the end of its segment"
            );
        }
        if self.is_sealed() {
            assert!(
                self.chosen.last().is_none_or(|s| *s < self.folded),
                "a sealed replica holds no chosen slot past its end"
            );
            assert!(
                self.folded == self.first_unchosen(),
                "a sealed fold ends at the first unchosen slot"
            );
        }
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
    pub fn chosen(&self) -> &BTreeSet<Slot> {
        assert!(
            self.chosen.first().is_none_or(|s| *s >= self.floor),
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
        let chosen = self.chosen.contains(&slot);
        if chosen {
            assert!(slot >= self.floor, "a held chosen slot is retained");
        }
        chosen
    }

    /// The value chosen at `slot`, if known, read from the deployment's
    /// `log`.
    ///
    /// # Panics
    ///
    /// If `log` holds no record at a slot known chosen, or another assertion
    /// on its own invariants fails: a programmer error, never an operating
    /// condition.
    #[must_use]
    pub fn chosen_at<'a, L: ChosenLog + ?Sized>(
        &self,
        log: &'a L,
        slot: Slot,
    ) -> Option<&'a Command> {
        if !self.chosen.contains(&slot) {
            return None;
        }
        assert!(slot >= self.floor, "a held chosen slot is retained");
        Some(chosen_command(log, slot).1)
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
                !self.chosen.contains(&hole),
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

    /// Each `Roll` this fold applied at slot `from` or later, in slot
    /// order: its slot, the first slot of the segment it sealed, and the
    /// tail segment it opened. Both are read from the fold itself, so they
    /// are functions of the chosen log whatever path folded the slot (the
    /// walk, the refold past a healed hole, the fold at boot or after a
    /// trim-point jump).
    ///
    /// # Panics
    ///
    /// If a folded `Roll` does not end its segment at the slot after it, or
    /// the segment it sealed does not start below it.
    #[must_use]
    pub fn rolls_from(&self, from: Slot) -> Vec<(Slot, Slot, Segment)> {
        let rolls: Vec<(Slot, Slot, Segment)> = self
            .outcomes
            .range(from..)
            .filter_map(|(slot, outcome)| match outcome {
                Outcome::Rolled(tail) => Some((*slot, self.state_at(*slot).segment.start, *tail)),
                _ => None,
            })
            .collect();
        for (slot, start, tail) in &rolls {
            assert!(
                tail.start.0 == slot.0 + 1,
                "R2: a Roll ends its segment at the slot after it"
            );
            assert!(*start <= *slot, "a Roll lies inside the segment it seals");
        }
        rolls
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
        let highest = *self.chosen.range(hole..).next_back()?;
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
    pub fn accepted_at<'a, L: ChosenLog + ?Sized>(
        &self,
        log: &'a L,
        seq: Seq,
    ) -> Option<&'a Entry> {
        let entry = self
            .positions
            .get(&seq)
            .and_then(|slot| self.chosen_at(log, *slot))
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
    pub fn compaction_target<L: ChosenLog + ?Sized>(&self, log: &L) -> Option<Slot> {
        let mut first = self.slot_holding(log, self.state.first_seq);
        loop {
            let needed = self
                .chosen
                .range(first..self.folded)
                .filter_map(|slot| {
                    // Only a single-writer retry reads a retained record: a
                    // multi-writer write is never compared with one.
                    if self.mode == WriterMode::Multi {
                        return None;
                    }
                    // A write at or past the journal's position when it
                    // folded (accepted, or refused ahead) is held at its own
                    // slot or above (`slot_holding`), never below `first`:
                    // only a retry below that position can pull the floor.
                    let entry = chosen_command(log, *slot).1.write()?;
                    let at = self.state_at(*slot);
                    (entry.seq >= at.first_seq).then(|| self.slot_holding(log, entry.seq))
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
        // A sealed replica keeps the `Roll` that ends its segment (R6): its
        // base, the state at its floor, stays in the segment it keeps.
        if let Some(end) = self.sealed_end() {
            assert!(end.0 >= 1, "a sealed end lies past the `Roll`");
            first = first.min(Slot(end.0 - 1));
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
    fn slot_holding<L: ChosenLog + ?Sized>(&self, log: &L, position: Seq) -> Slot {
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
            .chosen_at(log, *slot)
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
    pub fn read<L: ChosenLog + ?Sized>(
        &self,
        log: &L,
        from: Seq,
        limit: usize,
        max_bytes: usize,
    ) -> LogRead {
        let state = self.state.view();
        if from < state.first_seq {
            return LogRead::Truncated(state);
        }
        if self.is_sealed() && from >= state.next_seq {
            return LogRead::Sealed {
                end: state.next_seq,
                next: self.state.segment,
            };
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
        // The record limit is checked before each record, inside the walk.
        'writes: while at < state.next_seq {
            let (start, slot) = self
                .positions
                .range(..=at)
                .next_back()
                .expect("every position below next_seq lies in a retained write");
            let entry = self
                .chosen_at(log, *slot)
                .and_then(Command::write)
                .expect("an indexed write is a retained chosen write");
            let offset = usize::try_from(at.0 - start.0).expect("a batch fits in memory");
            assert!(
                offset < entry.records.len(),
                "a position below next_seq lies inside the write indexed below it"
            );
            for record in &entry.records[offset..] {
                let size = record.len();
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
        // A read at or past the end returns no record: its page ends where
        // it starts.
        assert!(
            page.next() <= state.next_seq.max(from),
            "a read page ends at or below the journal's next position"
        );
        assert!(
            page.records.len() <= limit,
            "a read page honours its record limit"
        );
        assert!(
            page.records.len() <= 1 || bytes <= max_bytes,
            "a read page honours its byte budget"
        );
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
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn apply_page(&self) -> usize {
        assert!(self.apply_page >= 1, "an apply page releases a slot");
        assert!(
            self.apply_page <= APPLY_BATCH,
            "an apply page never exceeds its ceiling"
        );
        self.apply_page
    }

    // ---- the segment a replica keeps (R6) ------------------------------------

    /// Bind this replica to the segment of its base, the journal state at
    /// its floor (R6 of `docs/architecture/04-7-segments.md`): the fold
    /// stops after the `Roll` that ends that segment, and the slots past it
    /// are not this replica's. A `ReplicaNode` binds at boot and after it
    /// joins a segment; a node never binds.
    ///
    /// The fold may already be past the end (a boot whose durable prefix
    /// ends at the `Roll`): the replica is sealed at once.
    ///
    /// # Panics
    ///
    /// If the fold passed more than one `Roll` since its base: a bound fold
    /// never walks past the end of its segment.
    pub fn bind_segment(&mut self) {
        let kept = self.base.segment.id;
        assert!(
            self.state.segment.id == kept || self.state.segment.id == kept.successor(),
            "a bound fold lies in its segment or just past its end"
        );
        self.bound = Some(kept);
        self.drop_past_end();
        assert!(self.bound == Some(kept), "the replica is bound to its base");
        assert!(
            !self.advance_pending || !self.is_sealed(),
            "a sealed replica defers no walk"
        );
    }

    /// Keep the whole log again, before a fresh replica jumps to the start
    /// of the segment it joins (`ReplicaNode::join_segment`), which binds it
    /// to that segment next: the jump never sees a bound that its new base
    /// left.
    ///
    /// # Panics
    ///
    /// If the replica holds a chosen slot: only a fresh replica unbinds.
    pub(crate) fn unbind_segment(&mut self) {
        assert!(self.chosen.is_empty(), "only a fresh replica unbinds");
        assert!(
            self.folded == self.floor,
            "an unbound replica walked nothing"
        );
        self.bound = None;
        assert!(!self.is_sealed(), "an unbound replica is never sealed");
    }

    /// The segment this replica keeps, or `None` when it keeps the whole log.
    #[must_use]
    pub fn segment_bound(&self) -> Option<SegmentId> {
        self.bound
    }

    /// Whether the fold applied the `Roll` that ends the segment this
    /// replica keeps (R6): the walk stops there, and a read at or past the
    /// end is [`LogRead::Sealed`]. Never for an unbound replica.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.bound.is_some_and(|kept| self.state.segment.id != kept)
    }

    /// A sealed replica holds no chosen slot past the end of its segment:
    /// a slot learned out of order there is not its own, so it goes, and
    /// the walk defers nothing. Returns the first slot past the end, the
    /// slot its caller drops its records from.
    fn drop_past_end(&mut self) -> Option<Slot> {
        if !self.is_sealed() {
            return None;
        }
        // A sealed fold applied the `Roll`, so it reached the first unchosen
        // slot: the walk stops only once the prefix through the `Roll` is
        // folded.
        assert!(
            self.folded == self.first_unchosen(),
            "a sealed fold ends at the first unchosen slot"
        );
        let end = self.folded;
        let _past = self.chosen.split_off(&end);
        self.advance_pending = false;
        assert!(
            self.chosen.last().is_none_or(|s| *s < end),
            "a sealed replica holds no chosen slot past its end"
        );
        Some(end)
    }

    /// The first slot past the end of the segment this replica keeps, once
    /// sealed: the slot after the `Roll`.
    #[must_use]
    pub fn sealed_end(&self) -> Option<Slot> {
        self.is_sealed().then_some(self.folded)
    }

    // ---- learning -------------------------------------------------------------

    /// Learn `slot` chosen with `command`. The caller has already checked the
    /// slot is retained and not yet known chosen here, and holds the record
    /// in its `log`. A slot that fills the hole the fold is stopped at
    /// resumes the fold at once.
    ///
    /// # Panics
    ///
    /// If `slot` is already known chosen, or `log` does not hold `command`
    /// at `slot`, or the replica is sealed (its caller refuses a slot past
    /// the end of the segment it keeps).
    pub fn learn<L: ChosenLog + ?Sized>(&mut self, log: &L, slot: Slot, command: &Command) {
        assert!(
            !self.chosen.contains(&slot),
            "a slot is learned chosen once"
        );
        assert!(slot >= self.folded, "a folded slot is never relearned");
        assert!(
            !self.is_sealed(),
            "a sealed replica learns no slot past the end of its segment"
        );
        // The pair of the coupling `chosen_command` reads back: the caller
        // wrote the record before it hands the slot over.
        assert!(
            log.record(slot).is_some_and(|(_, held)| held == command),
            "a learned slot's record carries the learned command"
        );
        let folded = self.folded;
        self.chosen.insert(slot);
        // The fold resumes from its head through every chosen slot it now
        // holds: a no-op unless `slot` filled the head.
        self.refold(log);
        assert!(self.chosen.contains(&slot), "a learned slot is held chosen");
        assert!(self.folded >= folded, "learning never moves the fold back");
    }

    /// Fold one slot at the fold's head, and return its verdict.
    fn fold_one<L: ChosenLog + ?Sized>(
        &mut self,
        log: &L,
        slot: Slot,
        command: &Command,
    ) -> Outcome {
        assert!(
            slot == self.folded,
            "the journal fold advances one slot at a time"
        );
        let before = self.state;
        let mut state = self.state;
        let outcome = state.apply(self.mode, slot, command, |seq| self.accepted_at(log, seq));
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
    fn refold<L: ChosenLog + ?Sized>(&mut self, log: &L) {
        let end = self.first_unchosen();
        while self.folded < end && !self.is_sealed() {
            let slot = self.folded;
            if !self.chosen.contains(&slot) {
                break;
            }
            let command = chosen_command(log, slot).1;
            self.fold_one(log, slot, command);
        }
        // The loop stops at `end` or at a hole: `assert_invariants` checks
        // the hole.
        assert!(
            self.folded <= end,
            "a refold never passes the chosen prefix"
        );
    }

    /// Walk the contiguous chosen prefix forward, folding and surfacing each
    /// newly applied `(slot, command)` in order (no gaps), bounded per batch.
    /// Returns the compaction target ([`Replica::compaction_target`]) when a
    /// `Truncate` was folded, for the caller to compact *after* the walk.
    ///
    /// A fold stopped at a hole below the prefix **holds** the walk: nothing
    /// past the hole is judged until it heals.
    ///
    /// The walk reads each applied command and the ballot of its record from
    /// the deployment's `log`, and hands the ballot on with the slot
    /// ([`Replica::committed`]). A port, not the acceptor's map: the replica
    /// consumes "slot chosen, value" and must not acquire the accepted log
    /// merely because one deployment colocates the two roles.
    ///
    /// # Panics
    ///
    /// If the walk's contiguity or the chosen/accepted coupling is broken.
    pub fn advance<L: ChosenLog + ?Sized>(
        &mut self,
        log: &L,
        writes: &mut Vec<WriteOp>,
    ) -> Option<Slot> {
        let chosen_before = self.chosen_index;
        let writes_before = writes.len();
        self.refold(log);
        if self.folded == self.first_unchosen() {
            let mut next = self.first_unchosen();
            let mut advanced = 0_usize;
            while advanced < self.apply_page && !self.is_sealed() && self.chosen.contains(&next) {
                // The walk is the *only* writer of `chosen_index`, and it
                // advances exactly one slot per iteration — the contiguity the
                // apply seam and the boot rebuild are built on.
                assert!(
                    next == self.first_unchosen(),
                    "the chosen prefix advances one slot at a time"
                );
                // The chosen/accepted coupling, per applied slot: the log
                // holds the record of every slot known chosen.
                let (ballot, command) = chosen_command(log, next);
                self.chosen_index = Some(next);
                writes.push(WriteOp::SetChosenIndex(next));
                let outcome = self.fold_one(log, next, command);
                self.committed
                    .push((next, ballot, command.clone(), outcome));
                next = Slot(next.0 + 1);
                advanced += 1;
            }
            // A small apply page (#338) makes a walk yield mid-burst.
            probe!(
                sometimes,
                advanced == self.apply_page && self.chosen.contains(&next),
                "apply: a walk yields with the next slot already chosen"
            );
            // Postcondition: either the walk consumed the entire contiguous
            // chosen prefix, or exactly one bounded chunk was released.
            assert!(
                advanced == self.apply_page
                    || self.is_sealed()
                    || !self.chosen.contains(&self.first_unchosen()),
                "the walk consumes, bounds or seals the contiguous chosen prefix"
            );
        }
        self.advance_pending = self.chosen.contains(&self.first_unchosen());
        self.drop_past_end();
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
            self.compaction_target(log)
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
    ///   retained retry read, or it holds the `Roll` that ends a sealed
    ///   replica's segment. A floor that stays below the slots a decided
    ///   truncation released never compacts (the mutants
    ///   `compaction_target -> None`, `-> Some(Default)` and
    ///   `slot_holding -> Default` of the first mutation run, #269).
    ///
    /// # Panics
    ///
    /// If the compaction dropped a record a retained retry reads, or left an
    /// unneeded slot at the floor.
    pub fn assert_compacted<L: ChosenLog + ?Sized>(&mut self, log: &L) {
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
        // The journal's first record pins the floor slot that holds it. An
        // empty journal pins nothing this way: its first position is its
        // next one, past the base and past every record the folded floor
        // slot accepted.
        let mut pinned = self.floor == self.folded || at_floor(self.state.first_seq);
        // The `Roll` that ends a sealed replica's segment pins its slot: the
        // base stays in the segment the replica keeps.
        pinned |= self
            .sealed_end()
            .is_some_and(|end| self.floor.0 + 1 == end.0);
        if self.mode == WriterMode::Single {
            for slot in self.chosen.range(self.floor..self.folded) {
                let Some(entry) = chosen_command(log, *slot).1.write() else {
                    continue;
                };
                // A write at or past the journal's position when it folded
                // reads no earlier record, and its own record is retained.
                let at = self.state_at(*slot);
                if entry.seq < at.first_seq {
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
    pub fn truncate<L: ChosenLog + ?Sized>(&mut self, log: &L, first: Slot) -> JournalState {
        assert!(
            self.compaction_target(log)
                .is_some_and(|t| first.0 <= t.0 + 1),
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
        self.advance_pending = self.chosen.contains(&self.first_unchosen());
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
    pub fn trim_to<L: ChosenLog + ?Sized>(
        &mut self,
        log: &L,
        point: Slot,
        sealed: JournalState,
    ) -> JournalState {
        assert!(point.0 > 0, "a trim point has a chosen slot below it");
        let boundary = Slot(point.0 - 1);
        self.chosen_index = self.chosen_index.max(Some(boundary));
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
        self.refold(log);
        self.advance_pending = self.chosen.contains(&self.first_unchosen());
        self.drop_past_end();
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
        // The base comes from this node's own fold, or from a pooled peer's
        // `TrimmedTo`. The trust rule (decision 30): a correct peer sends the
        // state its own fold reached, which holds the journal's invariants,
        // and the transport proves the bytes. A broken state is a detected
        // corruption of a peer's fold.
        base.assert_invariants();
        // The write half of the pair `from_boot` reads back: the sealed tail
        // segment opens at or below the new floor.
        assert!(
            base.segment.start <= first,
            "the sealed tail segment opens at or below the sealed floor"
        );
        self.base = base;
        self.floor = first;
        self.chosen = self.chosen.split_off(&first);
        self.history = self.history.split_off(&first);
        self.outcomes = self.outcomes.split_off(&first);
        self.positions.retain(|_, slot| *slot >= first);
        assert!(
            self.chosen.first().is_none_or(|s| *s >= first),
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
