//! The **journal fold** (`docs/journal/02-data-plane.md` §J2.2, §J2.4.1,
//! §J2.6): the state machine of one journal, judged at apply in slot order.
//!
//! A replica applies the chosen slots of its segment in order and folds
//! each command into a [`JournalState`]. Every replica of a segment folds
//! the same slots to the same state and gives each command the same
//! [`Answer`]. TLC checks these rules in `specs/tla/journal-state/` (the
//! operators `Res` and `Step` of `Journal.tla`), and `journal-sim` checks
//! that this code folds as those operators do (`FoldTrace.tla`).
//!
//! The rules, in the order the fold judges them:
//!
//! - **A fenced `Write`** (`Write(uuid, expected_seq, batch)`): an empty
//!   batch is refused (`Empty`). Then the position is judged before the
//!   fence (§J2.4.1). Below `next_seq`: below `first_seq` it is `Truncated`;
//!   below the start of this segment it is `Below` (#1301 (a resent write
//!   across a seal)); else it is `Duplicate` when the write that took that
//!   position in this segment is the same write, and `Taken` when it is not.
//!   At or above `next_seq`: a uuid that is not the leader is `Fenced`; a
//!   position above `next_seq` is `Early`; else the write is accepted.
//! - **An unfenced `Write`**: accepted at `next_seq` while the journal is
//!   multi-writer, else `WrongMode`.
//! - **`Truncate`**: a fenced one applies while its uuid leads, an unfenced
//!   one while the journal is multi-writer. A uuid that does not lead is
//!   `Fenced`, a call shaped for the other mode is `WrongMode`. It raises
//!   `first_seq`, clamped to `next_seq`, and never lowers it.
//! - **`SetLeader(new, old)`**: a compare-and-set. It applies when `old` is
//!   the leader value and `new` is not (`None` is multi-writer, §J2.4.1).
//! - **`Upgrade`** is reserved: `Reserved`, nothing moves.
//!
//! **The fold has no term.** The boundary state of a segment (§J3.1) holds
//! `next_seq`, `first_seq` and the leader value, and the next segment folds
//! from it. The leader value alone is the fence; the core's hidden term
//! counter is not part of the journal fold.
//!
//! A command that is refused, a `Noop`, a `SetLeader` and a `Truncate` take
//! a slot and no position: positions are dense (§J2.1).

use paros_consensus::journal::{LeaderUuid, Seq};

use crate::command::{Command, Fence, Leader, Write};

/// The **boundary state** of a segment (§J3.1, #1303 (two seals in a row)):
/// the journal state at the seal, without the records. The first replica of
/// a sealed segment that applied every slot writes it into the segment's
/// record, and the replicas of the next segment fold from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BoundaryState {
    /// The position the next accepted record takes.
    pub next_seq: Seq,
    /// The trim floor: every record below it was truncated.
    pub first_seq: Seq,
    /// The leader value.
    pub leader: Leader,
}

impl BoundaryState {
    /// The boundary state of a fresh journal: no record, and the leader
    /// value that the journal's initial configuration gives (`Unset`, or
    /// `Multi` for a journal that starts multi-writer; §J2.4.1).
    ///
    /// # Panics
    ///
    /// When `leader` is a uuid: a fresh journal has no writer yet.
    #[must_use]
    pub fn fresh(leader: Leader) -> Self {
        assert!(
            matches!(leader, Leader::Unset | Leader::Multi),
            "a fresh journal is unset or multi-writer"
        );
        let state = Self {
            next_seq: Seq(0),
            first_seq: Seq(0),
            leader,
        };
        assert!(state.is_valid(), "a fresh boundary state is valid");
        state
    }

    /// Whether the state is well formed: `first_seq <= next_seq` and a valid
    /// leader. A decoded boundary state is checked with it.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.first_seq <= self.next_seq && self.leader.is_valid()
    }

    /// Whether this boundary state can follow `before`, the boundary state
    /// of the segment before it: `next_seq` and `first_seq` never move back
    /// across a chain of seals (§J3.1, `NoMoveBack` of `journal-state/`).
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn follows(&self, before: &Self) -> bool {
        let follows = self.next_seq >= before.next_seq && self.first_seq >= before.first_seq;
        if follows {
            assert!(
                self.next_seq >= before.first_seq,
                "a boundary that follows another is above its floor"
            );
        }
        follows
    }
}

/// What a client learns of the journal: the leader value and the two
/// positions. Every refusal names the view it was judged against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JournalView {
    /// The leader value.
    pub leader: Leader,
    /// The position the next accepted record takes.
    pub next_seq: Seq,
    /// The first position a reader may start at.
    pub first_seq: Seq,
}

/// The answer of one applied command (the answers table of §J2).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Answer {
    /// The write is accepted: its records hold `[seq, seq + count)`. Final.
    Written {
        /// The position of the first record.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// A fenced write whose position holds the same write in this segment:
    /// written at `seq`. Final.
    Duplicate {
        /// The position of the first record.
        seq: Seq,
        /// How many records.
        count: u64,
    },
    /// A fenced write whose position holds another write in this segment:
    /// not written. Final.
    Taken(JournalView),
    /// A fenced write or truncate whose uuid does not lead: not written,
    /// and, uuids being fresh, never written (§J2.3). Final.
    Fenced(JournalView),
    /// A fenced write above `next_seq`: send it again later. Not final.
    Early(JournalView),
    /// A fenced write below `first_seq`: its fate is unknown. Not final.
    Truncated(JournalView),
    /// A fenced write whose position is in an older segment (#1301): the
    /// client sends `Resolve` to a replica of that segment
    /// ([`crate::resolve()`]). Not final.
    Below(JournalView),
    /// A call shaped for the other mode (§J2.4.1): an unfenced write or
    /// truncate on a journal that is not multi-writer, or a fenced truncate
    /// on a multi-writer one. Final for that copy.
    WrongMode(JournalView),
    /// A write with no record. The edge refuses it before it reaches
    /// consensus (§J2.7); the fold refuses it too, so a slot that holds one
    /// moves nothing.
    Empty(JournalView),
    /// The `SetLeader` applied: the view after it.
    LeaderSet(JournalView),
    /// The `SetLeader` lost: `old` is not the leader value, or `new` already
    /// is.
    LeaderRefused(JournalView),
    /// The `Truncate` applied: the view after it.
    Trimmed(JournalView),
    /// A reserved command: nothing moved.
    Reserved(JournalView),
    /// A `Noop`: nothing moved.
    Noop,
}

impl Answer {
    /// Whether the answer moves the state: an accepted write, a won
    /// `SetLeader`, an applied `Truncate`.
    #[must_use]
    pub fn applied(&self) -> bool {
        matches!(
            self,
            Self::Written { .. } | Self::LeaderSet(_) | Self::Trimmed(_)
        )
    }
}

/// The fold of one segment (§J2, §J3.1): the leader value, the positions,
/// and the first position of the segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JournalState {
    leader: Leader,
    next_seq: Seq,
    first_seq: Seq,
    /// The first position of this segment: the `next_seq` of the boundary
    /// state it started from. A fenced write below it is in an older
    /// segment, whose records this fold does not hold.
    start_seq: Seq,
}

impl JournalState {
    /// The fold of a segment that starts from `boundary`: the boundary state
    /// of the segment before it, or [`BoundaryState::fresh`] for the first
    /// segment.
    ///
    /// # Panics
    ///
    /// When `boundary` is not well formed.
    #[must_use]
    pub fn from_boundary(boundary: BoundaryState) -> Self {
        assert!(
            boundary.is_valid(),
            "a fold starts from a valid boundary state"
        );
        let state = Self {
            leader: boundary.leader,
            next_seq: boundary.next_seq,
            first_seq: boundary.first_seq,
            start_seq: boundary.next_seq,
        };
        state.assert_invariants();
        assert!(
            state.boundary() == boundary,
            "a fold starts at its boundary"
        );
        state
    }

    /// The leader value.
    #[must_use]
    pub fn leader(&self) -> Leader {
        self.leader
    }

    /// The position the next accepted record takes.
    #[must_use]
    pub fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// The first position a reader may start at.
    #[must_use]
    pub fn first_seq(&self) -> Seq {
        self.first_seq
    }

    /// The first position of this segment.
    #[must_use]
    pub fn start_seq(&self) -> Seq {
        self.start_seq
    }

    /// What a client learns of this state.
    #[must_use]
    pub fn view(&self) -> JournalView {
        JournalView {
            leader: self.leader,
            next_seq: self.next_seq,
            first_seq: self.first_seq,
        }
    }

    /// The boundary state at this point: what the replica writes into the
    /// segment record once it applied every slot of a sealed segment.
    ///
    /// # Panics
    ///
    /// When the state is not well formed (see [`Self::assert_invariants`]).
    #[must_use]
    pub fn boundary(&self) -> BoundaryState {
        let boundary = BoundaryState {
            next_seq: self.next_seq,
            first_seq: self.first_seq,
            leader: self.leader,
        };
        assert!(boundary.is_valid(), "a fold's boundary state is valid");
        boundary
    }

    /// Whether the fold holds the records at `seq`: at or above the start
    /// of the segment and below `next_seq`. A `Resolve` and a resend are
    /// judged against the records of this segment only.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    #[must_use]
    pub fn holds(&self, seq: Seq) -> bool {
        let holds = self.start_seq <= seq && seq < self.next_seq;
        if holds {
            assert!(
                self.next_seq > self.start_seq,
                "a segment that holds a record is not empty"
            );
        }
        holds
    }

    /// Apply one decided `command` and give its answer. `written_at(seq)`
    /// answers "which write took position `seq` in this segment?": the
    /// write whose first record is at `seq`. The fold asks it only for a
    /// position that it [`holds`](Self::holds).
    ///
    /// # Panics
    ///
    /// When a transition breaks the state's own rules (a programmer error,
    /// never an operating condition).
    pub fn apply<'a>(
        &mut self,
        command: &Command,
        written_at: impl Fn(Seq) -> Option<&'a Write>,
    ) -> Answer {
        self.assert_invariants();
        let before = *self;
        let answer = self.judge(command, written_at);
        self.assert_transition(&before, &answer);
        self.assert_invariants();
        answer
    }

    fn judge<'a>(
        &mut self,
        command: &Command,
        written_at: impl Fn(Seq) -> Option<&'a Write>,
    ) -> Answer {
        match command {
            Command::Noop => Answer::Noop,
            Command::Write(write) => match write.fence {
                Fence::Leader { uuid, expected_seq } => {
                    self.apply_fenced(write, uuid, expected_seq, written_at)
                }
                Fence::Open => self.apply_open(write),
            },
            Command::Truncate { fence, up_to } => self.apply_truncate(*fence, *up_to),
            Command::SetLeader { new, old } => self.apply_set_leader(*new, *old),
            Command::Upgrade { .. } => Answer::Reserved(self.view()),
        }
    }

    /// A fenced write: the position before the fence (§J2.4.1).
    fn apply_fenced<'a>(
        &mut self,
        write: &Write,
        uuid: LeaderUuid,
        expected_seq: Seq,
        written_at: impl Fn(Seq) -> Option<&'a Write>,
    ) -> Answer {
        assert!(
            matches!(write.fence, Fence::Leader { .. }),
            "a fenced write carries its fence"
        );
        if write.records.is_empty() {
            return Answer::Empty(self.view());
        }
        if expected_seq < self.next_seq {
            if expected_seq < self.first_seq {
                return Answer::Truncated(self.view());
            }
            if expected_seq < self.start_seq {
                return Answer::Below(self.view());
            }
            assert!(
                self.holds(expected_seq),
                "the fold holds the position it looks up"
            );
            return match written_at(expected_seq) {
                Some(original) if original.same_write(write) => {
                    assert!(
                        expected_seq.0 + write.count() <= self.next_seq.0,
                        "a written write lies below the next position"
                    );
                    Answer::Duplicate {
                        seq: expected_seq,
                        count: write.count(),
                    }
                }
                _ => Answer::Taken(self.view()),
            };
        }
        if self.leader != Leader::Uuid(uuid) {
            return Answer::Fenced(self.view());
        }
        if expected_seq > self.next_seq {
            return Answer::Early(self.view());
        }
        assert!(
            expected_seq == self.next_seq,
            "an accepted write takes the next position"
        );
        assert!(uuid.is_set(), "a leading uuid is set");
        self.append(write)
    }

    /// An unfenced write: accepted at the next position in multi-writer
    /// mode (§J2.4).
    fn apply_open(&mut self, write: &Write) -> Answer {
        assert!(write.fence == Fence::Open, "an open write has no fence");
        if write.records.is_empty() {
            return Answer::Empty(self.view());
        }
        if self.leader != Leader::Multi {
            return Answer::WrongMode(self.view());
        }
        self.append(write)
    }

    fn append(&mut self, write: &Write) -> Answer {
        let count = write.count();
        assert!(count > 0, "an accepted write carries records");
        let seq = self.next_seq;
        self.next_seq = Seq(seq
            .0
            .checked_add(count)
            .expect("a journal's positions fit a u64"));
        assert!(
            self.next_seq > seq,
            "an accepted write advances the next position"
        );
        Answer::Written { seq, count }
    }

    /// `Truncate` (§J2.6): fenced in single-writer mode, open in
    /// multi-writer mode.
    fn apply_truncate(&mut self, fence: Option<LeaderUuid>, up_to: Seq) -> Answer {
        match (fence, self.leader) {
            (Some(uuid), Leader::Uuid(leader)) if uuid == leader => self.trim(up_to),
            (None, Leader::Multi) => self.trim(up_to),
            (Some(_), Leader::Multi) | (None, Leader::Uuid(_) | Leader::Unset) => {
                Answer::WrongMode(self.view())
            }
            (Some(_), Leader::Uuid(_) | Leader::Unset) => Answer::Fenced(self.view()),
        }
    }

    fn trim(&mut self, up_to: Seq) -> Answer {
        let before = self.first_seq;
        self.first_seq = self.first_seq.max(up_to.min(self.next_seq));
        assert!(
            self.first_seq >= before,
            "a truncation never lowers first_seq"
        );
        assert!(
            self.first_seq <= self.next_seq,
            "a truncation is clamped to next_seq"
        );
        Answer::Trimmed(self.view())
    }

    /// `SetLeader(new, old)`: a compare-and-set on the leader value.
    fn apply_set_leader(&mut self, new: Option<LeaderUuid>, old: Leader) -> Answer {
        if new.is_some_and(|uuid| !uuid.is_set()) || old != self.leader {
            return Answer::LeaderRefused(self.view());
        }
        let installed = Leader::from_new(new);
        if installed == self.leader {
            return Answer::LeaderRefused(self.view());
        }
        assert!(
            old == self.leader,
            "a won SetLeader named the leader in force"
        );
        self.leader = installed;
        assert!(self.leader != old, "a won SetLeader changes the leader");
        Answer::LeaderSet(self.view())
    }

    /// The postconditions of one step: what each answer may move.
    fn assert_transition(&self, before: &Self, answer: &Answer) {
        assert!(
            self.start_seq == before.start_seq,
            "a segment's start never moves"
        );
        assert!(self.next_seq >= before.next_seq, "next_seq never decreases");
        assert!(
            self.first_seq >= before.first_seq,
            "first_seq never decreases"
        );
        match answer {
            Answer::Written { seq, count } => {
                assert!(
                    *seq == before.next_seq,
                    "a write is accepted at the next position"
                );
                assert!(
                    self.next_seq.0 == seq.0 + count,
                    "a write advances by its records"
                );
                assert!(
                    self.leader == before.leader && self.first_seq == before.first_seq,
                    "a write moves only next_seq"
                );
            }
            Answer::LeaderSet(view) => {
                assert!(
                    *view == self.view(),
                    "a won SetLeader reports the state after it"
                );
                assert!(
                    self.leader != before.leader,
                    "a won SetLeader changes the leader"
                );
                assert!(
                    self.next_seq == before.next_seq && self.first_seq == before.first_seq,
                    "a SetLeader moves no position"
                );
            }
            Answer::Trimmed(view) => {
                assert!(
                    *view == self.view(),
                    "a truncation reports the state after it"
                );
                assert!(
                    self.next_seq == before.next_seq && self.leader == before.leader,
                    "a truncation moves only first_seq"
                );
            }
            Answer::Duplicate { seq, count } => {
                assert!(*self == *before, "a duplicate moves nothing");
                assert!(
                    seq.0 + count <= self.next_seq.0,
                    "a duplicate lies below next_seq"
                );
            }
            Answer::Noop => assert!(*self == *before, "a Noop moves nothing"),
            Answer::Taken(judged)
            | Answer::Fenced(judged)
            | Answer::Early(judged)
            | Answer::Truncated(judged)
            | Answer::Below(judged)
            | Answer::WrongMode(judged)
            | Answer::Empty(judged)
            | Answer::LeaderRefused(judged)
            | Answer::Reserved(judged) => {
                assert!(*self == *before, "a refused command moves nothing");
                assert!(
                    *judged == before.view(),
                    "a refusal names the state it was judged against"
                );
            }
        }
    }

    /// The state's own rules.
    ///
    /// # Panics
    ///
    /// When `first_seq` passed `next_seq`, the segment starts above
    /// `next_seq`, or the unset uuid leads.
    pub fn assert_invariants(&self) {
        assert!(
            self.first_seq <= self.next_seq,
            "a journal's first position never passes its next one"
        );
        assert!(
            self.start_seq <= self.next_seq,
            "a segment starts at or below the next position"
        );
        assert!(self.leader.is_valid(), "the unset uuid never leads");
    }
}

#[cfg(test)]
mod tests;
