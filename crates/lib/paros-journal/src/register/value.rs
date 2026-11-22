//! The **value of the register** and its compare-and-set changes
//! (`docs/journal/03-segments-and-records.md` §J3.4).
//!
//! The register holds the controller term and one [`SegmentRecord`] for
//! each segment of the journal. A [`Change`] is what one register
//! operation asks: [`RegisterValue::apply`] checks its guard on the value of
//! the highest write that the operation read, and gives the new value or a
//! [`ChangeRefusal`]. A refusal changes nothing. The register writes the
//! new value only at the generation of the read, so the guard and the write
//! are one compare-and-set (`specs/tla/cas-register/`).

use std::collections::BTreeMap;

use paros_consensus::journal::Seq;
use paros_consensus::{NodeId, Slot};

use crate::codec::Release;
use crate::fold::BoundaryState;
use crate::segment::{OpeningCall, RecordRefusal, SegmentId, SegmentRecord, SegmentState};

/// The controller term and its holder (§J6, `specs/tla/grid-controller/`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Term {
    /// The term number: 0 before the first election.
    pub number: u64,
    /// The controller that won the term, `None` before the first election.
    pub holder: Option<NodeId>,
}

/// The value of the journal's matchmaker register.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RegisterValue {
    term: Term,
    segments: BTreeMap<SegmentId, SegmentRecord>,
}

/// One compare-and-set on the register: its guard and its new value are
/// [`RegisterValue::apply`] (§J3.4, the table of the records).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// A controller candidate raises the term by one and holds it. Of two
    /// candidates that read the same term, one loses: the register takes
    /// one write at each generation.
    Elect {
        /// The candidate.
        holder: NodeId,
    },
    /// The grid controller's `reshape(expected configuration, new grid,
    /// term)` call opens the next segment (§J6.1). A call that the register
    /// already took gets "ok" again.
    Reshape {
        /// The call: its term, the newest segment it expects, the grid and
        /// the replica set.
        opening: OpeningCall,
        /// The release the new segment is written with.
        release: Release,
    },
    /// The first leader of the next configuration seals `segment` at `end`
    /// and puts the next segment in force at `end` (§J3.1).
    Seal {
        /// The segment in force.
        segment: SegmentId,
        /// One above the highest vote that the leader's Phase 1 saw in its
        /// grid, a CTRL faulty vote included.
        end: Slot,
    },
    /// The first replica that applied every slot of the sealed `segment`
    /// writes its boundary state.
    Boundary {
        /// The sealed segment.
        segment: SegmentId,
        /// The fold at the end of the segment.
        state: BoundaryState,
    },
    /// The leader retires the sealed `segment`.
    Retire {
        /// The sealed segment.
        segment: SegmentId,
        /// The replicas of the set that hold every slot of it.
        holders: usize,
        /// Whether a found damage of the segment is still not repaired.
        unrepaired_damage: bool,
    },
    /// The grid controller's heal of the retired `segment` (§J3.1, #1314):
    /// `remove` leaves its replica set and `add` takes its place, once
    /// `add`'s own report says that its copy reached the segment end. A
    /// compare-and-set on what the controller read: the term of the
    /// register is `term`, and the record is `read`, the record the
    /// controller planned on. A trim, a heal of another controller or a new
    /// term since the read refuses it (controls `cas_term`, `cas_blind` of
    /// `specs/tla/re-replication/`). A heal that the register already took
    /// gets "ok" again, so a retry after a lost reply is safe.
    Rereplicate {
        /// The retired segment.
        segment: SegmentId,
        /// The record the controller read when it planned the heal.
        read: SegmentRecord,
        /// The controller term of the heal.
        term: u64,
        /// The member that leaves the set.
        remove: NodeId,
        /// The new replica, whose copy is complete.
        add: NodeId,
    },
    /// The offloader marks `segment` offloaded (after v1).
    Offload {
        /// The segment.
        segment: SegmentId,
    },
    /// A replica whose fold has `first_seq` at or above the last `seq` of
    /// `segment` marks it trimmed.
    Trim {
        /// The segment.
        segment: SegmentId,
        /// The floor of the replica's fold.
        first_seq: Seq,
    },
    /// The value again, if `segment` is in `state`: a step that acts on a
    /// record it only read makes the record's write done first (a replica
    /// that drops its copy after the offload mark, §J3.4, "act on a write
    /// seen done").
    Confirm {
        /// The segment.
        segment: SegmentId,
        /// The state the step acts on.
        state: SegmentState,
    },
}

/// Why the register refused a [`Change`]. A refusal changes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeRefusal {
    /// The term of a reshape call is not the term of the register: the
    /// caller is deposed, or it did not win the term yet.
    Term {
        /// The term of the register.
        held: u64,
        /// The term of the call.
        asked: u64,
    },
    /// The expected configuration of a reshape call is not the newest
    /// segment.
    Expected {
        /// The newest segment, `None` for a journal with no segment.
        newest: Option<SegmentId>,
        /// The segment the call expected.
        asked: Option<SegmentId>,
    },
    /// A reshape needs the newest segment in force and every older one
    /// retired: this segment is not.
    Unretired(SegmentId),
    /// The register holds no record of this segment.
    NoSegment(SegmentId),
    /// A seal needs a pending successor of the segment.
    NoSuccessor(SegmentId),
    /// A confirm found the segment in another state.
    Confirm(SegmentState),
    /// A heal read a record that the register no longer holds: the record
    /// changed since the controller's read.
    Stale(SegmentId),
    /// The record of the segment refused the step.
    Record(SegmentId, RecordRefusal),
}

impl RegisterValue {
    /// The register of a new journal: term 0, no segment.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A register as the codec or a test builds it.
    ///
    /// # Panics
    ///
    /// When a record is keyed under another id than its own.
    #[must_use]
    pub fn from_parts(term: Term, segments: BTreeMap<SegmentId, SegmentRecord>) -> Self {
        let value = Self { term, segments };
        value.assert_invariants();
        value
    }

    /// The controller term.
    #[must_use]
    pub fn term(&self) -> Term {
        self.term
    }

    /// The records, by segment.
    #[must_use]
    pub fn segments(&self) -> &BTreeMap<SegmentId, SegmentRecord> {
        &self.segments
    }

    /// The record of `segment`.
    #[must_use]
    pub fn segment(&self, segment: SegmentId) -> Option<&SegmentRecord> {
        self.segments.get(&segment)
    }

    /// The newest segment.
    #[must_use]
    pub fn newest(&self) -> Option<&SegmentRecord> {
        self.segments.values().next_back()
    }

    /// The segment in force: the one the seal end of its predecessor put in
    /// force, and the first segment from its reshape on (§J3.4, "the one
    /// meaning of in force").
    #[must_use]
    pub fn in_force(&self) -> Option<&SegmentRecord> {
        self.segments
            .values()
            .rev()
            .find(|record| record.state() == SegmentState::InForce)
    }

    /// The new value that `change` writes over this one, or its refusal.
    ///
    /// # Errors
    ///
    /// The guard of the change, as [`ChangeRefusal`].
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn apply(&self, change: &Change) -> Result<Self, ChangeRefusal> {
        self.assert_invariants();
        let mut next = self.clone();
        match change {
            Change::Elect { holder } => {
                next.term = Term {
                    number: self.term.number + 1,
                    holder: Some(*holder),
                };
            }
            Change::Reshape { opening, release } => next.reshape(opening, *release)?,
            Change::Seal { segment, end } => next.seal(*segment, *end)?,
            Change::Boundary { segment, state } => {
                next.record(*segment, |record| record.write_boundary(*state))?;
            }
            Change::Retire {
                segment,
                holders,
                unrepaired_damage,
            } => next.record(*segment, |record| {
                record.retire(*holders, *unrepaired_damage)
            })?,
            Change::Rereplicate {
                segment,
                read,
                term,
                remove,
                add,
            } => next.rereplicate(*segment, read, *term, *remove, *add)?,
            Change::Offload { segment } => next.record(*segment, SegmentRecord::mark_offloaded)?,
            Change::Trim { segment, first_seq } => {
                next.record(*segment, |record| record.trim(*first_seq))?;
            }
            Change::Confirm { segment, state } => {
                let record = self
                    .segment(*segment)
                    .ok_or(ChangeRefusal::NoSegment(*segment))?;
                if record.state() != *state {
                    return Err(ChangeRefusal::Confirm(record.state()));
                }
            }
        }
        next.assert_invariants();
        assert!(next.term.number >= self.term.number, "the term never falls");
        assert!(
            self.segments
                .keys()
                .all(|id| next.segments.contains_key(id)),
            "a record is never removed"
        );
        assert!(
            next.segments.len() <= self.segments.len() + 1,
            "a change opens at most one segment"
        );
        Ok(next)
    }

    /// `Reshape`: the guards `Idempotent`, `TermCheck`, `ExpCheck` and
    /// `RetireGuard` of `specs/tla/grid-controller/`, in this order. The
    /// grid is a v1 grid by its type (`BoundGuard`).
    fn reshape(&mut self, opening: &OpeningCall, release: Release) -> Result<(), ChangeRefusal> {
        if self
            .segments
            .values()
            .any(|record| record.opening() == opening)
        {
            return Ok(());
        }
        if opening.term != self.term.number {
            return Err(ChangeRefusal::Term {
                held: self.term.number,
                asked: opening.term,
            });
        }
        let newest = self.newest().map(SegmentRecord::id);
        if opening.expected != newest {
            return Err(ChangeRefusal::Expected {
                newest,
                asked: opening.expected,
            });
        }
        if let Some(unretired) = self.segments.values().find(|record| {
            if Some(record.id()) == newest {
                record.state() != SegmentState::InForce
            } else {
                record.state() < SegmentState::Retired
            }
        }) {
            return Err(ChangeRefusal::Unretired(unretired.id()));
        }
        let id = SegmentId(newest.map_or(1, |SegmentId(n)| n + 1));
        let mut record = SegmentRecord::open(id, release, opening.clone())
            .map_err(|refusal| ChangeRefusal::Record(id, refusal))?;
        if newest.is_none() {
            // The first segment has no predecessor to seal: its reshape puts
            // it in force at the first slot.
            record
                .put_in_force(Slot(0))
                .expect("a new record is pending");
        }
        assert!(
            self.segments.insert(id, record).is_none(),
            "a reshape opens a new segment"
        );
        Ok(())
    }

    /// `Seal`: `segment` in force and its successor pending, both in one
    /// write.
    fn seal(&mut self, segment: SegmentId, end: Slot) -> Result<(), ChangeRefusal> {
        let record = self
            .segments
            .get(&segment)
            .ok_or(ChangeRefusal::NoSuccessor(segment))?;
        if record.state() != SegmentState::InForce {
            return Err(ChangeRefusal::Record(
                segment,
                RecordRefusal::State(record.state()),
            ));
        }
        let next = SegmentId(segment.0 + 1);
        match self.segments.get(&next) {
            Some(record) if record.state() == SegmentState::Pending => {}
            _ => return Err(ChangeRefusal::NoSuccessor(segment)),
        }
        self.record(segment, |record| record.seal(end))?;
        self.record(next, |record| record.put_in_force(end))
            .expect("a pending successor goes in force");
        Ok(())
    }

    /// `Rereplicate`: the heal that the register took already is "ok"
    /// again; else the term, then the record the controller read, then the
    /// guard of the record.
    fn rereplicate(
        &mut self,
        segment: SegmentId,
        read: &SegmentRecord,
        term: u64,
        remove: NodeId,
        add: NodeId,
    ) -> Result<(), ChangeRefusal> {
        let held = self
            .segments
            .get(&segment)
            .ok_or(ChangeRefusal::NoSegment(segment))?;
        let mut healed = read.clone();
        if read.id() == segment && healed.replace_replica(remove, add).is_ok() && *held == healed {
            return Ok(());
        }
        if term != self.term.number {
            return Err(ChangeRefusal::Term {
                held: self.term.number,
                asked: term,
            });
        }
        if held != read {
            return Err(ChangeRefusal::Stale(segment));
        }
        self.record(segment, |record| record.replace_replica(remove, add))
    }

    fn record(
        &mut self,
        segment: SegmentId,
        step: impl FnOnce(&mut SegmentRecord) -> Result<(), RecordRefusal>,
    ) -> Result<(), ChangeRefusal> {
        let record = self
            .segments
            .get_mut(&segment)
            .ok_or(ChangeRefusal::NoSegment(segment))?;
        step(record).map_err(|refusal| ChangeRefusal::Record(segment, refusal))
    }

    /// The value's own rules.
    ///
    /// # Panics
    ///
    /// When a record is keyed under another id, at most one segment is in
    /// force, or a holder is named before the first election.
    pub fn assert_invariants(&self) {
        for (id, record) in &self.segments {
            assert!(*id == record.id(), "a record is keyed by its id");
            record.assert_invariants();
        }
        assert!(
            self.segments
                .values()
                .filter(|record| record.state() == SegmentState::InForce)
                .count()
                <= 1,
            "at most one segment is in force"
        );
        assert!(
            (self.term.number == 0) == self.term.holder.is_none(),
            "a term above 0 has a holder"
        );
    }
}

#[cfg(test)]
mod tests;
