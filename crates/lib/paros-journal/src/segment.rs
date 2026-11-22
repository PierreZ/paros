//! The **segment record** (`docs/journal/03-segments-and-records.md` §J3,
//! §J3.4): what the journal's matchmakers hold for each segment, from the
//! reshape that opens it to its trim.
//!
//! A segment is the run of slots of one configuration: one grid and one
//! replica set. Each step of its life is one compare-and-set on the
//! journal's matchmaker register. This module holds the record and the
//! guard of each step as pure transitions; the register that runs them at a
//! matchmaker majority is #1325 (the matchmaker register as an optional
//! role). The states, in order:
//!
//! | State | Entered by | Guard here |
//! |---|---|---|
//! | `Pending` | the reshape call ([`SegmentRecord::open`]) | a v1 grid, a replica set |
//! | `InForce` | the seal end of the predecessor ([`SegmentRecord::put_in_force`]) | from `Pending` |
//! | `Sealed` | the seal end, written once ([`SegmentRecord::seal`]) | from `InForce`, end at or above start |
//! | `Retired` | the leader, after `F + 1` holders and the boundary state ([`SegmentRecord::retire`]) | from `Sealed`, boundary written, `F + 1` holders, no found damage left |
//! | `Retired`, a new replica set | the grid controller's heal ([`SegmentRecord::replace_replica`], #1314) | from `Retired` only, one member out and one new replica in |
//! | `Offloaded` | the offloader, after v1 ([`SegmentRecord::mark_offloaded`]) | from `Sealed` or `Retired` |
//! | `Trimmed` | a replica whose fold has `first_seq` at or above the segment's last `seq` ([`SegmentRecord::trim`]) | from `Sealed`, `Retired` or `Offloaded` |
//!
//! The boundary state ([`SegmentRecord::write_boundary`]) is a field, not a
//! state: the first replica that applied every slot of a sealed segment
//! writes it once.
//!
//! What the register adds and this module cannot see: the controller term,
//! the expected configuration of the reshape (the newest record of the
//! table) and the "act on a write seen done" rule (§J3.4). Those are #1325
//! and #1328 (paros-journal-control).

use paros_consensus::journal::Seq;
use paros_consensus::{NodeId, Slot};

use crate::codec::Release;
use crate::fold::BoundaryState;

/// The number of a segment in the chain of a journal, from 1 (the
/// configuration number `g` of `journal-state/`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

/// A **v1 grid** (§J5.1; decided by Pierre on 2026-11-20, #1315, decision
/// 5): the partitioned grid, one or more columns of `2F + 1` acceptors.
/// A write is done at `F + 1` acceptors of one column; a Phase 1 takes
/// `F + 1` of each column. The majority is the case of one column.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Grid {
    f: u8,
    columns: Vec<Vec<NodeId>>,
}

/// Why a grid is not a v1 grid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GridError {
    /// A grid has at least one column.
    NoColumn,
    /// Each column has `2F + 1` acceptors.
    ColumnSize {
        /// The index of the column.
        column: usize,
        /// Its size.
        size: usize,
    },
    /// An acceptor is in the grid once.
    Repeated(NodeId),
}

impl Grid {
    /// The partitioned grid with `columns` of `2f + 1` acceptors each.
    ///
    /// # Errors
    ///
    /// When the grid has no column, a column does not have `2f + 1`
    /// acceptors, or an acceptor is in it twice.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn new(f: u8, columns: Vec<Vec<NodeId>>) -> Result<Self, GridError> {
        if columns.is_empty() {
            return Err(GridError::NoColumn);
        }
        let rows = 2 * usize::from(f) + 1;
        if let Some((column, members)) = columns.iter().enumerate().find(|(_, c)| c.len() != rows) {
            return Err(GridError::ColumnSize {
                column,
                size: members.len(),
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        if let Some(repeated) = columns.iter().flatten().find(|node| !seen.insert(**node)) {
            return Err(GridError::Repeated(*repeated));
        }
        let grid = Self { f, columns };
        assert!(grid.rows() == rows, "every column has 2F + 1 rows");
        assert!(grid.acks() <= grid.rows(), "a write quorum fits a column");
        Ok(grid)
    }

    /// `F`: the failures each column survives.
    #[must_use]
    pub fn f(&self) -> u8 {
        self.f
    }

    /// The columns, each of `2F + 1` acceptors.
    #[must_use]
    pub fn columns(&self) -> &[Vec<NodeId>] {
        &self.columns
    }

    /// The acceptors of a column: `2F + 1`.
    #[must_use]
    pub fn rows(&self) -> usize {
        2 * usize::from(self.f) + 1
    }

    /// The acceptors of one column that a write needs: `F + 1`.
    #[must_use]
    pub fn acks(&self) -> usize {
        usize::from(self.f) + 1
    }
}

/// The states of a segment, in order (§J3.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SegmentState {
    /// The reshape announced the configuration. No leader writes in it yet.
    Pending,
    /// The predecessor is sealed: leaders write here.
    InForce,
    /// No new slot. The replicas may still apply the slots below the end.
    Sealed,
    /// The old acceptors stop; the next segment never needs these slots.
    Retired,
    /// The object store holds the segment (after v1: nothing sets it in v1).
    Offloaded,
    /// The segment is below the trim floor.
    Trimmed,
}

/// The identity of the reshape call that opened a segment (§J3.4, §J6.1):
/// written once, with the new segment. The register compares a retry with
/// it, so a call that it already took gets "ok" again.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OpeningCall {
    /// The controller term of the call.
    pub term: u64,
    /// The configuration the controller expected to be the newest: `None`
    /// for the first segment of a journal.
    pub expected: Option<SegmentId>,
    /// The grid of the new segment.
    pub grid: Grid,
    /// The replica set of the new segment, named in the same call.
    pub replicas: Vec<NodeId>,
}

/// Why a step of a segment record is refused. A refusal changes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordRefusal {
    /// The record is not in a state the step starts from.
    State(SegmentState),
    /// The replica set of a new segment is empty or names a node twice.
    Replicas,
    /// The opening call names an expected segment at or above the new one.
    Expected,
    /// The seal end is below the start of the segment.
    EndBelowStart {
        /// The start slot.
        start: Slot,
        /// The end asked for.
        end: Slot,
    },
    /// The boundary state is already written: it is written once.
    BoundaryWritten,
    /// The boundary state is not well formed.
    BoundaryInvalid,
    /// The step needs the boundary state, and it is not written yet.
    NoBoundary,
    /// Fewer than `F + 1` replicas of the set hold the segment.
    FewHolders {
        /// The holders counted.
        holders: usize,
        /// The holders the retire needs: `F + 1`.
        needed: usize,
    },
    /// A found damage of the segment is not repaired (a margin, Pierre,
    /// 2026-11-20, #1263).
    Damage,
    /// A heal removes a node that is not a member of the set.
    NotMember(NodeId),
    /// A heal adds a node that is a member of the set already.
    AlreadyMember(NodeId),
    /// The trim floor is below the last `seq` of the segment.
    FloorBelowEnd {
        /// The floor of the fold.
        first_seq: Seq,
        /// The `next_seq` of the segment's boundary state.
        last: Seq,
    },
}

/// The record of one segment (§J3.4).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentRecord {
    id: SegmentId,
    release: Release,
    opening: OpeningCall,
    replicas: Vec<NodeId>,
    state: SegmentState,
    start: Option<Slot>,
    end: Option<Slot>,
    boundary: Option<BoundaryState>,
}

impl SegmentRecord {
    /// The record that the reshape call `opening` writes for segment `id`,
    /// in state `Pending`, with its release.
    ///
    /// # Errors
    ///
    /// When the replica set is empty or names a node twice, or the expected
    /// segment is not below `id`.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn open(
        id: SegmentId,
        release: Release,
        opening: OpeningCall,
    ) -> Result<Self, RecordRefusal> {
        let mut seen = std::collections::BTreeSet::new();
        if opening.replicas.is_empty() || !opening.replicas.iter().all(|node| seen.insert(*node)) {
            return Err(RecordRefusal::Replicas);
        }
        if opening.expected.is_some_and(|expected| expected >= id) {
            return Err(RecordRefusal::Expected);
        }
        let record = Self {
            id,
            release,
            replicas: opening.replicas.clone(),
            opening,
            state: SegmentState::Pending,
            start: None,
            end: None,
            boundary: None,
        };
        record.assert_invariants();
        assert!(
            record.state == SegmentState::Pending,
            "a new segment is pending"
        );
        Ok(record)
    }

    /// A record as the codec read it back.
    ///
    /// # Errors
    ///
    /// When the fields break a rule of the record (`decode` refuses it).
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument for each field of the record, as the codec reads them"
    )]
    pub fn from_parts(
        id: SegmentId,
        release: Release,
        opening: OpeningCall,
        replicas: Vec<NodeId>,
        state: SegmentState,
        start: Option<Slot>,
        end: Option<Slot>,
        boundary: Option<BoundaryState>,
    ) -> Result<Self, RecordRefusal> {
        let record = Self {
            id,
            release,
            opening,
            replicas,
            state,
            start,
            end,
            boundary,
        };
        match record.check() {
            Ok(()) => {
                record.assert_invariants();
                Ok(record)
            }
            Err(refusal) => Err(refusal),
        }
    }

    /// The segment's number.
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// The release the segment was written with.
    #[must_use]
    pub fn release(&self) -> Release {
        self.release
    }

    /// The call that opened the segment.
    #[must_use]
    pub fn opening(&self) -> &OpeningCall {
        &self.opening
    }

    /// The grid of the segment.
    #[must_use]
    pub fn grid(&self) -> &Grid {
        &self.opening.grid
    }

    /// The replica set of the segment.
    #[must_use]
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// The state of the segment.
    #[must_use]
    pub fn state(&self) -> SegmentState {
        self.state
    }

    /// The first slot of the segment, once it is in force.
    #[must_use]
    pub fn start(&self) -> Option<Slot> {
        self.start
    }

    /// The seal end: one above the last slot of the segment, once sealed.
    #[must_use]
    pub fn end(&self) -> Option<Slot> {
        self.end
    }

    /// The boundary state, once written.
    #[must_use]
    pub fn boundary(&self) -> Option<BoundaryState> {
        self.boundary
    }

    /// The seal end of the predecessor put this segment in force at slot
    /// `start` (the predecessor's end; 0 for the first segment).
    ///
    /// # Errors
    ///
    /// When the segment is not `Pending`.
    pub fn put_in_force(&mut self, start: Slot) -> Result<(), RecordRefusal> {
        self.step(&[SegmentState::Pending], |record| {
            record.start = Some(start);
            record.state = SegmentState::InForce;
            Ok(())
        })
    }

    /// The first leader of the next configuration writes the seal end, once
    /// (§J3.1): one above the highest slot where its Phase 1 saw a vote in
    /// this grid, a CTRL faulty vote included (#1313).
    ///
    /// # Errors
    ///
    /// When the segment is not `InForce` (the end is written once), or the
    /// end is below the start.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn seal(&mut self, end: Slot) -> Result<(), RecordRefusal> {
        self.step(&[SegmentState::InForce], |record| {
            let start = record.start.expect("a segment in force has a start");
            if end < start {
                return Err(RecordRefusal::EndBelowStart { start, end });
            }
            assert!(record.end.is_none(), "the seal end is written once");
            record.end = Some(end);
            record.state = SegmentState::Sealed;
            Ok(())
        })
    }

    /// The first replica of the set that applied every slot of the sealed
    /// segment writes its boundary state, once (§J3.1, #1303).
    ///
    /// # Errors
    ///
    /// When the segment is not `Sealed`, the boundary state is written
    /// already, or `boundary` is not well formed.
    pub fn write_boundary(&mut self, boundary: BoundaryState) -> Result<(), RecordRefusal> {
        self.step(&[SegmentState::Sealed], |record| {
            if record.boundary.is_some() {
                return Err(RecordRefusal::BoundaryWritten);
            }
            if !boundary.is_valid() {
                return Err(RecordRefusal::BoundaryInvalid);
            }
            record.boundary = Some(boundary);
            Ok(())
        })
    }

    /// The leader retires the sealed segment (§J3.1, §J3.4): `holders`
    /// replicas of the set hold every slot, and `unrepaired_damage` says
    /// whether a found damage of the segment is still not repaired. The old
    /// acceptors stop only after this write is done.
    ///
    /// # Errors
    ///
    /// When the segment is not `Sealed`, the boundary state is not written,
    /// fewer than `F + 1` replicas hold it, or a damage is not repaired.
    pub fn retire(&mut self, holders: usize, unrepaired_damage: bool) -> Result<(), RecordRefusal> {
        let needed = self.grid().acks();
        self.step(&[SegmentState::Sealed], |record| {
            if record.boundary.is_none() {
                return Err(RecordRefusal::NoBoundary);
            }
            if holders < needed {
                return Err(RecordRefusal::FewHolders { holders, needed });
            }
            if unrepaired_damage {
                return Err(RecordRefusal::Damage);
            }
            record.state = SegmentState::Retired;
            Ok(())
        })
    }

    /// The grid controller's heal of a retired segment (§J3.1, #1314):
    /// `remove` leaves the replica set and `add` takes its place. The new
    /// replica's own report says that its copy reached the segment end; the
    /// register checks that the record is the one the controller read
    /// ([`Change::Rereplicate`](crate::register::Change::Rereplicate)). The
    /// set keeps its size and its order.
    ///
    /// # Errors
    ///
    /// When the segment is not `Retired` (a sealed segment still has its
    /// acceptors, and a trimmed one stays trimmed), `remove` is not a
    /// member, or `add` is a member already.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer error, never
    /// an operating condition.
    pub fn replace_replica(&mut self, remove: NodeId, add: NodeId) -> Result<(), RecordRefusal> {
        let size = self.replicas.len();
        self.step(&[SegmentState::Retired], |record| {
            let Some(at) = record.replicas.iter().position(|node| *node == remove) else {
                return Err(RecordRefusal::NotMember(remove));
            };
            if record.replicas.contains(&add) {
                return Err(RecordRefusal::AlreadyMember(add));
            }
            record.replicas[at] = add;
            Ok(())
        })?;
        assert!(
            self.replicas.len() == size,
            "a heal keeps the size of the set"
        );
        assert!(
            self.replicas.contains(&add) && !self.replicas.contains(&remove),
            "a heal swaps one member"
        );
        Ok(())
    }

    /// The offloader marks the segment offloaded (after v1: nothing calls it
    /// in v1, §J3.1), from "not offloaded and not trimmed".
    ///
    /// # Errors
    ///
    /// When the segment is not `Sealed` or `Retired`, or its boundary state
    /// is not written: the offloader reads the segment up to the `next_seq`
    /// of the record.
    pub fn mark_offloaded(&mut self) -> Result<(), RecordRefusal> {
        self.step(&[SegmentState::Sealed, SegmentState::Retired], |record| {
            if record.boundary.is_none() {
                return Err(RecordRefusal::NoBoundary);
            }
            record.state = SegmentState::Offloaded;
            Ok(())
        })
    }

    /// A replica whose fold has `first_seq` at or above the last `seq` of
    /// the segment marks it trimmed (§J3.2): never the open segment, never
    /// on the intent to truncate. A trimmed record never becomes live again.
    ///
    /// # Errors
    ///
    /// When the segment is not `Sealed`, `Retired` or `Offloaded`, its
    /// boundary state is not written, or `first_seq` is below its last
    /// `seq`.
    pub fn trim(&mut self, first_seq: Seq) -> Result<(), RecordRefusal> {
        self.step(
            &[
                SegmentState::Sealed,
                SegmentState::Retired,
                SegmentState::Offloaded,
            ],
            |record| {
                let Some(boundary) = record.boundary else {
                    return Err(RecordRefusal::NoBoundary);
                };
                if first_seq < boundary.next_seq {
                    return Err(RecordRefusal::FloorBelowEnd {
                        first_seq,
                        last: boundary.next_seq,
                    });
                }
                record.state = SegmentState::Trimmed;
                Ok(())
            },
        )
    }

    /// One compare-and-set: from one of `from`, `change` either moves the
    /// record or refuses; a refusal changes nothing.
    fn step(
        &mut self,
        from: &[SegmentState],
        change: impl FnOnce(&mut Self) -> Result<(), RecordRefusal>,
    ) -> Result<(), RecordRefusal> {
        self.assert_invariants();
        if !from.contains(&self.state) {
            return Err(RecordRefusal::State(self.state));
        }
        let before = self.clone();
        let mut after = self.clone();
        let result = change(&mut after);
        match result {
            Ok(()) => {
                after.assert_invariants();
                assert!(after.state >= before.state, "a segment never moves back");
                assert!(
                    before.end.is_none() || after.end == before.end,
                    "the seal end never changes"
                );
                assert!(
                    before.boundary.is_none() || after.boundary == before.boundary,
                    "the boundary state never changes"
                );
                *self = after;
            }
            Err(_) => assert!(*self == before, "a refusal changes nothing"),
        }
        result
    }

    /// The rules of the fields, as a refusal: what `from_parts` checks.
    fn check(&self) -> Result<(), RecordRefusal> {
        let mut seen = std::collections::BTreeSet::new();
        if self.replicas.is_empty() || !self.replicas.iter().all(|node| seen.insert(*node)) {
            return Err(RecordRefusal::Replicas);
        }
        if self
            .opening
            .expected
            .is_some_and(|expected| expected >= self.id)
        {
            return Err(RecordRefusal::Expected);
        }
        if (self.state >= SegmentState::InForce) != self.start.is_some()
            || (self.state >= SegmentState::Sealed) != self.end.is_some()
        {
            return Err(RecordRefusal::State(self.state));
        }
        if let (Some(start), Some(end)) = (self.start, self.end)
            && end < start
        {
            return Err(RecordRefusal::EndBelowStart { start, end });
        }
        if self.boundary.is_some_and(|b| !b.is_valid()) {
            return Err(RecordRefusal::BoundaryInvalid);
        }
        if self.boundary.is_some() && self.state < SegmentState::Sealed {
            return Err(RecordRefusal::State(self.state));
        }
        if self.state > SegmentState::Sealed && self.boundary.is_none() {
            return Err(RecordRefusal::NoBoundary);
        }
        Ok(())
    }

    /// The record's own rules.
    ///
    /// # Panics
    ///
    /// When a field breaks a rule: a start before `InForce` or none after
    /// it, an end before `Sealed` or none after it, an end below the start,
    /// a boundary state before `Sealed`, no boundary state after `Sealed`,
    /// or an empty replica set.
    pub fn assert_invariants(&self) {
        assert!(!self.replicas.is_empty(), "a segment has replicas");
        assert!(
            (self.state >= SegmentState::InForce) == self.start.is_some(),
            "a segment has a start exactly from in force on"
        );
        assert!(
            (self.state >= SegmentState::Sealed) == self.end.is_some(),
            "a segment has an end exactly from sealed on"
        );
        if let (Some(start), Some(end)) = (self.start, self.end) {
            assert!(end >= start, "a seal end is at or above the start");
        }
        if self.boundary.is_some() {
            assert!(
                self.state >= SegmentState::Sealed,
                "only a sealed segment has a boundary state"
            );
        }
        if self.state > SegmentState::Sealed {
            assert!(
                self.boundary.is_some(),
                "a segment past sealed has its boundary state"
            );
        }
    }
}

#[cfg(test)]
mod tests;
