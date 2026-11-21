//! The **segment replica** (§J4.1, §J3.1): one replica of one segment.
//!
//! The replica holds the chosen log of its segment and folds it in slot
//! order with [`JournalState`], from the boundary state of the segment
//! before it (#1303 (two seals in a row)). Each segment has its own replica
//! set, and a write reaches only that set (#1271): the leader sends each
//! chosen slot to the set of the slot's segment.
//!
//! The rules, each from `specs/tla/journal-state/` (`Apply`, `BoundWrite`,
//! `TakeBase`):
//!
//! - **The base comes from the record of the segment just before**, never
//!   from the newest record (control `record_latest` of `seal-chain/`). The
//!   first segment folds from the fresh journal. The replica takes the base
//!   only from a register value that a done write gave (§J3.4).
//! - **The replica applies in slot order and never steps over a hole**: the
//!   fold of a slot depends on every slot before it.
//! - **The end of its segment is known only from a done write of the seal
//!   end** (control `end_read`). When its fold reaches the end, the replica
//!   writes the boundary state of its segment by compare-and-set, once for
//!   the segment: the first replica's write wins, and every write carries
//!   the same value, the fold at the end.
//! - **A chosen slot is durable at the replica before its answer leaves**
//!   (§J4.2, rule 1): the `Applied` reply waits on the batch that holds the
//!   slot.
//! - **A replica that misses slots asks its peers** (`Pull`): a peer of the
//!   set that applied a slot gives it (the model's `Apply`, "a peer of its
//!   set applied it").
//! - **`Resolve`** (#1320) is answered from the fold and the positions of
//!   the segment, read only.

use std::collections::BTreeMap;

use paros_consensus::journal::{LeaderUuid, Seq};
use paros_consensus::{NodeId, Slot};

use super::message::Message;
use super::{Flight, PersistId};
use crate::command::{Command, Leader, Record, Write};
use crate::fold::{Answer, BoundaryState, JournalState};
use crate::register::{Change, RegisterValue};
use crate::resolve::{ResolveAnswer, resolve};
use crate::segment::SegmentId;

/// The most chosen slots one `Pull` answer carries.
pub const PULL_PAGE: usize = 64;

/// One durable write of a replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicaOp {
    /// The base of the fold: the boundary state of the segment before, and
    /// the first slot of the segment.
    Base {
        /// The boundary state.
        boundary: BoundaryState,
        /// The first slot of the segment.
        start: Slot,
    },
    /// `slot` chose `command`.
    Chosen {
        /// The slot.
        slot: Slot,
        /// The command.
        command: Command,
    },
}

/// What one step of the replica asks of its driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicaEffect {
    /// Make `ops` durable as one atomic batch `id`.
    Persist {
        /// The batch id.
        id: PersistId,
        /// The writes, in order.
        ops: Vec<ReplicaOp>,
    },
    /// Send `message` to `to` once batch `after` is durable.
    Send {
        /// The receiver.
        to: NodeId,
        /// The message.
        message: Message,
        /// The batch the send waits on.
        after: Option<PersistId>,
    },
    /// Run this compare-and-set on the journal's register, and give the
    /// replica the value once the write is done
    /// ([`SegmentReplica::observe`]).
    Register(Change),
}

/// What the disk of a replica holds: the input of a restart.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicaDisk {
    /// The base of the fold, once taken.
    pub base: Option<(BoundaryState, Slot)>,
    /// The chosen slots it holds.
    pub chosen: BTreeMap<Slot, Command>,
    /// The highest batch the disk holds.
    pub durable: Option<PersistId>,
}

impl ReplicaDisk {
    /// Apply one durable batch, as a driver does.
    ///
    /// # Panics
    ///
    /// When the batch id does not grow, the base is written twice, or a slot
    /// changes its command.
    pub fn apply(&mut self, id: PersistId, ops: &[ReplicaOp]) {
        assert!(
            self.durable.is_none_or(|durable| durable < id),
            "batches are durable in id order"
        );
        for op in ops {
            match op {
                ReplicaOp::Base { boundary, start } => {
                    assert!(self.base.is_none(), "the base is written once");
                    self.base = Some((*boundary, *start));
                }
                ReplicaOp::Chosen { slot, command } => {
                    let previous = self.chosen.insert(*slot, command.clone());
                    assert!(
                        previous.is_none_or(|previous| previous == *command),
                        "a chosen slot keeps its command"
                    );
                }
            }
        }
        self.durable = Some(id);
    }
}

/// The fold of a replica that has its base.
#[derive(Clone, Debug)]
struct Folded {
    start: Slot,
    state: JournalState,
    /// The next slot to apply.
    front: Slot,
    /// The fenced writes the fold accepted, by position.
    positions: BTreeMap<Seq, Write>,
    /// The answer of each applied slot.
    answers: BTreeMap<Slot, Answer>,
}

/// One replica of one segment.
#[derive(Clone, Debug)]
pub struct SegmentReplica {
    id: NodeId,
    segment: SegmentId,
    set: Vec<NodeId>,
    initial: Leader,
    chosen: BTreeMap<Slot, Command>,
    folded: Option<Folded>,
    /// The end of the segment, from a done write of the seal.
    end: Option<Slot>,
    /// Whether a done value showed the segment's boundary state written.
    boundary_written: bool,
    flight: Flight,
}

impl SegmentReplica {
    /// The replica `id` of `segment`, whose replica set is `set`, that
    /// starts from what its disk holds. `initial` is the leader value of the
    /// fresh journal (`Unset`, or `Multi` for a journal that starts
    /// multi-writer, §J2.4.1): the first segment folds from it.
    ///
    /// # Panics
    ///
    /// When `id` is not in `set`, or `initial` is a uuid.
    #[must_use]
    pub fn recover(
        id: NodeId,
        segment: SegmentId,
        set: Vec<NodeId>,
        initial: Leader,
        disk: ReplicaDisk,
    ) -> Self {
        assert!(set.contains(&id), "a replica is in its own set");
        assert!(
            matches!(initial, Leader::Unset | Leader::Multi),
            "a fresh journal is unset or multi-writer"
        );
        let mut replica = Self {
            id,
            segment,
            set,
            initial,
            chosen: disk.chosen,
            folded: None,
            end: None,
            boundary_written: false,
            flight: Flight::after(disk.durable),
        };
        if let Some((boundary, start)) = disk.base {
            replica.folded = Some(Folded::new(boundary, start));
            // The answers of a refold go nowhere: the leader that held them
            // asks again with its `Commit`.
            replica.walk();
        }
        replica.assert_invariants();
        replica
    }

    /// The replica's node.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The replica's segment.
    #[must_use]
    pub fn segment(&self) -> SegmentId {
        self.segment
    }

    /// The next slot the replica applies, once it has its base.
    #[must_use]
    pub fn front(&self) -> Option<Slot> {
        self.folded.as_ref().map(|folded| folded.front)
    }

    /// The first slot of the segment, once the replica has its base.
    #[must_use]
    pub fn start(&self) -> Option<Slot> {
        self.folded.as_ref().map(|folded| folded.start)
    }

    /// The fold, once the replica has its base.
    #[must_use]
    pub fn state(&self) -> Option<&JournalState> {
        self.folded.as_ref().map(|folded| &folded.state)
    }

    /// The end of the segment, once a done seal showed it.
    #[must_use]
    pub fn end(&self) -> Option<Slot> {
        self.end
    }

    /// Whether the replica applied every slot of its sealed segment.
    #[must_use]
    pub fn full(&self) -> bool {
        self.end.is_some() && self.front() == self.end
    }

    /// The chosen slots the replica holds.
    #[must_use]
    pub fn chosen(&self) -> &BTreeMap<Slot, Command> {
        &self.chosen
    }

    /// One step: a message from `from`. A message that is not for this
    /// replica's segment changes nothing.
    ///
    /// # Panics
    ///
    /// When a `Commit` names a command other than the one the replica holds
    /// at that slot: two commands chosen at one slot break Paxos (a
    /// detected safety violation).
    pub fn step(&mut self, from: NodeId, message: Message) -> Vec<ReplicaEffect> {
        let effects = match message {
            Message::Commit {
                segment,
                slot,
                command,
            } if segment == self.segment => self.commit(from, slot, command),
            Message::Pull { segment, from: at } if segment == self.segment => self.pull(from, at),
            _ => Vec::new(),
        };
        self.assert_invariants();
        effects
    }

    /// A register value that a done write gave: the replica learns the end
    /// of its segment and the base of its fold, and writes the boundary
    /// state when its fold is at the end.
    ///
    /// # Panics
    ///
    /// When the value moves a fact the replica already holds: a done end or
    /// a done base never changes (`EndOnce`, `RecordTrue`).
    pub fn observe(&mut self, value: &RegisterValue) -> Vec<ReplicaEffect> {
        let mut effects = Vec::new();
        if let Some(record) = value.segment(self.segment) {
            if let Some(end) = record.end() {
                assert!(
                    self.end.is_none_or(|known| known == end),
                    "a done seal end never changes"
                );
                self.end = Some(end);
            }
            self.boundary_written |= record.boundary().is_some();
            if self.folded.is_none()
                && let Some(start) = record.start()
                && let Some(boundary) = self.base_of(value)
            {
                effects.extend(self.take_base(boundary, start));
            }
        }
        effects.extend(self.boundary());
        self.assert_invariants();
        effects
    }

    /// The periodic step: ask the peers for the slots the replica misses,
    /// and ask again for the write of the boundary state.
    #[must_use]
    pub fn tick(&self) -> Vec<ReplicaEffect> {
        let mut effects = Vec::new();
        if let Some(front) = self.front()
            && !self.full()
        {
            for peer in self.set.iter().filter(|peer| **peer != self.id) {
                effects.push(ReplicaEffect::Send {
                    to: *peer,
                    message: Message::Pull {
                        segment: self.segment,
                        from: front,
                    },
                    after: None,
                });
            }
        }
        effects.extend(self.boundary());
        effects
    }

    /// The answer to `Resolve(uuid, expected_seq, records)` (#1320): read
    /// only. `NotCovered` until the replica has its base.
    #[must_use]
    pub fn resolve(
        &self,
        uuid: LeaderUuid,
        expected_seq: Seq,
        records: &[Record],
    ) -> ResolveAnswer {
        let Some(folded) = &self.folded else {
            return ResolveAnswer::NotCovered;
        };
        resolve(&folded.state, uuid, expected_seq, records, |seq| {
            folded.positions.get(&seq)
        })
    }

    /// The boundary state of the segment before, from `value`: the fresh
    /// journal for the first segment.
    fn base_of(&self, value: &RegisterValue) -> Option<BoundaryState> {
        let before = SegmentId(self.segment.0.checked_sub(1)?);
        match value.segment(before) {
            Some(record) => record.boundary(),
            None if value.segments().keys().next() == Some(&self.segment) => {
                Some(BoundaryState::fresh(self.initial))
            }
            None => None,
        }
    }

    fn take_base(&mut self, boundary: BoundaryState, start: Slot) -> Vec<ReplicaEffect> {
        assert!(self.folded.is_none(), "the base is taken once");
        let id = self.flight.batch();
        self.folded = Some(Folded::new(boundary, start));
        // Nobody holds the answers of the slots applied at the base: a
        // leader that holds one asks again with its `Commit`.
        self.walk();
        assert!(
            self.folded.as_ref().is_some_and(|f| f.start == start),
            "the fold starts at the start of the segment"
        );
        let mut effects = vec![ReplicaEffect::Persist {
            id,
            ops: vec![ReplicaOp::Base { boundary, start }],
        }];
        effects.extend(self.boundary());
        effects
    }

    fn commit(&mut self, from: NodeId, slot: Slot, command: Command) -> Vec<ReplicaEffect> {
        let mut effects = Vec::new();
        if let Some(held) = self.chosen.get(&slot) {
            assert!(
                *held == command,
                "one command is chosen at a slot (Paxos agreement)"
            );
        } else if self.end.is_none_or(|end| slot < end)
            && self.start().is_none_or(|start| slot >= start)
        {
            let id = self.flight.batch();
            self.chosen.insert(slot, command.clone());
            effects.push(ReplicaEffect::Persist {
                id,
                ops: vec![ReplicaOp::Chosen {
                    slot,
                    command: command.clone(),
                }],
            });
        } else {
            return effects;
        }
        let mut applied = self.walk();
        if applied.iter().all(|(s, ..)| *s != slot)
            && let Some(folded) = &self.folded
            && let Some(answer) = folded.answers.get(&slot)
        {
            applied.push((slot, command, answer.clone()));
        }
        let after = self.flight.last();
        for (slot, command, answer) in applied {
            effects.push(ReplicaEffect::Send {
                to: from,
                message: Message::Applied {
                    segment: self.segment,
                    slot,
                    command,
                    answer,
                },
                after,
            });
        }
        effects.extend(self.boundary());
        effects
    }

    fn pull(&self, from: NodeId, at: Slot) -> Vec<ReplicaEffect> {
        let after = self.flight.last();
        self.chosen
            .range(at..)
            .take(PULL_PAGE)
            .map(|(slot, command)| ReplicaEffect::Send {
                to: from,
                message: Message::Commit {
                    segment: self.segment,
                    slot: *slot,
                    command: command.clone(),
                },
                after,
            })
            .collect()
    }

    /// Apply every chosen slot from the front, in order, and stop at a
    /// hole. Returns each applied slot with its command and its answer.
    fn walk(&mut self) -> Vec<(Slot, Command, Answer)> {
        let Some(folded) = &mut self.folded else {
            return Vec::new();
        };
        let mut applied = Vec::new();
        while let Some(command) = self.chosen.get(&folded.front) {
            let answer = folded
                .state
                .apply(command, |seq| folded.positions.get(&seq));
            if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
                folded.positions.insert(*seq, write.clone());
            }
            folded.answers.insert(folded.front, answer.clone());
            applied.push((folded.front, command.clone(), answer));
            folded.front = Slot(folded.front.0 + 1);
        }
        assert!(
            !self.chosen.contains_key(&folded.front),
            "the walk stops at a hole"
        );
        applied
    }

    /// The write of the boundary state, when the fold is at the end of the
    /// sealed segment and no done value showed it written.
    fn boundary(&self) -> Vec<ReplicaEffect> {
        match (&self.folded, self.full(), self.boundary_written) {
            (Some(folded), true, false) => vec![ReplicaEffect::Register(Change::Boundary {
                segment: self.segment,
                state: folded.state.boundary(),
            })],
            _ => Vec::new(),
        }
    }

    /// The replica's own rules.
    ///
    /// # Panics
    ///
    /// When the front passed the end, or the fold is behind a slot it holds
    /// with no hole before it.
    pub fn assert_invariants(&self) {
        if let Some(folded) = &self.folded {
            folded.state.assert_invariants();
            assert!(folded.front >= folded.start, "the front is in the segment");
            assert!(
                self.end.is_none_or(|end| folded.front <= end),
                "the front never passes the end"
            );
            assert!(
                !self.chosen.contains_key(&folded.front),
                "the fold applies every slot it can"
            );
            assert!(
                self.chosen.keys().next().is_none_or(|s| *s >= folded.start),
                "the replica holds no slot of another segment"
            );
        }
    }
}

impl Folded {
    fn new(boundary: BoundaryState, start: Slot) -> Self {
        Self {
            start,
            state: JournalState::from_boundary(boundary),
            front: start,
            positions: BTreeMap::new(),
            answers: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests;
