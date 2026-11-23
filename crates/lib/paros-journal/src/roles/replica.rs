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
//! - **`Holding`** is answered with whether the disk holds every slot of the
//!   sealed segment: the leader counts the holders for the retire, and a new
//!   replica of a heal finds its source.
//!
//! **The heal of a retired segment** (#1314, §J3.1, checked by
//! `specs/tla/re-replication/`). The grid controller starts a new replica
//! of a retired segment with [`SegmentReplica::join`], for one copy
//! ([`CopyId`]). The new replica:
//!
//! - **copies only from a member of the set that holds every slot up to the
//!   segment end** (control `copy_any`, `live_outside`): it asks the members
//!   `Holding` and pulls only from a member that answered "full";
//! - **knows the end from the record**, a done write of the seal, never from
//!   its source: its copy is complete only when its fold reaches that end;
//! - **reports its complete copy once its slots are durable**
//!   ([`ReplicaEffect::Copied`]), and the report names its copy: a new plan
//!   starts a new copy on an empty disk (control `report_sticky`);
//! - **never answers past its copy** (control `answer_partial`): `Resolve`
//!   answers `NotCovered` for a position its fold has not reached, so a
//!   partial copy never answers as the tail of an open segment.
//!
//! **A replica that is not in the set and is no plan's new replica drops its
//! copy** ([`ReplicaEffect::Drop`]): a removed member, also one that comes
//! back after a restart, and a new replica whose plan ended. To join again
//! it copies again. A trimmed record makes every replica drop its copy.
//! The replica acts on register values in the register's order: each value
//! that [`SegmentReplica::observe`] takes is at or after the last one.

use std::collections::BTreeMap;

use paros_consensus::journal::{LeaderUuid, Seq};
use paros_consensus::{NodeId, Slot};

use super::message::Message;
use super::{Flight, PersistId};
use crate::command::{Command, Leader, Record, Write};
use crate::fold::{Answer, BoundaryState, JournalState};
use crate::register::{Change, RegisterValue};
use crate::resolve::{ResolveAnswer, resolve};
use crate::segment::{SegmentId, SegmentState};

/// The most chosen slots one `Pull` answer carries.
pub const PULL_PAGE: usize = 64;

/// The copy of one heal (#1314): the controller term of the plan and its
/// number in that term. A "done" report belongs to one copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CopyId {
    /// The controller term of the plan.
    pub term: u64,
    /// The plan's number in its term.
    pub number: u64,
}

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
    /// The copy of a heal that the replica runs as a new replica, or `None`
    /// once the plan ended or the replica is a member (#1314). It is on the
    /// disk, so a new replica that restarts during its plan keeps its copy:
    /// the plan may still write it into the set.
    Joining(Option<CopyId>),
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
    /// Run this compare-and-set on the journal's register once the batch
    /// `after` is durable, and give the replica the value once the write is
    /// done ([`SegmentReplica::observe`]). The boundary state that the
    /// change writes is the fold of the replica's disk, as every answer of
    /// the replica (`BoundWrite` of `specs/tla/journal-state/`: a replica
    /// that holds every slot of its segment on its disk).
    Register {
        /// The change.
        change: Change,
        /// The batch the change waits on.
        after: Option<PersistId>,
    },
    /// Tell the grid controller that the copy `copy` of `segment` is
    /// complete, once the batch `after` is durable (#1314).
    Copied {
        /// The segment.
        segment: SegmentId,
        /// The copy.
        copy: CopyId,
        /// The batch the report waits on: the last slot of the copy.
        after: Option<PersistId>,
    },
    /// Drop the replica's copy of the segment and stop the replica: it is
    /// not in the set and is no plan's new replica, or the segment is
    /// trimmed. The replica takes no step after it.
    Drop,
}

/// What the disk of a replica holds: the input of a restart.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicaDisk {
    /// The base of the fold, once taken.
    pub base: Option<(BoundaryState, Slot)>,
    /// The chosen slots it holds.
    pub chosen: BTreeMap<Slot, Command>,
    /// The copy of a heal that the replica runs as a new replica.
    pub joining: Option<CopyId>,
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
                ReplicaOp::Joining(copy) => self.joining = *copy,
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
    /// The copy of a heal that this new replica runs, until its plan ends.
    joining: Option<CopyId>,
    /// The members that answered that they hold the whole segment: the
    /// sources of a new replica.
    sources: std::collections::BTreeSet<NodeId>,
    /// Whether the replica dropped its copy.
    dropped: bool,
    flight: Flight,
}

impl SegmentReplica {
    /// The replica `id` of `segment`, whose replica set is `set`, that
    /// starts from what its disk holds. `initial` is the leader value of the
    /// fresh journal (`Unset`, or `Multi` for a journal that starts
    /// multi-writer, §J2.4.1): the first segment folds from it. A replica
    /// that is not in `set` (a removed member, or a new replica of a heal
    /// that stopped: its plan ends with it) drops its copy at the first
    /// register value it observes.
    ///
    /// # Panics
    ///
    /// When `initial` is a uuid.
    #[must_use]
    pub fn recover(
        id: NodeId,
        segment: SegmentId,
        set: Vec<NodeId>,
        initial: Leader,
        disk: ReplicaDisk,
    ) -> Self {
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
            joining: disk.joining,
            sources: std::collections::BTreeSet::new(),
            dropped: false,
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

    /// A new replica `id` of the retired `segment`, for the copy `copy` of a
    /// heal (#1314), and the batch that records its copy: it starts with
    /// nothing in memory, and `set`, the replica set of the record that the
    /// controller read, as the members to copy from. A new plan starts a new
    /// copy. The records of an earlier copy may stay on the disk (the
    /// storage has no delete; **Open**, §J3.1): they are chosen slots of the
    /// same segment, so a later recovery folds true values, and the copy
    /// reports complete only on what it holds. The
    /// driver starts a copy only above the copy the node runs: a lower one
    /// comes from a deposed controller and is refused, so the copy that a
    /// later term counts stays on the disk. It refuses a copy whose term is
    /// below the term of the register value it holds: that copy's plan can
    /// no longer commit. It never starts a copy of a
    /// segment that the node serves as a member, and it judges that on a
    /// register value that it saw done after the ask arrived: a commit that
    /// named the node may be done although the node has not seen it, and
    /// the new copy would wipe a member's disk (`specs/tla/heal-reuse/`,
    /// control `stale_view`).
    ///
    /// `durable` is the highest batch that the node's disk holds for this
    /// replica (the records of an earlier copy): the batches of the copy
    /// follow it, so the flight rule never takes a new batch as durable.
    ///
    /// # Panics
    ///
    /// When `id` is in `set`, or `initial` is a uuid.
    #[must_use]
    pub fn join(
        id: NodeId,
        segment: SegmentId,
        set: Vec<NodeId>,
        initial: Leader,
        copy: CopyId,
        durable: Option<PersistId>,
    ) -> (Self, Vec<ReplicaEffect>) {
        assert!(!set.contains(&id), "a new replica is not a member yet");
        let disk = ReplicaDisk {
            durable,
            ..ReplicaDisk::default()
        };
        let mut replica = Self::recover(id, segment, set, initial, disk);
        let effects = replica.set_joining(Some(copy));
        assert!(replica.folded.is_none(), "a copy starts on an empty disk");
        replica.assert_invariants();
        (replica, effects)
    }

    /// The copy of a heal that this new replica runs.
    #[must_use]
    pub fn joining(&self) -> Option<CopyId> {
        self.joining
    }

    /// Whether the replica dropped its copy.
    #[must_use]
    pub fn dropped(&self) -> bool {
        self.dropped
    }

    /// The replica set the replica last saw: its peers.
    #[must_use]
    pub fn set(&self) -> &[NodeId] {
        &self.set
    }

    /// The plan of the copy `copy` ended with no new set, as its controller
    /// knows: the controller lost its term, its read became old, or the
    /// register refused the change. `value` is a register value that the
    /// driver saw done after the abandon arrived. A refusal does not prove
    /// that the heal did not commit (the "ok" of an earlier copy of the
    /// change can be lost), but a change that the register refused or that
    /// an old read ended can never commit later, so `value` shows whether it
    /// did. If `value` names the replica, it is a member and keeps its copy;
    /// else it is no plan's new replica any more and drops its copy
    /// (`specs/tla/heal-reuse/`, control `stale_view`).
    ///
    /// # Panics
    ///
    /// When `value` moves a fact the replica already holds (as
    /// [`Self::observe`]), or an assertion on its own postconditions fails.
    pub fn abandon(&mut self, copy: CopyId, value: &RegisterValue) -> Vec<ReplicaEffect> {
        let named = value
            .segment(self.segment)
            .is_some_and(|record| record.replicas().contains(&self.id));
        let mut effects = if self.joining == Some(copy) && !self.dropped && !named {
            self.set_joining(None)
        } else {
            Vec::new()
        };
        effects.extend(self.observe(value));
        assert!(self.joining != Some(copy), "an abandoned copy is over");
        self.assert_invariants();
        effects
    }

    /// Record the copy that the replica runs, in one batch.
    fn set_joining(&mut self, copy: Option<CopyId>) -> Vec<ReplicaEffect> {
        self.joining = copy;
        let id = self.flight.batch();
        vec![ReplicaEffect::Persist {
            id,
            ops: vec![ReplicaOp::Joining(copy)],
        }]
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

    /// The answer of the fold at `slot`, once the walk applied it.
    #[must_use]
    pub fn answer(&self, slot: Slot) -> Option<&Answer> {
        self.folded.as_ref()?.answers.get(&slot)
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
        if self.dropped {
            return Vec::new();
        }
        let effects = match message {
            Message::Commit {
                segment,
                slot,
                command,
            } if segment == self.segment => self.commit(from, slot, command),
            Message::Pull { segment, from: at } if segment == self.segment => self.pull(from, at),
            Message::Holding { segment } if segment == self.segment => {
                vec![ReplicaEffect::Send {
                    to: from,
                    message: Message::Holds {
                        segment,
                        full: self.full(),
                    },
                    after: self.flight.last(),
                }]
            }
            Message::Holds { segment, full } if segment == self.segment => self.holds(from, full),
            _ => Vec::new(),
        };
        self.assert_invariants();
        effects
    }

    /// A register value that a done write gave: the replica learns the end
    /// of its segment and the base of its fold, and writes the boundary
    /// state when its fold is at the end. A new replica whose copy has a
    /// term below the value's term, and that the value does not name, ends
    /// its copy and drops it: no plan of a term that is over can write it
    /// into the set.
    ///
    /// # Panics
    ///
    /// When the value moves a fact the replica already holds: a done end or
    /// a done base never changes (`EndOnce`, `RecordTrue`).
    pub fn observe(&mut self, value: &RegisterValue) -> Vec<ReplicaEffect> {
        if self.dropped {
            return Vec::new();
        }
        let mut effects = Vec::new();
        if let Some(record) = value.segment(self.segment) {
            if !record.replicas().contains(&self.id)
                && self
                    .joining
                    .is_some_and(|copy| copy.term < value.term().number)
            {
                // An orphan copy: its controller's term is over, so the
                // register refuses every change of that term and no plan
                // writes this copy into the set (`TermFence`). The copy
                // ends here, with no abandon (`specs/tla/heal-reuse/`,
                // `Orphaned`, control `orphan_kept`).
                effects.extend(self.set_joining(None));
            }
            if record.state() == SegmentState::Trimmed
                || (self.joining.is_none() && !record.replicas().contains(&self.id))
            {
                effects.extend(self.drop_copy());
                return effects;
            }
            if record.replicas().contains(&self.id) && self.joining.is_some() {
                // The heal that added this replica is done: it is a member.
                effects.extend(self.set_joining(None));
            }
            self.set = record.replicas().to_vec();
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
        effects.extend(self.copied());
        self.assert_invariants();
        effects
    }

    /// The periodic step: ask the peers for the slots the replica misses,
    /// and ask again for the write of the boundary state. A new replica asks
    /// the members whether they hold the whole segment, pulls only from
    /// those that do, and reports its complete copy again.
    #[must_use]
    pub fn tick(&self) -> Vec<ReplicaEffect> {
        if self.dropped {
            return Vec::new();
        }
        let mut effects = Vec::new();
        let peers = self.set.iter().filter(|peer| **peer != self.id);
        if self.joining.is_some() && !self.full() {
            for peer in peers.clone() {
                effects.push(ReplicaEffect::Send {
                    to: *peer,
                    message: Message::Holding {
                        segment: self.segment,
                    },
                    after: None,
                });
            }
        }
        if let Some(front) = self.front()
            && !self.full()
        {
            for peer in peers.filter(|peer| self.joining.is_none() || self.sources.contains(peer)) {
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
        effects.extend(self.copied());
        effects
    }

    /// The answer to `Resolve(uuid, expected_seq, records)` (#1320), and
    /// the batch it waits on: read only. The answer reads the replica's
    /// fold, which slots of batches still in flight may have moved, so it
    /// goes to the client once `after` is durable, as every answer of the
    /// replica (the flight rule, §J4.2). `NotCovered` until the replica has
    /// its base.
    #[must_use]
    pub fn resolve(
        &self,
        uuid: LeaderUuid,
        expected_seq: Seq,
        records: &[Record],
    ) -> (ResolveAnswer, Option<PersistId>) {
        let after = self.flight.last();
        let Some(folded) = &self.folded else {
            return (ResolveAnswer::NotCovered, after);
        };
        let answer = resolve(&folded.state, uuid, expected_seq, records, |seq| {
            folded.positions.get(&seq)
        });
        (answer, after)
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
        effects.extend(self.copied());
        effects
    }

    /// A member's answer to the new replica's `Holding`: a member that holds
    /// the whole segment is a source, and the copy pulls from it now.
    fn holds(&mut self, from: NodeId, full: bool) -> Vec<ReplicaEffect> {
        if self.joining.is_none() || !self.set.contains(&from) || from == self.id {
            return Vec::new();
        }
        if !full {
            self.sources.remove(&from);
            return Vec::new();
        }
        self.sources.insert(from);
        match self.front() {
            Some(front) if !self.full() => vec![ReplicaEffect::Send {
                to: from,
                message: Message::Pull {
                    segment: self.segment,
                    from: front,
                },
                after: None,
            }],
            _ => Vec::new(),
        }
    }

    /// The report of a complete copy, while the replica is a new replica.
    fn copied(&self) -> Vec<ReplicaEffect> {
        match self.joining {
            Some(copy) if self.full() => vec![ReplicaEffect::Copied {
                segment: self.segment,
                copy,
                after: self.flight.last(),
            }],
            _ => Vec::new(),
        }
    }

    fn drop_copy(&mut self) -> Vec<ReplicaEffect> {
        self.dropped = true;
        self.joining = None;
        self.sources.clear();
        assert!(self.dropped, "a dropped replica takes no step");
        vec![ReplicaEffect::Drop]
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
            (Some(folded), true, false) => vec![ReplicaEffect::Register {
                change: Change::Boundary {
                    segment: self.segment,
                    state: folded.state.boundary(),
                },
                after: self.flight.last(),
            }],
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
        assert!(
            self.joining.is_none() || !self.dropped,
            "a dropped replica is no plan's new replica"
        );
        assert!(
            self.sources.iter().all(|source| *source != self.id),
            "a new replica copies from another replica"
        );
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
