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
//!
//! **The compaction** (§J3.2, `specs/tla/journal-compact/`). At each tick the
//! replica drops the slots of its log below its compaction point: the first
//! slot that holds a record at or above its trim floor (`first_seq`), or its
//! front when it holds none. The same batch keeps the fold at that point
//! and the floor that chose it ([`Trim`]). The replica reports the point to
//! the leader (`CompactedTo`), which drops the acceptors' votes once `F + 1`
//! replicas of the set reached it. A `Pull` below the point gets the trim
//! instead of the slots (`TrimmedTo`), and the laggard takes it: its fold
//! jumps to the point, with no bytes and no ballot.
//!
//! A fold that starts at a trim judges every slot as the whole log does,
//! except the answer of a write that names a position below the trim
//! floor: the writes held there are gone. So until the fold's own
//! `first_seq` reaches the floor, the replica gives no answer for the slots
//! it applies, serves no read and answers `Resolve` with `NotCovered`. The
//! state of the fold never depends on those writes: a lookup only chooses
//! between `Duplicate` and `Taken`, and neither moves the state.

use std::collections::BTreeMap;

use paros_consensus::journal::{LeaderUuid, Seq};
use paros_consensus::{NodeId, Slot};

use super::message::Message;
use super::{Flight, PersistId};
use crate::command::{Command, Leader, Record, Write};
use crate::fold::{Answer, BoundaryState, JournalState};
use crate::register::{Change, RegisterValue};
use crate::resolve::{ResolveAnswer, resolve};
use crate::segment::{Grid, SegmentId, SegmentState};

mod read;

use read::PendingRead;
pub use read::{MAX_READS, READ_PAGE, READ_TICKS, ReadAnswer};

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
    /// The replica dropped the slots of its log below `trim.slot`, and keeps
    /// the fold there (its own compaction, or a jump to a peer's).
    Compact(Trim),
    /// The copy of a heal that the replica runs as a new replica, or `None`
    /// once the plan ended or the replica is a member (#1314). It is on the
    /// disk, so a new replica that restarts during its plan keeps its copy:
    /// the plan may still write it into the set.
    Joining(Option<CopyId>),
}

/// The fold of a replica at its compaction point (§J3.2): the trim point
/// that a laggard takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trim {
    /// The compaction point: the log holds no slot below it.
    pub slot: Slot,
    /// The fold at the compaction point.
    pub state: JournalState,
    /// The trim floor that chose the point: every write that a slot below
    /// the point holds lies below it.
    pub floor: Seq,
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
    /// Answer the read `read` once the batch `after` is durable: the fold
    /// it reads may hold slots of batches still in flight.
    Read {
        /// The reader's id of the read.
        read: u64,
        /// The answer.
        answer: ReadAnswer,
        /// The batch the answer waits on.
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
    /// The fold at the compaction point, once the replica compacted.
    pub trim: Option<Trim>,
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
                ReplicaOp::Compact(trim) => {
                    assert!(self.base.is_some(), "a replica compacts after its base");
                    assert!(
                        self.trim.is_none_or(|held| held.slot < trim.slot),
                        "a compaction point only moves up"
                    );
                    self.chosen.retain(|slot, _| *slot >= trim.slot);
                    self.trim = Some(*trim);
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
    /// Where the log starts: the base, or the last compaction point.
    origin: Trim,
    /// The first slot whose answer the fold gives: the fold starts at a
    /// trim, and answers again once its `first_seq` reaches the trim floor.
    answered: Slot,
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
    /// The grid of the segment, from a done value: the acceptors a read
    /// asks.
    grid: Option<Grid>,
    /// The open reads, by the reader's id.
    reads: BTreeMap<u64, PendingRead>,
    /// The first batch id of this incarnation: every id that it emits is at
    /// or above it, and the boot of its reads
    /// ([`ReadTag::boot`](super::message::ReadTag)).
    read_boot: PersistId,
    /// The reads of this incarnation that asked the grid.
    read_count: u64,
    /// The retired segments below this one that are not trimmed, with the
    /// last `seq` of each (the `next_seq` of its boundary state), from the
    /// last value observed: the trims this replica's floor may write. The
    /// register takes the trim of a sealed segment too, but a replica waits
    /// for its retire: the controller opens the next segment only once
    /// every older one is retired (`ReshapeGuard` of
    /// `specs/tla/journal-state/`), and a trimmed record never retires.
    older: BTreeMap<SegmentId, Seq>,
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
            matches!(initial, Leader::Unset | Leader::Multi(_)),
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
            grid: None,
            reads: BTreeMap::new(),
            read_boot: PersistId(disk.durable.map_or(0, |PersistId(id)| id + 1)),
            read_count: 0,
            older: BTreeMap::new(),
            flight: Flight::after(disk.durable),
        };
        if let Some((boundary, start)) = disk.base {
            let mut folded = Folded::new(boundary, start);
            if let Some(trim) = disk.trim {
                folded.jump(trim);
            }
            replica.folded = Some(folded);
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
            Message::Learn { segment, slot } if segment == self.segment => self.learn(from, slot),
            Message::Holding { segment } if segment == self.segment => {
                probe!(
                    sometimes,
                    self.full(),
                    "replica: it holds the whole segment"
                );
                probe!(
                    sometimes,
                    !self.full(),
                    "replica: it does not hold the whole segment yet"
                );
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
            Message::PreReadAck {
                segment,
                read,
                watermark,
                bound,
            } if segment == self.segment => self.pre_read_ack(from, read, watermark, bound),
            Message::Compaction { segment } if segment == self.segment => self.report(from),
            Message::TrimmedTo {
                segment,
                slot,
                state,
                floor,
            } if segment == self.segment && self.set.contains(&from) => {
                self.jump(Trim { slot, state, floor })
            }
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
                probe!(
                    reachable,
                    "replica: a replica outside the set drops its copy"
                );
                effects.extend(self.drop_copy());
                return effects;
            }
            if record.replicas().contains(&self.id) && self.joining.is_some() {
                // The heal that added this replica is done: it is a member.
                probe!(reachable, "replica: a new replica becomes a member");
                effects.extend(self.set_joining(None));
            }
            self.set = record.replicas().to_vec();
            self.grid = Some(record.grid().clone());
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
        self.older = value
            .segments()
            .range(..self.segment)
            .filter(|(_, record)| {
                matches!(
                    record.state(),
                    SegmentState::Retired | SegmentState::Offloaded
                )
            })
            .filter_map(|(id, record)| record.boundary().map(|b| (*id, b.next_seq)))
            .collect();
        effects.extend(self.boundary());
        effects.extend(self.copied());
        effects.extend(self.serve_reads());
        effects.extend(self.trims());
        self.assert_invariants();
        effects
    }

    /// The periodic step: ask the peers for the slots the replica misses,
    /// and ask again for the write of the boundary state. A new replica asks
    /// the members whether they hold the whole segment, pulls only from
    /// those that do, and reports its complete copy again. An open read
    /// asks again the acceptors that did not answer, and a read open too
    /// long is unserved.
    pub fn tick(&mut self) -> Vec<ReplicaEffect> {
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
        effects.extend(self.tick_reads());
        // Last: the answers above wait on no compaction batch.
        effects.extend(self.compact());
        effects
    }

    /// The compaction point of the fold and the fold there: the first slot
    /// that holds a record at or above the trim floor (`first_seq`), or the
    /// front when no slot does. The fold is replayed from the origin over
    /// the slots the log holds: whether a write is a record never depends
    /// on the writes below it. `None` with no base, and while the fold is
    /// blind: a record below the origin may lie at or above `first_seq`,
    /// so the point would not cover it.
    ///
    /// # Panics
    ///
    /// When the log misses an applied slot, or the replay does not give
    /// the fold: a programmer error, never an operating condition.
    #[must_use]
    pub fn compaction_point(&self) -> Option<Trim> {
        let folded = self.folded.as_ref()?;
        if folded.blind() {
            return None;
        }
        let first_seq = folded.state.first_seq();
        // Every write below the origin lies below the origin's floor, at
        // or below `first_seq` since the fold is not blind.
        let floor = first_seq;
        let mut state = folded.origin.state;
        for slot in (folded.origin.slot.0..folded.front.0).map(Slot) {
            let command = self
                .chosen
                .get(&slot)
                .expect("the log holds the applied slots");
            let before = state;
            if let Answer::Written { seq, count } = state.apply(command, |_| None)
                && seq.0 + count > first_seq.0
            {
                return Some(Trim {
                    slot,
                    state: before,
                    floor,
                });
            }
        }
        assert!(state == folded.state, "a replay gives the fold");
        Some(Trim {
            slot: folded.front,
            state,
            floor,
        })
    }

    /// Drop the slots of the log below the compaction point, in one batch
    /// that keeps the fold there.
    fn compact(&mut self) -> Vec<ReplicaEffect> {
        let Some(trim) = self.compaction_point() else {
            return Vec::new();
        };
        let folded = self.folded.as_mut().expect("a compaction point has a fold");
        if trim.slot <= folded.origin.slot {
            return Vec::new();
        }
        probe!(reachable, "replica: it compacts its log");
        folded.compact(trim);
        self.chosen.retain(|slot, _| *slot >= trim.slot);
        let id = self.flight.batch();
        vec![ReplicaEffect::Persist {
            id,
            ops: vec![ReplicaOp::Compact(trim)],
        }]
    }

    /// The answer to the leader's `Compaction`: the compaction point and
    /// the first answered slot, once durable.
    fn report(&self, from: NodeId) -> Vec<ReplicaEffect> {
        let Some(folded) = &self.folded else {
            return Vec::new();
        };
        vec![ReplicaEffect::Send {
            to: from,
            message: Message::CompactedTo {
                segment: self.segment,
                compacted: folded.origin.slot,
                answers: folded.answered,
            },
            after: self.flight.last(),
        }]
    }

    /// A peer's trim above the front: the fold jumps to it (§J3.2, the
    /// trim-point jump). A trim at or below the front changes nothing.
    fn jump(&mut self, trim: Trim) -> Vec<ReplicaEffect> {
        let Some(folded) = &mut self.folded else {
            return Vec::new();
        };
        if trim.slot <= folded.front || self.end.is_some_and(|end| trim.slot > end) {
            return Vec::new();
        }
        probe!(reachable, "replica: a laggard jumps to a peer's trim");
        folded.jump(trim);
        self.chosen.retain(|slot, _| *slot >= trim.slot);
        let id = self.flight.batch();
        // The answers of the slots a jump applies go nowhere: the fold
        // gives none until it reaches the floor.
        self.walk();
        let mut effects = vec![ReplicaEffect::Persist {
            id,
            ops: vec![ReplicaOp::Compact(trim)],
        }];
        effects.extend(self.boundary());
        effects.extend(self.copied());
        effects.extend(self.trims());
        effects
    }

    /// The answer to `Resolve(uuid, expected_seq, records)` (#1320), and
    /// the batch it waits on: read only. The answer reads the replica's
    /// fold, which slots of batches still in flight may have moved, so it
    /// goes to the client once `after` is durable, as every answer of the
    /// replica (the flight rule, §J4.2). `NotCovered` until the replica has
    /// its base, and once it dropped its copy.
    #[must_use]
    pub fn resolve(
        &self,
        uuid: LeaderUuid,
        expected_seq: Seq,
        records: &[Record],
    ) -> (ResolveAnswer, Option<PersistId>) {
        let after = self.flight.last();
        if self.dropped {
            probe!(
                reachable,
                "replica: a Resolve at a dropped copy is not covered"
            );
            return (ResolveAnswer::NotCovered, after);
        }
        let Some(folded) = &self.folded else {
            probe!(
                reachable,
                "replica: a Resolve before the base is not covered"
            );
            return (ResolveAnswer::NotCovered, after);
        };
        if folded.blind() {
            probe!(
                reachable,
                "replica: a Resolve below the trim floor is not covered"
            );
            return (ResolveAnswer::NotCovered, after);
        }
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
        if self
            .folded
            .as_ref()
            .is_some_and(|folded| slot < folded.origin.slot)
        {
            probe!(
                reachable,
                "replica: a Commit below its compaction point is dropped"
            );
            return effects;
        }
        // The compaction point is at or above the start, so a slot below
        // the start returned above. The seal end counts every vote of the
        // segment (§J3.1), so no slot at or above the end is chosen there:
        // a `Commit` past the end is a detected violation of the seal,
        // never a late message.
        assert!(
            self.end.is_none_or(|end| slot < end),
            "a slot chosen for a segment is below its seal end"
        );
        if let Some(held) = self.chosen.get(&slot) {
            assert!(
                *held == command,
                "one command is chosen at a slot (Paxos agreement)"
            );
        } else {
            let id = self.flight.batch();
            self.chosen.insert(slot, command.clone());
            effects.push(ReplicaEffect::Persist {
                id,
                ops: vec![ReplicaOp::Chosen {
                    slot,
                    command: command.clone(),
                }],
            });
        }
        let mut applied = self.walk();
        probe!(
            sometimes,
            applied.is_empty() && !self.full(),
            "replica: a chosen slot waits behind a hole"
        );
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
        effects.extend(self.serve_reads());
        effects.extend(self.trims());
        effects
    }

    /// The trim of each retired segment below this one that the fold's
    /// floor covers (§J3.2, `Trim` of `specs/tla/journal-state/`): the
    /// floor is decided, since the fold applied the `Truncate`, and the
    /// change waits on the batch of the slot that moved it. Each trim is
    /// asked once for each value observed; the register refuses a record
    /// that is trimmed already.
    fn trims(&mut self) -> Vec<ReplicaEffect> {
        let Some(folded) = &self.folded else {
            return Vec::new();
        };
        let first_seq = folded.state.first_seq();
        let covered: Vec<SegmentId> = self
            .older
            .iter()
            .filter(|(_, last)| first_seq >= **last)
            .map(|(segment, _)| *segment)
            .collect();
        let after = self.flight.last();
        covered
            .into_iter()
            .map(|segment| {
                probe!(
                    reachable,
                    "replica: a floor over a retired segment trims it"
                );
                let last = self.older.remove(&segment).expect("an older segment");
                assert!(first_seq >= last, "a trim never cuts above the floor");
                assert!(
                    segment < self.segment,
                    "a replica trims only an older segment"
                );
                ReplicaEffect::Register {
                    change: Change::Trim { segment, first_seq },
                    after,
                }
            })
            .collect()
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
        probe!(reachable, "replica: a new replica finds a full source");
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
            Some(copy) if self.full() => {
                probe!(reachable, "replica: a new replica reports its copy");
                vec![ReplicaEffect::Copied {
                    segment: self.segment,
                    copy,
                    after: self.flight.last(),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn drop_copy(&mut self) -> Vec<ReplicaEffect> {
        self.dropped = true;
        self.joining = None;
        self.sources.clear();
        let mut effects = self.unserve_reads();
        assert!(self.reads.is_empty(), "a dropped replica holds no read");
        effects.push(ReplicaEffect::Drop);
        effects
    }

    /// The answer to a leader's `Learn` (CTRL, §J5.3): the command that
    /// `slot` chose, once the batch that holds it is durable. No answer when
    /// the replica did not apply the slot, or dropped it (`Holds` of
    /// `specs/tla/journal-state/`).
    fn learn(&self, from: NodeId, slot: Slot) -> Vec<ReplicaEffect> {
        let applied = self.front().is_some_and(|front| slot < front);
        let Some(command) = self.chosen.get(&slot).filter(|_| applied) else {
            probe!(
                reachable,
                "replica: a Learn of a slot it did not apply is not answered"
            );
            return Vec::new();
        };
        probe!(reachable, "replica: a Learn is answered");
        vec![ReplicaEffect::Send {
            to: from,
            message: Message::Learned {
                segment: self.segment,
                slot,
                command: command.clone(),
            },
            after: self.flight.last(),
        }]
    }

    fn pull(&self, from: NodeId, at: Slot) -> Vec<ReplicaEffect> {
        let after = self.flight.last();
        let mut effects = Vec::new();
        if let Some(folded) = &self.folded
            && at < folded.origin.slot
        {
            probe!(
                reachable,
                "replica: a Pull below its compaction point gets the trim"
            );
            let trim = folded.origin;
            effects.push(ReplicaEffect::Send {
                to: from,
                message: Message::TrimmedTo {
                    segment: self.segment,
                    slot: trim.slot,
                    state: trim.state,
                    floor: trim.floor,
                },
                after,
            });
        }
        probe!(
            sometimes,
            self.chosen.range(at..).next().is_some(),
            "replica: a Pull is served from its slots"
        );
        effects.extend(
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
                }),
        );
        effects
    }

    /// Apply every chosen slot from the front, in order, and stop at a
    /// hole. Returns each applied slot with its command and its answer.
    fn walk(&mut self) -> Vec<(Slot, Command, Answer)> {
        let Some(folded) = &mut self.folded else {
            return Vec::new();
        };
        let mut applied = Vec::new();
        while let Some(command) = self.chosen.get(&folded.front) {
            let blind = folded.blind();
            let answer = folded
                .state
                .apply(command, |seq| folded.positions.get(&seq));
            if let (Answer::Written { seq, .. }, Command::Write(write)) = (&answer, command) {
                folded.positions.insert(*seq, write.clone());
            }
            probe!(
                sometimes,
                blind,
                "replica: a slot below the trim floor applies with no answer"
            );
            if blind {
                folded.answered = Slot(folded.front.0 + 1);
            } else {
                folded.answers.insert(folded.front, answer.clone());
                applied.push((folded.front, command.clone(), answer));
            }
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
            (Some(folded), true, false) => {
                probe!(
                    reachable,
                    "replica: a full replica writes the boundary state"
                );
                vec![ReplicaEffect::Register {
                    change: Change::Boundary {
                        segment: self.segment,
                        state: folded.state.boundary(),
                    },
                    after: self.flight.last(),
                }]
            }
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
        assert!(
            self.reads.len() <= MAX_READS,
            "a replica holds at most MAX_READS reads"
        );
        assert!(
            self.reads.is_empty() || self.folded.is_some(),
            "a replica with no base holds no read"
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
            assert!(
                self.chosen
                    .keys()
                    .next()
                    .is_none_or(|s| *s >= folded.origin.slot),
                "the log holds no slot below its compaction point"
            );
            assert!(
                folded.origin.slot >= folded.start && folded.origin.slot <= folded.front,
                "the compaction point is in the applied prefix"
            );
            assert!(
                folded.answered >= folded.origin.slot,
                "the fold answers from its origin at the earliest"
            );
            assert!(
                folded.answered <= folded.front,
                "the fold answers only what it applied"
            );
        }
    }
}

impl Folded {
    fn new(boundary: BoundaryState, start: Slot) -> Self {
        let state = JournalState::from_boundary(boundary);
        Self {
            start,
            state,
            front: start,
            positions: BTreeMap::new(),
            answers: BTreeMap::new(),
            origin: Trim {
                slot: start,
                state,
                floor: state.first_seq(),
            },
            answered: start,
        }
    }

    /// Whether the fold gives no answer: it starts at a trim, and its
    /// `first_seq` is below the trim floor.
    fn blind(&self) -> bool {
        self.state.first_seq() < self.origin.floor
    }

    /// Drop what the fold keeps below its own compaction point `trim`.
    fn compact(&mut self, trim: Trim) {
        assert!(trim.slot > self.origin.slot, "a compaction point moves up");
        assert!(
            trim.slot <= self.front,
            "a replica compacts what it applied"
        );
        self.origin = trim;
        self.answers.retain(|slot, _| *slot >= trim.slot);
        // A write that starts below the floor and ends above it stays: a
        // read from the floor starts at the write that covers it
        // (`read::records`). A write wholly below the floor is never read or
        // looked up again.
        self.positions
            .retain(|seq, write| seq.0 + write.count() > trim.floor.0);
        assert!(
            self.positions
                .iter()
                .all(|(seq, write)| seq.0 + write.count() > trim.floor.0),
            "a compaction keeps no write wholly below the floor"
        );
        self.answered = self.answered.max(trim.slot);
    }

    /// Start the fold again at `trim`: a laggard's jump, or a restart after
    /// a compaction. The answers start once the fold reaches the floor.
    fn jump(&mut self, trim: Trim) {
        assert!(trim.slot >= self.start, "a trim is in the segment");
        self.origin = trim;
        self.state = trim.state;
        self.front = trim.slot;
        self.positions.clear();
        self.answers.clear();
        self.answered = trim.slot;
    }
}

#[cfg(test)]
mod tests;
