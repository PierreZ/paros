//! The **journal acceptor** (§J4.1): one acceptor of the journal's grids.
//!
//! It is the core's [`Acceptor`] over the journal's [`Command`], with the
//! flight rule of §J4.2 around it: each step that writes emits one persist
//! batch, and its reply waits on that batch. A reply with no write (a
//! `Prepare` at the ballot already promised, an `Accept` that the acceptor
//! already holds, a refusal) waits on the last batch that the acceptor
//! emitted, because the batch that recorded the state it reports may still
//! be in flight (`specs/tla/flight-rule/`, control `own_batch`).
//!
//! **An `Accept` that raises the promise writes the promise and the vote in
//! one batch** (#1309 (the state is the disk), `specs/tla/journal-state/`):
//! a vote that is durable before its promise lets the acceptor start again
//! with its old promise and take a lower ballot's `Accept` over a chosen
//! vote (control `order_vote_first`). The batch lists the promise first,
//! and a batch is atomic (§J4.2, rule 4).
//!
//! One acceptor serves every segment whose grid names it, with one promise:
//! the leader picks the acceptors of a slot from its own grid, and a Phase 1
//! reads each segment that is not retired (§J3.1). The acceptor never reads
//! the chosen prefix (`AGENTS.md`).
//!
//! **The compaction of a segment** (§J3.2, #1318,
//! `specs/tla/journal-compact/`). The leader sends `Compact` once `F + 1`
//! replicas of the segment's set dropped the slots below a point; the
//! acceptor then drops its votes of the segment below that point, in one
//! batch after the batches before it. A dropped slot is chosen and applied:
//!
//! - **a promise reports every dropped range**, so the leader proposes
//!   nothing there and its seal end counts the range as votes;
//! - **an `Accept` in a dropped range gets no answer**: the slot is chosen,
//!   and only a leader that did not read the drop proposes there;
//! - **the watermark of a leaderless read counts the dropped slots**: a slot
//!   that a quorum chose and then dropped is still a slot the reader must
//!   apply before it serves.
//!
//! **A CTRL repair** (§J5.3, `specs/tla/journal-ctrl/`). A vote whose value
//! the disk lost is faulty: the acceptor reports its ballot in each promise.
//! The leader sends `Repair` with the command that its round chose at its
//! ballot; the acceptor writes it at the higher of the two ballots, with the
//! promise raised to it, in one batch. A vote above the choosing ballot
//! carries the chosen command (P2c), so the write gives back the lost vote.

use std::collections::BTreeMap;

use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::contract::limits::PROMISE_BATCH;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::{Ballot, NodeId, Slot};

use super::message::{Message, PromisePage, ReadTag};
use super::{Flight, PersistId};
use crate::command::Command;
use crate::segment::SegmentId;

/// One write of an acceptor's persist batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptorOp {
    /// A write of the core's acceptor: a promise or a vote.
    Write(AcceptorWrite<Command>),
    /// Drop the votes of the slots `start .. below` of `segment`.
    Compact {
        /// The segment.
        segment: SegmentId,
        /// The first slot of the segment.
        start: Slot,
        /// The slot below which the votes go.
        below: Slot,
    },
}

impl From<AcceptorWrite<Command>> for AcceptorOp {
    fn from(write: AcceptorWrite<Command>) -> Self {
        Self::Write(write)
    }
}

/// What one step of the acceptor asks of its driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptorEffect {
    /// Make `ops` durable as one atomic batch `id`.
    Persist {
        /// The batch id.
        id: PersistId,
        /// The writes, in order.
        ops: Vec<AcceptorOp>,
    },
    /// Send `message` to `to` once batch `after` is durable.
    Send {
        /// The receiver.
        to: NodeId,
        /// The message.
        message: Message,
        /// The batch the send waits on; `None` waits on nothing.
        after: Option<PersistId>,
    },
}

/// What the disk of an acceptor holds: the input of a restart (§J4.2, rule
/// 2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AcceptorDisk {
    /// The durable promise.
    pub promised: Ballot,
    /// The durable votes.
    pub votes: BTreeMap<Slot, (Ballot, Command)>,
    /// The votes whose value the disk lost (CTRL): the ballot is kept.
    pub faulty: BTreeMap<Slot, Ballot>,
    /// The ranges whose votes the acceptor dropped, `start .. below` by
    /// start, one for each segment.
    pub dropped: BTreeMap<Slot, Slot>,
    /// The highest batch the disk holds.
    pub durable: Option<PersistId>,
}

impl AcceptorDisk {
    /// Apply one durable batch, as a driver does.
    ///
    /// # Panics
    ///
    /// When the batch id does not grow, or a write breaks the acceptor's
    /// order (a vote above the promise).
    pub fn apply(&mut self, id: PersistId, ops: &[AcceptorOp]) {
        assert!(
            self.durable.is_none_or(|durable| durable < id),
            "batches are durable in id order"
        );
        for op in ops {
            match op {
                AcceptorOp::Write(AcceptorWrite::SetPromise(ballot)) => {
                    assert!(*ballot >= self.promised, "a promise never falls");
                    self.promised = *ballot;
                }
                AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                    slot,
                    ballot,
                    value,
                }) => {
                    assert!(
                        *ballot <= self.promised,
                        "a vote is durable only under its promise"
                    );
                    assert!(
                        !dropped_at(&self.dropped, *slot),
                        "a vote is never written in a dropped range"
                    );
                    self.faulty.remove(slot);
                    self.votes.insert(*slot, (*ballot, value.clone()));
                }
                AcceptorOp::Compact { start, below, .. } => {
                    assert!(start < below, "a compaction drops a slot");
                    drop_range(&mut self.dropped, *start, *below);
                    self.votes.retain(|slot, _| !(start..below).contains(&slot));
                    self.faulty
                        .retain(|slot, _| !(start..below).contains(&slot));
                }
            }
        }
        self.durable = Some(id);
    }
}

/// One acceptor of the journal.
#[derive(Clone, Debug)]
pub struct JournalAcceptor {
    id: NodeId,
    acceptor: Acceptor<Command>,
    flight: Flight,
    /// The votes and faulty entries one promise page carries at most.
    page: usize,
    /// The newest segment that a `Prepare` at or above the promise named
    /// since the acceptor started: its belief, the fence of a leaderless
    /// read (§J3.3). In memory only: an acceptor that starts again answers
    /// no read until a `Prepare` tells it, so it never names a segment
    /// older than one it forgot. An `Accept` that raises the promise names
    /// no segment, so it clears the belief.
    bound: Option<SegmentId>,
    /// The ranges whose votes it dropped, `start .. below` by start.
    dropped: BTreeMap<Slot, Slot>,
}

impl JournalAcceptor {
    /// The acceptor that starts from what its disk holds, and answers a
    /// `Prepare` with pages of at most `page` entries.
    ///
    /// # Panics
    ///
    /// When `page` is zero or above [`PROMISE_BATCH`], or the disk breaks
    /// the acceptor's invariants (a vote above the promise, a slot both
    /// readable and faulty).
    #[must_use]
    pub fn recover(id: NodeId, disk: AcceptorDisk, page: usize) -> Self {
        assert!(page > 0, "a promise page carries at least one entry");
        assert!(
            page <= PROMISE_BATCH,
            "a promise page is at most PROMISE_BATCH"
        );
        assert!(
            disk.votes
                .keys()
                .chain(disk.faulty.keys())
                .all(|slot| !dropped_at(&disk.dropped, *slot)),
            "a disk keeps no vote in a dropped range"
        );
        let acceptor = Acceptor::new(disk.promised, disk.votes, Slot(0), disk.faulty);
        let role = Self {
            id,
            acceptor,
            flight: Flight::after(disk.durable),
            page,
            bound: None,
            dropped: disk.dropped,
        };
        assert!(
            role.acceptor.promised() == disk.promised,
            "the acceptor starts at its durable promise"
        );
        assert!(
            role.flight.last() == disk.durable,
            "a reply with no write waits on the durable batch"
        );
        role
    }

    /// The acceptor's node.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The promise in memory: at or above the durable one.
    #[must_use]
    pub fn promised(&self) -> Ballot {
        self.acceptor.promised()
    }

    /// The acceptor's belief: the newest segment that a `Prepare` at or
    /// above its promise named since it started.
    #[must_use]
    pub fn bound(&self) -> Option<SegmentId> {
        self.bound
    }

    /// The ranges whose votes the acceptor dropped, `start .. below` by
    /// start.
    #[must_use]
    pub fn dropped(&self) -> &BTreeMap<Slot, Slot> {
        &self.dropped
    }

    /// The vote in memory at `slot`.
    #[must_use]
    pub fn vote(&self, slot: Slot) -> Option<&(Ballot, Command)> {
        self.acceptor.record(slot)
    }

    /// One step: a message from `from`. A message that is not for an
    /// acceptor changes nothing.
    ///
    /// # Panics
    ///
    /// When an `Accept` names a command other than the one the acceptor
    /// holds at the same slot and ballot: two commands at one slot and
    /// ballot break Paxos (a detected safety violation).
    pub fn step(&mut self, from: NodeId, message: Message) -> Vec<AcceptorEffect> {
        let promised = self.acceptor.promised();
        let effects = match message {
            Message::Prepare {
                ballot,
                from: slot,
                segment,
            } => self.prepare(from, ballot, slot, segment),
            Message::PreRead { segment, read } => self.pre_read(from, segment, read),
            Message::Accept {
                ballot,
                slot,
                command,
            } => self.accept(from, ballot, slot, command),
            Message::Compact {
                segment,
                start,
                below,
            } => self.compact(segment, start, below),
            Message::Repair {
                ballot,
                slot,
                command,
            } => self.repair(ballot, slot, command),
            _ => Vec::new(),
        };
        assert!(
            self.acceptor.promised() >= promised,
            "a step never lowers the promise"
        );
        assert!(
            effects
                .iter()
                .filter(|effect| matches!(effect, AcceptorEffect::Persist { .. }))
                .count()
                <= 1,
            "a step emits at most one batch"
        );
        effects
    }

    fn prepare(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        slot: Slot,
        segment: SegmentId,
    ) -> Vec<AcceptorEffect> {
        let mut ops = Vec::new();
        match self.acceptor.prepare(ballot, slot, &mut ops) {
            PrepareOutcome::Promised { raised } => {
                self.bound = self.bound.max(Some(segment));
                assert!(raised != ops.is_empty(), "a raised promise writes");
                probe!(sometimes, raised, "acceptor: a Prepare raises the promise");
                probe!(
                    sometimes,
                    !raised,
                    "acceptor: a Prepare at the promise writes nothing"
                );
                let mut effects = self.persist(ops);
                let page = self.acceptor.promise_page(slot, self.page);
                effects.push(AcceptorEffect::Send {
                    to: from,
                    message: Message::Promise {
                        ballot,
                        from: slot,
                        page: PromisePage {
                            votes: page.accepted,
                            faulty: page.faulty,
                            next: page.next_from_slot,
                            dropped: self.dropped.clone(),
                        },
                    },
                    after: self.flight.last(),
                });
                effects
            }
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => {
                assert!(ops.is_empty(), "a refusal writes nothing");
                probe!(
                    reachable,
                    "acceptor: a Prepare below the promise is refused"
                );
                vec![self.nack(from, ballot)]
            }
        }
    }

    fn accept(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: Command,
    ) -> Vec<AcceptorEffect> {
        if dropped_at(&self.dropped, slot) {
            probe!(
                reachable,
                "acceptor: an Accept in a dropped range is not answered"
            );
            return Vec::new();
        }
        match self.acceptor.admit(ballot, slot) {
            AcceptOutcome::Admitted => {}
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => {
                probe!(
                    reachable,
                    "acceptor: an Accept below the promise is refused"
                );
                return vec![self.nack(from, ballot)];
            }
        }
        // An admitted `Accept` is at or above the promise, and a held vote is
        // at or below it: a held vote at this ballot means the promise is it.
        let held = self.acceptor.record(slot) == Some(&(ballot, command.clone()));
        let mut effects = Vec::new();
        probe!(
            sometimes,
            held,
            "acceptor: an Accept it holds writes nothing"
        );
        probe!(
            sometimes,
            ballot > self.acceptor.promised(),
            "acceptor: an Accept raises the promise"
        );
        if ballot > self.acceptor.promised() {
            // A promise that a `Prepare` did not raise names no segment.
            self.bound = None;
        }
        if !held {
            // The promise first, then the vote, in one atomic batch.
            let mut ops = Vec::new();
            self.acceptor.set_promise(ballot, &mut ops);
            self.acceptor
                .record_accepted(slot, ballot, command, &mut ops);
            assert!(
                matches!(
                    ops.last(),
                    Some(AcceptorOp::Write(AcceptorWrite::AppendAccepted { .. }))
                ),
                "the vote is the last write of its batch"
            );
            effects = self.persist(ops);
        }
        effects.push(AcceptorEffect::Send {
            to: from,
            message: Message::Accepted { ballot, slot },
            after: self.flight.last(),
        });
        effects
    }

    /// The answer to a leaderless read: the vote watermark and the belief,
    /// once the last batch is durable. No answer with no belief.
    fn pre_read(&self, from: NodeId, segment: SegmentId, read: ReadTag) -> Vec<AcceptorEffect> {
        let Some(bound) = self.bound else {
            probe!(
                reachable,
                "acceptor: a read finds no belief and is not answered"
            );
            return Vec::new();
        };
        probe!(
            sometimes,
            bound > segment,
            "acceptor: a read meets a newer segment"
        );
        vec![AcceptorEffect::Send {
            to: from,
            message: Message::PreReadAck {
                segment,
                read,
                watermark: self.watermark(),
                bound,
            },
            after: self.flight.last(),
        }]
    }

    /// The highest slot the acceptor voted in, a dropped slot included.
    fn watermark(&self) -> Option<Slot> {
        let dropped = self.dropped.values().max().map(|below| Slot(below.0 - 1));
        probe!(
            sometimes,
            dropped > self.acceptor.vote_watermark(),
            "acceptor: a read watermark counts a dropped slot"
        );
        self.acceptor.vote_watermark().max(dropped)
    }

    /// Drop the votes of `start .. below`, in one batch after the batches
    /// before it. A range that is dropped already writes nothing.
    fn compact(&mut self, segment: SegmentId, start: Slot, below: Slot) -> Vec<AcceptorEffect> {
        if start >= below || self.dropped.get(&start).is_some_and(|held| *held >= below) {
            return Vec::new();
        }
        probe!(reachable, "acceptor: a compaction drops votes");
        drop_range(&mut self.dropped, start, below);
        let range = start..below;
        let records = self
            .acceptor
            .records()
            .iter()
            .filter(|(slot, _)| !range.contains(slot))
            .map(|(slot, vote)| (*slot, vote.clone()))
            .collect();
        let faulty = self
            .acceptor
            .faulty()
            .iter()
            .filter(|(slot, _)| !range.contains(slot))
            .map(|(slot, ballot)| (*slot, *ballot))
            .collect();
        let promised = self.acceptor.promised();
        self.acceptor = Acceptor::new(promised, records, Slot(0), faulty);
        assert!(
            self.acceptor.promised() == promised,
            "a compaction keeps the promise"
        );
        assert!(
            self.acceptor
                .records()
                .keys()
                .all(|slot| !range.contains(slot)),
            "a compaction keeps no vote in its range"
        );
        self.persist(vec![AcceptorOp::Compact {
            segment,
            start,
            below,
        }])
    }

    /// The CTRL repair of a faulty vote (§J5.3, `specs/tla/journal-ctrl/`,
    /// `AccRepair`): `command` was chosen at `ballot`, so the acceptor
    /// writes it at the higher of `ballot` and the ballot that the faulty
    /// vote keeps, with the promise raised to it, in one batch. A vote above
    /// `ballot` carries the chosen command (P2c), so the repaired vote is the
    /// one the disk lost; a vote below gives way to the vote at `ballot`. An
    /// acceptor whose vote at `slot` is readable, or in a dropped range,
    /// writes nothing.
    fn repair(&mut self, ballot: Ballot, slot: Slot, command: Command) -> Vec<AcceptorEffect> {
        let Some(faulty) = self.acceptor.faulty().get(&slot).copied() else {
            probe!(
                reachable,
                "acceptor: a Repair of a readable vote writes nothing"
            );
            return Vec::new();
        };
        assert!(
            !dropped_at(&self.dropped, slot),
            "a compaction keeps no faulty vote in its range"
        );
        probe!(
            sometimes,
            faulty > ballot,
            "acceptor: a Repair keeps the ballot of the faulty vote"
        );
        probe!(
            sometimes,
            faulty < ballot,
            "acceptor: a Repair writes at the choosing ballot"
        );
        let promised = self.acceptor.promised();
        let mut ops = Vec::new();
        let at = self.acceptor.learn_chosen(slot, ballot, &command, &mut ops);
        if at > promised {
            // A promise that a `Prepare` did not raise names no segment.
            self.bound = None;
        }
        assert!(
            at == faulty.max(ballot),
            "a repair writes at the higher ballot"
        );
        assert!(
            self.acceptor.record(slot) == Some(&(at, command)),
            "a repair holds the chosen command"
        );
        assert!(
            !self.acceptor.faulty().contains_key(&slot),
            "a repaired vote is readable"
        );
        self.persist(ops)
    }

    fn nack(&self, to: NodeId, ballot: Ballot) -> AcceptorEffect {
        AcceptorEffect::Send {
            to,
            message: Message::Nack {
                ballot,
                promised: self.acceptor.promised(),
            },
            after: self.flight.last(),
        }
    }

    /// The batch of `ops`, if any.
    fn persist(&mut self, ops: Vec<AcceptorOp>) -> Vec<AcceptorEffect> {
        if ops.is_empty() {
            return Vec::new();
        }
        let id = self.flight.batch();
        vec![AcceptorEffect::Persist { id, ops }]
    }
}

/// Whether `slot` is in one of the `dropped` ranges.
fn dropped_at(dropped: &BTreeMap<Slot, Slot>, slot: Slot) -> bool {
    dropped
        .range(..=slot)
        .next_back()
        .is_some_and(|(_, below)| slot < *below)
}

/// Add the range `start .. below` to `dropped`: a range of one segment only
/// grows.
fn drop_range(dropped: &mut BTreeMap<Slot, Slot>, start: Slot, below: Slot) {
    let held = dropped.entry(start).or_insert(below);
    *held = (*held).max(below);
}

#[cfg(test)]
mod tests;
