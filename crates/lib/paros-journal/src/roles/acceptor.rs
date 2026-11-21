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

use std::collections::BTreeMap;

use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::contract::limits::PROMISE_BATCH;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::{Ballot, NodeId, Slot};

use super::message::{Message, PromisePage};
use super::{Flight, PersistId};
use crate::command::Command;

/// One write of an acceptor's persist batch.
pub type AcceptorOp = AcceptorWrite<Command>;

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
                AcceptorWrite::SetPromise(ballot) => {
                    assert!(*ballot >= self.promised, "a promise never falls");
                    self.promised = *ballot;
                }
                AcceptorWrite::AppendAccepted {
                    slot,
                    ballot,
                    value,
                } => {
                    assert!(
                        *ballot <= self.promised,
                        "a vote is durable only under its promise"
                    );
                    self.faulty.remove(slot);
                    self.votes.insert(*slot, (*ballot, value.clone()));
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
        let acceptor = Acceptor::new(disk.promised, disk.votes, Slot(0), disk.faulty);
        let role = Self {
            id,
            acceptor,
            flight: Flight::after(disk.durable),
            page,
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
            Message::Prepare { ballot, from: slot } => self.prepare(from, ballot, slot),
            Message::Accept {
                ballot,
                slot,
                command,
            } => self.accept(from, ballot, slot, command),
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

    fn prepare(&mut self, from: NodeId, ballot: Ballot, slot: Slot) -> Vec<AcceptorEffect> {
        let mut ops = Vec::new();
        match self.acceptor.prepare(ballot, slot, &mut ops) {
            PrepareOutcome::Promised { raised } => {
                assert!(raised != ops.is_empty(), "a raised promise writes");
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
                        },
                    },
                    after: self.flight.last(),
                });
                effects
            }
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => {
                assert!(ops.is_empty(), "a refusal writes nothing");
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
        match self.acceptor.admit(ballot, slot) {
            AcceptOutcome::Admitted => {}
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => {
                return vec![self.nack(from, ballot)];
            }
        }
        // An admitted `Accept` is at or above the promise, and a held vote is
        // at or below it: a held vote at this ballot means the promise is it.
        let held = self.acceptor.record(slot) == Some(&(ballot, command.clone()));
        let mut effects = Vec::new();
        if !held {
            // The promise first, then the vote, in one atomic batch.
            let mut ops = Vec::new();
            self.acceptor.set_promise(ballot, &mut ops);
            self.acceptor
                .record_accepted(slot, ballot, command, &mut ops);
            assert!(
                matches!(ops.last(), Some(AcceptorWrite::AppendAccepted { .. })),
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

#[cfg(test)]
mod tests;
