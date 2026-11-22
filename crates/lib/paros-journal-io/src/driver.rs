//! The **generic driver** (§J4.2): it drives one [`Host`] with a storage,
//! a transport, a clock, a register and an events port, and keeps the
//! flight rule.
//!
//! The driver is written against the traits of this crate only. It spawns
//! nothing, blocks on nothing and needs no `Send`: a program calls
//! [`Driver::poll`] in its own loop, on any runtime or none.
//!
//! **The flight rule** (rule 1). Each step of the `Host` may emit persist
//! batches and sends. The driver steps on at once: it keeps the batches in
//! one group and commits the whole group in one storage write at the end of
//! a [`poll`](Driver::poll) (group commit), in id order for each role. A
//! send, or a register change, that names a batch waits until that batch is
//! durable; one that names a durable batch, or none, goes at once. A
//! commit that fails stops every role that had a batch in it
//! ([`Input::Failed`], rule 4): the batch may or may not be durable, so the
//! sends that wait on it never go.
//!
//! **Recovery** (rule 2). A role with a disk starts with a read of its
//! records. The driver gives them to the `Host` as an input, and learns
//! from them the highest durable batch of the role, which a reply with no
//! write waits on.
//!
//! **Events** (rule 5). The driver reports each step of the `Host` to its
//! [`Events`] port once every batch of the step is settled: durable, or in
//! a commit that failed.
//!
//! A crash of the process is a drop of the driver: what was not committed
//! is lost, and so are the sends that waited on it. A new driver on the
//! same storage serves the roles again from their records.

use std::collections::{BTreeMap, VecDeque};

use paros_consensus::journal::JournalId;
use paros_journal::Command;
use paros_journal::SegmentId;
use paros_journal::register::{Change, RegisterValue};
use paros_journal::roles::{CopyId, PersistId};

use crate::clock::Clock;
use crate::events::{Event, Events};
use crate::host::{Assignment, CallId, CallOutcome, Effect, Host, HostConfig, Input, Stop};
use crate::layout::{self, Op};
use crate::register::Register;
use crate::storage::Storage;
use crate::transport::Transport;
use crate::wire::{Address, Envelope, Time};

/// The answer of a client call, with the leader that gave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallReply {
    /// The leader.
    pub address: Address,
    /// The call.
    pub call: CallId,
    /// The answer.
    pub outcome: CallOutcome,
}

/// What a role tells the outside of its journal through the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notice {
    /// The new replica at `address` holds a complete, durable copy `copy`
    /// of `segment`: the grid controller of the plan may write the new set
    /// (#1314).
    Copied {
        /// The new replica.
        address: Address,
        /// The segment.
        segment: SegmentId,
        /// The copy.
        copy: CopyId,
    },
    /// A done write retired `segment`: its acceptors that no later grid
    /// names may stop (§J3.4).
    Retired {
        /// The leader that saw the done write.
        address: Address,
        /// The segment.
        segment: SegmentId,
    },
}

/// A step of the `Host` that the driver has not reported yet, with the
/// batches it persisted.
struct Pending {
    event: Event,
    batches: Vec<(Address, PersistId)>,
}

/// The driver of one process.
pub struct Driver<S, T, C, R, E> {
    host: Host,
    storage: S,
    transport: T,
    clock: C,
    register: R,
    events: E,
    /// The inputs that wait for their step, in order.
    inputs: VecDeque<Input>,
    /// The batches of the next commit, in the order the roles emitted them.
    group: Vec<(Address, PersistId, Vec<Op>)>,
    /// The sends that wait on a batch of their sender.
    held: Vec<(PersistId, Envelope)>,
    /// The register changes that wait on a batch of their role.
    held_changes: Vec<(PersistId, Address, Change)>,
    /// The highest durable batch of each role that runs.
    durable: BTreeMap<Address, PersistId>,
    /// The highest settled batch of each role: durable, or failed.
    settled: BTreeMap<Address, PersistId>,
    /// The steps not reported yet.
    pending: VecDeque<Pending>,
    /// The value of the last done write that the `Host` observed.
    observed: BTreeMap<JournalId, RegisterValue>,
    timer: Option<Time>,
    calls: CallId,
    replies: Vec<CallReply>,
    notices: Vec<Notice>,
    stopped: Vec<(Address, Stop)>,
}

impl<S, T, C, R, E> Driver<S, T, C, R, E>
where
    S: Storage,
    T: Transport,
    C: Clock,
    R: Register,
    E: Events,
{
    /// A driver of a new [`Host`] with `config`.
    #[must_use]
    pub fn new(
        config: HostConfig,
        storage: S,
        transport: T,
        clock: C,
        register: R,
        events: E,
    ) -> Self {
        Self {
            host: Host::new(config),
            storage,
            transport,
            clock,
            register,
            events,
            inputs: VecDeque::new(),
            group: Vec::new(),
            held: Vec::new(),
            held_changes: Vec::new(),
            durable: BTreeMap::new(),
            settled: BTreeMap::new(),
            pending: VecDeque::new(),
            observed: BTreeMap::new(),
            timer: None,
            calls: 0,
            replies: Vec::new(),
            notices: Vec::new(),
            stopped: Vec::new(),
        }
    }

    /// The `Host`, to read its roles.
    #[must_use]
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The storage.
    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// The transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// The events port.
    #[must_use]
    pub fn events(&self) -> &E {
        &self.events
    }

    /// The time of the timer that the `Host` armed.
    #[must_use]
    pub fn timer(&self) -> Option<Time> {
        self.timer
    }

    /// The storage, the transport, the clock, the register and the events
    /// port. What was not committed is lost, as in a crash.
    pub fn into_parts(self) -> (S, T, C, R, E) {
        (
            self.storage,
            self.transport,
            self.clock,
            self.register,
            self.events,
        )
    }

    /// Serve the role that `assignment` names (the `Assignments` port,
    /// §J4.2, rule 5).
    pub fn assign(&mut self, assignment: Assignment) {
        self.watch(assignment.address().journal);
        self.input(Input::Assign(assignment));
    }

    /// Stop serving the role at `address`.
    pub fn unassign(&mut self, address: Address) {
        self.input(Input::Unassign(address));
    }

    /// Send `command` to the leader at `address`. The answer comes in
    /// [`take_replies`](Self::take_replies), with the id this returns. The
    /// ids of one driver grow from 1.
    pub fn call(&mut self, address: Address, command: Command) -> CallId {
        self.calls += 1;
        let call = self.calls;
        self.input(Input::Call {
            address,
            call,
            command,
        });
        call
    }

    /// The answers of client calls since the last take.
    pub fn take_replies(&mut self) -> Vec<CallReply> {
        std::mem::take(&mut self.replies)
    }

    /// What the roles told their outside since the last call: a new
    /// replica's complete copy, for the grid controller of its plan, and a
    /// done retire, for placement (the old acceptors may stop).
    pub fn take_notices(&mut self) -> Vec<Notice> {
        std::mem::take(&mut self.notices)
    }

    /// The roles that stopped since the last take, and why.
    pub fn take_stopped(&mut self) -> Vec<(Address, Stop)> {
        std::mem::take(&mut self.stopped)
    }

    /// One round of work: step every message that arrived, fire the timer
    /// when it is due, give the roles each new register value, then commit
    /// the batches of the round in one write and release what waited on
    /// them. Returns whether the round did any work.
    ///
    /// # Panics
    ///
    /// When the round ends with a batch to commit, or with a send or a
    /// register change that still waits on a batch: the flight rule of the
    /// driver is broken.
    pub fn poll(&mut self) -> bool {
        let mut busy = false;
        while let Some(envelope) = self.transport.receive() {
            busy = true;
            self.input(Input::Message(envelope));
        }
        if self.timer.is_some_and(|at| at <= self.clock.now()) {
            busy = true;
            self.timer = None;
            self.input(Input::TimerFired);
        }
        let journals: Vec<JournalId> = self.host.journals().collect();
        for journal in journals {
            busy |= self.watch(journal);
        }
        // A release can step new batches (a register value that a held
        // change gave): commit until the round leaves none.
        while self.commit() {
            busy = true;
        }
        assert!(
            self.group.is_empty(),
            "a round ends with no batch to commit"
        );
        assert!(self.held.is_empty(), "a round ends with no held send");
        assert!(
            self.held_changes.is_empty(),
            "a round ends with no held register change"
        );
        self.report();
        busy
    }

    /// Give the `Host` the register value of `journal` when it is new.
    fn watch(&mut self, journal: JournalId) -> bool {
        let Some(value) = self.register.read(journal) else {
            return false;
        };
        if self.observed.get(&journal) == Some(&value) {
            return false;
        }
        self.observed.insert(journal, value.clone());
        self.input(Input::Observe { journal, value });
        true
    }

    /// Step `input`, then every input that its effects give.
    fn input(&mut self, input: Input) {
        self.inputs.push_back(input);
        self.drain();
    }

    /// Step every input that waits, in order.
    fn drain(&mut self) {
        while let Some(input) = self.inputs.pop_front() {
            let now = self.clock.now();
            let effects = self.host.step(now, input.clone());
            let mut batches = Vec::new();
            for effect in &effects {
                if let Effect::Persist { address, id, .. } = effect {
                    batches.push((*address, *id));
                }
                self.apply(effect.clone());
            }
            self.pending.push_back(Pending {
                event: Event {
                    now,
                    input,
                    effects,
                },
                batches,
            });
        }
    }

    fn apply(&mut self, effect: Effect) {
        match effect {
            Effect::Read { address } => match self.storage.scan(address) {
                Ok(entries) => {
                    self.durable.remove(&address);
                    if let Some(id) = layout::durable_of(&entries) {
                        self.durable.insert(address, id);
                        self.settled.insert(address, id);
                    }
                    self.inputs.push_back(Input::Recovered { address, entries });
                }
                Err(_) => self.inputs.push_back(Input::Unreadable { address }),
            },
            Effect::Persist { address, id, ops } => {
                assert!(
                    self.group
                        .iter()
                        .rev()
                        .find(|(a, ..)| *a == address)
                        .is_none_or(|(_, last, _)| *last < id),
                    "the batches of a role are in id order"
                );
                self.group.push((address, id, ops));
            }
            Effect::Send { envelope, after } => {
                if self.is_durable(envelope.sender(), after) {
                    self.transport.send(envelope);
                } else {
                    let id = after.expect("a held send names a batch");
                    self.held.push((id, envelope));
                }
            }
            Effect::Register {
                address,
                change,
                after,
            } => {
                if self.is_durable(address, after) {
                    self.change(address, &change);
                } else {
                    let id = after.expect("a held change names a batch");
                    self.held_changes.push((id, address, change));
                }
            }
            Effect::Reply {
                address,
                call,
                outcome,
            } => self.replies.push(CallReply {
                address,
                call,
                outcome,
            }),
            Effect::SetTimer { at } => {
                self.timer = Some(self.timer.map_or(at, |armed| armed.min(at)));
            }
            Effect::Copied {
                address,
                segment,
                copy,
                after,
            } => {
                // A report of a copy whose last batch is not durable yet is
                // dropped: the replica reports again at its next tick.
                if self.is_durable(address, after) {
                    self.notices.push(Notice::Copied {
                        address,
                        segment,
                        copy,
                    });
                }
            }
            Effect::Retired { address, segment } => {
                self.notices.push(Notice::Retired { address, segment });
            }
            Effect::Stopped { address, why } => {
                // A stopped role writes and sends nothing more: its batches
                // leave the group, settled, and what waited on them is
                // dropped.
                let other = |role: Address| role != address;
                if let Some(id) = self
                    .group
                    .iter()
                    .rev()
                    .find_map(|(role, id, _)| (!other(*role)).then_some(*id))
                {
                    self.settled.insert(address, id);
                }
                self.group.retain(|(role, ..)| other(*role));
                self.held.retain(|(_, envelope)| other(envelope.sender()));
                self.held_changes.retain(|(_, role, _)| other(*role));
                self.durable.remove(&address);
                self.stopped.push((address, why));
            }
        }
    }

    fn is_durable(&self, address: Address, after: Option<PersistId>) -> bool {
        after.is_none_or(|id| self.durable.get(&address).is_some_and(|d| *d >= id))
    }

    /// Run `change` on the register, and give the `Host` its result.
    fn change(&mut self, address: Address, change: &Change) {
        match self.register.change(address.journal, change) {
            Ok(value) => {
                self.observed.insert(address.journal, value.clone());
                self.inputs.push_back(Input::Observe {
                    journal: address.journal,
                    value,
                });
            }
            Err(_) => self.inputs.push_back(Input::Refused { address }),
        }
    }

    /// Commit the group in one write, then release what waited on it.
    /// Returns whether there was a group.
    fn commit(&mut self) -> bool {
        if self.group.is_empty() {
            return false;
        }
        let group = std::mem::take(&mut self.group);
        let ops: Vec<Op> = group.iter().flat_map(|(_, _, ops)| ops.clone()).collect();
        let mut tops: BTreeMap<Address, PersistId> = BTreeMap::new();
        for (address, id, _) in &group {
            tops.insert(*address, *id);
        }
        for (address, id) in &tops {
            self.settled.insert(*address, *id);
        }
        if self.storage.commit(&ops, true).is_ok() {
            self.durable.extend(tops);
            self.release();
        } else {
            // Each role of the group stops, and its stop drops what waited
            // on the group.
            for address in tops.into_keys() {
                self.input(Input::Failed { address });
            }
        }
        true
    }

    /// Send what waited on a batch that is durable now.
    fn release(&mut self) {
        let held = std::mem::take(&mut self.held);
        for (id, envelope) in held {
            if self.is_durable(envelope.sender(), Some(id)) {
                self.transport.send(envelope);
            } else {
                self.held.push((id, envelope));
            }
        }
        let changes = std::mem::take(&mut self.held_changes);
        for (id, address, change) in changes {
            if self.is_durable(address, Some(id)) {
                self.change(address, &change);
            } else {
                self.held_changes.push((id, address, change));
            }
        }
        self.drain();
    }

    /// Report, in order, each step whose batches are all settled.
    fn report(&mut self) {
        while let Some(front) = self.pending.front() {
            let settled = front
                .batches
                .iter()
                .all(|(address, id)| self.settled.get(address).is_some_and(|s| s >= id));
            if !settled {
                break;
            }
            let pending = self.pending.pop_front().expect("a front");
            self.events.record(pending.event);
        }
    }
}

#[cfg(test)]
mod tests;
