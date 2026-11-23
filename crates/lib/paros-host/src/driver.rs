//! The **generic driver** (§J4.2): it drives one [`Machine`] with a
//! storage, a transport, a clock, the output ports and an events port, and
//! keeps the flight rule.
//!
//! The driver is written against the traits of this crate only. It spawns
//! nothing, blocks on nothing and needs no `Send`: a program calls
//! [`Driver::poll`] in its own loop, on any runtime or none.
//!
//! **The flight rule** (rule 1). Each step of the machine may emit persist
//! batches, sends and outputs. The driver steps on at once: it keeps the
//! batches in one group and commits the whole group in one storage write at
//! the end of a [`poll`](Driver::poll) (group commit), in id order for each
//! role. A send, or an output, that names a batch waits until that batch is
//! durable; one that names a durable batch, or none, goes at once. A commit
//! that fails stops every role that had a batch in it
//! ([`Machine::failed`], rule 4): the batch may or may not be durable, so
//! what waits on it never goes.
//!
//! **Recovery** (rule 2). A role with a disk starts with a read of its
//! records ([`Effect::Read`]). The driver gives them to the machine as an
//! input, and learns from them the highest durable batch of the role, which
//! a send with no write waits on.
//!
//! **Events** (rule 5). The driver reports each step of the machine to its
//! [`Events`] port once every batch of the step is settled: durable, or in a
//! commit that failed.
//!
//! A crash of the process is a drop of the driver: what was not committed
//! is lost, and so are the sends that waited on it. A new driver on the
//! same storage serves the roles again from their records.

use std::collections::{BTreeMap, VecDeque};

use crate::family::Family;
use crate::host::Effect;
use crate::io::{Clock, Entropy, NoDraws, Storage, Transport};
use crate::machine::Machine;
use crate::ports::{Event, Events, Ports};
use crate::time::Time;

type Fam<M> = <M as Machine>::Family;
type Address<M> = <Fam<M> as Family>::Address;
type Batch<M> = <Fam<M> as Family>::Batch;
type Op<M> = <Fam<M> as Family>::Op;
type Envelope<M> = <Fam<M> as Family>::Envelope;
type Output<M> = <Fam<M> as Family>::Output;
type Stop<M> = <Fam<M> as Family>::Stop;
type Records<M> = <Fam<M> as Family>::Records;
/// One persist batch in the group: its role, its id and its ops.
type Grouped<M> = (Address<M>, Batch<M>, Vec<Op<M>>);

/// A step of the machine that the driver has not reported yet, with the
/// batches it persisted.
struct Pending<M: Machine> {
    event: Event<M::Input, M::Family>,
    batches: Vec<(Address<M>, Batch<M>)>,
}

/// The driver of one process.
pub struct Driver<M: Machine, S, T, C, P, E, D = NoDraws> {
    machine: M,
    storage: S,
    transport: T,
    clock: C,
    entropy: D,
    ports: P,
    events: E,
    /// The inputs that wait for their step, in order.
    inputs: VecDeque<M::Input>,
    /// The batches of the next commit, in the order the roles emitted them.
    group: Vec<Grouped<M>>,
    /// The sends that wait on a batch of their sender.
    held: Vec<(Batch<M>, Address<M>, Envelope<M>)>,
    /// The outputs that wait on a batch of their role.
    held_outputs: Vec<(Batch<M>, Address<M>, Output<M>)>,
    /// The highest durable batch of each role that runs.
    durable: BTreeMap<Address<M>, Batch<M>>,
    /// The highest settled batch of each role: durable, or failed.
    settled: BTreeMap<Address<M>, Batch<M>>,
    /// The steps not reported yet.
    pending: VecDeque<Pending<M>>,
    /// The deadline of the timer of each role that set one.
    timers: BTreeMap<Address<M>, Time>,
    stopped: Vec<(Address<M>, Stop<M>)>,
}

impl<M, S, T, C, P, E> Driver<M, S, T, C, P, E>
where
    M: Machine,
    S: Storage<M::Family>,
    T: Transport<M::Family>,
    C: Clock,
    P: Ports<M>,
    E: Events<M>,
{
    /// A driver of `machine`, whose roles ask no random draw
    /// ([`with_entropy`](Self::with_entropy) gives a random source).
    #[must_use]
    pub fn new(machine: M, storage: S, transport: T, clock: C, ports: P, events: E) -> Self {
        Self {
            machine,
            storage,
            transport,
            clock,
            entropy: NoDraws,
            ports,
            events,
            inputs: VecDeque::new(),
            group: Vec::new(),
            held: Vec::new(),
            held_outputs: Vec::new(),
            durable: BTreeMap::new(),
            settled: BTreeMap::new(),
            pending: VecDeque::new(),
            timers: BTreeMap::new(),
            stopped: Vec::new(),
        }
    }

    /// The same driver, which draws each random value that a role asks for
    /// from `entropy`.
    ///
    /// # Panics
    ///
    /// When a step already ran: the random source is part of the driver
    /// from its first step.
    #[must_use]
    pub fn with_entropy<D: Entropy>(self, entropy: D) -> Driver<M, S, T, C, P, E, D> {
        assert!(
            self.pending.is_empty() && self.inputs.is_empty() && self.timers.is_empty(),
            "the random source comes before the first step"
        );
        Driver {
            machine: self.machine,
            storage: self.storage,
            transport: self.transport,
            clock: self.clock,
            entropy,
            ports: self.ports,
            events: self.events,
            inputs: self.inputs,
            group: self.group,
            held: self.held,
            held_outputs: self.held_outputs,
            durable: self.durable,
            settled: self.settled,
            pending: self.pending,
            timers: self.timers,
            stopped: self.stopped,
        }
    }
}

impl<M, S, T, C, P, E, D> Driver<M, S, T, C, P, E, D>
where
    M: Machine,
    S: Storage<M::Family>,
    T: Transport<M::Family>,
    C: Clock,
    P: Ports<M>,
    E: Events<M>,
    D: Entropy,
{
    /// The machine, to read its roles.
    #[must_use]
    pub fn machine(&self) -> &M {
        &self.machine
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

    /// The clock.
    #[must_use]
    pub fn clock(&self) -> &C {
        &self.clock
    }

    /// The output ports.
    #[must_use]
    pub fn ports(&self) -> &P {
        &self.ports
    }

    /// The output ports, to take what they hold.
    pub fn ports_mut(&mut self) -> &mut P {
        &mut self.ports
    }

    /// The events port.
    #[must_use]
    pub fn events(&self) -> &E {
        &self.events
    }

    /// The earliest deadline of the timers that the roles set: when the
    /// program should poll again.
    #[must_use]
    pub fn timer(&self) -> Option<Time> {
        self.timers.values().min().copied()
    }

    /// The random source.
    #[must_use]
    pub fn entropy(&self) -> &D {
        &self.entropy
    }

    /// The storage, the transport, the clock, the ports and the events port.
    /// What was not committed is lost, as in a crash.
    pub fn into_parts(self) -> (S, T, C, P, E) {
        (
            self.storage,
            self.transport,
            self.clock,
            self.ports,
            self.events,
        )
    }

    /// The roles that stopped since the last take, and why.
    pub fn take_stopped(&mut self) -> Vec<(Address<M>, Stop<M>)> {
        std::mem::take(&mut self.stopped)
    }

    /// Step `input`, then every input that its effects give.
    pub fn input(&mut self, input: M::Input) {
        self.inputs.push_back(input);
        self.drain();
    }

    /// One round of work: step every message that arrived, fire each timer
    /// that is due to its role alone (in deadline order, then address
    /// order), step the inputs of the ports, then commit the batches
    /// of the round in one write and release what waited on them. Returns
    /// whether the round did any work.
    ///
    /// # Panics
    ///
    /// When the round ends with a batch to commit, or with a send or an
    /// output that still waits on a batch: the flight rule of the driver is
    /// broken.
    pub fn poll(&mut self) -> bool {
        let mut busy = false;
        while let Some(envelope) = self.transport.receive() {
            busy = true;
            self.input(M::message(envelope));
        }
        let now = self.clock.now();
        let mut due: Vec<(Time, Address<M>)> = self
            .timers
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(address, at)| (*at, *address))
            .collect();
        due.sort();
        for (_, address) in due {
            busy = true;
            self.timers.remove(&address);
            self.input(M::timer_fired(address));
        }
        for input in self.ports.poll(&self.machine) {
            busy = true;
            self.input(input);
        }
        // A release can step new batches (an input that a held output gave):
        // commit until the round leaves none.
        while self.commit() {
            busy = true;
        }
        assert!(
            self.group.is_empty(),
            "a round ends with no batch to commit"
        );
        assert!(self.held.is_empty(), "a round ends with no held send");
        assert!(
            self.held_outputs.is_empty(),
            "a round ends with no held output"
        );
        self.report();
        busy
    }

    /// Step every input that waits, in order.
    fn drain(&mut self) {
        while let Some(input) = self.inputs.pop_front() {
            let now = self.clock.now();
            let effects = self.machine.step(now, input.clone());
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

    fn apply(&mut self, effect: Effect<M::Family>) {
        match effect {
            Effect::Read { address } => match self.storage.scan(address) {
                Ok(records) => {
                    self.durable.remove(&address);
                    if let Some(id) = <Fam<M> as Family>::durable(&records) {
                        self.durable.insert(address, id);
                        self.settled.insert(address, id);
                    }
                    let records = self.with_group(address, records);
                    self.inputs.push_back(M::recovered(address, records));
                }
                Err(_) => self.inputs.push_back(M::unreadable(address)),
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
            Effect::Send {
                sender,
                envelope,
                after,
            } => {
                if self.is_durable(sender, after) {
                    self.transport.send(envelope);
                } else {
                    let id = after.expect("a held send names a batch");
                    self.held.push((id, sender, envelope));
                }
            }
            Effect::Output {
                address,
                output,
                after,
            } => {
                if self.is_durable(address, after) {
                    let inputs = self.ports.output(address, output);
                    self.inputs.extend(inputs);
                } else if <Fam<M> as Family>::holds(&output) {
                    let id = after.expect("a held output names a batch");
                    self.held_outputs.push((id, address, output));
                }
            }
            Effect::SetTimer { address, at } => {
                self.timers.insert(address, at);
            }
            Effect::Draw { address } => {
                let value = self.entropy.draw();
                self.inputs.push_back(M::drawn(address, value));
            }
            Effect::Stopped { address, why } => self.on_stopped(address, why),
        }
    }

    /// The records of the role at `address` as the disk will hold them
    /// once the group commits: `records`, with the ops of the role's
    /// batches that the group still holds on top. A role that starts again
    /// before the commit reads its own last writes, and its new batches
    /// follow the last one.
    fn with_group(&self, address: Address<M>, records: Records<M>) -> Records<M> {
        let ops: Vec<Op<M>> = self
            .group
            .iter()
            .filter(|(role, ..)| *role == address)
            .flat_map(|(_, _, ops)| ops.iter().cloned())
            .collect();
        <Fam<M> as Family>::overlay(records, &ops)
    }

    fn is_durable(&self, address: Address<M>, after: Option<Batch<M>>) -> bool {
        after.is_none_or(|id| self.durable.get(&address).is_some_and(|d| *d >= id))
    }

    /// Commit the group in one write, then release what waited on it.
    /// Returns whether there was a group.
    fn commit(&mut self) -> bool {
        if self.group.is_empty() {
            return false;
        }
        let group = std::mem::take(&mut self.group);
        let ops: Vec<Op<M>> = group.iter().flat_map(|(_, _, ops)| ops.clone()).collect();
        let mut tops: BTreeMap<Address<M>, Batch<M>> = BTreeMap::new();
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
                self.input(M::failed(address));
            }
        }
        true
    }

    /// The role at `address` stopped: what waited on its batches is
    /// dropped.
    fn on_stopped(&mut self, address: Address<M>, why: Stop<M>) {
        // A stopped role writes and sends nothing more: what waited on its
        // batches is dropped. A role that ended by its own step keeps its
        // last batches in the group, so they reach the disk; any other stop
        // takes them out of the group, settled.
        let other = |role: Address<M>| role != address;
        if !<Fam<M> as Family>::keeps_batches(&why) {
            if let Some(id) = self
                .group
                .iter()
                .rev()
                .find_map(|(role, id, _)| (!other(*role)).then_some(*id))
            {
                self.settled.insert(address, id);
            }
            self.group.retain(|(role, ..)| other(*role));
        }
        self.held.retain(|(_, sender, _)| other(*sender));
        let (stopped, kept) = std::mem::take(&mut self.held_outputs)
            .into_iter()
            .partition(|(_, role, _)| !other(*role));
        self.held_outputs = kept;
        for (_, role, output) in stopped {
            self.ports.dropped(role, output);
        }
        self.durable.remove(&address);
        self.timers.remove(&address);
        self.stopped.push((address, why));
    }

    /// Send what waited on a batch that is durable now.
    fn release(&mut self) {
        let held = std::mem::take(&mut self.held);
        for (id, sender, envelope) in held {
            if self.is_durable(sender, Some(id)) {
                self.transport.send(envelope);
            } else {
                self.held.push((id, sender, envelope));
            }
        }
        let outputs = std::mem::take(&mut self.held_outputs);
        for (id, address, output) in outputs {
            if self.is_durable(address, Some(id)) {
                let inputs = self.ports.output(address, output);
                self.inputs.extend(inputs);
            } else {
                self.held_outputs.push((id, address, output));
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
