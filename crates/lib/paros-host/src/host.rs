//! The **`Host`** (§J4.2, rule 3): the pure state machine that serves many
//! roles in one process.
//!
//! The `Host` maps each address to its role, starts a role that placement
//! assigns (with a read of its records first when it has a disk), routes
//! each input to its role, arms one timer for the periodic step of every
//! role, and stops a role. It does no I/O and reads no clock: the driver
//! gives the time with every input, so a recorded run replays (rule 2).
//!
//! The methods ([`assign`](Host::assign), [`deliver`](Host::deliver),
//! [`stop`](Host::stop), ...) are the blocks of a step: a host of a family
//! that routes some inputs itself (the journal's broadcast of a register
//! value) builds its own step from them and ends it with
//! [`settle`](Host::settle). [`Host::step`] is the step of a family that
//! needs nothing more ([`Input`]).

use std::collections::BTreeMap;

use crate::family::Family;
use crate::role::{Role, Start};
use crate::time::Time;

type Address<R> = <<R as Role>::Family as Family>::Address;
type Assignment<R> = <<R as Role>::Family as Family>::Assignment;
type Stop<R> = <<R as Role>::Family as Family>::Stop;

/// One input of a [`Host`] that routes nothing itself. Every input is
/// recorded (§J4.2, rule 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input<F: Family> {
    /// Serve a role (the `Assignments` port, rule 5).
    Assign(F::Assignment),
    /// Stop serving a role.
    Unassign(F::Address),
    /// The records of a role that starts, as [`Effect::Read`] asked.
    Recovered {
        /// The role.
        address: F::Address,
        /// Its records.
        records: F::Records,
    },
    /// The read of a role that starts failed.
    Unreadable {
        /// The role.
        address: F::Address,
    },
    /// A storage commit that held a batch of this role failed (rule 4).
    Failed {
        /// The role.
        address: F::Address,
    },
    /// A message arrived.
    Message(F::Envelope),
    /// An input from outside the host for the role at `address` (a client
    /// call).
    Deliver {
        /// The role.
        address: F::Address,
        /// The input.
        input: F::Input,
    },
    /// The timer that [`Effect::SetTimer`] armed fired.
    TimerFired,
}

/// One effect of a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect<F: Family> {
    /// Read every record of the role at `address`, and give them back as
    /// the input of a recovery.
    Read {
        /// The role.
        address: F::Address,
    },
    /// Make `ops` durable as batch `id` of the role at `address`. The ops
    /// are atomic with every batch that the same commit holds.
    Persist {
        /// The role.
        address: F::Address,
        /// The batch id of the role.
        id: F::Batch,
        /// The ops, in order.
        ops: Vec<F::Op>,
    },
    /// Send `envelope` once batch `after` of its sender is durable.
    Send {
        /// The role that sends.
        sender: F::Address,
        /// The message.
        envelope: F::Envelope,
        /// The batch of the sender it waits on; `None` waits on nothing.
        after: Option<F::Batch>,
    },
    /// Give `output` to the driver's ports once batch `after` of the role is
    /// durable.
    Output {
        /// The role.
        address: F::Address,
        /// The output.
        output: F::Output,
        /// The batch of the role it waits on; `None` waits on nothing.
        after: Option<F::Batch>,
    },
    /// Fire the timer at `at`.
    SetTimer {
        /// The time.
        at: Time,
    },
    /// The role at `address` stopped, and the host no longer serves it.
    Stopped {
        /// The role.
        address: F::Address,
        /// Why.
        why: F::Stop,
    },
}

/// A role that the `Host` serves.
enum Slot<R: Role> {
    /// The role waits for its records, to start as the assignment says.
    Recovering(Assignment<R>),
    /// The role runs.
    Running(R),
}

impl<R: Role + Clone> Clone for Slot<R> {
    fn clone(&self) -> Self {
        match self {
            Self::Recovering(assignment) => Self::Recovering(assignment.clone()),
            Self::Running(role) => Self::Running(role.clone()),
        }
    }
}

impl<R: Role + core::fmt::Debug> core::fmt::Debug for Slot<R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Recovering(assignment) => f.debug_tuple("Recovering").field(assignment).finish(),
            Self::Running(role) => f.debug_tuple("Running").field(role).finish(),
        }
    }
}

/// The roles of one process.
pub struct Host<R: Role> {
    /// The nanoseconds between two periodic steps.
    tick: u64,
    shared: R::Shared,
    roles: BTreeMap<Address<R>, Slot<R>>,
    /// Whether the timer is armed.
    armed: bool,
}

impl<R> Clone for Host<R>
where
    R: Role + Clone,
    R::Shared: Clone,
{
    fn clone(&self) -> Self {
        Self {
            tick: self.tick,
            shared: self.shared.clone(),
            roles: self.roles.clone(),
            armed: self.armed,
        }
    }
}

impl<R> core::fmt::Debug for Host<R>
where
    R: Role + core::fmt::Debug,
    R::Shared: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Host")
            .field("tick", &self.tick)
            .field("shared", &self.shared)
            .field("roles", &self.roles)
            .field("armed", &self.armed)
            .finish()
    }
}

impl<R: Role> Host<R> {
    /// A `Host` that serves no role yet, whose periodic step comes every
    /// `tick` nanoseconds.
    ///
    /// # Panics
    ///
    /// When `tick` is zero.
    #[must_use]
    pub fn new(tick: u64, shared: R::Shared) -> Self {
        assert!(tick > 0, "the timer moves time forward");
        Self {
            tick,
            shared,
            roles: BTreeMap::new(),
            armed: false,
        }
    }

    /// The state of the process that every role reads.
    #[must_use]
    pub fn shared(&self) -> &R::Shared {
        &self.shared
    }

    /// The state of the process that every role reads, to change it. Only
    /// a step changes it, so a recorded run replays.
    pub fn shared_mut(&mut self) -> &mut R::Shared {
        &mut self.shared
    }

    /// The roles that the `Host` serves, a role that still recovers too, in
    /// address order.
    pub fn addresses(&self) -> impl Iterator<Item = Address<R>> + '_ {
        self.roles.keys().copied()
    }

    /// Whether the `Host` serves the role at `address`.
    #[must_use]
    pub fn serves(&self, address: Address<R>) -> bool {
        self.roles.contains_key(&address)
    }

    /// The role at `address`, once it runs.
    #[must_use]
    pub fn role(&self, address: Address<R>) -> Option<&R> {
        match self.roles.get(&address) {
            Some(Slot::Running(role)) => Some(role),
            _ => None,
        }
    }

    /// Every role from `from` on, in address order, a role that still reads
    /// its records too (`None`).
    pub fn range(&self, from: Address<R>) -> impl Iterator<Item = (Address<R>, Option<&R>)> + '_ {
        self.roles.range(from..).map(|(address, slot)| match slot {
            Slot::Running(role) => (*address, Some(role)),
            Slot::Recovering(_) => (*address, None),
        })
    }

    /// The assignment of the role at `address` while it reads its records.
    #[must_use]
    pub fn pending(&self, address: Address<R>) -> Option<&Assignment<R>> {
        match self.roles.get(&address) {
            Some(Slot::Recovering(assignment)) => Some(assignment),
            _ => None,
        }
    }

    /// One step of a family that routes nothing itself: `input` at `now`.
    ///
    /// # Panics
    ///
    /// When a role breaks its own contract, or a postcondition of the step
    /// fails ([`settle`](Self::settle)).
    pub fn step(&mut self, now: Time, input: Input<R::Family>) -> Vec<Effect<R::Family>> {
        let mut effects = Vec::new();
        match input {
            Input::Assign(assignment) => self.assign(now, assignment, &mut effects),
            Input::Unassign(address) => {
                self.stop(address, <R::Family as Family>::unassigned(), &mut effects);
            }
            Input::Recovered { address, records } => {
                self.recovered(now, address, &records, &mut effects);
            }
            Input::Unreadable { address } => self.unreadable(address, &mut effects),
            Input::Failed { address } => {
                self.stop(address, <R::Family as Family>::failed(), &mut effects);
            }
            Input::Message(envelope) => {
                let (address, input) = <R::Family as Family>::route(envelope);
                self.deliver(now, address, input, &mut effects);
            }
            Input::Deliver { address, input } => {
                self.deliver(now, address, input, &mut effects);
            }
            Input::TimerFired => self.tick(now, &mut effects),
        }
        self.settle(&mut effects);
        effects
    }

    /// Serve the role that `assignment` names. An address that has a role
    /// already takes no second one: placement may say it again.
    pub fn assign(
        &mut self,
        now: Time,
        assignment: Assignment<R>,
        effects: &mut Vec<Effect<R::Family>>,
    ) {
        let address = <R::Family as Family>::address(&assignment);
        if self.roles.contains_key(&address) {
            return;
        }
        self.arm(now, effects);
        match R::start(&self.shared, now, assignment.clone()) {
            Start::Running(role, out) => {
                self.roles.insert(address, Slot::Running(role));
                effects.extend(out);
            }
            Start::Recover => {
                self.roles.insert(address, Slot::Recovering(assignment));
                effects.push(Effect::Read { address });
            }
            Start::Refused(why) => effects.push(Effect::Stopped { address, why }),
        }
    }

    /// The records of the role at `address`, which reads them to start. A
    /// read of a role that stopped, or that runs already, changes nothing.
    pub fn recovered(
        &mut self,
        now: Time,
        address: Address<R>,
        records: &<R::Family as Family>::Records,
        effects: &mut Vec<Effect<R::Family>>,
    ) {
        let Some(assignment) = self.pending(address).cloned() else {
            return;
        };
        match R::recover(&self.shared, now, address, assignment, records) {
            Ok((role, out)) => {
                self.roles.insert(address, Slot::Running(role));
                effects.extend(out);
            }
            Err(why) => self.stop(address, why, effects),
        }
    }

    /// The read of the role at `address` failed: a role that reads its
    /// records stops; any other role is not touched.
    pub fn unreadable(&mut self, address: Address<R>, effects: &mut Vec<Effect<R::Family>>) {
        if self.pending(address).is_some() {
            self.stop(address, <R::Family as Family>::unreadable(), effects);
        }
    }

    /// Step `input` on the role at `address`. Returns whether a role that
    /// runs took it: a role that reads its records, or that the process does
    /// not serve, loses the input, as a role that is down.
    pub fn deliver(
        &mut self,
        now: Time,
        address: Address<R>,
        input: <R::Family as Family>::Input,
        effects: &mut Vec<Effect<R::Family>>,
    ) -> bool {
        let Some(Slot::Running(role)) = self.roles.get_mut(&address) else {
            return false;
        };
        effects.extend(role.step(&self.shared, now, address, input));
        true
    }

    /// The periodic step of every role that runs, in address order, then
    /// the timer again.
    pub fn tick(&mut self, now: Time, effects: &mut Vec<Effect<R::Family>>) {
        for (address, slot) in &mut self.roles {
            if let Slot::Running(role) = slot {
                effects.extend(role.tick(&self.shared, now, *address));
            }
        }
        self.armed = true;
        effects.push(Effect::SetTimer {
            at: now.after(self.tick),
        });
    }

    /// Arm the timer when it is not armed.
    pub fn arm(&mut self, now: Time, effects: &mut Vec<Effect<R::Family>>) {
        if !self.armed {
            self.armed = true;
            effects.push(Effect::SetTimer {
                at: now.after(self.tick),
            });
        }
    }

    /// Stop the role at `address` for `why`: its last effects, then
    /// [`Effect::Stopped`]. An address with no role changes nothing.
    ///
    /// # Panics
    ///
    /// When the role is still served after the stop.
    pub fn stop(
        &mut self,
        address: Address<R>,
        why: Stop<R>,
        effects: &mut Vec<Effect<R::Family>>,
    ) {
        let Some(slot) = self.roles.remove(&address) else {
            return;
        };
        if let Slot::Running(mut role) = slot {
            effects.extend(role.stop(address, &why));
        }
        effects.push(Effect::Stopped { address, why });
        assert!(!self.roles.contains_key(&address), "a stopped role is gone");
    }

    /// The end of every step: stop each role that ended by its own step
    /// ([`Role::done`]), then check that only a role that the `Host` serves
    /// sends.
    ///
    /// # Panics
    ///
    /// When a send of `effects` comes from a role that the `Host` does not
    /// serve.
    pub fn settle(&mut self, effects: &mut Vec<Effect<R::Family>>) {
        let done: Vec<(Address<R>, Stop<R>)> = self
            .roles
            .iter()
            .filter_map(|(address, slot)| match slot {
                Slot::Running(role) => role.done().map(|why| (*address, why)),
                Slot::Recovering(_) => None,
            })
            .collect();
        for (address, why) in done {
            self.stop(address, why, effects);
        }
        assert!(
            effects.iter().all(|effect| match effect {
                Effect::Send { sender, .. } => self.roles.contains_key(sender),
                _ => true,
            }),
            "only a role that the Host serves sends"
        );
    }
}

#[cfg(test)]
mod tests;
