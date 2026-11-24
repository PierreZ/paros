//! The **`Machine` trait**: what the [`Driver`](crate::Driver) drives.
//!
//! A machine is a pure step, `input` at `now` to effects, plus the inputs
//! that the driver itself builds: a recovery, a failed read, a failed
//! commit, a lost record, a message, the timer of a role and a random draw.
//! [`Host`] is a machine with the inputs of [`Input`]. A family that routes some inputs itself (the journal) wraps a
//! `Host` in a machine of its own, with its own inputs.

use core::fmt::Debug;

use crate::family::Family;
use crate::host::{Effect, Host, Input};
use crate::role::Role;
use crate::time::Time;

/// A pure state machine that a driver drives.
pub trait Machine {
    /// The types of the roles that the machine serves.
    type Family: Family;
    /// One input of the machine. Every input is recorded (§J4.2, rule 2).
    type Input: Clone + Debug + Eq;

    /// One step: `input` at `now`.
    fn step(&mut self, now: Time, input: Self::Input) -> Vec<Effect<Self::Family>>;

    /// The input that gives the role at `address` its records.
    fn recovered(
        address: <Self::Family as Family>::Address,
        records: <Self::Family as Family>::Records,
    ) -> Self::Input;

    /// The input that says the read of the role at `address` failed.
    fn unreadable(address: <Self::Family as Family>::Address) -> Self::Input;

    /// The input that says a commit with a batch of the role at `address`
    /// wrote nothing.
    fn failed(address: <Self::Family as Family>::Address) -> Self::Input;

    /// The input that says the disk lost a record of the role at `address`:
    /// the role stops for good.
    fn lost(address: <Self::Family as Family>::Address) -> Self::Input;

    /// The input of a message that arrived.
    fn message(envelope: <Self::Family as Family>::Envelope) -> Self::Input;

    /// The input that says the timer of the role at `address` is due.
    fn timer_fired(address: <Self::Family as Family>::Address) -> Self::Input;

    /// The input that gives the role at `address` the random value it asked
    /// for.
    fn drawn(address: <Self::Family as Family>::Address, value: u64) -> Self::Input;
}

impl<R: Role> Machine for Host<R> {
    type Family = R::Family;
    type Input = Input<R::Family>;

    fn step(&mut self, now: Time, input: Self::Input) -> Vec<Effect<R::Family>> {
        Host::step(self, now, input)
    }

    fn recovered(
        address: <R::Family as Family>::Address,
        records: <R::Family as Family>::Records,
    ) -> Self::Input {
        Input::Recovered { address, records }
    }

    fn unreadable(address: <R::Family as Family>::Address) -> Self::Input {
        Input::Unreadable { address }
    }

    fn failed(address: <R::Family as Family>::Address) -> Self::Input {
        Input::Failed { address }
    }

    fn lost(address: <R::Family as Family>::Address) -> Self::Input {
        Input::Lost { address }
    }

    fn message(envelope: <R::Family as Family>::Envelope) -> Self::Input {
        Input::Message(envelope)
    }

    fn timer_fired(address: <R::Family as Family>::Address) -> Self::Input {
        Input::TimerFired { address }
    }

    fn drawn(address: <R::Family as Family>::Address, value: u64) -> Self::Input {
        Input::Drawn { address, value }
    }
}
