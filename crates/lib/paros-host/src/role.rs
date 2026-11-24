//! The **`Role` trait**: one actor of a host, a pure step under the flight
//! rule (§J4.2, rule 1; `docs/architecture/04-roles.md` §4.4, "A role is an
//! actor").
//!
//! A role reads no clock, draws no random value and does no I/O. Each call
//! takes the time and one input, and returns the role's effects
//! ([`Effect`]): a persist batch with its id, a send that names the batch
//! it waits on, an output that names the batch it waits on, the deadline of
//! the role's one timer, and an ask for a random value. A role with a disk starts from its records
//! ([`Role::recover`]); a role with no disk starts at once
//! ([`Start::Running`]).
//!
//! One type implements `Role` for every kind of role that a host serves (an
//! enum of the kinds): the host keeps one map from address to role.

use crate::family::Family;
use crate::host::Effect;
use crate::time::Time;

/// A started role and the effects of its start, or the reason it stops.
pub type Started<R> =
    Result<(R, Vec<Effect<<R as Role>::Family>>), <<R as Role>::Family as Family>::Stop>;

/// How a role starts when placement assigns it.
#[derive(Debug)]
pub enum Start<R: Role> {
    /// The role runs now, with the effects of its start.
    Running(R, Vec<Effect<R::Family>>),
    /// The role reads its records first ([`Effect::Read`]); it runs from
    /// [`Role::recover`].
    Recover,
    /// The role does not start, for this reason.
    Refused(<R::Family as Family>::Stop),
}

/// One role of a host.
///
/// `Shared` is the state of the process that every role reads and no role
/// writes (a value that a done write gave, the settings of the host). The
/// code above the host writes it, through [`crate::Host::shared_mut`].
pub trait Role: Sized {
    /// The types that every role of the host shares.
    type Family: Family;
    /// The state of the process that every role reads.
    type Shared;

    /// Start the role that `assignment` names, at `now`.
    fn start(
        shared: &Self::Shared,
        now: Time,
        assignment: <Self::Family as Family>::Assignment,
    ) -> Start<Self>;

    /// Start the role that `assignment` names at `address` from its records,
    /// once [`Start::Recover`] asked for them.
    ///
    /// # Errors
    ///
    /// The records do not let the role start (they are damaged): the role
    /// stops for the reason returned.
    fn recover(
        shared: &Self::Shared,
        now: Time,
        address: <Self::Family as Family>::Address,
        assignment: <Self::Family as Family>::Assignment,
        records: &<Self::Family as Family>::Records,
    ) -> Started<Self>;

    /// One input of the role at `address`, at `now`.
    fn step(
        &mut self,
        shared: &Self::Shared,
        now: Time,
        address: <Self::Family as Family>::Address,
        input: <Self::Family as Family>::Input,
    ) -> Vec<Effect<Self::Family>>;

    /// The timer that the role at `address` set ([`Effect::SetTimer`]) is
    /// due, at `now`. The timer is gone: a role that wants another sets it.
    fn fire(
        &mut self,
        shared: &Self::Shared,
        now: Time,
        address: <Self::Family as Family>::Address,
    ) -> Vec<Effect<Self::Family>>;

    /// The random value that the role at `address` asked for
    /// ([`Effect::Draw`]), at `now`. A role draws no random value itself: the
    /// driver draws it and the value is a recorded input (§J4.2, rule 2).
    ///
    /// # Panics
    ///
    /// The default panics: a role that asks no draw gets none.
    fn drawn(
        &mut self,
        shared: &Self::Shared,
        now: Time,
        address: <Self::Family as Family>::Address,
        value: u64,
    ) -> Vec<Effect<Self::Family>> {
        let _ = (shared, now, address, value);
        unreachable!("a role that asks no draw gets none")
    }

    /// The role at `address` stops for `why`: the effects of its end (the
    /// answers of the calls it still holds).
    fn stop(
        &mut self,
        address: <Self::Family as Family>::Address,
        why: &<Self::Family as Family>::Stop,
    ) -> Vec<Effect<Self::Family>>;

    /// The reason the role ended by its own step, if it did: the host stops
    /// it after the step.
    fn done(&self) -> Option<<Self::Family as Family>::Stop> {
        None
    }
}
