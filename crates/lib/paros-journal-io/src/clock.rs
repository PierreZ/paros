//! The **clock trait**: the time that the driver gives with every input
//! (§J4.2, rule 2). The [`Host`](crate::Host) reads no clock.

use crate::wire::Time;

/// A clock of one process.
///
/// The contract, which [`crate::contract::clock`] checks: the time never
/// goes back.
pub trait Clock {
    /// The time now.
    fn now(&self) -> Time;
}
