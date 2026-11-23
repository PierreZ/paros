//! The **clock trait**: the time that the driver gives with every input
//! (§J4.2, rule 2). The [`Host`](crate::Host) reads no clock.
//!
//! It is the clock of the generic host ([`paros_host::Clock`]): one clock
//! serves every role of a process. The contract, which
//! [`crate::contract::clock`] checks: the time never goes back.

pub use paros_host::Clock;
