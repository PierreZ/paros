//! The **`Events` port** (§J4.2, rule 5): the recorded input of each step
//! of the [`Host`](crate::Host), with the time and the effects of the step.
//!
//! The trace check reads it, live and chunk by chunk (moonpool-tla `Live`,
//! §J7.3, #1310 (check long runs live)), and so does the grid controller,
//! for its signals. A run replays from its inputs alone. The port is the
//! generic host's ([`paros_host::Events`]); the driver reports a step once
//! the storage commit that holds its batches returned.

/// One step of the [`Host`](crate::Host): the input, its time and its
/// effects.
pub type Event = paros_host::Event<crate::host::Input, crate::family::Journal>;

pub use paros_host::Events;
