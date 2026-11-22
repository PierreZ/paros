//! The **`Events` port** (§J4.2, rule 5): the recorded input of each step
//! of the [`Host`](crate::Host), with the time and the effects of the step.
//!
//! The trace check reads it, live and chunk by chunk (moonpool-tla `Live`,
//! §J7.3, #1310 (check long runs live)), and so does the grid controller,
//! for its signals. A run replays from its inputs alone.

use crate::host::{Effect, Input};
use crate::wire::Time;

/// One step of the [`Host`](crate::Host): the input, its time and its
/// effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// The time the driver gave.
    pub now: Time,
    /// The input.
    pub input: Input,
    /// The effects of the step.
    pub effects: Vec<Effect>,
}

/// Where the driver reports each step.
///
/// The driver reports a step once the storage commit that holds its batches
/// returned (durable, or failed and so unknown): a reader never sees a step
/// whose write may still be undone by a later report.
pub trait Events {
    /// Record one step.
    fn record(&mut self, event: Event);
}

/// No reader: the steps are not kept.
impl Events for () {
    fn record(&mut self, _event: Event) {}
}

/// Every step, in order.
impl Events for Vec<Event> {
    fn record(&mut self, event: Event) {
        self.push(event);
    }
}
