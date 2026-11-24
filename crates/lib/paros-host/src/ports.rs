//! The **`Events` port** and the **output ports** of a driver (§J4.2,
//! rule 5).
//!
//! [`Events`] records each step of the machine (its time, its input and
//! its effects) once every batch of the step is settled. The trace check
//! reads it, live and chunk by chunk, and so does a controller, for its
//! signals: a run replays from its inputs alone.
//!
//! [`Ports`] takes each output of a role ([`Effect::Output`]) once the batch
//! it names is durable: a reply to a client, a call to a register, a notice.
//! An output may give inputs back to the machine (the value of a done
//! register write), which the driver steps in the same round.

use crate::family::Family;
use crate::host::Effect;
use crate::machine::Machine;
use crate::time::Time;

/// One step of a machine: the input, its time and its effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event<I, F: Family> {
    /// The time the driver gave.
    pub now: Time,
    /// The input.
    pub input: I,
    /// The effects of the step.
    pub effects: Vec<Effect<F>>,
}

/// Where the driver reports each step.
///
/// The driver reports a step once the storage commit that holds its batches
/// returned (durable, or failed and so unknown): a reader never sees a step
/// whose write may still be undone by a later report.
pub trait Events<M: Machine> {
    /// Record one step.
    fn record(&mut self, event: Event<M::Input, M::Family>);
}

/// No reader: the steps are not kept.
impl<M: Machine> Events<M> for () {
    fn record(&mut self, _event: Event<M::Input, M::Family>) {}
}

/// Every step, in order.
impl<M: Machine> Events<M> for Vec<Event<M::Input, M::Family>> {
    fn record(&mut self, event: Event<M::Input, M::Family>) {
        self.push(event);
    }
}

/// Where the outputs of the roles go.
pub trait Ports<M: Machine> {
    /// The inputs that the outside gives the machine at each round, after
    /// the messages and the timer (a new register value).
    fn poll(&mut self, machine: &M) -> Vec<M::Input> {
        let _ = machine;
        Vec::new()
    }

    /// The output of the role at `address`, whose batch is durable. Returns
    /// the inputs it gives back to the machine.
    fn output(
        &mut self,
        address: <M::Family as Family>::Address,
        output: <M::Family as Family>::Output,
    ) -> Vec<M::Input>;

    /// An output of the role at `address` that waited on a batch, and that a
    /// stop of the role drops: its batch may be lost.
    fn dropped(
        &mut self,
        address: <M::Family as Family>::Address,
        output: <M::Family as Family>::Output,
    ) {
        let _ = (address, output);
    }

    /// The role at `address` stopped: an output that the ports keep for it
    /// (a call to run again) is not the output of a later role at the same
    /// address.
    fn stopped(&mut self, address: <M::Family as Family>::Address) {
        let _ = address;
    }
}

/// Every output, in order; none gives an input back.
impl<M: Machine> Ports<M>
    for Vec<(
        <M::Family as Family>::Address,
        <M::Family as Family>::Output,
    )>
{
    fn output(
        &mut self,
        address: <M::Family as Family>::Address,
        output: <M::Family as Family>::Output,
    ) -> Vec<M::Input> {
        self.push((address, output));
        Vec::new()
    }
}
