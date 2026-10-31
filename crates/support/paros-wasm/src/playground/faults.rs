//! The faults of the action log: `crash`, `restart`, `isolate` and `heal`,
//! played by moonpool's own fault injector at their simulated times. The
//! machines and the client do not know a fault is coming: the code they run
//! is the shipped code, and only the simulated world around it changes.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider};

use crate::playground::machine::machine_ip;
use crate::playground::model::{Action, Op};

/// The machines that faults hold down or cut off now, shared with the
/// operator, which reports them after each step.
#[derive(Debug, Default)]
pub(crate) struct FaultState {
    /// Crashed and not restarted, by machine index.
    pub(crate) down: BTreeSet<usize>,
    /// Isolated and not healed, by machine index.
    pub(crate) isolated: BTreeSet<usize>,
    /// The client's IP, which an `isolate` cuts too. The operator sets it
    /// when it starts.
    pub(crate) client_ip: Option<String>,
    /// The fault step the operator reached and waits on, by its index in
    /// the log. The injector plays it on its next beat and clears it.
    pub(crate) wanted: Option<usize>,
}

/// The shared [`FaultState`].
pub(crate) type Faults = Arc<Mutex<FaultState>>;

/// Plays the fault steps of the log.
pub(crate) struct Injector {
    machines: usize,
    actions: Vec<Action>,
    faults: Faults,
}

impl Injector {
    pub(crate) fn new(machines: usize, actions: Vec<Action>, faults: Faults) -> Self {
        Self {
            machines,
            actions,
            faults,
        }
    }

    /// The machine's IP and its peers': every other machine and the client.
    fn peers(&self, machine: usize) -> Vec<String> {
        let mut peers: Vec<String> = (0..self.machines)
            .filter(|m| *m != machine)
            .map(machine_ip)
            .collect();
        if let Some(client) = &self
            .faults
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .client_ip
        {
            peers.push(client.clone());
        }
        peers
    }

    fn play(&self, ctx: &FaultContext, op: &Op) -> SimulationResult<()> {
        let Some(machine) = op.fault_target() else {
            return Ok(());
        };
        let ip = machine_ip(machine);
        match op {
            Op::Crash { .. } => {
                ctx.crash(&ip)?;
                self.state().down.insert(machine);
            }
            Op::Restart { .. } => {
                ctx.restart(&ip)?;
                self.state().down.remove(&machine);
            }
            Op::Isolate { .. } => {
                for peer in self.peers(machine) {
                    ctx.partition(&ip, &peer)?;
                }
                self.state().isolated.insert(machine);
            }
            Op::Heal { .. } => {
                for peer in self.peers(machine) {
                    ctx.heal_partition(&ip, &peer)?;
                }
                self.state().isolated.remove(&machine);
            }
            _ => {}
        }
        Ok(())
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FaultState> {
        self.faults.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// How often the injector looks for a fault step. It wakes on this beat for
/// the whole run, whatever the log holds: its timers, and the draws they
/// take, are then the same in every run of one config, so a fault added to
/// the log never changes what happened before it. A fault plays when the
/// operator reaches its step, as every step does: never during the step
/// before it.
const BEAT: Duration = Duration::from_millis(10);

#[async_trait]
impl FaultInjector for Injector {
    fn name(&self) -> &'static str {
        "playground_faults"
    }

    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        while !ctx.chaos_shutdown().is_cancelled() {
            let wanted = self.state().wanted;
            if let Some(action) = wanted.and_then(|index| self.actions.get(index)) {
                self.play(ctx, &action.op)?;
                self.state().wanted = None;
            }
            if ctx.time().sleep(BEAT).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}
