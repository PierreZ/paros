//! The fault steps: `crash`, `restart`, `isolate` and `heal`, played by
//! moonpool's own fault injector when the operator reaches them. The
//! machines and the client do not know a fault is coming: the code they run
//! is the shipped code, and only the simulated world around it changes.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider};

use crate::playground::machine::machine_ip;
use crate::playground::model::{Op, StepResult};

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
    /// The fault steps asked for and not yet played, each with its index
    /// in the log. The injector plays them on its next beat, in order.
    pub(crate) wanted: VecDeque<(usize, Op)>,
    /// The last fault step played, by its index in the log, until the
    /// operator reads where everything lives after it.
    pub(crate) played: Option<usize>,
}

/// The shared [`FaultState`].
pub(crate) type Faults = Arc<Mutex<FaultState>>;

/// Plays the fault steps the operator reaches.
pub(crate) struct Injector {
    machines: usize,
    faults: Faults,
    results: Arc<Mutex<Vec<StepResult>>>,
}

impl Injector {
    pub(crate) fn new(
        machines: usize,
        faults: Faults,
        results: Arc<Mutex<Vec<StepResult>>>,
    ) -> Self {
        Self {
            machines,
            faults,
            results,
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
        let state = self.state();
        let already = match op {
            Op::Crash { .. } => state.down.contains(&machine),
            // A machine that runs is killed and started again in place.
            Op::Restart { .. } => false,
            Op::Isolate { .. } => state.isolated.contains(&machine),
            _ => !state.isolated.contains(&machine),
        };
        drop(state);
        if already {
            return Err(moonpool_sim::SimulationError::InvalidState(format!(
                "machine-{machine} is {} already",
                match op {
                    Op::Crash { .. } => "down",
                    Op::Isolate { .. } => "cut off",
                    _ => "on the network",
                }
            )));
        }
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
/// the whole run, whatever the page does: its timers, and the draws they
/// take, are then the same in every run of one config, so a fault asked for
/// later never changes what happened before it. A fault plays on the beat
/// after it is asked for, even while the operator runs a long step.
const BEAT: Duration = Duration::from_millis(10);

#[async_trait]
impl FaultInjector for Injector {
    fn name(&self) -> &'static str {
        "playground_faults"
    }

    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        while !ctx.chaos_shutdown().is_cancelled() {
            let wanted = self.state().wanted.pop_front();
            if let Some((index, op)) = wanted {
                let at = u64::try_from(ctx.time().now().as_millis()).unwrap_or(u64::MAX);
                let (ok, detail) = match self.play(ctx, &op) {
                    Ok(()) => (true, fault_detail(&op)),
                    Err(error) => (false, error.to_string()),
                };
                tracing::info!(index, ok, %detail, "operator_step");
                self.state().played = Some(index);
                self.results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(StepResult {
                        index,
                        start_ms: at,
                        end_ms: at,
                        ok,
                        detail,
                    });
            }
            if ctx.time().sleep(BEAT).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// What a fault step did, for people.
fn fault_detail(op: &Op) -> String {
    let machine = op.fault_target().unwrap_or_default();
    match op {
        Op::Crash { .. } => format!("machine-{machine} crashed: its unsynced writes are lost"),
        Op::Restart { .. } => format!("machine-{machine} started again on its disk"),
        Op::Isolate { .. } => format!("machine-{machine} is cut from the network"),
        _ => format!("machine-{machine} is on the network again"),
    }
}
