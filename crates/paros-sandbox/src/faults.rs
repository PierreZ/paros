//! The fault steps of the action log: crash, restart, partition and heal,
//! each at its time, through moonpool's fault context. No other fault is
//! drawn: the network and the disks are simulated, but quiet.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider};

use crate::machine::machine_ip;
use crate::model::{Action, Op};

/// How often the injector looks at the log.
const TICK: Duration = Duration::from_millis(50);

/// Runs the fault steps in time order.
pub(crate) struct Faults {
    actions: Vec<Action>,
}

impl Faults {
    pub(crate) fn new(actions: &[Action]) -> Self {
        let mut actions: Vec<Action> = actions
            .iter()
            .filter(|action| !action.op.is_operator())
            .cloned()
            .collect();
        actions.sort_by_key(|action| action.at_ms);
        Self { actions }
    }
}

#[async_trait]
impl FaultInjector for Faults {
    fn name(&self) -> &'static str {
        "sandbox-faults"
    }

    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        // A fixed tick, whatever the log holds: a sleep draws from the
        // seed's one random stream, so the injector sleeps the same way in
        // every run, and a step added to the log never changes what came
        // before it. A fault lands on the first tick at or after its time.
        let mut next = 0;
        loop {
            let now = u64::try_from(ctx.time().now().as_millis()).unwrap_or(u64::MAX);
            while let Some(action) = self.actions.get(next).filter(|a| a.at_ms <= now) {
                match &action.op {
                    Op::Crash { machine } => ctx.crash(&machine_ip(*machine))?,
                    Op::Restart { machine } => ctx.restart(&machine_ip(*machine))?,
                    Op::Partition { a, b } => ctx.partition(&machine_ip(*a), &machine_ip(*b))?,
                    Op::Heal { a, b } => ctx.heal_partition(&machine_ip(*a), &machine_ip(*b))?,
                    Op::Init | Op::Write { .. } | Op::Admit { .. } => {}
                }
                next += 1;
            }
            if ctx.chaos_shutdown().is_cancelled() || ctx.time().sleep(TICK).await.is_err() {
                return Ok(());
            }
        }
    }
}
