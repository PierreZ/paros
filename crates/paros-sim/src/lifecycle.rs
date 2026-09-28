//! Scripted process lifecycle for the corpus: the workload enqueues crash and
//! restart commands on the shared state handle, and a moonpool fault injector
//! — factory-built per timeline, so scripted sequences replay exactly from the
//! root seed plus recipe — executes them through
//! [`FaultContext::crash`] / [`FaultContext::restart`] and acknowledges each.
//!
//! A crash here is moonpool's own force-kill with no recovery timer armed: the
//! process task is aborted, its connections die, its un-synced staged writes
//! die with the storage handle, and the node stays down until the script says
//! otherwise. A restart boots a fresh incarnation from the process factory,
//! which restores from the durable [`StorageWorld`](crate::world::StorageWorld).
//!
//! The corpus scripts whole fault schedules through it. The main campaign
//! registers it too, for the one lifecycle act its chain client takes as an
//! operator: rebooting every member of a configuration it just installed
//! (#173). Like the client's other operator acts (a reconfiguration, a
//! retirement), that one is not bound to the chaos window, so the injector
//! drains the queue for the whole run — more slowly once the window closed —
//! and the orchestrator aborts it when the run ends.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimContext, SimulationResult, StateHandle, TimeProvider,
};

const LIFECYCLE_KEY: &str = "paros-scripted-lifecycle";
/// Poll cadence of the injector loop and of a workload waiting for its
/// acknowledgement, in simulated time (deterministic per seed).
const POLL: Duration = Duration::from_millis(1);
/// The injector's cadence once the chaos window closed: the queue is fed by
/// rare operator acts there, and a millisecond beat for a tail of tens of
/// seconds is only simulator events.
const QUIET_POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug)]
enum Op {
    Crash(String),
    Restart(String),
}

#[derive(Default)]
struct Queue {
    ops: Vec<Op>,
    executed: usize,
}

fn queue(state: &StateHandle) -> Arc<Mutex<Queue>> {
    crate::state::published(state, LIFECYCLE_KEY, Queue::default)
}

/// Enqueue `op` and wait until the injector has executed it (the kill or
/// restart event is then scheduled; it lands at the next simulator step).
#[tracing::instrument(level = "debug", skip(ctx), fields(op = ?op))]
async fn run(ctx: &SimContext, op: Op) {
    let queue = queue(ctx.state());
    let position = {
        let mut guard = queue.lock().unwrap_or_else(PoisonError::into_inner);
        guard.ops.push(op);
        guard.ops.len()
    };
    loop {
        if queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .executed
            >= position
            || ctx.shutdown().is_cancelled()
        {
            return;
        }
        if ctx.time().sleep(POLL).await.is_err() {
            return;
        }
    }
}

/// Crash `ip` and hold it down until [`restart`].
#[tracing::instrument(level = "debug", skip(ctx))]
pub(crate) async fn crash(ctx: &SimContext, ip: &str) {
    run(ctx, Op::Crash(ip.to_string())).await;
}

/// Restart `ip`: a held-down node boots again; a live one is rebooted.
#[tracing::instrument(level = "debug", skip(ctx))]
pub(crate) async fn restart(ctx: &SimContext, ip: &str) {
    run(ctx, Op::Restart(ip.to_string())).await;
}

/// The injector: drains the queue in order for the whole run (see the
/// module doc).
pub(crate) struct ScriptedLifecycle;

#[async_trait]
impl FaultInjector for ScriptedLifecycle {
    fn name(&self) -> &'static str {
        "paros-scripted-lifecycle"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let queue = queue(ctx.state());
        loop {
            let pending: Vec<Op> = {
                let guard = queue.lock().unwrap_or_else(PoisonError::into_inner);
                guard.ops[guard.executed..].to_vec()
            };
            for op in pending {
                match &op {
                    Op::Crash(ip) => ctx.crash(ip)?,
                    Op::Restart(ip) => ctx.restart(ip)?,
                }
                queue
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .executed += 1;
            }
            let poll = if ctx.chaos_shutdown().is_cancelled() {
                QUIET_POLL
            } else {
                POLL
            };
            if ctx.time().sleep(poll).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}
