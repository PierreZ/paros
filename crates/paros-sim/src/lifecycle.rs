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
    if let Some(queue) = state.get::<Arc<Mutex<Queue>>>(LIFECYCLE_KEY) {
        return queue;
    }
    let queue = Arc::new(Mutex::new(Queue::default()));
    state.publish(LIFECYCLE_KEY, queue.clone());
    queue
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

/// The injector: drains the queue in order for as long as the chaos window is
/// open (the corpus opens it for the whole run).
pub(crate) struct ScriptedLifecycle;

#[async_trait]
impl FaultInjector for ScriptedLifecycle {
    fn name(&self) -> &'static str {
        "paros-scripted-lifecycle"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let queue = queue(ctx.state());
        while !ctx.chaos_shutdown().is_cancelled() {
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
            if ctx.time().sleep(POLL).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}
