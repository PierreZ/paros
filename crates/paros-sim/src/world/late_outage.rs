//! The **departed straggler's late outage** (#263, decided on 2026-10-09):
//! on a seed that draws the departed-straggler scenario
//! (`crate::shape::departed_straggler`), the correlated outage may strike
//! once the owner's member-removing reconfiguration took effect, early in
//! the recovery tail.
//!
//! The scenario needs a claim, a removal and then the outage, but an owner
//! wins its claim around 5.4 s into a run on the median, after the 4 s
//! chaos window moonpool's own outage (`super::outage::regime`) must strike
//! inside. So this injector waits for a configuration that removed a member
//! of the bootstrap one ([`crate::audit::AuditWorld::has_departure`]), at most
//! [`LATE_WINDOW`] into the tail, then takes every acceptor and proxy leader
//! down at one instant, each back after its own delay and one straggler
//! last, and plans the straggler's loss while every victim is down
//! (`super::outage::plan_losses`). The rest of the tail, the workload's
//! whole recovery budget, is still a genuine recovery that every liveness
//! oracle judges. Every other seed keeps a quiet tail.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable, sim_random_range,
};

use super::outage::{DOWN, LossShape, POLL, STRAGGLER, plan_losses};

/// How far into the recovery tail the late outage may still strike. Far
/// below the workload's recovery budget (45 s at its floor), so a straggler
/// back last still leaves the tail most of its budget to converge in.
pub(crate) const LATE_WINDOW: Duration = Duration::from_secs(4);

/// The last instant the late outage may strike, and the owner's
/// after-claim removal stays armed on a scenario seed
/// (`ChainWorkload`'s `remove_next`).
pub(crate) fn late_deadline() -> Duration {
    Duration::from_millis(crate::CHAOS_DURATION_MS) + LATE_WINDOW
}

const _: () = assert!(LATE_WINDOW.as_millis() + STRAGGLER.end.as_millis() < 45_000);

/// The late-outage injector (see the module doc): a fresh one per timeline.
pub(crate) struct LateOutage;

#[async_trait]
impl FaultInjector for LateOutage {
    fn name(&self) -> &'static str {
        "paros-late-outage"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::departed_straggler(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let main = crate::shape::journals(ctx.state()).main;
        loop {
            if time.now() >= late_deadline() {
                return Ok(());
            }
            if crate::audit::audit_world_for(ctx.state(), main).has_departure() {
                break;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        if time.now() >= Duration::from_millis(crate::CHAOS_DURATION_MS) {
            assert_reachable!("outage: a departed straggler's outage strikes in the recovery tail");
        }
        strike(ctx)?;
        plan_losses(ctx.state(), LossShape::DEPARTED_STRAGGLER);
        Ok(())
    }
}

/// Take every acceptor and proxy leader down now, each back after its own
/// delay in [`DOWN`], one acceptor straggling in [`STRAGGLER`].
fn strike(ctx: &FaultContext) -> SimulationResult<()> {
    let acceptors = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
    let proxies = ctx.ips_in_group(crate::roles::PROXY_GROUP);
    let straggler =
        usize::try_from(sim_random_range(0..acceptors.len().max(1) as u64)).unwrap_or(0);
    let millis = |range: &std::ops::Range<Duration>| {
        Duration::from_millis(sim_random_range(
            u64::try_from(range.start.as_millis()).unwrap_or(0)
                ..u64::try_from(range.end.as_millis()).unwrap_or(1),
        ))
    };
    for (rank, ip) in acceptors.iter().enumerate() {
        let down = if rank == straggler {
            millis(&STRAGGLER)
        } else {
            millis(&DOWN)
        };
        ctx.crash_for(ip, down)?;
    }
    for ip in &proxies {
        ctx.crash_for(ip, millis(&DOWN))?;
    }
    Ok(())
}
