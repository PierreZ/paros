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
//! last, with the straggler's loss planned at that instant
//! (`super::outage::plan_losses`). The straggler is the one clean copy the
//! loss left, so the successor must campaign, and judge the slot through
//! the prior configuration, while it is still down (#267). The rest of the tail, the workload's
//! whole recovery budget, is still a genuine recovery that every liveness
//! oracle judges. Every other seed keeps a quiet tail.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use super::outage::{LossShape, POLL, STRAGGLER, may_strike, plan_losses, strike};

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
                if may_strike(ctx) {
                    break;
                }
                assert_reachable!("outage: a late outage waits for a commit in flight");
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        if time.now() >= Duration::from_millis(crate::CHAOS_DURATION_MS) {
            assert_reachable!("outage: a departed straggler's outage strikes in the recovery tail");
        }
        // The loss is planned at the instant of the strike, with no await
        // between them, so no peer repairs a copy from memory first; it is
        // planned first so the straggler is the holder it left clean.
        let kept = plan_losses(
            ctx.state(),
            LossShape::DEPARTED_STRAGGLER.with_spares_kept(),
        );
        strike(ctx, &kept)
    }
}
