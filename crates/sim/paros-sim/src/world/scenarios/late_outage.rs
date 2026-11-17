//! The **departed straggler's late outage** (#263, decided on 2026-10-09):
//! on a seed that draws the departed-straggler scenario
//! (`crate::shape::departed_straggler`), the correlated outage strikes
//! once the owner's member-removing reconfiguration took effect.
//!
//! The scenario needs a claim, a removal and then the outage. An owner
//! wins its claim around 5.4 s into a run on the median, and chaos lasts
//! the whole run (#454 (control plane meets chaos)), so the outage strikes
//! inside the chaos window. This injector waits for a configuration that
//! removed a member of the bootstrap one
//! ([`crate::audit::AuditWorld::has_departure`]), until the chaos cutoff,
//! then takes every acceptor and proxy leader down at one instant, each
//! back after its own delay and one straggler last, with the straggler's
//! loss planned at that instant (`crate::world::outage::plan_losses`). The
//! straggler is the one clean copy the loss left, so the successor must
//! campaign, and judge the slot through the prior configuration, while it
//! is still down (#267). No strike comes after the cutoff (rule 8,
//! `docs/architecture/06-verification.md`): the tail is a genuine recovery
//! that every liveness oracle judges.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider};

use crate::world::outage::{LossShape, POLL, plan_losses, strike};

/// Whether the last instant the late outage may strike (the chaos cutoff)
/// passed, and with
/// it the owner's after-claim removal stops being armed on a scenario seed
/// (`ChainWorkload`'s `remove_next`).
pub(crate) fn past_late_deadline(now: Duration) -> bool {
    crate::past_cutoff(now)
}

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
            if past_late_deadline(time.now()) {
                return Ok(());
            }
            if crate::audit::audit_world_for(ctx.state(), main).has_departure() {
                break;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        // The loss is planned at the instant of the strike, with no await
        // between them, so no peer repairs a copy from memory first; it is
        // planned first so the straggler is the holder it left clean.
        let kept = plan_losses(
            ctx.state(),
            &ctx.shards(),
            LossShape::DEPARTED_STRAGGLER.with_spares_kept(),
            &|_| false,
        );
        strike(ctx, &kept)
    }
}
