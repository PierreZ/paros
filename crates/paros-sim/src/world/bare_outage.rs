//! The **bare-quorum outage** (#270): on a seed that draws the bare-quorum
//! scenario (`crate::shape::bare_quorum`), the correlated outage strikes
//! the moment the main journal's custody ledger holds a decided slot a
//! member of its deciding configuration never held, every copy settled
//! ([`super::StorageWorld::holds_short_slot`]), and the loss planned at
//! that instant takes every copy of it (`super::outage::LossShape::BARE_QUORUM`).
//!
//! Such a slot exists only between its decision and the lagging member's
//! accept, a few milliseconds at most, so moonpool's own outage
//! (`super::outage::regime`) almost never lands on one. This injector
//! polls for it through the chaos window and strikes once. The slot's
//! Phase-1 tally then reads `faulty, faulty, none`: the decided value is
//! gone, and a leader must refuse the no-op fill a `none` majority would
//! otherwise license. Every other seed keeps moonpool's outage alone.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use super::late_outage::strike;
use super::outage::{LossShape, POLL, plan_losses};

/// The bare-quorum injector (see the module doc): a fresh one per timeline.
pub(crate) struct BareOutage;

#[async_trait]
impl FaultInjector for BareOutage {
    fn name(&self) -> &'static str {
        "paros-bare-outage"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::bare_quorum(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let main = crate::shape::journals(ctx.state()).main;
        let chaos = Duration::from_millis(crate::CHAOS_DURATION_MS);
        loop {
            if time.now() >= chaos || ctx.chaos_shutdown().is_cancelled() {
                return Ok(());
            }
            let decided = crate::audit::audit_world_for(ctx.state(), main).decided_slots();
            let short = super::storage_world_for(ctx.state(), main)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .holds_short_slot(&decided);
            if short {
                break;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        assert_reachable!("outage: the bare-quorum outage strikes on a slot short of a member");
        // Planned at the instant of the strike, with no await between them,
        // so the lagging member has not accepted the slot yet.
        let _ = plan_losses(ctx.state(), LossShape::BARE_QUORUM);
        strike(ctx, &[])
    }
}
