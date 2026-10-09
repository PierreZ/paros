//! The **wiped-founder scenario** (#246): on a seed that draws it
//! (`crate::shape::wiped_founder`), a founding member loses its whole disk
//! in the middle of `init`, through moonpool's own `CrashAndWipe`. The
//! machine at that address comes back as a new one, with a new `node_id`,
//! and never rejoins as the old one (`crate::machine`).
//!
//! The scenario draws one of two moments, and this injector polls the
//! machine board for it through the chaos window, then strikes once:
//!
//! - **before any vote**: every founder promised and none voted; the
//!   injector wipes a founder other than `cell init`'s receiver. The
//!   receiver's decree goes on and forms the other founders over the old
//!   machine, which the new one refuses. With three founders the two others
//!   are a majority and choose the plan: the cell forms with the old id as a
//!   dead member. With two, the receiver does not vote for a plan it cannot
//!   choose, and the next ballot draws one over the new machine. On a
//!   one-founder cell the founder is the receiver: no vote names the old
//!   machine, and `init` forms the cell over the new one;
//! - **after a vote**: a founder voted and another did not, which is
//!   wiped: the vote names the old machine. With three founders the two
//!   that kept their disks choose it; with two, the plan lost a majority
//!   and `cell init` refuses `cell_lost` (`crate::machine::cell_lost`).
//!
//! The machine group's attrition draws the same wipe at any reboot
//! (`crate::MACHINE_WIPE_WEIGHT`); this injector only aims it.

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use super::outage::POLL;

/// The wiped-founder injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct WipedFounder;

#[async_trait]
impl FaultInjector for WipedFounder {
    fn name(&self) -> &'static str {
        "paros-wiped-founder"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::wiped_founder(ctx.state()) {
            return Ok(());
        }
        let after_vote = moonpool_sim::sim_random_bool(0.5);
        let time = ctx.time();
        let target = loop {
            if time.now() >= crate::CHAOS_DURATION || ctx.chaos_shutdown().is_cancelled() {
                return Ok(());
            }
            let target = crate::machine::wipe_target(ctx.state(), after_vote, |ip| ctx.is_dead(ip));
            if let Some(target) = target {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        if after_vote {
            assert_reachable!("machine: the scenario wipes a founder after another founder voted");
        } else {
            assert_reachable!("machine: the scenario wipes a promised founder before any vote");
        }
        tracing::info!(%target, after_vote, "wiped_founder_strikes");
        ctx.reboot_with_delays(
            &target.ip().to_string(),
            RebootKind::CrashAndWipe,
            &(250..1_501),
            &(0..1),
        )
    }
}
