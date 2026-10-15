//! The **gone-machine scenario** (#475 (re-place after the Down bound),
//! `crate::shape::gone_machine`): on a drained-machine seed that draws it,
//! the operator picks an admitted machine that holds a journal's slot
//! another storage machine could take (`crate::chain_workload::fleet`'s
//! drain step), and this injector crashes it through moonpool's own `Crash`
//! for longer than every `machine_replace_after` the knob draws. The cell
//! coordinator marks it down, then re-places its roles once the bound and
//! any maintenance window over it passed (#359). The machine comes back
//! later as a new incarnation and keeps no slot it lost.
//!
//! On a coin the machine comes back soon after the longest bound instead
//! (`BACK_SOON_MS` past it): the coordinator may have started a move, and
//! the machine answers before the move ends. The coordinator then releases
//! the slot the journal's configuration left (`machine::moves`' trim).
//!
//! The cell keeps its founding member up, so the tail's liveness claims
//! still hold; the strike may come up to `LATE_WINDOW` into the tail.

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
    buggify_with_prob,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// How long the machine stays down, in milliseconds: past the largest
/// `machine_replace_after` the simulation draws (`crate::shape`: a renewal
/// period of at most 1 s, plus at most 3 s, plus at most 8 s).
const GONE_MS: std::ops::Range<usize> = 13_000..18_001;

/// How long past the longest `machine_replace_after` a machine that comes
/// back soon stays down, in milliseconds: a move's first steps fit in it.
const BACK_SOON_MS: std::ops::Range<usize> = 500..3_001;

/// The gone-machine injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct GoneMachine;

#[async_trait]
impl FaultInjector for GoneMachine {
    fn name(&self) -> &'static str {
        "paros-gone-machine"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::gone_machine(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            if crate::past_cutoff(time.now(), LATE_WINDOW) {
                return Ok(());
            }
            if let Some(target) = crate::machine::gone_target(ctx.state(), |ip| ctx.is_dead(ip)) {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("machine: the scenario takes a machine holding a slot down");
        tracing::info!(%target, "gone_machine_strikes");
        let hold = if buggify_with_prob!(0.5) {
            assert_reachable!("machine: a gone machine comes back soon after the bound");
            let bound =
                usize::try_from(crate::shape::longest_replace_after(ctx.state()).as_millis())
                    .unwrap_or(GONE_MS.start);
            bound + BACK_SOON_MS.start..bound + BACK_SOON_MS.end
        } else {
            GONE_MS
        };
        ctx.reboot_with_delays(&target, RebootKind::Crash, &hold, &(0..1))
    }
}
