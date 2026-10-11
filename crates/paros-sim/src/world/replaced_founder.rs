//! The **replaced-founder scenario** (#423): on a seed that draws it
//! (`crate::shape::replaced_founder`), a founding member of a formed cell of
//! three or more loses its whole disk through moonpool's own `CrashAndWipe`.
//! The machine at that address comes back as a new, idle one, and the cell
//! keeps a majority. Client 0 then admits that machine with `cell
//! add-machine` and runs `init` again over the founders
//! (`crate::chain_workload::fleet`): the re-run meets two formed founders and
//! one admitted machine at a listed address, and must find the cell.
//!
//! The injector polls the machine board for the moment every founder formed
//! (`crate::machine::replace_target`), then strikes once. The cell often
//! forms only after the chaos window, so like the moved founder
//! (`super::moved_founder`) it may strike up to `LATE_WINDOW` into the
//! recovery tail; the cell keeps a majority of its members.

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// The replaced-founder injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct ReplacedFounder;

#[async_trait]
impl FaultInjector for ReplacedFounder {
    fn name(&self) -> &'static str {
        "paros-replaced-founder"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::replaced_founder(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            if time.now() >= crate::CHAOS_DURATION + LATE_WINDOW {
                return Ok(());
            }
            if let Some(target) = crate::machine::replace_target(ctx.state(), |ip| ctx.is_dead(ip))
            {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("machine: the scenario wipes a founder of a formed cell");
        tracing::info!(%target, "replaced_founder_strikes");
        ctx.reboot_with_delays(&target, RebootKind::CrashAndWipe, &(250..1_501), &(0..1))
    }
}
