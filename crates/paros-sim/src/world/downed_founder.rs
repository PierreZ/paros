//! The **downed-founder scenario** (#213 (three founding members)): on a
//! seed that draws it (`crate::shape::downed_founder`), one founding member
//! of a cell of three or more goes down for a few seconds through
//! moonpool's own `crash_for`, while operators run `init` and create
//! tenants. The other founders keep a majority, so the cell decree, `init`'s
//! fleet half, every tenant creation and every control-journal write must
//! still be chosen without it (`crate::chain_workload::fleet`).
//!
//! The injector strikes once, on a coin: during `init` (an operator sent
//! it and the cell is not formed yet) or, more often, once every founder
//! formed. The
//! founder comes back by itself at its deadline. `init` mostly runs in the
//! recovery tail, so like the silent machine (`super::silent_machine`) the
//! injector may strike up to `LATE_WINDOW` into the tail; the cell keeps a
//! majority throughout, so the tail's liveness claims still hold.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// How long the founder stays down, in milliseconds: long enough for an
/// operator's `init` or several tenant creations to run without it. With
/// 3–6 s, one struck seed in five created a tenant inside the window.
const DOWN_MS: std::ops::Range<u64> = 6_000..10_001;

/// The downed-founder injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct DownedFounder;

#[async_trait]
impl FaultInjector for DownedFounder {
    fn name(&self) -> &'static str {
        "paros-downed-founder"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::downed_founder(ctx.state()) {
            return Ok(());
        }
        // During `init` or after it: the cell decree and `init`'s fleet
        // half, or the tenant creations of a formed cell. The decree needs
        // every founder's promise (`q1 = n`), so a strike during `init`
        // mostly delays the cell until the founder is back; the gates
        // wait on a formed cell, so it gets three seeds in four.
        let formed = !moonpool_sim::sim_random_bool(0.25);
        let time = ctx.time();
        let target = loop {
            if time.now() >= crate::CHAOS_DURATION + LATE_WINDOW {
                return Ok(());
            }
            if let Some(target) =
                crate::machine::downed_target(ctx.state(), formed, |ip| ctx.is_dead(ip))
            {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        if formed {
            assert_reachable!("machine: the scenario holds a founder of a formed cell down");
        } else {
            assert_reachable!("machine: the scenario holds a founder down during init");
        }
        let down = Duration::from_millis(moonpool_sim::sim_random_range(DOWN_MS));
        let now = time.now();
        tracing::info!(%target, ?down, formed, "downed_founder_strikes");
        crate::machine::note_downed(ctx.state(), now, now + down);
        ctx.crash_for(&target, down)
    }
}
