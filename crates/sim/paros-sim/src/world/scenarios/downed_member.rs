//! The **downed-member scenario** (#213 (three founding members)): on a
//! seed that draws it (`crate::shape::downed_member`), one member of a
//! cell of three or more goes down for a few seconds through moonpool's own
//! `crash_for`, while operators run `init` and create tenants. The other
//! members keep a majority, so the cell decree, `init`'s fleet half, every
//! tenant creation and every control-journal write must still be chosen
//! without it (`crate::workload::fleet`).
//!
//! The injector strikes once, on a coin: during `init` (an operator sent
//! it and the cell is not formed yet), when the member is an initial one
//! because the first configuration is the only one, or, more often, once
//! every initial member formed, when it is any member in force of the plan
//! journals (#685 (no founders)). The member comes back by itself at its
//! deadline. `init` mostly runs in the
//! recovery tail, so like the silent machine (`super::silent_machine`) the
//! injector may strike up to `LATE_WINDOW` into the tail; the cell keeps a
//! majority throughout, so the tail's liveness claims still hold.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use super::late_outage::LATE_WINDOW;
use crate::world::outage::POLL;

/// How long the initial member stays down, in milliseconds: long enough for an
/// operator's `init` or several tenant creations to run without it. With
/// 3–6 s, one struck seed in five created a tenant inside the window.
const DOWN_MS: std::ops::Range<u64> = 6_000..10_001;

/// The downed-member injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct DownedMember;

#[async_trait]
impl FaultInjector for DownedMember {
    fn name(&self) -> &'static str {
        "paros-downed-member"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::downed_member(ctx.state()) {
            return Ok(());
        }
        // During `init` or after it: the cell decree and `init`'s fleet
        // half, or the tenant creations of a formed cell. The decree needs
        // every initial member's promise (`q1 = n`), so a strike during `init`
        // mostly delays the cell until the initial member is back; the gates
        // wait on a formed cell, so it gets three strikes in four.
        let formed = !moonpool_sim::sim_random_bool(0.25);
        let time = ctx.time();
        let target = loop {
            if crate::past_cutoff(time.now(), LATE_WINDOW) {
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
            assert_reachable!("machine: the scenario holds a member of a formed cell down");
        } else {
            assert_reachable!("machine: the scenario holds an initial member down during init");
        }
        let down = Duration::from_millis(moonpool_sim::sim_random_range(DOWN_MS));
        let now = time.now();
        tracing::info!(%target, ?down, formed, "downed_member_strikes");
        crate::machine::note_downed(ctx.state(), now, now + down);
        ctx.crash_for(&target, down)
    }
}
