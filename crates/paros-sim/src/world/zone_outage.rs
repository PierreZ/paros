//! The **zone-outage scenario** (#215 (zones)): on a seed that draws it
//! (`crate::shape::zone_outage`), every machine of one failure domain goes
//! down at once for a few seconds through moonpool's own `crash_for`, while
//! operators create tenants and journals and write to them. The founding
//! members stand one per zone, and the tenant coordinator places each
//! journal under the zone rule (`paros::placement`), so the cell's control
//! journals and every journal whose durability survives a failure must keep
//! committing without that zone (`crate::chain_workload::fleet`).
//!
//! The injector strikes once, once every founder formed, at a zone drawn
//! from the three. Every machine comes back by itself at its deadline.
//! Like the downed founder (`super::downed_founder`), it may strike up to
//! `LATE_WINDOW` into the recovery tail; the cell keeps a majority
//! throughout, so the tail's liveness claims still hold.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// How long the zone stays down, in milliseconds: long enough for an
/// operator to create a tenant or a journal and write to it without the
/// zone, as the downed founder's hold.
const DOWN_MS: std::ops::Range<u64> = 6_000..10_001;

/// The zone-outage injector (see the module doc): a fresh one per timeline.
pub(crate) struct ZoneOutage;

#[async_trait]
impl FaultInjector for ZoneOutage {
    fn name(&self) -> &'static str {
        "paros-zone-outage"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::zone_outage(ctx.state()) {
            return Ok(());
        }
        let zone = format!("zone-{}", moonpool_sim::sim_random_range(0_u32..3_u32));
        let time = ctx.time();
        let targets = loop {
            if crate::past_cutoff(time.now(), LATE_WINDOW) {
                return Ok(());
            }
            if let Some(targets) =
                crate::machine::zone_targets(ctx.state(), &zone, |ip| ctx.is_dead(ip))
            {
                break targets;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("zones: the scenario takes a zone of a formed cell down");
        if targets.len() > 1 {
            assert_reachable!("zones: a zone outage takes several machines down at once");
        }
        let down = Duration::from_millis(moonpool_sim::sim_random_range(DOWN_MS));
        let now = time.now();
        tracing::info!(%zone, machines = targets.len(), ?down, "zone_outage_strikes");
        crate::machine::note_zone_down(ctx.state(), now, now + down);
        for target in &targets {
            ctx.crash_for(target, down)?;
        }
        Ok(())
    }
}
