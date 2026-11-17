//! The **zone-outage scenario** (#215 (zones)): on a seed that draws it
//! (`crate::shape::zone_outage`), every machine of one failure domain goes
//! down at once for a few seconds through moonpool's own `crash_for`, while
//! operators create tenants and journals and write to them. The initial
//! members stand one per zone, and the tenant coordinator places each
//! journal under the zone rule (`paros::model::placement`), so the cell's control
//! journals and every journal whose durability survives a failure must keep
//! committing without that zone (`crate::workload::fleet`).
//!
//! The injector strikes once, once every initial member formed and a client
//! created a journal, at the zone of the three with the most live
//! machines. Every machine comes back by itself at its deadline. It strikes
//! only before the chaos cutoff (rule 8,
//! `docs/architecture/06-verification.md`); the cell keeps a majority
//! throughout.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

use crate::world::outage::POLL;

/// How long the zone stays down, in milliseconds: long enough for an
/// operator to create a tenant or a journal and write to it without the
/// zone. A tenant created whole inside a 6–10 s hold fired on 0 of 300 hunt
/// seeds, on 1 of 300 with 10–16 s (#623 (zone-down gates in the sweep)).
const DOWN_MS: std::ops::Range<u64> = 10_000..16_001;

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
        // The zones are tried from a drawn one, and the outage takes the one
        // with the most live machines: on a cell of four, the zone of two.
        let first = moonpool_sim::sim_random_range(0_u32..3_u32);
        let time = ctx.time();
        let (zone, targets) = loop {
            if crate::past_cutoff(time.now()) {
                return Ok(());
            }
            let fullest = (0..3_u32)
                .map(|i| format!("zone-{}", (first + i) % 3))
                .filter_map(|zone| {
                    crate::machine::zone_targets(ctx.state(), &zone, |ip| ctx.is_dead(ip))
                        .map(|targets| (zone, targets))
                })
                .reduce(|best, next| {
                    if next.1.len() > best.1.len() {
                        next
                    } else {
                        best
                    }
                });
            // The outage waits for a created journal, so the clients have
            // one to append to without the zone.
            let journal = crate::machine::journal_created(ctx.state());
            if let Some(fullest) = fullest.filter(|_| journal) {
                break fullest;
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
