//! The **lagging-acceptor scenario** (#340): on a seed that draws it
//! (`crate::shape::lagging_acceptor`), one acceptor goes down early in the
//! chaos window and stays down until a peer's floor passes the chosen
//! prefix it holds (`crate::audit::AuditWorld::floor_passed`), or until
//! [`LATE_WINDOW`] into the recovery tail. It then boots below every
//! floor, and its first catch-up meets a trim point (`Message::TrimmedTo`):
//! it jumps, its allocator frontier far below the point.
//!
//! The journal truncates little in the chaos window, and a node the
//! attrition takes down comes back fast, so a node behind every floor is
//! rare: 3 jumps in 300 hunt seeds. The scenario also makes every client
//! compact at every truncation step (`crate::chain_workload`). Like the
//! departed straggler's outage (`super::late_outage`) and the silent
//! machine (`super::silent_machine`), the hold may reach `LATE_WINDOW` into
//! the tail; one acceptor down is a fault every quorum system the run
//! draws already meets under attrition, and the tail after the restart is
//! still the recovery the oracles judge.
//!
//! The crash is a power loss, so it waits for a moment no budgeted commit
//! is in flight on the node (#332, [`super::cut`]).

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable, sim_random_range,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// When the acceptor goes down, from the opening of the chaos window:
/// once the cell formed and the journal has a leader on most seeds, and
/// early enough that the others make progress without it.
const START: std::ops::Range<u64> = 800..2_000;

/// How often the injector checks the floors while the acceptor is down.
const WATCH: Duration = Duration::from_millis(20);

/// The lagging-acceptor injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct LaggingAcceptor;

#[async_trait]
impl FaultInjector for LaggingAcceptor {
    fn name(&self) -> &'static str {
        "paros-lagging-acceptor"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::lagging_acceptor(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let deadline = crate::CHAOS_DURATION + LATE_WINDOW;
        if time
            .sleep(Duration::from_millis(sim_random_range(START)))
            .await
            .is_err()
        {
            return Ok(());
        }
        // Node ids are ranks in the acceptor group.
        let acceptors = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
        let rank = sim_random_range(0..u64::try_from(acceptors.len()).unwrap_or(0));
        let Some(target) = usize::try_from(rank)
            .ok()
            .and_then(|rank| acceptors.get(rank))
            .cloned()
        else {
            return Ok(());
        };
        loop {
            if time.now() >= deadline || ctx.chaos_shutdown().is_cancelled() {
                return Ok(());
            }
            if !ctx.is_dead(&target)
                && super::cut::outage_permitted(ctx.state(), std::slice::from_ref(&target))
            {
                break;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        // Checked and struck with no await between them (#332).
        super::cut::assert_outage_permitted(ctx.state(), std::slice::from_ref(&target));
        ctx.crash(&target)?;
        assert_reachable!("storage: the lagging-acceptor scenario holds an acceptor down");
        tracing::info!(%target, rank, "lagging_acceptor_down");
        let audit = crate::audit::audit_world(ctx.state());
        let passed = loop {
            if audit.floor_passed(rank) {
                break true;
            }
            if time.now() >= deadline || time.sleep(WATCH).await.is_err() {
                break false;
            }
        };
        if passed {
            assert_reachable!("storage: a peer's floor passes the lagging acceptor's prefix");
        }
        tracing::info!(%target, passed, "lagging_acceptor_back");
        ctx.restart(&target)
    }
}
