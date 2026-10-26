//! The **lagging-replica scenario** (#550 (replica serves an unreported fold
//! page)): on a seed that draws it (`crate::shape::lagging_replica`), one
//! replica goes down early in the chaos window and stays down until a node
//! of the cell decided [`AHEAD`] slots past the chosen prefix the replica
//! holds (`crate::audit::AuditWorld::cell_ahead_of`), or until
//! [`LATE_WINDOW`] into the recovery tail. It then boots behind the cell and
//! catches up through an apply walk that spans several pages (every replica
//! walks a small page on a scenario seed, `crate::shape::replica_apply_page`),
//! while the clients' reads keep reaching it.
//!
//! A replica the attrition takes down comes back fast, and the apply page
//! is small on few seeds, so a replica that pages a backlog while it serves
//! reads is rare: the witness of #550 was 1 red in 1,000 hunt seeds, and only
//! with a local amplification. Like the lagging acceptor
//! (`super::lagging_acceptor`), the hold may reach `LATE_WINDOW` into the
//! tail; a replica is in no quorum, so the cell never waits on it, and the
//! tail after the restart is still the recovery the oracles judge.
//!
//! A replica stores on a quiet seat, outside the copy budget (#144): a cut
//! there is torn or whole and spends no budget, so the crash needs no
//! permit from the cut ledger.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable, sim_random_range,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// When the replica goes down, from the opening of the chaos window: once
/// the cell formed and the journal has a leader on most seeds, and early
/// enough that the cell decides a backlog without it.
const START: std::ops::Range<u64> = 800..2_000;

/// How far the cell must be past the replica's prefix before it comes back:
/// three pages of the largest small page (`crate::shape::replica_apply_page`
/// draws 1 or 2), so the catch-up spans several pages on every scenario seed.
/// A run decides a dozen slots or so: a hold of 16 never ended before the
/// tail in 30 hunt seeds.
pub(crate) const AHEAD: u64 = 6;

/// How often the injector checks the cell's progress while the replica is
/// down.
const WATCH: Duration = Duration::from_millis(20);

/// The lagging-replica injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct LaggingReplica;

#[async_trait]
impl FaultInjector for LaggingReplica {
    fn name(&self) -> &'static str {
        "paros-lagging-replica"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::lagging_replica(ctx.state()) {
            return Ok(());
        }
        let replicas = ctx.ips_in_group(crate::roles::REPLICA_GROUP);
        if replicas.is_empty() {
            return Ok(());
        }
        let time = ctx.time();
        if time
            .sleep(Duration::from_millis(sim_random_range(START)))
            .await
            .is_err()
        {
            return Ok(());
        }
        // Replica ids are their ranks in the replica group, offset.
        let rank = sim_random_range(0..u64::try_from(replicas.len()).unwrap_or(0));
        let Some(target) = usize::try_from(rank)
            .ok()
            .and_then(|rank| replicas.get(rank))
            .cloned()
        else {
            return Ok(());
        };
        let id = crate::roles::replica_node_id(paros::core::ReplicaId(rank)).0;
        loop {
            if crate::past_cutoff(time.now(), LATE_WINDOW) || ctx.chaos_shutdown().is_cancelled() {
                return Ok(());
            }
            if !ctx.is_dead(&target) {
                break;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
        ctx.crash(&target)?;
        assert_reachable!("storage: the lagging-replica scenario holds a replica down");
        tracing::info!(%target, rank, "lagging_replica_down");
        let audit = crate::audit::audit_world(ctx.state());
        let ahead = loop {
            if audit.cell_ahead_of(id, AHEAD) {
                break true;
            }
            if crate::past_cutoff(time.now(), LATE_WINDOW) || time.sleep(WATCH).await.is_err() {
                break false;
            }
        };
        if ahead {
            assert_reachable!("storage: the cell decides a backlog past a held replica");
        }
        tracing::info!(%target, ahead, "lagging_replica_back");
        ctx.restart(&target)
    }
}
