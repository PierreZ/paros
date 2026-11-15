//! The **gone node** (PR #1064 (gone-node gap)): a removed acceptor whose
//! machine dies for good before anyone retires it. The operator's `RETIRE`
//! step takes the loss on its own BUGGIFY location
//! (`StorageWorld::go`, under the retirement's guards: the kill policy of
//! `FoundationDB`'s simulator, so the survivors still satisfy the
//! configuration in force). This injector crashes the node; its next boot
//! reads the park and stays down for the rest of the run.
//!
//! No matchmaker records a gone node, so a stale belief that names it stays
//! in every later history above the floor. Every later campaign must still
//! win: a candidate drops such a configuration once each owner of its
//! registrations testified it never opened Phase 1 there.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable};

/// How often the injector looks for a node the operator lost.
const WATCH: Duration = Duration::from_millis(50);

/// How long a gone node stays crashed before its boot reads the park.
const BEFORE_BOOT: Duration = Duration::from_millis(200);

/// The gone-node injector (see the module doc): a fresh one per timeline.
pub(crate) struct GoneNode;

#[async_trait]
impl FaultInjector for GoneNode {
    fn name(&self) -> &'static str {
        "paros-gone-node"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let time = ctx.time();
        loop {
            if ctx.chaos_shutdown().is_cancelled() || time.sleep(WATCH).await.is_err() {
                return Ok(());
            }
            let gone = crate::world::storage_world(ctx.state())
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take_gone();
            for target in gone {
                if !ctx.is_dead(&target) {
                    ctx.crash(&target)?;
                }
                assert_reachable!("gc: a gone node is crashed and never boots again");
                tracing::info!(%target, "gone_node_down");
                if time.sleep(BEFORE_BOOT).await.is_err() {
                    return Ok(());
                }
                // Its boot reads the park and exits: down for good.
                ctx.restart(&target)?;
            }
        }
    }
}
