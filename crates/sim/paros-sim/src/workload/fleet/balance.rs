//! The balancer's pause (#687 (rebalance a cell)): on its own BUGGIFY
//! location of `ADMIT`, the operator pauses or resumes the cell's balancer,
//! as `parosctl cell balance --pause` and `--resume` do, through
//! `CellSession::set_balance`. It resumes twice as often as it pauses, so a
//! run is mostly balanced.
//!
//! The oracle: an applied request is in the registry the session folded
//! (the library asserts it), and a pause never stops a move in flight: the
//! registry keeps it (`registry: a release ends a balance move`).

use moonpool_sim::{RandomProvider, SimContext, assert_reachable};
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::core::NodeId;

use super::FleetOps;

impl FleetOps {
    /// Pause or resume the cell's balancer (see the module doc). Whether
    /// it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn balance_step(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, paros::model::address::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            self.leader_seeds.next(),
            policy,
        );
        let pause = draw.is_multiple_of(3);
        let request = ctx.random().random_range(1..u64::MAX);
        let Ok(_) = session
            .set_balance(&cell.client, cell.first(draw), request, pause)
            .await
        else {
            return true;
        };
        if session.registry().balance().paused() && session.registry().balance().moving().is_some()
        {
            assert_reachable!("balance: a paused balancer still has a move in flight");
        }
        if pause {
            assert_reachable!("balance: an operator pauses the balancer");
        } else {
            assert_reachable!("balance: an operator resumes the balancer");
        }
        true
    }
}
