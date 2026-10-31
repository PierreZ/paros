//! The control plane's mode (#685 (special tenants are tenants)): on its
//! own BUGGIFY location of `ADMIT`, the operator sets how the control
//! plane's `F` is chosen, as `parosctl cell configure --control-plane`
//! does, through `CellSession::configure_control_plane`. It draws `auto`
//! with a target of 0 to 3, or `manual` with an `F` of 0 to 3: a manual
//! `F` the cell's storage machines cannot place is refused at apply.
//!
//! The oracles: an applied request is the mode the registry holds (the
//! library asserts it); a refused one was a manual `F` wider than the
//! storage machines the registry held when it folded the request; an auto
//! mode is never refused.

use moonpool_sim::{RandomProvider, SimContext, assert_always, assert_reachable};
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::core::NodeId;
use paros::model::registry::control_plane::{self, ControlPlaneMode, MAX_FAILURES};

use super::FleetOps;

impl FleetOps {
    /// Set the control plane's mode (see the module doc). Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn control_plane_step(
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
        let first = cell.first(draw);
        let failures = ctx.random().random_range(0..MAX_FAILURES + 1);
        let mode = if draw.is_multiple_of(2) {
            ControlPlaneMode::Auto { target: failures }
        } else {
            ControlPlaneMode::Manual { failures }
        };
        let request = ctx.random().random_range(1..u64::MAX);
        let Ok(applied) = session
            .configure_control_plane(&cell.client, first, request, mode)
            .await
        else {
            return true;
        };
        let storage = session.registry().storage_machines();
        if applied {
            match mode {
                ControlPlaneMode::Auto { .. } => {
                    assert_reachable!("control plane: an operator sets the auto mode");
                }
                ControlPlaneMode::Manual { .. } => {
                    assert_reachable!("control plane: an operator sets the manual mode");
                }
            }
        } else {
            assert_reachable!("control plane: a manual mode too wide is refused");
            assert_always!(
                !mode.is_auto(),
                "control plane: an auto mode is never refused",
                { "mode" => mode.to_string() }
            );
            // The refusal was judged where the request folded; a machine
            // that registered after it may since make it fit, never the
            // reverse while no machine was drained.
            if storage < control_plane::acceptors(failures) {
                assert_reachable!("control plane: a refused mode still does not fit");
            }
        }
        true
    }
}
