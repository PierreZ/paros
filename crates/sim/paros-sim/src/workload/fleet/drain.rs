//! A drain (#474 (journals on admitted machines)): on its own BUGGIFY
//! location in `ADMIT`, the operator drains a registered admitted storage
//! machine through `paros::client::cell::CellSession::drain`, the code
//! `parosctl` would print. The cell coordinator then moves every journal off
//! it. The operator's session fences the coordinator's, so the term that
//! started a move often resigns, and a later term must finish it. A
//! journal may have no machine to move to: its move waits. On a
//! drained-machine seed (`crate::shape::drained_machine`), the operator
//! drains one machine at a time, only one that holds a journal's or a
//! matchmaker set's slot another storage machine could take (#501
//! (matchmaker sets off the founders)), at an `ADMIT` or right after a
//! journal create, and admits machines until then. On its own BUGGIFY
//! location, the operator drains a machine renamed since it joined (#504
//! (red drain seeds)).
//!
//! On a coin, the operator drains a machine that holds a tenant's batcher
//! (#145 (batchers)): the cell coordinator releases that slot. On a
//! batchers seed (`crate::shape::batchers`), the operator does so at every
//! journal step of the chaos window, once a read of the registry shows a
//! booked batcher, until one drain landed in the run.
//!
//! Once a drained machine holds no slot, the operator removes it on a coin
//! (`CellSession::remove`, #359): its `node_id` is tombstoned for good.
//!
//! On a gone-machine seed (`crate::shape::gone_machine`, #475 (re-place
//! after the Down bound)), the operator picks the machine the same way but
//! does not drain it: `crate::world::scenarios::gone_machine` takes it down for longer
//! than the re-placement bound, so the cell coordinator must re-place its
//! roles. On a coin it first opens a maintenance window over the machine
//! or its zone (#359): a short one that ends while the machine is down, or
//! one that outlasts the run, under which no slot it held may move.

use std::collections::BTreeSet;

use moonpool_sim::{SimContext, assert_reachable, buggify_with_prob};
use paros::client::cell::{CellSession, Removal};
use paros::client::checkpoint::CheckpointPolicy;
use paros::core::NodeId;
use paros::machine::Class;
use paros::model::registry::{BookingTarget, NodeStanding, Registry, Role};

use super::FleetOps;

/// Well-known `StateHandle` key: a batchers seed's one drain of a batcher's
/// machine landed (#145 (batchers)).
const BATCHER_DRAINED_KEY: &str = "paros-batcher-drained";

/// Whether a drain of a batcher's machine landed in this run, on a
/// batchers seed (`crate::shape::batchers`): the scenario drains one.
pub(in crate::workload) fn batcher_drained(state: &moonpool_sim::StateHandle) -> bool {
    *crate::state::published(state, BATCHER_DRAINED_KEY, || false)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl FleetOps {
    /// Drain one registered admitted storage machine, when one is
    /// registered; with `loaded`, only one that holds a journal's slot
    /// another storage machine could take. Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn drain(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
        loaded: bool,
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
        if session.open(&cell.client, first).await.is_err() {
            return false;
        }
        let admitted: Vec<NodeId> = session
            .registry()
            .nodes()
            .filter(|(id, n)| {
                !cell.members.contains(&id.0)
                    && n.standing == NodeStanding::Registered
                    && n.class == Class::Storage
            })
            .map(|(id, _)| id)
            .collect();
        let registry = session.registry();
        // The scenario drains one machine at a time, so a journal keeps
        // machines to go to. A drained one, holding no slot, is removed on a
        // coin (#359).
        let drained: Vec<NodeId> = registry
            .nodes()
            .filter(|(id, n)| n.standing == NodeStanding::Draining && registry.booked(*id) == 0)
            .map(|(id, _)| id)
            .collect();
        if let Some(node) = drained.first().copied()
            && buggify_with_prob!(0.5)
        {
            return self
                .remove_machine(ctx, &mut session, (&cell.client, first), node)
                .await;
        }
        let draining = registry
            .nodes()
            .any(|(_, n)| n.standing == NodeStanding::Draining);
        if loaded && draining {
            return false;
        }
        let gone = crate::shape::gone_machine(ctx.state());
        if gone && crate::machine::gone_requested(ctx.state()) {
            return false;
        }
        let mut admitted: Vec<NodeId> = if loaded {
            admitted
                .into_iter()
                .filter(|node| movable(registry, &cell.members, *node))
                .collect()
        } else {
            admitted
        };
        // On its own location, a machine renamed since it took its slots
        // (#504 (red drain seeds), witness 14650518500742172559 on
        // e61b30e): its peers must follow its new name for its journals to
        // elect the leader a move asks to reconfigure (#454 (control plane
        // meets chaos), `readdress_peer`).
        let renamed = crate::machine::renamed_ids(ctx.state());
        if admitted.iter().any(|node| renamed.contains(&node.0)) && buggify_with_prob!(0.5) {
            admitted.retain(|node| renamed.contains(&node.0));
            assert_reachable!("drain: an operator drains a renamed machine");
        }
        if admitted.is_empty() {
            return false;
        }
        let node = admitted[usize::try_from(draw % admitted.len() as u64).unwrap_or(0)];
        if gone {
            // A window first, on a coin, then the machine goes down (#475).
            if buggify_with_prob!(0.75)
                && let Some(scope) = super::windows::scope_of(&session, node)
            {
                let for_ms = super::windows::window_ms(ctx);
                self.send_window(ctx, &mut session, (&cell.client, first), scope, for_ms)
                    .await;
            }
            if crate::machine::request_gone(ctx.state(), node.0) {
                assert_reachable!("drain: an operator picks a machine to take down");
                tracing::info!(node = node.0, "machine_gone_requested");
            }
            return true;
        }
        if let Ok(true) = session.drain(&cell.client, first, node).await {
            assert_reachable!("drain: an operator drains an admitted machine");
            tracing::info!(node = node.0, "machine_drained");
        }
        true
    }

    /// The batchers scenario's drain (#145 (batchers)): read the registry
    /// without a session, and only when it books a tenant's batcher,
    /// drain that machine ([`FleetOps::drain_batcher`]). A session fences
    /// the coordinator's term, so an operator that opened one at every step
    /// would keep the coordinator from booking. Records a drain that
    /// landed ([`batcher_drained`]).
    pub(in crate::workload) async fn drain_booked_batcher(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        let Some(cell) = self.learn(ctx).await else {
            return;
        };
        let mut folded = super::judged_cell(&cell.members);
        let loaded = paros::client::checkpoint::load(
            &mut folded,
            cell.journals.cell,
            &cell.client,
            cell.first(draw),
            0,
        )
        .await;
        if !matches!(
            loaded,
            paros::client::checkpoint::LoadOutcome::Loaded { .. }
        ) || batcher_host(folded.state().registry()).is_none()
        {
            return;
        }
        if self.drain_batcher(ctx, policy, draw).await {
            *crate::state::published(ctx.state(), BATCHER_DRAINED_KEY, || false)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        }
    }

    /// Drain a registered machine that holds a tenant's batcher (#145
    /// (batchers)), when one does and no machine is draining: the cell
    /// coordinator releases the slot and books the count on the machines
    /// that stay. Not on a drained-machine or gone-machine seed, which
    /// drain one machine of their own. Whether the drain landed.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn drain_batcher(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        if crate::shape::drained_machine(ctx.state()) || crate::shape::gone_machine(ctx.state()) {
            return false;
        }
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
        if session.open(&cell.client, first).await.is_err() {
            return false;
        }
        let registry = session.registry();
        if registry
            .nodes()
            .any(|(_, n)| n.standing == NodeStanding::Draining)
        {
            return false;
        }
        let Some(node) = batcher_host(registry) else {
            return false;
        };
        if let Ok(true) = session.drain(&cell.client, first, node).await {
            assert_reachable!("drain: an operator drains a batcher's machine");
            tracing::info!(node = node.0, "machine_drained");
            return true;
        }
        false
    }

    /// Remove the drained machine `node` (#359), through `session`. Whether
    /// it ran.
    async fn remove_machine(
        &mut self,
        ctx: &SimContext,
        session: &mut CellSession,
        (client, first): (&crate::client::ChainClient, usize),
        node: NodeId,
    ) -> bool {
        crate::machine::note_remove_asked(ctx.state(), node.0);
        match session.remove(client, first, node).await {
            Ok(Removal::Removed) => {
                assert_reachable!("drain: an operator removes a drained machine");
                tracing::info!(node = node.0, "machine_removed");
                crate::machine::note_removed(ctx.state(), node.0);
                true
            }
            Ok(Removal::Unchanged) => true,
            Ok(Removal::NotDrained { .. } | Removal::NotDraining | Removal::Unknown) | Err(_) => {
                false
            }
        }
    }
}

/// Whether `node` holds a journal's slot, or a matchmaker set's (#501
/// (matchmaker sets off the founders)), that another storage machine could
/// take: a founding member (`members`) or a registered storage machine.
fn movable(registry: &Registry, members: &[u64], node: NodeId) -> bool {
    let storage: BTreeSet<NodeId> = members
        .iter()
        .map(|id| NodeId(*id))
        .chain(
            registry
                .nodes()
                .filter(|(_, n)| {
                    n.standing == NodeStanding::Registered && n.class == Class::Storage
                })
                .map(|(id, _)| id),
        )
        .collect();
    registry.bookings().any(|(_, b)| {
        let holders: BTreeSet<NodeId> = registry
            .bookings()
            .filter(|(_, o)| o.target == b.target && o.role.class() == Class::Storage)
            .map(|(_, o)| o.node)
            .collect();
        let moves = match b.target {
            BookingTarget::Journal(_) => b.role == Role::Acceptor,
            BookingTarget::Set { .. } => b.role == Role::Matchmaker,
            BookingTarget::Tenant { .. } => false,
        };
        b.node == node && moves && storage.iter().any(|m| !holders.contains(m))
    })
}

/// A registered machine that holds a journal's batcher or proxy leader
/// slot, or a tenant's frontend slot (#145 (batchers), #753 (per-journal
/// proxies), #723 (frontend as a parosd role)), if any: a drain moves
/// them, which the tenant coordinator places to the counts.
fn batcher_host(registry: &Registry) -> Option<NodeId> {
    registry
        .bookings()
        .filter(|(_, b)| matches!(b.role, Role::Batcher | Role::ProxyLeader | Role::Frontend))
        .map(|(_, b)| b.node)
        .find(|node| {
            registry
                .get(*node)
                .is_some_and(|n| n.standing == NodeStanding::Registered)
        })
}
