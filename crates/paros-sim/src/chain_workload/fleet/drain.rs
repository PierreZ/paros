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
//! journal create, and admits machines until then.

use std::collections::BTreeSet;

use moonpool_sim::{SimContext, assert_reachable};
use paros::NodeId;
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::machine::Class;
use paros::system::{BookingTarget, NodeStanding, Role};

use super::FleetOps;

impl FleetOps {
    /// Drain one registered admitted storage machine, when one is
    /// registered; with `loaded`, only one that holds a journal's slot
    /// another storage machine could take. Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn drain(
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
        let founders: Vec<(NodeId, paros::Address)> = cell
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
        // The storage machines a journal may go to: the founding members and
        // every registered storage machine.
        let storage: BTreeSet<NodeId> = cell
            .members
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
        // Whether `node` holds a journal's slot, or a matchmaker set's (#501
        // (matchmaker sets off the founders)), that another storage machine
        // could take.
        let movable = |node: NodeId| {
            registry.bookings().any(|(_, b)| {
                let holders: BTreeSet<NodeId> = registry
                    .bookings()
                    .filter(|(_, o)| o.target == b.target && o.role.class() == Class::Storage)
                    .map(|(_, o)| o.node)
                    .collect();
                let moves = match b.target {
                    BookingTarget::Journal(_) => b.role == Role::Acceptor,
                    BookingTarget::Set { .. } => b.role == Role::Matchmaker,
                };
                b.node == node && moves && storage.iter().any(|m| !holders.contains(m))
            })
        };
        // The scenario drains one machine at a time, so a journal keeps
        // machines to go to.
        let draining = registry
            .nodes()
            .any(|(_, n)| n.standing == NodeStanding::Draining);
        if loaded && draining {
            return false;
        }
        let admitted: Vec<NodeId> = if loaded {
            admitted.into_iter().filter(|node| movable(*node)).collect()
        } else {
            admitted
        };
        if admitted.is_empty() {
            return false;
        }
        let node = admitted[usize::try_from(draw % admitted.len() as u64).unwrap_or(0)];
        if let Ok(true) = session.drain(&cell.client, first, node).await {
            assert_reachable!("drain: an operator drains an admitted machine");
            tracing::info!(node = node.0, "machine_drained");
        }
        true
    }
}
