//! The log world's view: one [`NodeView`] per node, whether it is running or
//! not.
//!
//! A crashed node still renders — from its **disk**, which is exactly what
//! survives a crash — so the stage never blanks a node out and the player can
//! see what a restart is about to read back.

use paros_core::{NodeId, QuorumSystem};

use crate::view::{
    AttemptView, ChosenView, ClientView, ElectionView, GapView, GridCellView, MatchmakerView,
    NodeFlavour, NodeView, ProposalView, ReachView, ReadRoundView, ReadView, SlotView,
    WorldFlavour, WorldView, quorum_view, show_ballot, show_role,
};
use crate::world::{Client, NO_CHECK_QUORUM, World, quorum_name};

impl World {
    /// Render the whole world.
    #[must_use]
    pub fn view(&self) -> WorldView {
        WorldView {
            flavour: WorldFlavour::Log,
            clock: self.clock,
            nodes: (0..self.pool.len()).map(|i| self.node_view(i)).collect(),
            wire: self.wire.iter().map(|entry| self.render(entry)).collect(),
            clients: self.clients.iter().map(client_view).collect(),
            matchmakers: Vec::<MatchmakerView>::new(),
            chosen: None::<ChosenView>,
            // The log world has no reach: a partition here is the player not
            // delivering, message by message.
            reach: None::<ReachView>,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn node_view(&self, index: usize) -> NodeView {
        let id = self.pool[index];
        let disk = &self.disks[index];
        let Some(node) = self.nodes[index].as_ref() else {
            // A crashed node shows its disk: that is exactly what survives.
            return NodeView {
                id: id.0,
                flavour: NodeFlavour::Colocated,
                alive: false,
                role: None,
                attempt: None::<AttemptView>,
                ballot: None,
                leader: None,
                promised: Some(show_ballot(disk.hard_state().max_promised_ballot)),
                accepted: disk
                    .records()
                    .iter()
                    .map(|(slot, (ballot, command))| {
                        let chosen = disk.hard_state().chosen_index.is_some_and(|ci| *slot <= ci);
                        SlotView::accepted(*slot, *ballot, command, chosen, disk.has_applied(*slot))
                    })
                    .collect(),
                chosen_index: disk.hard_state().chosen_index.map(|s| s.0),
                first_unchosen: None,
                next_slot: None,
                chosen_gap: None,
                floor: Some(disk.floor().0),
                election: None,
                open_rounds: Vec::new(),
                pending_accepts: false,
                read_rounds: Vec::new(),
                recovery_remaining: 0,
                acceptors: disk.config().peers.iter().map(|n| n.0).collect(),
                quorum_system: quorum_name(disk.config().quorum_system),
                quorum: quorum_view(disk.config().quorum_system),
                grid_cell: grid_cell(&disk.config().peers, disk.config().quorum_system, id),
                applied: disk
                    .applied()
                    .iter()
                    .map(|(slot, command)| SlotView::applied_entry(*slot, command))
                    .collect(),
                armed_seam: self.armed_seams[index],
            };
        };
        let accepted = node
            .acceptor()
            .records()
            .iter()
            .map(|(slot, (ballot, command))| {
                SlotView::accepted(
                    *slot,
                    *ballot,
                    command,
                    node.replica().is_chosen(*slot),
                    disk.has_applied(*slot),
                )
            })
            .collect();
        let timeout = node.election_timeout();
        NodeView {
            id: id.0,
            flavour: NodeFlavour::Colocated,
            alive: true,
            role: Some(show_role(node.role())),
            attempt: None::<AttemptView>,
            ballot: Some(show_ballot(node.ballot())),
            leader: node.leader().map(|l| l.0),
            promised: Some(show_ballot(node.acceptor().promised())),
            accepted,
            chosen_index: node.replica().chosen_index().map(|s| s.0),
            first_unchosen: Some(node.replica().first_unchosen().0),
            next_slot: Some(node.proposer().next_slot().0),
            chosen_gap: node.replica().chosen_gap().map(|(hole, highest)| GapView {
                hole: hole.0,
                highest: highest.0,
            }),
            floor: Some(node.acceptor().first_slot().0),
            election: Some(ElectionView {
                timeout,
                held: timeout == NO_CHECK_QUORUM,
            }),
            open_rounds: node.proposer().rounds().keys().map(|s| s.0).collect(),
            pending_accepts: node.has_pending_accepts(),
            read_rounds: self.read_round_views(id),
            recovery_remaining: node.proposer().recovery_remaining(),
            acceptors: node.acceptors().members().iter().map(|n| n.0).collect(),
            quorum_system: quorum_name(node.acceptors().quorum_system()),
            quorum: quorum_view(node.acceptors().quorum_system()),
            grid_cell: grid_cell(
                node.acceptors().members(),
                node.acceptors().quorum_system(),
                id,
            ),
            applied: disk
                .applied()
                .iter()
                .map(|(slot, command)| SlotView::applied_entry(*slot, command))
                .collect(),
            armed_seam: self.armed_seams[index],
        }
    }

    fn read_round_views(&self, id: NodeId) -> Vec<ReadRoundView> {
        self.clients
            .iter()
            .flat_map(|c| c.reads.iter())
            .filter(|r| r.node == id && !r.served)
            .map(|r| ReadRoundView {
                ctx: r.ctx,
                index: r.index.map(|s| s.0),
                acks: r.acks.len(),
            })
            .collect()
    }
}

/// Where `id` sits in the grid `system` lays over the sorted `members`:
/// member `i` at `(i / cols, i % cols)`, exactly as the configuration does it.
/// `None` for every system that lays out no grid.
fn grid_cell(members: &[NodeId], system: QuorumSystem, id: NodeId) -> Option<GridCellView> {
    let QuorumSystem::Grid { cols, .. } = system else {
        return None;
    };
    let index = members.iter().position(|member| *member == id)?;
    let cols = cols.max(1);
    Some(GridCellView {
        row: u64::try_from(index / cols).unwrap_or(0),
        column: u64::try_from(index % cols).unwrap_or(0),
    })
}

fn client_view(client: &Client) -> ClientView {
    ClientView {
        id: client.id.0,
        proposals: client
            .proposals
            .iter()
            .map(|p| ProposalView {
                seq: p.seq.0,
                value: p.value.clone(),
                node: p.node.0,
                slot: p.slot.map(|s| s.0),
                acked: p.acked,
            })
            .collect(),
        reads: client
            .reads
            .iter()
            .map(|r| ReadView {
                ctx: r.ctx,
                node: r.node.0,
                index: r.index.map(|s| s.0),
                served: r.served,
            })
            .collect(),
    }
}
