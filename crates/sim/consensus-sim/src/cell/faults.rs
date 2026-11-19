//! The disk faults of a walk ([`Action::Rot`](super::Action::Rot)): a disk
//! loses the value of one record and keeps its identity, the recoverable
//! faulty entry of CTRL (Stage 8). A node that runs may find the damage by
//! a scrub and write the record again from its own copy
//! (`ColocatedNode::repair_record`); a node that boots on the disk reports
//! the slot faulty and repairs it from its peers.

use moonpool_walk::{Cx, sometimes};
use paros_consensus::{Slot, Storage};

use super::{Cell, reach};

impl Cell {
    /// Rot one readable record of one node's disk. At most one disk holds a
    /// faulty copy of a slot at a time: CTRL repairs a faulty record from
    /// the readable copies of its peers, and a fault on every copy of a
    /// chosen slot loses the slot, which no protocol recovers.
    pub(super) fn rot(&mut self, cx: &mut Cx<'_, Self>) {
        let ids: Vec<_> = self.nodes.iter().map(|site| site.id).collect();
        let Some(&id) = cx.rng().pick(&ids) else {
            return;
        };
        let faulty: Vec<Slot> = self
            .nodes
            .iter()
            .flat_map(|site| site.disk.faulty_entries())
            .map(|(slot, _)| slot)
            .collect();
        let disk = &self.node(id).disk;
        let readable: Vec<Slot> = (disk.first_slot().0..=disk.last_slot().0)
            .map(Slot)
            .filter(|slot| disk.accepted(*slot).is_some() && !faulty.contains(slot))
            .collect();
        let Some(&slot) = cx.rng().pick(&readable) else {
            return;
        };
        let chosen = disk.hard_state().chosen_index.is_some_and(|c| slot <= c);
        sometimes!(chosen, reach::ROT_CHOSEN);
        self.node_mut(id).rot(slot);
        // A scrub finds the damage while the node runs, or it stays latent
        // until the next boot reads the disk.
        if cx.rng().chance(1, 2)
            && let Some(node) = self.node_mut(id).live.as_mut()
        {
            let rewritten = node.repair_record(slot);
            sometimes!(rewritten.is_some(), reach::SCRUBBED);
            self.drain_node(id, cx);
        }
    }
}
