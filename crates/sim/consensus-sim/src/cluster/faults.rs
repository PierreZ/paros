//! The disk faults of a walk ([`Action::Rot`](super::Action::Rot)): a disk
//! loses the value of one record and keeps its identity, the recoverable
//! faulty entry of CTRL (Stage 8). A node that runs may find the damage by
//! a scrub and write the record again from its own copy
//! (`ColocatedNode::repair_record`); a node that boots on the disk reports
//! the slot faulty and repairs it from its peers.

use moonpool_walk::{Cx, sometimes};
use paros_consensus::{NodeId, Slot, Storage, WriteOp};

use super::{Cluster, reach};

impl Cluster {
    /// Rot one readable record of one node's disk, or, aimed at the split
    /// floor, every copy of one trimmed slot.
    ///
    /// At most one disk holds a faulty copy of a slot that no peer trimmed:
    /// CTRL repairs a faulty record from the readable copies of its peers,
    /// and a fault on every copy of a chosen slot loses the slot, which no
    /// protocol recovers. Below a peer's trim point the slot is chosen and
    /// the trim point carries the journal state its value folded to, so a
    /// fault on every copy that is left loses nothing: a node recovers the
    /// slot with the trim-point jump.
    pub(super) fn rot(&mut self, cx: &mut Cx<'_, Self>) {
        let faulty: Vec<Slot> = self
            .nodes
            .iter()
            .flat_map(|site| site.disk.faulty_entries())
            .map(|(slot, _)| slot)
            .collect();
        // Half the rots aim at the split floor, when the cluster has one.
        let split = self.split_floor();
        if !split.is_empty() && cx.rng().chance(1, 2) {
            let slot = *cx.rng().pick(&split).expect("a split slot");
            self.rot_split(slot, cx);
            return;
        }
        let ids: Vec<_> = self.nodes.iter().map(|site| site.id).collect();
        let Some(&id) = cx.rng().pick(&ids) else {
            return;
        };
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

    /// The split floor: each slot that a node already trimmed, that a
    /// second node still holds a readable copy of, and that a node which
    /// did not trim it has not learned chosen. That node learns the slot
    /// only from the trim point, and its campaign counts on the copies
    /// left below it.
    fn split_floor(&self) -> Vec<Slot> {
        let trimmed = self
            .nodes
            .iter()
            .map(|site| site.disk.first_slot())
            .max()
            .unwrap_or(Slot(0));
        let lowest = self
            .nodes
            .iter()
            .map(|site| site.disk.first_slot())
            .min()
            .unwrap_or(Slot(0));
        (lowest.0..trimmed.0)
            .map(Slot)
            .filter(|slot| {
                let below = |site: &&super::sites::NodeSite| site.disk.first_slot() <= *slot;
                let held = self
                    .nodes
                    .iter()
                    .filter(below)
                    .any(|site| site.disk.accepted(*slot).is_some());
                let lagging = self.nodes.iter().filter(below).any(|site| {
                    site.disk
                        .hard_state()
                        .chosen_index
                        .is_none_or(|c| c < *slot)
                });
                held && lagging
            })
            .collect()
    }

    /// Rot every readable copy of `slot` on the disks that did not trim it.
    /// A node that runs on such a disk meets the read error and stops: the
    /// walk restarts it later, and its boot reports the slot faulty. No
    /// scrub runs, so the campaign of the node that lags meets only faulty
    /// copies, its repair blocks on the slot, and the trim point of the
    /// node that trimmed it resolves the repair (the probe "a trim-point
    /// jump resolves a repair probe's blocked slot").
    pub(super) fn rot_split(&mut self, slot: Slot, cx: &mut Cx<'_, Self>) {
        let holders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|site| site.disk.first_slot() <= slot && site.disk.accepted(slot).is_some())
            .map(|site| site.id)
            .collect();
        assert!(!holders.is_empty(), "a split slot has a readable copy");
        sometimes!(true, reach::ROT_SPLIT_FLOOR);
        for &id in &holders {
            self.node_mut(id).rot(slot);
        }
        // The nodes that trimmed the slot stop too, so the nodes that lag
        // can elect a leader among themselves: its repair blocks on the
        // slot, and the trim point reaches it when a node that trimmed is
        // back and hears its beat.
        let down: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|site| holders.contains(&site.id) || site.disk.first_slot() > slot)
            .map(|site| site.id)
            .collect();
        for id in down {
            if self.node(id).live.is_some() {
                self.crash_node(id, cx);
                cx.crashed(&super::Party::Node(id));
            }
        }
    }

    /// After node `id` persisted `writes`: when they compact its log, a
    /// peer that lags may now need the trim point, and half the time a
    /// disk fault strikes the split floor at once, while the peers still
    /// hold the slots that this node dropped. Returns whether node `id`
    /// still runs.
    pub(super) fn rot_at_compaction(
        &mut self,
        id: NodeId,
        writes: &[WriteOp],
        cx: &mut Cx<'_, Self>,
    ) -> bool {
        let compacted = writes
            .iter()
            .any(|op| matches!(op, WriteOp::Truncate { .. }));
        if compacted && self.flavor.allows(super::Action::Rot) && cx.rng().chance(1, 2) {
            let split = self.split_floor();
            if let Some(&slot) = cx.rng().pick(&split) {
                self.rot_split(slot, cx);
            }
        }
        self.node(id).live.is_some()
    }
}
