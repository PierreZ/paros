//! The operator's moves on the pool of a cell with matchmakers: retire a
//! node ([`Action::Retire`](super::Action::Retire)), book a node again
//! ([`Action::Rejoin`](super::Action::Rejoin)), and admit a late node to the
//! pool of one node ([`Action::Admit`](super::Action::Admit)).
//!
//! The operator retires a node only on the evidence the core asks for: a
//! GC floor that a leader made effective at a matchmaker quorum
//! (`ColocatedNode::gc_effective`), or the floor a beat carried to a node
//! whose slot was released (`ColocatedNode::may_retire_heard`). It keeps
//! its own half of the contract (root `AGENTS.md`, *Retire carries the
//! evidence*): it never retires a node that a reconfiguration it asked for
//! above the floor names. A retired node is down for good, until the
//! operator books it again: it then boots from its disk and rejoins.

use moonpool_walk::{Cx, sometimes};
use paros_consensus::{Ballot, NodeId};

use super::{Cell, reach};

impl Cell {
    /// Retire one node that the evidence releases, or ask the matchmakers
    /// to check it when the check is still missing.
    pub(super) fn retire(&mut self, cx: &mut Cx<'_, Self>) {
        let leader = self.pick_leader(cx);
        let floor = leader.and_then(|id| {
            let node = self.node(id).live.as_ref()?;
            node.gc_effective().map(|(floor, _)| floor)
        });
        let candidates: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|site| Some(site.id) != leader && site.live.is_some())
            .map(|site| site.id)
            .collect();
        let Some(&id) = cx.rng().pick(&candidates) else {
            return;
        };
        let heard = floor.is_none() || cx.rng().chance(1, 3);
        let node = self.node_mut(id).live.as_mut().expect("a candidate is up");
        let may = if heard {
            let may = node.may_retire_heard();
            if !may {
                let _ = node.check_retirement_heard();
            }
            may && node
                .gc_floor_heard()
                .is_some_and(|floor| !self.asked_names(id, floor))
        } else {
            let floor = floor.expect("a floor on the operator's path");
            if self.asked_names(id, floor) {
                return;
            }
            let node = self.node_mut(id).live.as_mut().expect("a candidate is up");
            let may = node.may_retire(floor);
            if !may {
                let _ = node.check_retirement(floor);
            }
            may
        };
        if may {
            sometimes!(true, reach::RETIRED);
            self.retired.insert(id);
            self.crash_node(id);
            cx.crashed(&super::Party::Node(id));
            return;
        }
        self.drain_node(id, cx);
    }

    /// Whether a reconfiguration the operator asked for above `floor`
    /// names `id`: the operator's half of a retirement.
    fn asked_names(&self, id: NodeId, floor: Ballot) -> bool {
        self.asked
            .iter()
            .any(|(ballot, members)| *ballot > floor && members.contains(&id))
    }

    /// Book a retired node again: it boots from its disk, and its boot
    /// rejoins. Or book again a node that runs and is no member: it
    /// rejoins with no reboot.
    pub(super) fn rejoin(&mut self, cx: &mut Cx<'_, Self>) {
        let retired: Vec<NodeId> = self.retired.iter().copied().collect();
        if let Some(&id) = cx.rng().pick(&retired) {
            sometimes!(true, reach::REBOOKED);
            self.retired.remove(&id);
            self.boot_node(id, true, cx);
            return;
        }
        let running: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|site| site.live.as_ref().is_some_and(|node| !node.is_acceptor()))
            .map(|site| site.id)
            .collect();
        let Some(&id) = cx.rng().pick(&running) else {
            return;
        };
        let node = self.node_mut(id).live.as_mut().expect("a running node");
        let _ = node.rejoin();
        self.drain_node(id, cx);
    }

    /// One node admits one late node to its pool.
    pub(super) fn admit(&mut self, cx: &mut Cx<'_, Self>) {
        let late = self.late_ids();
        // A lagging-reader walk admits the late nodes only at node 0: when
        // node 0 or a late node leads, it moves onto a late node while every
        // other pool lags until the settle rounds (`Cell::tick_all`).
        let steer = self.flavor.lagging_reader;
        let pairs: Vec<(NodeId, NodeId)> = self
            .nodes
            .iter()
            .filter(|site| site.live.is_some())
            .filter(|site| !steer || site.id == NodeId(0))
            .flat_map(|site| late.iter().map(move |l| (site.id, *l)))
            .filter(|(id, l)| id != l && !self.admitted(*id).contains(l))
            .collect();
        let Some(&(id, late)) = cx.rng().pick(&pairs) else {
            return;
        };
        let node = self.node_mut(id).live.as_mut().expect("a live node");
        let grew = node.extend_pool(&[late]);
        sometimes!(grew, reach::ADMITTED);
        self.admitted.entry(id).or_default().insert(late);
    }

    /// Every node admits every late node.
    pub(super) fn admit_all(&mut self) {
        let late = self.late_ids();
        if late.is_empty() {
            return;
        }
        for site in &mut self.nodes {
            let known = self.admitted.entry(site.id).or_default();
            known.extend(late.iter().filter(|l| **l != site.id));
            if let Some(node) = site.live.as_mut() {
                let _ = node.extend_pool(&late);
            }
        }
    }
}
