//! The operator's moves on the pool of a cluster with matchmakers: retire a
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
use paros_consensus::membership::ProxyId;
use paros_consensus::{Ballot, NodeId};

use super::{Cluster, ClusterEvent, Mutant, Steer, reach};

impl Cluster {
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
        let sticky = self
            .checking
            .filter(|id| self.flavor.steer == Steer::Retiring && candidates.contains(id));
        let Some(id) = sticky.or_else(|| cx.rng().pick(&candidates).copied()) else {
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
        self.checking = (!may).then_some(id);
        if may || self.flavor.mutant == Some(Mutant::RetireUnchecked) {
            sometimes!(true, reach::RETIRED);
            cx.record(ClusterEvent::Retire { node: id });
            self.retired.insert(id);
            self.crash_node(id, cx);
            cx.crashed(&super::Party::Node(id));
            return;
        }
        self.drain_node(id, cx);
    }

    /// One operator move of a [`Steer::Retiring`](super::Steer::Retiring)
    /// walk: a reconfiguration, a retirement or a rejoin.
    pub(super) fn operate(&mut self, cx: &mut Cx<'_, Self>) {
        match cx.rng().below(3) {
            0 => self.reconfigure(cx),
            1 => self.retire(cx),
            _ => self.rejoin(cx),
        }
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
            cx.record(ClusterEvent::Rejoin { node: id });
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

    /// One party admits one late node to its pool: a node, a replica or a
    /// proxy leader. Each pool lags on its own, as a deployment's registry
    /// reaches each process at its own time.
    pub(super) fn admit(&mut self, cx: &mut Cx<'_, Self>) {
        let late = self.late_ids();
        // A lagging-reader walk admits the late nodes only at node 0: when
        // node 0 or a late node leads, it moves onto a late node while every
        // other pool lags until the settle rounds (`Cluster::tick_all`).
        let steer = self.flavor.steer == Steer::LaggingReader;
        let nodes = self
            .nodes
            .iter()
            .filter(|site| site.live.is_some())
            .filter(|site| !steer || site.id == NodeId(0))
            .map(|site| Admitter::Node(site.id));
        // A replica's pool lags as a node's does: it ignores what a late
        // node teaches until it admits it.
        let replicas = self
            .replicas
            .iter()
            .filter(|site| site.live.is_some() && !steer)
            .map(|site| Admitter::Replica(site.id));
        // A proxy leader's pool lags too: a delegation whose configuration
        // names a late node it has not admitted is ignored.
        let proxies = self
            .proxies
            .iter()
            .filter(|site| site.live.is_some() && !steer)
            .map(|site| Admitter::Proxy(site.id));
        let pairs: Vec<(Admitter, NodeId)> = nodes
            .chain(replicas)
            .chain(proxies)
            .flat_map(|who| late.iter().map(move |l| (who, *l)))
            .filter(|(who, l)| !self.admits(*who, *l))
            .collect();
        let Some(&(who, late)) = cx.rng().pick(&pairs) else {
            return;
        };
        match who {
            Admitter::Node(id) => {
                let node = self.node_mut(id).live.as_mut().expect("a live node");
                let grew = node.extend_pool(&[late]);
                sometimes!(grew, reach::ADMITTED);
                self.admitted.entry(id).or_default().insert(late);
            }
            Admitter::Replica(id) => {
                let replica = self.replica_mut(id).live.as_mut().expect("a live replica");
                let grew = replica.extend_pool(&[late]);
                sometimes!(grew, reach::REPLICA_ADMITTED);
                self.admitted.entry(id).or_default().insert(late);
            }
            Admitter::Proxy(id) => {
                let proxy = self.proxy_mut(id).live.as_mut().expect("a live proxy");
                let grew = proxy.extend_pool(&[late]);
                sometimes!(grew, reach::PROXY_ADMITTED);
                self.proxy_admitted.entry(id).or_default().insert(late);
            }
        }
    }

    /// Whether `who` already holds `late` in its pool: its own id, or a
    /// late node it admitted.
    fn admits(&self, who: Admitter, late: NodeId) -> bool {
        match who {
            Admitter::Node(id) | Admitter::Replica(id) => {
                id == late || self.admitted(id).contains(&late)
            }
            Admitter::Proxy(id) => self
                .proxy_admitted
                .get(&id)
                .is_some_and(|known| known.contains(&late)),
        }
    }

    /// Every node, replica and proxy leader admits every late node.
    pub(super) fn admit_all(&mut self) {
        let late = self.late_ids();
        if late.is_empty() {
            return;
        }
        self.admit_nodes_and_replicas();
        for site in &mut self.proxies {
            self.proxy_admitted
                .entry(site.id)
                .or_default()
                .extend(late.iter().copied());
            if let Some(proxy) = site.live.as_mut() {
                let _ = proxy.extend_pool(&late);
            }
        }
    }

    /// Every node and every replica admits every late node; the proxy
    /// leaders do not.
    pub(super) fn admit_nodes_and_replicas(&mut self) {
        let late = self.late_ids();
        for site in &mut self.nodes {
            let known = self.admitted.entry(site.id).or_default();
            known.extend(late.iter().filter(|l| **l != site.id));
            if let Some(node) = site.live.as_mut() {
                let _ = node.extend_pool(&late);
            }
        }
        for site in &mut self.replicas {
            self.admitted
                .entry(site.id)
                .or_default()
                .extend(late.iter().copied());
            if let Some(replica) = site.live.as_mut() {
                let _ = replica.extend_pool(&late);
            }
        }
    }
}

/// A party whose pool may lag: it admits a late node on its own.
#[derive(Clone, Copy)]
enum Admitter {
    Node(NodeId),
    Replica(NodeId),
    Proxy(ProxyId),
}
