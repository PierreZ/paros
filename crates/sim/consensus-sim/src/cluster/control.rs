//! The journal controllers of a walk (#1564 (journal controller, version
//! 1), step 6): the driver gathers the feedback of every party that runs,
//! two controllers fold it into their views, decide with
//! `paros_consensus::controller::evaluate`, and the driver carries each
//! order with the calls of today (`ColocatedNode::reconfigure`,
//! `check_retirement`, `may_retire`).
//!
//! - The **fresh** controller sees each feedback that reaches it at once.
//!   In the chaos a feedback is lost on its way one time in four.
//! - The **stale** controller sees every feedback [`LAG`] control rounds
//!   late: a second controller with an older view, which two controllers at
//!   the same time make safe (§4.8.1 of `docs/architecture/`).
//!
//! When the faults stop, one member of the configuration in force stops for
//! good ([`Cluster::calm`](moonpool_walk::Model::calm)). Goal 2 is the
//! oracle ([`Cluster::healed`]): once the faults stop, within the settle
//! rounds of the walk, each member of the configuration in force runs and
//! the fresh controller is `Steady`, or it holds for a reason that the
//! oracle confirms from the cluster with its own count.

use std::collections::{BTreeSet, VecDeque};

use moonpool_walk::{Cx, Phase, sometimes};
use paros_consensus::NodeId;
use paros_consensus::contract::feedback::{MatchmakerFeedback, NodeFeedback, ProxyFeedback};
use paros_consensus::controller::{Decision, Order, Policy, Spare, View, evaluate};
use paros_consensus::node::ReconfigureResult;

use super::{Cluster, ClusterEvent, Mutant, reach};
use crate::oracle::healed::{Facts, HEALED, judge, judge_order};

/// The control rounds by which the stale controller lags the fresh one.
pub(super) const LAG: usize = 8;

/// The thresholds of the walk's controllers. A control round runs at each
/// tick of the cluster and reads every party that runs, so a party that runs
/// is seen at age 0 or 1 once the faults stop, and a member that stopped for
/// good is missed at each window of the leader. Measured on the `healing`
/// flavor, 200 seeds each: every policy of `max_age` in {0, 1, 2, 4, 16} and
/// `silent_windows` in {1, 2, 3, 5} heals every seed, so this walk does not
/// tell the thresholds apart, and they are no default of the core. The heal
/// takes 9 to 82 control rounds after the calm (p50 24, p90 41, p99 59), in
/// the 200 settle rounds that bound goal 2 here.
pub const POLICY: Policy = Policy {
    max_age: 4,
    silent_windows: 2,
};

/// One round of feedback: each party that runs, as the driver read it.
#[derive(Clone, Debug, Default)]
struct Round {
    nodes: Vec<NodeFeedback>,
    proxies: Vec<ProxyFeedback>,
    matchmakers: Vec<MatchmakerFeedback>,
}

/// The two controllers of a walk and what the driver knows about the pool.
#[derive(Debug, Default)]
pub(super) struct Controllers {
    /// The view of the fresh controller.
    fresh: View,
    /// The view of the stale controller.
    stale: View,
    /// The rounds that the stale controller has not seen yet, oldest first.
    behind: VecDeque<Round>,
    /// Each node that a configuration in force named: the driver offers
    /// only the other nodes of the pool as spares.
    members: BTreeSet<NodeId>,
    /// The member that stopped for good when the faults stopped.
    pub(super) dead: Option<NodeId>,
}

impl Cluster {
    /// One control round: the views age by a tick, the driver gathers the
    /// feedback, each controller decides, and the driver carries the order.
    pub(super) fn control(&mut self, cx: &mut Cx<'_, Self>) {
        if !self.flavor.controllers {
            return;
        }
        let chaos = cx.phase() == Phase::Chaos;
        let round = self.gather();
        let lost: Vec<bool> =
            (0..round.nodes.len() + round.proxies.len() + round.matchmakers.len())
                .map(|_| chaos && cx.rng().chance(1, 4))
                .collect();
        let spares = self.spares();
        let control = &mut self.control;
        control.fresh.tick();
        control.stale.tick();
        let late = self.flavor.mutant == Some(Mutant::LateView);
        if !late {
            observe(&mut control.fresh, &round, &lost);
        }
        control.behind.push_back(round);
        if control.behind.len() > LAG {
            let old = control.behind.pop_front().expect("a round behind");
            observe(&mut control.stale, &old, &[]);
            if late {
                observe(&mut control.fresh, &old, &[]);
            }
        }
        assert!(control.behind.len() <= LAG, "the stale view lags by LAG");
        let fresh = evaluate(&self.control.fresh, &spares, &POLICY);
        let stale = evaluate(&self.control.stale, &spares, &POLICY);
        sometimes!(
            matches!(fresh, Decision::Order(Order::Reconfigure { .. })),
            reach::CONTROL_RECONFIGURE
        );
        sometimes!(
            matches!(fresh, Decision::Order(Order::Retire { .. })),
            reach::CONTROL_RETIRE
        );
        sometimes!(
            matches!(stale, Decision::Order(_)),
            reach::CONTROL_STALE_ORDER
        );
        if !chaos && let Decision::Order(order) = &fresh {
            let members = self.leader_members();
            judge_order(order, &members, |id| self.node(id).live.is_some());
        }
        for decision in [fresh, stale] {
            if let Decision::Order(order) = decision {
                self.carry(order, cx);
            }
        }
    }

    /// The feedback of every party that runs, and the members of every
    /// configuration in force that a leader reports.
    fn gather(&mut self) -> Round {
        let mut round = Round::default();
        for site in &self.nodes {
            let Some(node) = &site.live else { continue };
            round.nodes.push(node.feedback());
            if node.is_leader() {
                self.control
                    .members
                    .extend(node.acceptors().members().iter().copied());
            }
        }
        for site in &self.proxies {
            if let Some(proxy) = &site.live {
                round.proxies.push(proxy.feedback());
            }
        }
        for site in &self.matchmakers {
            if let Some(matchmaker) = &site.live {
                round.matchmakers.push(matchmaker.feedback());
            }
        }
        round
    }

    /// The spares that the driver offers: each node of the pool that no
    /// configuration in force named, that is not retired, that did not
    /// stop for good and that does not leave the cell, in the order of the
    /// pool. A node that was a member is no spare: rule 6 retires it once a
    /// floor is effective, as it retires the node that leaves.
    fn spares(&self) -> Vec<Spare> {
        let spares: Vec<Spare> = self
            .pool
            .iter()
            .copied()
            .filter(|id| Some(*id) != self.leaving())
            .filter(|id| !self.control.members.contains(id))
            .filter(|id| !self.retired.contains(id) && self.control.dead != Some(*id))
            .map(|node| Spare { node, domain: None })
            .collect();
        assert!(
            spares.iter().all(|s| self.pool.contains(&s.node)),
            "a spare is a node of the pool"
        );
        spares
    }

    /// The node that leaves the cell: the last node of the pool, which the
    /// driver offers to no controller.
    fn leaving(&self) -> Option<NodeId> {
        let last = self.pool.last().copied();
        assert!(
            last.is_none_or(|id| id.0 >= self.flavor.acceptors),
            "the node that leaves is no member at boot"
        );
        last
    }

    /// Carry `order` as a driver does: to the node it names, with the calls
    /// of today. An order for another leadership is dropped.
    fn carry(&mut self, order: Order, cx: &mut Cx<'_, Self>) {
        if self.flavor.mutant == Some(Mutant::DropOrders) {
            return;
        }
        match order {
            Order::Reconfigure {
                leader,
                ballot,
                since,
                config,
            } => {
                let Some(node) = self.node_mut(leader).live.as_mut() else {
                    return;
                };
                if !node.is_leader() || node.ballot() != ballot || node.acceptors_since() != since {
                    return;
                }
                let result = node.reconfigure(&config);
                sometimes!(
                    matches!(result, ReconfigureResult::Started(_)),
                    reach::CONTROL_STARTED
                );
                if let ReconfigureResult::Started(started) = result {
                    self.asked.push((started, config.members().to_vec()));
                }
                self.drain_node(leader, cx);
            }
            Order::Retire {
                node: id,
                watermark,
            } => {
                if self.asked_names(id, watermark) {
                    return;
                }
                let Some(node) = self.node_mut(id).live.as_mut() else {
                    return;
                };
                if node.may_retire(watermark) {
                    sometimes!(true, reach::CONTROL_RETIRED);
                    cx.record(ClusterEvent::Retire { node: id });
                    self.retired.insert(id);
                    self.crash_node(id, cx);
                    cx.crashed(&super::Party::Node(id));
                    return;
                }
                let _ = node.check_retirement(watermark);
                self.drain_node(id, cx);
            }
        }
    }

    /// When the faults stop, a member of the configuration in force stops
    /// for good: the one machine that the controller must replace.
    pub(super) fn stop_for_good(&mut self, cx: &mut Cx<'_, Self>) {
        if !self.flavor.controllers {
            return;
        }
        let members: Vec<NodeId> = self
            .leaders()
            .first()
            .and_then(|id| self.node(*id).live.as_ref())
            .map_or_else(
                || (0..self.flavor.acceptors).map(NodeId).collect(),
                |node| node.acceptors().members().to_vec(),
            );
        let Some(&id) = cx.rng().pick(&members) else {
            return;
        };
        assert!(!self.retired.contains(&id), "a member is not retired");
        self.control.dead = Some(id);
        if self.node(id).live.is_some() {
            self.crash_node(id, cx);
            cx.crashed(&super::Party::Node(id));
        }
    }

    /// Goal 2, judged once the faults stop: each member of the
    /// configuration in force runs and the fresh controller is `Steady`, or
    /// it holds for a reason that the oracle confirms with its own count of
    /// the cluster. The oracle never calls the rules: it counts the members
    /// that run and the nodes that could replace one.
    pub(super) fn healed(&self) -> Result<(), String> {
        if !self.flavor.controllers {
            return Ok(());
        }
        let leaders = self.leaders();
        let [leader] = leaders.as_slice() else {
            return Err(format!("{HEALED}: {} leaders", leaders.len()));
        };
        let node = self.node(*leader).live.as_ref().expect("a leader runs");
        let members = node.acceptors().members();
        let down: Vec<NodeId> = members
            .iter()
            .copied()
            .filter(|id| self.node(*id).live.is_none())
            .collect();
        // The driver's own count of a node that could replace one.
        let spare_runs = self.pool.iter().any(|id| {
            !members.contains(id)
                && Some(*id) != self.leaving()
                && self.node(*id).live.is_some()
                && !self.retired.contains(id)
                && !self.control.members.contains(id)
        });
        let decision = evaluate(&self.control.fresh, &self.spares(), &POLICY);
        judge(
            &decision,
            &Facts {
                members,
                down: &down,
                spare_runs,
            },
        )
    }

    /// The members of the configuration of a leader, or none.
    fn leader_members(&self) -> Vec<NodeId> {
        self.leaders()
            .first()
            .and_then(|id| self.node(*id).live.as_ref())
            .map_or_else(Vec::new, |node| node.acceptors().members().to_vec())
    }
}

/// Fold `round` into `view`, without the feedback that `lost` marks.
fn observe(view: &mut View, round: &Round, lost: &[bool]) {
    let mut lost = lost.iter().copied().chain(std::iter::repeat(false));
    for feedback in &round.nodes {
        if !lost.next().expect("an endless mark") {
            view.observe_node(feedback.clone());
        }
    }
    for feedback in &round.proxies {
        if !lost.next().expect("an endless mark") {
            view.observe_proxy(feedback.clone());
        }
    }
    for feedback in &round.matchmakers {
        if !lost.next().expect("an endless mark") {
            view.observe_matchmaker(feedback.clone());
        }
    }
}
