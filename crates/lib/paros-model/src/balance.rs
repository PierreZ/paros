//! The cell's balancer (#687 (rebalance a cell), §3.4): it evens out the
//! acceptors the machines of one zone host, and then their busyness. It is a
//! pure function of the registry fold, the storage machines and which
//! machines stay: no I/O, no clock, no RNG. The tenant coordinator calls
//! [`Balance::round`] once per watch round
//! (`paros_coordinator::cell::campaign`), and a view calls [`judge`] to say
//! how the cell stands (`parosctl cell balance`).
//!
//! Placement spreads a journal at create (zones in round robin, then the
//! least busy and the least loaded machine of a zone), and the moves take a
//! journal off a leaving machine. Nothing else moves a journal, so after
//! `cell add-machine` the new machine hosts only what is created later.
//! The balancer closes that gap, one acceptor at a time:
//!
//! 1. Each watch round, it compares the most and the least loaded machine
//!    of each zone (roles that weigh, [`placement::Machine::booked`]),
//!    among the storage machines that stay and are up. When every zone is
//!    within `spread` roles, it compares the busiest and the least busy
//!    machine of each zone ([`placement::Machine::busy`]): a gap of
//!    `busy_spread` steps or more is a gap too, when the move keeps the
//!    roles within `spread`.
//! 2. Once a gap holds for `rounds` rounds in a row (the hysteresis), it
//!    names one acceptor booking on the loaded machine whose journal holds
//!    no role on the light one: a [`Rebalance`]. The coordinator records it
//!    in the registry (`BalanceStarted`), which judges it again at apply.
//! 3. The move path takes it (`paros_coordinator::tenant::moves`): that
//!    booking leaves as a draining machine's would, for the light machine
//!    alone. It retires no member: the machine stays and takes roles again.
//!    The mode changes pass over the journal while it moves
//!    (`paros_coordinator::tenant::modes`).
//! 4. The registry ends the move when it releases the booking. A move that
//!    does not end in [`ABANDON_ROUNDS`] rounds of one term is abandoned
//!    (`BalanceAbandoned`).
//!
//! Repair comes first. No move starts while a machine is draining, slow,
//! down or gone, or while a role sits on a machine that leaves: the moves off
//! it run first. An operator may pause the balancer (`SetBalance`): it then
//! starts no move, and the move in flight still ends.
//!
//! The move in flight lives in the registry, so a later term goes on with
//! it. A term that ends in the middle of one leaves a journal with one slot
//! more than its configuration names, or a slot outside the configuration
//! in force: both are the mode change's to release, as for a move off a
//! machine that answered again.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;

use crate::placement;
use crate::registry::balancing::{BalanceMove, BalanceReason};
use crate::registry::{BookingTarget, NodeStanding, Registry, Role};

/// How many watch rounds a move may take in one term before it is
/// abandoned. A move waits for a leader, a warm member, a configuration in
/// force and the GC watermark: a few rounds each.
pub const ABANDON_ROUNDS: u32 = 64;

/// The balancer's bounds, from the driver's tunables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// The largest gap, in roles hosted, between the most and the least
    /// loaded machine of a zone that needs no move (floor 1).
    pub spread: u32,
    /// How many rounds in a row a gap must hold before a move starts
    /// (floor 1).
    pub rounds: u32,
    /// The smallest gap, in busyness steps, between the busiest and the
    /// least busy machine of a zone that needs a move (floor 1, at most
    /// [`placement::BUSY_STEPS`]).
    pub busy_spread: u8,
    /// The largest gap, in journals led, between the machine that leads
    /// the most and a member that leads the fewest that needs no handoff
    /// (floor 1).
    pub leader_spread: u32,
}

/// One acceptor booking the balancer moves off its machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rebalance {
    /// The journal the booking is of.
    pub journal: JournalIdentifier,
    /// The booking that leaves.
    pub booking: u64,
    /// The machine it leaves.
    pub from: NodeId,
    /// The machine it goes to.
    pub to: NodeId,
    /// Why.
    pub reason: BalanceReason,
}

impl From<&BalanceMove> for Rebalance {
    fn from(moved: &BalanceMove) -> Self {
        Self {
            journal: moved.journal,
            booking: moved.booking,
            from: moved.from,
            to: moved.to,
            reason: moved.reason,
        }
    }
}

/// One leadership the balancer hands on (#687): `journal`'s leader on
/// `from` hands it to its member on `to`, with the cooperative handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderMove {
    /// The journal.
    pub journal: JournalIdentifier,
    /// The machine that leads it, and leads the most journals.
    pub from: NodeId,
    /// Its member that leads the fewest.
    pub to: NodeId,
}

/// The journals each machine leads, as its `Load` answered them.
pub type Leading = BTreeMap<NodeId, BTreeSet<JournalIdentifier>>;

/// A gap between two machines of one zone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The zone.
    pub zone: String,
    /// Why it is a gap: roles or busyness.
    pub reason: BalanceReason,
    /// The loaded (or busy) machine, its roles and its busyness step.
    pub from: (NodeId, u64, u8),
    /// The light (or idle) machine, its roles and its busyness step.
    pub to: (NodeId, u64, u8),
}

/// Why the cell repairs before it balances.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Repair {
    /// A machine drains.
    Draining(NodeId),
    /// A machine that serves is down, slow or gone.
    Unwell(NodeId),
    /// A role sits on a machine that leaves.
    Stranded(NodeId),
}

/// How the cell stands, for the balancer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Judgement {
    /// A move is in flight.
    Moving(BalanceMove),
    /// An operator paused the balancer.
    Paused,
    /// The cell repairs first.
    Repairing(Repair),
    /// A gap no move can close: every acceptor on the loaded machine has a
    /// role of its journal on the light one.
    Stuck(Gap),
    /// A gap the balancer closes.
    Unbalanced(Gap),
    /// Every zone is within the spread.
    Balanced,
}

/// How the cell stands: the move in flight first, then a pause, then a
/// repair, then the widest gap. `stays` tells whether a machine stays this
/// round (registered, not gone, not slow outside a maintenance window).
#[must_use]
pub fn judge(
    registry: &Registry,
    machines: &[placement::Machine],
    stays: impl Fn(NodeId) -> bool,
    bounds: Bounds,
) -> Judgement {
    if let Some(moved) = registry.balance().moving() {
        return Judgement::Moving(*moved);
    }
    if registry.balance().paused() {
        return Judgement::Paused;
    }
    if let Some(repair) = repairing(registry, &stays) {
        return Judgement::Repairing(repair);
    }
    let Some(gap) = widest_gap(machines, &stays, bounds) else {
        return Judgement::Balanced;
    };
    if choose(registry, &gap).is_some() {
        Judgement::Unbalanced(gap)
    } else {
        Judgement::Stuck(gap)
    }
}

/// What one balance round asks of the coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing.
    Idle,
    /// Record this move (`BalanceStarted`), then hand it to the move path.
    Start(Rebalance),
    /// Hand the move in flight to the move path.
    Moving(Rebalance),
    /// Give the move in flight up (`BalanceAbandoned`).
    Abandon(u64),
}

/// The balancer of one coordinator term: its hysteresis and how long the
/// move in flight has run in this term.
#[derive(Debug)]
pub struct Balance {
    bounds: Bounds,
    /// How many rounds in a row a gap held.
    over: u32,
    /// The booking of the move in flight, and how many rounds of this term
    /// it ran.
    moving: Option<(u64, u32)>,
    /// The booking of the last move this term started.
    started: Option<u64>,
    /// Whether the last round judged the cell balanced: only then are the
    /// leaders balanced.
    balanced: bool,
    /// How many rounds in a row a leader gap held.
    leaders_over: u32,
}

impl Balance {
    /// The balancer of a new term.
    ///
    /// # Panics
    ///
    /// If a bound is below its floor of 1, or the busy spread is above
    /// [`placement::BUSY_STEPS`] (the tunables refuse both).
    #[must_use]
    pub fn new(bounds: Bounds) -> Self {
        assert!(bounds.spread >= 1, "a balance spread is at least 1");
        assert!(bounds.rounds >= 1, "a balance hysteresis is at least 1");
        assert!(bounds.busy_spread >= 1, "a busy spread is at least 1");
        assert!(bounds.leader_spread >= 1, "a leader spread is at least 1");
        assert!(
            bounds.busy_spread <= placement::BUSY_STEPS,
            "a busy spread is at most the busyness scale"
        );
        Self {
            bounds,
            over: 0,
            moving: None,
            started: None,
            balanced: false,
            leaders_over: 0,
        }
    }

    /// One watch round of the leaders, after [`Balance::round`] of the same
    /// round: the leadership to hand on, if any. Leaders are balanced only
    /// on a balanced cell, once a gap held `rounds` rounds in a row, and a
    /// handoff starts the count again, so the next `Load` answers see it.
    pub fn leader_round(
        &mut self,
        registry: &Registry,
        machines: &[placement::Machine],
        stays: impl Fn(NodeId) -> bool,
        leading: &Leading,
    ) -> Option<LeaderMove> {
        if !self.balanced || registry.balance().paused() {
            self.leaders_over = 0;
            return None;
        }
        let Some(handoff) = leader_gap(
            registry,
            machines,
            stays,
            leading,
            self.bounds.leader_spread,
        ) else {
            self.leaders_over = 0;
            return None;
        };
        self.leaders_over += 1;
        if self.leaders_over < self.bounds.rounds {
            return None;
        }
        self.leaders_over = 0;
        moonpool_assertions::reachable!(
            "balance: a cell hands a leadership to a machine that leads fewer"
        );
        tracing::info!(journal = %handoff.journal, from = handoff.from.0, to = handoff.to.0, "leader_balance_chosen");
        Some(handoff)
    }

    /// One watch round over `registry` and its storage machines `machines`
    /// as placement sees them (with their busyness). `stays` tells whether
    /// a machine stays this round.
    ///
    /// # Panics
    ///
    /// If its own state breaks: a chosen move off another machine than the
    /// loaded one, or onto another than the light one.
    pub fn round(
        &mut self,
        registry: &Registry,
        machines: &[placement::Machine],
        stays: impl Fn(NodeId) -> bool,
    ) -> Step {
        let judgement = judge(registry, machines, &stays, self.bounds);
        self.balanced = judgement == Judgement::Balanced;
        if let Judgement::Moving(moved) = &judgement {
            self.over = 0;
            let rounds = match self.moving {
                Some((booking, rounds)) if booking == moved.booking => rounds + 1,
                _ => {
                    if self.started != Some(moved.booking) {
                        moonpool_assertions::reachable!(
                            "balance: a term goes on with a move an earlier term started"
                        );
                    }
                    0
                }
            };
            if rounds >= ABANDON_ROUNDS {
                moonpool_assertions::reachable!("balance: a rebalance move is abandoned");
                tracing::info!(journal = %moved.journal, from = moved.from.0, "rebalance_abandoned");
                self.moving = None;
                return Step::Abandon(moved.booking);
            }
            self.moving = Some((moved.booking, rounds));
            return Step::Moving(Rebalance::from(moved));
        }
        if let Some((booking, _)) = self.moving.take()
            && self.started == Some(booking)
        {
            moonpool_assertions::reachable!("balance: a rebalance move finished");
        }
        let gap = match judgement {
            Judgement::Unbalanced(gap) => gap,
            Judgement::Stuck(_) => {
                moonpool_assertions::reachable!(
                    "balance: no acceptor can leave the most loaded machine"
                );
                self.over = 0;
                return Step::Idle;
            }
            Judgement::Moving(_)
            | Judgement::Paused
            | Judgement::Repairing(_)
            | Judgement::Balanced => {
                self.over = 0;
                return Step::Idle;
            }
        };
        self.over += 1;
        if self.over < self.bounds.rounds {
            return Step::Idle;
        }
        self.over = 0;
        let rebalance = choose(registry, &gap).expect("an unbalanced cell has a move to make");
        assert_eq!(
            rebalance.from, gap.from.0,
            "a move leaves the loaded machine"
        );
        assert_eq!(rebalance.to, gap.to.0, "a move goes to the light machine");
        match gap.reason {
            BalanceReason::Roles => {
                moonpool_assertions::reachable!("balance: a cell picks a rebalance move");
            }
            BalanceReason::Busy => {
                moonpool_assertions::reachable!(
                    "balance: a cell moves an acceptor off a busy machine"
                );
            }
        }
        tracing::info!(journal = %rebalance.journal, from = rebalance.from.0, to = rebalance.to.0, reason = %gap.reason, "rebalance_chosen");
        self.started = Some(rebalance.booking);
        Step::Start(rebalance)
    }
}

/// The leadership to hand on: from the machine that leads the most journals
/// (ties by the lowest id), the first journal it leads, by id, whose member
/// on another machine leads more than `spread` fewer; to the member that
/// leads the fewest. Among `machines` that store, stay and are up; a
/// member is a machine the registry books an acceptor of the journal on.
///
/// # Panics
///
/// If it would hand a leadership to the machine that holds it (a
/// programmer error).
#[must_use]
pub fn leader_gap(
    registry: &Registry,
    machines: &[placement::Machine],
    stays: impl Fn(NodeId) -> bool,
    leading: &Leading,
    spread: u32,
) -> Option<LeaderMove> {
    let counts: BTreeMap<NodeId, u64> = machines
        .iter()
        .filter(|m| m.class.stores() && m.up && stays(m.node))
        .map(|m| (m.node, leading.get(&m.node).map_or(0, |l| l.len() as u64)))
        .collect();
    let (&from, &most) = counts
        .iter()
        .max_by_key(|(node, count)| (**count, std::cmp::Reverse(**node)))?;
    let spread = u64::from(spread);
    leading.get(&from)?.iter().find_map(|&journal| {
        let target = BookingTarget::Journal(journal);
        let (to, fewest) = registry
            .bookings()
            .filter(|(_, b)| b.role == Role::Acceptor && b.target == target && b.node != from)
            .filter_map(|(_, b)| counts.get(&b.node).map(|c| (b.node, *c)))
            .min_by_key(|(node, count)| (*count, *node))?;
        (most > fewest + spread).then(|| {
            assert_ne!(from, to, "a leader handoff changes machines");
            LeaderMove { journal, from, to }
        })
    })
}

/// Why the cell repairs, if it does: a machine is draining, a serving one
/// is down, slow or gone, or a role sits on a machine that leaves. The
/// moves off it come first.
fn repairing(registry: &Registry, stays: impl Fn(NodeId) -> bool) -> Option<Repair> {
    if let Some((node, _)) = registry
        .nodes()
        .find(|(_, n)| n.standing == NodeStanding::Draining)
    {
        return Some(Repair::Draining(node));
    }
    if let Some((node, _)) = registry.nodes().find(|(node, n)| {
        n.standing == NodeStanding::Registered
            && registry.serves(*node)
            && (!registry.liveness(*node).up || registry.is_slow(*node) || !stays(*node))
    }) {
        return Some(Repair::Unwell(node));
    }
    registry
        .bookings()
        .find(|(_, b)| !stays(b.node))
        .map(|(_, b)| Repair::Stranded(b.node))
}

/// The gap of the zone with the widest one, among `machines` that store,
/// stay and are up: a roles gap above `spread` first, else a busyness gap
/// of `busy_spread` steps or more whose move keeps the roles within
/// `spread`. `None` when every zone is within both.
fn widest_gap(
    machines: &[placement::Machine],
    stays: impl Fn(NodeId) -> bool,
    bounds: Bounds,
) -> Option<Gap> {
    let mut zones: BTreeMap<&str, Vec<&placement::Machine>> = BTreeMap::new();
    for machine in machines
        .iter()
        .filter(|m| m.class.stores() && m.up && stays(m.node))
    {
        zones
            .entry(machine.zone.as_str())
            .or_default()
            .push(machine);
    }
    let spread = u64::from(bounds.spread);
    let gap = |zone: &str, reason, from: &placement::Machine, to: &placement::Machine| Gap {
        zone: zone.to_owned(),
        reason,
        from: (from.node, from.booked, from.busy),
        to: (to.node, to.booked, to.busy),
    };
    // The lowest id breaks a tie, so a round's choice is a function of the
    // fold and the busyness alone.
    let mut roles: Option<(u64, Gap)> = None;
    let mut busy: Option<(u8, Gap)> = None;
    for (zone, members) in &zones {
        let most = members
            .iter()
            .max_by_key(|m| (m.booked, std::cmp::Reverse(m.node)))?;
        let least = members.iter().min_by_key(|m| (m.booked, m.node))?;
        let width = most.booked - least.booked;
        if width > spread && roles.as_ref().is_none_or(|(w, _)| width > *w) {
            assert_ne!(most.node, least.node, "a gap is between two machines");
            roles = Some((width, gap(zone, BalanceReason::Roles, most, least)));
        }
        let busiest = members
            .iter()
            .max_by_key(|m| (m.busy, m.booked, std::cmp::Reverse(m.node)))?;
        let idlest = members.iter().min_by_key(|m| (m.busy, m.booked, m.node))?;
        let steps = busiest.busy - idlest.busy;
        // The move takes one role off the busy machine and gives one to the
        // idle one: the idle one must not end above the busy one by more
        // than the spread, or the roles balance moves it back.
        let keeps_roles = idlest.booked + 2 <= busiest.booked + spread;
        if steps >= bounds.busy_spread
            && keeps_roles
            && busy.as_ref().is_none_or(|(w, _)| steps > *w)
        {
            assert_ne!(
                busiest.node, idlest.node,
                "a busy gap is between two machines"
            );
            busy = Some((steps, gap(zone, BalanceReason::Busy, busiest, idlest)));
        }
    }
    roles.map(|(_, gap)| gap).or(busy.map(|(_, gap)| gap))
}

/// The first acceptor booking, by id, on the gap's loaded machine of a live
/// journal the cell hosts (not a tenant's control journal, which moves by
/// its own step) whose journal holds no role on the light machine: the one
/// the move takes.
fn choose(registry: &Registry, gap: &Gap) -> Option<Rebalance> {
    let (from, to) = (gap.from.0, gap.to.0);
    registry
        .bookings()
        .filter(|(_, b)| b.node == from && b.role == Role::Acceptor)
        .find_map(|(booking, b)| match b.target {
            BookingTarget::Journal(journal)
                if !registry.dropped(journal.tenant)
                    && registry
                        .hosted_tenant(journal.tenant)
                        .is_none_or(|hosted| hosted.control != journal.journal)
                    && !registry
                        .bookings()
                        .any(|(_, o)| o.node == to && o.target == b.target) =>
            {
                Some(Rebalance {
                    journal,
                    booking,
                    from,
                    to,
                    reason: gap.reason,
                })
            }
            _ => None,
        })
}

#[cfg(test)]
mod tests;
