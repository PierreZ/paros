//! The cell's balancer (#687 (rebalance a cell), §3.4): it evens out the
//! acceptors the machines of one zone host. It is a pure function of the
//! registry fold and of which machines stay: no I/O, no clock, no RNG. The
//! tenant coordinator calls it once per watch round
//! (`paros_coordinator::cell::campaign`).
//!
//! Placement spreads a journal at create (zones in round robin, then the
//! least busy and the least loaded machine of a zone), and the moves take a
//! journal off a leaving machine. Nothing else moves a journal, so after
//! `cell add-machine` the new machine hosts only what is created later.
//! The rebalancer closes that gap, one acceptor at a time:
//!
//! 1. Each watch round, it compares the most and the least loaded machine
//!    of each zone (roles that weigh, [`placement::Machine::booked`]),
//!    among the storage machines that stay and are up.
//! 2. Once a zone's gap is above `balance_spread` for `balance_rounds`
//!    rounds in a row (the hysteresis), it names one acceptor booking on
//!    the most loaded machine whose journal holds no role on the least
//!    loaded one: a [`Rebalance`].
//! 3. The move path takes it (`paros_coordinator::tenant::moves`): that booking leaves
//!    as a draining machine's would. The move books a slot on another
//!    machine of the cell with `placement::plan`, which never books the
//!    machine the booking leaves, reconfigures the journal, then releases
//!    the old slot. It retires no member: the machine stays and takes
//!    roles again. The mode changes pass over the journal while it moves
//!    (`paros_coordinator::tenant::modes`).
//!
//! Repair comes first. No rebalance starts while a machine is draining,
//! slow, down or gone, or while a role sits on a machine that leaves: the
//! moves off it run first. At most one rebalance runs at a time, and one
//! that does not end in [`ABANDON_ROUNDS`] rounds is abandoned.
//!
//! The rebalance in flight lives in the term only. A term that ends in the
//! middle of one leaves a journal with one slot more than its
//! configuration names, or a slot outside the configuration in force:
//! both are the mode change's to release (`paros_coordinator::tenant::modes`), as for
//! a move off a machine that answered again.

use std::collections::BTreeMap;

use crate::placement;
use crate::registry::{BookingTarget, NodeStanding, Registry, Role};
use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;

/// How many watch rounds a rebalance may take before it is abandoned. A
/// move waits for a leader, a configuration in force and the GC watermark:
/// a few rounds each.
pub const ABANDON_ROUNDS: u32 = 64;

/// The rebalancer's bounds, from the driver's tunables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// The largest gap, in roles hosted, between the most and the least
    /// loaded machine of a zone that needs no move (floor 1).
    pub spread: u32,
    /// How many rounds in a row the gap must be larger before a move starts
    /// (floor 1).
    pub rounds: u32,
}

/// One acceptor booking the rebalancer moves off its machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rebalance {
    /// The journal the booking is of.
    pub journal: JournalIdentifier,
    /// The booking that leaves.
    pub booking: u64,
    /// The machine it leaves.
    pub from: NodeId,
}

/// The rebalancer of one coordinator term.
#[derive(Debug)]
pub struct Balance {
    bounds: Bounds,
    /// How many rounds in a row a zone's gap was above the spread.
    over: u32,
    /// The rebalance in flight, and how many rounds it ran.
    moving: Option<(Rebalance, u32)>,
}

impl Balance {
    /// The rebalancer of a new term.
    ///
    /// # Panics
    ///
    /// If a bound is below its floor of 1 (the tunables refuse it).
    #[must_use]
    pub fn new(bounds: Bounds) -> Self {
        assert!(bounds.spread >= 1, "a rebalance spread is at least 1");
        assert!(bounds.rounds >= 1, "a rebalance hysteresis is at least 1");
        Self {
            bounds,
            over: 0,
            moving: None,
        }
    }

    /// One watch round over `registry` and its storage machines `machines`
    /// as placement sees them. `stays` tells whether a machine stays this
    /// round (registered, not gone, not slow outside a maintenance window).
    /// It returns the rebalance in flight, if any, for the move path to take
    /// this round.
    ///
    /// # Panics
    ///
    /// If its own state breaks: a gap counted while a move is in flight, or
    /// a chosen move off another machine than the most loaded one.
    pub fn round(
        &mut self,
        registry: &Registry,
        machines: &[placement::Machine],
        stays: impl Fn(NodeId) -> bool,
    ) -> Option<Rebalance> {
        if let Some((rebalance, rounds)) = self.moving.take() {
            assert!(self.over == 0, "a rebalance in flight counts no gap");
            if registry.booking(rebalance.booking).is_none() {
                moonpool_assertions::reachable!("balance: a rebalance move finished");
                tracing::info!(journal = %rebalance.journal, from = rebalance.from.0, "journal_rebalanced");
                return None;
            }
            if rounds >= ABANDON_ROUNDS {
                moonpool_assertions::reachable!("balance: a rebalance move is abandoned");
                tracing::info!(journal = %rebalance.journal, from = rebalance.from.0, "rebalance_abandoned");
                return None;
            }
            self.moving = Some((rebalance, rounds + 1));
            return Some(rebalance);
        }
        if repairing(registry, &stays) {
            self.over = 0;
            return None;
        }
        let Some((from, to)) = widest_gap(machines, &stays, self.bounds.spread) else {
            self.over = 0;
            return None;
        };
        self.over += 1;
        if self.over < self.bounds.rounds {
            return None;
        }
        self.over = 0;
        let rebalance = choose(registry, from, to);
        let Some(rebalance) = rebalance else {
            moonpool_assertions::reachable!(
                "balance: no acceptor can leave the most loaded machine"
            );
            return None;
        };
        assert_eq!(
            rebalance.from, from,
            "a rebalance leaves the most loaded machine"
        );
        moonpool_assertions::reachable!("balance: a cell picks a rebalance move");
        tracing::info!(journal = %rebalance.journal, from = from.0, to = to.0, "rebalance_chosen");
        self.moving = Some((rebalance, 0));
        Some(rebalance)
    }
}

/// Whether the cell repairs: a machine is draining, slow, down or gone, or
/// a role sits on a machine that leaves. The moves off it come first.
fn repairing(registry: &Registry, stays: impl Fn(NodeId) -> bool) -> bool {
    let draining = registry
        .nodes()
        .any(|(_, n)| n.standing == NodeStanding::Draining);
    let unwell = registry.nodes().any(|(node, n)| {
        n.standing == NodeStanding::Registered
            && registry.serves(node)
            && (!registry.liveness(node).up || registry.is_slow(node) || !stays(node))
    });
    let stranded = registry.bookings().any(|(_, b)| !stays(b.node));
    draining || unwell || stranded
}

/// The most and the least loaded machine of the zone with the widest gap
/// above `spread`, among `machines` that store, stay and are up. `None`
/// when every zone is within the spread.
fn widest_gap(
    machines: &[placement::Machine],
    stays: impl Fn(NodeId) -> bool,
    spread: u32,
) -> Option<(NodeId, NodeId)> {
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
    let mut widest: Option<(u64, NodeId, NodeId)> = None;
    for zone in zones.values() {
        // The lowest id breaks a tie, so a round's choice is a function of
        // the fold alone.
        let most = zone
            .iter()
            .max_by_key(|m| (m.booked, std::cmp::Reverse(m.node)))?;
        let least = zone.iter().min_by_key(|m| (m.booked, m.node))?;
        let gap = most.booked - least.booked;
        if gap > u64::from(spread) && widest.is_none_or(|(w, _, _)| gap > w) {
            assert_ne!(most.node, least.node, "a gap is between two machines");
            widest = Some((gap, most.node, least.node));
        }
    }
    widest.map(|(_, most, least)| (most, least))
}

/// The first acceptor booking, by id, on `from` of a live journal the cell
/// hosts (not a tenant's control journal, which moves by its own step)
/// whose journal holds no role on `to`: the one the rebalance moves.
fn choose(registry: &Registry, from: NodeId, to: NodeId) -> Option<Rebalance> {
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
                })
            }
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::{Class, Locality};
    use crate::registry::SystemCommand;
    use paros_core::journal::{JournalId, TenantId};

    fn journal(n: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(7), JournalId(n))
    }

    /// A registry of `nodes` storage machines in zone `z`, each with the
    /// acceptor bookings `bookings` names (booking id, node, journal).
    fn registry(nodes: &[u64], bookings: &[(u64, u64, u64)]) -> Registry {
        let mut reg = Registry::new(nodes.iter().copied().map(NodeId));
        let mut seq = 0;
        let mut fold = |reg: &mut Registry, command: SystemCommand| {
            reg.fold(seq, &command.encode());
            seq += 1;
        };
        for &id in nodes {
            fold(
                &mut reg,
                SystemCommand::RegisterNode {
                    id: NodeId(id),
                    addr: format!("n{id}"),
                    class: Class::Storage,
                    locality: Locality::zone("z"),
                    name: format!("m{id}"),
                    incarnation: 1,
                    machine_interface: Vec::new(),
                },
            );
        }
        for &(booking, node, j) in bookings {
            fold(
                &mut reg,
                SystemCommand::BookCapacity {
                    booking,
                    node: NodeId(node),
                    role: Role::Acceptor,
                    target: BookingTarget::Journal(journal(j)),
                },
            );
        }
        for &(booking, node, _) in bookings {
            assert_eq!(reg.booking(booking).map(|b| b.node), Some(NodeId(node)));
        }
        reg
    }

    /// Every machine stays.
    fn all(_: NodeId) -> bool {
        true
    }

    fn machines(reg: &Registry) -> Vec<placement::Machine> {
        placement::machines(reg, reg.pool())
    }

    const BOUNDS: Bounds = Bounds {
        spread: 1,
        rounds: 2,
    };

    /// Three journals of one acceptor each on machine 1, none on 2: a new
    /// machine joined.
    fn joined() -> Registry {
        registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 1, 3)])
    }

    #[test]
    fn a_gap_held_for_its_rounds_moves_the_first_acceptor_off_the_loaded_machine() {
        let reg = joined();
        let mut balance = Balance::new(BOUNDS);
        assert_eq!(
            balance.round(&reg, &machines(&reg), all),
            None,
            "one round is no trend"
        );
        let chosen = balance.round(&reg, &machines(&reg), all);
        assert_eq!(
            chosen,
            Some(Rebalance {
                journal: journal(1),
                booking: 10,
                from: NodeId(1),
            })
        );
        // In flight, it is named again every round.
        assert_eq!(balance.round(&reg, &machines(&reg), all), chosen);
    }

    #[test]
    fn a_rebalance_ends_once_its_booking_is_released() {
        let reg = joined();
        let mut balance = Balance::new(Bounds {
            spread: 1,
            rounds: 1,
        });
        assert!(balance.round(&reg, &machines(&reg), all).is_some());
        let mut after = reg.clone();
        let seq = after.next_seq();
        after.fold(
            seq,
            &SystemCommand::ReleaseCapacity { booking: 10 }.encode(),
        );
        assert!(after.booking(10).is_none());
        assert_eq!(balance.round(&after, &machines(&after), all), None, "done");
        assert!(
            balance.moving.is_none(),
            "nothing is in flight once it ended"
        );
    }

    #[test]
    fn a_rebalance_that_does_not_end_is_abandoned() {
        let reg = joined();
        let mut balance = Balance::new(Bounds {
            spread: 1,
            rounds: 1,
        });
        let chosen = balance.round(&reg, &machines(&reg), all);
        assert!(chosen.is_some());
        for _ in 0..ABANDON_ROUNDS {
            assert_eq!(balance.round(&reg, &machines(&reg), all), chosen);
        }
        assert_eq!(balance.round(&reg, &machines(&reg), all), None, "abandoned");
    }

    #[test]
    fn a_cell_within_the_spread_moves_nothing() {
        let reg = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 2, 3)]);
        let mut balance = Balance::new(Bounds {
            spread: 1,
            rounds: 1,
        });
        for _ in 0..4 {
            assert_eq!(balance.round(&reg, &machines(&reg), all), None);
        }
    }

    #[test]
    fn repair_comes_before_balance() {
        let reg = joined();
        let mut balance = Balance::new(Bounds {
            spread: 1,
            rounds: 1,
        });
        // A machine gone (#475) holds every balance back.
        let gone = |node: NodeId| node != NodeId(2);
        assert!(repairing(&reg, gone));
        assert_eq!(balance.round(&reg, &machines(&reg), gone), None);
        // A draining machine too.
        let mut draining = reg.clone();
        let seq = draining.next_seq();
        draining.fold(
            seq,
            &SystemCommand::RegisterNode {
                id: NodeId(3),
                addr: "n3".into(),
                class: Class::Storage,
                locality: Locality::zone("z"),
                name: "m3".into(),
                incarnation: 1,
                machine_interface: Vec::new(),
            }
            .encode(),
        );
        draining.fold(
            seq + 1,
            &SystemCommand::DrainNode { id: NodeId(3) }.encode(),
        );
        assert_eq!(
            draining.get(NodeId(3)).map(|n| n.standing),
            Some(NodeStanding::Draining)
        );
        assert!(repairing(&draining, all));
        assert_eq!(balance.round(&draining, &machines(&draining), all), None);
        assert!(!repairing(&reg, all));
    }

    #[test]
    fn an_acceptor_whose_journal_is_on_the_light_machine_stays() {
        // Journal 1 has a member on both: only journal 2 may move.
        let reg = registry(&[1, 2], &[(10, 1, 1), (11, 1, 2), (12, 1, 4), (13, 2, 1)]);
        assert_eq!(
            choose(&reg, NodeId(1), NodeId(2)).map(|r| r.journal),
            Some(journal(2))
        );
        // Every journal on the loaded machine also sits on the light one.
        let all = registry(&[1, 2], &[(10, 1, 1), (11, 2, 1)]);
        assert_eq!(choose(&all, NodeId(1), NodeId(2)), None);
    }

    #[test]
    fn the_widest_gap_names_the_most_and_the_least_loaded_machine() {
        let reg = registry(
            &[1, 2, 3],
            &[(10, 1, 1), (11, 1, 2), (12, 1, 3), (13, 2, 4)],
        );
        assert_eq!(
            widest_gap(&machines(&reg), all, 1),
            Some((NodeId(1), NodeId(3)))
        );
        assert_eq!(widest_gap(&machines(&reg), all, 3), None);
    }
}
