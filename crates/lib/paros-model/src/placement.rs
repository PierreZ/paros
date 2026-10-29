//! **Placement** (#212 (placement), `docs/architecture/03-3-coordinators-and-placement.md` §3.3, §3.4): the
//! pure function a coordinator runs to decide where the instances of one role
//! of one journal or matchmaker set go.
//!
//! [`plan`] maps `(desired, available, current)` to a list of [`Step`]s: the
//! slots to book and the bookings to release so that the target holds
//! exactly the desired count. It draws no randomness, reads no clock and does
//! no I/O, so a new coordinator that folds the same registry computes the
//! same steps, and the simulation runs the same code as `parosd`. The core
//! knows nothing of failure domains; this module is the only reader of them.
//!
//! The rules (§3.4, amended on 2026-10-07, #215):
//!
//! 1. **Class and capacity.** An instance goes only to a machine of its
//!    role's class with a free slot, as the registry judges a booking at
//!    apply ([`crate::registry::RegistryRefusal::WrongClass`],
//!    [`crate::registry::RegistryRefusal::NoCapacity`]), and only to a
//!    machine that serves the cell: a founding member, or one the cell
//!    coordinator saw admitted ([`Registry::serves`], #511).
//! 2. **One machine, one storage instance.** A machine never holds two
//!    storage instances (acceptor, replica, matchmaker) of one target, and
//!    never two instances of one role of one target.
//! 3. **Zone by zone in round robin.** The next instance goes to the zone
//!    (failure domain) that holds the fewest instances of the target so far.
//! 4. **Least-loaded inside a zone.** Inside that zone, the machine with the
//!    fewest booked slots takes it. Zones that tie on rule 3 go in the order
//!    of their machines' mean load, so the cell fills evenly.
//! 5. **Ties broken by a draw seeded per tenant.** Zones and machines that
//!    tie are ordered by a hash of the caller's seed (the tenant id), so two
//!    tenants of the same shape spread over different machines (shuffle
//!    sharding inside the cell), and one tenant always gets the same order.
//! 6. **Up first.** A machine the cell coordinator marked down takes an
//!    instance only when the machines that are up cannot hold them all.
//!
//! A release goes the other way: a machine that is down first, then the
//! most loaded.
//!
//! **The zone rule** (#215 (zones), §5) judges a planned membership, not a
//! machine: [`zoned`] labels the members with their machines' zones, and
//! the configuration's own [`AcceptorConfig::zone_rule`] says whether it
//! survives the loss of any one zone and no zone commits alone. The rule
//! binds a placement only where it can hold ([`zone_rule_binds`]): a
//! durability that survives a failure, over machines that span three zones
//! or more. A plan the rule refuses is planned again under another draw
//! ([`ZONE_DRAWS`]), and refused when none holds.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::NodeId;
use paros_core::membership::{AcceptorConfig, FailureDomain, QuorumSystem};

use crate::machine::Class;
use crate::registry::{Booking, BookingTarget, NodeStanding, Registry, Role};

/// A machine as the placement sees it: what the registry holds of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Machine {
    /// Its identity.
    pub node: NodeId,
    /// Its class.
    pub class: Class,
    /// Its failure domain (its zone).
    pub zone: String,
    /// The slots of its capacity.
    pub capacity: u64,
    /// The slots of its capacity booked on it now ([`Registry::weight`]).
    pub booked: u64,
    /// Its weightless bookings ([`Booking::weighs`], #753 (per-journal
    /// proxies)): they take no slot, and break ties of load inside a zone.
    pub light: u64,
    /// Whether the cell coordinator holds it up and not slow (#361 (slow
    /// machines)): placement books a slow machine last, as a down one, and
    /// moves its roles off it.
    pub up: bool,
    /// Whether the cell coordinator marked it below the free-bytes floor
    /// (#360 (disk full)): placement books it after every machine with
    /// space, and the mark moves no role already placed.
    pub low_space: bool,
}

impl Machine {
    /// The slots it has left.
    #[must_use]
    pub fn free(&self) -> u64 {
        self.capacity.saturating_sub(self.booked)
    }
}

/// The machines of `nodes` the registry can book: registered, not
/// draining, and serving the cell ([`Registry::serves`]), with their class,
/// zone, capacity, bookings and liveness. A node the registry does not hold
/// is left out, and so is one registered whose admission never landed: it
/// may never serve (#511 (placement books machines that never served):
/// a wiped founder's replacement that an admission registered, then stuck
/// in another cell decree, took a matchmaker set's slot for good).
#[must_use]
pub fn machines(registry: &Registry, nodes: impl IntoIterator<Item = NodeId>) -> Vec<Machine> {
    let mut seen = BTreeSet::new();
    nodes
        .into_iter()
        .filter(|node| seen.insert(*node))
        .filter_map(|node| {
            let held = registry
                .get(node)
                .filter(|n| n.standing == NodeStanding::Registered)?;
            if !registry.serves(node) {
                moonpool_assertions::reachable!(
                    "placement: a machine not yet admitted is passed over"
                );
                return None;
            }
            if registry.liveness(node).low_space {
                moonpool_assertions::reachable!("placement: a machine low on space is booked last");
            }
            Some(Machine {
                node,
                class: held.class,
                zone: held.locality.zone.clone(),
                capacity: held.capacity,
                booked: registry.weight(node),
                light: registry.booked(node) - registry.weight(node),
                up: registry.liveness(node).up && !registry.liveness(node).slow,
                low_space: registry.liveness(node).low_space,
            })
        })
        .collect()
}

/// How many draws a placement under the zone rule tries before it refuses
/// the shape: each draw orders the tied zones and machines another way.
pub const ZONE_DRAWS: u64 = 8;

/// The failure domain of a machine in `zone` (#215 (zones)): the registry's
/// `failure_domain` name is the zone, inside the cell's own region (one
/// cell is one region until #253 (multi-region cells)). `None` for a
/// machine with no zone, or one too long to be a label.
#[must_use]
pub fn domain(zone: &str) -> Option<FailureDomain> {
    let domain = FailureDomain {
        region: String::new(),
        az: zone.to_string(),
    };
    (!zone.is_empty() && domain.is_well_formed()).then_some(domain)
}

/// The configuration of `members` under `system`, labeled with each
/// member's zone when every member has one on `machines` (#215 (zones)),
/// else unlabeled: the configuration a coordinator proposes for a plan.
///
/// # Panics
///
/// As [`AcceptorConfig::new`].
#[must_use]
pub fn zoned(members: &[NodeId], machines: &[Machine], system: QuorumSystem) -> AcceptorConfig {
    let labeled: Option<Vec<(NodeId, FailureDomain)>> = members
        .iter()
        .map(|node| {
            let machine = machines.iter().find(|m| m.node == *node)?;
            Some((*node, domain(&machine.zone)?))
        })
        .collect();
    let config = match labeled {
        Some(labeled) if !labeled.is_empty() => AcceptorConfig::zoned(labeled, system),
        _ => AcceptorConfig::new(members.to_vec(), system),
    };
    assert_eq!(
        config.members().len(),
        members.iter().collect::<BTreeSet<_>>().len(),
        "a planned configuration names each member once"
    );
    config
}

/// Whether the zone rule binds a placement of `class` over `machines`
/// (#215 (zones)): the durability survives a failure (`F >= 1`; `F = 0` is
/// exempt, a majority over one acceptor survives no zone), and the
/// machines of the class span three zones or more (over two zones, no
/// configuration survives one zone while no zone commits alone).
#[must_use]
pub fn zone_rule_binds(survive_machines: u32, class: Class, machines: &[Machine]) -> bool {
    let zones: BTreeSet<&str> = machines
        .iter()
        .filter(|m| m.class == class && domain(&m.zone).is_some())
        .map(|m| m.zone.as_str())
        .collect();
    survive_machines >= 1 && zones.len() >= 3
}

/// What a placement asks for: `count` instances of `role` for `target`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Want {
    /// The role.
    pub role: Role,
    /// The journal or matchmaker set.
    pub target: BookingTarget,
    /// How many instances.
    pub count: usize,
}

/// One change a placement asks the cell coordinator to write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Book one slot of `node` for the role of the target.
    Book {
        /// The machine.
        node: NodeId,
    },
    /// Release this live booking.
    Release {
        /// The booking's id.
        booking: u64,
        /// The machine whose slot it frees.
        node: NodeId,
    },
}

/// Why a placement cannot hold the count it was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortfall {
    /// Enough machines of the class could take the instances, but too few
    /// have a free slot: the cell is full for this request. Refuse it; a
    /// later request may fit once slots are released.
    Capacity {
        /// How many instances found no slot.
        short: usize,
    },
    /// Too few machines of the class that do not hold an instance of the
    /// target already, whatever their capacity.
    Machines {
        /// How many instances found no machine.
        short: usize,
    },
}

/// The steps that make `target` hold exactly `want.count` instances of
/// `want.role`, from the `current` live bookings of the target (any role)
/// and the `machines` that may take a new one. `seed` breaks ties (the
/// caller's tenant id): the same inputs always give the same steps.
///
/// The bookings kept are the current ones; new ones are added with the rules
/// of the module documentation, and extra ones are released.
///
/// # Errors
///
/// [`Shortfall`] when the machines cannot hold the count; no step is
/// returned then, so a refused request books nothing.
///
/// # Panics
///
/// If `current` names a booking of another target (a programmer error).
pub fn plan(
    want: &Want,
    machines: &[Machine],
    current: &[(u64, Booking)],
    seed: u64,
) -> Result<Vec<Step>, Shortfall> {
    for (_, booking) in current {
        assert!(
            booking.target == want.target,
            "a placement reads the bookings of its own target"
        );
    }
    let by_node: BTreeMap<NodeId, &Machine> = machines.iter().map(|m| (m.node, m)).collect();
    let held: Vec<(u64, NodeId)> = current
        .iter()
        .filter(|(_, b)| b.role == want.role)
        .map(|(id, b)| (*id, b.node))
        .collect();
    let steps = if held.len() > want.count {
        release(&held, &by_node, held.len() - want.count, seed)
    } else {
        add(want, machines, current, want.count - held.len(), seed)?
    };
    assert_eq!(
        held.len()
            + steps
                .iter()
                .filter(|s| matches!(s, Step::Book { .. }))
                .count()
            - steps
                .iter()
                .filter(|s| matches!(s, Step::Release { .. }))
                .count(),
        want.count,
        "a plan reaches the count it was asked for"
    );
    Ok(steps)
}

/// The nodes a plan books, in the order it books them.
#[must_use]
pub fn booked(steps: &[Step]) -> Vec<NodeId> {
    steps
        .iter()
        .filter_map(|step| match step {
            Step::Book { node } => Some(*node),
            Step::Release { .. } => None,
        })
        .collect()
}

/// `extra` of the `held` bookings to release: down machines first, then the
/// most loaded, ties by the seeded order.
fn release(
    held: &[(u64, NodeId)],
    by_node: &BTreeMap<NodeId, &Machine>,
    extra: usize,
    seed: u64,
) -> Vec<Step> {
    let mut order: Vec<(u64, NodeId)> = held.to_vec();
    // A machine the registry no longer offers sorts as down and loaded.
    order.sort_by_key(|(_, node)| {
        let machine = by_node.get(node);
        (
            machine.is_some_and(|m| m.up),
            u64::MAX - machine.map_or(u64::MAX, |m| m.booked),
            rank(seed, node.0),
        )
    });
    let steps: Vec<Step> = order
        .into_iter()
        .take(extra)
        .map(|(booking, node)| Step::Release { booking, node })
        .collect();
    assert_eq!(steps.len(), extra, "a plan releases only what it holds");
    steps
}

/// `missing` new instances for `want`, never on a machine `current` already
/// uses for the target in a way rule 2 forbids.
fn add(
    want: &Want,
    machines: &[Machine],
    current: &[(u64, Booking)],
    missing: usize,
    seed: u64,
) -> Result<Vec<Step>, Shortfall> {
    let storage = want.role.class() == Class::Storage;
    let excluded: BTreeSet<NodeId> = current
        .iter()
        .filter(|(_, b)| b.role == want.role || (storage && b.role.class() == Class::Storage))
        .map(|(_, b)| b.node)
        .collect();
    let fitting: Vec<&Machine> = machines
        .iter()
        .filter(|m| m.class == want.role.class() && !excluded.contains(&m.node))
        .collect();
    // A weightless booking takes no slot (#753 (per-journal proxies)).
    let weighs = Booking::weighs_for(want.role, want.target);
    let eligible: Vec<&Machine> = fitting
        .iter()
        .copied()
        .filter(|m| !weighs || m.free() > 0)
        .collect();
    if eligible.len() < missing {
        let short = missing - eligible.len();
        return Err(if fitting.len() >= missing {
            Shortfall::Capacity { short }
        } else {
            Shortfall::Machines { short }
        });
    }
    // Instances of this role the target holds per zone: rule 3 counts them.
    let mut per_zone: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, booking) in current.iter().filter(|(_, b)| b.role == want.role) {
        if let Some(machine) = machines.iter().find(|m| m.node == booking.node) {
            *per_zone.entry(machine.zone.as_str()).or_default() += 1;
        }
    }
    let mut left = eligible;
    let mut chosen = Vec::with_capacity(missing);
    // Rule 6: the machines that are up first, the others only after them;
    // a machine low on space (#360 (disk full)) counts as down here.
    let first = |m: &Machine| m.up && !m.low_space;
    for up in [true, false] {
        while chosen.len() < missing {
            // Zones that tie on the target's instances go least-loaded
            // first (the mean of their machines' booked slots), then by the
            // draw.
            let Some(zone) = left
                .iter()
                .filter(|m| first(m) == up)
                .map(|m| {
                    let zone = m.zone.as_str();
                    let (sum, n) = left
                        .iter()
                        .filter(|o| first(o) == up && o.zone == zone)
                        .fold((0_u64, 0_u64), |(sum, n), o| {
                            (sum.saturating_add(o.booked), n + 1)
                        });
                    (zone, sum.saturating_mul(1024) / n.max(1))
                })
                .min_by_key(|(zone, load)| {
                    (
                        per_zone.get(zone).copied().unwrap_or(0),
                        *load,
                        rank(seed, zone_hash(zone)),
                    )
                })
                .map(|(zone, _)| zone)
            else {
                break;
            };
            let (index, machine) = left
                .iter()
                .enumerate()
                .filter(|(_, m)| first(m) == up && m.zone == zone)
                .min_by_key(|(_, m)| (m.booked, m.light, rank(seed, m.node.0)))
                .map(|(i, m)| (i, *m))
                .expect("a zone picked holds a machine");
            *per_zone.entry(zone).or_default() += 1;
            chosen.push(machine.node);
            left.swap_remove(index);
        }
    }
    assert_eq!(
        chosen.len(),
        missing,
        "enough eligible machines fill the count"
    );
    let distinct: BTreeSet<NodeId> = chosen.iter().copied().collect();
    assert_eq!(distinct.len(), chosen.len(), "a plan books a machine once");
    assert!(
        chosen.iter().all(|node| !excluded.contains(node)),
        "a plan never books a machine the target uses already"
    );
    Ok(chosen.into_iter().map(|node| Step::Book { node }).collect())
}

/// A zone name's FNV-1a hash: its identity in the seeded order.
fn zone_hash(zone: &str) -> u64 {
    zone.bytes().fold(0xCBF2_9CE4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3)
    })
}

/// The seeded rank of `value`: splitmix64 over the seed and the value.
fn rank(seed: u64, value: u64) -> u64 {
    let mut x = seed ^ value.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

    fn machine(node: u64, zone: &str, capacity: u64) -> Machine {
        Machine {
            node: NodeId(node),
            class: Class::Storage,
            zone: zone.to_string(),
            capacity,
            booked: 0,
            light: 0,
            up: true,
            low_space: false,
        }
    }

    fn journal(id: u64) -> BookingTarget {
        BookingTarget::Journal(JournalIdentifier::new(TenantId(7), JournalId(id)))
    }

    fn want(target: BookingTarget, count: usize) -> Want {
        Want {
            role: Role::Acceptor,
            target,
            count,
        }
    }

    /// Book `steps` into `machines` and `bookings`, as the registry would.
    fn apply(
        machines: &mut [Machine],
        bookings: &mut Vec<(u64, Booking)>,
        want: &Want,
        steps: &[Step],
    ) {
        for step in steps {
            match *step {
                Step::Book { node } => {
                    let m = machines.iter_mut().find(|m| m.node == node).expect("known");
                    assert!(m.free() > 0, "never past capacity");
                    m.booked += 1;
                    let id = bookings.len() as u64 + 1000;
                    bookings.push((
                        id,
                        Booking {
                            node,
                            role: want.role,
                            target: want.target,
                        },
                    ));
                }
                Step::Release { booking, node } => {
                    bookings.retain(|(id, _)| *id != booking);
                    let m = machines.iter_mut().find(|m| m.node == node).expect("known");
                    m.booked -= 1;
                }
            }
        }
    }

    fn of(bookings: &[(u64, Booking)], target: BookingTarget) -> Vec<(u64, Booking)> {
        bookings
            .iter()
            .filter(|(_, b)| b.target == target)
            .copied()
            .collect()
    }

    #[test]
    fn six_machines_take_three_journals_of_two_on_six_machines() {
        // The example of §3.4: six storage machines, three journals of two.
        let mut machines: Vec<Machine> = (1..=6)
            .map(|n| machine(n, &format!("zone-{}", n % 3), 4))
            .collect();
        let mut bookings = Vec::new();
        for j in 1..=3 {
            let want = want(journal(j), 2);
            let steps = plan(&want, &machines, &of(&bookings, journal(j)), 7).expect("fits");
            apply(&mut machines, &mut bookings, &want, &steps);
        }
        let used: BTreeSet<NodeId> = bookings.iter().map(|(_, b)| b.node).collect();
        assert_eq!(used.len(), 6, "every machine takes one instance");
    }

    #[test]
    fn five_acceptors_spread_over_three_zones_two_two_one() {
        let machines: Vec<Machine> = (1..=9)
            .map(|n| machine(n, &format!("zone-{}", n % 3), 4))
            .collect();
        let steps = plan(&want(journal(1), 5), &machines, &[], 11).expect("fits");
        let mut per_zone: BTreeMap<String, usize> = BTreeMap::new();
        for node in booked(&steps) {
            let m = machines.iter().find(|m| m.node == node).expect("known");
            *per_zone.entry(m.zone.clone()).or_default() += 1;
        }
        let mut counts: Vec<usize> = per_zone.into_values().collect();
        counts.sort_unstable();
        assert_eq!(counts, vec![1, 2, 2]);
    }

    #[test]
    fn the_least_loaded_machine_of_a_zone_takes_the_instance() {
        let mut machines = vec![machine(1, "a", 4), machine(2, "a", 4)];
        machines[0].booked = 3;
        let steps = plan(&want(journal(1), 1), &machines, &[], 3).expect("fits");
        assert_eq!(booked(&steps), vec![NodeId(2)]);
    }

    #[test]
    fn a_machine_marked_down_is_the_last_choice() {
        let mut machines = vec![machine(1, "a", 4), machine(2, "b", 4), machine(3, "c", 4)];
        machines[0].up = false;
        let steps = plan(&want(journal(1), 2), &machines, &[], 5).expect("fits");
        let mut nodes = booked(&steps);
        nodes.sort_unstable();
        assert_eq!(nodes, vec![NodeId(2), NodeId(3)]);
        let steps = plan(&want(journal(1), 3), &machines, &[], 5).expect("fits");
        assert_eq!(booked(&steps).last(), Some(&NodeId(1)));
    }

    #[test]
    fn a_machine_low_on_space_is_the_last_choice_and_keeps_its_slots() {
        let mut machines = vec![machine(1, "a", 4), machine(2, "b", 4), machine(3, "c", 4)];
        machines[0].low_space = true;
        let steps = plan(&want(journal(1), 2), &machines, &[], 5).expect("fits");
        let mut nodes = booked(&steps);
        nodes.sort_unstable();
        assert_eq!(nodes, vec![NodeId(2), NodeId(3)]);
        let mut bookings = Vec::new();
        let three = want(journal(1), 3);
        let steps = plan(&three, &machines, &[], 5).expect("fits");
        assert_eq!(booked(&steps).last(), Some(&NodeId(1)));
        apply(&mut machines, &mut bookings, &three, &steps);
        // The mark moves no role already placed: the same count plans nothing.
        assert!(
            plan(&three, &machines, &bookings, 5)
                .expect("fits")
                .is_empty()
        );
    }

    #[test]
    fn a_full_cell_is_a_capacity_shortfall_and_a_small_one_a_machine_shortfall() {
        let mut machines = vec![machine(1, "a", 1), machine(2, "b", 1), machine(3, "c", 1)];
        machines[0].booked = 1;
        assert_eq!(
            plan(&want(journal(1), 3), &machines, &[], 1),
            Err(Shortfall::Capacity { short: 1 })
        );
        assert_eq!(
            plan(&want(journal(1), 4), &machines, &[], 1),
            Err(Shortfall::Machines { short: 2 })
        );
    }

    #[test]
    fn a_journal_proxy_takes_no_slot_and_spreads_by_zone() {
        // Full stateless machines still take a journal's proxy leaders:
        // the booking weighs nothing (#753 (per-journal proxies)).
        let mut machines = vec![machine(1, "a", 1), machine(2, "b", 1), machine(3, "a", 1)];
        for m in &mut machines {
            m.class = Class::Stateless;
            m.booked = 1;
        }
        machines[0].light = 5;
        let proxies = Want {
            role: Role::ProxyLeader,
            target: journal(1),
            count: 2,
        };
        let steps = plan(&proxies, &machines, &[], 1).expect("fits");
        let mut nodes = booked(&steps);
        nodes.sort_unstable();
        // One per zone, and the lighter machine of zone a.
        assert_eq!(nodes, vec![NodeId(2), NodeId(3)]);
        // A tenant's slot still weighs.
        let frontends = Want {
            role: Role::Frontend,
            target: BookingTarget::Tenant {
                tenant: TenantId(1),
            },
            count: 1,
        };
        assert_eq!(
            plan(&frontends, &machines, &[], 1),
            Err(Shortfall::Capacity { short: 1 })
        );
    }

    #[test]
    fn a_stateless_machine_never_takes_a_storage_role() {
        let mut machines = vec![machine(1, "a", 4), machine(2, "b", 4)];
        machines[1].class = Class::Stateless;
        assert_eq!(
            plan(&want(journal(1), 2), &machines, &[], 1),
            Err(Shortfall::Machines { short: 1 })
        );
    }

    #[test]
    fn one_journal_never_puts_two_storage_instances_on_a_machine() {
        let machines = vec![machine(1, "a", 4), machine(2, "b", 4)];
        let replica = Booking {
            node: NodeId(1),
            role: Role::Replica,
            target: journal(1),
        };
        let steps = plan(&want(journal(1), 1), &machines, &[(9, replica)], 1).expect("fits");
        assert_eq!(booked(&steps), vec![NodeId(2)]);
    }

    #[test]
    fn a_smaller_count_releases_the_down_then_the_most_loaded() {
        let mut machines = vec![machine(1, "a", 4), machine(2, "b", 4), machine(3, "c", 4)];
        let mut bookings = Vec::new();
        let three = want(journal(1), 3);
        let steps = plan(&three, &machines, &[], 1).expect("fits");
        apply(&mut machines, &mut bookings, &three, &steps);
        machines[1].up = false;
        machines[2].booked = 3;
        let steps = plan(
            &want(journal(1), 1),
            &machines,
            &of(&bookings, journal(1)),
            1,
        )
        .expect("a release always fits");
        let released: Vec<NodeId> = steps
            .iter()
            .map(|s| match s {
                Step::Release { node, .. } => *node,
                Step::Book { .. } => panic!("a smaller count only releases"),
            })
            .collect();
        assert_eq!(released, vec![NodeId(2), NodeId(3)]);
        assert_eq!(
            plan(
                &want(journal(1), 0),
                &machines,
                &of(&bookings, journal(1)),
                1
            )
            .map(|s| s.len()),
            Ok(3)
        );
    }

    #[test]
    fn the_same_inputs_give_the_same_steps_and_seeds_shuffle_ties() {
        let machines: Vec<Machine> = (1..=12)
            .map(|n| machine(n, &format!("zone-{}", n % 3), 4))
            .collect();
        let one = plan(&want(journal(1), 3), &machines, &[], 42);
        assert_eq!(one, plan(&want(journal(1), 3), &machines, &[], 42));
        let shapes: BTreeSet<Vec<NodeId>> = (0..16)
            .map(|seed| booked(&plan(&want(journal(1), 3), &machines, &[], seed).expect("fits")))
            .collect();
        assert!(shapes.len() > 1, "tenants of one shape spread differently");
    }

    /// Many cells and counts from a fixed generator: every plan keeps the
    /// rules, and a zone never holds two more instances than another one
    /// that still had a free machine.
    #[test]
    fn every_plan_keeps_the_rules() {
        let mut x: u64 = 0x5EED;
        let mut next = |bound: u64| {
            x = rank(x, 1);
            x % bound
        };
        for _ in 0..500 {
            let count = 1 + usize::try_from(next(12)).expect("small");
            let zones = 1 + next(4);
            let mut machines: Vec<Machine> = (1..=count as u64)
                .map(|n| {
                    let mut m = machine(n, &format!("z{}", next(zones)), 1 + next(4));
                    m.booked = next(m.capacity + 1);
                    m.up = next(5) != 0;
                    m
                })
                .collect();
            let asked = usize::try_from(next(6)).expect("small");
            let wanted = want(journal(1), asked);
            let seed = next(u64::MAX);
            match plan(&wanted, &machines, &[], seed) {
                Ok(steps) => {
                    let nodes = booked(&steps);
                    assert_eq!(nodes.len(), asked);
                    let mut bookings = Vec::new();
                    apply(&mut machines, &mut bookings, &wanted, &steps);
                    let mut per_zone: BTreeMap<&str, usize> = BTreeMap::new();
                    for node in &nodes {
                        let m = machines.iter().find(|m| m.node == *node).expect("known");
                        *per_zone.entry(m.zone.as_str()).or_default() += 1;
                    }
                    // A zone with a free, up machine left never trails the
                    // busiest zone by two when every instance went to an up
                    // machine.
                    let all_up = nodes
                        .iter()
                        .all(|n| machines.iter().any(|m| m.node == *n && m.up));
                    let most = per_zone.values().copied().max().unwrap_or(0);
                    for m in machines.iter().filter(|m| m.up && m.free() > 0) {
                        if all_up && !nodes.contains(&m.node) {
                            let here = per_zone.get(m.zone.as_str()).copied().unwrap_or(0);
                            assert!(here + 1 >= most, "zones fill in round robin");
                        }
                    }
                }
                Err(Shortfall::Capacity { short } | Shortfall::Machines { short }) => {
                    let free = machines.iter().filter(|m| m.free() > 0).count();
                    assert_eq!(free + short, asked);
                }
            }
        }
    }

    #[test]
    fn a_registered_machine_is_offered_only_once_it_serves() {
        use crate::registry::SystemCommand;
        let register = |id: u64| SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class: Class::Storage,
            capacity: 2,
            locality: crate::machine::Locality::zone("z"),
            name: format!("m{id}"),
            incarnation: u128::from(id),
            machine_interface: Vec::new(),
        };
        let mut registry = Registry::new([NodeId(1)]);
        registry.fold(0, &register(1).encode());
        registry.fold(1, &register(2).encode());
        let offered = |registry: &Registry| -> Vec<NodeId> {
            machines(registry, [NodeId(1), NodeId(2)])
                .iter()
                .map(|m| m.node)
                .collect()
        };
        // The founding member serves; the registered machine whose
        // admission never landed is passed over (#511).
        assert_eq!(offered(&registry), [NodeId(1)]);
        registry.fold(
            2,
            &SystemCommand::MachineAdmitted { id: NodeId(2) }.encode(),
        );
        assert_eq!(offered(&registry), [NodeId(1), NodeId(2)]);
    }

    /// The zone rule binds only where it can hold, and a round-robin plan
    /// over three zones meets it (#215 (zones)).
    #[test]
    fn the_zone_rule_binds_over_three_zones_and_a_spread_plan_meets_it() {
        let two = vec![machine(1, "a", 4), machine(2, "b", 4), machine(3, "b", 4)];
        assert!(
            !zone_rule_binds(1, Class::Storage, &two),
            "two zones never bind"
        );
        let three = vec![
            machine(1, "a", 4),
            machine(2, "b", 4),
            machine(3, "c", 4),
            machine(4, "a", 4),
        ];
        assert!(zone_rule_binds(1, Class::Storage, &three));
        assert!(
            !zone_rule_binds(0, Class::Storage, &three),
            "F = 0 is exempt"
        );
        let three_of = want(journal(1), 3);
        let steps = plan(&three_of, &three, &[], 7).expect("fits");
        let config = zoned(&booked(&steps), &three, QuorumSystem::Majority);
        assert!(config.is_zoned());
        assert!(config.zone_rule().holds(), "zone round robin spreads 1/1/1");
        // Four members over three zones is 2/1/1: it loses its majority.
        let four = plan(&want(journal(2), 4), &three, &[], 7).expect("fits");
        let config = zoned(&booked(&four), &three, QuorumSystem::Majority);
        assert!(!config.zone_rule().holds());
        // A machine with no zone leaves the configuration unlabeled.
        let bare = vec![machine(1, "", 4), machine(2, "b", 4), machine(3, "c", 4)];
        let config = zoned(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &bare,
            QuorumSystem::Majority,
        );
        assert!(!config.is_zoned());
    }
}
