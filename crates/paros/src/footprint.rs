//! **The tenant footprint** (#476, `docs/architecture.md` §3.4): the slots a
//! tenant's birth books against its cell's capacity, so a cell hosts only a
//! tenant it can run. A footprint is one coordinator slot, the tenant's
//! matchmaker set, one acceptor quorum of its durability for data, and one
//! `failures-per-partition=1` quorum for its control journal (a
//! `failures-per-partition=0` tenant's control journal still needs one).
//! The cell tenant and the universe tenant have one too: `init` books them.
//!
//! The footprint is a reservation of slots (`BookingTarget::Tenant`), not
//! a placement of role instances: its storage slots may stack on one
//! machine, and the roles a tenant runs later book their own slots on top.
//! Its coordinator slot is where the cell coordinator places the tenant's
//! coordinator, on a `stateless` machine. While no `stateless` machine that
//! serves the cell has a free slot, the founding members host the
//! coordinator and the footprint books no coordinator slot; a cell refuses
//! a tenant only for want of `storage` slots.
//!
//! Both functions are pure, so every writer that folded the same registry
//! computes the same slots: [`plan`] books a footprint at birth, written as
//! one `GrantCapacity` (all slots or none), and [`fix`] is one step of the
//! cell coordinator's watch, which keeps every footprint whole: it places a
//! coordinator once a `stateless` machine can take it, places a dead one
//! (a machine marked down, draining or gone) again, and moves storage slots
//! off a draining machine.

use std::collections::BTreeMap;

use paros_core::{NodeId, TenantId};

use crate::machine::Class;
use crate::placement::{self, Machine};
use crate::system::{BookingTarget, NodeStanding, Registry, Role, Slot};
use crate::tenant::Durability;

/// The durability a tenant is born with: its footprint's. A creation does
/// not name one yet, and a journal's own durability books its own slots.
pub const BIRTH: Durability = Durability::DEFAULT;

/// The slots of one footprint, by part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Footprint {
    /// One acceptor quorum of the tenant's durability: `F+1`.
    pub acceptors: u64,
    /// One `failures-per-partition=1` quorum for its control journal: 2.
    pub control: u64,
    /// Its matchmaker set: `2F+1`.
    pub matchmakers: u64,
}

/// The quorum of a `failures-per-partition=1` partition: 2 of 3.
const CONTROL_QUORUM: u64 = 2;

impl Footprint {
    /// The footprint of a tenant of `durability`.
    ///
    /// # Panics
    ///
    /// Never: a footprint always books its two quorums and its set.
    #[must_use]
    pub fn of(durability: Durability) -> Self {
        let f = u64::from(durability.failures_per_partition);
        let footprint = Footprint {
            acceptors: f + 1,
            control: CONTROL_QUORUM,
            matchmakers: 2 * f + 1,
        };
        assert!(
            footprint.storage() > 1 + CONTROL_QUORUM,
            "a footprint books storage"
        );
        footprint
    }

    /// The footprint every tenant is born with.
    #[must_use]
    pub fn birth() -> Self {
        Self::of(BIRTH)
    }

    /// Its `storage` slots.
    #[must_use]
    pub fn storage(&self) -> u64 {
        self.acceptors + self.control + self.matchmakers
    }

    /// Its `storage` slots by role: the two acceptor quorums, then the set.
    fn by_role(&self) -> [(Role, u64); 2] {
        [
            (Role::Acceptor, self.acceptors + self.control),
            (Role::Matchmaker, self.matchmakers),
        ]
    }
}

/// Why a footprint does not fit: `short` slots of `class` found no machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shortfall {
    /// The class.
    pub class: Class,
    /// How many slots found no free machine.
    pub short: u64,
}

/// Whether a footprint may be refused from `registry`: every founding
/// member registered (#349). An unregistered founder offers no slot, so a
/// refusal before then could refuse a tenant the cell can host.
#[must_use]
pub fn ready(registry: &Registry) -> bool {
    registry.genesis().all(|node| registry.get(node).is_some())
}

/// The machines a footprint may book: registered, not draining, serving the
/// cell.
fn offered(registry: &Registry) -> Vec<Machine> {
    placement::machines(registry, registry.pool())
}

/// The machine of `class` that takes the next slot: up first, then the most
/// free slots, then the lowest id. `taken` counts the slots a plan holds on
/// each machine already. `None` when no machine has a free slot.
fn pick(
    machines: &[Machine],
    class: Class,
    taken: &BTreeMap<NodeId, u64>,
    skip: Option<NodeId>,
) -> Option<NodeId> {
    machines
        .iter()
        .filter(|m| m.class == class && Some(m.node) != skip)
        .map(|m| {
            (
                m,
                m.free()
                    .saturating_sub(taken.get(&m.node).copied().unwrap_or(0)),
            )
        })
        .filter(|(_, free)| *free > 0)
        .max_by(|(a, free_a), (b, free_b)| {
            a.up.cmp(&b.up)
                .then(free_a.cmp(free_b))
                .then(b.node.cmp(&a.node))
        })
        .map(|(m, _)| m.node)
}

/// A booking id for the `k`th slot `tenant` asks for at `salt`, never one
/// the registry spent or `held` holds: a function of the fold alone.
fn booking_id(registry: &Registry, tenant: TenantId, salt: u64, held: &[Slot]) -> u64 {
    let mut k = 0_u64;
    loop {
        let mut x = tenant.0 ^ salt.rotate_left(23) ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        if x != 0
            && registry.booking(x).is_none()
            && !registry.spent().any(|s| s == x)
            && held.iter().all(|s| s.booking != x)
        {
            return x;
        }
        k += 1;
    }
}

/// The slots `tenant`'s footprint books, from `registry`: every `storage`
/// slot spread over the cell's storage machines (the most free first), and
/// the coordinator slot on a `stateless` machine when one has a free slot.
///
/// # Errors
///
/// Too few free `storage` slots: the cell refuses the tenant.
///
/// # Panics
///
/// If `tenant` is unset (a programmer error of the caller).
pub fn plan(
    registry: &Registry,
    tenant: TenantId,
    footprint: &Footprint,
) -> Result<Vec<Slot>, Shortfall> {
    assert!(tenant.is_set(), "a footprint is a tenant's");
    let machines = offered(registry);
    let target = BookingTarget::Tenant { tenant };
    let salt = registry.next_seq();
    let mut taken: BTreeMap<NodeId, u64> = BTreeMap::new();
    let mut slots: Vec<Slot> = Vec::new();
    let mut short = 0;
    for (role, count) in footprint.by_role() {
        for _ in 0..count {
            let Some(node) = pick(&machines, Class::Storage, &taken, None) else {
                short += 1;
                continue;
            };
            *taken.entry(node).or_default() += 1;
            let booking = booking_id(registry, tenant, salt, &slots);
            slots.push(Slot {
                booking,
                node,
                role,
                target,
            });
        }
    }
    if short > 0 {
        moonpool_assertions::reachable!("footprint: a tenant's footprint does not fit");
        return Err(Shortfall {
            class: Class::Storage,
            short,
        });
    }
    if let Some(node) = pick(&machines, Class::Stateless, &taken, None) {
        let booking = booking_id(registry, tenant, salt, &slots);
        slots.push(Slot {
            booking,
            node,
            role: Role::Coordinator,
            target,
        });
    }
    assert_eq!(
        slots
            .iter()
            .filter(|s| s.role.class() == Class::Storage)
            .count() as u64,
        footprint.storage(),
        "a planned footprint holds every storage slot"
    );
    Ok(slots)
}

/// One step that keeps a footprint whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fix {
    /// Book this slot.
    Book(Slot),
    /// Release this booking: a slot on a machine that left, now replaced.
    Release {
        /// The booking.
        booking: u64,
    },
}

/// The next step that keeps `tenant`'s granted footprint whole, from
/// `registry`, or `None` when it is whole or nothing can be done yet:
///
/// - its coordinator slot: placed on a `stateless` machine that serves the
///   cell, is up and has a free slot, when it has none on a live machine
///   (registered, not draining, up); a slot on a machine that is not live is
///   released once another one holds the coordinator, and so is every live
///   slot but the first (a dead one whose machine came back up);
/// - its `storage` slots: one booked for each the footprint lacks on a
///   registered machine that is not draining (a retired machine took its
///   slots with it), then a slot on a draining machine is released.
#[must_use]
pub fn fix(registry: &Registry, tenant: TenantId, footprint: &Footprint) -> Option<Fix> {
    let target = BookingTarget::Tenant { tenant };
    let held: Vec<(u64, crate::system::Booking)> = registry
        .bookings()
        .filter(|(_, b)| b.target == target)
        .map(|(id, b)| (id, *b))
        .collect();
    let registered = |node: NodeId| {
        registry
            .get(node)
            .is_some_and(|n| n.standing == NodeStanding::Registered)
    };
    let live = |node: NodeId| registered(node) && registry.liveness(node).up;
    let machines = offered(registry);
    let salt = registry.next_seq();
    let taken = BTreeMap::new();
    // The coordinator first: it is the one slot that runs something.
    let coordinators: Vec<(u64, NodeId)> = held
        .iter()
        .filter(|(_, b)| b.role == Role::Coordinator)
        .map(|(id, b)| (*id, b.node))
        .collect();
    let placed = coordinators.iter().any(|(_, node)| live(*node));
    if !placed {
        let dead = coordinators.first().map(|(_, node)| *node);
        let up: Vec<Machine> = machines.iter().filter(|m| m.up).cloned().collect();
        if let Some(node) = pick(&up, Class::Stateless, &taken, dead) {
            if dead.is_some() {
                moonpool_assertions::reachable!(
                    "footprint: a dead tenant coordinator is placed again"
                );
            }
            return Some(Fix::Book(Slot {
                booking: booking_id(registry, tenant, salt, &[]),
                node,
                role: Role::Coordinator,
                target,
            }));
        }
    } else if let Some((booking, _)) = coordinators.iter().find(|(_, node)| !live(*node)) {
        return Some(Fix::Release { booking: *booking });
    } else if let Some((booking, _)) = coordinators.iter().filter(|(_, node)| live(*node)).nth(1) {
        // A dead coordinator's machine came back up (same incarnation,
        // `MachineUp`) after its re-placement and before its release: two
        // live slots, and the release above sees none dead. Keep the first.
        return Some(Fix::Release { booking: *booking });
    }
    for (role, count) in footprint.by_role() {
        let of_role: Vec<(u64, NodeId)> = held
            .iter()
            .filter(|(_, b)| b.role == role)
            .map(|(id, b)| (*id, b.node))
            .collect();
        let staying = of_role.iter().filter(|(_, node)| registered(*node)).count() as u64;
        if staying < count {
            let node = pick(&machines, Class::Storage, &taken, None)?;
            return Some(Fix::Book(Slot {
                booking: booking_id(registry, tenant, salt, &[]),
                node,
                role,
                target,
            }));
        }
        if let Some((booking, _)) = of_role.iter().find(|(_, node)| !registered(*node)) {
            moonpool_assertions::reachable!("footprint: a slot leaves a draining machine");
            return Some(Fix::Release { booking: *booking });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::{GrantKey, RegistryEvent, SystemCommand};

    fn register(reg: &mut Registry, seq: u64, id: u64, class: Class, capacity: u64) {
        let command = SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class,
            capacity,
            locality: crate::machine::Locality::default(),
            name: String::new(),
            incarnation: 1,
            machine_interface: Vec::new(),
        };
        assert!(matches!(
            reg.fold(seq, &command.encode()),
            RegistryEvent::Registered { .. }
        ));
    }

    #[test]
    fn the_birth_footprint_is_seven_storage_slots() {
        let footprint = Footprint::birth();
        assert_eq!(footprint.storage(), 7);
        let none = Footprint::of(Durability::new(0, 1).unwrap());
        assert_eq!((none.acceptors, none.control, none.matchmakers), (1, 2, 1));
    }

    #[test]
    fn a_plan_spreads_stacks_and_refuses() {
        let mut reg = Registry::new([NodeId(1), NodeId(2)]);
        assert!(!ready(&reg), "an unregistered founder holds a refusal back");
        register(&mut reg, 0, 1, Class::Storage, 4);
        register(&mut reg, 1, 2, Class::Storage, 4);
        assert!(ready(&reg));
        let tenant = TenantId(9);
        let slots = plan(&reg, tenant, &Footprint::birth()).unwrap();
        assert_eq!(slots.len(), 7, "no stateless machine: no coordinator slot");
        let on_one = slots.iter().filter(|s| s.node == NodeId(1)).count();
        assert!((3..=4).contains(&on_one), "the slots spread: {on_one}");
        let grant = SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(tenant),
            tenant,
            slots: Some(slots),
        };
        assert!(matches!(
            reg.fold(2, &grant.encode()),
            RegistryEvent::Granted { .. }
        ));
        // One slot left: a second tenant does not fit.
        assert_eq!(
            plan(&reg, TenantId(10), &Footprint::birth()),
            Err(Shortfall {
                class: Class::Storage,
                short: 6
            })
        );
        assert_eq!(
            fix(&reg, tenant, &Footprint::birth()),
            None,
            "a whole footprint needs nothing"
        );
    }

    #[test]
    fn the_coordinator_is_placed_on_a_stateless_machine_and_again_when_it_dies() {
        let mut reg = Registry::new([NodeId(1)]);
        register(&mut reg, 0, 1, Class::Storage, 16);
        let tenant = TenantId(9);
        let slots = plan(&reg, tenant, &Footprint::birth()).unwrap();
        let grant = SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(tenant),
            tenant,
            slots: Some(slots),
        };
        assert!(matches!(
            reg.fold(1, &grant.encode()),
            RegistryEvent::Granted { .. }
        ));
        assert_eq!(
            fix(&reg, tenant, &Footprint::birth()),
            None,
            "no stateless machine yet"
        );
        // A stateless machine serves the cell: the coordinator goes there.
        register(&mut reg, 2, 5, Class::Stateless, 1);
        let admitted = SystemCommand::MachineAdmitted { id: NodeId(5) };
        reg.fold(3, &admitted.encode());
        let Some(Fix::Book(slot)) = fix(&reg, tenant, &Footprint::birth()) else {
            panic!("the coordinator is placed");
        };
        assert_eq!((slot.node, slot.role), (NodeId(5), Role::Coordinator));
        let book = SystemCommand::BookCapacity {
            booking: slot.booking,
            node: slot.node,
            role: slot.role,
            target: slot.target,
        };
        assert!(matches!(
            reg.fold(4, &book.encode()),
            RegistryEvent::Booked { .. }
        ));
        assert_eq!(fix(&reg, tenant, &Footprint::birth()), None);
        // It dies, and a second stateless machine takes it over.
        register(&mut reg, 5, 6, Class::Stateless, 1);
        reg.fold(
            6,
            &SystemCommand::MachineAdmitted { id: NodeId(6) }.encode(),
        );
        let down = SystemCommand::MachineDown {
            id: NodeId(5),
            incarnation: 1,
        };
        assert!(matches!(
            reg.fold(7, &down.encode()),
            RegistryEvent::MachineDown { .. }
        ));
        let Some(Fix::Book(again)) = fix(&reg, tenant, &Footprint::birth()) else {
            panic!("a dead coordinator is placed again");
        };
        assert_eq!(again.node, NodeId(6));
        let book = SystemCommand::BookCapacity {
            booking: again.booking,
            node: again.node,
            role: again.role,
            target: again.target,
        };
        assert!(matches!(
            reg.fold(8, &book.encode()),
            RegistryEvent::Booked { .. }
        ));
        assert_eq!(
            fix(&reg, tenant, &Footprint::birth()),
            Some(Fix::Release {
                booking: slot.booking
            })
        );
        // It answers again before its release: two live coordinator slots,
        // and the next step releases one of them.
        let up = SystemCommand::MachineUp {
            id: NodeId(5),
            incarnation: 1,
        };
        assert!(matches!(
            reg.fold(9, &up.encode()),
            RegistryEvent::MachineUp { was_down: true, .. }
        ));
        let Some(Fix::Release { booking }) = fix(&reg, tenant, &Footprint::birth()) else {
            panic!("a second live coordinator is released");
        };
        assert!(booking == slot.booking || booking == again.booking);
        let release = SystemCommand::ReleaseCapacity { booking };
        assert!(matches!(
            reg.fold(10, &release.encode()),
            RegistryEvent::Released { .. }
        ));
        assert_eq!(fix(&reg, tenant, &Footprint::birth()), None);
    }
}
