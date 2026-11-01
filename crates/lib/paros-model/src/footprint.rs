//! **The tenant footprint** (#476, `docs/architecture/03-4-tenant-modes.md` §3.4): what a
//! tenant's birth asks of its cell. Since #837 (remove role slots) a
//! machine has no capacity in role slots, so a footprint reserves no
//! storage: a cell refuses a tenant only when no `storage` machine serves
//! it, and the roles a tenant runs book their own machines later. What is
//! left is the tenant's coordinator slot (`BookingTarget::Tenant`): where
//! the cell coordinator places the tenant's coordinator, on a `stateless`
//! machine. While no `stateless` machine serves the cell, the founding
//! members host the coordinator and the footprint books no slot.
//! The cell tenant and the universe tenant have one too: `init` books them.
//!
//! Both functions are pure, so every writer that folded the same registry
//! computes the same slots: [`plan`] decides a footprint at birth, written
//! as one `GrantCapacity`, and [`fix`] is one step of the cell
//! coordinator's watch, which keeps every footprint whole: it places a
//! coordinator once a `stateless` machine can take it, places a dead one
//! (a machine marked down, draining or gone) again, and releases a storage
//! slot an older cell booked for a footprint.

use paros_core::NodeId;
use paros_core::journal::TenantId;

use crate::machine::Class;
use crate::placement::{self, Machine};
use crate::registry::{BookingTarget, NodeStanding, Registry, Role, Slot};
use crate::tenant::Durability;

/// The durability a tenant is born with: its footprint's. A creation does
/// not name one yet, and a journal's own durability books its own slots.
pub const BIRTH: Durability = Durability::DEFAULT;

/// What a tenant's birth asks of its cell (#837 (remove role slots)): a
/// coordinator, and at least one storage machine that serves the cell.
/// A machine has no capacity in role slots, so a footprint reserves no
/// storage: the roles a tenant runs book their own machines later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Footprint {
    /// The `storage` machines that must serve the cell: at least one.
    pub storage_machines: u64,
}

impl Footprint {
    /// The footprint every tenant is born with. A durability wider than
    /// the cell's machines is not a refusal here: #591 (refuse, never clamp)
    /// judges each journal's create.
    #[must_use]
    pub fn birth() -> Self {
        Footprint {
            storage_machines: 1,
        }
    }
}

/// Why a footprint does not fit: `short` machines of `class` are missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shortfall {
    /// The class.
    pub class: Class,
    /// How many machines of the class are missing.
    pub short: u64,
}

/// Whether a footprint may be refused from `registry`: every founding
/// member registered (#349) or marked down. An unregistered founder that is
/// up offers no slot yet, so a refusal before then could refuse a tenant the
/// cell can host. A founder the coordinator marked down may never register
/// (a wiped founder's old id never rejoins), so the cell answers from the
/// registry as it stands, and the creation ends `ERROR` (#614 (a tenant
/// waits for a wiped founder); witness 13017347449761959302 on 266a6af,
/// recipe `469168:1407352323402923827,35122:7802532925377564957`).
#[must_use]
pub fn ready(registry: &Registry) -> bool {
    registry
        .genesis()
        .all(|node| registry.get(node).is_some() || !registry.liveness(node).up)
}

/// The machines a footprint may book: registered, not draining, serving the
/// cell.
fn offered(registry: &Registry) -> Vec<Machine> {
    placement::machines(registry, registry.pool())
}

/// The machine of `class` that takes the coordinator: up first, then the
/// fewest roles hosted, then the lowest id. `None` when no machine of the
/// class is offered.
fn pick(machines: &[Machine], class: Class, skip: Option<NodeId>) -> Option<NodeId> {
    machines
        .iter()
        .filter(|m| m.class.takes(class) && Some(m.node) != skip)
        .max_by(|a, b| {
            a.up.cmp(&b.up)
                .then(b.booked.cmp(&a.booked))
                .then(b.node.cmp(&a.node))
        })
        .map(|m| m.node)
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

/// The slots `tenant`'s footprint books, from `registry`: the coordinator
/// slot on a `stateless` machine when one serves the cell, else none (the
/// founding members host the coordinator).
///
/// # Errors
///
/// Too few `storage` machines serve the cell: the cell refuses the tenant.
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
    let storage = machines
        .iter()
        .filter(|m| m.class.stores())
        .count() as u64;
    if storage < footprint.storage_machines {
        return Err(Shortfall {
            class: Class::Storage,
            short: footprint.storage_machines - storage,
        });
    }
    let target = BookingTarget::Tenant { tenant };
    let salt = registry.next_seq();
    let mut slots: Vec<Slot> = Vec::new();
    if let Some(node) = pick(&machines, Class::Stateless, None) {
        let booking = booking_id(registry, tenant, salt, &slots);
        slots.push(Slot {
            booking,
            node,
            role: Role::Coordinator,
            target,
        });
    }
    assert!(slots.len() <= 1, "a footprint books its coordinator only");
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
///   cell and is up, when it has none on a live machine (registered, not
///   draining, up); a slot on a machine that is not live is released once
///   another one holds the coordinator, and so is every live slot but the
///   first (a dead one whose machine came back up);
/// - an acceptor or matchmaker slot of the footprint (booked by a cell
///   before #837 (remove role slots)) is released.
#[must_use]
pub fn fix(registry: &Registry, tenant: TenantId) -> Option<Fix> {
    let target = BookingTarget::Tenant { tenant };
    let held: Vec<(u64, crate::registry::Booking)> = registry
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
        if let Some(node) = pick(&up, Class::Stateless, dead) {
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
    // A footprint reserves no storage any more (#837 (remove role slots)):
    // an acceptor or matchmaker slot a cell booked for it before is
    // released. Another tenant-level slot (a capacity request's replica) is
    // not the footprint's.
    if let Some((booking, _)) = held
        .iter()
        .find(|(_, b)| matches!(b.role, Role::Acceptor | Role::Matchmaker))
    {
        // Never in the simulation, whose cells are born after #837: the
        // unit tests cover it.
        return Some(Fix::Release { booking: *booking });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{GrantKey, RegistryEvent, SystemCommand};

    fn register(reg: &mut Registry, seq: u64, id: u64, class: Class) {
        let command = SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class,
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

    fn grant(reg: &mut Registry, seq: u64, tenant: TenantId, slots: Vec<Slot>) {
        let grant = SystemCommand::GrantCapacity {
            key: GrantKey::Footprint(tenant),
            tenant,
            slots: Some(slots),
        };
        assert!(matches!(
            reg.fold(seq, &grant.encode()),
            RegistryEvent::Granted { .. }
        ));
    }

    #[test]
    fn a_footprint_books_no_storage_and_any_number_of_tenants_fit() {
        let mut reg = Registry::new([NodeId(1)]);
        assert!(!ready(&reg), "an unregistered founder holds a refusal back");
        register(&mut reg, 0, 1, Class::Storage);
        assert!(ready(&reg));
        // One storage machine hosts as many tenants as are created: no slot
        // runs out (#837 (remove role slots)).
        for (seq, tenant) in (1..).zip((9..60).map(TenantId)) {
            let slots = plan(&reg, tenant, &Footprint::birth()).unwrap();
            assert!(slots.is_empty(), "no stateless machine: no slot");
            grant(&mut reg, seq, tenant, slots);
            assert_eq!(fix(&reg, tenant), None, "a whole footprint needs nothing");
        }
    }

    #[test]
    fn an_all_machine_counts_as_storage_and_takes_the_coordinator() {
        // A machine takes every role by default (#839 (PAROS_ROLES)): a cell
        // of `all` machines hosts a tenant, and one of them takes its
        // coordinator.
        let mut reg = Registry::new([NodeId(1)]);
        register(&mut reg, 0, 1, Class::All);
        let slots = plan(&reg, TenantId(9), &Footprint::birth()).unwrap();
        assert_eq!(slots.len(), 1, "the coordinator takes the one machine");
        assert_eq!(slots[0].node, NodeId(1));
        assert_eq!(slots[0].role, Role::Coordinator);
    }

    #[test]
    fn a_cell_with_no_storage_machine_refuses_a_tenant() {
        let mut reg = Registry::new([NodeId(1)]);
        register(&mut reg, 0, 1, Class::Stateless);
        assert_eq!(
            plan(&reg, TenantId(9), &Footprint::birth()),
            Err(Shortfall {
                class: Class::Storage,
                short: 1
            })
        );
    }

    #[test]
    fn an_old_storage_slot_of_a_footprint_is_released() {
        let mut reg = Registry::new([NodeId(1)]);
        register(&mut reg, 0, 1, Class::Storage);
        let tenant = TenantId(9);
        let old = Slot {
            booking: 77,
            node: NodeId(1),
            role: Role::Acceptor,
            target: BookingTarget::Tenant { tenant },
        };
        grant(&mut reg, 1, tenant, vec![old]);
        assert_eq!(fix(&reg, tenant), Some(Fix::Release { booking: 77 }));
        let release = SystemCommand::ReleaseCapacity { booking: 77 };
        assert!(matches!(
            reg.fold(2, &release.encode()),
            RegistryEvent::Released { .. }
        ));
        assert_eq!(fix(&reg, tenant), None);
    }

    #[test]
    fn the_coordinator_is_placed_on_a_stateless_machine_and_again_when_it_dies() {
        let mut reg = Registry::new([NodeId(1)]);
        register(&mut reg, 0, 1, Class::Storage);
        let tenant = TenantId(9);
        let slots = plan(&reg, tenant, &Footprint::birth()).unwrap();
        grant(&mut reg, 1, tenant, slots);
        assert_eq!(fix(&reg, tenant), None, "no stateless machine yet");
        // A stateless machine serves the cell: the coordinator goes there.
        register(&mut reg, 2, 5, Class::Stateless);
        let admitted = SystemCommand::MachineAdmitted { id: NodeId(5) };
        reg.fold(3, &admitted.encode());
        let Some(Fix::Book(slot)) = fix(&reg, tenant) else {
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
        assert_eq!(fix(&reg, tenant), None);
        // It dies, and a second stateless machine takes it over.
        register(&mut reg, 5, 6, Class::Stateless);
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
        let Some(Fix::Book(again)) = fix(&reg, tenant) else {
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
            fix(&reg, tenant),
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
        let Some(Fix::Release { booking }) = fix(&reg, tenant) else {
            panic!("a second live coordinator is released");
        };
        assert!(booking == slot.booking || booking == again.booking);
        let release = SystemCommand::ReleaseCapacity { booking };
        assert!(matches!(
            reg.fold(10, &release.encode()),
            RegistryEvent::Released { .. }
        ));
        assert_eq!(fix(&reg, tenant), None);
    }
}
