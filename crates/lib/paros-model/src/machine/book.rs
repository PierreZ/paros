//! A cell's **address book** (#349, `docs/architecture/03-2-machines.md` §3.2): where each
//! machine of the cell is dialed, read from the registry fold, never from the
//! plan `init` wrote.
//!
//! A founding member is in the registry's genesis pool by id. Its address
//! starts as the one the cell plan names, and its own `RegisterNode` (written
//! by the cell coordinator when the address it advertises changed, #349)
//! replaces it. Every other machine of the cell is in the book once it
//! registered. A retired machine is not in the book.

use paros_core::NodeId;

use crate::address::Address;
use crate::registry::{NodeStanding, Registry};

/// The address peers dial for each founding member of `founders`: the
/// address `registry` holds for it when it registered one (#349), else the
/// cell plan's. Same order as `founders`.
///
/// # Panics
///
/// Never: the assertions check that every founder keeps one address.
#[must_use]
pub fn address_book(founders: &[(NodeId, Address)], registry: &Registry) -> Vec<(NodeId, Address)> {
    let book: Vec<(NodeId, Address)> = founders
        .iter()
        .map(|(id, planned)| {
            let registered = registry
                .address(*id)
                .and_then(|addr| Address::parse(addr).ok());
            (*id, registered.unwrap_or_else(|| planned.clone()))
        })
        .collect();
    assert_eq!(
        book.len(),
        founders.len(),
        "every founder keeps one address"
    );
    assert!(
        book.iter().zip(founders).all(|(a, b)| a.0 == b.0),
        "the book keeps the founders' ids and order"
    );
    book
}

/// Every machine of the cell and where it is dialed: the founding members
/// ([`address_book`]), then every registered machine that is not a founding
/// member and not retired, in id order.
#[must_use]
pub fn cell_book(founders: &[(NodeId, Address)], registry: &Registry) -> Vec<(NodeId, Address)> {
    let founder = |id: NodeId| founders.iter().any(|(f, _)| *f == id);
    let mut book = address_book(founders, registry);
    book.extend(
        registry
            .nodes()
            .filter(|(id, node)| node.standing != NodeStanding::Retired && !founder(*id))
            .filter_map(|(id, node)| Address::parse(&node.addr).ok().map(|addr| (id, addr))),
    );
    book
}

/// The machines of another cell's entry `endpoint` (#232 (multiple
/// cells)), each at the address that cell's `registry` holds for it when it
/// registered one (#349: a machine that moved), else at the endpoint's own.
/// Same ids and order as `endpoint`: what a universe pointer names.
///
/// # Panics
///
/// Never: the assertion checks that the hosts keep the endpoint's ids.
#[must_use]
pub fn current_hosts(endpoint: &[(u64, Address)], registry: &Registry) -> Vec<(u64, Address)> {
    let founders: Vec<(NodeId, Address)> = endpoint
        .iter()
        .map(|(id, addr)| (NodeId(*id), addr.clone()))
        .collect();
    let hosts: Vec<(u64, Address)> = address_book(&founders, registry)
        .into_iter()
        .map(|(id, addr)| (id.0, addr))
        .collect();
    assert!(
        hosts.iter().map(|h| h.0).eq(endpoint.iter().map(|h| h.0)),
        "the hosts keep the endpoint's ids and order"
    );
    hosts
}

/// The machines that serve a tenant's control journal `journal` (#685
/// (special tenants are tenants)), sorted: the founding members, which
/// serve every tenant control journal from its birth and stay members,
/// and every machine that holds one of its acceptor slots: the coordinator
/// grows the journal onto them to the control plane's `2F+1`
/// (`paros_coordinator::tenant::controls`).
///
/// # Panics
///
/// Never: the assertion checks that every founder is kept.
#[must_use]
pub fn control_members(
    founders: &[NodeId],
    registry: &Registry,
    journal: paros_core::journal::JournalIdentifier,
) -> Vec<NodeId> {
    let target = crate::registry::BookingTarget::Journal(journal);
    let mut members: Vec<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.role == crate::registry::Role::Acceptor && b.target == target)
        .map(|(_, b)| b.node)
        .chain(founders.iter().copied())
        .collect();
    members.sort_unstable();
    members.dedup();
    assert!(
        founders.iter().all(|f| members.contains(f)),
        "a control journal's members hold every founding member"
    );
    members
}

/// `members` as machine `me` dials them: its own entry at the address it
/// advertises now (`own`), which the plan or the registry may not hold yet
/// (#349: a machine that moved reaches itself there).
///
/// # Panics
///
/// If an assertion on its own invariants fails: a programmer error, never
/// an operating condition.
#[must_use]
pub fn with_own(
    members: &[(NodeId, Address)],
    me: NodeId,
    own: &Address,
) -> Vec<(NodeId, Address)> {
    let book: Vec<(NodeId, Address)> = members
        .iter()
        .map(|(id, addr)| {
            let addr = if *id == me { own } else { addr };
            (*id, addr.clone())
        })
        .collect();
    assert!(
        !members.iter().any(|(id, _)| *id == me) || book.contains(&(me, own.clone())),
        "a member dials itself where it is"
    );
    book
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{BookingTarget, Class, Role, SystemCommand};
    use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

    fn register(registry: &mut Registry, seq: u64, id: u64, addr: &str) {
        let record = SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: addr.into(),
            class: Class::Storage,
            locality: crate::machine::Locality::default(),
            name: format!("m{id}"),
            incarnation: 1,
            machine_interface: Vec::new(),
        }
        .encode();
        registry.fold(seq, &record);
    }

    fn book(registry: &mut Registry, booking: u64, node: u64, role: Role, target: BookingTarget) {
        let seq = registry.next_seq();
        let record = SystemCommand::BookCapacity {
            booking,
            node: NodeId(node),
            role,
            target,
        }
        .encode();
        registry.fold(seq, &record);
    }

    #[test]
    fn a_control_journal_runs_on_its_founders_and_its_booked_acceptors() {
        let control = JournalIdentifier::new(TenantId(50), JournalId(51));
        let other = JournalIdentifier::new(TenantId(50), JournalId(52));
        let founders = [NodeId(1), NodeId(2)];
        let mut registry = Registry::new(founders);
        for id in [1, 3, 4, 5] {
            let seq = registry.next_seq();
            register(&mut registry, seq, id, &format!("m{id}.paros:1"));
        }
        // On the founders alone before any booking.
        assert_eq!(control_members(&founders, &registry, control), founders);
        // A slot of the journal on a founder adds no member twice.
        book(
            &mut registry,
            10,
            1,
            Role::Acceptor,
            BookingTarget::Journal(control),
        );
        assert_eq!(control_members(&founders, &registry, control), founders);
        // The negative space: another journal's slot, and another role.
        book(
            &mut registry,
            11,
            4,
            Role::Acceptor,
            BookingTarget::Journal(other),
        );
        book(
            &mut registry,
            12,
            5,
            Role::Replica,
            BookingTarget::Journal(control),
        );
        assert_eq!(control_members(&founders, &registry, control), founders);
        // An acceptor slot of the journal outside the founders is a member,
        // in id order.
        book(
            &mut registry,
            13,
            3,
            Role::Acceptor,
            BookingTarget::Journal(control),
        );
        assert_eq!(
            control_members(&founders, &registry, control),
            [NodeId(1), NodeId(2), NodeId(3)]
        );
        // Released, it leaves.
        let seq = registry.next_seq();
        registry.fold(
            seq,
            &SystemCommand::ReleaseCapacity { booking: 13 }.encode(),
        );
        assert_eq!(control_members(&founders, &registry, control), founders);
    }

    #[test]
    fn a_registered_address_replaces_the_plans() {
        let plan: Vec<(NodeId, Address)> = vec![
            (NodeId(1), "a.paros:1".parse().expect("an address")),
            (NodeId(2), "b.paros:1".parse().expect("an address")),
        ];
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        assert_eq!(address_book(&plan, &registry), plan);
        register(&mut registry, 0, 2, "b2.paros:1");
        register(&mut registry, 1, 7, "c.paros:1");
        let book = address_book(&plan, &registry);
        assert_eq!(book[0], plan[0]);
        assert_eq!(book[1].1.to_string(), "b2.paros:1");
        let all = cell_book(&plan, &registry);
        assert_eq!(all.len(), 3);
        assert_eq!(all[2].0, NodeId(7));
    }

    #[test]
    fn current_hosts_follow_a_renamed_machine_and_keep_the_endpoint() {
        let addr = |a: &str| Address::parse(a).unwrap();
        let endpoint = vec![
            (2, addr("machine-0.paros:4500")),
            (1, addr("machine-1.paros:4500")),
        ];
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        // Nothing registered: the endpoint's own addresses.
        assert_eq!(current_hosts(&endpoint, &registry), endpoint);
        // Machine 2 moved and registered its new name.
        register(&mut registry, 0, 2, "machine-0-1.paros:4500");
        assert_eq!(
            current_hosts(&endpoint, &registry),
            vec![
                (2, addr("machine-0-1.paros:4500")),
                (1, addr("machine-1.paros:4500")),
            ]
        );
        assert!(current_hosts(&[], &registry).is_empty());
    }
}
