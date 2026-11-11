//! A cell's **address book** (#349, `docs/architecture/03-2-machines.md` §3.2): where each
//! machine of the cell is dialed, read from the registry fold, never from the
//! plan `init` wrote.
//!
//! Every machine of the cell is a node of the registry: the initial members
//! from the fold's seed, every other machine from its registration (#685
//! (special tenants are tenants)). A machine that registered is dialed at
//! the address it registered last; one that never registered
//! (`registrations == 0`) at the address the caller knows for it (the cell
//! plan's, or an admission's). A retired machine is not in the book.

use paros_core::NodeId;

use crate::address::Address;
use crate::registry::{NodeStanding, Registry};

/// Every machine of the cell and where it is dialed, in id order: every
/// node of `registry` not retired, at the address it registered (#349)
/// when it registered one, else at the address `known` holds for it; a
/// machine of `known` the fold does not hold yet keeps its known address.
/// A node that never registered and `known` does not name is dialed
/// nowhere, so it is left out.
///
/// # Panics
///
/// Never: the assertions check that the book is sorted, names each
/// machine once, and keeps every known machine not retired.
#[must_use]
pub fn cell_book(known: &[(NodeId, Address)], registry: &Registry) -> Vec<(NodeId, Address)> {
    let known_addr = |id: NodeId| known.iter().find(|(k, _)| *k == id).map(|(_, a)| a.clone());
    let mut book: Vec<(NodeId, Address)> = registry
        .nodes()
        .filter(|(_, node)| node.standing != NodeStanding::Retired)
        .filter_map(|(id, node)| {
            let registered = (node.registrations > 0)
                .then(|| Address::parse(&node.addr).ok())
                .flatten();
            registered.or_else(|| known_addr(id)).map(|addr| (id, addr))
        })
        .collect();
    book.extend(
        known
            .iter()
            .filter(|(id, _)| registry.get(*id).is_none())
            .cloned(),
    );
    book.sort_by_key(|(id, _)| *id);
    assert!(
        book.windows(2).all(|w| w[0].0 < w[1].0),
        "the book names each machine once, in id order"
    );
    assert!(
        known
            .iter()
            .filter(|(id, _)| registry
                .get(*id)
                .is_none_or(|n| n.standing != NodeStanding::Retired))
            .all(|(id, _)| book.iter().any(|(b, _)| b == id)),
        "the book keeps every known machine not retired"
    );
    book
}

/// The machines of `book` that `registry` does not hold retired: what a
/// machine caches of its cell (#858 (boot from the cached registry)). A
/// removed initial member is gone like any machine.
///
/// # Panics
///
/// Never: the assertion checks that the filter adds no machine.
#[must_use]
pub fn unretired(book: &[(NodeId, Address)], registry: &Registry) -> Vec<(NodeId, Address)> {
    let kept: Vec<(NodeId, Address)> = book
        .iter()
        .filter(|(id, _)| {
            registry
                .get(*id)
                .is_none_or(|n| n.standing != NodeStanding::Retired)
        })
        .cloned()
        .collect();
    assert!(
        kept.iter().all(|m| book.contains(m)),
        "a filter adds no machine"
    );
    kept
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
    let hosts: Vec<(u64, Address)> = endpoint
        .iter()
        .map(|(id, own)| {
            let registered = registry
                .address(NodeId(*id))
                .and_then(|addr| Address::parse(addr).ok());
            (*id, registered.unwrap_or_else(|| own.clone()))
        })
        .collect();
    assert!(
        hosts.iter().map(|h| h.0).eq(endpoint.iter().map(|h| h.0)),
        "the hosts keep the endpoint's ids and order"
    );
    hosts
}

/// The machines that hold one of the control journal `journal`'s acceptor
/// slots (#685 (special tenants are tenants)), sorted: every member in
/// force holds one (`paros_coordinator::tenant::controls`). Empty before
/// the coordinator books the machines `init` formed the journal on.
///
/// # Panics
///
/// Never: the assertion checks that the list is sorted.
#[must_use]
pub fn control_members(
    registry: &Registry,
    journal: paros_core::journal::JournalIdentifier,
) -> Vec<NodeId> {
    let target = crate::registry::BookingTarget::Journal(journal);
    let mut members: Vec<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.role == crate::registry::Role::Acceptor && b.target == target)
        .map(|(_, b)| b.node)
        .collect();
    members.sort_unstable();
    members.dedup();
    assert!(
        members.windows(2).all(|w| w[0] < w[1]),
        "a control journal's members are sorted"
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

/// [`with_own`], and machine `me` last at `own` when `members` does not
/// name it: a machine dials itself even when its starting book holds only
/// the plan's members. An admitted machine with no cached registry fold
/// may host the cell's journals once a drain moved them, and the plan's
/// members may all be gone (CI shard 2 on PR #934 (convergence), seed
/// 2398894731891847411: the one initial member was drained, removed and
/// shut down, and the cell views of the three other machines dialed only
/// it, so the cell answered no view for the rest of the run).
///
/// # Panics
///
/// If an assertion on its own invariants fails: a programmer error, never
/// an operating condition.
#[must_use]
pub fn with_self(
    members: &[(NodeId, Address)],
    me: NodeId,
    own: &Address,
) -> Vec<(NodeId, Address)> {
    let mut book = with_own(members, me, own);
    if !book.iter().any(|(id, _)| *id == me) {
        book.push((me, own.clone()));
    }
    assert!(
        book.len() >= members.len(),
        "a book keeps every machine it was given"
    );
    assert_eq!(
        book.iter().filter(|(id, _)| *id == me).count(),
        1,
        "a machine dials itself once"
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
    fn a_control_journal_runs_on_its_booked_acceptors() {
        let control = JournalIdentifier::new(TenantId(50), JournalId(51));
        let other = JournalIdentifier::new(TenantId(50), JournalId(52));
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        for id in [1, 3, 4, 5] {
            let seq = registry.next_seq();
            register(&mut registry, seq, id, &format!("m{id}.paros:1"));
        }
        // No member before any booking.
        assert!(control_members(&registry, control).is_empty());
        book(
            &mut registry,
            10,
            1,
            Role::Acceptor,
            BookingTarget::Journal(control),
        );
        assert_eq!(control_members(&registry, control), [NodeId(1)]);
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
        assert_eq!(control_members(&registry, control), [NodeId(1)]);
        // Every acceptor slot of the journal is a member, in id order.
        book(
            &mut registry,
            13,
            3,
            Role::Acceptor,
            BookingTarget::Journal(control),
        );
        assert_eq!(control_members(&registry, control), [NodeId(1), NodeId(3)]);
        // Released, it leaves.
        let seq = registry.next_seq();
        registry.fold(
            seq,
            &SystemCommand::ReleaseCapacity { booking: 13 }.encode(),
        );
        assert_eq!(control_members(&registry, control), [NodeId(1)]);
    }

    #[test]
    fn the_cell_book_is_the_registry_at_registered_addresses_else_known() {
        let plan: Vec<(NodeId, Address)> = vec![
            (NodeId(2), "b.paros:1".parse().expect("an address")),
            (NodeId(1), "a.paros:1".parse().expect("an address")),
        ];
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        // Nothing registered: the known addresses, in id order.
        assert_eq!(
            cell_book(&plan, &registry),
            vec![plan[1].clone(), plan[0].clone()]
        );
        // A registered address replaces the known one (#349); a machine
        // outside the seed joins the book at its registration.
        register(&mut registry, 0, 2, "b2.paros:1");
        register(&mut registry, 1, 7, "c.paros:1");
        let book = cell_book(&plan, &registry);
        assert_eq!(book.len(), 3);
        assert_eq!(book[0], plan[1]);
        assert_eq!(book[1].1.to_string(), "b2.paros:1");
        assert_eq!(book[2].0, NodeId(7));
        // A known machine the fold does not hold keeps its known address;
        // a seeded one that never registered and is not known is dialed
        // nowhere.
        let known: Vec<(NodeId, Address)> = vec![(NodeId(9), "i.paros:1".parse().unwrap())];
        let ids: Vec<NodeId> = cell_book(&known, &registry)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, [NodeId(2), NodeId(7), NodeId(9)]);
        assert!(cell_book(&[], &Registry::default()).is_empty());
    }

    #[test]
    fn a_retired_initial_member_leaves_the_book() {
        let plan: Vec<(NodeId, Address)> = vec![
            (NodeId(1), "a.paros:1".parse().expect("an address")),
            (NodeId(2), "b.paros:1".parse().expect("an address")),
        ];
        let mut registry = Registry::new([NodeId(1), NodeId(2)]);
        register(&mut registry, 0, 1, "a.paros:1");
        register(&mut registry, 1, 7, "c.paros:1");
        let ids = |registry: &Registry| -> Vec<NodeId> {
            unretired(&cell_book(&plan, registry), registry)
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        };
        assert_eq!(ids(&registry), [NodeId(1), NodeId(2), NodeId(7)]);
        // Draining, machine 1 is still dialed.
        registry.fold(2, &SystemCommand::DrainNode { id: NodeId(1) }.encode());
        assert_eq!(ids(&registry), [NodeId(1), NodeId(2), NodeId(7)]);
        // Retired, it is gone like any machine, from the book too; the
        // initial member that never registered stays at the plan's address.
        registry.fold(3, &SystemCommand::RetireNode { id: NodeId(1) }.encode());
        assert_eq!(ids(&registry), [NodeId(2), NodeId(7)]);
        let book = cell_book(&plan, &registry);
        assert_eq!(book.len(), 2, "a retired machine leaves the book");
        assert_eq!(book[0], plan[1]);
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

    #[test]
    fn a_member_dials_itself_where_it_is_and_the_others_where_the_book_says() {
        let addr = |text: &str| Address::parse(text).unwrap();
        let members = vec![
            (NodeId(1), addr("10.0.0.1:4500")),
            (NodeId(2), addr("10.0.0.2:4500")),
        ];
        let own = addr("machine-2.paros:4500");
        assert_eq!(
            with_own(&members, NodeId(2), &own),
            vec![(NodeId(1), addr("10.0.0.1:4500")), (NodeId(2), own.clone())]
        );
        assert_eq!(
            with_own(&members, NodeId(3), &own),
            members,
            "a machine outside the book changes nothing"
        );
    }

    #[test]
    fn a_machine_outside_the_book_dials_itself_last() {
        let addr = |text: &str| Address::parse(text).unwrap();
        let members = vec![(NodeId(1), addr("10.0.0.1:4500"))];
        let own = addr("machine-3.paros:4500");
        assert_eq!(
            with_self(&members, NodeId(3), &own),
            vec![(NodeId(1), addr("10.0.0.1:4500")), (NodeId(3), own.clone())]
        );
        assert_eq!(
            with_self(&members, NodeId(1), &own),
            vec![(NodeId(1), own.clone())],
            "a member is not added twice"
        );
        assert_eq!(with_self(&[], NodeId(3), &own), vec![(NodeId(3), own)]);
    }
}
