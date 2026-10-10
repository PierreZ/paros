//! A cell's **address book** (#349, `docs/architecture.md` §3.2): where each
//! machine of the cell is dialed, read from the registry fold, never from the
//! plan `init` wrote.
//!
//! A founding member is in the registry's genesis pool by id. Its address
//! starts as the one the cell plan names, and its own `RegisterNode` (written
//! by the cell coordinator when the address it advertises changed, #349)
//! replaces it. Every other machine of the cell is in the book once it
//! registered. A retired machine is not in the book.

use paros_core::NodeId;

use crate::Address;
use crate::system::{NodeStanding, Registry};

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

/// `members` as machine `me` dials them: its own entry at the address it
/// advertises now (`own`), which the plan or the registry may not hold yet
/// (#349: a machine that moved reaches itself there).
pub(crate) fn with_own(
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
    use crate::system::{Class, SystemCommand};

    fn register(registry: &mut Registry, seq: u64, id: u64, addr: &str) {
        let record = SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: addr.into(),
            class: Class::Storage,
            capacity: 1,
            failure_domain: String::new(),
            name: format!("m{id}"),
            incarnation: 1,
        }
        .encode();
        registry.fold(seq, &record);
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
}
