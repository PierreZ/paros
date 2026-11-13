//! The **cell fence** (#216, #539 (matchmaker cell fence)), opened narrowly
//! for a tenant move between cells (#783 (cross the cell fence)).
//!
//! A machine steps no peer batch of another cell, and its matchmaker host
//! answers no request of another cell: a machine at an address another cell
//! left, or a bad configuration, never reaches it. A tenant move needs both
//! cells' machines to talk for that tenant's journals and matchmaker set, so
//! the fence also admits a message of cell `C` for tenant `T` while the
//! cell's registry names machines of `C` that `T` uses (`MovePeers`, #782
//! (peers of another cell)). Every other tenant stays fenced.
//!
//! A tenant that moved out of the cell (#785 (moved record)) keeps one
//! opening, for its matchmaker set only: every journal of it boots on its
//! birth set and walks the successor chain (#784), so the frozen
//! generations it left here answer the cells it lives in now. A matchmaker
//! never provisions a set for another cell's request (the hazard #539
//! closed): it only opens one it holds or one the registry books here.
//!
//! The node loop writes the fence after each registry fold; the edge tasks
//! and the matchmaker host read it. Reads draw no randomness.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, PoisonError, RwLock};

use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_model::registry::Registry;

/// What a cell admits from another cell (#783).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Openings {
    /// Each other cell, with the tenants whose messages of it are admitted.
    peers: BTreeMap<u64, BTreeSet<TenantId>>,
    /// The tenants that moved out: their matchmaker sets answer any cell.
    moved: BTreeSet<TenantId>,
    /// The tenants born in another cell, with that cell's initial members
    /// (#784 (coordinator handover)): the bootstrap generation of their
    /// sets is those members', never this cell's initial members'.
    born_elsewhere: BTreeMap<TenantId, Vec<NodeId>>,
}

/// A machine's cell fence: its own cell, and the openings its registry fold
/// names (#783 (cross the cell fence)). Cheap to clone: the clones share one
/// state.
#[derive(Clone, Debug)]
pub struct CellFence {
    own: u64,
    open: Arc<RwLock<Openings>>,
}

impl CellFence {
    /// The fence of cell `own` (`0` on a deployment without a cell), with
    /// no opening.
    #[must_use]
    pub fn new(own: u64) -> Self {
        Self {
            own,
            open: Arc::default(),
        }
    }

    /// The cell this fence keeps.
    #[must_use]
    pub fn own(&self) -> u64 {
        self.own
    }

    /// Whether a peer message of cell `cell` for tenant `tenant` passes: of
    /// this cell, or of a cell whose machines `tenant` uses in an open move.
    #[must_use]
    pub fn admits(&self, cell: u64, tenant: TenantId) -> bool {
        if cell == self.own {
            return true;
        }
        let open = self.open.read().unwrap_or_else(PoisonError::into_inner);
        open.peers.get(&cell).is_some_and(|t| t.contains(&tenant))
    }

    /// Whether a matchmaker request of cell `cell` for tenant `tenant`
    /// passes: as a peer message does, or for a tenant that moved out of
    /// this cell (its frozen generations answer the cell it lives in now).
    #[must_use]
    pub fn admits_matchmaker(&self, cell: u64, tenant: TenantId) -> bool {
        if self.admits(cell, tenant) {
            return true;
        }
        let open = self.open.read().unwrap_or_else(PoisonError::into_inner);
        open.moved.contains(&tenant)
    }

    /// The initial members of the cell `tenant` was born in, when that is
    /// another cell, as the registry fold says (#784 (coordinator
    /// handover)): the bootstrap generation of its set. `None` for a tenant
    /// born here.
    #[must_use]
    pub fn born_elsewhere(&self, tenant: TenantId) -> Option<Vec<NodeId>> {
        let open = self.open.read().unwrap_or_else(PoisonError::into_inner);
        open.born_elsewhere.get(&tenant).cloned()
    }

    /// Open the fence as `registry` says: the machines of other cells each
    /// moving tenant uses, and the tenants that moved out. Whether it moved.
    ///
    /// # Panics
    ///
    /// If the registry names this fence's own cell as a move's other cell:
    /// the fold refuses it.
    pub fn follow(&self, registry: &Registry) -> bool {
        let mut wanted = Openings::default();
        for (tenant, peers) in registry.all_move_peers() {
            assert_ne!(peers.cell_id, self.own, "a move names another cell");
            wanted
                .peers
                .entry(peers.cell_id)
                .or_default()
                .insert(tenant);
        }
        wanted.moved = registry.moved_tenants().map(|(t, _)| t).collect();
        wanted.born_elsewhere = registry
            .all_born_elsewhere()
            .filter_map(|t| registry.born(t).map(|born| (t, born.to_vec())))
            .collect();
        let mut open = self.open.write().unwrap_or_else(PoisonError::into_inner);
        if *open == wanted {
            return false;
        }
        *open = wanted;
        true
    }
}

impl From<u64> for CellFence {
    fn from(own: u64) -> Self {
        Self::new(own)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_closed_fence_admits_only_its_own_cell() {
        let fence = CellFence::new(7);
        assert!(fence.admits(7, TenantId(1)));
        assert!(!fence.admits(8, TenantId(1)));
        assert!(!fence.admits_matchmaker(8, TenantId(1)));
        // Clones share the state.
        let clone = fence.clone();
        assert_eq!(clone.own(), 7);
        assert!(!clone.follow(&Registry::new([])));
    }

    /// A registry of cell `7` that hosts tenant `300`, released to cell
    /// `8`, with one machine of cell `8` named for it.
    fn moving() -> Registry {
        use paros_core::NodeId;
        use paros_core::journal::JournalId;
        use paros_model::registry::moves::MovePeer;
        use paros_model::registry::{
            BookingTarget, GrantKey, HostedTenant, Role, Slot, SystemCommand,
        };
        let tenant = TenantId(300);
        let mut registry = Registry::new([NodeId(1)]);
        let records = [
            SystemCommand::RegisterNode {
                id: NodeId(1),
                addr: "n1".into(),
                class: paros_model::registry::Class::Storage,
                locality: paros_model::machine::Locality::default(),
                name: "m1".into(),
                incarnation: 1,
                machine_interface: Vec::new(),
                disk: paros_model::machine::Disk::UNKNOWN,
            },
            SystemCommand::JoinFleet {
                fleet_id: 5,
                cell_id: 7,
                version: 1,
                universe: None,
            },
            SystemCommand::GrantCapacity {
                key: GrantKey::Footprint(tenant),
                tenant,
                slots: Some(vec![
                    Slot {
                        booking: 1,
                        node: NodeId(1),
                        role: Role::Acceptor,
                        target: BookingTarget::Tenant { tenant },
                    },
                    // The machine the tenant is born on (#685 (no founders)).
                    Slot {
                        booking: 2,
                        node: NodeId(1),
                        role: Role::Acceptor,
                        target: BookingTarget::Journal(
                            paros_core::journal::JournalIdentifier::new(tenant, JournalId(301)),
                        ),
                    },
                ]),
            },
            SystemCommand::HostTenant {
                fleet_id: 5,
                tenant,
                hosted: HostedTenant {
                    control: JournalId(301),
                    name: b"acme".to_vec(),
                    survives: paros_model::tenant::Survives::Az,
                    members: vec![NodeId(1)],
                },
            },
            SystemCommand::ReleaseTenant {
                fleet_id: 5,
                tenant,
                to: 8,
                born: vec![NodeId(1)],
            },
            SystemCommand::MovePeers {
                tenant,
                cell_id: 8,
                peers: vec![MovePeer {
                    id: NodeId(81),
                    addr: "10.0.8.1:4500".into(),
                    machine_interface: Vec::new(),
                }],
            },
        ];
        for (seq, command) in (0..).zip(records) {
            use paros_model::checkpoint::Checkpointable as _;
            registry.apply(seq, &command.encode());
        }
        assert_eq!(registry.leaving(tenant), Some(8));
        registry
    }

    #[test]
    fn a_move_opens_the_fence_for_its_tenant_and_other_cell_only() {
        let fence = CellFence::new(7);
        assert!(fence.follow(&moving()));
        assert!(!fence.follow(&moving()), "the same openings change nothing");
        assert!(fence.admits(8, TenantId(300)));
        assert!(fence.admits_matchmaker(8, TenantId(300)));
        // Born here: its sets boot on this cell's initial members.
        assert_eq!(fence.born_elsewhere(TenantId(300)), None);
        // Another tenant of that cell, or that tenant from a third cell,
        // stays fenced.
        assert!(!fence.admits(8, TenantId(301)));
        assert!(!fence.admits(9, TenantId(300)));
        // Closed again once the registry names no move.
        assert!(fence.follow(&Registry::new([])));
        assert!(!fence.admits(8, TenantId(300)));
    }

    #[test]
    fn a_moved_tenants_sets_answer_any_cell_but_its_journals_stay_fenced() {
        use paros_model::checkpoint::Checkpointable as _;
        use paros_model::registry::SystemCommand;
        let mut registry = moving();
        registry.apply(
            6,
            &SystemCommand::MovedTenant {
                fleet_id: 5,
                tenant: TenantId(300),
                to: 8,
            }
            .encode(),
        );
        let fence = CellFence::new(7);
        fence.follow(&registry);
        assert!(!fence.admits(8, TenantId(300)), "the move is over");
        assert!(fence.admits_matchmaker(8, TenantId(300)));
        assert!(fence.admits_matchmaker(9, TenantId(300)));
        assert!(!fence.admits_matchmaker(9, TenantId(301)));
    }
}
