//! What a cell records of a **tenant move between cells** (#782-#785, §3.7):
//! the source's release, the target's adoption and settlement, the machines
//! of the other cell the tenant uses, and the source's moved record.
//!
//! The order of a move (decided by Pierre on 2026-11-01, #784 (coordinator
//! handover), option A):
//!
//! 1. The operator writes `MoveTenant` to the universe directory.
//! 2. The source coordinator stops every step for the tenant, then writes
//!    [`SystemCommand::ReleaseTenant`]: its machines still serve the
//!    tenant's journals, and its coordinator writes nothing more for it.
//! 3. The target coordinator sees the release and writes
//!    [`SystemCommand::AdoptTenant`], then claims the tenant control
//!    journal. That claim is the fence: only the target writes for the
//!    tenant from then on. It moves each journal onto its own machines, and
//!    writes [`SystemCommand::SettleTenant`] when the last one is there.
//! 4. The operator flips the directory entry. The source writes
//!    [`SystemCommand::MovedTenant`] and releases its bookings.
//!
//! While a move is open, each cell names the machines of the other cell
//! that the tenant uses ([`SystemCommand::MovePeers`], #782 (peers of
//! another cell)). Its machines open peer lanes to them, and take their peer
//! batches for that tenant's journals only (#783 (cross the cell fence)).
//!
//! A moved record is not a tombstone (#785 (moved record)): a later move may
//! bring the tenant back, and a removal still writes `DropTenant`.

use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_wire::system as wire;

use crate::registry::Registry;
use crate::registry::event::{RegistryEvent, RegistryRefusal};
use crate::registry::universe::HostedTenant;

/// A machine of another cell that a moving tenant uses (#782 (peers of
/// another cell)): its id, the address its peers dial, and its machine
/// interface (#404), opaque here.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MovePeer {
    /// The machine's id.
    pub id: NodeId,
    /// The address its peers dial.
    pub addr: String,
    /// Its machine interface (#404): empty when it serves none.
    pub machine_interface: Vec<u8>,
}

/// The machines of cell `cell_id` that a moving tenant uses (#782).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MovePeers {
    /// The other cell.
    pub cell_id: u64,
    /// Its machines the tenant uses, in id order.
    pub peers: Vec<MovePeer>,
}

/// What a cell records of a tenant that takes part in a move (#784).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TenantMove {
    /// The cell the tenant was released to: this cell is the source.
    pub(crate) leaving: Option<u64>,
    /// The cell the tenant arrives from: this cell is the target, and the
    /// move did not settle yet.
    pub(crate) arriving: Option<u64>,
    /// The founding members of the cell the tenant was born in (empty: this
    /// cell's own). They are the bootstrap members of its control journal
    /// and the bootstrap matchmaker set of its journals, and stay so for the
    /// tenant's life.
    pub(crate) born: Vec<NodeId>,
    /// The machines of the other cell the tenant uses.
    pub(crate) peers: Option<MovePeers>,
}

impl TenantMove {
    /// Whether the record holds nothing: the tenant takes part in no move.
    fn is_empty(&self) -> bool {
        self.leaving.is_none()
            && self.arriving.is_none()
            && self.born.is_empty()
            && self.peers.is_none()
    }
}

impl MovePeer {
    pub(crate) fn to_wire(&self) -> wire::MovePeer {
        wire::MovePeer {
            id: self.id.0,
            addr: self.addr.clone(),
            machine_interface: self.machine_interface.clone(),
        }
    }

    pub(crate) fn from_wire(peer: wire::MovePeer) -> Result<Self, &'static str> {
        if peer.id == 0 || peer.addr.is_empty() {
            return Err("a move peer names no machine or no address");
        }
        Ok(Self {
            id: NodeId(peer.id),
            addr: peer.addr,
            machine_interface: peer.machine_interface,
        })
    }
}

/// `peers` read back, in id order, each once.
pub(crate) fn peers_from_wire(peers: Vec<wire::MovePeer>) -> Result<Vec<MovePeer>, &'static str> {
    let mut peers = peers
        .into_iter()
        .map(MovePeer::from_wire)
        .collect::<Result<Vec<_>, _>>()?;
    peers.sort();
    let before = peers.len();
    peers.dedup_by_key(|p| p.id);
    if peers.len() != before {
        return Err("a move names one peer twice");
    }
    Ok(peers)
}

/// The open moves of a registry, and its moved tenants.
pub(crate) type Moves = (
    std::collections::BTreeMap<TenantId, TenantMove>,
    std::collections::BTreeMap<TenantId, u64>,
);

/// The tenant moves of a registry checkpoint, and its moved tenants.
pub(crate) fn moves_from_wire(
    states: Vec<wire::TenantMoveState>,
    gone: Vec<wire::MovedTenant>,
) -> Result<Moves, &'static str> {
    let mut open = std::collections::BTreeMap::new();
    for m in states {
        if m.tenant == 0 {
            return Err("a registry state moves an unset tenant");
        }
        let peers = peers_from_wire(m.peers)?;
        let peers = match (m.peers_cell, peers.is_empty()) {
            (0, true) => None,
            (0, false) | (_, true) => return Err("a registry state names half a move's peers"),
            (cell_id, false) => Some(MovePeers { cell_id, peers }),
        };
        let record = TenantMove {
            leaving: (m.leaving != 0).then_some(m.leaving),
            arriving: (m.arriving != 0).then_some(m.arriving),
            born: m.born.into_iter().map(NodeId).collect(),
            peers,
        };
        if record.is_empty() || (record.leaving.is_some() && record.arriving.is_some()) {
            return Err("a registry state holds a move it cannot");
        }
        open.insert(TenantId(m.tenant), record);
    }
    let mut out = std::collections::BTreeMap::new();
    for m in gone {
        if m.tenant == 0 || m.to == 0 {
            return Err("a registry state names an unset moved tenant");
        }
        out.insert(TenantId(m.tenant), m.to);
    }
    Ok((open, out))
}

/// The tenant moves of `registry` on the wire, in tenant id order.
pub(crate) fn moves_to_wire(
    registry: &Registry,
) -> (Vec<wire::TenantMoveState>, Vec<wire::MovedTenant>) {
    let open = registry
        .moves
        .iter()
        .map(|(tenant, m)| wire::TenantMoveState {
            tenant: tenant.0,
            leaving: m.leaving.unwrap_or(0),
            arriving: m.arriving.unwrap_or(0),
            born: m.born.iter().map(|n| n.0).collect(),
            peers_cell: m.peers.as_ref().map_or(0, |p| p.cell_id),
            peers: m
                .peers
                .as_ref()
                .map(|p| p.peers.iter().map(MovePeer::to_wire).collect())
                .unwrap_or_default(),
        })
        .collect();
    let gone = registry
        .moved
        .iter()
        .map(|(tenant, to)| wire::MovedTenant {
            fleet_id: 0,
            tenant: tenant.0,
            to: *to,
        })
        .collect();
    (open, gone)
}

impl Registry {
    /// The fleet check every move record passes: the cell joined fleet
    /// `fleet_id`, and the tenant is set.
    fn move_fleet(&self, fleet_id: u64, tenant: TenantId) -> Option<RegistryEvent> {
        if self.fleet.is_none_or(|fleet| fleet.fleet_id != fleet_id) {
            return Some(RegistryEvent::Refused(RegistryRefusal::OtherFleet {
                fleet_id,
            }));
        }
        if !tenant.is_set() {
            return Some(RegistryEvent::Refused(RegistryRefusal::Malformed));
        }
        None
    }

    /// The source releases `tenant` to cell `to` (#784): a tenant it hosts,
    /// that does not arrive here, to another cell.
    pub(crate) fn release_tenant(
        &mut self,
        fleet_id: u64,
        tenant: TenantId,
        (to, born): (u64, Vec<NodeId>),
    ) -> RegistryEvent {
        if let Some(refused) = self.move_fleet(fleet_id, tenant) {
            return refused;
        }
        if to == 0 || born.is_empty() || self.fleet.is_some_and(|f| f.cell_id == to) {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if !self.hosted.contains_key(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::UnknownTenant { tenant });
        }
        let held = self.moves.get(&tenant).map(|m| (m.leaving, m.arriving));
        match held {
            Some((Some(held), _)) if held == to => RegistryEvent::Unchanged,
            Some((Some(_), _) | (None, Some(_))) => {
                RegistryEvent::Refused(RegistryRefusal::MoveOpen { tenant })
            }
            Some((None, None)) | None => {
                let record = self.moves.entry(tenant).or_default();
                record.leaving = Some(to);
                // The target reads the tenant's birth cell here: this
                // cell's founding members, for a tenant born here.
                if record.born.is_empty() {
                    record.born = born;
                }
                RegistryEvent::TenantReleased { tenant, to }
            }
        }
    }

    /// The target adopts `tenant` from cell `from` (#784): it hosts it, its
    /// journals still on `from`'s machines, born over `born`. Like
    /// `HostTenant`: never a dropped tenant, and only once its footprint is
    /// booked (#476). A tenant that moved out of this cell before comes back
    /// (#785): its moved record is forgotten.
    pub(crate) fn adopt_tenant(
        &mut self,
        fleet_id: u64,
        tenant: TenantId,
        hosted: HostedTenant,
        (from, born): (u64, Vec<NodeId>),
    ) -> RegistryEvent {
        if let Some(refused) = self.move_fleet(fleet_id, tenant) {
            return refused;
        }
        if !hosted.control.is_set()
            || from == 0
            || born.is_empty()
            || self.fleet.is_some_and(|f| f.cell_id == from)
        {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if self.dropped.contains(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::TenantDropped { tenant });
        }
        if self.hosted.contains_key(&tenant) {
            return RegistryEvent::Unchanged;
        }
        if !self.footprint(tenant).is_some_and(super::Grant::granted) {
            return RegistryEvent::Refused(RegistryRefusal::NoFootprint { tenant });
        }
        let control = hosted.control;
        self.hosted.insert(tenant, hosted);
        self.moved.remove(&tenant);
        // The tenant's birth cell: its own founders when it was born here.
        let born = if born.iter().all(|n| self.genesis.contains(n)) {
            Vec::new()
        } else {
            born
        };
        let record = self.moves.entry(tenant).or_default();
        record.leaving = None;
        record.arriving = Some(from);
        record.born = born;
        RegistryEvent::TenantAdopted {
            tenant,
            control,
            from,
        }
    }

    /// The target's move of `tenant` ended (#784): its journals run on this
    /// cell's machines.
    pub(crate) fn settle_tenant(&mut self, fleet_id: u64, tenant: TenantId) -> RegistryEvent {
        if let Some(refused) = self.move_fleet(fleet_id, tenant) {
            return refused;
        }
        if !self.hosted.contains_key(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::UnknownTenant { tenant });
        }
        let Some(record) = self.moves.get_mut(&tenant) else {
            return RegistryEvent::Unchanged;
        };
        if record.arriving.take().is_none() {
            return RegistryEvent::Unchanged;
        }
        let from = record.peers.as_ref().map_or(0, |p| p.cell_id);
        if record.is_empty() {
            self.moves.remove(&tenant);
        }
        RegistryEvent::TenantSettled { tenant, from }
    }

    /// The machines of cell `cell_id` that `tenant` uses (#782): a tenant
    /// this cell hosts, of another cell. An empty list forgets them.
    pub(crate) fn set_move_peers(
        &mut self,
        tenant: TenantId,
        cell_id: u64,
        peers: Vec<MovePeer>,
    ) -> RegistryEvent {
        if !tenant.is_set() || cell_id == 0 || self.fleet.is_none_or(|f| f.cell_id == cell_id) {
            return RegistryEvent::Refused(RegistryRefusal::Malformed);
        }
        if !self.hosted.contains_key(&tenant) {
            return RegistryEvent::Refused(RegistryRefusal::UnknownTenant { tenant });
        }
        let wanted = (!peers.is_empty()).then_some(MovePeers { cell_id, peers });
        let record = self.moves.entry(tenant).or_default();
        if record.peers == wanted {
            if record.is_empty() {
                self.moves.remove(&tenant);
            }
            return RegistryEvent::Unchanged;
        }
        record.peers = wanted;
        if record.is_empty() {
            self.moves.remove(&tenant);
        }
        RegistryEvent::MovePeersSet { tenant, cell_id }
    }

    /// The source forgets `tenant`, which moved to cell `to` (#785): it
    /// hosts it no more. Not a tombstone: a later adoption brings it back.
    pub(crate) fn moved_tenant(
        &mut self,
        fleet_id: u64,
        tenant: TenantId,
        to: u64,
    ) -> RegistryEvent {
        if let Some(refused) = self.move_fleet(fleet_id, tenant) {
            return refused;
        }
        if self.moved.get(&tenant) == Some(&to) && !self.hosted.contains_key(&tenant) {
            return RegistryEvent::Unchanged;
        }
        if self.moves.get(&tenant).and_then(|m| m.leaving) != Some(to) {
            return RegistryEvent::Refused(RegistryRefusal::NotLeaving { tenant });
        }
        self.hosted.remove(&tenant);
        self.moves.remove(&tenant);
        self.moved.insert(tenant, to);
        RegistryEvent::TenantMovedOut { tenant, to }
    }

    /// The cell `tenant` was released to, while its move is open here as
    /// the source (#784): this cell's coordinator writes nothing for it.
    #[must_use]
    pub fn leaving(&self, tenant: TenantId) -> Option<u64> {
        self.moves.get(&tenant).and_then(|m| m.leaving)
    }

    /// The cell `tenant` arrives from, while its move into this cell has not
    /// settled (#784).
    #[must_use]
    pub fn arriving(&self, tenant: TenantId) -> Option<u64> {
        self.moves.get(&tenant).and_then(|m| m.arriving)
    }

    /// Whether `tenant` takes part in an open move here, as the source or
    /// the target (#784).
    #[must_use]
    pub fn moving(&self, tenant: TenantId) -> bool {
        self.leaving(tenant).is_some() || self.arriving(tenant).is_some()
    }

    /// The founding members of the cell `tenant` was born in, when it was
    /// born in another cell (#784): the bootstrap members of its control
    /// journal and the bootstrap matchmaker set of its journals. `None` for
    /// a tenant born here.
    #[must_use]
    pub fn born(&self, tenant: TenantId) -> Option<&[NodeId]> {
        self.moves
            .get(&tenant)
            .map(|m| m.born.as_slice())
            .filter(|born| !born.is_empty())
    }

    /// The machines of another cell that `tenant` uses (#782).
    #[must_use]
    pub fn move_peers(&self, tenant: TenantId) -> Option<&MovePeers> {
        self.moves.get(&tenant).and_then(|m| m.peers.as_ref())
    }

    /// Every tenant with machines of another cell, with them (#782).
    pub fn all_move_peers(&self) -> impl Iterator<Item = (TenantId, &MovePeers)> + '_ {
        self.moves
            .iter()
            .filter_map(|(t, m)| m.peers.as_ref().map(|p| (*t, p)))
    }

    /// The cell `tenant` moved to, once this cell forgot it (#785).
    #[must_use]
    pub fn moved(&self, tenant: TenantId) -> Option<u64> {
        self.moved.get(&tenant).copied()
    }

    /// Every tenant that moved out of this cell, with the cell it moved to
    /// (#785).
    pub fn moved_tenants(&self) -> impl Iterator<Item = (TenantId, u64)> + '_ {
        self.moved.iter().map(|(t, to)| (*t, *to))
    }
}
