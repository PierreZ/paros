//! The registry's checkpoint (#189): its fold to and from the wire.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{JournalId, NodeId, TenantId};
use paros_wire::system as wire;
use prost::Message as _;

use crate::checkpoint::Checkpointable;
use crate::machine::{incarnation_from_halves, parse_machine_tags};
use crate::registry::Registry;
use crate::registry::command::{Grant, GrantKey};
use crate::registry::event::RegistryEvent;
use crate::registry::records::{
    Booking, BookingTarget, Liveness, MaintenanceScope, MaintenanceWindow, NodeStanding,
    RegisteredNode,
};
use crate::registry::universe::{FleetRegistration, HostedTenant, pointer_from_wire};
use crate::tags::Tags;

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

impl Checkpointable for Registry {
    type Event = RegistryEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state =
            wire::RegistryState::decode(state).map_err(|_| "a registry state does not decode")?;
        let mut nodes = BTreeMap::new();
        for n in state.nodes {
            // A genesis node registers too (#349): its entry is the address
            // peers dial, and it crosses a checkpoint like any other (#211).
            let id = NodeId(n.id);
            let (class, locality, tags) = Tags::from_wire(n.tags)
                .and_then(parse_machine_tags)
                .map_err(|_| "a registry state node's tags are malformed or unknown")?;
            nodes.insert(
                id,
                RegisteredNode {
                    addr: n.addr,
                    locality,
                    name: n.name,
                    class,
                    capacity: n.capacity,
                    standing: NodeStanding::from_wire(n.standing)?,
                    registrations: n.registrations,
                    incarnation: incarnation_from_halves(n.incarnation_high, n.incarnation_low),
                    machine_interface: n.machine_interface,
                    admitted: n.admitted,
                    tags,
                },
            );
        }
        let spent: BTreeSet<u64> = state.spent.into_iter().collect();
        let mut bookings = BTreeMap::new();
        for b in state.bookings {
            if !spent.contains(&b.booking) {
                return Err("a registry state holds a booking whose id is not spent");
            }
            bookings.insert(
                b.booking,
                Booking {
                    node: NodeId(b.node),
                    role: b.role.parse()?,
                    target: BookingTarget::from_wire(b.tenant, b.journal, b.set, b.tenant_level)?,
                },
            );
        }
        let liveness = liveness_from_wire(state.liveness);
        let fleet = match (state.fleet_id, state.cell_id) {
            (0, 0) => None,
            (0, _) | (_, 0) => return Err("a registry state names half a fleet registration"),
            (fleet_id, cell_id) => Some(FleetRegistration {
                fleet_id,
                cell_id,
                version: state.version,
            }),
        };
        let universe =
            pointer_from_wire(state.directory_tenant, state.directory_journal, state.hosts)?;
        if universe.is_some() && fleet.is_none() {
            return Err("a registry state points at a universe it did not join");
        }
        let mut hosted = BTreeMap::new();
        for h in state.hosted {
            if h.control_journal == 0 {
                return Err("a registry state hosts a tenant with no control journal");
            }
            hosted.insert(
                TenantId(h.tenant),
                HostedTenant {
                    control: JournalId(h.control_journal),
                    name: h.name,
                    survives: crate::tenant::Survives::from_wire(h.survives)?,
                },
            );
        }
        let dropped: BTreeSet<TenantId> = state.dropped.into_iter().map(TenantId).collect();
        if hosted.keys().chain(&dropped).any(|t| !t.is_set())
            || hosted.keys().any(|t| dropped.contains(t))
            || (fleet.is_none() && !(hosted.is_empty() && dropped.is_empty()))
        {
            return Err("a registry state hosts a tenant it cannot");
        }
        let maintenance = maintenance_from_wire(state.maintenance, covers_up_to)?;
        let grants = grants_from_wire(state.grants, &spent)?;
        self.nodes = nodes;
        self.bookings = bookings;
        self.grants = grants;
        self.spent = spent;
        self.maintenance = maintenance;
        self.liveness = liveness;
        self.fleet = fleet;
        self.universe = universe;
        self.hosted = hosted;
        self.dropped = dropped;
        self.tagged = state.tagged.into_iter().collect();
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The liveness marks of a registry checkpoint: each machine's incarnation,
/// down or up, and slow or not (#361 (slow machines)).
pub(crate) fn liveness_from_wire(states: Vec<wire::LivenessState>) -> BTreeMap<NodeId, Liveness> {
    states
        .into_iter()
        .map(|l| {
            (
                NodeId(l.id),
                Liveness {
                    incarnation: incarnation_from_halves(l.incarnation_high, l.incarnation_low),
                    up: l.up,
                    slow: l.slow,
                    low_space: l.low_space,
                },
            )
        })
        .collect()
}

/// The open maintenance windows of a registry checkpoint (#359), each
/// opened at or below `covers_up_to`.
pub(crate) fn maintenance_from_wire(
    states: Vec<wire::MaintenanceState>,
    covers_up_to: u64,
) -> Result<BTreeMap<MaintenanceScope, MaintenanceWindow>, &'static str> {
    let mut maintenance = BTreeMap::new();
    for m in states {
        if m.for_ms == 0 || m.seq > covers_up_to {
            return Err("a registry state holds a maintenance window it cannot");
        }
        maintenance.insert(
            MaintenanceScope::from_wire(m.node, m.zone)?,
            MaintenanceWindow {
                for_ms: m.for_ms,
                seq: m.seq,
            },
        );
    }
    Ok(maintenance)
}

/// The grants of a registry checkpoint (#476): each under its key, its
/// booking ids spent.
pub(crate) fn grants_from_wire(
    states: Vec<wire::GrantState>,
    spent: &BTreeSet<u64>,
) -> Result<BTreeMap<GrantKey, Grant>, &'static str> {
    let mut grants = BTreeMap::new();
    for g in states {
        let key = match (g.footprint, g.request) {
            (true, 0) => GrantKey::Footprint(TenantId(g.tenant)),
            (false, request) if request != 0 => GrantKey::Request(request),
            _ => return Err("a registry state names a grant by no key"),
        };
        if g.refused && !g.bookings.is_empty() {
            return Err("a registry state holds a refused grant that booked");
        }
        if g.bookings.iter().any(|b| !spent.contains(b)) {
            return Err("a registry state holds a grant whose booking id is not spent");
        }
        grants.insert(
            key,
            Grant {
                tenant: TenantId(g.tenant),
                bookings: (!g.refused).then_some(g.bookings),
            },
        );
    }
    Ok(grants)
}
