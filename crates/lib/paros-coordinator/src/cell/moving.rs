//! **The cell's side of a tenant move** (#782-#785, §3.7): the cell
//! coordinator writes the source's release and moved record, the target's
//! adoption, and on both sides the machines of the other cell the tenant
//! uses, into its cell control journal.
//!
//! The universe directory is the work order, as for a creation (#225): the
//! operator writes `MoveTenant`, and each cell coordinator reads the entry
//! at every watch round ([`follow`]). The source reads the target's
//! registry, and the target the source's, through the machines the
//! directory names for that cell (#232 (multiple cells)). No coordinator
//! claims the other cell's journals: it only reads them.
//!
//! One round writes, for each tenant whose entry names a move:
//!
//! - **The source** (the entry's cell): `ReleaseTenant`, with the tenant's
//!   birth founders. From then on no step of its tenant coordinator acts
//!   on the tenant. Then `MovePeers`: the target's machines booked for the
//!   tenant.
//! - **The target** (the entry's `moving_to`), once the source released the
//!   tenant to it: the tenant's footprint, then `AdoptTenant`. Then
//!   `MovePeers`: the source's machines booked for the tenant, and its
//!   birth founders. The target's tenant coordinator then moves each
//!   journal ([`crate::tenant::arrivals`]).
//! - **The source, after the flip** (the entry names the target and no
//!   move): `MovedTenant`. The watch then releases the tenant's slots.
//!
//! A move whose tenant was born in a third cell reaches that cell's
//! founding members through neither cell's peers: the sim moves a tenant
//! between two cells only. The gap is recorded in §3.7.

use std::collections::{BTreeMap, BTreeSet};

use paros_client::Client;
use paros_client::cell::CellSession;
use paros_client::checkpoint::{Folder, LoadOutcome, load};
use paros_client::fleet::Interrupted;
use paros_core::NodeId;
use paros_core::journal::{JournalIdentifier, TenantId};
use paros_model::address::Address;
use paros_model::fleet::{FleetDirectory, TenantKind, TenantState};
use paros_model::registry::moves::MovePeer;
use paros_model::registry::{HostedTenant, Registry, SystemCommand};
use paros_net::runtime::ParosProviders;

/// The other cells' registries a coordinator term read for its moves, by
/// cell id.
pub(crate) type Others = BTreeMap<u64, Folder<Registry>>;

/// The other cells' registries one round read, with the machines each was
/// read through, by cell id.
pub(crate) type Read<'a> = BTreeMap<u64, (&'a Registry, &'a [(u64, Address)])>;

/// Where cell `cell_id` is read: its control journal, and the machines
/// that serve it. The directory's endpoint, or, for the cell that hosts the
/// universe (whose entry names none), the universe pointer `registry`
/// recorded. `None` when the directory names no way to it.
pub(crate) fn reach(
    directory: &FleetDirectory,
    registry: &Registry,
    cell_id: u64,
) -> Option<(JournalIdentifier, Vec<(u64, Address)>)> {
    let entry = directory.cell(cell_id)?;
    let control = directory.tenant(entry.control_tenant)?.control;
    let journal = JournalIdentifier::new(entry.control_tenant, control);
    let mut endpoint = directory.endpoint(cell_id).to_vec();
    if endpoint.is_empty()
        && directory.universe_cell() == Some(cell_id)
        && let Some(pointer) = registry.universe()
    {
        endpoint.clone_from(&pointer.hosts);
    }
    if endpoint.is_empty() || !journal.is_set() {
        return None;
    }
    Some((journal, endpoint))
}

/// The cells this cell must read for the moves the directory holds: the
/// other side of each move it takes part in.
fn sides(directory: &FleetDirectory, own: u64) -> BTreeSet<u64> {
    let mut cells = BTreeSet::new();
    for (_, entry) in directory.tenants() {
        let Some(to) = entry.moving_to else { continue };
        if entry.cell_id == own {
            cells.insert(to);
        } else if to == own {
            cells.insert(entry.cell_id);
        }
    }
    assert!(!cells.contains(&own), "a move's other side is another cell");
    cells
}

/// The machines of another cell, read from its `registry` and dialed at
/// `endpoint` when it registered no address, that `tenant` uses: each one
/// with a booking of the tenant, and `extra`. In id order.
fn peers_of(
    registry: &Registry,
    endpoint: &[(u64, Address)],
    tenant: TenantId,
    extra: &[NodeId],
) -> Vec<MovePeer> {
    let mut nodes: BTreeSet<NodeId> = registry
        .bookings()
        .filter(|(_, b)| b.target.tenant() == tenant)
        .map(|(_, b)| b.node)
        .collect();
    nodes.extend(extra.iter().copied());
    let peers: Vec<MovePeer> = nodes
        .into_iter()
        .filter_map(|id| match registry.get(id) {
            Some(node) => Some(MovePeer {
                id,
                addr: node.addr.clone(),
                machine_interface: node.machine_interface.clone(),
            }),
            None => endpoint
                .iter()
                .find(|(e, _)| *e == id.0)
                .map(|(_, addr)| MovePeer {
                    id,
                    addr: addr.to_string(),
                    machine_interface: Vec::new(),
                }),
        })
        .collect();
    assert!(
        peers.windows(2).all(|w| w[0].id < w[1].id),
        "move peers are in id order"
    );
    peers
}

/// The cell steps of every move the directory holds for cell `own`, from
/// its `registry`, the founding members `founders` and the other cells'
/// registries `others` with their endpoints. At most one step per tenant:
/// each round writes it, and the next round the one after.
pub(crate) fn steps(
    directory: &FleetDirectory,
    registry: &Registry,
    (own, founders): (u64, &[NodeId]),
    others: &Read<'_>,
) -> Vec<SystemCommand> {
    let Some(fleet_id) = directory.fleet() else {
        return Vec::new();
    };
    if registry
        .fleet()
        .is_none_or(|f| (f.fleet_id, f.cell_id) != (fleet_id, own))
    {
        return Vec::new();
    }
    let mut steps = Vec::new();
    for (tenant, entry) in directory.tenants() {
        if entry.kind != TenantKind::Users || registry.dropped(tenant) {
            continue;
        }
        let step = match entry.moving_to {
            // The source, while the move is open.
            Some(to) if entry.cell_id == own && registry.hosts(tenant) => {
                if registry.leaving(tenant) == Some(to) {
                    others.get(&to).and_then(|(target, endpoint)| {
                        let peers = peers_of(target, endpoint, tenant, &[]);
                        changed(registry, tenant, to, peers)
                    })
                } else if registry.leaving(tenant).is_none() && !registry.moving(tenant) {
                    let born = registry
                        .born(tenant)
                        .map_or_else(|| founders.to_vec(), <[NodeId]>::to_vec);
                    Some(SystemCommand::ReleaseTenant {
                        fleet_id,
                        tenant,
                        to,
                        born,
                    })
                } else {
                    None
                }
            }
            // The target, once the source released the tenant to it.
            Some(to) if to == own && entry.state != TenantState::Removing => {
                let from = entry.cell_id;
                others.get(&from).and_then(|(source, endpoint)| {
                    if source.leaving(tenant) != Some(own) {
                        return None;
                    }
                    let born = source.born(tenant)?.to_vec();
                    if registry.hosts(tenant) {
                        let peers = peers_of(source, endpoint, tenant, &born);
                        return changed(registry, tenant, from, peers);
                    }
                    match registry.footprint(tenant) {
                        Some(grant) if grant.granted() => Some(SystemCommand::AdoptTenant {
                            fleet_id,
                            tenant,
                            hosted: HostedTenant {
                                control: entry.control,
                                name: entry.name.clone(),
                                survives: entry.survives,
                            },
                            from,
                            born,
                        }),
                        // The operator reads the refused footprint.
                        Some(_) => None,
                        None => crate::universe::footprint_grant(registry, tenant),
                    }
                })
            }
            // The source, after the flip: it forgets the tenant.
            None if entry.cell_id != own && registry.leaving(tenant) == Some(entry.cell_id) => {
                Some(SystemCommand::MovedTenant {
                    fleet_id,
                    tenant,
                    to: entry.cell_id,
                })
            }
            _ => None,
        };
        steps.extend(step);
    }
    steps
}

/// The `MovePeers` that names `peers` of cell `cell_id` for `tenant`, when
/// `registry` names others. `None` when it names them already, or there
/// are none yet.
fn changed(
    registry: &Registry,
    tenant: TenantId,
    cell_id: u64,
    peers: Vec<MovePeer>,
) -> Option<SystemCommand> {
    let held = registry
        .move_peers(tenant)
        .filter(|p| p.cell_id == cell_id)
        .map(|p| p.peers.as_slice());
    if peers.is_empty() || held == Some(peers.as_slice()) {
        return None;
    }
    Some(SystemCommand::MovePeers {
        tenant,
        cell_id,
        peers,
    })
}

/// Follow the moves the universe `directory` holds through the term's
/// `session`: read the other side of each, then write the cell's steps
/// ([`steps`]). How many entries it wrote. A cell that cannot be read
/// writes nothing for its moves this round.
///
/// # Errors
///
/// A write did not land: the term writes nothing more.
pub(crate) async fn follow<P: ParosProviders>(
    client: &Client<P>,
    session: &mut CellSession,
    directory: &FleetDirectory,
    others: &mut Others,
) -> Result<usize, Interrupted> {
    let own = session.journals().cell_id;
    let cells = sides(directory, own);
    if cells.is_empty() {
        return Ok(0);
    }
    let mut endpoints = BTreeMap::new();
    for cell in cells {
        let Some((journal, endpoint)) = reach(directory, session.registry(), cell) else {
            continue;
        };
        let reader = client.elsewhere(&endpoint);
        let fold = others.entry(cell).or_insert_with(|| {
            Folder::new(Registry::new(endpoint.iter().map(|(id, _)| NodeId(*id))))
        });
        if matches!(
            load(fold, journal, &reader, 0, 0).await,
            LoadOutcome::Loaded { .. }
        ) {
            endpoints.insert(cell, endpoint);
        }
    }
    let read: Read<'_> = endpoints
        .iter()
        .filter_map(|(cell, endpoint)| {
            others
                .get(cell)
                .map(|f| (*cell, (f.state(), endpoint.as_slice())))
        })
        .collect();
    let founders = session.members().to_vec();
    let steps = steps(directory, session.registry(), (own, &founders), &read);
    let mut wrote = 0;
    for step in steps {
        session.record(client, 0, &step).await?;
        reached(&step);
        wrote += 1;
    }
    Ok(wrote)
}

/// The gate each written step fires.
fn reached(step: &SystemCommand) {
    match step {
        SystemCommand::ReleaseTenant { .. } => {
            moonpool_assertions::reachable!("move: the source releases a tenant");
        }
        SystemCommand::AdoptTenant { .. } => {
            moonpool_assertions::reachable!("move: the target adopts a tenant");
        }
        SystemCommand::MovePeers { .. } => {
            moonpool_assertions::reachable!("move: a cell names the other cell's machines");
        }
        SystemCommand::MovedTenant { .. } => {
            moonpool_assertions::reachable!("move: the source forgets a tenant that moved");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
