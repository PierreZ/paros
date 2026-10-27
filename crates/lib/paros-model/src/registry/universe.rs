//! What the cell records of the universe (#232, §3.7): the tenants it hosts, the universe pointer and the fleet registration.

use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_wire::system as wire;

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

/// What the cell records of a tenant it hosts (#210): its control journal,
/// where every machine that serves the tenant learns its journals, and the
/// name and `survives` its first coordinator describes it with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedTenant {
    /// The tenant control journal's id, inside the tenant.
    pub control: JournalId,
    /// The tenant's name.
    pub name: Vec<u8>,
    /// What it survives (#252).
    pub survives: crate::tenant::Survives,
}

/// The **universe pointer** (#232 (multiple cells), §3.7): where a cell
/// that does not host the universe tenant reads the universe directory.
/// Recorded by its join; its readers dial `hosts`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniversePointer {
    /// The universe directory: the universe tenant's control journal.
    pub directory: JournalIdentifier,
    /// Machines of the cell that hosts it, by node id and address.
    pub hosts: Vec<(u64, crate::address::Address)>,
}

pub(crate) type PointerWire = (u64, u64, Vec<wire::UniverseHost>);

/// A `JoinFleet` on the wire, its universe pointer flattened.
pub(crate) fn join_to_wire(
    fleet_id: u64,
    cell_id: u64,
    version: u32,
    universe: Option<&UniversePointer>,
) -> wire::JoinFleet {
    let (directory_tenant, directory_journal, hosts) = pointer_to_wire(universe);
    wire::JoinFleet {
        fleet_id,
        cell_id,
        version,
        directory_tenant,
        directory_journal,
        hosts,
    }
}

pub(crate) fn pointer_to_wire(pointer: Option<&UniversePointer>) -> PointerWire {
    pointer.map_or((0, 0, Vec::new()), |p| {
        (
            p.directory.tenant.0,
            p.directory.journal.0,
            p.hosts
                .iter()
                .map(|(node_id, addr)| wire::UniverseHost {
                    node_id: *node_id,
                    addr: addr.to_string(),
                })
                .collect(),
        )
    })
}

pub(crate) fn pointer_from_wire(
    tenant: u64,
    journal: u64,
    hosts: Vec<wire::UniverseHost>,
) -> Result<Option<UniversePointer>, &'static str> {
    if tenant == 0 && journal == 0 && hosts.is_empty() {
        return Ok(None);
    }
    let directory = JournalIdentifier::new(TenantId(tenant), JournalId(journal));
    if !directory.is_set() || hosts.is_empty() {
        return Err("a universe pointer names no directory or no host");
    }
    let hosts = hosts
        .into_iter()
        .map(|h| {
            let addr = crate::address::Address::parse(&h.addr)
                .map_err(|_| "a universe host's address does not parse")?;
            if h.node_id == 0 {
                return Err("a universe host names no node");
            }
            Ok((h.node_id, addr))
        })
        .collect::<Result<_, _>>()?;
    Ok(Some(UniversePointer { directory, hosts }))
}

/// The cell side of the fleet's registration (#229, §3.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetRegistration {
    /// The fleet.
    pub fleet_id: u64,
    /// This cell's id.
    pub cell_id: u64,
    /// The metadata version.
    pub version: u32,
}
