//! **`Resolve`** (#216, `docs/architecture.md` §3.5): a client handed only
//! an entry endpoint asks it which references serve its tenant.
//!
//! The entry endpoint is a few addresses, or a DNS name in front of them.
//! [`resolve`] asks each address in turn until a machine answers with a
//! verdict: the tenant's cell, id and control journal, and the cell's
//! machines. A machine that answers `unavailable`, or does not answer, is
//! passed over. A client caches the answer and resolves again when a call is
//! refused (§3.5): an answer is a hint read from a fold, never a fact.
//!
//! Draws no randomness and decides no retry beyond one pass over the entry
//! endpoint: every other choice is the caller's.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

use super::Server;
use super::bootstrap::call_once;
use crate::rpc::NodeClient;
use crate::rpc::machine as wire;
use crate::rpc::methods::ResolveRpc;
use crate::{Address, Names};

/// Which references serve a tenant, as one machine of its cell answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// The universe the answering cell belongs to.
    pub universe_id: u64,
    /// The tenant's cell.
    pub cell_id: u64,
    /// The tenant.
    pub tenant: TenantId,
    /// Its control journal: where its journals' names live.
    pub control: JournalIdentifier,
    /// The cell's machines, where its registry places them.
    pub machines: Vec<(NodeId, Address)>,
    /// Each machine's interface as the registry holds it (#404 (static vs
    /// dynamic endpoints)): the encoded reference its data plane is served
    /// in. A machine with none is absent.
    pub interfaces: BTreeMap<NodeId, Vec<u8>>,
    /// The universe directory's position the answer was read at.
    pub at: u64,
    /// The universe's name, for people (#239 (names at the edge)); empty
    /// when the directory names none.
    pub universe_name: Vec<u8>,
    /// The cell's name, for people; empty when the directory names none.
    pub cell_name: Vec<u8>,
}

/// What resolving a tenant through the entry endpoint came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The tenant resolved.
    Resolved(Resolved),
    /// A machine answered, and refused: its label (`unknown_tenant`,
    /// `not_ready`, `internal`, `other_cell`, `no_universe`), the cell that
    /// answered, and the tenant's cell when it named one (`other_cell`).
    Refused {
        /// The refusal's label.
        refusal: String,
        /// The answering machine's cell.
        cell_id: u64,
        /// The tenant's cell, on `other_cell`; else 0.
        tenant_cell: u64,
        /// The answering machine's cell's name, for people (#239 (names at
        /// the edge)); empty when the machine knows none.
        cell_name: Vec<u8>,
        /// The tenant's cell's name, on `other_cell`; else empty.
        tenant_cell_name: Vec<u8>,
    },
    /// No machine of the entry endpoint gave a verdict.
    Unavailable,
    /// A machine answered with a resolution that does not decode.
    Malformed,
}

/// Resolve tenant `tenant` (its name) through the entry endpoint `entry`,
/// dialed through `names`: each address asked once, in order, each call
/// within `timeout`.
#[tracing::instrument(level = "debug", skip_all, fields(entry = entry.len()))]
pub async fn resolve<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    entry: &[Address],
    tenant: &[u8],
    timeout: Duration,
) -> Resolution {
    let request = wire::Resolve {
        tenant: tenant.to_vec(),
    };
    let first = ask(providers, rpc, names, entry, &request, timeout).await;
    // Another cell hosts the tenant (#232 (multiple cells)): ask once more,
    // at the entry endpoint the universe directory records for it. One hop
    // only, so a redirect never loops.
    let Some(ack) = &first else {
        return Resolution::Unavailable;
    };
    if ack.refusal != "other_cell" || ack.tenant_cell_endpoint.is_empty() {
        return from_wire(ack);
    }
    let Ok(endpoint) = ack
        .tenant_cell_endpoint
        .iter()
        .map(|member| Address::parse(&member.addr))
        .collect::<Result<Vec<_>, _>>()
    else {
        return Resolution::Malformed;
    };
    moonpool_assertions::reachable!("resolve: a client follows a tenant to its other cell");
    match ask(providers, rpc, names, &endpoint, &request, timeout).await {
        Some(second) => from_wire(&second),
        _ => Resolution::Unavailable,
    }
}

/// Ask each address of `entry` in turn: the first answer that is a
/// verdict (anything but `unavailable`), else `None`.
async fn ask<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    entry: &[Address],
    request: &wire::Resolve,
    timeout: Duration,
) -> Option<wire::ResolveAck> {
    for target in entry {
        match call_once::<P, ResolveRpc>(providers, rpc, names, target, request, timeout).await {
            Some(Ok(ack)) if ack.refusal == "unavailable" => {}
            Some(Ok(ack)) => return Some(ack),
            _ => {}
        }
    }
    None
}

impl Resolved {
    /// A client's servers for the cell's machines, through `rpc` and
    /// `names`: each machine's data plane through the interface the answer
    /// holds for it, else through its well-known endpoints.
    ///
    /// # Panics
    ///
    /// Never: the assertion checks one server per machine.
    #[must_use]
    pub fn servers<P: Providers>(&self, rpc: &RpcHandle<P>, names: &Names) -> Vec<Server<P>> {
        let servers: Vec<Server<P>> = self
            .machines
            .iter()
            .map(|(id, addr)| {
                let node = NodeClient::named(rpc, names.clone(), addr.clone());
                let node = match self.interfaces.get(id) {
                    Some(interface) => node.with_interface(interface),
                    None => node,
                };
                Server { id: id.0, node }
            })
            .collect();
        assert_eq!(servers.len(), self.machines.len(), "one server per machine");
        servers
    }
}

/// A machine's answer, judged.
#[must_use]
pub fn from_wire(ack: &wire::ResolveAck) -> Resolution {
    if !ack.refusal.is_empty() {
        return Resolution::Refused {
            refusal: ack.refusal.clone(),
            cell_id: ack.cell_id,
            tenant_cell: ack.tenant_cell,
            cell_name: ack.cell_name.clone(),
            tenant_cell_name: ack.tenant_cell_name.clone(),
        };
    }
    let control = ack
        .control
        .as_ref()
        .map_or(JournalIdentifier::new(TenantId(0), JournalId(0)), |c| {
            JournalIdentifier::new(TenantId(c.tenant), JournalId(c.journal))
        });
    let machines: Option<Vec<(NodeId, Address)>> = ack
        .machines
        .iter()
        .map(|m| {
            let addr = m.addr.parse::<Address>().ok()?;
            (m.node_id != 0).then_some((NodeId(m.node_id), addr))
        })
        .collect();
    let well_formed = ack.cell_id != 0
        && ack.tenant != 0
        && ack.tenant_cell == ack.cell_id
        && control.is_set()
        && control.tenant.0 == ack.tenant;
    match machines {
        Some(machines) if well_formed && !machines.is_empty() => Resolution::Resolved(Resolved {
            universe_id: ack.universe_id,
            cell_id: ack.cell_id,
            tenant: TenantId(ack.tenant),
            control,
            machines,
            interfaces: ack
                .machines
                .iter()
                .filter(|m| !m.machine_interface.is_empty())
                .map(|m| (NodeId(m.node_id), m.machine_interface.clone()))
                .collect(),
            at: ack.at,
            universe_name: ack.universe_name.clone(),
            cell_name: ack.cell_name.clone(),
        }),
        _ => Resolution::Malformed,
    }
}
