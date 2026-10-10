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

use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

use super::bootstrap::call_once;
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
    /// The universe directory's position the answer was read at.
    pub at: u64,
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
    for target in entry {
        match call_once::<P, ResolveRpc>(providers, rpc, names, target, &request, timeout).await {
            Some(Ok(ack)) if ack.refusal == "unavailable" => {}
            Some(Ok(ack)) => return from_wire(&ack),
            _ => {}
        }
    }
    Resolution::Unavailable
}

/// A machine's answer, judged.
#[must_use]
pub fn from_wire(ack: &wire::ResolveAck) -> Resolution {
    if !ack.refusal.is_empty() {
        return Resolution::Refused {
            refusal: ack.refusal.clone(),
            cell_id: ack.cell_id,
            tenant_cell: ack.tenant_cell,
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
            at: ack.at,
        }),
        _ => Resolution::Malformed,
    }
}
