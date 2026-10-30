//! **The resolver** (#233 (resolver), `docs/architecture/03-5-frontend-and-resolver.md` §3.5):
//! the first hop of `Resolve`, "which cell serves tenant T".
//!
//! A machine given the frontend role also serves the resolver role
//! (`ResolveEntry`), with the same `Authz` and the same wall clock. The
//! resolver is thin: it reads the universe directory and nothing else of a
//! tenant, and it holds no key and no state of its own. For each request
//! it:
//!
//! 1. checks the token against the universe entry's root public keys (the
//!    machine's pin while the entry holds none): the token must allow
//!    `tenant.resolve` on the tenant (an `admin` token resolves any);
//! 2. resolves the tenant's name in the directory: only a `READY` `users`
//!    tenant resolves;
//! 3. answers a **redirect**: the tenant, its cell and that cell's entry
//!    endpoint, as the directory records it. The client then asks a machine
//!    of that cell `Resolve`, which names the tenant's frontends. The
//!    resolver never forwards a call and never answers a cell's machines.
//!
//! **Static stability** (§3.3): the answer is read from the directory last
//! read to its tail. While the universe directory cannot be read, the
//! resolver goes on answering from that copy.

use std::time::Duration;

use paros_client::names::{TenantResolution, tenant_in};
use paros_model::authz::{Denial, Operation, Request, Target};
use paros_model::fleet::{FleetDirectory, RootKeyEntry};
use paros_model::name::JournalName;
use paros_roles::frontend::FrontendRole;
use paros_wire::machine as wire;

/// The redirect `role` answers `request` with from `directory`, at the wall
/// clock `now` (since the Unix epoch); `cell_id` is the answering
/// machine's cell.
pub(super) fn answer(
    directory: &FleetDirectory,
    role: &FrontendRole,
    now: Duration,
    cell_id: u64,
    request: &wire::ResolveEntry,
) -> wire::ResolveAck {
    assert!(cell_id != 0, "a resolver serves in a cell");
    let refused = |refusal: &str| wire::ResolveAck {
        refusal: refusal.into(),
        cell_id,
        ..wire::ResolveAck::default()
    };
    let Some(universe_id) = directory.fleet() else {
        return refused("no_universe");
    };
    // The names first, before the token: a malformed request is the
    // caller's mistake, never a denial.
    let Some((tenant, target)) = target(request) else {
        moonpool_assertions::reachable!("resolver: a malformed request is refused");
        return refused(Denial::Malformed.as_str());
    };
    let keys: Vec<RootKeyEntry> = directory.root_keys().cloned().collect();
    let checked = role.authz.authorize(
        &request.token,
        &Request {
            operation: Operation::Resolve,
            target,
            now,
            keys: (!keys.is_empty()).then_some(keys.as_slice()),
        },
    );
    if let Err(denial) = checked {
        assert!(
            denial != Denial::Malformed,
            "an Authz never answers malformed"
        );
        moonpool_assertions::reachable!("resolver: a token is denied before any lookup");
        tracing::debug!(denial = denial.as_str(), "resolver_denied");
        return refused(denial.as_str());
    }
    let at = directory.next_seq();
    let mut ack = wire::ResolveAck {
        universe_id,
        at,
        universe_name: directory.universe_name().to_vec(),
        cell_name: directory.cell_name(cell_id).to_vec(),
        ..refused("")
    };
    match tenant_in(directory, tenant.as_bytes(), at) {
        TenantResolution::Resolved { tenant, .. } => {
            let entry = directory
                .tenant(tenant)
                .expect("a resolved tenant has an entry");
            assert!(entry.cell_id != 0, "a resolved tenant has a cell");
            ack.tenant = tenant.0;
            ack.tenant_cell = entry.cell_id;
            ack.tenant_cell_name = directory.cell_name(entry.cell_id).to_vec();
            ack.tenant_cell_endpoint = directory
                .endpoint(entry.cell_id)
                .iter()
                .map(|(id, addr)| wire::Member {
                    node_id: *id,
                    addr: addr.to_string(),
                    machine_interface: Vec::new(),
                    zone: String::new(),
                })
                .collect();
            if ack.tenant_cell_endpoint.is_empty() {
                ack.refusal = "no_endpoint".into();
            } else if entry.cell_id == cell_id {
                moonpool_assertions::reachable!("resolver: a tenant is redirected to its own cell");
            } else {
                moonpool_assertions::reachable!("resolver: a tenant is redirected to another cell");
            }
        }
        TenantResolution::NotReady { tenant, .. } => {
            ack.tenant = tenant.0;
            ack.refusal = "not_ready".into();
        }
        TenantResolution::Internal => ack.refusal = "internal".into(),
        TenantResolution::Unknown => {
            moonpool_assertions::reachable!("resolver: no tenant holds the name");
            ack.refusal = "unknown_tenant".into();
        }
        TenantResolution::Unreadable(_) => unreachable!("the directory was folded"),
    }
    assert!(
        ack.refusal.is_empty() != ack.tenant_cell_endpoint.is_empty(),
        "a redirect, and only a redirect, names an endpoint"
    );
    assert!(ack.control.is_none(), "a resolver names no control journal");
    assert!(
        ack.machines.is_empty() && ack.frontends.is_empty() && ack.batchers.is_empty(),
        "a resolver names no machine of a cell"
    );
    ack
}

/// The tenant `request` names and the target its token is checked for:
/// the tenant alone, or the journal the caller names in it. `None` when a
/// name is malformed.
fn target(request: &wire::ResolveEntry) -> Option<(&str, Target<'_>)> {
    let tenant = std::str::from_utf8(&request.tenant).ok()?;
    let journal = std::str::from_utf8(&request.journal).ok()?;
    if journal.is_empty() {
        return (!tenant.is_empty()).then_some((tenant, Target::Tenant { tenant }));
    }
    JournalName::new(tenant, journal).ok()?;
    assert!(!tenant.is_empty(), "a journal name has a tenant");
    Some((tenant, Target::Users { tenant, journal }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(tenant: &str, journal: &str) -> wire::ResolveEntry {
        wire::ResolveEntry {
            token: Vec::new(),
            tenant: tenant.into(),
            journal: journal.into(),
        }
    }

    #[test]
    fn a_request_names_a_tenant_alone_or_with_its_journal() {
        assert_eq!(
            target(&request("acme", "")),
            Some(("acme", Target::Tenant { tenant: "acme" }))
        );
        assert_eq!(
            target(&request("acme", "orders")),
            Some((
                "acme",
                Target::Users {
                    tenant: "acme",
                    journal: "orders"
                }
            ))
        );
        assert_eq!(target(&request("", "")), None);
        assert_eq!(target(&request("", "orders")), None);
    }
}
