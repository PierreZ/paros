//! **Through a resolver** (#233 (resolver), §3.5): the two hops of
//! `Resolve` for a client handed only the universe's entry endpoint and a
//! token.
//!
//! 1. [`redirect`] asks the resolvers `ResolveEntry`, each in turn, until one
//!    gives a verdict: the tenant's cell and that cell's entry endpoint, or
//!    a refusal (a denied token among them).
//! 2. [`resolve_through`] then asks that endpoint `Resolve` ([`super::resolve`]),
//!    whose answer names the cell's machines and the tenant's frontends.
//!
//! The hops are bounded: one pass over the resolvers, one over the cell's
//! endpoint, and at most the one `other_cell` hop [`super::resolve`]
//! follows when the resolver's copy of the directory was stale. A redirect
//! never loops.

use std::time::Duration;

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::TenantId;
use paros_net::runtime::ParosProviders;

use super::{Resolution, resolve};
use crate::bootstrap::call_once;
use paros_model::address::{Address, Names};
use paros_wire::machine as wire;
use paros_wire::methods::ResolveEntryRpc;

/// A resolver's redirect: where tenant `tenant` is served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Redirect {
    /// The universe the resolver's directory names.
    pub universe_id: u64,
    /// The tenant.
    pub tenant: TenantId,
    /// The tenant's cell.
    pub cell_id: u64,
    /// The tenant's cell's name, for people; empty when the directory names
    /// none.
    pub cell_name: Vec<u8>,
    /// That cell's entry endpoint, as the universe directory records it.
    pub endpoint: Vec<(NodeId, Address)>,
    /// The cell of the resolver that answered.
    pub resolver_cell: u64,
    /// The universe directory's position the redirect was read at.
    pub at: u64,
}

/// What asking the resolvers came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Redirection {
    /// A resolver named the tenant's cell.
    Redirected(Redirect),
    /// A resolver answered, and refused: its label (`invalid_token`,
    /// `expired`, `forbidden`, `malformed`, `unknown_tenant`, `not_ready`,
    /// `internal`, `no_universe`, `no_endpoint`) and its cell.
    Refused {
        /// The refusal's label.
        refusal: String,
        /// The answering resolver's cell.
        cell_id: u64,
    },
    /// No resolver gave a verdict.
    Unavailable,
    /// A resolver answered with a redirect that does not decode.
    Malformed,
}

/// Ask the resolvers `resolvers`, dialed through `names`, which cell serves
/// tenant `tenant` (its name) for the bearer of `token`: each asked once,
/// in order, each call within `timeout`. `journal` is the journal the
/// caller means to reach, or empty.
#[tracing::instrument(level = "debug", skip_all, fields(resolvers = resolvers.len()))]
pub async fn redirect<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    resolvers: &[Address],
    (tenant, journal): (&[u8], &[u8]),
    token: &[u8],
    timeout: Duration,
) -> Redirection {
    let request = wire::ResolveEntry {
        token: token.to_vec(),
        tenant: tenant.to_vec(),
        journal: journal.to_vec(),
    };
    for target in resolvers {
        let answer =
            call_once::<P, ResolveEntryRpc>(providers, rpc, names, target, &request, timeout).await;
        match answer {
            Some(Ok(ack)) if ack.refusal == "unavailable" => {}
            Some(Ok(ack)) => return redirect_from_wire(&ack),
            _ => {}
        }
    }
    Redirection::Unavailable
}

/// Resolve tenant `tenant` in both hops: the resolvers `resolvers` name its
/// cell, then that cell's endpoint names its machines and frontends.
/// A refusal of either hop is the answer; a resolver's refusal is
/// [`Resolution::Refused`] with the resolver's label.
///
/// # Panics
///
/// Never: a decoded redirect always names an endpoint.
#[tracing::instrument(level = "debug", skip_all, fields(resolvers = resolvers.len()))]
pub async fn resolve_through<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    resolvers: &[Address],
    (tenant, journal): (&[u8], &[u8]),
    token: &[u8],
    timeout: Duration,
) -> (Redirection, Resolution) {
    let redirection = redirect(
        providers,
        rpc,
        names,
        resolvers,
        (tenant, journal),
        token,
        timeout,
    )
    .await;
    let resolution = match &redirection {
        Redirection::Redirected(redirect) => {
            let endpoint: Vec<Address> = redirect
                .endpoint
                .iter()
                .map(|(_, addr)| addr.clone())
                .collect();
            assert!(!endpoint.is_empty(), "a redirect names an endpoint");
            resolve(providers, rpc, names, &endpoint, tenant, timeout).await
        }
        Redirection::Refused { refusal, cell_id } => Resolution::Refused {
            refusal: refusal.clone(),
            cell_id: *cell_id,
            tenant_cell: 0,
            cell_name: Vec::new(),
            tenant_cell_name: Vec::new(),
        },
        Redirection::Unavailable => Resolution::Unavailable,
        Redirection::Malformed => Resolution::Malformed,
    };
    (redirection, resolution)
}

/// A resolver's answer, judged.
#[must_use]
pub fn redirect_from_wire(ack: &wire::ResolveAck) -> Redirection {
    if !ack.refusal.is_empty() {
        return Redirection::Refused {
            refusal: ack.refusal.clone(),
            cell_id: ack.cell_id,
        };
    }
    let endpoint: Option<Vec<(NodeId, Address)>> = ack
        .tenant_cell_endpoint
        .iter()
        .map(|m| {
            let addr = m.addr.parse::<Address>().ok()?;
            (m.node_id != 0).then_some((NodeId(m.node_id), addr))
        })
        .collect();
    let well_formed = ack.cell_id != 0
        && ack.tenant != 0
        && ack.tenant_cell != 0
        && ack.control.is_none()
        && ack.machines.is_empty();
    match endpoint {
        Some(endpoint) if well_formed && !endpoint.is_empty() => {
            Redirection::Redirected(Redirect {
                universe_id: ack.universe_id,
                tenant: TenantId(ack.tenant),
                cell_id: ack.tenant_cell,
                cell_name: ack.tenant_cell_name.clone(),
                endpoint,
                resolver_cell: ack.cell_id,
                at: ack.at,
            })
        }
        _ => Redirection::Malformed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: u64) -> wire::Member {
        wire::Member {
            node_id: id,
            addr: format!("10.0.0.{id}:4500"),
            ..wire::Member::default()
        }
    }

    #[test]
    fn a_redirect_names_the_tenant_its_cell_and_the_cell_endpoint() {
        let ack = wire::ResolveAck {
            cell_id: 3,
            tenant: 7,
            tenant_cell: 9,
            tenant_cell_endpoint: vec![member(1), member(2)],
            ..wire::ResolveAck::default()
        };
        let Redirection::Redirected(redirect) = redirect_from_wire(&ack) else {
            panic!("a redirect decodes");
        };
        assert_eq!(redirect.tenant, TenantId(7));
        assert_eq!(redirect.cell_id, 9);
        assert_eq!(redirect.resolver_cell, 3);
        assert_eq!(redirect.endpoint.len(), 2);
    }

    #[test]
    fn a_refusal_or_a_redirect_without_endpoint_is_no_redirect() {
        let denied = wire::ResolveAck {
            refusal: "forbidden".into(),
            cell_id: 3,
            ..wire::ResolveAck::default()
        };
        assert_eq!(
            redirect_from_wire(&denied),
            Redirection::Refused {
                refusal: "forbidden".into(),
                cell_id: 3
            }
        );
        let bare = wire::ResolveAck {
            cell_id: 3,
            tenant: 7,
            tenant_cell: 9,
            ..wire::ResolveAck::default()
        };
        assert_eq!(redirect_from_wire(&bare), Redirection::Malformed);
    }
}
