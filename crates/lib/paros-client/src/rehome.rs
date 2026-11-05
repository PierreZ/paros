//! **A client follows a moved tenant** (#785 (moved record), §3.7): a
//! frontend answers a call for a tenant that lives in another cell now with
//! a `moved` verdict and forwards nothing. A client built with a
//! [`Resolving`] then resolves the tenant's name again, takes the frontends
//! of the cell that serves it, and retries the call there, once.
//!
//! The retry is safe: a `moved` call reached no machine, so it never ran.
//! A client with no [`Resolving`] returns the `moved` outcome to its
//! caller.

use std::sync::Arc;
use std::time::Duration;

use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_model::address::{Address, Names};
use paros_net::runtime::ParosProviders;
use paros_net::{NodeClient, Pass};

use crate::Server;
use crate::resolve::{Resolution, Resolved, resolve};

/// Where a client through frontends goes when its tenant moved: it
/// resolves the tenant
/// `name` through the machines at `entry` (`Resolve`, one `other_cell` hop),
/// and calls the cell's frontends with the client's own `pass`.
pub struct Resolving<P: ParosProviders> {
    providers: P,
    rpc: RpcHandle<P>,
    names: Names,
    entry: Vec<Address>,
    name: Vec<u8>,
    pass: Arc<Pass>,
    timeout: Duration,
}

impl<P: ParosProviders> Resolving<P> {
    /// Resolve the tenant `name` through the machines at `entry`, within
    /// `timeout`, for a client that presents `pass`.
    ///
    /// # Panics
    ///
    /// If `entry` is empty or `name` is empty: a client that cannot resolve
    /// is a programming error.
    #[must_use]
    pub fn new(
        (providers, rpc, names): (&P, &RpcHandle<P>, &Names),
        entry: Vec<Address>,
        name: Vec<u8>,
        pass: Arc<Pass>,
        timeout: Duration,
    ) -> Self {
        assert!(!entry.is_empty(), "a resolution needs an entry machine");
        assert!(!name.is_empty(), "a resolution names its tenant");
        Self {
            providers: providers.clone(),
            rpc: rpc.clone(),
            names: names.clone(),
            entry,
            name,
            pass,
            timeout,
        }
    }
}

impl<P: ParosProviders> Resolving<P> {
    /// The servers that serve the client's tenant now, or `None` when no
    /// resolution answered: the client then returns the `moved` outcome.
    pub(crate) async fn rehome(&self) -> Option<Vec<Server<P>>> {
        let resolution = resolve(
            &self.providers,
            &self.rpc,
            &self.names,
            &self.entry,
            &self.name,
            self.timeout,
        )
        .await;
        let Resolution::Resolved(resolved) = resolution else {
            return None;
        };
        let servers = frontends(&resolved, &self.rpc, &self.names, &self.pass);
        (!servers.is_empty()).then_some(servers)
    }
}

/// A client's servers for the frontends of the cell that `resolved` names:
/// its booked frontends, else its machines, which each serve the frontend
/// role (#723 (frontend as a parosd role)). Each call presents `pass`.
///
/// # Panics
///
/// Never: one server per frontend is an invariant, checked.
#[must_use]
pub fn frontends<P: ParosProviders>(
    resolved: &Resolved,
    rpc: &RpcHandle<P>,
    names: &Names,
    pass: &Arc<Pass>,
) -> Vec<Server<P>> {
    let chosen: &[(NodeId, Address)] = if resolved.frontends.is_empty() {
        &resolved.machines
    } else {
        &resolved.frontends
    };
    let servers: Vec<Server<P>> = chosen
        .iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::frontend(rpc, names.clone(), addr.clone(), pass.clone()),
        })
        .collect();
    assert_eq!(servers.len(), chosen.len(), "one server per frontend");
    servers
}
