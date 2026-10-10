//! The RPC runtime a sim workload talks to a cluster through: one
//! client-only moonpool-rpc runtime per workload run, and a [`NodeClient`]
//! per server bound to it.
//!
//! The chain workload opens one per run and builds its [`ChainClient`]s —
//! the library's `paros::client` — over it.

use std::time::Duration;

use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::{SimContext, SimProviders, SimulationError, SimulationResult, TaskProvider};
use paros::client::{ClientTunables, Server};
use paros::{NodeClient, parse_addr};
use tokio_util::sync::{CancellationToken, DropGuard};

/// A workload's client to one server.
pub(crate) type SimClient = NodeClient<SimProviders>;

/// The library's client (#221) over the simulation's providers: what the
/// chain workload drives every journal call through.
pub(crate) type ChainClient = paros::client::Client<SimProviders>;

/// A client-only RPC runtime, driven on its own task until this handle
/// drops: every exit path from a workload stops it (the drop guard cancels
/// the task, which drops the driver and with it every connection and
/// pending call).
///
/// **Why a detached task is safe here.** The runtime draws moonpool
/// randomness and BUGGIFY (dial and ping jitter, its request cuts), and a
/// detached task that outlived a run would shift the next run's stream. This
/// one cannot: the handle lives in the workload's own `run` scope, so the
/// cancel lands before `run` returns and the task ends at its next poll,
/// inside the same run. It consults no paros hook (hooks stay on node
/// loops). The determinism canary covers it: every seed runs twice and must
/// reproduce every draw.
pub(crate) struct ClientRuntime {
    rpc: RpcHandle<SimProviders>,
    _stop: DropGuard,
}

impl ClientRuntime {
    /// Start a runtime under `config`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn start(ctx: &SimContext, config: RpcConfig) -> SimulationResult<Self> {
        let (driver, rpc) = RpcDriver::client_only(ctx.providers().clone(), config)
            .map_err(|e| SimulationError::InvalidState(format!("client RPC runtime: {e}")))?;
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        ctx.task()
            .spawn_task("paros-client-rpc", async move {
                moonpool_sim::select! {
                    biased;
                    () = cancelled.cancelled() => {}
                    error = driver.run() => {
                        tracing::warn!(%error, "client RPC runtime failed");
                    }
                }
            })
            .detach();
        Ok(Self {
            rpc,
            _stop: stop.drop_guard(),
        })
    }

    /// The library client of `servers` — `(node id, ip)` pairs, in the
    /// order the client indexes them — under `tunables`.
    pub(crate) fn chain_client(
        &self,
        ctx: &SimContext,
        servers: &[(u64, String)],
        tunables: ClientTunables,
    ) -> SimulationResult<ChainClient> {
        if servers.is_empty() {
            return Err(SimulationError::InvalidState(
                "a client needs at least one server".into(),
            ));
        }
        let ips: Vec<String> = servers.iter().map(|(_, ip)| ip.clone()).collect();
        let stubs = self.clients(&ips)?;
        let servers = servers
            .iter()
            .zip(stubs)
            .map(|((id, _), node)| Server { id: *id, node })
            .collect();
        Ok(
            paros::client::Client::new(ctx.providers(), servers, tunables)
                .with_shutdown(ctx.shutdown().clone()),
        )
    }

    /// A [`Connector`] on this runtime: what builds a client over servers
    /// learned at runtime (#246, the machines `init` formed).
    pub(crate) fn connector(&self, ctx: &SimContext, tunables: ClientTunables) -> Connector {
        Connector {
            rpc: self.rpc.clone(),
            providers: ctx.providers().clone(),
            shutdown: ctx.shutdown().clone(),
            tunables,
            names: crate::machine::names(ctx.state()),
        }
    }

    /// One client per server, in `servers` order.
    pub(crate) fn clients(&self, servers: &[String]) -> SimulationResult<Vec<SimClient>> {
        servers
            .iter()
            .map(|ip| {
                let addr = parse_addr(ip)?;
                let addr = addr
                    .parse()
                    .map_err(|e| SimulationError::InvalidState(format!("bad address: {e}")))?;
                Ok(NodeClient::new(&self.rpc, addr))
            })
            .collect()
    }
}

/// The client runtime's shape: its connect budget and its liveness pings
/// (provider time, so a connection left half-open by a node restart is
/// failed deterministically instead of swallowing requests forever). The
/// three durations are the chain workload's knobs.
pub(crate) fn client_rpc_config(
    connect_timeout: Duration,
    ping_interval: Duration,
    ping_timeout: Duration,
) -> RpcConfig {
    let mut config = RpcConfig {
        // The nodes' own limit, so a large `Inspect` reply fits.
        max_frame_bytes: paros::MAX_FRAME_BYTES,
        connect_timeout,
        ..RpcConfig::default()
    };
    config.peer.ping_interval = ping_interval;
    config.peer.ping_timeout = ping_timeout;
    config
}

/// Builds library clients over servers learned at runtime (#246): the
/// machines a cell formed over, by their minted ids. Bound to its
/// [`ClientRuntime`]'s RPC runtime, so it lives no longer than the run.
#[derive(Clone)]
pub(crate) struct Connector {
    rpc: RpcHandle<SimProviders>,
    providers: SimProviders,
    shutdown: CancellationToken,
    tunables: ClientTunables,
    /// The run's name table (#257): the machines' advertised names.
    names: paros::Names,
}

impl Connector {
    /// The RPC runtime the clients are bound to.
    pub(crate) fn rpc(&self) -> &RpcHandle<SimProviders> {
        &self.rpc
    }

    /// The run's providers.
    pub(crate) fn providers(&self) -> &SimProviders {
        &self.providers
    }

    /// How the clients resolve the machines' advertised names (#257).
    pub(crate) fn names(&self) -> &paros::Names {
        &self.names
    }

    /// The library client of `servers` (`(node id, advertised address)`, in
    /// the order the client indexes them), each resolved at every call.
    pub(crate) fn client(&self, servers: &[(u64, paros::Address)]) -> ChainClient {
        moonpool_sim::assert_always!(
            !servers.is_empty(),
            "client: a client over learned servers names at least one"
        );
        let servers = servers
            .iter()
            .map(|(id, addr)| Server {
                id: *id,
                node: NodeClient::named(&self.rpc, self.names.clone(), addr.clone()),
            })
            .collect();
        paros::client::Client::new(&self.providers, servers, self.tunables)
            .with_shutdown(self.shutdown.clone())
    }
}
