//! The RPC runtime a sim workload talks to a cluster through: one
//! client-only moonpool-rpc runtime per workload run, and a [`NodeClient`]
//! per server bound to it.
//!
//! The corpus builds its [`CorpusClients`](crate::corpus) on it, and the
//! chain workload opens one per run.

use std::time::Duration;

use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::{SimContext, SimProviders, SimulationError, SimulationResult, TaskProvider};
use paros::{NodeClient, parse_addr};
use tokio_util::sync::{CancellationToken, DropGuard};

/// A workload's client to one server.
pub(crate) type SimClient = NodeClient<SimProviders>;

/// A client-only RPC runtime, driven on its own task until this handle
/// drops: every exit path from a workload stops it (the drop guard cancels
/// the task, which drops the driver and with it every connection and
/// pending call).
pub(crate) struct ClientRuntime {
    rpc: RpcHandle<SimProviders>,
    _stop: DropGuard,
}

impl ClientRuntime {
    /// Start a runtime under `config`.
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
/// three durations are the chain workload's knobs; the corpus passes the
/// production defaults through [`default_client_rpc_config`].
pub(crate) fn client_rpc_config(
    connect_timeout: Duration,
    ping_interval: Duration,
    ping_timeout: Duration,
) -> RpcConfig {
    let mut config = RpcConfig {
        connect_timeout,
        ..RpcConfig::default()
    };
    config.peer.ping_interval = ping_interval;
    config.peer.ping_timeout = ping_timeout;
    config
}

/// [`client_rpc_config`] at the production defaults.
pub(crate) fn default_client_rpc_config() -> RpcConfig {
    client_rpc_config(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
}
