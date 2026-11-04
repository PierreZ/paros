//! A test rig: a client and fake nodes on moonpool's simulated network,
//! driven on one thread by the simulation's executor, so a test calls the
//! shipped policy loops over a real RPC runtime (the same rig as
//! `paros-machine`'s, without a disk).
//!
//! The fake nodes ([`cell`]) judge every journal call with the core's own
//! journal state machine; a test scripts the replies that the honest fold
//! never gives (a busy leader, a silent server, an unknown journal).

pub mod cell;

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::task::Poll;
use std::time::Duration;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::security::SecurityConfig;
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{NetworkConfiguration, SimProviders, SimWorld};

use crate::{Client, ClientTunables};
use cell::Cell;

/// The rig's seed: a display seed, not a witness.
const SEED: u64 = 7;

/// The most polls of the driver one test may take (two per simulation
/// step): a test that needs more is stuck.
const MAX_STEPS: u32 = 2_000_000;

/// The port every fake node listens on.
pub const PORT: u16 = 4500;

/// The IP of the client's machine.
const CLIENT_IP: &str = "10.0.1.1";

/// One simulated world.
pub struct Rig {
    sim: SimWorld,
}

impl Rig {
    pub fn new() -> Self {
        moonpool_sim::set_sim_seed(SEED);
        // No network fault: a test pins one behaviour, the sweep the faults.
        let mut network = NetworkConfiguration::fast_local();
        network.disable_fault_injection();
        Self {
            sim: SimWorld::new_with_network_config_and_seed(network, SEED),
        }
    }

    /// The providers of the machine at `ip`.
    pub fn providers(&self, ip: &str) -> SimProviders {
        let ip: IpAddr = ip.parse().expect("a test IP");
        SimProviders::new(self.sim.downgrade(), ip)
    }

    /// The providers of the client's machine.
    pub fn client_providers(&self) -> SimProviders {
        self.providers(CLIENT_IP)
    }

    /// Run `future` to its end, stepping the simulated network and clock
    /// while it waits.
    ///
    /// One event per step, and only once every task that `future` woke
    /// ran: the executor polls this driver again only after a full drain.
    /// A step taken first would move the clock past work that is ready
    /// now (a task woken by a channel, not by an event).
    pub fn run<F: Future>(&mut self, future: F) -> F::Output {
        let sim = &mut self.sim;
        let mut steps = 0;
        let mut drained = false;
        Executor::new(SEED).block_on(async {
            let mut future = std::pin::pin!(future);
            std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    steps += 1;
                    assert!(steps < MAX_STEPS, "the test is stuck");
                    if drained && sim.has_pending_events() {
                        sim.step();
                    }
                    drained = !drained;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        })
    }

    /// Run `body` with a client of the fake nodes `ids` of `cell`, each
    /// started on its own machine first.
    pub fn run_with<F: Future>(
        &mut self,
        cell: &Cell,
        ids: &[u64],
        body: impl FnOnce(Client<SimProviders>) -> F,
    ) -> F::Output {
        self.run_on(cell, ids, |client, _| body(client))
    }

    /// [`Rig::run_with`], `body` given the client machine's providers too.
    pub fn run_on<F: Future>(
        &mut self,
        cell: &Cell,
        ids: &[u64],
        body: impl FnOnce(Client<SimProviders>, SimProviders) -> F,
    ) -> F::Output {
        let nodes: Vec<(u64, SimProviders)> = ids
            .iter()
            .map(|id| (*id, self.providers(&node_addr(*id).ip().to_string())))
            .collect();
        let providers = self.client_providers();
        let ids = ids.to_vec();
        let cell = cell.clone();
        self.run(async move {
            for (id, node) in &nodes {
                cell.start(node, *id).await;
            }
            body(client(&providers, &ids), providers).await
        })
    }
}

/// The address of fake node `index` (from 1).
pub fn node_addr(index: u64) -> SocketAddr {
    format!("10.0.0.{index}:{PORT}")
        .parse()
        .expect("a test address")
}

/// A machine's RPC runtime listening at `addr`, its driver spawned.
pub async fn listen(providers: &SimProviders, addr: SocketAddr) -> RpcHandle<SimProviders> {
    let config = RpcConfig {
        security: SecurityConfig::trusted_network(),
        ..RpcConfig::default()
    };
    let (driver, rpc) = RpcDriver::listen(providers.clone(), &addr.to_string(), config)
        .await
        .expect("listen");
    providers
        .local_task()
        .spawn_local_task("rig-server", async move {
            let _ = driver.run().await;
        })
        .detach();
    rpc
}

/// A client-only RPC runtime on `providers`, its driver spawned.
pub fn runtime(providers: &SimProviders) -> RpcHandle<SimProviders> {
    let (driver, rpc) =
        RpcDriver::client_only(providers.clone(), RpcConfig::default()).expect("a valid config");
    providers
        .local_task()
        .spawn_local_task("rig-client", async move {
            let _ = driver.run().await;
        })
        .detach();
    rpc
}

/// The tunables of a rig client: short bounds, so a silent server costs a
/// test little simulated time.
pub fn tunables() -> ClientTunables {
    ClientTunables {
        request_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_millis(300),
        redirect_backoff: Duration::from_millis(10),
        retry_backoff: Duration::from_millis(10),
        ..ClientTunables::default()
    }
}

/// A client of the fake nodes `ids` (node `i` at [`node_addr`]`(i)`).
pub fn client(providers: &SimProviders, ids: &[u64]) -> Client<SimProviders> {
    let rpc = runtime(providers);
    let addrs: Vec<(u64, SocketAddr)> = ids.iter().map(|id| (*id, node_addr(*id))).collect();
    Client::connect(providers, &rpc, &addrs, tunables())
}
