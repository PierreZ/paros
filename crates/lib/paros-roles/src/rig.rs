//! A test rig: role loops on moonpool's simulated network, driven on one
//! thread by the simulation's executor, so a test calls the shipped loops
//! (`run_node`, `run_matchmaker`, ...) over a real RPC runtime and a
//! client of the library (the same rig as `paros-machine`'s).

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::task::Poll;
use std::time::Duration;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{NetworkConfiguration, SimProviders, SimWorld};
use paros_client::{Client, ClientTunables};
use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

/// The rig's seed: a display seed, not a witness.
const SEED: u64 = 7;

/// The most polls of the driver one test may take (two per simulation
/// step): a test that needs more is stuck.
const MAX_STEPS: u32 = 4_000_000;

/// The port every role listens on.
pub const PORT: u16 = 4500;

/// The IP of the client's machine.
pub const CLIENT_IP: &str = "10.0.1.1";

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

    /// The providers of node `index` (at [`addr`]`(index)`).
    pub fn node(&self, index: u64) -> SimProviders {
        self.providers(&addr(index).ip().to_string())
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
}

/// The address of test machine `index` (from 1).
pub fn addr(index: u64) -> SocketAddr {
    format!("10.0.0.{index}:{PORT}")
        .parse()
        .expect("a test address")
}

/// Nodes `ids`, each with its address, as `run_node` names its pool.
pub fn pool(ids: &[u64]) -> Vec<(NodeId, String)> {
    ids.iter()
        .map(|id| (NodeId(*id), addr(*id).to_string()))
        .collect()
}

/// The journal every rig test serves.
pub fn journal() -> JournalIdentifier {
    JournalIdentifier::new(TenantId(5), JournalId(9))
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

/// A client of nodes `ids` (node `i` at [`addr`]`(i)`).
pub fn client(providers: &SimProviders, ids: &[u64]) -> Client<SimProviders> {
    let rpc = runtime(providers);
    let addrs: Vec<(u64, SocketAddr)> = ids.iter().map(|id| (*id, addr(*id))).collect();
    Client::connect(providers, &rpc, &addrs, tunables())
}

/// Sleep `duration` of simulated time on `providers`.
pub async fn sleep(providers: &SimProviders, duration: Duration) {
    use moonpool_core::{LocalProviders, TimeProvider};
    let _ = providers.time().sleep(duration).await;
}
