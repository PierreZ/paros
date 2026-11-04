//! A test rig: machines on moonpool's simulated network and disk, driven on
//! one thread by the simulation's executor, so a test calls the shipped
//! machine code over a real RPC runtime (the same rig as `paros-net`'s).

use std::future::Future;
use std::net::IpAddr;
use std::task::Poll;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{NetworkConfiguration, SimProviders, SimWorld, StorageConfiguration};
use paros_storage::journal::{Geometry, JournalStoreConfig};

/// The rig's seed: a display seed, not a witness.
const SEED: u64 = 7;

/// The most simulation steps one test may take: a test that needs more is
/// stuck.
const MAX_STEPS: u32 = 1_000_000;

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
        let mut sim = SimWorld::new_with_network_config_and_seed(network, SEED);
        sim.set_storage_config(StorageConfiguration::fast_local());
        Self { sim }
    }

    /// The providers of the machine at `ip`.
    pub fn providers(&self, ip: &str) -> SimProviders {
        let ip: IpAddr = ip.parse().expect("a test IP");
        SimProviders::new(self.sim.downgrade(), ip)
    }

    /// Run `future` to its end, stepping the simulated network, disk and
    /// clock while it waits.
    pub fn run<F: Future>(&mut self, future: F) -> F::Output {
        let sim = &mut self.sim;
        let mut steps = 0;
        Executor::new(SEED).block_on(async {
            let mut future = std::pin::pin!(future);
            std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(output),
                Poll::Pending => {
                    steps += 1;
                    assert!(steps < MAX_STEPS, "the test is stuck");
                    if sim.has_pending_events() {
                        sim.step();
                    }
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        })
    }
}

/// A client-only RPC runtime on `providers`, its driver spawned.
pub fn client(providers: &SimProviders) -> RpcHandle<SimProviders> {
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

/// A store with segments of a few blocks, so a short test formats fast and
/// crosses segment rollover and prefix deletion. Tests only: a deployment
/// keeps the default geometry (#600 (drop the small layout)).
pub fn compact() -> JournalStoreConfig {
    JournalStoreConfig {
        geometry: Geometry {
            persist_blocks: 8,
            gap_blocks: 0,
            entry_blocks: 16,
        },
        ..JournalStoreConfig::default()
    }
}
