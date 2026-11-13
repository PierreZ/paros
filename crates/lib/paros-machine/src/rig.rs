//! A test rig: machines on moonpool's simulated network and disk, driven on
//! one thread by the simulation's executor, so a test calls the shipped
//! machine code over a real RPC runtime (the same rig as `paros-net`'s).

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::task::Poll;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{NetworkConfiguration, SimProviders, SimWorld, StorageConfiguration};
use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_model::address::{Address, Names};
use paros_storage::journal::{Geometry, JournalStoreConfig};

use crate::{CellPlan, Class, Locality, MachineFacts};

/// The rig's seed: a display seed, not a witness.
const SEED: u64 = 7;

/// The most polls of the driver one test may take (two per simulation
/// step): a test that needs more is stuck.
const MAX_STEPS: u32 = 2_000_000;

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

    /// Lose power on the machine at `ip`: every directory entry no sync
    /// made durable is lost.
    pub fn power_loss(&mut self, ip: &str) {
        let ip: IpAddr = ip.parse().expect("a test IP");
        let mut config = StorageConfiguration::fast_local();
        config.unsynced_dir_entry_loss_probability = 1.0;
        self.sim.set_storage_config(config);
        self.sim.simulate_crash_for_process(ip, true);
        self.sim
            .set_storage_config(StorageConfiguration::fast_local());
    }

    /// Run `future` to its end, stepping the simulated network, disk and
    /// clock while it waits.
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

/// The facts of machine `node` of `class`, listening and advertising at
/// `listen`.
pub fn facts(node: NodeId, listen: SocketAddr, class: Class) -> MachineFacts {
    MachineFacts {
        node_id: node,
        class,
        locality: Locality::default(),
        name: format!("m{}", node.0),
        addr: Address::from(listen),
        listen,
        names: Names::literal(),
        incarnation: 0,
        machine_interface: Vec::new(),
        load: paros_model::load::LoadBoard::default(),
        metrics: paros_observe::metrics::Metrics::default(),
        frontend: None,
        disk: paros_model::machine::Disk::UNKNOWN,
    }
}

/// A valid plan of cell `cell` over `members`: its control and election
/// journals in tenant `cell`, no fleet.
pub fn plan(cell: u64, members: Vec<(NodeId, Address)>) -> CellPlan {
    let control = JournalIdentifier::new(TenantId(cell), JournalId(1));
    let election = JournalIdentifier::new(TenantId(cell), JournalId(2));
    let plan = CellPlan {
        cell_id: cell,
        members,
        control,
        fleet: None,
        election,
        journals: vec![control, election],
    };
    plan.check().expect("a valid test plan");
    plan
}

/// The address of test machine `index`.
pub fn machine_addr(index: u64) -> SocketAddr {
    format!("10.0.8.{index}:4500")
        .parse()
        .expect("a test address")
}

/// Test machine `index` at its address, as a plan or a book names it.
pub fn machine(index: u64) -> (NodeId, Address) {
    (NodeId(index), Address::from(machine_addr(index)))
}
