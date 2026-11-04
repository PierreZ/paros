//! A test rig: role loops on moonpool's simulated network, driven on one
//! thread by the simulation's executor, so a test calls the shipped loops
//! (`run_node`, `run_matchmaker`, ...) over a real RPC runtime and a
//! client of the library (the same rig as `paros-machine`'s).

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use moonpool_core::{LocalTaskProvider, ThreadProviders};
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};
use moonpool_sim::executor::Executor;
use moonpool_sim::{NetworkConfiguration, SimProviders, SimWorld};
use paros_client::{Client, ClientTunables};
use paros_core::journal::{JournalId, JournalIdentifier, TenantId, WriterMode};
use paros_core::matchmaker::MatchmakerConfig;
use paros_core::membership::{MatchmakerId, ProxyId, QuorumSystem};
use paros_core::{Config, NodeId};
use paros_net::transport::Transport;
use paros_observe::audit::NoAudit;
use paros_storage::MemStorage;
use paros_storage::matchmaker::MemMatchmakerStorage;
use tokio_util::sync::CancellationToken;

use crate::matchmaker::HostedSet;
use crate::node::{BootKind, DriverTunables, RunError, SingleStore};
use crate::proxy_leader::{ProxyConfig, ProxyJournal};

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

/// The address of test machine `index` (from 1): a node.
pub fn addr(index: u64) -> SocketAddr {
    at(0, index)
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

/// The address of role `index` (from 1) on test subnet `net`: `0` the
/// nodes, `2` the matchmakers, `3` the proxy leaders, `4` the replicas.
pub fn at(net: u8, index: u64) -> SocketAddr {
    format!("10.0.{net}.{index}:{PORT}")
        .parse()
        .expect("a test address")
}

/// The node id of replica `rank`: outside every pool the tests draw.
pub fn replica_id(rank: u64) -> NodeId {
    NodeId(100 + rank)
}

/// A deployment of one journal: its acceptors and every opt-in role, each
/// on its own machine, every store in memory and booted for the first time.
#[derive(Clone, Debug, Default)]
pub struct Deployment {
    /// The bootstrap acceptors.
    pub peers: Vec<u64>,
    /// Nodes of the pool outside the bootstrap configuration.
    pub spares: Vec<u64>,
    /// The bootstrap matchmaker set, `MatchmakerId(0..matchmakers)`.
    pub matchmakers: u64,
    /// Matchmakers of the pool outside the bootstrap set.
    pub spare_matchmakers: u64,
    /// The proxy leaders, `ProxyId(0..proxies)`.
    pub proxies: u64,
    /// The replicas, [`replica_id`]`(0..replicas)`.
    pub replicas: u64,
    /// The driver's tunables of every role.
    pub tunables: DriverTunables,
    /// Who may write the journal.
    pub writer_mode: WriterMode,
    /// The bootstrap configuration's quorum system.
    pub quorum_system: QuorumSystem,
}

/// One role's run, its end named by the role.
type Role = Pin<Box<dyn Future<Output = (String, Result<(), RunError>)>>>;

/// What one role's run ended with, named by the role.
pub type Ends = Vec<(String, Result<(), RunError>)>;

impl Deployment {
    /// A plain deployment of `peers`.
    pub fn plain(peers: &[u64]) -> Self {
        Self {
            peers: peers.to_vec(),
            ..Self::default()
        }
    }

    /// Every node of the pool, sorted.
    pub fn pool(&self) -> Vec<u64> {
        let mut pool: Vec<u64> = self.peers.iter().chain(&self.spares).copied().collect();
        pool.sort_unstable();
        pool
    }

    fn matchmaker_ids(count: u64) -> Vec<MatchmakerId> {
        (0..count).map(MatchmakerId).collect()
    }

    /// The configuration of node `id` (a replica's too).
    pub fn config(&self, id: NodeId) -> Config {
        let mut config = Config::new(id, journal());
        config.peers = self.peers.iter().map(|id| NodeId(*id)).collect();
        if !self.spares.is_empty() {
            config.nodes = self.pool().into_iter().map(NodeId).collect();
        }
        config.matchmakers = Self::matchmaker_ids(self.matchmakers);
        if self.spare_matchmakers > 0 {
            config.matchmaker_pool =
                Self::matchmaker_ids(self.matchmakers + self.spare_matchmakers);
        }
        config.proxy_count = usize::try_from(self.proxies).expect("a few proxies");
        config.replica_count = usize::try_from(self.replicas).expect("a few replicas");
        config.writer_mode = self.writer_mode;
        config.quorum_system = self.quorum_system;
        config
    }

    fn named(net: u8, ids: impl Iterator<Item = u64>) -> Vec<(u64, String)> {
        ids.map(|id| (id, at(net, id + 1).to_string())).collect()
    }

    /// Run every role until `body` ends, then stop them all. `body` is
    /// given the client machine's providers and each pool node's own stop,
    /// in [`Deployment::pool`] order. Each role's exit, and `body`'s answer.
    // One role per block: the wiring of every role a deployment runs.
    #[allow(clippy::too_many_lines)]
    pub fn run<T>(
        &self,
        body: impl AsyncFnOnce(SimProviders, Vec<CancellationToken>) -> T,
    ) -> (Ends, T) {
        let mut rig = Rig::new();
        let client = rig.providers(CLIENT_IP);
        let everyone = CancellationToken::new();
        let pool = self.pool();
        let stops: Vec<CancellationToken> = pool.iter().map(|_| everyone.child_token()).collect();
        let members = crate::rig::pool(&pool);
        let matchmakers: Vec<(MatchmakerId, String)> =
            Self::named(2, 0..self.matchmakers + self.spare_matchmakers)
                .into_iter()
                .map(|(id, addr)| (MatchmakerId(id), addr))
                .collect();
        let proxies: Vec<(ProxyId, String)> = Self::named(3, 0..self.proxies)
            .into_iter()
            .map(|(id, addr)| (ProxyId(id), addr))
            .collect();
        let replicas: Vec<(NodeId, String)> = Self::named(4, 0..self.replicas)
            .into_iter()
            .map(|(rank, addr)| (replica_id(rank), addr))
            .collect();
        let mut roles: Vec<Role> = Vec::new();
        for (id, stop) in pool.iter().zip(&stops) {
            let providers = rig.node(*id);
            let storage = MemStorage::new(self.config(NodeId(*id)));
            let wiring = (
                members.clone(),
                matchmakers.clone(),
                proxies.clone(),
                replicas.clone(),
            );
            let (stop, tunables, id) = (stop.clone(), self.tunables, *id);
            roles.push(Box::pin(async move {
                let (members, matchmakers, proxies, replicas) = wiring;
                let end = crate::node::run_node(
                    providers,
                    storage,
                    addr(id).to_string(),
                    members,
                    matchmakers,
                    proxies,
                    replicas,
                    BootKind::FirstBoot,
                    tunables,
                    Transport::Plaintext,
                    stop,
                    &NoAudit,
                )
                .await;
                (format!("node {id}"), end)
            }));
        }
        let bootstrap = Self::matchmaker_ids(self.matchmakers);
        for (id, address) in &matchmakers {
            let providers = rig.providers(address.split(':').next().expect("an ip"));
            let config = MatchmakerConfig {
                id: *id,
                bootstrap: bootstrap.clone(),
            };
            let sets = vec![HostedSet {
                tenant: journal().tenant,
                journals: vec![journal().journal],
                storage: MemMatchmakerStorage::new(),
                boot: BootKind::FirstBoot,
            }];
            let (stop, tunables, address, matchmaker) =
                (everyone.child_token(), self.tunables, address.clone(), id.0);
            roles.push(Box::pin(async move {
                let end = crate::matchmaker::run_matchmaker(
                    providers,
                    sets,
                    address,
                    config,
                    tunables,
                    Transport::Plaintext,
                    stop,
                    |_| NoAudit,
                )
                .await;
                (format!("matchmaker {matchmaker}"), end)
            }));
        }
        let acceptors = self.config(NodeId(self.peers[0])).bootstrap();
        for (id, address) in &proxies {
            let providers = rig.providers(address.split(':').next().expect("an ip"));
            let config = ProxyConfig {
                id: *id,
                journals: vec![ProxyJournal {
                    journal: journal(),
                    acceptors: acceptors.clone(),
                }],
            };
            let wiring = (members.clone(), replicas.clone());
            let (stop, tunables, address, id) =
                (everyone.child_token(), self.tunables, address.clone(), id.0);
            roles.push(Box::pin(async move {
                let (members, replicas) = wiring;
                let end = crate::proxy_leader::run_proxy(
                    providers,
                    address,
                    config,
                    members,
                    replicas,
                    tunables,
                    Transport::Plaintext,
                    stop,
                    &NoAudit,
                    |_| NoAudit,
                )
                .await;
                (format!("proxy {id}"), end)
            }));
        }
        for (id, address) in &replicas {
            let providers = rig.providers(address.split(':').next().expect("an ip"));
            let stores = SingleStore {
                journal: journal(),
                store: Some((MemStorage::new(self.config(*id)), BootKind::FirstBoot)),
                audit: NoAudit,
            };
            let (stop, tunables, address, members, id) = (
                everyone.child_token(),
                self.tunables,
                address.clone(),
                members.clone(),
                id.0,
            );
            roles.push(Box::pin(async move {
                let end = crate::replica::run_replica(
                    providers,
                    stores,
                    address,
                    members,
                    tunables,
                    Transport::Plaintext,
                    stop,
                )
                .await;
                (format!("replica {id}"), end)
            }));
        }
        rig.run(async move {
            let answer = async {
                let answer = body(client, stops).await;
                everyone.cancel();
                answer
            };
            futures::join!(futures::future::join_all(roles), answer)
        })
    }
}

/// Every role of `ends` stopped cleanly.
pub fn clean(ends: Ends) {
    for (role, end) in ends {
        if let Err(error) = end {
            panic!("{role} ended with {error:?}");
        }
    }
}
