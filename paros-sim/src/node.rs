//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this just bridges the sim boundary —
//! it derives a cluster-consistent membership from the topology, then pulls the
//! providers, local address, and shutdown token out of [`SimContext`] and hands
//! them to the same `run_node` a production `tokio::main` would call.

use std::net::IpAddr;

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationResult, StateHandle};
use paros::{
    Ballot, Config, Entry, HardState, MemStorage, NodeId, NodeStorage, Slot, Storage, parse_addr,
    run_node,
};

/// A paros node in the simulation.
pub struct NodeProcess;

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Build the full cluster membership. `all_process_ips()` excludes this
        // node, so add `my_ip` and sort numerically: every node derives the
        // *same* ordered list, so `NodeId(i) <-> ips[i]` is consistent
        // cluster-wide without any coordination.
        let my_ip = ctx.my_ip().to_string();
        let mut ips: Vec<String> = ctx.topology().all_process_ips().to_vec();
        ips.push(my_ip.clone());
        ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
        ips.dedup();

        let members = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| {
                parse_addr(ip)
                    .map(|addr| (NodeId(u64::try_from(i).expect("node index fits u64")), addr))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

        let self_rank = NodeId(
            u64::try_from(
                ips.iter()
                    .position(|ip| ip == &my_ip)
                    .expect("self is a member"),
            )
            .expect("node index fits u64"),
        );
        let config = Config {
            id: self_rank,
            peers: members.iter().map(|(id, _)| *id).collect(),
        };

        // Durable storage that survives a chaos `Crash`/restart: it mirrors the
        // node's `HardState` into the per-iteration `StateHandle` (shared across a
        // process's reboots, fresh per seed), keyed by this node's IP. This is the
        // sim's stand-in for real durable disk; the storage stage swaps in a faulty
        // fake without touching the driver.
        let storage = DurableStorage::restore(
            config,
            ctx.state().clone(),
            format!("paros-hardstate:{my_ip}"),
        );

        run_node(
            ctx.providers().clone(),
            storage,
            parse_addr(&my_ip)?,
            members,
            ctx.shutdown().clone(),
        )
        .await
    }
}

/// A [`NodeStorage`] whose durable [`HardState`] is mirrored into the moonpool
/// per-iteration [`StateHandle`], keyed by the node's IP. The `StateHandle` is
/// shared across a process's reboots within an iteration (and fresh per seed), so
/// state written before a chaos `Crash` is read back on restart, exactly like a
/// real disk, while staying deterministic across the seed sweep.
struct DurableStorage {
    inner: MemStorage,
    state: StateHandle,
    key: String,
}

impl DurableStorage {
    /// Build storage for `config`, seeding it from any [`HardState`] a prior boot
    /// of this node (same IP, same iteration) persisted into `state`.
    fn restore(config: Config, state: StateHandle, key: String) -> Self {
        let mut inner = MemStorage::new(config);
        if let Some(hard_state) = state.get::<HardState>(&key) {
            inner.set_hard_state(hard_state);
        }
        Self { inner, state, key }
    }
}

impl Storage for DurableStorage {
    fn initial_state(&self) -> (HardState, Config) {
        self.inner.initial_state()
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Entry)> {
        self.inner.accepted(slot)
    }
    fn first_slot(&self) -> Slot {
        self.inner.first_slot()
    }
    fn last_slot(&self) -> Slot {
        self.inner.last_slot()
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        self.inner.snapshot()
    }
}

impl NodeStorage for DurableStorage {
    fn set_hard_state(&mut self, hard_state: HardState) {
        // Persist to the per-iteration StateHandle FIRST (survives restart), then
        // update the in-memory mirror the driver reads back.
        self.state.publish(&self.key, hard_state.clone());
        self.inner.set_hard_state(hard_state);
    }
}
