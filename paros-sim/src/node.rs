//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. It
//! derives a cluster-consistent membership from the topology, then wires the
//! node to a per-node handle on the shared [`StorageWorld`] — the sim's stand-in
//! for durable disk — before handing the providers, address, and shutdown token
//! to the same `run_node` a production `tokio::main` would call.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationResult, StateHandle};
use paros::{
    Ballot, Config, Entry, HardState, MemStorage, MustSync, NodeId, NodeStorage, Slot, Storage,
    StorageError, parse_addr, run_node,
};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";

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
            ..Config::default()
        };

        // The per-iteration durable-storage world, shared by every node and
        // surviving chaos crash/restart (it lives in the `StateHandle`, which is
        // fresh per seed but stable across a process's reboots). Each node reaches
        // it through a `Weak` handle upgraded per op.
        let world = storage_world(ctx.state());
        let storage = DurableStorage::restore(config, Arc::downgrade(&world), my_ip.clone());

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

/// Get-or-create the singleton [`StorageWorld`] for this iteration. Get-then-
/// publish is race-free: the sim executor is single-threaded and this runs
/// synchronously (no `.await` between the get and the publish).
fn storage_world(state: &StateHandle) -> Arc<Mutex<StorageWorld>> {
    if let Some(world) = state.get::<Arc<Mutex<StorageWorld>>>(STORAGE_WORLD_KEY) {
        return world;
    }
    let world = Arc::new(Mutex::new(StorageWorld::default()));
    state.publish(STORAGE_WORLD_KEY, world.clone());
    world
}

/// One node's durable records: the scalars, the per-slot accepted log, and an
/// optional snapshot blob. The [`StorageWorld`] owns one of these per node IP.
#[derive(Default)]
struct NodeDisk {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Entry)>,
    snapshot: Option<Vec<u8>>,
}

/// The per-iteration durable-storage world: every node's durable records, keyed
/// by IP. It is **protocol-blind** — it stores records, never knowing what is
/// committed. It outlives process crashes (owned by the `StateHandle`), so a
/// write that reached it before a crash is read back on restart, exactly like a
/// real disk. This is where a later storage-fault stage rolls seeded faults under
/// a cluster-wide budget.
#[derive(Default)]
struct StorageWorld {
    disks: HashMap<String, NodeDisk>,
}

/// A [`NodeStorage`] handle onto one node's slice of the shared [`StorageWorld`].
///
/// It holds a `Weak` to the world, upgraded per op (moonpool's "world held via
/// Weak, upgraded per op" convention). Reads are served from `boot` — a snapshot
/// of this node's durable records taken at construction — because the core only
/// reads storage once, at boot. Writes go to the world: promise-raises and
/// accepted-appends are written straight through (they are always in a
/// [`MustSync::Sync`] batch and must survive a crash); a chosen-index advance is
/// staged locally and only reaches the world on a `sync(Sync)`, so a relaxed
/// chosen-index-only advance is lost on crash — the [`MustSync`] payoff.
struct DurableStorage {
    /// Read view: this node's durable records as of boot.
    boot: MemStorage,
    /// The shared world, upgraded per op.
    world: Weak<Mutex<StorageWorld>>,
    /// This node's IP — its key into the world.
    key: String,
    /// A chosen-index advance staged by a relaxed batch, not yet fsync'd (lost on
    /// crash unless a later `sync(Sync)` flushes it).
    unsynced_chosen_index: Option<Slot>,
}

impl DurableStorage {
    /// Build storage for `config`, seeding the read view from any durable records
    /// a prior boot of this node (same IP, same iteration) left in the world.
    fn restore(config: Config, world: Weak<Mutex<StorageWorld>>, key: String) -> Self {
        let mut boot = MemStorage::new(config);
        if let Some(strong) = world.upgrade() {
            let guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(disk) = guard.disks.get(&key) {
                // Seed the read view through the semantic ops (records, not a blob).
                let _ = boot.persist_ballot(disk.hard_state.max_promised_ballot);
                for (slot, (ballot, entry)) in &disk.accepted {
                    let _ = boot.append_accepted(*slot, *ballot, entry.clone());
                }
                if let Some(ci) = disk.hard_state.chosen_index {
                    let _ = boot.set_chosen_index(ci);
                }
            }
        }
        Self {
            boot,
            world,
            key,
            unsynced_chosen_index: None,
        }
    }

    /// Run `f` against this node's durable disk in the shared world.
    fn with_disk<R>(&self, f: impl FnOnce(&mut NodeDisk) -> R) -> Result<R, StorageError> {
        let strong = self
            .world
            .upgrade()
            .ok_or_else(|| StorageError::Io("storage world dropped".into()))?;
        let mut guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(f(guard.disks.entry(self.key.clone()).or_default()))
    }
}

impl NodeStorage for DurableStorage {
    fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.with_disk(|d| d.hard_state.max_promised_ballot = ballot)
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        entry: Entry,
    ) -> Result<(), StorageError> {
        self.with_disk(|d| {
            d.accepted.insert(slot, (ballot, entry));
        })
    }

    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        // Stage locally; a relaxed batch leaves it unsynced (lost on crash), a
        // `sync(Sync)` flushes it to the world.
        self.unsynced_chosen_index = Some(slot);
        Ok(())
    }

    fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        if must_sync == MustSync::Sync
            && let Some(slot) = self.unsynced_chosen_index.take()
        {
            self.with_disk(|d| d.hard_state.chosen_index = Some(slot))?;
        }
        Ok(())
    }

    fn install_snapshot(&mut self, up_to: Slot, bytes: &[u8]) -> Result<(), StorageError> {
        let bytes = bytes.to_vec();
        self.with_disk(|d| {
            d.snapshot = Some(bytes);
            d.accepted.retain(|s, _| *s > up_to);
        })
    }

    fn truncate(&mut self, first: Slot) -> Result<(), StorageError> {
        self.with_disk(|d| d.accepted.retain(|s, _| *s >= first))
    }
}

impl Storage for DurableStorage {
    fn initial_state(&self) -> (HardState, Config) {
        self.boot.initial_state()
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Entry)> {
        self.boot.accepted(slot)
    }
    fn first_slot(&self) -> Slot {
        self.boot.first_slot()
    }
    fn last_slot(&self) -> Slot {
        self.boot.last_slot()
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        self.boot.snapshot()
    }
}
