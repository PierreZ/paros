//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this bridges the sim boundary. It
//! derives a cluster-consistent membership from the topology, wires the node to a
//! per-node handle on the shared [`StorageWorld`] (the sim's stand-in for durable
//! disk), and runs the same `run_node` a production `tokio::main` would — inside a
//! recovery loop that turns a `buggify`-injected seam crash into a real
//! crash+restart: `run_node` unwinds, the volatile `RawNode` is dropped, and the
//! next iteration rebuilds it from the durable [`StorageWorld`].

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, StateHandle, TimeProvider, buggify_with_prob,
};
use paros::{
    Ballot, Command, Config, DriverHooks, HardState, MemStorage, MustSync, NodeId, NodeStorage,
    Seam, Slot, Storage, StorageError, is_seam_crash, parse_addr, run_node,
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
        // surviving crash/restart (it lives in the `StateHandle`, fresh per seed
        // but stable across a process's reboots). Each node reaches it through a
        // `Weak` handle upgraded per op.
        let world = storage_world(ctx.state());
        let hooks = BuggifyHooks {
            time: ctx.time().clone(),
            cutoff: Duration::from_millis(crate::CHAOS_DURATION_MS),
        };

        // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
        // drop the volatile node, rebuild storage from the (surviving) world, and
        // re-run — a faithful clean crash + recovery. Attrition (process kill) is
        // handled by the harness; this covers the seams *inside* a Ready batch
        // that attrition cannot reach.
        loop {
            let storage =
                DurableStorage::restore(config.clone(), Arc::downgrade(&world), my_ip.clone());
            match run_node(
                ctx.providers().clone(),
                storage,
                parse_addr(&my_ip)?,
                members.clone(),
                ctx.shutdown().clone(),
                &hooks,
            )
            .await
            {
                // Simulated crash at a durability seam: fall through to recover
                // and re-run (rebuilding volatile state from the durable world).
                Err(e) if is_seam_crash(&e) => {}
                other => return other,
            }
        }
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

/// One node's durable records: the scalars, the per-slot accepted log, and the
/// compaction floor. The [`StorageWorld`] owns one of these per node IP.
#[derive(Default)]
struct NodeDisk {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// The first slot still retained. Everything below it has been truncated.
    first_slot: Slot,
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
/// reads storage once, at boot.
///
/// Writes stage locally and reach the durable world only on a
/// [`sync`](NodeStorage::sync): a [`MustSync::Sync`] batch flushes the stage
/// through (fsync); a [`MustSync::Relaxed`] batch leaves it staged, so it is lost
/// if the incarnation is dropped before a later sync. Because the stage lives in
/// this handle (dropped when `run_node` unwinds on a seam crash), a crash *before*
/// the fsync loses the whole un-synced batch — a faithful clean crash.
struct DurableStorage {
    /// Read view: this node's durable records as of boot.
    boot: MemStorage,
    /// The shared world, upgraded per op.
    world: Weak<Mutex<StorageWorld>>,
    /// This node's IP — its key into the world.
    key: String,
    /// Writes staged since the last flush (lost if the incarnation is dropped).
    staged_ballot: Option<Ballot>,
    staged_accepted: BTreeMap<Slot, (Ballot, Command)>,
    staged_chosen: Option<Slot>,
    staged_floor: Option<Slot>,
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
                // Set the floor first so first_slot() reads it back on boot.
                let _ = boot.truncate(disk.first_slot);
                let _ = boot.persist_ballot(disk.hard_state.max_promised_ballot);
                for (slot, (ballot, command)) in &disk.accepted {
                    let _ = boot.append_accepted(*slot, *ballot, command.clone());
                }
                if let Some(ci) = disk.hard_state.chosen_index {
                    let _ = boot.set_chosen_index(ci);
                }
                let _ = boot.sync(MustSync::Sync);
            }
        }
        Self {
            boot,
            world,
            key,
            staged_ballot: None,
            staged_accepted: BTreeMap::new(),
            staged_chosen: None,
            staged_floor: None,
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
        self.staged_ballot = Some(ballot);
        Ok(())
    }

    fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.staged_accepted.insert(slot, (ballot, command));
        Ok(())
    }

    fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.staged_chosen = Some(slot);
        Ok(())
    }

    fn sync(&mut self, must_sync: MustSync) -> Result<(), StorageError> {
        // A relaxed (chosen-index-only) batch keeps its stage un-flushed: it is
        // durable only once a later Sync flushes it, and lost on a crash before
        // then. A Sync batch flushes the whole stage through to the world.
        if must_sync != MustSync::Sync {
            return Ok(());
        }
        let ballot = self.staged_ballot.take();
        let accepted = std::mem::take(&mut self.staged_accepted);
        let chosen = self.staged_chosen.take();
        let floor = self.staged_floor.take();
        self.with_disk(|d| {
            if let Some(b) = ballot {
                // The promise is monotonic: never let a flush lower it. A
                // SetPromise write only ever raises it, but an InstallSnapshot
                // carries the *server's* ballot, which can be below this node's own
                // promise, so take the max (matching `MemStorage::install_snapshot`).
                d.hard_state.max_promised_ballot = d.hard_state.max_promised_ballot.max(b);
            }
            for (slot, record) in accepted {
                d.accepted.insert(slot, record);
            }
            if let Some(c) = chosen {
                d.hard_state.chosen_index = Some(c);
            }
            // Apply the truncation last, after the chosen index it sits behind, so
            // a flushed floor never outruns the flushed chosen index.
            if let Some(f) = floor {
                d.first_slot = d.first_slot.max(f);
                d.accepted.retain(|s, _| *s >= d.first_slot);
            }
        })
    }

    fn truncate(&mut self, first: Slot) -> Result<(), StorageError> {
        // Stage the floor like every other write: it reaches the durable world
        // only on the next Sync flush (Truncate classifies as MustSync::Sync).
        self.staged_floor = Some(self.staged_floor.map_or(first, |f| f.max(first)));
        Ok(())
    }

    fn snapshot(&self) -> Vec<u8> {
        // Opaque marker of the chosen prefix (the sim has no application state
        // machine); the boot read view carries this node's durable chosen index.
        self.boot.snapshot()
    }

    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        _snapshot: Vec<u8>,
    ) -> Result<(), StorageError> {
        // Stage the install like every other write (InstallSnapshot is
        // MustSync::Sync): the chosen index, the adopted ballot, and the floor
        // (`chosen_index + 1`) reach the durable world on the next Sync flush,
        // where the floor is applied last so it never outruns the chosen index.
        self.staged_chosen = Some(chosen_index);
        self.staged_ballot = Some(ballot);
        let first = Slot(chosen_index.0 + 1);
        self.staged_floor = Some(self.staged_floor.map_or(first, |f| f.max(first)));
        Ok(())
    }
}

impl Storage for DurableStorage {
    fn initial_state(&self) -> (HardState, Config) {
        self.boot.initial_state()
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.boot.accepted(slot)
    }
    fn first_slot(&self) -> Slot {
        self.boot.first_slot()
    }
    fn last_slot(&self) -> Slot {
        self.boot.last_slot()
    }
}

/// Simulation hooks for driver decisions that process-level attrition cannot
/// reach. Every behavior has its own `BUGGIFY` location, so activation is
/// independent and replayable. All hooks turn off with the chaos window, leaving
/// the settle tail genuinely quiet for convergence.
struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    fn active(&self) -> bool {
        self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn crash_at(&self, seam: Seam) -> bool {
        self.active()
            && match seam {
                Seam::BeforeSync => buggify_with_prob!(0.03),
                Seam::AfterSyncBeforeSend => buggify_with_prob!(0.03),
            }
    }

    fn skip_accept_resend(&self) -> bool {
        self.active() && buggify_with_prob!(0.95)
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }
}
