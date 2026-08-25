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

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimulationResult, StateHandle, TimeProvider, assert_always, buggify_knob,
    buggify_with_prob,
};

use crate::audit::{NodeAudit, audit_world};
use crate::chain::{AppliedTransition, ChainState, hash_text};
use paros::{
    Ballot, ClientId, ClientSeq, Command, Config, DriverHooks, HardState, MemStorage, Message,
    MustSync, NodeId, NodeStorage, Seam, SessionEntry, Slot, Storage, StorageError, is_seam_crash,
    parse_addr, run_node,
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
        // The per-iteration shared audit: pure observation, published beside the
        // storage world so every node folds its transitions into one incremental
        // checker. It never influences the driver — that is `hooks`' job.
        let audit = NodeAudit::new(ctx.time().clone(), audit_world(ctx.state()));

        // Recovery loop: a `buggify`-injected seam crash unwinds `run_node`, we
        // drop the volatile node, rebuild storage from the (surviving) world, and
        // re-run — a faithful clean crash + recovery. Attrition (process kill) is
        // handled by the harness; this covers the seams *inside* a Ready batch
        // that attrition cannot reach.
        loop {
            let storage = DurableStorage::restore(
                config.clone(),
                Arc::downgrade(&world),
                my_ip.clone(),
                self_rank.0,
            );
            match run_node(
                ctx.providers().clone(),
                storage,
                parse_addr(&my_ip)?,
                members.clone(),
                ctx.shutdown().clone(),
                &hooks,
                &audit,
            )
            .await
            {
                // Simulated crash at a durability seam: fall through to recover
                // and re-run (rebuilding volatile state from the durable world).
                // The restart delay is workload-buggified config (prong 2): a
                // real process does not restart in zero time, and a node held
                // down while the cluster keeps committing and truncating comes
                // back below the compaction floor — a second, independent
                // generator for snapshot recovery besides the attrition window.
                Err(e) if is_seam_crash(&e) => {
                    let delay_ms = buggify_knob!(0_u64, 250_u64..3_001_u64);
                    if delay_ms > 0 {
                        ctx.time().sleep(Duration::from_millis(delay_ms)).await.ok();
                    }
                }
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
    /// Application-produced snapshot state, durable across clean reboot.
    chain: ChainState,
    /// Sealed at-most-once ledger records for truncated slots (#94): read back
    /// on boot so a restart suppresses re-chosen identities like every peer.
    sealed: BTreeMap<(ClientId, ClientSeq), Slot>,
}

/// The per-iteration durable-storage world: every node's durable records, keyed
/// by IP. It is **protocol-blind** — it stores records, never knowing what is
/// committed. It outlives process crashes (owned by the `StateHandle`), so a
/// write that reached it before a crash is read back on restart, exactly like a
/// real disk. This is where a later storage-fault stage rolls seeded faults under
/// a cluster-wide budget.
#[derive(Default)]
struct StorageWorld {
    disks: BTreeMap<String, NodeDisk>,
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
    /// Stable numeric identity used only on application trace facts.
    node_id: u64,
    /// This incarnation's application state, including transitions staged for
    /// the next durability flush.
    application: ChainState,
    /// Writes staged since the last flush (lost if the incarnation is dropped).
    staged_ballot: Option<Ballot>,
    staged_accepted: BTreeMap<Slot, (Ballot, Command)>,
    staged_chosen: Option<Slot>,
    staged_floor: Option<Slot>,
    staged_snapshot: Option<ChainState>,
    staged_applies: Vec<PendingApply>,
    /// Sealed ledger records staged with a truncate / snapshot install (#94),
    /// flushed to the durable world with the rest of the batch.
    staged_sealed: Vec<SessionEntry>,
}

struct PendingApply {
    slot: Slot,
    transition: AppliedTransition,
}

impl DurableStorage {
    /// Build storage for `config`, seeding the read view from any durable records
    /// a prior boot of this node (same IP, same iteration) left in the world.
    fn restore(
        config: Config,
        world: Weak<Mutex<StorageWorld>>,
        key: String,
        node_id: u64,
    ) -> Self {
        let mut boot = MemStorage::new(config);
        let mut application = ChainState::default();
        if let Some(strong) = world.upgrade() {
            let guard = strong.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(disk) = guard.disks.get(&key) {
                application = disk.chain;
                // Seed the read view through the semantic ops (records, not a blob).
                // Set the floor first so first_slot() reads it back on boot; the
                // sealed ledger rides on the same op, exactly as a truncate
                // persisted it.
                let sealed: Vec<SessionEntry> = disk
                    .sealed
                    .iter()
                    .map(|(&(client, seq), &slot)| (client, seq, slot))
                    .collect();
                let _ = boot.truncate(disk.first_slot, &sealed);
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
            node_id,
            application,
            staged_ballot: None,
            staged_accepted: BTreeMap::new(),
            staged_chosen: None,
            staged_floor: None,
            staged_snapshot: None,
            staged_applies: Vec::new(),
            staged_sealed: Vec::new(),
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
        let snapshot = self.staged_snapshot.take();
        let applies = std::mem::take(&mut self.staged_applies);
        let sealed = std::mem::take(&mut self.staged_sealed);
        self.with_disk(|d| {
            // Sealed ledger records are upserts keyed by (client, seq); the
            // first-slot claim wins, matching the core's ledger semantics.
            for (client, seq, slot) in sealed {
                d.sealed.entry((client, seq)).or_insert(slot);
            }
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
            if let Some(installed) = snapshot {
                d.chain = installed;
            }
            if let Some(last) = applies.last() {
                d.chain = last.transition.next;
            }
        })?;

        if let Some(installed) = snapshot {
            tracing::info!(
                node = self.node_id,
                index = installed.applied_count,
                state = %hash_text(installed.chain_hash),
                "chain_snapshot_installed"
            );
        }
        for pending in applies {
            let next = pending.transition.next;
            tracing::info!(
                target: "chain",
                node = self.node_id,
                slot = pending.slot.0,
                index = next.applied_count,
                cmd = %hash_text(pending.transition.cmd_hash),
                state = %hash_text(next.chain_hash),
                kind = pending.transition.kind,
                "command_applied"
            );
        }
        Ok(())
    }

    fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        // Stage the floor like every other write: it reaches the durable world
        // only on the next Sync flush (Truncate classifies as MustSync::Sync).
        // The sealed ledger records ride in the same staged batch.
        self.staged_floor = Some(self.staged_floor.map_or(first, |f| f.max(first)));
        self.staged_sealed.extend_from_slice(sealed);
        Ok(())
    }

    fn snapshot(&self) -> Vec<u8> {
        self.application.encode()
    }

    fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        // Stage the install like every other write (InstallSnapshot is
        // MustSync::Sync): the chosen index, the adopted ballot, the floor
        // (`chosen_index + 1`), and the serving peer's session ledger reach the
        // durable world on the next Sync flush, where the floor is applied last
        // so it never outruns the chosen index.
        self.staged_sealed.extend_from_slice(sessions);
        self.staged_chosen = Some(chosen_index);
        self.staged_ballot = Some(ballot);
        let first = Slot(chosen_index.0 + 1);
        self.staged_floor = Some(self.staged_floor.map_or(first, |f| f.max(first)));
        let installed = ChainState::decode(&snapshot).map_err(StorageError::Corruption)?;
        assert_always!(
            installed.applied_slot() == Some(chosen_index),
            "chain: snapshot state matches its boundary"
        );
        assert_always!(
            installed.applied_count >= self.application.applied_count,
            "chain: snapshot install does not regress state"
        );
        self.application = installed;
        self.staged_snapshot = Some(installed);
        Ok(())
    }

    fn apply(
        &mut self,
        chosen_index: Slot,
        slot: Slot,
        command: &Command,
    ) -> Result<(), StorageError> {
        assert_always!(
            slot <= chosen_index,
            "chain: apply does not outrun chosen prefix"
        );
        if self
            .application
            .applied_slot()
            .is_some_and(|applied| slot <= applied)
        {
            return Ok(());
        }
        let expected = self
            .application
            .applied_slot()
            .map_or(Slot(0), |applied| Slot(applied.0.saturating_add(1)));
        if slot != expected {
            eprintln!(
                "CONTIGUITY: node={} slot={} expected={} chosen_index={} applied_count={}",
                self.node_id, slot.0, expected.0, chosen_index.0, self.application.applied_count
            );
        }
        assert_always!(
            slot == expected,
            "chain: local application transition is contiguous"
        );
        let transition = self.application.apply(command);
        self.application = transition.next;
        self.staged_applies.push(PendingApply { slot, transition });
        Ok(())
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.application.applied_slot()
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
    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.boot.sealed_sessions()
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
                Seam::AfterApplyBeforeSync => buggify_with_prob!(0.03),
            }
    }

    fn skip_accept_resend(&self) -> bool {
        self.active() && buggify_with_prob!(0.95)
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }

    fn shortest_election_timeout(&self) -> bool {
        self.active() && buggify_with_prob!(0.5)
    }

    fn drop_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Three locations, selected independently per seed: an isolated
        // `Accept` loss is the interleaving behind a stranded chosen-gap wedge
        // (#80) — one earlier slot's Accept vanishes while later slots land —
        // while losing a `Promise`/`Prepare` stretches elections open, and a
        // lost `Nack` keeps a below-floor candidate's campaign alive long
        // enough for the answering snapshot to land mid-election (the
        // truncated-quorum Nack otherwise steps the candidate down before the
        // `CatchUpRequest`'s snapshot offer arrives — the #88 window).
        match msg {
            Message::Accept { .. } => buggify_with_prob!(0.05),
            Message::Prepare { .. } | Message::Promise { .. } => buggify_with_prob!(0.10),
            Message::Nack { .. } => buggify_with_prob!(0.25),
            // A dropped `Commit` delays a follower's floor-raise (truncation
            // applies lazily at its Truncate slot), widening the mixed-floor
            // window the #88 mid-election snapshot needs — and leaves the
            // follower hole commit-replay catch-up must heal (#80's terrain).
            Message::Commit { .. } => buggify_with_prob!(0.05),
            // The lost *ack*: a slot durably accepted by a quorum whose
            // proposer never learns it — the pure quorum-intersection edge
            // that forces a re-propose under a new ballot (P2c for real).
            Message::Accepted { .. } => buggify_with_prob!(0.05),
            // Starve the read fence / the catch-up push direction. Kept low:
            // these fire per tick per peer, and a high rate is just a
            // partition, which is moonpool's job.
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => buggify_with_prob!(0.02),
            // Repair traffic for a node that is already behind: a lost
            // response costs one beat of latency and re-derives on the next.
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn duplicate_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Moonpool has no message-duplication fault, so this seam is the only
        // duplicate generator. The quorum-counting kinds are the point of the
        // location: every quorum in the core is set-based today, and this
        // keeps a future "optimization" into counters from fabricating a
        // quorum out of a duplicated ack.
        match msg {
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. } => {
                buggify_with_prob!(0.05)
            }
            Message::Commit { .. } => buggify_with_prob!(0.05),
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            _ => false,
        }
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        if !self.active() {
            return false;
        }
        // Dropped *after* the server committed/applied: the client's retry
        // must take the `(client, seq)` dedup path, the at-most-once edge the
        // truncated-dedup-window hazard lives on. One location per reply kind.
        match reply {
            paros::Reply::Propose => buggify_with_prob!(0.10),
            paros::Reply::ProposeDedup => buggify_with_prob!(0.10),
            paros::Reply::Read => buggify_with_prob!(0.10),
        }
    }
}
