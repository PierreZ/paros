//! The acceptor role: `run_acceptor`, its recovery loop, and the system journals' rig.

use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{
    SimContext, SimTimeProvider, SimulationError, SimulationResult, TimeProvider, assert_always,
    assert_reachable,
};

use super::Down;
use super::RoleRig;
use super::arm_role;
use super::ranked;
use super::replica_book;
use super::stay_down;
use super::stores::SimStores;
use super::stores::journal_dir;
use super::stores::resolve_provisioning;
use crate::audit::journals::{journal_board, lock as board_lock};
use crate::audit::{AuditWorld, NodeAudit, audit_world_for};
use crate::roles::Deployment;
use crate::world::{ParkReason, StorageWorld, storage_world_for};
use paros::{
    BootRefusal, Config, ControlPlan, MatchmakerId, NodeId, ProxyId, RunError, parse_addr,
    run_journals,
};

/// An acceptor: the provider-generic node driver inside the crash/recovery
/// loop (see the module doc).
// One recovery loop with per-exit-kind handling; splitting the arms would
// scatter the crash/park/restart contract this function *is*.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "debug", skip_all, fields(node = self_rank.0))]
pub(super) async fn run_acceptor(
    ctx: &SimContext,
    deployment: &Deployment,
    self_rank: NodeId,
    my_ip: &str,
) -> SimulationResult<()> {
    // The node pool is the map's acceptor list, in `NodeId` order — never
    // "every process in the topology". The matchmaker set is the map's
    // matchmaker list, empty on a plain seed. The bootstrap membership is
    // protocol data drawn once per seed (`crate::shape::bootstrap_ranks`):
    // the whole pool by default, and on a matchmaker seed possibly a subset
    // that leaves spares for a reconfiguration to pull in.
    let members = ranked(deployment.acceptors(), NodeId)?;
    let matchmakers = ranked(deployment.matchmakers(), MatchmakerId)?;
    // The proxy leaders (#142): the map's proxy list, whose length is the
    // `Config`'s `proxy_count` — zero on a seed without proxies, the plain
    // deployment whose every Phase 2 stays colocated.
    let proxies = ranked(deployment.proxies(), ProxyId)?;
    // The replica tier (#144): the learners outside the pool, each under
    // its wire `NodeId`; empty on a seed without replicas.
    let replicas = replica_book(deployment)?;
    let pool: Vec<NodeId> = members.iter().map(|(id, _)| *id).collect();
    let bootstrap: Vec<NodeId> =
        crate::shape::bootstrap_ranks(ctx.state(), pool.len(), !matchmakers.is_empty())
            .into_iter()
            .map(NodeId)
            .collect();
    // The matchmaker *pool* is the address book (`matchmakers`, every
    // matchmaker process); the bootstrap matchmaker set (#125) is protocol
    // data drawn once per seed, possibly a subset that leaves spares.
    let matchmaker_pool: Vec<MatchmakerId> = matchmakers.iter().map(|(id, _)| *id).collect();
    let matchmaker_bootstrap: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_pool.len())
            .into_iter()
            .map(MatchmakerId)
            .collect();
    // The quorum system is protocol data too (#140): the run's policy, drawn
    // once per seed, applied to the bootstrap configuration's own size. A
    // majority on a plain seed or one the swarm leaves alone; a flexible split on the seeds
    // the swarm turns it on for.
    let policy = crate::shape::quorum_policy(ctx.state(), pool.len());
    let quorum_system = policy.system(bootstrap.len());
    let config = Config {
        journal: crate::shape::identifiers(ctx.state()).main,
        id: self_rank,
        peers: bootstrap,
        quorum_system,
        nodes: pool,
        matchmakers: matchmaker_bootstrap,
        matchmaker_pool,
        proxy_count: proxies.len(),
        replica_count: deployment.replica_count(),
        writer_mode: paros::WriterMode::Single,
    };
    // The run's journals (#188): the static list every node serves — the
    // default journal alone unless the deployment is plain and the seed drew
    // more — and the one held on every node for the chaos window, if any.
    let plan = crate::shape::journals(ctx.state());
    // The store: the library's `JournalStorage` on the simulated disk
    // (#187, #176, #261), its layout drawn once per seed.
    let layout = crate::shape::journal_layout(ctx.state());
    let board = journal_board(ctx.state());
    board_lock(&board).arm(&plan);

    // This node's rig: every knob the swarm draws *for the node* (the driver
    // tunables, the write-window crash bias, the disk's fault rates), drawn
    // by its first incarnation of the seed and handed back unchanged to
    // every later one. Armed here, after the registry draws above, so the
    // seed's draw schedule keeps its order.
    let RoleRig {
        incarnation,
        checker: _,
        audit: _,
    } = arm_role(ctx, my_ip);
    // The seed's scenarios decide the driver's named BUGGIFY locations
    // (`paros::scenario`) before the node's first beat; each draw is fixed
    // by its first caller.
    crate::shape::withhold_gc(ctx.state());
    crate::shape::lost_verdict(ctx.state());
    crate::shape::lagging_fold(ctx.state());
    let shape = incarnation.shape;
    // The copy budget is sized by the run's configuration floor
    // (`crate::shape::config_floor`): the whole pool on a plain seed, the
    // smallest set a reconfiguration may shrink to on a matchmaker seed —
    // and by the clean copies the run's quorum-system policy demands over
    // every size the run may put in force, floor to pool (a majority, the
    // split's Phase-1 quorum, or — on a grid seed — the whole floor: a grid
    // tolerates no permanent loss). The storage world's budget and the
    // audit's restart note below read the same two numbers.
    // One seat per journal (#188): its own configuration, its own
    // per-iteration durable world (shared by every node's copy of the
    // journal, surviving crash/restart, reached through a `Weak` handle
    // upgraded per op), its own audit world and audit port. Nothing crosses:
    // a journal's budget, fault ledger, parked identities and oracles are
    // its own. The default journal is the seed's deployment (its
    // matchmakers, proxies, replicas and bootstrap); every other journal is
    // plain Multi-Paxos over the whole pool — the matchmaker plane, the
    // proxy leaders and the replica tier each serve one journal.
    let mut seats: Vec<Seat> = plan
        .ids
        .iter()
        .map(|&journal| {
            let config = if journal == plan.main {
                config.clone()
            } else {
                Config {
                    journal,
                    peers: config.pool().to_vec(),
                    quorum_system: policy.system(config.pool().len()),
                    matchmakers: Vec::new(),
                    matchmaker_pool: Vec::new(),
                    proxy_count: 0,
                    replica_count: 0,
                    writer_mode: plan.mode(journal),
                    ..config.clone()
                }
            };
            let floor = crate::shape::config_floor(config.pool().len(), config.has_matchmakers());
            let clean_copies = policy.clean_copies(floor, config.pool().len());
            let world = storage_world_for(ctx.state(), journal);
            {
                let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                guard.set_budget(floor, clean_copies);
                // The pool above that floor is the retirement budget (#123):
                // every identity a configuration may leave behind.
                guard.set_pool_size(config.pool().len());
            }
            let checker = audit_world_for(ctx.state(), journal);
            let audit = NodeAudit::new(ctx.time().clone(), checker.clone())
                .in_journal(journal, board.clone());
            Seat {
                journal,
                config,
                world,
                checker,
                audit,
                floor,
                clean_copies,
                quiet: false,
                created: false,
                deleted: false,
            }
        })
        .collect();
    // The system journals (#189), on a seed that drew them: every node
    // follows the registry, and the seeds — the lowest
    // ranks — host them, a static configuration of plain Multi-Paxos on a
    // fault-free disk.
    let system = crate::shape::system_journals(ctx.state())
        .then(|| system_rig(ctx, deployment, &members, &mut seats, self_rank));
    let tunables = shape.tunables;
    if incarnation.is_restart() {
        // A process-level revival (attrition, or the chain client's
        // scripted reboot). Told to every journal's audit so it can
        // judge the overlap this boot may be ending: a node that was down
        // while a peer sat terminally parked (persistent storage loss +
        // transient process loss at once) is the composition that costs a
        // small cluster its quorum until exactly this boot returns it.
        for seat in &seats {
            let parked_peers = seat
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .parked_count_excluding(my_ip);
            seat.checker.note_process_restart(
                self_rank.0,
                parked_peers,
                seat.floor,
                seat.clean_copies,
            );
        }
        // The disk's wipe coin (#124): a restart that comes back on an empty
        // disk. Moonpool's own `prob_wipe` wipes without asking the copy
        // budget, so the amnesia fault is the ledger's, drawn here at the one
        // place a lost disk shows — a reboot — and executed on the simulated
        // disk (`world::wipe`). What happens next is the **library's**
        // call (#147): the identity boots below as an existing member on an
        // empty store, and `run_node` refuses the amnesiac store. A wiped
        // node is replaced through an acceptor reconfiguration, never
        // rejoined (an empty disk under an old identity would answer a
        // Phase 1 with "nothing accepted" for slots it voted on). Only a
        // matchmaker deployment can replace, so the coin is dark on a plain
        // seed, and on a seed serving several journals (#201): the wiped
        // node stays down for every journal it serves, and only the default
        // one can replace it. The world's dead-node budget bounds it either
        // way.
        let wipe = config.has_matchmakers()
            && seats.len() == 1
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.wipe_pct) / 100.0);
        if wipe
            && seats[0]
                .world
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .wipe(my_ip, self_rank.0)
        {
            // The disk is genuinely emptied: the journal's files go,
            // durably (#176).
            crate::world::wipe::wipe_dir(ctx.storage(), &journal_dir(seats[0].journal)).await;
            assert_reachable!("journal store: a wiped node's journal is deleted");
            // BUGGIFY pairing: the wipe coin fired within the budget.
            assert_reachable!("storage: a restarted node's disk is wiped and the identity retired");
            tracing::info!(node = self_rank.0, "storage_wiped");
        }
    }
    // #207: the operator restarts this node with an edited configuration
    // file — a peer dropped from the bootstrap membership — and the library
    // refuses the store that was formatted under the old one; the operator
    // restores the file and restarts (the `ConfigMismatch` arm below). Only
    // a node whose every journal it serves is the one seat, on a seed
    // without system journals: a refused journal is down for good on a node
    // that still serves another, and the operator's correction is a restart
    // of the whole process. The edit is applied below, inside the loop and
    // only to an identity the provisioning ledger knows: a first boot under
    // an edited file would format the edit.
    let mut edit = OperatorEdit::new(
        incarnation.is_restart()
            && seats.len() == 1
            && system.is_none()
            && seats[0].config.peers.len() > 1
            && ctx.time().now() < crate::CHAOS_DURATION
            && moonpool_sim::buggify_with_prob!(f64::from(shape.config_edit_pct) / 100.0),
    );

    // Recovery loop: a fail-stop storage fault unwinds the driver, we drop
    // the volatile nodes, rebuild storage from the (surviving) worlds, and
    // re-run, as `parosd`'s supervisor restarts it. A process kill
    // (attrition, or a driver `hint!` the regime strikes) is handled by the
    // harness: a fresh factory instance boots. A journal that is down for
    // good on this node — retired by the operator (#123), or terminally
    // parked by a detected persistent corruption — is one the opener
    // declines (`SimStores::open`), and a node with every journal down
    // exits cleanly: it stays down. A **wiped** identity (#124) is not on
    // that list: it boots, and the library refuses it (#147, below).
    loop {
        resolve_provisioning(ctx, &seats, my_ip).await;
        let provisioned = seats[0]
            .world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provisioned(my_ip);
        if edit.apply(provisioned, &mut seats[0].config, |config| {
            config.peers.pop();
        }) {
            // BUGGIFY pairing: the operator's edit genuinely reaches a boot.
            assert_reachable!("operator: a node restarts under an edited configuration");
            tracing::info!(node = self_rank.0, "config_edited");
        }
        let stores = SimStores {
            ctx,
            seats: &mut seats,
            ip: my_ip,
            rank: self_rank.0,
            journal_store: (ctx.storage().clone(), layout),
            system: system.as_ref().map(|(_, board)| board.clone()),
        };
        // Boxed: the node loop's future is large (every arm's state lives
        // in it), and this incarnation loop awaits it on its own identifier.
        match Box::pin(run_journals(
            ctx.providers().clone(),
            stores,
            self_rank,
            parse_addr(my_ip)?,
            members.clone(),
            matchmakers.clone(),
            proxies.clone(),
            replicas.clone(),
            system.as_ref().map(|(plan, _)| plan.clone()),
            None,
            tunables,
            ctx.shutdown().clone(),
        ))
        .await
        {
            // An injected storage fault surfaced as the driver's typed
            // crash decision (issue #19 A) and took the node's last live
            // journal (#188: a fault quarantines its journal; a node left
            // with none is the fail-stop crash), so the node re-enters the
            // same Stage-4 crash/restart path — the next iteration boots from
            // whatever the disks *actually* hold, which is how an ambiguous
            // write's two possible outcomes both resolve. Its restart delay
            // is its own independent BUGGIFY location.
            Err(RunError::Storage(_)) => {
                // Stage 7's baseline for a *persistent* detected fault —
                // a rotted record or an FS-metadata fault — is detect ⇒
                // crash, and restarting cannot help: the boot scan would
                // re-detect the same record forever. The journal stays down
                // on this node for the run (the availability disaster the
                // CTRL paper measures; Stage 8 buys it back), bounded by its
                // world's dead-node budget so the cluster keeps a live
                // quorum. A node whose every journal is parked stays down;
                // its audits are told so convergence excuses exactly these
                // nodes, and only these. A *wiped* identity is parked for the
                // budget only: it boots, and the library refuses its empty
                // store (#147). On a journal seed a transient fault (a failed
                // sync of the boot's own writes) can reach it first, and it
                // restarts like any other (#176).
                let parked = seats.iter().all(|seat| {
                    seat.world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .park_reason(my_ip)
                        .is_some_and(|reason| reason != ParkReason::Wiped)
                });
                if parked {
                    assert_reachable!("storage: a corruption-crashed node stays down");
                    for seat in &seats {
                        stay_down(&seat.checker, Down::StorageParked(self_rank.0));
                    }
                    return Ok(());
                }
                assert_reachable!("a storage-fault crash recovers through the restart path");
            }
            // The library refused the store (#147). Amnesia is the wipe
            // coin's outcome and the one the rule exists for: the identity
            // stays down for the run, replaced by reconfiguration — the
            // audit already excused it from convergence when the driver
            // reported the refusal. The harness cross-checks the refusal
            // against its own injection: only a wiped disk is ever
            // amnesiac here.
            Err(RunError::Refused(BootRefusal::Amnesia)) => {
                let wiped = seats.iter().any(|seat| {
                    seat.world
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .is_wiped(my_ip)
                });
                assert_always!(
                    wiped,
                    "storage: an amnesia refusal names a wiped identity",
                    { "node" => self_rank.0 }
                );
                tracing::info!(node = self_rank.0, "amnesia_refused_stays_down");
                return Ok(());
            }
            // #207: the library refused a store formatted under another
            // configuration. Only the operator's edit above ever changes
            // one, so the refusal must name it; the operator restores the
            // file and restarts, and the node comes back as the member it
            // was — nothing was written by the refused boot.
            Err(RunError::Refused(BootRefusal::ConfigMismatch)) => {
                let restored = edit.restore(&mut seats[0].config);
                assert_always!(
                    restored,
                    "storage: a configuration refusal names an operator's edit",
                    { "node" => self_rank.0 }
                );
                if !restored {
                    return Err(SimulationError::InvalidState(format!(
                        "node {} refused a configuration nobody edited",
                        self_rank.0
                    )));
                }
                tracing::info!(node = self_rank.0, "config_restored");
            }
            // A first boot on a formatted store is a harness bug: the
            // provisioning ledger and the disks disagree.
            Err(RunError::Refused(BootRefusal::AlreadyFormatted)) => {
                assert_always!(
                    false,
                    "storage: a first boot never meets a formatted store",
                    { "node" => self_rank.0 }
                );
                return Err(SimulationError::InvalidState(format!(
                    "node {} booted as first boot on a formatted store",
                    self_rank.0
                )));
            }
            // The only non-crash exit: a genuine infrastructure failure
            // propagates to the harness instead of being retried.
            Err(RunError::Infra(e)) => return Err(e),
            Ok(()) => return Ok(()),
        }
    }
}

/// The operator's edit of a configuration file (#207): drawn at a restart,
/// applied to the first boot of an identity the provisioning ledger knows,
/// and undone when the library refuses it.
pub(super) struct OperatorEdit<C> {
    /// The coin fired and the edit has not reached a boot yet.
    pending: bool,
    /// The configuration the edit replaced, until the refusal restores it.
    edited_from: Option<C>,
}

impl<C: Clone> OperatorEdit<C> {
    pub(super) fn new(pending: bool) -> Self {
        Self {
            pending,
            edited_from: None,
        }
    }

    /// Apply the pending edit to `config` when `provisioned` (a first boot
    /// under an edited file would format the edit); `true` when it did.
    pub(super) fn apply(
        &mut self,
        provisioned: bool,
        config: &mut C,
        edit: impl FnOnce(&mut C),
    ) -> bool {
        if !self.pending || !provisioned {
            return false;
        }
        self.pending = false;
        self.edited_from = Some(config.clone());
        edit(config);
        true
    }

    /// The library refused the edit: restore the original into `config`;
    /// `false` when there was no edit to restore (a refusal nobody caused).
    pub(super) fn restore(&mut self, config: &mut C) -> bool {
        match self.edited_from.take() {
            Some(original) => {
                *config = original;
                true
            }
            None => false,
        }
    }
}

/// One journal's seat on an acceptor process (#188): its configuration, its
/// storage world, its audit world and its audit port.
pub(super) struct Seat {
    pub(super) journal: paros::JournalIdentifier,
    pub(super) config: Config,
    pub(super) world: Arc<Mutex<StorageWorld>>,
    pub(super) checker: Arc<AuditWorld>,
    pub(super) audit: NodeAudit<SimTimeProvider>,
    /// The journal's configuration floor and its clean-copy budget (the
    /// numbers its storage world was sized by).
    pub(super) floor: usize,
    pub(super) clean_copies: usize,
    /// A system journal or a spare's (#189): stored
    /// ordered, with the injector dark, outside the copy budget — the
    /// storage fault model is the genesis journals' business.
    pub(super) quiet: bool,
    /// Created at runtime (#189, a spare's): opened only when the registry
    /// admits the node, never at boot.
    pub(super) created: bool,
    /// Tombstoned (#189): never opened again.
    pub(super) deleted: bool,
}

impl Seat {
    /// A quiet seat for a system journal or a created one (#189): its
    /// own storage world (no budget is set, so nothing is injected) and its
    /// own audit world and port, reporting to the system board too.
    pub(super) fn quiet(
        ctx: &SimContext,
        journal: paros::JournalIdentifier,
        config: Config,
        system: &Arc<Mutex<crate::audit::system::SystemBoard>>,
    ) -> Self {
        // A journal of its own (a system or a created one) takes no injected
        // damage, so its world holds no budget; a joiner's seat on a genesis
        // journal (#189, a spare) shares that journal's world and leaves its
        // budget as the genesis nodes sized it — a fault-free copy only ever
        // adds to what it defends.
        let world = storage_world_for(ctx.state(), journal);
        let checker = audit_world_for(ctx.state(), journal);
        // The pair of the creator's record (#241): a created journal runs in
        // the mode its decided create gave it.
        assert_always!(
            config.writer_mode == checker.mode(),
            "journal: a node opens a journal in the mode it was created with",
            { "journal" => journal.to_string() }
        );
        let audit = NodeAudit::new(ctx.time().clone(), checker.clone())
            .in_journal(journal, journal_board(ctx.state()))
            .with_system(system.clone());
        let members = config.peers.len();
        Self {
            journal,
            config,
            world,
            checker,
            audit,
            floor: members,
            clean_copies: members,
            quiet: true,
            created: false,
            deleted: false,
        }
    }
}

/// Arm the system journals on a genesis node (#189): the board, the seats of
/// the system journals on a seed, every seat's port reporting to the board,
/// and the plan the driver follows them by. The fleet tenant and the cell
/// control journal are the machines' (#246, `crate::machine`).
fn system_rig(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    seats: &mut Vec<Seat>,
    self_rank: NodeId,
) -> (ControlPlan, Arc<Mutex<crate::audit::system::SystemBoard>>) {
    let board = crate::audit::system::system_board(ctx.state());
    let (system_plan, seeds) = system_plan(ctx, deployment, members, self_rank);
    for seat in seats.iter_mut() {
        seat.audit = seat.audit.clone().with_system(board.clone());
    }
    if seeds.contains(&self_rank) {
        let identifiers = crate::shape::identifiers(ctx.state());
        let journal = identifiers.registry;
        let config = Config {
            peers: seeds.clone(),
            quorum_system: paros::QuorumSystem::Majority,
            ..Config::new(self_rank, journal)
        };
        seats.push(Seat::quiet(ctx, journal, config, &board));
    }
    (system_plan, board)
}

/// The system plan every node of the run follows (#189), with the seeds'
/// identities; arms the system board on the way.
pub(super) fn system_plan(
    ctx: &SimContext,
    deployment: &Deployment,
    members: &[(NodeId, String)],
    self_id: NodeId,
) -> (ControlPlan, Vec<NodeId>) {
    let identifiers = crate::shape::identifiers(ctx.state());
    let seeds: Vec<NodeId> = crate::shape::seed_ranks(members.len())
        .into_iter()
        .map(NodeId)
        .collect();
    let board = crate::audit::system::system_board(ctx.state());
    let spares = spare_template(ctx, deployment);
    let machines = crate::shape::joiner_machines(ctx.state(), deployment.joiners().len());
    crate::audit::system::lock(&board).arm(
        members.iter().map(|(id, _)| id.0),
        !deployment.joiners().is_empty(),
        spares.is_some() && !deployment.joiners().is_empty(),
        machines.iter().enumerate().map(|(rank, machine)| {
            (
                crate::roles::joiner_node_id(rank).0,
                (machine.class, machine.capacity),
            )
        }),
    );
    // A joiner's class is its machine's (#211); a genesis node is storage.
    let class = machines
        .iter()
        .enumerate()
        .find(|(rank, _)| crate::roles::joiner_node_id(*rank) == self_id)
        .map_or(paros::system::Class::Storage, |(_, machine)| machine.class);
    (
        ControlPlan {
            self_id,
            class,
            seeds: members
                .iter()
                .filter(|(id, _)| seeds.contains(id))
                .cloned()
                .collect(),
            cell: identifiers.registry,
            founders: members.iter().map(|(id, _)| *id).collect(),
            spares: spares.into_iter().collect(),
        },
        seeds,
    )
}

/// The default journal's configuration a joiner joins as a spare once the
/// registry admits it (#189) — the same run-level draws every genesis node
/// built its own from (the bootstrap ranks, the matchmaker set, the quorum
/// policy; each fixed by its first caller). Only where a reconfiguration can
/// pull a joiner in and every process of the deployment can reach it: a
/// seed with matchmakers, and neither proxy leaders nor replicas (their
/// address books and pools are static; a joiner leading would reach
/// neither).
fn spare_template(ctx: &SimContext, deployment: &Deployment) -> Option<Config> {
    if deployment.matchmakers().is_empty()
        || !deployment.proxies().is_empty()
        || !deployment.replicas().is_empty()
    {
        return None;
    }
    let pool_len = deployment.acceptors().len();
    let bootstrap: Vec<NodeId> = crate::shape::bootstrap_ranks(ctx.state(), pool_len, true)
        .into_iter()
        .map(NodeId)
        .collect();
    let matchmaker_len = deployment.matchmakers().len();
    let matchmakers: Vec<MatchmakerId> =
        crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_len)
            .into_iter()
            .map(MatchmakerId)
            .collect();
    let policy = crate::shape::quorum_policy(ctx.state(), pool_len);
    Some(Config {
        journal: crate::shape::identifiers(ctx.state()).main,
        id: NodeId(0),
        quorum_system: policy.system(bootstrap.len()),
        peers: bootstrap,
        nodes: (0..pool_len as u64).map(NodeId).collect(),
        matchmakers,
        matchmaker_pool: (0..matchmaker_len as u64).map(MatchmakerId).collect(),
        proxy_count: 0,
        replica_count: 0,
        writer_mode: paros::WriterMode::Single,
    })
}
