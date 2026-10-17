//! The chain client's **fleet operations** (#229, #246): `init` whole and
//! creating and removing tenants, run through the library's
//! `paros::client::initialize` and `paros::client::fleet` — the code
//! `parosctl` ships — against the machines (`crate::machine`). No cell is
//! handed to an operator: the first `init` forms it over the founding members
//! the layout drew (`cell init`'s decree, #277), and every operator learns its control journals from the `init` it
//! ran or through `Inspect` at the machines, never from the harness (§3.8:
//! no identifier is fixed).
//!
//! Every client is an operator: several run fleet operations at once and
//! fence each other through the journals' generations, and an operation
//! that loses is resumed later from what the journals hold. These shapes make
//! the state machines' middles likely, each a BUGGIFY location paired with a
//! reachable where it fires:
//!
//! - **`init` whole, again** (#246): an operator that already saw the cell
//!   runs `init` once more, which must find nothing left to do or finish
//!   what an interrupted run left; `cell init` started at another founder,
//!   or a second one at once at another founder, which must converge on the
//!   one cell; and `cell init` sent to a machine outside the founders, which
//!   must be refused;
//! - **a crash at a step** (one location per operation): the operator takes
//!   one step and stops, as if it died there; its next fleet step resumes the
//!   same operation, which must end where an uninterrupted one would;
//! - **a changed identity**: an `init` told another cell's id must be
//!   refused; a tenant created under an id this client had already used must
//!   be refused, and the creator redraws;
//! - **a crash between the fleet directory's checkpoint and its truncate**
//!   (#247, the registry owner's shape for the fleet tenant): the checkpoint
//!   stays mid-log, and every later fold of the directory verifies it on the
//!   way.
//!
//! Every fold a session makes — the directory's and the cell's — is held to the
//! checkpoint oracle the registry's owner is (`judge_folds`): a checkpoint
//! met with the whole prefix folded is that prefix's state.
//!
//! **The fleet directory equals the cell's tenant list** (FDB's
//! `MetaclusterConsistency`): after an operation that left the session
//! holding both journals, when a fresh read finds neither written since its
//! folds — so the two folds are one instant's — every tenant the cell hosts
//! is in the fleet directory under that cell, and every `READY` `users`
//! tenant in the fleet directory is hosted. A tenant mid-operation
//! (`REGISTERING`, `REMOVING`) may be either.
//! Mid-run the check claims nothing of its own: a session that does not
//! hold both journals skips it. **At the end of every run** (#247) it runs
//! over the final folds: in the recovery tail every operator finishes the
//! operation it stopped in ([`FleetOps::settle`]), and the last one judges
//! the control plane once every fleet writer is quiet
//! ([`FleetOps::final_check`]) — the directory's equality, no tenant left
//! mid-operation, a started `init` `READY`, every live node's registry fold
//! at the tail.

use paros::Address;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use moonpool_sim::{
    RandomProvider, SimContext, SimTimeProvider, StateHandle, TimeProvider, assert_always,
    assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::client::checkpoint::{CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome};
use paros::client::fleet::{FleetRefusal, FleetSession, Interrupted, Run, Stage, Step};
use paros::client::names::{TenantResolution, resolve_tenant};
use paros::client::{ClaimOutcome, Writer};
use paros::fleet::{CellState, FleetDirectory, TenantKind, TenantState};
use paros::machine::ControlJournals;
use paros::system::{NodeStanding, Registry};
use paros::{JournalId, JournalIdentifier, NodeId, TenantId};

use crate::audit::liveness::JudgedRegistry;
use crate::client::{ChainClient, Connector};
use crate::shape::MachineLayout;

mod admit;
mod capacity;
mod cell;
mod converge;
mod drain;
mod election;
mod healthy;
mod journals;
mod keys;
mod load;
mod other_cell;
mod placement;
mod replaced;
mod resolve;
mod sets;
mod tags;
mod view;

use cell::Cell;

/// The tenant names an operation is drawn from: few, so two operators race
/// for one often.
const NAMES: [&[u8]; 3] = [b"acme", b"globex", b"initech"];

/// The run's completed tenant removals (#239), shared by every operator:
/// a name is reusable once its removal completes, its id never is.
#[derive(Default)]
struct TenantNames {
    /// Each removed tenant, by the fleet whose directory removed it (the
    /// fleet tenant's control journal), to the name it held. An id is never
    /// reused inside one fleet directory, the authority that enforces it. A
    /// fleet formed after its cell was lost knows nothing of the old one: a
    /// create still pending there may register the same id again (witness
    /// 10193140021688847625, PR #562 (acceptor partitions)).
    removed: BTreeMap<(JournalIdentifier, TenantId), Vec<u8>>,
    /// Each cell, to the name the first resolution through it gave
    /// (#239 (names at the edge)).
    cells: BTreeMap<u64, Vec<u8>>,
}

/// The tenants `fleet`'s directory removed, in the run so far.
fn removed_in(state: &StateHandle, fleet: JournalIdentifier) -> BTreeSet<TenantId> {
    with_names(state, |names| {
        names
            .removed
            .keys()
            .filter(|(at, _)| *at == fleet)
            .map(|(_, tenant)| *tenant)
            .collect()
    })
}

/// Run `f` over the run's [`TenantNames`].
fn with_names<R>(state: &StateHandle, f: impl FnOnce(&mut TenantNames) -> R) -> R {
    let names = crate::state::published(state, TENANT_NAMES_KEY, TenantNames::default);
    let mut names = names
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut names)
}

const TENANT_NAMES_KEY: &str = "paros-tenant-names";

/// Run `f` over the operator whose turn it is to finish its pending
/// operation in the recovery tail ([`FleetOps::take_turn`]).
fn with_turn<R>(state: &StateHandle, f: impl FnOnce(&mut Option<u64>) -> R) -> R {
    let turn = crate::state::published(state, TURN_KEY, || None::<u64>);
    let mut turn = turn
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f(&mut turn)
}

const TURN_KEY: &str = "paros-fleet-turn";

/// An operation this client stopped in the middle of — the crash shape, or
/// a run that did not end (interrupted, its target killed under it, going
/// round) — to resume on its next fleet step, and at the latest in the
/// recovery tail (#247): an operator remembers what it was doing.
#[derive(Clone, Debug)]
enum Pending {
    /// `init` whole (#246): nothing decided it yet.
    Initialize,
    /// `init`'s fleet half, on a cell this client knows.
    Init,
    /// The name and the identifiers this client's creation drew.
    Create(Vec<u8>, Vec<JournalIdentifier>),
    Remove(Vec<u8>),
}

/// The chain client's fleet state across its steps.
pub(super) struct FleetOps {
    /// Builds a client over the machines this operator learns.
    connector: Connector,
    /// Every machine's address, in rank order.
    machines: Vec<Address>,
    /// Every frontend's address, in rank order (#192 (the frontend)).
    frontends: Vec<Address>,
    /// The machines' layout: the founding members `cell init` lists.
    layout: MachineLayout,
    /// The cell, once this operator learned it.
    cell: Option<Cell>,
    /// How long an `init`, or one machine's `Inspect`, may take.
    patience: Duration,
    /// The acceptors' node registry (#189) and its genesis pool size, on a
    /// seed that runs the system journals: every live node's fold of it
    /// reaches its tail after chaos.
    registry: Option<(JournalIdentifier, usize)>,
    client_id: u64,
    /// The universe's and the cell's names this operator's `init` writes,
    /// whole or its fleet half alone: `parosctl init` always names both
    /// (#239 (names at the edge)).
    init_names: (String, String),
    /// A fresh seed per session and checkpointer (#241).
    leader_seeds: super::LeaderSeeds,
    /// This client saw an `init` end on the cell it knows.
    initialized: bool,
    /// Tenant identifiers this client had created: a deliberate reuse names one.
    ever_created: Vec<JournalIdentifier>,
    pending: Option<Pending>,
    /// The cell this client forgot as unknown to its machines, until it
    /// learns one: a lost cell, when the next one differs.
    forgotten: Option<paros::machine::ControlJournals>,
    /// How long into an operation its target is killed, and how long it
    /// stays down (`ChainConfig::fleet_kill_delay_ms`, `fleet_kill_down_ms`).
    kill_ms: (u64, u64),
    /// The pending operation was cut short by its target's kill.
    killed: bool,
    /// The journals this operator learned (#246): every call it makes at
    /// the cell names one of them.
    learned: super::system::Learned,
    /// The machine whose admission this operator stopped after its
    /// registration (#216), to finish on its next `ADMIT`.
    admitting: Option<Address>,
    /// A journal request no answer decided yet (#210): sent again, with the
    /// same id, at this client's next journal step.
    journal_pending: Option<paros::client::journals::JournalRequest>,
    /// Each tenant's journal names, as this client resolved them through
    /// the library (#239 (names at the edge)): a cached resolution goes
    /// stale when its journal is deleted and the name created again.
    journal_names: BTreeMap<TenantId, paros::client::names::JournalNames>,
    /// The tenants where a create the cell had no slot for was refused
    /// (#212 (placement)), until this client's next create there is placed.
    refused: BTreeSet<TenantId>,
    /// The replaced-founder scenario's steps this operator took (#423).
    replaced: replaced::Steps,
    /// The single-writer journals this client created (#215 (zones)): on a
    /// zone-outage seed, a journal step while a zone is down appends to one.
    created_single: Vec<JournalIdentifier>,
    /// This client made its one frontend call before a tenant was `READY`
    /// (#446 (a gate that never fires cannot fail the sweep)).
    frontend_cold: bool,
    /// The run's shared state: the completed removals (#239).
    state: StateHandle,
    /// The simulated clock: when an operation ran (#213 (three founding
    /// members)).
    time: SimTimeProvider,
}

impl FleetOps {
    /// The fleet operations of client `client_id` on `deployment`.
    pub(super) fn new(
        ctx: &SimContext,
        deployment: &crate::roles::Deployment,
        connector: Connector,
        patience: Duration,
        client_id: u64,
        leader_seeds: super::LeaderSeeds,
        kill_ms: (u64, u64),
    ) -> moonpool_sim::SimulationResult<Self> {
        let machines = crate::machine::machine_addrs(ctx.state(), deployment)?;
        let layout = crate::shape::machine_layout(ctx.state(), machines.len());
        let registry = crate::shape::system_journals(ctx.state()).then(|| {
            (
                crate::shape::identifiers(ctx.state()).registry,
                deployment.acceptors().len(),
            )
        });
        Ok(Self {
            connector,
            machines,
            frontends: crate::frontend::frontend_addrs(deployment),
            layout,
            cell: None,
            patience,
            registry,
            client_id,
            init_names: ("universe".into(), format!("cell-{}", client_id % 4)),
            leader_seeds,
            initialized: false,
            ever_created: Vec::new(),
            pending: None,
            forgotten: None,
            kill_ms,
            killed: false,
            learned: super::system::Learned::default(),
            admitting: None,
            journal_pending: None,
            journal_names: BTreeMap::new(),
            refused: BTreeSet::new(),
            created_single: Vec::new(),
            replaced: replaced::Steps::default(),
            frontend_cold: false,
            state: ctx.state().clone(),
            time: ctx.time().clone(),
        })
    }

    /// A session over `journals` of `cell` writing both journals under
    /// leader uuids of its own (#241). `None` only for journals that name no
    /// fleet tenant, which a learned cell's never do.
    fn session(
        &self,
        cell: &Cell,
        journals: ControlJournals,
        policy: CheckpointPolicy,
    ) -> Option<FleetSession> {
        FleetSession::new(
            journals,
            self.leader_seeds.next(),
            Registry::new(cell.members.iter().copied().map(NodeId)),
            policy,
        )
        .map(|session| {
            session
                .with_names(self.init_names.0.as_bytes(), self.init_names.1.as_bytes())
                .with_root_key(crate::frontend::signing_entry(&self.state))
        })
    }

    /// `FLEET_INIT`: run `init` whole while this operator knows no cell, or
    /// again on its own location; else `init`'s fleet half for the cell it
    /// knows — or resume the operation this client stopped in the middle of.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn init(&mut self, ctx: &SimContext, policy: CheckpointPolicy, draw: u64) {
        if self.pending.is_some() {
            let _ = self.resume(ctx, policy, draw).await;
            return;
        }
        // On a replaced-founder seed, the re-run the scenario asked for.
        let rerun = std::mem::take(&mut self.replaced.rerun_next);
        let cell = match &self.cell {
            Some(_) if rerun || buggify_with_prob!(0.1) => {
                assert_reachable!("init: an operator that knows the cell runs init again");
                None
            }
            known => known.clone(),
        };
        let Some(cell) = cell else {
            let ended = self.initialize(ctx, draw).await;
            self.stopped(ended, false, Pending::Initialize);
            return;
        };
        let client = cell.client.clone();
        let first = cell.first(draw);
        if self.initialized && buggify_with_prob!(0.1) {
            // An operator talking to another cell than the one the fleet tenant holds.
            assert_reachable!("fleet: an init is told another cell's id");
            let wrong = ControlJournals {
                cell_id: cell.journals.cell_id ^ 2,
                ..cell.journals
            };
            let Some(mut session) = self.session(&cell, wrong, policy) else {
                return;
            };

            let run = session.init(&client, first, draw | 1, Duration::ZERO).await;
            judge_folds(&session);
            assert_always!(
                !matches!(run.outcome, Step::Done { .. }) && run.steps.is_empty(),
                "fleet: an init naming another cell writes nothing and never ends",
                { "steps" => run.steps.len() }
            );
            if matches!(
                run.outcome,
                Step::Refused(FleetRefusal::CellMismatch { .. })
            ) {
                assert_reachable!("fleet: a step talking to another cell is refused");
            }
            return;
        }
        if self.initialized && buggify_with_prob!(0.15) {
            self.checkpoint_directory_and_stop(&client, cell.fleet, policy, first)
                .await;
            return;
        }
        let Some(mut session) = self.session(&cell, cell.journals, policy) else {
            return;
        };
        if buggify_with_prob!(0.2) {
            match session.init_step(&client, first, draw | 1).await {
                Step::Advanced(stage) => {
                    assert_reachable!("fleet: an init stops after one step");
                    reach(stage);
                    self.pending = Some(Pending::Init);
                }
                // An interrupted step may have landed (an ambiguous or
                // unanswered write): the operator keeps it to resume.
                Step::Interrupted(interrupted) => {
                    self.forget_unknown(&interrupted);
                    self.pending = Some(Pending::Init);
                }
                _ => {}
            }
            judge_folds(&session);
            return;
        }
        let kill = self.killer(ctx, &cell, first);
        let (ended, cut_short) =
            futures::join!(self.finish_init(&client, &mut session, first, draw), kill);
        judge_folds(&session);
        self.stopped(ended, cut_short, Pending::Init);
    }

    /// The process kill of an operation's target (#247): with its own
    /// BUGGIFY location, a future that crashes the machine `first` names
    /// `kill_ms.0` into the operation it is joined with — while a step is in
    /// flight — and restarts it `kill_ms.1` later. Moonpool's own kill: the
    /// process dies with its connections and unsynced writes, then reboots
    /// from its disk. Whether it fired.
    fn killer<'a>(
        &self,
        ctx: &'a SimContext,
        cell: &Cell,
        first: usize,
    ) -> impl Future<Output = bool> + use<'a> {
        let ip = cell.ip(ctx, first).filter(|_| buggify_with_prob!(0.1));
        let (delay, down) = self.kill_ms;
        async move {
            let Some(ip) = ip else {
                return false;
            };
            if ctx
                .time()
                .sleep(Duration::from_millis(delay))
                .await
                .is_err()
            {
                return false;
            }
            assert_reachable!("fleet: an operation's target is killed while a step is in flight");
            crate::lifecycle::crash(ctx, &ip).await;
            let _ = ctx.time().sleep(Duration::from_millis(down)).await;
            crate::lifecycle::restart(ctx, &ip).await;
            true
        }
    }

    /// Keep an operation that did not end as this client's pending one;
    /// `killed` when its target was killed under it.
    fn stopped(&mut self, ended: bool, killed: bool, pending: Pending) {
        if !ended {
            if killed {
                assert_reachable!("fleet: an operation its target's kill cut short is pending");
            }
            self.killed = killed;
            self.pending = Some(pending);
        }
    }

    /// An operator that crashes between the fleet directory's checkpoint and
    /// its truncate (#247, the registry's shape for the fleet tenant): open
    /// the fleet tenant's control journal as its owner, write a checkpoint of
    /// the fold, and stop. The checkpoint stays mid-log; every later fold
    /// verifies it on the way, and the next owner's checkpoint truncates past
    /// it.
    async fn checkpoint_directory_and_stop(
        &self,
        client: &ChainClient,
        fleet: JournalIdentifier,
        policy: CheckpointPolicy,
        first: usize,
    ) {
        let mut owner = Checkpointer::new(
            fleet,
            self.leader_seeds.next(),
            FleetDirectory::default(),
            policy,
        );
        let OpenOutcome::Open { diverged, .. } = owner.open(client, first).await else {
            return;
        };
        assert_always!(
            diverged.is_none(),
            "checkpoint: an owner's load finds each checkpoint its prefix's state",
            { "journal" => fleet.to_string(), "seq" => diverged.unwrap_or_default() }
        );
        if buggify_with_prob!(0.3) {
            // It stops inside the run instead (#353): no fold restores from
            // it.
            if owner.write_run_without_end(client, first).await.is_ok() {
                assert_reachable!("fleet: an operator stops inside the directory's checkpoint run");
            }
            return;
        }
        if owner.write_checkpoint(client, first).await.is_ok() {
            assert_reachable!(
                "fleet: an operator stops between the directory's checkpoint and its truncate"
            );
        }
    }

    /// `TENANT`: create (an even `class`) or remove a `users` tenant named
    /// from the alphabet — or resume the operation this client stopped in.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn tenant(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        (class, payload): (u64, u64),
    ) {
        if self.pending.is_some() {
            let _ = self.resume(ctx, policy, payload).await;
            return;
        }
        let Some(cell) = self.learn(ctx).await else {
            // No machine serves a cell yet: nothing to create a tenant in.
            assert_reachable!("fleet: a tenant operation finds no cell formed yet");
            return;
        };
        let name = NAMES[usize::try_from(class % NAMES.len() as u64).unwrap_or(0)].to_vec();
        let client = cell.client.clone();
        let first = cell.first(payload);
        let Some(mut session) = self.session(&cell, cell.journals, policy) else {
            return;
        };
        if self.initialized && buggify_with_prob!(0.15) {
            // A root key rotation instead (#245 (Biscuit tokens)).
            self.rotate(&client, &mut session, first).await;
            judge_folds(&session);
            return;
        }
        if class % 2 == 0 {
            if buggify_with_prob!(0.2) {
                let identifier = tenant_identifier(payload);
                let step = session.create_step(&client, first, &name, identifier).await;
                match step {
                    Step::Advanced(stage) => {
                        assert_reachable!("fleet: a tenant creation stops after one step");
                        reach(stage);
                        self.pending = Some(Pending::Create(name, vec![identifier]));
                    }
                    // An interrupted step may have landed: a creation resumes
                    // only by its own identifier, so the operator keeps it
                    // (seed 17211107884392273235 left one `Registering`).
                    Step::Interrupted(interrupted) => {
                        self.forget_unknown(&interrupted);
                        self.pending = Some(Pending::Create(name, vec![identifier]));
                    }
                    _ => {}
                }
                judge_folds(&session);
                return;
            }
            let draws = vec![
                tenant_identifier(payload),
                tenant_identifier(payload.rotate_left(23) ^ 0x7e57),
            ];
            let kill = self.killer(ctx, &cell, first);
            let pending = Pending::Create(name.clone(), draws.clone());
            let (ended, cut_short) = futures::join!(
                self.create(&client, &mut session, first, name, draws, payload),
                kill
            );
            self.stopped(ended, cut_short, pending);
        } else {
            // The removal's crash is its own location, and fires often: a
            // removal needs a `READY` tenant of that name to write its first
            // step at all, so a shared 20% left "a tenant removal resumed
            // after a crash" the sweep's rarest gate, near its seed cap.
            if buggify_with_prob!(0.5) {
                match session.remove_step(&client, first, &name).await {
                    Step::Advanced(stage) => {
                        assert_reachable!("fleet: a tenant removal stops after one step");
                        reach(stage);
                        self.pending = Some(Pending::Remove(name));
                    }
                    // An interrupted step may have landed: keep it to resume.
                    Step::Interrupted(interrupted) => {
                        self.forget_unknown(&interrupted);
                        self.pending = Some(Pending::Remove(name));
                    }
                    _ => {}
                }
                judge_folds(&session);
                return;
            }
            let kill = self.killer(ctx, &cell, first);
            let pending = Pending::Remove(name.clone());
            let (ended, cut_short) =
                futures::join!(self.remove(&client, &mut session, first, &name), kill);
            self.stopped(ended, cut_short, pending);
        }
        judge_folds(&session);
    }

    /// Run the operation this client stopped in, to its end; whether none is
    /// pending any more.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    async fn resume(&mut self, ctx: &SimContext, policy: CheckpointPolicy, draw: u64) -> bool {
        if self.pending.is_none() {
            return true;
        }
        if matches!(self.pending, Some(Pending::Initialize)) {
            if self.initialize(ctx, draw).await {
                self.pending = None;
                return true;
            }
            return false;
        }
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        // Learning a lost cell's successor drops an operation of the lost
        // universe (`forget_lost_universe`).
        let Some(pending) = self.pending.take() else {
            return true;
        };
        let client = cell.client.clone();
        let first = cell.first(draw);
        let Some(mut session) = self.session(&cell, cell.journals, policy) else {
            self.pending = Some(pending);
            return false;
        };
        let ended = match &pending {
            // Resumed above, before a cell is needed; `init` whole if ever here.
            Pending::Initialize => self.initialize(ctx, draw).await,
            Pending::Init => self.finish_init(&client, &mut session, first, draw).await,
            Pending::Create(name, identifiers) => {
                // Only this creation's own identifiers resume it (a tenant is
                // created once).
                self.create(
                    &client,
                    &mut session,
                    first,
                    name.clone(),
                    identifiers.clone(),
                    draw,
                )
                .await
            }
            Pending::Remove(name) => self.remove(&client, &mut session, first, name).await,
        };
        judge_folds(&session);
        if !ended {
            self.pending = Some(pending);
            return false;
        }
        if std::mem::take(&mut self.killed) {
            assert_reachable!("fleet: an operation its target's kill cut short ends on resumption");
        }
        true
    }

    /// Run `init` to its end; whether it ended (done or refused).
    async fn finish_init(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        draw: u64,
    ) -> bool {
        let run = session.init(client, first, draw | 1, Duration::ZERO).await;
        run.steps.iter().copied().for_each(reach);
        match run.outcome {
            Step::Done {
                result: fleet,
                last,
            } => {
                let finished = last == Some(Stage::CellReady);
                if finished && let Some(known) = &self.cell {
                    let cell = known.journals.cell_id;
                    let directory_cell = session.directory().cell(cell).map(|c| c.state);
                    let joined = session.cell().fleet().map(|f| (f.fleet_id, f.cell_id));
                    assert_always!(
                        directory_cell == Some(CellState::Ready) && joined == Some((fleet, cell)),
                        "fleet: a finished init leaves the cell READY in the directory and joined on its side",
                        { "cell" => cell, "fleet" => fleet }
                    );
                    assert_sometimes!(
                        run.steps.first() != Some(&Stage::FormFleet),
                        "fleet: an init resumes from where the journals stand"
                    );
                }
                assert_sometimes!(finished, "fleet: an init registers the cell READY");
                self.initialized = true;
                self.check_directory(client, session, first).await;
                true
            }
            Step::Refused(_) => true,
            Step::Interrupted(interrupted) => {
                self.forget_unknown(&interrupted);
                false
            }
            Step::Advanced(_) => false,
        }
    }

    /// The gates of a tenant creation that ran while the downed-founder
    /// (#213 (three founding members)) or zone-outage (#215 (zones))
    /// scenario held machines down for the whole run.
    fn outage_gates(&self, run: &paros::client::fleet::Run<TenantId>, started: Duration) {
        if !run.steps.is_empty()
            && crate::machine::founder_down_through(&self.state, started, self.time.now())
        {
            // #213 (three founding members): the cell control journal and
            // the fleet tenant chose this run's writes without that member.
            assert_reachable!(
                "fleet: a control-journal write is chosen with a founding member down"
            );
            if matches!(
                run.outcome,
                Step::Done {
                    last: Some(Stage::TenantReady),
                    ..
                }
            ) {
                assert_reachable!("fleet: a tenant is created with a founding member down");
            }
        }
        if !run.steps.is_empty()
            && crate::machine::zone_down_through(&self.state, started, self.time.now())
        {
            // #215 (zones): the cell control journal and the fleet tenant
            // chose this run's writes without a whole zone.
            assert_reachable!("zones: a control-journal write is chosen with a zone down");
            if matches!(
                run.outcome,
                Step::Done {
                    last: Some(Stage::TenantReady),
                    ..
                }
            ) {
                assert_reachable!("zones: a tenant is created with a zone down");
            }
        }
    }

    /// Create `name` under the first of `draws` the fleet tenant does
    /// not hold, to its end; whether it ended.
    async fn create(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        name: Vec<u8>,
        draws: Vec<JournalIdentifier>,
        draw: u64,
    ) -> bool {
        // An identifier this client had created — the collision a random u64 never
        // makes on its own — must be refused, and the creator redraws.
        if !self.ever_created.is_empty() && buggify_with_prob!(0.2) {
            assert_reachable!("fleet: a tenant creation reuses an id it created");
            let reused = self.ever_created
                [usize::try_from(draw % self.ever_created.len() as u64).unwrap_or(0)];
            let step = session.create_step(client, first, &name, reused).await;
            assert_always!(
                !matches!(step, Step::Advanced(Stage::RegisterTenant)),
                "fleet: a reused tenant id is never registered again",
                { "tenant" => reused.tenant.0 }
            );
            if step == Step::Refused(FleetRefusal::IdTaken) {
                assert_reachable!("fleet: a duplicate tenant id is refused");
            }
        }
        let tags = tags::drawn(&self.state);
        for draw in &draws {
            tags::note(&self.state, tags::Target::Tenant(draw.tenant), &tags);
        }
        session.use_tags(tags);
        let started = self.time.now();
        let run = session
            .create_tenant(client, first, &name, draws.clone(), Duration::ZERO)
            .await;
        run.steps.iter().copied().for_each(reach);
        self.outage_gates(&run, started);
        self.created(session, &name, &run);
        match run.outcome {
            Step::Done { result, .. } => {
                self.check_directory(client, session, first).await;
                self.resolve_tenant_name(client, first, &name, Some(result))
                    .await;
                self.resolve_at_entry(first, &name, Some(result)).await;
                // An internal tenant has no name: the user path never
                // resolves one.
                if buggify_with_prob!(0.2) {
                    self.resolve_tenant_name(client, first, b"", None).await;
                    self.resolve_at_entry(first, b"", None).await;
                }
                true
            }
            Step::Refused(FleetRefusal::NotInitialized) => {
                assert_always!(
                    !self.initialized,
                    "fleet: a tenant is refused as uninitialized only before init ends",
                    { "client" => self.client_id }
                );
                assert_reachable!("fleet: a tenant operation before init is refused");
                true
            }
            Step::Refused(FleetRefusal::NameTaken { tenant, .. }) => {
                let holder = session.directory().named(&name).map(|(t, _)| t);
                assert_always!(
                    holder == Some(tenant) && draws.iter().all(|d| d.tenant != tenant),
                    "fleet: a second creation of a name is refused for another identifier",
                    {
                        "tenant" => tenant.0,
                        "holder" => holder.map_or(0, |t| t.0),
                        "own_draw" => draws.iter().any(|d| d.tenant == tenant)
                    }
                );
                assert_reachable!("fleet: a second creation of a name is refused");
                true
            }
            Step::Refused(FleetRefusal::Removed { .. }) => {
                assert_reachable!(
                    "fleet: a creation overtaken by a removal is refused by the cell"
                );
                true
            }
            Step::Refused(_) => true,
            Step::Interrupted(interrupted) => {
                self.forget_unknown(&interrupted);
                false
            }
            Step::Advanced(_) => false,
        }
    }

    /// Resolve the tenant `name` through the library (#239), as an entry
    /// role does, and hold it: a name never resolves to a tenant whose
    /// removal completed before the resolution started (ids are never
    /// reused, names are), and an internal tenant never resolves.
    async fn resolve_tenant_name(
        &self,
        client: &ChainClient,
        first: usize,
        name: &[u8],
        created: Option<TenantId>,
    ) {
        let Some(cell) = &self.cell else {
            return;
        };
        let removed = removed_in(&self.state, cell.fleet);
        let resolution = resolve_tenant(client, first, cell.fleet, name).await;
        if let TenantResolution::Resolved {
            tenant, control, ..
        } = &resolution
        {
            assert_always!(
                !removed.contains(tenant),
                "names: a tenant name never resolves to a removed tenant",
                { "tenant" => tenant.0 }
            );
            assert_always!(
                !name.is_empty() && control.tenant == *tenant && control.is_set(),
                "names: a tenant name resolves to a named tenant and its control journal",
                { "tenant" => tenant.0 }
            );
        }
        if name.is_empty() {
            assert_always!(
                matches!(
                    resolution,
                    TenantResolution::Internal | TenantResolution::Unreadable(_)
                ),
                "names: an internal tenant is never resolved through the user path"
            );
            if resolution == TenantResolution::Internal {
                assert_reachable!("names: an internal tenant's resolution is refused");
            }
        } else {
            assert_always!(
                resolution != TenantResolution::Internal,
                "names: a user's tenant name is never an internal tenant's"
            );
        }
        if let Some(created) = created {
            assert_sometimes!(
                matches!(resolution, TenantResolution::Resolved { tenant, .. } if tenant == created),
                "names: a tenant name resolves to the tenant its creation made"
            );
        }
    }

    /// The oracles of a creation's run.
    fn created(&mut self, session: &FleetSession, name: &[u8], run: &Run<TenantId>) {
        let Step::Done { result, last } = run.outcome else {
            return;
        };
        let created = last == Some(Stage::TenantReady);
        if created {
            let entry = session.directory().tenant(result);
            assert_always!(
                entry.is_some_and(|t| t.state == TenantState::Ready
                    && t.name == name
                    && t.kind == TenantKind::Users)
                    && session.cell().hosts(result),
                "fleet: a created tenant is READY in the directory and hosted by its cell",
                { "tenant" => result.0 }
            );
            assert_sometimes!(
                run.steps.first() != Some(&Stage::RegisterTenant),
                "fleet: a tenant creation resumed from REGISTERING"
            );
            let fleet = self.cell.as_ref().map(|cell| cell.fleet);
            if with_names(&self.state, |names| {
                names.removed.iter().any(|((at, tenant), removed)| {
                    Some(*at) == fleet && removed == name && *tenant != result
                })
            }) {
                assert_reachable!(
                    "names: a tenant name is created again after a completed removal"
                );
            }
        }
        assert_sometimes!(created, "fleet: a tenant is created READY");
        if let Some(entry) = session.directory().tenant(result) {
            let identifier = JournalIdentifier::new(result, entry.control);
            if !self.ever_created.contains(&identifier) {
                self.ever_created.push(identifier);
            }
        }
    }

    /// Remove `name` to its end; whether it ended.
    async fn remove(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        name: &[u8],
    ) -> bool {
        let run = session
            .remove_tenant(client, first, name, Duration::ZERO)
            .await;
        run.steps.iter().copied().for_each(reach);
        match run.outcome {
            Step::Done { result, last } => {
                let removed = last == Some(Stage::RemoveTenant);
                if let (Some(tenant), true) = (result, removed) {
                    if let Some(cell) = &self.cell {
                        with_names(&self.state, |names| {
                            names.removed.insert((cell.fleet, tenant), name.to_vec())
                        });
                    }
                    assert_always!(
                        session.directory().named(name).is_none()
                            && session.directory().is_removed(tenant)
                            && !session.cell().hosts(tenant)
                            && session.cell().dropped(tenant),
                        "fleet: a removed tenant is gone from the directory and tombstoned on its cell",
                        { "tenant" => tenant.0 }
                    );
                    assert_sometimes!(
                        run.steps.first() != Some(&Stage::TenantRemoving),
                        "fleet: a tenant removal resumed after a crash"
                    );
                }
                assert_sometimes!(removed, "fleet: a tenant is removed");
                self.check_directory(client, session, first).await;
                true
            }
            Step::Refused(_) => true,
            Step::Interrupted(interrupted) => {
                self.forget_unknown(&interrupted);
                false
            }
            Step::Advanced(_) => false,
        }
    }

    /// Forget the cached cell when the machines no longer know its journals
    /// (#304): a one-founder cell was wiped and a new cell formed at the
    /// same address, so the next `learn()` asks the machines through
    /// `Inspect` again. The machine is right: it never held the old cell.
    fn forget_unknown(&mut self, interrupted: &Interrupted) {
        if let Interrupted::NotClaimed {
            outcome: ClaimOutcome::UnknownJournal,
            ..
        } = interrupted
        {
            assert_reachable!("fleet: an operator forgets a cell its machines do not know");
            self.forgotten = self
                .cell
                .take()
                .map(|cell| cell.journals)
                .or(self.forgotten);
            // The init this client saw end was that cell's: the cell it
            // learns next may still be initializing (#454 (control plane
            // meets chaos): witness 2649599687404826405, a cell lost after
            // its init ended and another one formed).
            self.initialized = false;
        }
    }

    /// The fleet directory against the cell's tenant list, when the session's
    /// two folds are one instant's: it holds the directory already (the
    /// check claims nothing), the cell is read to its tail after the
    /// directory's fold, and a fresh read of the directory then finds this
    /// client still the owner with nothing written past the fold. The cell
    /// coordinator writes the cell's side from the directory alone (#225),
    /// so the directory stood still over the cell's read.
    async fn check_directory(
        &self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
    ) {
        let Some(cell) = &self.cell else {
            return;
        };
        if !session.holds_directory() || session.read_cell(client, first).await.is_err() {
            return;
        }
        let Some(directory_floor) = still(
            client,
            first,
            cell.fleet,
            session.writer(),
            session.directory().next_seq(),
        )
        .await
        else {
            return;
        };
        // The two folds are of one instant. The fleet tenant's checkpoint and truncation
        // ran under its owner's policy on the way.
        if directory_floor > 0 {
            assert_reachable!("fleet: the directory is read past a truncation to its checkpoint");
        }
        if session.directory().fleet().is_none() || session.cell().fleet().is_none() {
            return;
        }
        directory_equals_cell(session.directory(), session.cell());
        assert_reachable!("fleet: the directory is checked against the cell's tenant list");
    }
}

/// How long the recovery tail gives the fleet's control plane (#247): a
/// pending operation to end, the final folds to be read, every node's
/// registry fold to reach the tail. An oracle threshold — **never
/// buggified**: the chaos window is over, moonpool is in recovery mode, and a
/// control plane that cannot finish one operation in this long is stuck.
const FLEET_SETTLE: Duration = Duration::from_secs(20);

/// The pause before an operator's next resumption in the recovery tail:
/// `beat` times a factor drawn uniformly from `1..=2^min(attempt, 4)`. Two
/// operators resuming one interrupted operation each claim its journals, so
/// a fixed beat keeps them superseding each other in lockstep, generation
/// after generation, past [`FLEET_SETTLE`] (witness 17409558280995831005:
/// two clients finishing one `init` traded the fleet tenant's control
/// journal from generation 12 to 48); a randomized, growing backoff lets one
/// of them run its steps uncontested.
fn settle_backoff(ctx: &SimContext, beat: Duration, attempt: u32) -> Duration {
    let span = 1_u32 << attempt.min(4);
    let factor = 1 + ctx.random().random_range(0..span);
    beat * factor
}

impl FleetOps {
    /// The recovery tail's fleet half (#247): resume this client's pending
    /// operation until it ends. Liveness: once the chaos window closed, an
    /// operation an operator stopped in — a crash at a step, a run its
    /// target's kill or a rival cut short — is finished by that operator.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn settle(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        beat: Duration,
    ) {
        if self.pending.is_none() {
            return;
        }
        if !self.take_turn(ctx, beat).await {
            return;
        }
        let deadline = ctx.time().now() + FLEET_SETTLE;
        let mut draw = self.client_id;
        let mut attempt = 0_u32;
        while !self.resume(ctx, policy, draw).await {
            if ctx.time().now() >= deadline
                || ctx.shutdown().is_cancelled()
                || ctx
                    .time()
                    .sleep(settle_backoff(ctx, beat, attempt))
                    .await
                    .is_err()
            {
                break;
            }
            draw = draw.wrapping_add(1);
            attempt = attempt.saturating_add(1);
        }
        // A lost cell (#246) ends no operation: an operator must act
        // outside paros first.
        assert_always!(
            self.pending.is_none()
                || ctx.shutdown().is_cancelled()
                || crate::machine::cell_lost(ctx.state()),
            "fleet: an operation an operator stopped in ends in the recovery tail",
            { "client" => self.client_id, "pending" => format!("{:?}", self.pending) }
        );
        assert_reachable!("fleet: a pending operation is finished in the recovery tail");
        with_turn(ctx.state(), |turn| *turn = None);
    }

    /// Wait for this operator's turn to finish its pending operation in the
    /// recovery tail (#513 (fleet Init duel)): one operator at a time. A
    /// tenant removal two operators resume at once duels the same way
    /// (#361 (slow machines), witness 11003452246573997144 on 16c59ae: two
    /// clients resuming one `Remove` superseded each other's claim on the
    /// fleet tenant until [`FLEET_SETTLE`]), so every operation takes the
    /// turn. Two operators
    /// that finish one `init` at once each claim its journals, so each
    /// supersedes the other's steps; with the tail's slow links one step
    /// takes seconds, and neither ended in [`FLEET_SETTLE`] (witness
    /// 14782271020639680373: client 0's whole `init` was superseded twice by
    /// client 1's fleet half). Operators of one universe do not race their
    /// own `init` once the incident is over; the duel itself is still run
    /// in the chaos window, where nothing owes an end. The next operator
    /// starts its own deadline once the first is done, and finds the
    /// `init` ended. False when the run shut down first.
    async fn take_turn(&self, ctx: &SimContext, beat: Duration) -> bool {
        loop {
            let mine = with_turn(ctx.state(), |turn| match turn {
                Some(holder) if *holder != self.client_id => false,
                _ => {
                    *turn = Some(self.client_id);
                    true
                }
            });
            if mine {
                return true;
            }
            if ctx.shutdown().is_cancelled() || ctx.time().sleep(beat).await.is_err() {
                return false;
            }
        }
    }

    /// The end-of-run check over the final folds (#247), run by the last
    /// client to [`FleetOps::settle`] — every operator has finished, so the
    /// fleet tenant's and the cell's control journals are quiet and one read of each is one
    /// instant's:
    ///
    /// - **liveness** — a started `init` left the cell `READY` in the directory and
    ///   joined on its side, and no `users` tenant is left `REGISTERING` or
    ///   `REMOVING` (every operator finished its own operation; #240's
    ///   coordinator will own an orphan's); every live node's registry fold
    ///   reaches the registry's tail;
    /// - **directory equality** (FDB's `MetaclusterConsistency`), on every
    ///   seed rather than when one session happened to hold both journals:
    ///   membership and `cell_id` — every tenant the cell hosts is in the
    ///   directory under that cell, and every `READY` one in it is hosted. Its
    ///   assignments and counts join once #212 lands.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn final_check(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        expected: &[u64],
    ) {
        self.final_fleet(ctx, ctx.time().now() + FLEET_SETTLE).await;
        // The cell's election settles too (#240), on its own deadline.
        if crate::machine::formed_cell(ctx.state()).is_some()
            && !crate::machine::cell_lost(ctx.state())
            && let Some(cell) = self.learn(ctx).await
        {
            election::election_settles(ctx, &cell, ctx.time().now() + FLEET_SETTLE).await;
            self.renamed_registered(ctx, &cell, ctx.time().now() + FLEET_SETTLE)
                .await;
            let learn = |journal| self.note_journals([journal]);
            placement::placements_booked(ctx, &cell, ctx.time().now() + FLEET_SETTLE, learn).await;
            sets::sets_booked(ctx, &cell, ctx.time().now() + FLEET_SETTLE).await;
            // Every journal the cell can hold heals (#475 (re-place after
            // the Down bound)).
            self.journals_heal(ctx, &cell, ctx.time().now() + FLEET_SETTLE)
                .await;
            // The machines' own `Resolve` folds escape after chaos (#457).
            self.resolve_answers(ctx, &cell, ctx.time().now() + FLEET_SETTLE)
                .await;
        }
        // Every quiet journal world converges (#447 (model checks for every
        // journal world)), on its own deadline.
        converge::worlds_converge(ctx, ctx.time().now() + FLEET_SETTLE).await;
        // Its own deadline (#304): a fleet half that took all of its time
        // must not leave the registry half none.
        self.final_registry(ctx, nodes, expected, ctx.time().now() + FLEET_SETTLE)
            .await;
    }

    /// [`FleetOps::final_check`]'s fleet half, over the cell the machines
    /// formed — learned through `Inspect` when this operator never saw it.
    async fn final_fleet(&mut self, ctx: &SimContext, deadline: Duration) {
        if crate::machine::formed_cell(ctx.state()).is_none() {
            if !crate::machine::init_sent(ctx.state()) {
                assert_reachable!(
                    "machine: a run that never sends init ends with its machines waiting"
                );
            }
            return;
        }
        if lost_cell(ctx) {
            return;
        }
        // The cell is learned again when its machines do not know the cached
        // one's journals (#304): a one-founder cell wiped and formed anew.
        let mut learned = None;
        let mut folds = None;
        let mut attempt = 0_u64;
        // A founding member wiped during the read may lose the cell too:
        // the excuse above holds whenever the cell is lost, not only on entry.
        while folds.is_none()
            && ctx.time().now() < deadline
            && !ctx.shutdown().is_cancelled()
            && !crate::machine::cell_lost(ctx.state())
        {
            if learned.is_none() {
                learned = self.learn(ctx).await;
            }
            let Some(known) = &learned else {
                if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                    break;
                }
                continue;
            };
            let first = known.first(attempt);
            attempt += 1;
            let directory =
                paros::client::fleet::read_directory(&known.client, first, known.fleet).await;
            let mut cell = judged_cell(&known.members);
            let loaded = paros::client::checkpoint::load(
                &mut cell,
                known.journals.cell,
                &known.client,
                first,
                0,
            )
            .await;
            match (directory, loaded) {
                (Ok(directory), LoadOutcome::Loaded { .. }) => folds = Some((directory, cell)),
                (_, LoadOutcome::UnknownJournal) => {
                    assert_reachable!("fleet: an operator forgets a cell its machines do not know");
                    self.cell = None;
                    learned = None;
                    if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                        break;
                    }
                }
                _ => {
                    if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                        break;
                    }
                }
            }
        }
        if ctx.shutdown().is_cancelled() {
            return;
        }
        if lost_cell(ctx) {
            return;
        }
        assert_always!(
            learned.is_some(),
            "fleet: a formed cell's control journals are learned through Inspect after chaos"
        );
        let Some(known) = learned else {
            return;
        };
        assert_always!(
            folds.is_some(),
            "fleet: the directory and the cell's journal are read to their tails after chaos"
        );
        let Some((directory, cell)) = folds else {
            return;
        };
        let cell = cell.state().registry();
        let cell_id = known.journals.cell_id;
        admit::admitted_registered(ctx, cell, cell_id);
        assert_reachable!("fleet: the final folds of the directory and the cell are compared");
        if let Some(fleet) = directory.fleet() {
            let ready = directory.cell(cell_id).map(|c| c.state);
            let joined = cell.fleet().map(|f| (f.fleet_id, f.cell_id));
            assert_always!(
                ready == Some(CellState::Ready) && joined == Some((fleet, cell_id)),
                "fleet: a started init leaves the cell READY and joined after chaos",
                { "fleet" => fleet, "cell" => cell_id }
            );
            assert_reachable!("fleet: a run ends with the cell READY in the directory");
        }
        no_tenant_mid_operation(&directory, cell);
        directory_equals_cell(&directory, cell);
    }

    /// The cell heals around a renamed founding member (#349): once chaos
    /// ends, the cell's registry holds every founding member that came back
    /// under a new name at that name, so its peers dial it there. The
    /// machine asks the coordinator, which writes it; the registry is read
    /// again until it holds every one, or until `deadline`.
    ///
    /// The names are read from the board again at every attempt (#506
    /// (renamed founder not registered)): a machine killed in the chaos
    /// window may reboot in the recovery tail, and that reboot may rename it
    /// again. A list taken once before the loop then waits for a name the
    /// machine no longer advertises. Witness: seed 1904763865968447698 on
    /// 2e959f3, founder 1 renamed `machine-1-1` then `machine-1-2` during
    /// the check; the registry held `machine-1-2`.
    ///
    /// The same read judges every renamed admitted machine (#501 (matchmaker
    /// sets off the founders), #529 (sets oracles)): the registry holds it at
    /// its new name, so the coordinator reaches it there and hands it a set
    /// or a journal. A machine the registry retired or does not hold is not
    /// judged here (`admit::admitted_registered` judges the second). Red
    /// before `machine::renamed_outside_registered`: recipe
    /// `6959312946067892202 110064:7756325629573379374` on b93880a, whose
    /// founder was renamed before the admitted machine registered its new
    /// name, so neither reached the other again.
    async fn renamed_registered(&self, ctx: &SimContext, known: &Cell, deadline: Duration) {
        let cell_id = known.journals.cell_id;
        let mut renamed = crate::machine::renamed_founders(ctx.state(), self.layout.founders);
        let mut admitted = crate::machine::renamed_admitted(ctx.state(), cell_id);
        if renamed.is_empty() && admitted.is_empty() {
            return;
        }
        let mut attempt = 0_u64;
        let (mut held, mut held_admitted) = (false, false);
        while !(held && held_admitted)
            && ctx.time().now() < deadline
            && !ctx.shutdown().is_cancelled()
        {
            renamed = crate::machine::renamed_founders(ctx.state(), self.layout.founders);
            admitted = crate::machine::renamed_admitted(ctx.state(), cell_id);
            let first = known.first(attempt);
            attempt += 1;
            let mut cell = judged_cell(&known.members);
            let loaded = paros::client::checkpoint::load(
                &mut cell,
                known.journals.cell,
                &known.client,
                first,
                0,
            )
            .await;
            let loaded = matches!(loaded, LoadOutcome::Loaded { .. });
            let registry = cell.state().registry();
            held = loaded
                && renamed
                    .iter()
                    .all(|(node, name)| registry.address(NodeId(*node)) == Some(name.as_str()));
            held_admitted = loaded
                && admitted.iter().all(|(node, name)| {
                    registry.get(NodeId(*node)).is_none_or(|entry| {
                        entry.standing == NodeStanding::Retired || entry.addr == name.as_str()
                    })
                });
            if !(held && held_admitted)
                && ctx.time().sleep(Duration::from_millis(100)).await.is_err()
            {
                return;
            }
        }
        if ctx.shutdown().is_cancelled() {
            return;
        }
        if !renamed.is_empty() {
            assert_always!(
                held,
                "machine: a renamed founding member is registered under its new name after chaos",
                { "renamed" => renamed.len() }
            );
            assert_reachable!("machine: the cell heals around a renamed founding member");
        }
        if !admitted.is_empty() {
            assert_always!(
                held_admitted,
                "machine: a renamed admitted machine is registered under its new name after chaos",
                { "renamed" => admitted.len(), "cell" => cell_id }
            );
        }
    }

    /// [`FleetOps::final_check`]'s registry half (#189), on a seed that runs
    /// the system journals: every live node in `expected` follows the
    /// acceptors' node registry to its tail.
    async fn final_registry(
        &self,
        ctx: &SimContext,
        nodes: &ChainClient,
        expected: &[u64],
        deadline: Duration,
    ) {
        let Some((registry, pool)) = self.registry else {
            return;
        };
        // The registry's seed is the acceptor of rank 0.
        let client = nodes.clone().with_own_leader_hint().rotating_over(1);
        let mut tail = None;
        while tail.is_none() && ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let mut fold = Folder::new(Registry::new((0..pool as u64).map(NodeId)));
            let loaded = paros::client::checkpoint::load(&mut fold, registry, &client, 0, 0).await;
            if let LoadOutcome::Loaded { .. } = loaded {
                tail = Some(fold.next_seq());
            } else if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                break;
            }
        }
        if ctx.shutdown().is_cancelled() {
            return;
        }
        assert_always!(
            tail.is_some(),
            "registry: the node registry is read to its tail after chaos"
        );
        let Some(tail) = tail else {
            return;
        };
        let board = crate::audit::system::system_board(ctx.state());
        let mut lagging = Vec::new();
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            lagging = crate::audit::system::lock(&board).registry_lagging(expected, tail);
            if lagging.is_empty() || ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                break;
            }
        }
        assert_always!(
            lagging.is_empty() || ctx.shutdown().is_cancelled(),
            "registry: every live node's fold reaches the registry's tail after chaos",
            { "tail" => tail, "lagging" => format!("{lagging:?}") }
        );
    }
}

/// A reader's fold of the cell control journal over the founding
/// `members`, the cell coordinator's liveness entries judged as they fold
/// (#211).
fn judged_cell(members: &[u64]) -> Folder<JudgedRegistry> {
    Folder::new(JudgedRegistry::new(members.iter().copied().map(NodeId)))
}

/// The fleet directory against the cell's tenant list (FDB's
/// `MetaclusterConsistency`) for two folds of one instant: the same fleet,
/// the cell in the directory, every tenant the cell hosts in the directory
/// under that cell, every `READY` `users` tenant in the directory hosted.
/// After chaos, no served tenant is left mid-operation: each is `READY`,
/// or `ERROR` once its cell refused its footprint (#476).
fn no_tenant_mid_operation(directory: &FleetDirectory, cell: &Registry) {
    for (tenant, entry) in directory.tenants() {
        if entry.kind == TenantKind::Users {
            // A creation its cell refused for its footprint ends `ERROR`
            // (#476): that is an end, not the middle of an operation.
            let refused = cell.footprint(tenant).is_some_and(|g| !g.granted());
            assert_always!(
                matches!(entry.state, TenantState::Ready)
                    || (entry.state == TenantState::Error && refused),
                "fleet: no tenant is left mid-operation after chaos",
                { "tenant" => tenant.0, "state" => format!("{:?}", entry.state) }
            );
        }
    }
}

/// Whether the cell is lost: a majority of the founding members wiped
/// after a vote named them (#246), so its control plane owes nothing. Asked
/// again after the final read, since a wipe may land during it.
fn lost_cell(ctx: &SimContext) -> bool {
    let lost = crate::machine::cell_lost(ctx.state());
    if lost {
        assert_reachable!(
            "machine: a run whose founding member was wiped during init ends without its cell"
        );
    }
    lost
}

fn directory_equals_cell(directory: &FleetDirectory, cell: &Registry) {
    let (Some(fleet), Some(joined)) = (directory.fleet(), cell.fleet()) else {
        return;
    };
    // The cell joins only after the fleet directory added it: both name one
    // fleet, and the directory holds the cell.
    assert_always!(
        fleet == joined.fleet_id && directory.cell(joined.cell_id).is_some(),
        "fleet: the directory and the cell name the same fleet and the cell is in it",
        { "directory_fleet" => fleet, "cell_fleet" => joined.fleet_id }
    );
    for tenant in cell.hosted() {
        assert_always!(
            directory
                .tenant(tenant)
                .is_some_and(|t| t.kind == TenantKind::Users && t.cell_id == joined.cell_id),
            "fleet: every tenant the cell hosts is in the directory under that cell",
            { "tenant" => tenant.0 }
        );
    }
    for (tenant, entry) in directory.tenants() {
        // A tenant is marked `ERROR` only once its cell refused its
        // footprint (#476), and such a tenant is never hosted.
        if entry.state == TenantState::Error {
            assert_always!(
                cell.footprint(tenant).is_some_and(|g| !g.granted()) && !cell.hosts(tenant),
                "fleet: a tenant marked ERROR is one its cell refused for its footprint",
                { "tenant" => tenant.0 }
            );
        }
        if entry.kind == TenantKind::Users && entry.state == TenantState::Ready {
            assert_always!(
                cell.hosts(tenant),
                "fleet: every READY tenant in the directory is hosted by its cell",
                { "tenant" => tenant.0 }
            );
        }
    }
}

/// Each fleet [`Stage`] a step wrote, its own reachable (#247): every
/// state machine's every step is proven written by some run.
fn reach(stage: Stage) {
    match stage {
        Stage::FormFleet => assert_reachable!("fleet: a step forms the fleet in the directory"),
        Stage::AddCell => assert_reachable!("fleet: a step adds the cell to the directory"),
        Stage::CellReady => {
            assert_reachable!("fleet: a step marks the cell READY in the directory");
        }
        Stage::RegisterTenant => {
            assert_reachable!("fleet: a step registers a tenant in the directory");
        }
        Stage::TenantError => {
            assert_reachable!("fleet: a tenant is refused for its footprint");
        }
        Stage::TenantReady => {
            assert_reachable!("fleet: a step marks a tenant READY in the directory");
        }
        Stage::TenantRemoving => {
            assert_reachable!("fleet: a step marks a tenant REMOVING in the directory");
        }
        Stage::RemoveTenant => {
            assert_reachable!("fleet: a step removes a tenant from the directory");
        }
        Stage::RegisterMachine => {
            assert_reachable!("admit: a step registers a machine in the cell");
        }
        Stage::Admit => assert_reachable!("admit: a step admits a machine"),
        Stage::AddRootKey => {
            assert_reachable!("fleet: a step adds a root key to the universe entry");
        }
        Stage::SetTags => assert_reachable!("fleet: a step sets an entity's tags"),
        Stage::RemoveRootKey => {
            assert_reachable!("fleet: a step removes a root key from the universe entry");
        }
    }
}

/// Every fold a session made found each checkpoint its prefix's state
/// (#247): the fleet directory's owner checkpoints as it writes, the
/// registry's owner too, and a fold that meets a checkpoint with the whole prefix folded compares
/// the two — the registry owner's oracle, for both of a session's journals.
fn judge_folds(session: &FleetSession) {
    let diverged = session.diverged();
    assert_always!(
        diverged.is_none(),
        "checkpoint: an owner's load finds each checkpoint its prefix's state",
        {
            "journal" => diverged.map(|(j, _)| j.to_string()).unwrap_or_default(),
            "seq" => diverged.map_or(0, |(_, seq)| seq)
        }
    );
}

/// A tenant identifier spread from one draw (#226: random, never a position; no
/// range is reserved, §3.8): a set tenant id and a set control journal id.
fn tenant_identifier(draw: u64) -> JournalIdentifier {
    let tenant = crate::chain::splitmix(draw).max(1);
    let journal = crate::chain::splitmix(draw ^ 0xc0_7e01).max(1);
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

/// Read where `journal` stands now; its floor when `writer` still owns it
/// and nothing was written past `folded`, else `None`.
async fn still(
    client: &ChainClient,
    first: usize,
    journal: JournalIdentifier,
    writer: &Writer,
    folded: u64,
) -> Option<u64> {
    let state = client.journal_state(journal, first).await?;
    (writer.owned().is_some() && writer.owned() == state.leader && state.next_seq.0 == folded)
        .then_some(state.first_seq.0)
}
