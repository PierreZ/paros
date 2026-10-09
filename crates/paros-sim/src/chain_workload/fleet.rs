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

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_sim::{
    RandomProvider, SimContext, TimeProvider, assert_always, assert_reachable, assert_sometimes,
    buggify_with_prob,
};
use paros::client::Writer;
use paros::client::checkpoint::{CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome};
use paros::client::fleet::{FleetRefusal, FleetSession, Run, Stage, Step};
use paros::fleet::{CellState, FleetDirectory, Groups, TenantState};
use paros::machine::ControlJournals;
use paros::system::Registry;
use paros::{JournalId, JournalIdentifier, NodeId, TenantId};

use crate::client::{ChainClient, Connector};
use crate::shape::MachineLayout;

mod cell;

use cell::Cell;

/// The tenant names an operation is drawn from: few, so two operators race
/// for one often.
const NAMES: [&[u8]; 3] = [b"acme", b"globex", b"initech"];

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
    machines: Vec<SocketAddr>,
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
    /// A fresh seed per session and checkpointer (#241).
    leader_seeds: super::LeaderSeeds,
    /// This client saw an `init` end.
    initialized: bool,
    /// Tenant identifiers this client had created: a deliberate reuse names one.
    ever_created: Vec<JournalIdentifier>,
    pending: Option<Pending>,
    /// How long into an operation its target is killed, and how long it
    /// stays down (`ChainConfig::fleet_kill_delay_ms`, `fleet_kill_down_ms`).
    kill_ms: (u64, u64),
    /// The pending operation was cut short by its target's kill.
    killed: bool,
    /// The journals this operator learned (#246): every call it makes at
    /// the cell names one of them.
    learned: super::system::Learned,
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
        let machines = crate::machine::machine_addrs(deployment)?;
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
            layout,
            cell: None,
            patience,
            registry,
            client_id,
            leader_seeds,
            initialized: false,
            ever_created: Vec::new(),
            pending: None,
            kill_ms,
            killed: false,
            learned: super::system::Learned::default(),
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
        let cell = match &self.cell {
            Some(_) if buggify_with_prob!(0.1) => {
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
            if let Step::Advanced(stage) = session.init_step(&client, first, draw | 1).await {
                assert_reachable!("fleet: an init stops after one step");
                reach(stage);
                self.pending = Some(Pending::Init);
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
        let ip = cell.ip(first).filter(|_| buggify_with_prob!(0.1));
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
        if class % 2 == 0 {
            if buggify_with_prob!(0.2) {
                let identifier = tenant_identifier(payload);
                let step = session.create_step(&client, first, &name, identifier).await;
                if let Step::Advanced(stage) = step {
                    assert_reachable!("fleet: a tenant creation stops after one step");
                    reach(stage);
                    self.pending = Some(Pending::Create(name, vec![identifier]));
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
                if let Step::Advanced(stage) = session.remove_step(&client, first, &name).await {
                    assert_reachable!("fleet: a tenant removal stops after one step");
                    reach(stage);
                    self.pending = Some(Pending::Remove(name));
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
        let Some(pending) = self.pending.take() else {
            return true;
        };
        if matches!(pending, Pending::Initialize) {
            if self.initialize(ctx, draw).await {
                return true;
            }
            self.pending = Some(pending);
            return false;
        }
        let Some(cell) = self.learn(ctx).await else {
            self.pending = Some(pending);
            return false;
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
            Step::Interrupted(_) | Step::Advanced(_) => false,
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
        let run = session
            .create_tenant(client, first, &name, draws.clone(), Duration::ZERO)
            .await;
        run.steps.iter().copied().for_each(reach);
        self.created(session, &name, &run);
        match run.outcome {
            Step::Done { .. } => {
                self.check_directory(client, session, first).await;
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
            Step::Interrupted(_) | Step::Advanced(_) => false,
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
                    && t.groups == Groups::SERVED)
                    && session.cell().hosts(result),
                "fleet: a created tenant is READY in the directory and hosted by its cell",
                { "tenant" => result.0 }
            );
            assert_sometimes!(
                run.steps.first() != Some(&Stage::RegisterTenant),
                "fleet: a tenant creation resumed from REGISTERING"
            );
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
            Step::Interrupted(_) | Step::Advanced(_) => false,
        }
    }

    /// The fleet directory against the cell's tenant list, when the session's
    /// two folds are one instant's: it holds both journals already (the
    /// check claims nothing), and a fresh read of each finds this client
    /// still the owner with nothing written past the fold.
    async fn check_directory(&self, client: &ChainClient, session: &FleetSession, first: usize) {
        let Some(cell) = &self.cell else {
            return;
        };
        if !session.holds_both() {
            return;
        }
        let (directory_writer, cell_writer) = session.writers();
        let Some(directory_floor) = still(
            client,
            first,
            cell.fleet,
            directory_writer,
            session.directory().next_seq(),
        )
        .await
        else {
            return;
        };
        if still(
            client,
            first,
            cell.journals.cell,
            cell_writer,
            session.cell().next_seq(),
        )
        .await
        .is_none()
        {
            return;
        }
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
        let deadline = ctx.time().now() + FLEET_SETTLE;
        self.final_fleet(ctx, deadline).await;
        self.final_registry(ctx, nodes, expected, deadline).await;
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
        if crate::machine::cell_lost(ctx.state()) {
            // A founding member wiped during `init` after a vote named it
            // (#246): no `init` forms the cell, and its control plane owes
            // nothing.
            assert_reachable!(
                "machine: a run whose founding member was wiped during init ends without its cell"
            );
            return;
        }
        let mut learned = None;
        while learned.is_none() && ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            learned = self.learn(ctx).await;
            if learned.is_none() && ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                break;
            }
        }
        if ctx.shutdown().is_cancelled() {
            return;
        }
        assert_always!(
            learned.is_some(),
            "fleet: a formed cell's control journals are learned through Inspect after chaos"
        );
        let Some(known) = learned else {
            return;
        };
        let mut folds = None;
        let mut attempt = 0_u64;
        while folds.is_none() && ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let first = known.first(attempt);
            attempt += 1;
            let directory =
                paros::client::fleet::read_directory(&known.client, first, known.fleet).await;
            let mut cell = Folder::new(Registry::new(known.members.iter().copied().map(NodeId)));
            let loaded = paros::client::checkpoint::load(
                &mut cell,
                known.journals.cell,
                &known.client,
                first,
                0,
            )
            .await;
            if let (Ok(directory), LoadOutcome::Loaded { .. }) = (directory, loaded) {
                folds = Some((directory, cell));
            } else if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
                break;
            }
        }
        if ctx.shutdown().is_cancelled() {
            return;
        }
        assert_always!(
            folds.is_some(),
            "fleet: the directory and the cell's journal are read to their tails after chaos"
        );
        let Some((directory, cell)) = folds else {
            return;
        };
        let cell = cell.state();
        let cell_id = known.journals.cell_id;
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
        for (tenant, entry) in directory.tenants() {
            if entry.groups == Groups::SERVED {
                assert_always!(
                    matches!(entry.state, TenantState::Ready),
                    "fleet: no tenant is left mid-operation after chaos",
                    { "tenant" => tenant.0, "state" => format!("{:?}", entry.state) }
                );
            }
        }
        directory_equals_cell(&directory, cell);
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

/// The fleet directory against the cell's tenant list (FDB's
/// `MetaclusterConsistency`) for two folds of one instant: the same fleet,
/// the cell in the directory, every tenant the cell hosts in the directory
/// under that cell, every `READY` `users` tenant in the directory hosted.
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
                .is_some_and(|t| t.groups == Groups::SERVED && t.cell_id == joined.cell_id),
            "fleet: every tenant the cell hosts is in the directory under that cell",
            { "tenant" => tenant.0 }
        );
    }
    for (tenant, entry) in directory.tenants() {
        if entry.groups == Groups::SERVED && entry.state == TenantState::Ready {
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
        Stage::JoinFleet => assert_reachable!("fleet: a step joins the cell to the fleet"),
        Stage::CellReady => {
            assert_reachable!("fleet: a step marks the cell READY in the directory");
        }
        Stage::RegisterTenant => {
            assert_reachable!("fleet: a step registers a tenant in the directory");
        }
        Stage::HostTenant => assert_reachable!("fleet: a step hosts a tenant on the cell"),
        Stage::TenantReady => {
            assert_reachable!("fleet: a step marks a tenant READY in the directory");
        }
        Stage::TenantRemoving => {
            assert_reachable!("fleet: a step marks a tenant REMOVING in the directory");
        }
        Stage::DropTenant => assert_reachable!("fleet: a step drops a tenant from the cell"),
        Stage::RemoveTenant => {
            assert_reachable!("fleet: a step removes a tenant from the directory");
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
