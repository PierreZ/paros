//! The chain client's **fleet operations** (#229): `init`'s fleet half and
//! creating and removing tenants, run through the library's
//! `paros::client::fleet` — the code `parosctl` ships — against meta's
//! control journal and the cell control journal at the seeds. Every frame is
//! the run's drawn one (`crate::shape::Frames`: no frame is fixed, §3.8).
//!
//! Every client is an operator: several run fleet operations at once and
//! fence each other through the journals' generations, and an operation
//! that loses is resumed later from what the journals hold. Three shapes make
//! the state machines' middles likely, each a BUGGIFY location paired with a
//! reachable where it fires:
//!
//! - **a crash at a step** (one location per operation): the operator takes
//!   one step and stops, as if it died there; its next fleet step resumes the
//!   same operation, which must end where an uninterrupted one would;
//! - **a changed identity**: an `init` told another cell's id must be
//!   refused; a tenant created under an id this client had already used must
//!   be refused, and the creator redraws.
//!
//! **Meta's directory equals the cell's tenant list** (FDB's
//! `MetaclusterConsistency`): after an operation that left the session
//! holding both journals, when a fresh read finds neither written since its
//! folds — so the two folds are one instant's — every tenant the cell hosts
//! is in meta under that cell, and every `READY` `users` tenant in meta is
//! hosted. A tenant mid-operation (`REGISTERING`, `REMOVING`) may be either.
//! The check claims nothing of its own: a session that does not hold both
//! journals skips it.
//!
//! No function here draws randomness: every choice is read off the
//! caller's step draws.

use std::time::Duration;

use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::client::Writer;
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::fleet::{FleetRefusal, FleetSession, Run, Stage, Step};
use paros::machine::ControlJournals;
use paros::meta::{CellState, Groups, TenantState};
use paros::system::Registry;
use paros::{JournalId, JournalKey, NodeId, TenantId};

use super::system::Announce;
use crate::client::ChainClient;

/// The tenant names an operation is drawn from: few, so two operators race
/// for one often.
const NAMES: [&[u8]; 3] = [b"acme", b"globex", b"initech"];

/// An operation this client stopped in the middle of (the crash shape), to
/// resume on its next fleet step.
#[derive(Clone, Debug)]
enum Pending {
    Init,
    /// The name and the frame this client's creation drew.
    Create(Vec<u8>, JournalKey),
    Remove(Vec<u8>),
}

/// The chain client's fleet state across its steps.
pub(super) struct FleetOps {
    /// The run runs the system journals (and so meta).
    active: bool,
    /// The run's frames: the cell's id, the cell tenant's control journal
    /// (the registry) and meta's.
    frames: ControlJournals,
    /// Meta's control journal: the run's frames always name it.
    meta: JournalKey,
    /// How many genesis ranks host the system journals.
    seeds: usize,
    /// The genesis pool size: the registry's genesis.
    pool: usize,
    client_id: u64,
    /// This client saw an `init` end.
    initialized: bool,
    /// Tenant frames this client had created: a deliberate reuse names one.
    ever_created: Vec<JournalKey>,
    pending: Option<Pending>,
}

impl FleetOps {
    /// The fleet operations of client `client_id` on `deployment`.
    pub(super) fn new(
        deployment: &crate::roles::Deployment,
        frames: crate::shape::Frames,
        active: bool,
        client_id: u64,
    ) -> Self {
        let pool = deployment.acceptors().len();
        Self {
            active,
            frames: ControlJournals {
                cell_id: frames.cell_id,
                cell: frames.registry,
                fleet: Some(frames.meta),
            },
            meta: frames.meta,
            seeds: crate::shape::seed_ranks(pool).len().max(1),
            pool,
            client_id,
            initialized: false,
            ever_created: Vec::new(),
            pending: None,
        }
    }

    /// A session over `frames` writing both journals as this client: meta
    /// as its operator, the cell's as its coordinator. `None` only for
    /// frames that name no meta, which the run's never do.
    fn session(&self, frames: ControlJournals, policy: CheckpointPolicy) -> Option<FleetSession> {
        FleetSession::new(
            frames,
            self.client_id,
            NodeId(self.client_id),
            Registry::new((0..self.pool as u64).map(NodeId)),
            policy,
        )
    }

    /// The seeds' client, announcing every write to meta and the registry.
    fn client(&self, ctx: &SimContext, nodes: &ChainClient) -> ChainClient {
        nodes
            .clone()
            .with_observer(std::sync::Arc::new(Announce::new(
                ctx,
                &[self.meta, self.frames.cell],
            )))
            .rotating_over(self.seeds.min(nodes.server_count()).max(1))
    }

    /// The server a call starts at.
    fn first(&self, draw: u64) -> usize {
        usize::try_from(draw % self.seeds as u64).unwrap_or(0)
    }

    /// `FLEET_INIT`: run `init`'s fleet half for the run's one cell — or
    /// resume the operation this client stopped in the middle of.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn init(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        if !self.active {
            return;
        }
        if self.pending.is_some() {
            self.resume(ctx, nodes, policy, draw).await;
            return;
        }
        let client = self.client(ctx, nodes);
        let first = self.first(draw);
        if self.initialized && buggify_with_prob!(0.1) {
            // An operator talking to another cell than the one meta holds.
            assert_reachable!("fleet: an init is told another cell's id");
            let wrong = ControlJournals {
                cell_id: self.frames.cell_id ^ 2,
                ..self.frames
            };
            let Some(mut session) = self.session(wrong, policy) else {
                return;
            };
            let run = session.init(&client, first, draw | 1, Duration::ZERO).await;
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
        let Some(mut session) = self.session(self.frames, policy) else {
            return;
        };
        if buggify_with_prob!(0.2) {
            if let Step::Advanced(_) = session.init_step(&client, first, draw | 1).await {
                assert_reachable!("fleet: an init stops after one step");
                self.pending = Some(Pending::Init);
            }
            return;
        }
        self.finish_init(&client, &mut session, first, draw).await;
    }

    /// `TENANT`: create (an even `class`) or remove a `users` tenant named
    /// from the alphabet — or resume the operation this client stopped in.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn tenant(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        policy: CheckpointPolicy,
        (class, payload): (u64, u64),
    ) {
        if !self.active {
            return;
        }
        if self.pending.is_some() {
            self.resume(ctx, nodes, policy, payload).await;
            return;
        }
        let name = NAMES[usize::try_from(class % NAMES.len() as u64).unwrap_or(0)].to_vec();
        let client = self.client(ctx, nodes);
        let first = self.first(payload);
        let Some(mut session) = self.session(self.frames, policy) else {
            return;
        };
        let crash = buggify_with_prob!(0.2);
        if class % 2 == 0 {
            if crash {
                let frame = tenant_frame(payload);
                let step = session.create_step(&client, first, &name, frame).await;
                if let Step::Advanced(_) = step {
                    assert_reachable!("fleet: a tenant creation stops after one step");
                    self.pending = Some(Pending::Create(name, frame));
                }
                return;
            }
            let draws = vec![
                tenant_frame(payload),
                tenant_frame(payload.rotate_left(23) ^ 0x7e57),
            ];
            self.create(&client, &mut session, first, name, draws, payload)
                .await;
        } else {
            if crash {
                if let Step::Advanced(_) = session.remove_step(&client, first, &name).await {
                    assert_reachable!("fleet: a tenant removal stops after one step");
                    self.pending = Some(Pending::Remove(name));
                }
                return;
            }
            self.remove(&client, &mut session, first, &name).await;
        }
    }

    /// Run the operation this client stopped in, to its end.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    async fn resume(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let client = self.client(ctx, nodes);
        let first = self.first(draw);
        let Some(mut session) = self.session(self.frames, policy) else {
            self.pending = Some(pending);
            return;
        };
        let ended = match &pending {
            Pending::Init => self.finish_init(&client, &mut session, first, draw).await,
            Pending::Create(name, frame) => {
                // Only this creation's own frame resumes it (a tenant is
                // created once).
                self.create(
                    &client,
                    &mut session,
                    first,
                    name.clone(),
                    vec![*frame],
                    draw,
                )
                .await
            }
            Pending::Remove(name) => self.remove(&client, &mut session, first, name).await,
        };
        if !ended {
            self.pending = Some(pending);
        }
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
        match run.outcome {
            Step::Done {
                result: fleet,
                last,
            } => {
                let finished = last == Some(Stage::CellReady);
                if finished {
                    let cell = self.frames.cell_id;
                    let meta_cell = session.meta().cell(cell).map(|c| c.state);
                    let joined = session.cell().fleet().map(|f| (f.fleet_id, f.cell_id));
                    assert_always!(
                        meta_cell == Some(CellState::Ready) && joined == Some((fleet, cell)),
                        "fleet: a finished init leaves the cell READY in meta and joined on its side",
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

    /// Create `name` under the first of `draws` meta does
    /// not hold, to its end; whether it ended.
    async fn create(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        name: Vec<u8>,
        draws: Vec<JournalKey>,
        draw: u64,
    ) -> bool {
        // A frame this client had created — the collision a random u64 never
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
                let holder = session.meta().named(&name).map(|(t, _)| t);
                assert_always!(
                    holder == Some(tenant) && draws.iter().all(|d| d.tenant != tenant),
                    "fleet: a second creation of a name is refused for another frame",
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
            let entry = session.meta().tenant(result);
            assert_always!(
                entry.is_some_and(|t| t.state == TenantState::Ready
                    && t.name == name
                    && t.groups == Groups::SERVED)
                    && session.cell().hosts(result),
                "fleet: a created tenant is READY in meta and hosted by its cell",
                { "tenant" => result.0 }
            );
            assert_sometimes!(
                run.steps.first() != Some(&Stage::RegisterTenant),
                "fleet: a tenant creation resumed from REGISTERING"
            );
        }
        assert_sometimes!(created, "fleet: a tenant is created READY");
        if let Some(entry) = session.meta().tenant(result) {
            let frame = JournalKey::new(result, entry.control);
            if !self.ever_created.contains(&frame) {
                self.ever_created.push(frame);
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
        match run.outcome {
            Step::Done { result, last } => {
                let removed = last == Some(Stage::RemoveTenant);
                if let (Some(tenant), true) = (result, removed) {
                    assert_always!(
                        session.meta().named(name).is_none()
                            && session.meta().is_removed(tenant)
                            && !session.cell().hosts(tenant)
                            && session.cell().dropped(tenant),
                        "fleet: a removed tenant is gone from meta and tombstoned on its cell",
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

    /// Meta's directory against the cell's tenant list, when the session's
    /// two folds are one instant's: it holds both journals already (the
    /// check claims nothing), and a fresh read of each finds this client
    /// still the owner with nothing written past the fold.
    async fn check_directory(&self, client: &ChainClient, session: &FleetSession, first: usize) {
        if !session.holds_both() {
            return;
        }
        let (meta_writer, cell_writer) = session.writers();
        let Some(meta_floor) = still(
            client,
            first,
            self.meta,
            meta_writer,
            session.meta().next_seq(),
        )
        .await
        else {
            return;
        };
        if still(
            client,
            first,
            self.frames.cell,
            cell_writer,
            session.cell().next_seq(),
        )
        .await
        .is_none()
        {
            return;
        }
        // The two folds are of one instant. Meta's checkpoint and truncation
        // ran under its owner's policy on the way.
        if meta_floor > 0 {
            assert_reachable!("fleet: meta is read past a truncation to its checkpoint");
        }
        let meta = session.meta();
        let cell = session.cell();
        let (Some(fleet), Some(joined)) = (meta.fleet(), cell.fleet()) else {
            return;
        };
        // The cell joins only after meta added it: both name one fleet, and
        // meta holds the cell.
        assert_always!(
            fleet == joined.fleet_id && meta.cell(joined.cell_id).is_some(),
            "fleet: meta and the cell name the same fleet and the cell is in meta",
            { "meta_fleet" => fleet, "cell_fleet" => joined.fleet_id }
        );
        for tenant in cell.hosted() {
            assert_always!(
                meta.tenant(tenant)
                    .is_some_and(|t| t.groups == Groups::SERVED && t.cell_id == joined.cell_id),
                "fleet: every tenant the cell hosts is in meta under that cell",
                { "tenant" => tenant.0 }
            );
        }
        for (tenant, entry) in meta.tenants() {
            if entry.groups == Groups::SERVED && entry.state == TenantState::Ready {
                assert_always!(
                    cell.hosts(tenant),
                    "fleet: every READY tenant in meta is hosted by its cell",
                    { "tenant" => tenant.0 }
                );
            }
        }
        assert_reachable!("fleet: meta's directory is checked against the cell's tenant list");
    }
}

/// A tenant frame spread from one draw (#226: random, never a position; no
/// range is reserved, §3.8): a set tenant id and a set control journal id.
fn tenant_frame(draw: u64) -> JournalKey {
    let tenant = crate::chain::splitmix(draw).max(1);
    let journal = crate::chain::splitmix(draw ^ 0xc0_7e01).max(1);
    JournalKey::new(TenantId(tenant), JournalId(journal))
}

/// Read where `journal` stands now; its floor when `writer` still owns it
/// and nothing was written past `folded`, else `None`.
async fn still(
    client: &ChainClient,
    first: usize,
    journal: JournalKey,
    writer: &Writer,
    folded: u64,
) -> Option<u64> {
    let state = client.journal_state(journal, first).await?;
    (writer.owned() == Some(state.generation.0)
        && state.owner.is_some_and(|o| o.0 == writer.owner())
        && state.next_seq.0 == folded)
        .then_some(state.first_seq.0)
}
