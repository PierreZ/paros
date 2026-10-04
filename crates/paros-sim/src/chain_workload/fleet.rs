//! The chain client's **fleet operations** (#229): `init`'s fleet half and
//! creating and removing tenants, run through the library's
//! `paros::client::fleet` — the code `parosctl` ships — against meta's
//! control journal (`1/1`) and the cell control journal (`2/1`) at the
//! seeds.
//!
//! Every client is an operator: several run fleet operations at once and
//! fence each other through the journals' generations, and an operation
//! that loses is resumed later from what the journals hold. Two shapes make
//! the state machines' middles likely:
//!
//! - **a crash at a step** (one location per operation): the operator takes
//!   one step and stops, as if it died there; its next fleet step resumes the
//!   same operation, which must end where an uninterrupted one would;
//! - **a changed identity**: an `init` told another cell's id, and a tenant
//!   created under an id this client had already used — both must be
//!   refused.
//!
//! **Meta's directory equals the cell's tenant list** (FDB's
//! `MetaclusterConsistency`): after each operation the client holds both
//! journals, and when a fresh read finds neither written since its folds —
//! so the two folds are one instant's — every tenant the cell hosts is in
//! meta under that cell, and every `READY` one in meta is hosted. A tenant
//! mid-operation (`REGISTERING`, `REMOVING`) may be either.
//!
//! No function here draws randomness: every choice is read off the
//! caller's step draws.

use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::fleet::{FleetRefusal, FleetSession, Stage, Step};
use paros::client::{ReadOutcome, Writer};
use paros::meta::{CellState, META, TenantState};
use paros::system::{REGISTRY, Registry};
use paros::{JournalKey, NodeId, Read, TenantId};

use super::system::Announce;
use crate::client::ChainClient;

/// The tenant names an operation is drawn from: few, so two operators race
/// for one often.
const NAMES: [&[u8]; 3] = [b"acme", b"globex", b"initech"];

/// The per-run cell id's key: one cell per run, minted by whoever asks
/// first, like `init` mints it once.
const CELL_ID_KEY: &str = "paros-cell-id";

/// An operation this client stopped in the middle of (the crash shape), to
/// resume on its next fleet step.
#[derive(Clone, Debug)]
enum Pending {
    Init,
    Create(Vec<u8>),
    Remove(Vec<u8>),
}

/// The chain client's fleet state across its steps.
pub(super) struct FleetOps {
    /// The run runs the system journals (and so meta).
    active: bool,
    /// How many genesis ranks host the system journals.
    seeds: usize,
    /// The genesis pool size: the registry's genesis.
    pool: usize,
    client_id: u64,
    /// This client saw an `init` end.
    initialized: bool,
    /// Tenant ids this client had created: a deliberate reuse names one.
    ever_created: Vec<TenantId>,
    pending: Option<Pending>,
}

impl FleetOps {
    /// The fleet operations of client `client_id` on `deployment`.
    pub(super) fn new(deployment: &crate::roles::Deployment, active: bool, client_id: u64) -> Self {
        let pool = deployment.acceptors().len();
        Self {
            active,
            seeds: crate::shape::seed_ranks(pool).len().max(1),
            pool,
            client_id,
            initialized: false,
            ever_created: Vec::new(),
            pending: None,
        }
    }

    /// A session writing both journals as this client: meta as its
    /// operator, the cell's as its coordinator.
    fn session(&self, policy: CheckpointPolicy) -> FleetSession {
        FleetSession::new(
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
            .with_observer(std::sync::Arc::new(Announce::new(ctx, &[META, REGISTRY])))
            .rotating_over(self.seeds.min(nodes.server_count()).max(1))
    }

    /// `FLEET_INIT`: run `init`'s fleet half for the run's one cell — or
    /// resume the operation this client stopped in the middle of.
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
        let cell_id = *crate::state::published_arc(ctx.state(), CELL_ID_KEY, || {
            crate::chain::splitmix(draw) | 1
        });
        let client = self.client(ctx, nodes);
        let first = self.first(draw);
        let mut session = self.session(policy);
        if self.initialized && buggify_with_prob!(0.1) {
            // An operator talking to another cell than the one meta holds.
            let run = session
                .init(&client, first, Some(cell_id ^ 2), draw | 1)
                .await;
            assert_always!(
                !matches!(run.outcome, Step::Done { .. }),
                "fleet: an init naming another cell never ends",
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
        if buggify_with_prob!(0.2) {
            if let Step::Advanced(_) = session
                .init_step(&client, first, Some(cell_id), draw | 1)
                .await
            {
                assert_reachable!("fleet: an init stops after one step");
                self.pending = Some(Pending::Init);
            }
            return;
        }
        self.finish_init(&client, &mut session, first, cell_id, draw)
            .await;
    }

    /// `TENANT`: create (an even `class`) or remove a tenant named from the
    /// alphabet — or resume the operation this client stopped in.
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
        let mut session = self.session(policy);
        let crash = buggify_with_prob!(0.2);
        if class % 2 == 0 {
            if crash {
                let step = session
                    .create_step(&client, first, &name, tenant_id(payload))
                    .await;
                if let Step::Advanced(_) = step {
                    assert_reachable!("fleet: a tenant creation stops after one step");
                    self.pending = Some(Pending::Create(name));
                }
                return;
            }
            self.create(&client, &mut session, first, name, payload)
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

    /// The server a call starts at.
    fn first(&self, draw: u64) -> usize {
        usize::try_from(draw % self.seeds as u64).unwrap_or(0)
    }

    /// Run the operation this client stopped in, to its end.
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
        let mut session = self.session(policy);
        match pending {
            Pending::Init => {
                let cell_id = *crate::state::published_arc(ctx.state(), CELL_ID_KEY, || {
                    crate::chain::splitmix(draw) | 1
                });
                // A re-run is told nothing it could get wrong: the cell's
                // id and the fleet's are read off the cell's side.
                if !self
                    .finish_init(&client, &mut session, first, cell_id, draw)
                    .await
                {
                    self.pending = Some(Pending::Init);
                }
            }
            Pending::Create(name) => {
                if !self
                    .create(&client, &mut session, first, name.clone(), draw)
                    .await
                {
                    self.pending = Some(Pending::Create(name));
                }
            }
            Pending::Remove(name) => {
                if !self.remove(&client, &mut session, first, &name).await {
                    self.pending = Some(Pending::Remove(name));
                }
            }
        }
    }

    /// Run `init` to its end; whether it ended (done or refused).
    async fn finish_init(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        cell_id: u64,
        draw: u64,
    ) -> bool {
        let run = session.init(client, first, Some(cell_id), draw | 1).await;
        match run.outcome {
            Step::Done {
                result: (fleet, cell),
                last,
            } => {
                assert_always!(
                    cell == cell_id,
                    "fleet: an init ends on the run's one cell",
                    { "cell" => cell, "expected" => cell_id }
                );
                let finished = last == Some(Stage::CellReady);
                if finished {
                    let meta_cell = session.meta().cell(cell).map(|c| c.state);
                    let joined = session.cell().fleet().map(|f| (f.fleet_id, f.cell_id));
                    assert_always!(
                        meta_cell == Some(CellState::Ready) && joined == Some((fleet, cell)),
                        "fleet: a finished init leaves the cell READY in meta and joined on its side"
                    );
                    assert_sometimes!(
                        run.steps.first() != Some(&Stage::JoinFleet),
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

    /// Create `name` to its end, redrawing a taken id once; whether it
    /// ended.
    async fn create(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        name: Vec<u8>,
        draw: u64,
    ) -> bool {
        // An id this client had created — the collision a random u64 never
        // makes on its own — must be refused, and the creator redraws.
        let reuse = !self.ever_created.is_empty() && buggify_with_prob!(0.2);
        let mut id = if reuse {
            self.ever_created[usize::try_from(draw % self.ever_created.len() as u64).unwrap_or(0)]
        } else {
            tenant_id(draw)
        };
        for attempt in 0..2_u64 {
            let run = session.create_tenant(client, first, &name, id).await;
            match run.outcome {
                Step::Done { result, last } => {
                    let created = last == Some(Stage::TenantReady);
                    if created {
                        let entry = session.meta().tenant(result);
                        assert_always!(
                            entry.is_some_and(|t| t.state == TenantState::Ready && t.name == name)
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
                    if !self.ever_created.contains(&result) {
                        self.ever_created.push(result);
                    }
                    self.check_directory(client, session, first).await;
                    return true;
                }
                Step::Refused(FleetRefusal::IdTaken) if attempt == 0 => {
                    if reuse {
                        assert_reachable!("fleet: a duplicate tenant id is refused");
                    }
                    id = tenant_id(draw.rotate_left(23) ^ 0x7e57);
                }
                Step::Refused(FleetRefusal::NotInitialized) => {
                    assert_always!(
                        !self.initialized,
                        "fleet: a tenant is refused as uninitialized only before init ends",
                        { "client" => self.client_id }
                    );
                    assert_reachable!("fleet: a tenant operation before init is refused");
                    return true;
                }
                Step::Refused(_) => return true,
                Step::Interrupted(_) | Step::Advanced(_) => return false,
            }
        }
        true
    }

    /// Remove `name` to its end; whether it ended.
    async fn remove(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
        name: &[u8],
    ) -> bool {
        let run = session.remove_tenant(client, first, name).await;
        match run.outcome {
            Step::Done { result, last } => {
                let removed = last == Some(Stage::RemoveTenant);
                if let (Some(tenant), true) = (result, removed) {
                    assert_always!(
                        session.meta().named(name).is_none()
                            && session.meta().is_removed(tenant)
                            && !session.cell().hosts(tenant),
                        "fleet: a removed tenant is gone from meta and from its cell",
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
    /// two folds are one instant's: it holds both journals, and a fresh read
    /// of each finds this client still the owner with nothing written past
    /// the fold.
    async fn check_directory(
        &self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
    ) {
        if session.open(client, first).await.is_err() {
            return;
        }
        let (meta_writer, cell_writer) = session.writers();
        let meta_at = (meta_writer.owned(), session.meta().next_seq());
        let cell_at = (cell_writer.owned(), session.cell().next_seq());
        let Some(meta_now) = still(client, first, META, meta_writer, meta_at.1).await else {
            return;
        };
        if still(client, first, REGISTRY, cell_writer, cell_at.1)
            .await
            .is_none()
        {
            return;
        }
        // The two folds are of one instant. Meta's checkpoint and truncation
        // ran under its owner's policy on the way.
        if meta_now > 0 {
            assert_reachable!("fleet: meta is read past a truncation to its checkpoint");
        }
        let meta = session.meta();
        let cell = session.cell();
        let (Some(fleet), Some(joined)) = (meta.fleet(), cell.fleet()) else {
            return;
        };
        assert_always!(
            fleet == joined.fleet_id && meta.cell(joined.cell_id).is_some(),
            "fleet: meta and the cell name the same fleet and the cell is in meta",
            { "meta_fleet" => fleet, "cell_fleet" => joined.fleet_id }
        );
        for tenant in cell.hosted() {
            assert_always!(
                meta.tenant(tenant)
                    .is_some_and(|t| t.cell_id == joined.cell_id),
                "fleet: every tenant the cell hosts is in meta under that cell",
                { "tenant" => tenant.0 }
            );
        }
        for (tenant, entry) in meta.tenants() {
            if entry.state == TenantState::Ready {
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

/// A tenant id in the user range, spread from one draw (#226: random, never
/// a position).
fn tenant_id(draw: u64) -> TenantId {
    let span = u64::MAX - TenantId::FIRST_USER.0;
    TenantId(TenantId::FIRST_USER.0 + crate::chain::splitmix(draw) % span)
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
    let read = Read {
        journal: journal.journal.0,
        tenant: journal.tenant.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    };
    let (ReadOutcome::Page { state, .. } | ReadOutcome::Truncated { state }) =
        client.read_any(&read, first).await.outcome
    else {
        return None;
    };
    (writer.owned() == Some(state.generation.0)
        && state.owner.is_some_and(|o| o.0 == writer.owner())
        && state.next_seq.0 == folded)
        .then_some(state.first_seq.0)
}
