//! The chain client's **fleet operations** (#229): `init`'s fleet steps,
//! creating and removing tenants — `paros::client::fleet`'s resumable state
//! machines over meta (`1/1`) and the cell control journal (`2/1`), stepped
//! one entry at a time against the seeds.
//!
//! An operator may stop between any two steps (one BUGGIFY location per
//! operation): the operation is left half done, and the next client to run
//! it — any client, under any draw — resumes it from what the two journals
//! hold. Tenant names come from a four-name alphabet, so a creation often
//! meets a name another client left `REGISTERING`, and resumes it.
//!
//! Two deliberate misbehaviours, each its own location, each judged where
//! it lands: a creation under a tenant id this client registered before
//! (meta must refuse it, and the creator redraws), and a creation resumed
//! with a stale context naming another fleet (the step must refuse it).
//!
//! At the end of the run, client 0 reads meta and the cell back and checks
//! they agree (FDB's metacluster consistency check, with one cell): every
//! tenant the cell hosts is in meta assigned to it, and every `READY`
//! tenant meta assigns to the cell is hosted by it.
//!
//! No function here draws randomness: every choice is read off the caller's
//! step draws.

use std::time::Duration;

use moonpool_sim::{
    SimContext, TimeProvider, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::TenantId;
use paros::client::fleet::{
    self, CellRegistration, FleetAction, FleetRefusal, FleetStep, TenantCreation, TenantRemoval,
};
use paros::system::{FleetContext, META, REGISTRY, TenantState};

use crate::client::ChainClient;

/// The tenant names a creation draws from: few, so creations and removals
/// meet the same name often.
const TENANT_NAMES: [&[u8]; 4] = [b"acme", b"globex", b"initech", b"umbrella"];

/// The most steps one operation runs before its client moves on (it ends in
/// at most four when nothing races it).
const MAX_STEPS: usize = 12;

/// The most unavailable steps one operation absorbs before its client moves
/// on (the operation resumes on a later step).
const UNAVAILABLE_STEPS: usize = 3;

/// A tenant id in the user range, spread from one draw (random, never a
/// log position).
fn drawn_tenant(draw: u64) -> TenantId {
    let span = u64::MAX - TenantId::FIRST_USER.0;
    TenantId(TenantId::FIRST_USER.0 + crate::chain::splitmix(draw) % span)
}

/// The chain client's fleet state across its steps.
pub(super) struct FleetOps {
    /// The run runs the system journals.
    active: bool,
    /// How many genesis ranks host the system journals.
    seeds: usize,
    client_id: u64,
    /// The run's cell id (what `init` would mint).
    cell_id: u64,
    /// The context this client's last finished creation reported.
    context: Option<FleetContext>,
    /// Every tenant id this client registered.
    registered: Vec<TenantId>,
    pause: Duration,
}

impl FleetOps {
    /// The fleet operations of client `client_id`: `active` when the run
    /// runs the system journals, over `pool` genesis nodes.
    pub(super) fn new(
        ctx: &SimContext,
        active: bool,
        pool: usize,
        client_id: u64,
        pause: Duration,
    ) -> Self {
        Self {
            active,
            seeds: crate::shape::seed_ranks(pool).len().max(1),
            client_id,
            cell_id: crate::shape::cell_id(ctx.state()),
            context: None,
            registered: Vec::new(),
            pause,
        }
    }

    fn client(&self, ctx: &SimContext, nodes: &ChainClient) -> ChainClient {
        super::system::seed_client(ctx, nodes, self.seeds, &[META, REGISTRY])
    }

    /// The seed a step starts at.
    fn first(&self, draw: u64) -> usize {
        usize::try_from(draw % self.seeds as u64).unwrap_or(0)
    }

    /// Wait out an unavailable step.
    async fn wait(&self, ctx: &SimContext) {
        let _ = ctx.time().sleep(self.pause).await;
    }

    /// `INIT_FLEET`: `init`'s fleet steps — register the run's cell in meta
    /// (minting the fleet from the draw), the cell's half, the cell `READY`.
    pub(super) async fn init(&mut self, ctx: &SimContext, nodes: &ChainClient, draw: u64) -> bool {
        if !self.active {
            return false;
        }
        let client = self.client(ctx, nodes);
        let first = self.first(draw);
        let mut registration = CellRegistration::new(
            self.client_id,
            self.cell_id,
            crate::chain::splitmix(draw).max(1),
        );
        let mut wrote: Vec<FleetAction> = Vec::new();
        let mut unavailable = 0;
        for _ in 0..MAX_STEPS {
            if !wrote.is_empty() && buggify_with_prob!(0.15) {
                // An operator that stops between two steps: the next run
                // resumes the registration.
                assert_reachable!("fleet: an init stops between two of its steps");
                return false;
            }
            match registration.step(&client, first).await {
                FleetStep::Stepped(action) => wrote.extend(action),
                FleetStep::Done(context) => {
                    assert_always!(
                        context.cell_id == self.cell_id,
                        "fleet: init registers the cell it was run for",
                        { "cell" => context.cell_id }
                    );
                    // A run that finished what another one started.
                    assert_sometimes!(
                        !wrote.is_empty() && !wrote.contains(&FleetAction::RegisterCell),
                        "fleet: an init resumes a registration another run started"
                    );
                    return true;
                }
                FleetStep::Refused(refusal) => {
                    // The cell is the run's own and meta has one fleet: no
                    // refusal is possible.
                    assert_always!(
                        false,
                        "fleet: init is never refused in a one-cell fleet",
                        { "refusal" => format!("{refusal:?}") }
                    );
                    return false;
                }
                FleetStep::IdTaken | FleetStep::Unavailable => {
                    unavailable += 1;
                    if unavailable >= UNAVAILABLE_STEPS {
                        return false;
                    }
                    self.wait(ctx).await;
                }
            }
        }
        false
    }

    /// `CREATE_TENANT`: create a tenant named from the alphabet — or resume
    /// one another run left half done — under an id drawn here.
    pub(super) async fn create(
        &mut self,
        ctx: &SimContext,
        nodes: &ChainClient,
        (class, payload): (u64, u64),
    ) {
        if !self.active {
            return;
        }
        let name =
            TENANT_NAMES[usize::try_from(class % TENANT_NAMES.len() as u64).unwrap_or(0)].to_vec();
        let client = self.client(ctx, nodes);
        let first = self.first(payload);
        // A deliberate reuse of an id this client registered before — the
        // collision a random u64 never makes on its own: meta must refuse it,
        // and the creator redraws.
        let reuse = !self.registered.is_empty() && buggify_with_prob!(0.2);
        let candidate = if reuse {
            assert_reachable!("fleet: a creation draws a tenant id it registered before");
            self.registered[usize::try_from(payload % self.registered.len() as u64).unwrap_or(0)]
        } else {
            drawn_tenant(payload ^ class.rotate_left(29))
        };
        let mut creation = TenantCreation::new(self.client_id, name, candidate);
        // A resume with a context saved from another fleet (a stale record):
        // the creation must refuse it, never complete under it.
        let stale = self.context.filter(|_| buggify_with_prob!(0.1));
        if let Some(context) = stale {
            assert_reachable!("fleet: a creation is resumed with another fleet's context");
            creation = creation.resume(FleetContext {
                fleet_id: context.fleet_id.wrapping_add(1).max(1),
                ..context
            });
        }
        let mut wrote: Vec<FleetAction> = Vec::new();
        let mut unavailable = 0;
        let mut redraws = 0_u64;
        for _ in 0..MAX_STEPS {
            if !wrote.is_empty() && buggify_with_prob!(0.15) {
                assert_reachable!("fleet: a tenant creation stops between two of its steps");
                return;
            }
            let step = creation.step(&client, first).await;
            if matches!(step, FleetStep::Stepped(Some(FleetAction::RegisterTenant))) {
                self.note_registered(creation.tenant());
            }
            match step {
                FleetStep::Stepped(action) => wrote.extend(action),
                FleetStep::Done(context) => {
                    assert_always!(
                        stale.is_none(),
                        "fleet: a creation resumed with a stale context never completes"
                    );
                    self.context = Some(context);
                    assert_sometimes!(true, "fleet: a tenant becomes ready");
                    // Resumed: this run wrote the later steps of a tenant
                    // another run registered.
                    assert_sometimes!(
                        !wrote.contains(&FleetAction::RegisterTenant)
                            && wrote.iter().any(|a| matches!(
                                a,
                                FleetAction::HostTenant | FleetAction::TenantReady
                            )),
                        "fleet: a tenant creation resumed from REGISTERING"
                    );
                    return;
                }
                FleetStep::IdTaken => {
                    if reuse {
                        assert_reachable!("fleet: a creation redraws a tenant id meta refused");
                    }
                    redraws += 1;
                    creation.redraw(drawn_tenant(payload.rotate_left(17) ^ redraws));
                }
                FleetStep::Refused(FleetRefusal::ContextChanged) if stale.is_some() => {
                    assert_reachable!("fleet: a creation with a stale context is refused");
                    return;
                }
                FleetStep::Refused(FleetRefusal::NoFleet | FleetRefusal::NoReadyCell) => {
                    // The fleet is not initialized yet: an operator runs
                    // init first, and the creation next time.
                    let _ = self.init(ctx, nodes, payload).await;
                    return;
                }
                // Removed under it, or a rival's removal in progress: a
                // creation that lost its race.
                FleetStep::Refused(_) => return,
                FleetStep::Unavailable => {
                    unavailable += 1;
                    if unavailable >= UNAVAILABLE_STEPS {
                        return;
                    }
                    self.wait(ctx).await;
                }
            }
        }
    }

    fn note_registered(&mut self, tenant: Option<TenantId>) {
        if let Some(tenant) = tenant
            && !self.registered.contains(&tenant)
        {
            self.registered.push(tenant);
        }
    }

    /// `REMOVE_TENANT`: remove a tenant named from the alphabet (whoever
    /// created it), or resume a removal another run left half done.
    pub(super) async fn remove(&mut self, ctx: &SimContext, nodes: &ChainClient, draw: u64) {
        if !self.active {
            return;
        }
        let name =
            TENANT_NAMES[usize::try_from(draw % TENANT_NAMES.len() as u64).unwrap_or(0)].to_vec();
        let client = self.client(ctx, nodes);
        let first = self.first(draw >> 8);
        let mut removal = TenantRemoval::new(self.client_id, name);
        let mut wrote = false;
        let mut unavailable = 0;
        for _ in 0..MAX_STEPS {
            if wrote && buggify_with_prob!(0.15) {
                assert_reachable!("fleet: a tenant removal stops between two of its steps");
                return;
            }
            match removal.step(&client, first).await {
                FleetStep::Stepped(action) => wrote |= action.is_some(),
                FleetStep::Done(_) => {
                    assert_sometimes!(wrote, "fleet: a tenant removal runs to the end");
                    return;
                }
                FleetStep::Refused(_) | FleetStep::IdTaken => return,
                FleetStep::Unavailable => {
                    unavailable += 1;
                    if unavailable >= UNAVAILABLE_STEPS {
                        return;
                    }
                    self.wait(ctx).await;
                }
            }
        }
    }

    /// The end of the run (client 0, after the recovery tail): read meta and
    /// the cell back and check they agree — every hosted tenant is in meta,
    /// assigned to this cell; every `READY` tenant meta assigns to this cell
    /// is hosted by it; a `READY` cell is registered on its own side.
    pub(super) async fn check_consistency(&self, ctx: &SimContext, nodes: &ChainClient) {
        if !self.active {
            return;
        }
        let client = self.client(ctx, nodes);
        let (Some(meta), Some(cell)) = (
            fleet::load_meta(&client, 0).await,
            fleet::load_cell(&client, 0).await,
        ) else {
            return;
        };
        let registered = cell.registration().map(|r| r.context);
        for (cell_id, entry) in meta.cells() {
            if cell_id == self.cell_id && entry.state == paros::system::CellState::Ready {
                assert_always!(
                    registered.is_some_and(|r| r.cell_id == cell_id
                        && meta.fleet_id() == Some(r.fleet_id)),
                    "fleet: a ready cell is registered to meta's fleet on its own side",
                    { "cell" => cell_id }
                );
            }
        }
        for (tenant, _) in cell.tenants() {
            assert_always!(
                meta.tenant(tenant)
                    .is_some_and(|entry| Some(entry.cell_id) == registered.map(|r| r.cell_id)),
                "fleet: every tenant a cell hosts is in meta, assigned to it",
                { "tenant" => tenant.0 }
            );
        }
        for (tenant, entry) in meta.tenants() {
            if entry.state == TenantState::Ready
                && Some(entry.cell_id) == registered.map(|r| r.cell_id)
            {
                assert_always!(
                    cell.tenant(tenant).is_some(),
                    "fleet: every ready tenant meta assigns to a cell is hosted by it",
                    { "tenant" => tenant.0 }
                );
            }
        }
        assert_sometimes!(
            meta.tenants().next().is_some(),
            "fleet: meta and its cell are compared with a tenant in them"
        );
    }
}
