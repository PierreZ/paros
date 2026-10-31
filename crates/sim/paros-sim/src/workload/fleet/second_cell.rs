//! The run's **second cell** (#232 (multiple cells)): on its own BUGGIFY
//! location in `TENANT`, an operator forms an idle cell (`parosctl cell init
//! --idle`) on a machine `init` does not list, and joins it to the universe
//! the run's cell hosts (`parosctl universe add-cell`). Its later steps
//! create a tenant in it, resolve that tenant through the run's machines,
//! and, on a coin, remove the tenant and the cell, and try to add the cell
//! again, which must be refused. The tenant lands there by an override
//! entry (`--cell`), or, on a coin, by its bucket: the operator first gives
//! the tenant's bucket to the second cell (`universe buckets assign`, #713
//! (bucket placement)), and gives it back before the cell is removed. A phase the chaos cuts short is taken
//! again at the next step, and one that ends goes on to the next one up to
//! the tenant's creation; one operator at a time.
//!
//! Oracles: an idle cell holds no universe tenant; a tenant created in the
//! second cell is hosted there and resolves there from the run's machines;
//! the run's frontends never serve it (they front the run's cell only); a
//! cell with a tenant is not removed; a removed cell is never added again.
//! After chaos, the directory equals the second cell's tenant list while
//! its machine keeps its vote (`final_second`).

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};

use moonpool_sim::{
    Providers as _, SimContext, TimeProvider as _, assert_always, assert_reachable,
    buggify_with_prob,
};
use paros::client::ReadOutcome;
use paros::client::checkpoint::{CheckpointPolicy, Folder, LoadOutcome};
use paros::client::fleet::{CellJoin, FleetRefusal, FleetSession, Run, Step};
use paros::client::initialize::{InitParams, InitRun};
use paros::client::resolve::Resolution;
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::fleet::{CellState, FleetDirectory};
use paros::model::name::JournalName;
use paros::model::registry::Registry;
use paros::net::Pass;

use super::super::system::Announce;
use super::cell::Cell;
use super::{FleetOps, judge_folds, reach, tenant_identifier};
use crate::frontend::Presented;

/// The second cell's one tenant: a name no other operation draws.
const NAME: &[u8] = b"hooli";

/// The second cell's name.
const CELL_NAME: &str = "cell-second";

/// The local id a frontend call binds the tenant's journal name to.
const LOCAL: JournalIdentifier =
    JournalIdentifier::new(TenantId(1), paros::core::journal::JournalId(1));

/// The second cell as the run knows it, shared by every operator.
#[derive(Default)]
pub(super) struct SecondCell {
    /// An operator is taking a phase.
    busy: bool,
    /// The idle cell, once formed: what `add-cell` joins.
    formed: Option<CellJoin>,
    /// The cell is in the directory, `READY`.
    joined: bool,
    /// Its tenant, once created.
    tenant: Option<TenantId>,
    /// The cell the tenant's bucket was given to (#713): the bucket goes
    /// back to the run's cell before that cell is removed.
    bucket: Option<u64>,
    /// The cell was removed from the directory.
    removed: bool,
    /// The machines racked for it in the middle of the run (#490 (second
    /// cell mid-run)), once an operator racked them: the cell forms over
    /// these, and never over a layout machine again.
    racked: Option<Vec<paros::model::address::Address>>,
}

impl SecondCell {
    /// Whether the cell's votes are still on its machines' disks: racked
    /// machines are never wiped (their group has no wipe regime), a layout
    /// machine may be.
    fn alive(&self, state: &moonpool_sim::StateHandle) -> bool {
        self.racked.is_some() || crate::machine::second_cell_alive(state)
    }
}

const SECOND_CELL_KEY: &str = "paros-second-cell";

/// Run `f` over the run's [`SecondCell`].
fn with_second<R>(state: &moonpool_sim::StateHandle, f: impl FnOnce(&mut SecondCell) -> R) -> R {
    let second = crate::state::published(state, SECOND_CELL_KEY, SecondCell::default);
    let mut second = second.lock().unwrap_or_else(PoisonError::into_inner);
    f(&mut second)
}

impl FleetOps {
    /// Take the second cell's next phase, if this run can have one and no
    /// other operator is taking one. Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn second_cell(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        if !self.initialized {
            return false;
        }
        let claimed = with_second(ctx.state(), |second| {
            if second.busy || second.removed {
                return false;
            }
            second.busy = true;
            true
        });
        if !claimed {
            return false;
        }
        // A phase that ends goes on to the next one in the same step, up
        // to the tenant's creation: a run's chaos window holds few steps.
        let progress = |second: &mut SecondCell| {
            (
                second.formed.is_some(),
                second.joined,
                second.tenant.is_some(),
            )
        };
        let mut ran = false;
        loop {
            let before = with_second(ctx.state(), progress);
            let step = self.second_phase(ctx, policy, draw).await;
            ran |= step;
            let after = with_second(ctx.state(), progress);
            if !step || after == before || after.2 {
                break;
            }
        }
        with_second(ctx.state(), |second| second.busy = false);
        ran
    }

    /// The phase the second cell is at.
    async fn second_phase(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        let (formed, joined, tenant, alive) = with_second(ctx.state(), |second| {
            (
                second.formed.clone(),
                second.joined,
                second.tenant,
                second.alive(ctx.state()),
            )
        });
        let Some(formed) = formed.filter(|_| alive) else {
            // Not formed yet, or its machine was wiped (a lost cell of one
            // member): form one on racked machines, or on an idle machine
            // `init` does not list.
            with_second(ctx.state(), |second| {
                *second = SecondCell {
                    busy: true,
                    racked: second.racked.take(),
                    ..SecondCell::default()
                }
            });
            return self.form_second(ctx, draw).await;
        };
        // Any operator may take a phase: it learns the cell's journals
        // from the run's record of the formation (#246).
        self.note_journals(
            [Some(formed.journals.cell), formed.journals.election]
                .into_iter()
                .flatten(),
        );
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        if !joined {
            self.join_second(ctx, &cell, policy, &formed, draw).await;
        } else if tenant.is_none() {
            self.host_in_second(ctx, &cell, policy, &formed, draw).await;
        } else if buggify_with_prob!(0.5) {
            self.remove_second(&cell, policy, &formed, draw).await;
        } else {
            self.resolve_in_second(ctx, &formed, draw).await;
        }
        true
    }

    /// Form an idle cell, with no universe: on machines racked for it in
    /// the middle of the run (#490 (second cell mid-run)), or on an
    /// unlisted idle machine of the layout, `init` over that one machine.
    async fn form_second(&mut self, ctx: &SimContext, draw: u64) -> bool {
        if let Some(racked) = Self::rack_second(ctx) {
            return self.form_racked(ctx, &racked, draw).await;
        }
        // A `stateless` machine is never a founding member.
        let unlisted: Vec<_> = (self.layout.listed..self.machines.len())
            .filter(|rank| {
                self.layout
                    .machines
                    .get(*rank)
                    .is_some_and(|machine| machine.class == paros::machine::Class::Storage)
            })
            .map(|rank| self.machines[rank].clone())
            .collect();
        let Some(target) = crate::machine::second_cell_target(ctx.state(), &unlisted) else {
            return false;
        };
        crate::machine::note_second_cell(ctx.state(), &target);
        assert_reachable!("universe: an operator forms an idle cell on an unlisted machine");
        let connector = self.connector.clone();
        let observer: Arc<dyn paros::client::CallObserver> = Arc::new(Announce::every(ctx));
        let token =
            crate::frontend::operator_token(ctx.state(), connector.providers().time().now());
        let run = paros::client::initialize::initialize(
            connector.providers(),
            connector.rpc(),
            connector.names(),
            std::slice::from_ref(&target),
            |servers| {
                connector
                    .client(servers)
                    .with_observer(observer.clone())
                    .rotating_over(servers.len())
            },
            InitParams {
                patience: self.patience,
                fleet_id: draw | 1,
                leader_seed: self.leader_seeds.next(),
                names: (String::new(), CELL_NAME.into()),
                token,
                root_key: None,
                survive_machines: 0,
                idle: true,
            },
        )
        .await;
        let (InitRun::Initialized(initialized) | InitRun::AlreadyInitialized(initialized)) = run
        else {
            // Its machine was admitted into the run's cell first, or the
            // decree did not end in time: a later step tries again.
            return true;
        };
        assert_always!(
            initialized.fleet_id == 0 && initialized.journals.fleet.is_none(),
            "universe: an idle cell holds no universe tenant",
            { "cell" => initialized.journals.cell_id }
        );
        let journals = initialized.journals;
        self.note_journals(
            [Some(journals.cell), journals.election]
                .into_iter()
                .flatten(),
        );
        let formed = CellJoin {
            journals,
            endpoint: initialized.servers.clone(),
            name: CELL_NAME.as_bytes().to_vec(),
        };
        with_second(ctx.state(), |second| second.formed = Some(formed));
        true
    }

    /// The machines racked for the second cell: racked by an earlier step,
    /// or now, on a coin, unless the cell already formed on a layout
    /// machine. `None` when the cell forms on a layout machine.
    fn rack_second(ctx: &SimContext) -> Option<Vec<paros::model::address::Address>> {
        if let Some(racked) = with_second(ctx.state(), |second| second.racked.clone()) {
            return Some(racked);
        }
        if crate::machine::second_cell_founded(ctx.state()) || !buggify_with_prob!(0.5) {
            return None;
        }
        let count = moonpool_sim::sim_random_range(
            *crate::machine::racked::CELL_TWO_RANGE.start()
                ..*crate::machine::racked::CELL_TWO_RANGE.end() + 1,
        );
        let ips = ctx
            .deployment()
            .spawn_group(crate::machine::racked::CELL_TWO_GROUP, count, || {
                Box::new(crate::machine::racked::RackedMachineProcess::racked())
            })
            .ok()?;
        let racked: Vec<_> = ips
            .iter()
            .filter_map(|ip| crate::machine::racked::racked_address(ip).ok())
            .collect();
        assert_always!(
            racked.len() == count,
            "universe: every racked machine has an address",
            { "count" => count }
        );
        assert_reachable!("universe: an operator racks machines for a second cell mid-run");
        with_second(ctx.state(), |second| second.racked = Some(racked.clone()));
        Some(racked)
    }

    /// `cell init --idle` over the racked machines, under chaos (#490
    /// (second cell mid-run)): 2F+1 founders, `F` one over three machines
    /// and zero below. On a coin the list also names a founding member of
    /// the run's cell, which is in a cell already: the formation is then
    /// refused, and a later step forms the cell over the racked machines
    /// alone.
    async fn form_racked(
        &mut self,
        ctx: &SimContext,
        racked: &[paros::model::address::Address],
        draw: u64,
    ) -> bool {
        let mut listed = racked.to_vec();
        let foreign = buggify_with_prob!(0.1) && self.layout.founders > 0;
        if foreign {
            listed.push(self.machines[0].clone());
        }
        let survive_machines = u32::from(racked.len() >= 3);
        let connector = self.connector.clone();
        let observer: Arc<dyn paros::client::CallObserver> = Arc::new(Announce::every(ctx));
        let token =
            crate::frontend::operator_token(ctx.state(), connector.providers().time().now());
        let run = paros::client::initialize::initialize(
            connector.providers(),
            connector.rpc(),
            connector.names(),
            &listed,
            |servers| {
                connector
                    .client(servers)
                    .with_observer(observer.clone())
                    .rotating_over(servers.len())
            },
            InitParams {
                patience: self.patience,
                fleet_id: draw | 1,
                leader_seed: self.leader_seeds.next(),
                names: (String::new(), CELL_NAME.into()),
                token,
                root_key: None,
                survive_machines,
                idle: true,
            },
        )
        .await;
        let (InitRun::Initialized(initialized) | InitRun::AlreadyInitialized(initialized)) = run
        else {
            if foreign && matches!(run, InitRun::Refused(_)) {
                assert_reachable!(
                    "universe: a second cell's init that lists a machine of the run's cell is refused"
                );
            }
            return true;
        };
        let members: BTreeSet<u64> = initialized
            .servers
            .iter()
            .chain(&initialized.admitted)
            .filter(|(_, addr)| !racked.contains(addr))
            .map(|(id, _)| *id)
            .collect();
        assert_always!(
            members.is_empty(),
            "universe: a second cell never takes a machine of the run's cell",
            { "foreign" => format!("{members:?}") }
        );
        assert_always!(
            initialized.fleet_id == 0 && initialized.journals.fleet.is_none(),
            "universe: an idle cell holds no universe tenant",
            { "cell" => initialized.journals.cell_id }
        );
        if initialized.members.len() + initialized.admitted.len() >= 2 {
            assert_reachable!("universe: a second cell of two or more machines forms during chaos");
        }
        let journals = initialized.journals;
        self.note_journals(
            [Some(journals.cell), journals.election]
                .into_iter()
                .flatten(),
        );
        let endpoint = initialized
            .servers
            .iter()
            .filter(|(_, addr)| racked.contains(addr))
            .cloned()
            .collect();
        let formed = CellJoin {
            journals,
            endpoint,
            name: CELL_NAME.as_bytes().to_vec(),
        };
        with_second(ctx.state(), |second| second.formed = Some(formed));
        true
    }

    /// A session over the run's cell, writing the universe directory.
    fn second_session(&self, cell: &Cell, policy: CheckpointPolicy) -> Option<FleetSession> {
        self.session(cell, cell.journals, policy)
            .map(|session| session.with_endpoint(cell.servers.clone()))
    }

    /// `universe add-cell`: join the idle cell to the universe.
    async fn join_second(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        policy: CheckpointPolicy,
        formed: &CellJoin,
        draw: u64,
    ) {
        let Some(mut session) = self.second_session(cell, policy) else {
            return;
        };
        let first = cell.first(draw);
        let run = session
            .add_cell(
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
                &cell.client,
                first,
                formed,
                self.patience,
            )
            .await;
        judge_folds(&session);
        run.steps.iter().copied().for_each(reach);
        if let Step::Done { result, .. } = run.outcome {
            let cell_id = formed.journals.cell_id;
            assert_always!(
                result == cell_id
                    && session
                        .directory()
                        .cell(cell_id)
                        .is_some_and(|entry| entry.state == CellState::Ready)
                    && session.registry_of(cell_id).fleet().is_some_and(|f| {
                        Some(f.fleet_id) == session.directory().fleet() && f.cell_id == cell_id
                    }),
                "universe: an added cell is READY in the directory and joined to it",
                { "cell" => cell_id }
            );
            assert_reachable!("universe: a second cell joins the universe");
            with_second(ctx.state(), |second| second.joined = true);
        }
    }

    /// Create the second cell's tenant in it.
    async fn host_in_second(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        policy: CheckpointPolicy,
        formed: &CellJoin,
        draw: u64,
    ) {
        let cell_id = formed.journals.cell_id;
        let Some(session) = self.second_session(cell, policy) else {
            return;
        };
        // By its bucket (#713): the tenant's bucket names the second cell
        // first. Else by an override entry.
        let by_bucket =
            buggify_with_prob!(0.5) || with_second(ctx.state(), |s| s.bucket == Some(cell_id));
        let mut session = if by_bucket {
            let mut session = session;
            let run = session
                .assign_buckets(
                    &cell.client,
                    cell.first(draw),
                    hooli_bucket(),
                    cell_id,
                    self.patience,
                )
                .await;
            judge_folds(&session);
            run.steps.iter().copied().for_each(reach);
            if !matches!(run.outcome, Step::Done { .. }) {
                return;
            }
            assert_always!(
                session.directory().bucket_cell_of(NAME) == Some(cell_id),
                "universe: an assigned bucket names its cell",
                { "cell" => cell_id }
            );
            assert_reachable!("universe: a bucket is given to a second cell");
            with_second(ctx.state(), |second| second.bucket = Some(cell_id));
            session
        } else {
            session.with_cell(cell_id)
        };
        let draws = [
            tenant_identifier(draw),
            tenant_identifier(draw.rotate_left(29) ^ 0x5ec0),
        ];
        let run = session
            .create_tenant(&cell.client, cell.first(draw), NAME, draws, self.patience)
            .await;
        judge_folds(&session);
        run.steps.iter().copied().for_each(reach);
        let Step::Done { result, .. } = run.outcome else {
            return;
        };
        assert_always!(
            session
                .directory()
                .tenant(result)
                .is_some_and(|tenant| tenant.cell_id == cell_id)
                && session.registry_of(cell_id).hosts(result)
                && !session.cell().hosts(result),
            "universe: a tenant created in the second cell is hosted there only",
            { "tenant" => result.0, "cell" => cell_id }
        );
        assert_reachable!("universe: a tenant is created in a second cell");
        if session
            .directory()
            .tenant(result)
            .is_some_and(|tenant| tenant.placement.is_bucket())
        {
            assert_reachable!("universe: a tenant lands in a second cell by its bucket");
        }
        with_second(ctx.state(), |second| second.tenant = Some(result));
        self.resolve_in_second(ctx, formed, draw).await;
    }

    /// Resolve the second cell's tenant through the run's founding members,
    /// which point at the second cell, and call it through a frontend of
    /// the run's cell, which does not serve it.
    async fn resolve_in_second(&mut self, ctx: &SimContext, formed: &CellJoin, draw: u64) {
        let founders = &self.machines[..self.layout.founders];
        let resolution = paros::client::resolve::resolve(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            founders,
            NAME,
            self.patience,
        )
        .await;
        if let Resolution::Resolved(resolved) = &resolution {
            assert_always!(
                resolved.cell_id == formed.journals.cell_id,
                "resolve: a tenant resolves to the cell that serves it",
                { "cell" => resolved.cell_id, "known" => formed.journals.cell_id }
            );
            assert_reachable!("universe: a tenant of a second cell resolves from the run's cell");
        }
        // Through a resolver of the run's cell (#233 (resolver)): it sends
        // the client to the second cell, whose machine names the rest.
        let now = self.connector.providers().time().now();
        let own = crate::frontend::token(
            ctx.state(),
            &String::from_utf8_lossy(NAME),
            "",
            Presented::Own,
            now,
        );
        let (redirection, resolution) = paros::client::resolve::resolve_through(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            founders,
            (NAME, b""),
            &own,
            self.patience,
        )
        .await;
        if let paros::client::resolve::Redirection::Redirected(redirect) = &redirection
            && with_second(ctx.state(), |second| second.tenant) == Some(redirect.tenant)
        {
            assert_always!(
                redirect.cell_id == formed.journals.cell_id,
                "resolver: a tenant is sent to the cell it was created in",
                { "cell" => redirect.cell_id, "known" => formed.journals.cell_id }
            );
            if let Resolution::Resolved(resolved) = &resolution {
                assert_always!(
                    resolved.cell_id == formed.journals.cell_id,
                    "resolve: a tenant resolves to the cell that serves it",
                    { "cell" => resolved.cell_id, "known" => formed.journals.cell_id }
                );
                assert_reachable!(
                    "resolver: a tenant of a second cell resolves through a resolver"
                );
            }
        }
        if self.frontends.is_empty() {
            return;
        }
        let Ok(named) = JournalName::new(
            &String::from_utf8_lossy(NAME),
            &String::from_utf8_lossy(b"orders"),
        ) else {
            return;
        };
        let now = self.connector.providers().time().now();
        let pass = Arc::new(Pass::new(crate::frontend::token(
            ctx.state(),
            &String::from_utf8_lossy(NAME),
            "",
            Presented::Own,
            now,
        )));
        pass.bind(LOCAL, named);
        let client = self.connector.through(&self.frontends, &pass);
        let first = usize::try_from(draw % self.frontends.len() as u64).unwrap_or(0);
        let request = paros::client::Reader::new(LOCAL, 0).request(1, 0);
        let outcome = client.read_any(&request, first).await.outcome;
        assert_always!(
            !matches!(
                outcome,
                ReadOutcome::Page { .. } | ReadOutcome::Truncated { .. }
            ),
            "frontend: a frontend never serves a tenant of another cell"
        );
    }

    /// Remove the second cell: first while it still hosts its tenant, which
    /// must be refused, then its tenant, then the cell; then add it again,
    /// which must be refused.
    async fn remove_second(
        &mut self,
        cell: &Cell,
        policy: CheckpointPolicy,
        formed: &CellJoin,
        draw: u64,
    ) {
        let cell_id = formed.journals.cell_id;
        let first = cell.first(draw);
        let Some(mut session) = self.second_session(cell, policy) else {
            return;
        };
        let early = session
            .remove_cell(&cell.client, first, cell_id, self.patience)
            .await;
        judge_folds(&session);
        match early.outcome {
            Step::Refused(FleetRefusal::CellNotEmpty { .. }) => {
                assert_reachable!("universe: a cell that hosts a tenant is not removed");
            }
            Step::Refused(FleetRefusal::CellHoldsBuckets { .. }) => {
                assert_reachable!("universe: a cell that holds a bucket is not removed");
            }
            _ => {}
        }
        // A phase an earlier step cut short may have removed the tenant
        // already: then the cell is empty, and its removal is due.
        assert_always!(
            !matches!(early.outcome, Step::Done { .. })
                || !with_second(&self.state, |second| second.tenant).is_some_and(|tenant| {
                    session
                        .directory()
                        .tenant(tenant)
                        .is_some_and(|entry| entry.cell_id == cell_id)
                }),
            "universe: a cell is removed only once it hosts no tenant",
            { "cell" => cell_id }
        );
        let removal = session
            .remove_tenant(&cell.client, first, NAME, self.patience)
            .await;
        judge_folds(&session);
        removal.steps.iter().copied().for_each(reach);
        if !matches!(removal.outcome, Step::Done { .. }) {
            return;
        }
        if with_second(&self.state, |second| second.bucket == Some(cell_id)) {
            // The tenant's bucket goes back to the run's cell first: a cell
            // that holds a bucket is not removed.
            let back = session
                .assign_buckets(
                    &cell.client,
                    first,
                    hooli_bucket(),
                    cell.journals.cell_id,
                    self.patience,
                )
                .await;
            judge_folds(&session);
            back.steps.iter().copied().for_each(reach);
            if !matches!(back.outcome, Step::Done { .. }) {
                return;
            }
            with_second(&self.state, |second| second.bucket = None);
        }
        let run: Run<u64> = session
            .remove_cell(&cell.client, first, cell_id, self.patience)
            .await;
        judge_folds(&session);
        run.steps.iter().copied().for_each(reach);
        if !matches!(run.outcome, Step::Done { .. }) {
            return;
        }
        assert_always!(
            session.directory().cell(cell_id).is_none() && session.directory().is_cell_removed(cell_id),
            "universe: a removed cell leaves the directory and is tombstoned",
            { "cell" => cell_id }
        );
        with_second(&self.state, |second| second.removed = true);
        let again = session
            .add_cell(
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
                &cell.client,
                first,
                formed,
                self.patience,
            )
            .await;
        judge_folds(&session);
        assert_always!(
            again.steps.is_empty()
                && matches!(
                    again.outcome,
                    Step::Refused(FleetRefusal::CellRemoved { .. }) | Step::Interrupted(_)
                ),
            "universe: a removed cell is never added again",
            { "cell" => cell_id }
        );
    }

    /// After chaos, the universe directory against the second cell's tenant
    /// list, while its machine keeps its vote and the cell is in the
    /// directory: a tenant the second cell hosts is in the directory under
    /// it, and a `READY` tenant of it in the directory is hosted.
    pub(super) async fn final_second(&mut self, ctx: &SimContext, directory: &FleetDirectory) {
        let formed = with_second(ctx.state(), |second| {
            second
                .formed
                .clone()
                .filter(|_| second.joined && !second.removed)
        });
        let Some(formed) = formed else {
            return;
        };
        let cell_id = formed.journals.cell_id;
        if !with_second(ctx.state(), |second| second.alive(ctx.state()))
            || directory.cell(cell_id).is_none()
        {
            return;
        }
        // This operator may have taken no phase: it learns the cell's
        // journals from the run's record of the formation (#246).
        self.note_journals([formed.journals.cell]);
        let client = self.connect(ctx, &formed.endpoint);
        let members = formed.endpoint.iter().map(|(id, _)| NodeId(*id));
        let mut fold = Folder::new(Registry::new(members));
        let loaded =
            paros::client::checkpoint::load(&mut fold, formed.journals.cell, &client, 0, 0).await;
        let LoadOutcome::Loaded { .. } = loaded else {
            return;
        };
        let registry = fold.state();
        for tenant in registry.hosted() {
            assert_always!(
                directory.tenant(tenant).is_some_and(|t| t.cell_id == cell_id),
                "fleet: every tenant the cell hosts is in the directory under that cell",
                { "tenant" => tenant.0 }
            );
        }
        for (tenant, entry) in directory.tenants() {
            // The cell's own cell tenant is in the directory, never hosted.
            if entry.cell_id == cell_id
                && entry.kind == paros::model::fleet::TenantKind::Users
                && entry.state == paros::model::fleet::TenantState::Ready
            {
                assert_always!(
                    registry.hosts(tenant),
                    "fleet: every READY tenant in the directory is hosted by its cell",
                    { "tenant" => tenant.0 }
                );
            }
        }
        assert_reachable!("universe: the final folds of a second cell are compared");
    }
}

/// The bucket of the second cell's tenant (#713 (bucket placement)): what
/// an operator gives the second cell. No other name the workload creates
/// falls in it (`no_workload_name_shares_the_second_cells_bucket`).
fn hooli_bucket() -> paros::model::fleet::buckets::BucketRange {
    let bucket = paros::model::fleet::buckets::bucket_of(NAME);
    paros::model::fleet::buckets::BucketRange::new(bucket, bucket)
        .expect("a bucket is below the bucket count")
}

/// The second cell's id, once formed: a tenant created there is dropped on
/// its registry, not on the run's cell's.
pub(super) fn cell_id(state: &moonpool_sim::StateHandle) -> Option<u64> {
    with_second(state, |second| {
        second.formed.as_ref().map(|formed| formed.journals.cell_id)
    })
}

/// The second cell's control journal, once it joined the universe: every
/// operator learns it from the directory's cell entry. A removal of a
/// tenant there reads it (`FleetSession::remove_tenant`).
pub(super) fn joined_journal(state: &moonpool_sim::StateHandle) -> Option<JournalIdentifier> {
    with_second(state, |second| {
        second
            .formed
            .as_ref()
            .filter(|_| second.joined)
            .map(|formed| formed.journals.cell)
    })
}

/// Whether an operator formed the run's second cell (#232 (multiple
/// cells)) and has not removed it: its later phases are due.
pub(super) fn started(state: &moonpool_sim::StateHandle) -> bool {
    with_second(state, |second| second.formed.is_some() && !second.removed)
}

#[cfg(test)]
mod tests {
    use paros::model::fleet::buckets::bucket_of;

    /// A tenant the workload creates on the run's cell by its bucket never
    /// shares the bucket an operator gives the second cell: it would land
    /// there.
    #[test]
    fn no_workload_name_shares_the_second_cells_bucket() {
        let mut names: Vec<&[u8]> = super::super::NAMES.to_vec();
        names.extend(super::super::journals::TAIL_NAMES);
        names.extend([super::super::resolve::NEVER_CREATED, b"nobody".as_slice()]);
        for name in names {
            assert_ne!(bucket_of(name), bucket_of(super::NAME), "{name:?}");
        }
    }
}
