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
//! the run's frontends never serve it (they front the run's cell only),
//! unless it lived there and moved back: then a frontend may serve it from
//! the tenant hop it cached, until its machines forget the journal; a
//! cell with a tenant is not removed; a removed cell is never added again.
//! After chaos, the directory equals the second cell's tenant list while
//! its machine keeps its vote (`final_second`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    Providers as _, SimContext, TimeProvider as _, assert_always, assert_always_or_unreachable,
    assert_reachable, buggify_with_prob,
};
use paros::client::checkpoint::{CheckpointPolicy, Folder, LoadOutcome};
use paros::client::fleet::{CellJoin, FleetRefusal, FleetSession, Run, Step};
use paros::client::initialize::{InitParams, InitRun};
use paros::client::resolve::Resolution;
use paros::client::{Answered, Attempted, CallObserver, ReadOutcome};
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::core::journal::TenantId;
use paros::model::fleet::{CellState, FleetDirectory, TenantState};
use paros::model::name::JournalName;
use paros::model::registry::Registry;
use paros::net::Pass;

use super::super::system::Announce;
use super::cell::Cell;
use super::{FleetOps, Pending, judge_folds, reach, tenant_identifier};
use crate::frontend::Presented;

/// The second cell's one tenant: a name no other operation draws.
const NAME: &[u8] = b"hooli";

/// Whether `directory` holds the second cell's tenant `REMOVING` in the
/// run's cell `run_cell`: a removal the run's cell judges after chaos
/// ([`FleetOps::pend_moved_removal`]).
fn removal_owed_by_run(directory: &FleetDirectory, run_cell: u64) -> bool {
    directory
        .named(NAME)
        .is_some_and(|(_, entry)| entry.state == TenantState::Removing && entry.cell_id == run_cell)
}

/// The second cell's name.
const CELL_NAME: &str = "cell-second";

/// The journal the second cell's tenant holds (#785 (moved record)).
const JOURNAL: &[u8] = b"orders";

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
    /// The cell its tenant lives in now, once a move took it out of the
    /// second cell (#784 (coordinator handover)): `None` is the second cell.
    home: Option<u64>,
    /// The cell an operator moves the tenant to, while the move has not
    /// ended: the next phase resumes it before anything else.
    moving_to: Option<u64>,
    /// The id of its tenant's journal [`JOURNAL`], once created (#785
    /// (moved record)): what a client of the other cell follows it to.
    journal: Option<paros::core::journal::JournalId>,
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

/// How far a move's target took the tenant, as one run of the move read
/// it (#784 (coordinator handover)).
#[derive(Clone, Copy, Debug, Default)]
struct Arrival {
    /// The target hosts the tenant as its own, no longer arriving.
    settled: bool,
    /// Whether the target hosts the tenant, and how many slots it booked
    /// for it: a change is progress.
    progress: (bool, usize),
}

/// Where the second cell's tenant stands for the convergence oracle (#784
/// (coordinator handover)): `None` while it has no tenant or never moved.
pub(super) enum Moved {
    /// A move is open: the tenant's journals are on both cells' machines,
    /// and the move's own oracles judge them.
    Open(TenantId),
    /// The tenant lives in this cell now: a machine of another cell left
    /// its journals by reconfiguration and owes them nothing.
    Home(TenantId, u64),
}

/// The second cell's tenant, if a move took it or is taking it.
pub(super) fn moved(state: &moonpool_sim::StateHandle) -> Option<Moved> {
    with_second(state, |second| {
        let tenant = second.tenant?;
        if second.moving_to.is_some() {
            return Some(Moved::Open(tenant));
        }
        second.home.map(|home| Moved::Home(tenant, home))
    })
}

/// The cells the second cell's tenant lived in, once a move took it
/// (#784 (coordinator handover)): the cell it was born in and the run's
/// cell. Empty while it never moved.
fn lived_in(state: &moonpool_sim::StateHandle, birth: u64) -> Vec<u64> {
    if !with_second(state, |second| second.home.is_some()) {
        return Vec::new();
    }
    std::iter::once(birth)
        .chain(crate::machine::formed_cell(state).map(|cell| cell.cell_id))
        .collect()
}

/// Whether the run's second cell is joined, not removed, and its votes
/// still on its machines' disks: the repointed-cell scenario's move waits
/// for it (`crate::world::scenarios::moved_member`, #854 (universe repoint
/// gates)).
pub(crate) fn second_cell_joined(state: &moonpool_sim::StateHandle) -> bool {
    with_second(state, |second| {
        second.joined && !second.removed && second.alive(state)
    })
}

/// Run `f` over the run's [`SecondCell`].
fn with_second<R>(state: &moonpool_sim::StateHandle, f: impl FnOnce(&mut SecondCell) -> R) -> R {
    let second = crate::state::published(state, SECOND_CELL_KEY, SecondCell::default);
    let mut second = second.lock().unwrap_or_else(PoisonError::into_inner);
    f(&mut second)
}

/// Whether the second cell's tenant moved out and came back (#785 (moved
/// record)): its home is its birth cell again after a move. A move out
/// again then books its machines in the run's cell a second time, where
/// its move back stopped its journals. Witness `replay-recipe
/// 8126885539797468740 810276:7248213314656309019` on 49ccb69: such a
/// machine never started the journal again, and the move never elected.
fn came_back(ctx: &SimContext, formed: &CellJoin) -> bool {
    let second_id = formed.journals.cell_id;
    with_second(ctx.state(), |second| second.home == Some(second_id))
}

/// Whether the next phase is the tenant's move, before any coin: a move is
/// open, or, on a moved-tenant seed (#785 (moved record)), the tenant never
/// moved out of its cell yet. The source writes its moved record only after
/// the flip, so the scenario moves the tenant before any other phase.
fn move_due(ctx: &SimContext) -> bool {
    // Drawn before the lock: the scenario takes its own.
    let moved = crate::shape::moved_tenant(ctx.state());
    with_second(ctx.state(), |second| {
        second.moving_to.is_some() || moved && second.home.is_none()
    })
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
        // On a moved-tenant seed (#785 (moved record)) the step goes on
        // into the move of the tenant it created.
        let moved = crate::shape::moved_tenant(ctx.state());
        let progress = |second: &mut SecondCell| {
            (
                second.formed.is_some(),
                second.joined,
                second.tenant.is_some(),
                second.moving_to.is_some() || second.home.is_some(),
            )
        };
        let mut ran = false;
        loop {
            let before = with_second(ctx.state(), progress);
            let step = self.second_phase(ctx, policy, draw).await;
            ran |= step;
            let after = with_second(ctx.state(), progress);
            if !step || after == before || (after.2 && !moved) || after.3 {
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
            self.join_second(ctx, &cell, policy, &formed, draw, false)
                .await;
        } else if tenant.is_none() {
            self.host_in_second(ctx, &cell, policy, &formed, draw).await;
        } else if move_due(ctx)
            || buggify_with_prob!(0.3)
            || (came_back(ctx, &formed) && buggify_with_prob!(0.5))
        {
            self.move_second(ctx, &cell, policy, &formed, draw).await;
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
        // A `stateless` machine is never an initial member.
        let unlisted: Vec<_> = (self.layout.listed..self.machines.len())
            .filter(|rank| {
                self.layout
                    .machines
                    .get(*rank)
                    .is_some_and(|machine| machine.class.stores())
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
        if crate::machine::second_cell_formed(ctx.state()) || !buggify_with_prob!(0.5) {
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

    /// `cell init --idle` over the racked machines, under chaos (#490 (second
    /// cell mid-run)): 2F+1 initial members, `F` one over three machines and
    /// zero below. On a coin the list also names an initial member of the run's
    /// cell, which is in a cell already: the formation is then refused, and a
    /// later step forms the cell over the racked machines alone.
    async fn form_racked(
        &mut self,
        ctx: &SimContext,
        racked: &[paros::model::address::Address],
        draw: u64,
    ) -> bool {
        let mut listed = racked.to_vec();
        let foreign = buggify_with_prob!(0.1) && self.layout.initial_members > 0;
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
        // A racked machine past the initial members joins by `cell add-machine`
        // into the second cell, not the run's cell (#844 (playground action
        // blocks): the playground admits a machine into either cell).
        if !initialized.admitted.is_empty() {
            assert_reachable!("universe: a machine is admitted into a second cell");
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
        repoint: bool,
    ) {
        let Some(mut session) = self.second_session(cell, policy) else {
            return;
        };
        let first = cell.first(draw);
        // Again for a joined cell: the pointer names the universe cell's
        // machines where its registry says they are now (#349).
        if repoint
            && session
                .read_universe_cell(&cell.client, first)
                .await
                .is_err()
        {
            return;
        }
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

    /// `universe add-cell` again for the joined second cell, as an
    /// operator does when the cell does not take its step for a tenant
    /// (#349): the universe cell's machines may have moved, and the join
    /// repoints the cell at them where they are now. Nothing when no
    /// second cell is joined, or it left.
    pub(super) async fn repoint_second(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        let formed = with_second(ctx.state(), |second| {
            second
                .formed
                .clone()
                .filter(|_| second.joined && !second.removed && second.alive(ctx.state()))
        });
        let Some(formed) = formed else {
            return;
        };
        assert_reachable!("universe: an operator sends add-cell again for a joined cell");
        // This operator may have taken no phase: it learns the cell's
        // journals from the run's record of the formation (#246), as
        // `second_phase` and `final_second` do. Witness: recipe
        // 909019442180230136 819568:11635441156010456268 on 6dcac86, an
        // operator's repoint read the election journal it never learned.
        self.note_journals(
            [Some(formed.journals.cell), formed.journals.election]
                .into_iter()
                .flatten(),
        );
        self.join_second(ctx, cell, policy, &formed, draw, true)
            .await;
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
        self.journal_in_second(ctx, formed, result).await;
        self.resolve_in_second(ctx, formed, draw).await;
    }

    /// Move the second cell's tenant to the run's cell, or back to the
    /// second cell (#784 (coordinator handover)): the operator opens the
    /// move, the two cells' coordinators move its journals while they
    /// serve, and the operator flips the directory once the target settled.
    /// A move the chaos cuts short is resumed by the next phase.
    async fn move_second(
        &mut self,
        ctx: &SimContext,
        cell: &Cell,
        policy: CheckpointPolicy,
        formed: &CellJoin,
        draw: u64,
    ) -> Arrival {
        let (second_id, run_id) = (formed.journals.cell_id, cell.journals.cell_id);
        let (home, pending) = with_second(ctx.state(), |second| {
            (second.home.unwrap_or(second_id), second.moving_to)
        });
        let to = pending.unwrap_or(if home == second_id { run_id } else { second_id });
        let Some(mut session) = self.second_session(cell, policy) else {
            return Arrival::default();
        };
        with_second(ctx.state(), |second| second.moving_to = Some(to));
        let run = session
            .move_tenant(&cell.client, cell.first(draw), (NAME, to), self.patience)
            .await;
        judge_folds(&session);
        run.steps.iter().copied().for_each(reach);
        match run.outcome {
            Step::Done { result, .. } => {
                let target = session.registry_of(to);
                assert_always!(
                    session
                        .directory()
                        .tenant(result)
                        .is_some_and(|t| t.cell_id == to && t.moving_to.is_none())
                        && target.hosts(result)
                        && target.arriving(result).is_none(),
                    "move: a moved tenant lives in its new cell, settled",
                    { "tenant" => result.0, "cell" => to }
                );
                assert_reachable!("move: a tenant moves to another cell");
                if to == second_id {
                    assert_reachable!("move: a tenant moves back to the cell it was born in");
                }
                with_second(ctx.state(), |second| {
                    second.home = Some(to);
                    second.moving_to = None;
                });
                self.resolve_in_second(ctx, formed, draw).await;
                Arrival::default()
            }
            Step::Refused(_) => {
                with_second(ctx.state(), |second| second.moving_to = None);
                Arrival::default()
            }
            // How far the target took the tenant, as the run read it.
            Step::Interrupted(_) | Step::Advanced(_) => session
                .directory()
                .named(NAME)
                .map_or_else(Arrival::default, |(tenant, _)| {
                    let target = session.registry_of(to);
                    Arrival {
                        settled: target.hosts(tenant)
                            && target.arriving(tenant).is_none()
                            && target.leaving(tenant).is_none(),
                        progress: (
                            target.hosts(tenant),
                            target
                                .bookings()
                                .filter(|(_, b)| b.target.tenant() == tenant)
                                .count(),
                        ),
                    }
                }),
        }
    }

    /// The recovery tail's end of a move the chaos left open (#784
    /// (coordinator handover)): take the move's steps again until it ends,
    /// for [`super::FLEET_SETTLE`], counted again each time the target
    /// takes the tenant further, up to [`super::HANDOVER_CEILING`], and
    /// once more when the target settled it. A move whose target cannot
    /// place the tenant never settles, and an open move has no abort yet,
    /// so the tail owes no end to an unsettled move; once the target
    /// settled the tenant, the flip is one directory write, and it must
    /// land.
    pub(crate) async fn finish_move(&mut self, ctx: &SimContext, policy: CheckpointPolicy) {
        let mut deadline = ctx.time().now() + super::FLEET_SETTLE;
        let ceiling = ctx.time().now() + super::HANDOVER_CEILING;
        let mut draw = self.client_id;
        let mut settled_at = None;
        let mut seen = None;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let formed = with_second(ctx.state(), |second| {
                second
                    .formed
                    .clone()
                    .filter(|_| second.joined && !second.removed && second.moving_to.is_some())
            });
            let Some(formed) = formed else {
                break;
            };
            if crate::excuses::cell_lost(ctx.state(), crate::excuses::Excuse::SkipLateCellLost) {
                break;
            }
            // Any operator may end the move: it learns the second cell's
            // journals from the run's record of the formation (#246), as a
            // phase does.
            self.note_journals(
                [Some(formed.journals.cell), formed.journals.election]
                    .into_iter()
                    .flatten(),
            );
            let Some(cell) = self.learn(ctx).await else {
                break;
            };
            let arrival = self.move_second(ctx, &cell, policy, &formed, draw).await;
            if with_second(ctx.state(), |second| second.moving_to.is_none()) {
                assert_reachable!("move: an open move ends in the recovery tail");
                return;
            }
            // The target took the tenant further: the move gets its time
            // again, up to the ceiling.
            if seen.replace(arrival.progress) != Some(arrival.progress) {
                deadline = deadline.max((ctx.time().now() + super::FLEET_SETTLE).min(ceiling));
            }
            // The target settled the tenant: the flip is owed from now on.
            if arrival.settled && settled_at.is_none() {
                settled_at = Some(ctx.time().now());
                deadline = ctx.time().now() + super::FLEET_SETTLE;
            }
            draw = draw.wrapping_add(1);
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return;
            }
        }
        assert_always!(
            settled_at.is_none()
                || ctx.shutdown().is_cancelled()
                || crate::excuses::cell_lost(ctx.state(), crate::excuses::Excuse::SkipLateCellLost),
            "move: a settled move flips in the recovery tail",
            { "client" => self.client_id }
        );
    }

    /// Resolve the second cell's tenant through the run's initial members,
    /// which point at the second cell, and call it through a frontend of
    /// the run's cell, which does not serve it.
    async fn resolve_in_second(&mut self, ctx: &SimContext, formed: &CellJoin, draw: u64) {
        let initial_members = &self.machines[..self.layout.initial_members];
        // The cell the tenant lives in now: a move (#784 (coordinator
        // handover)) may have taken it to the run's cell. While a move is
        // open, its flip may have landed with its reply lost, so the
        // move's target serves it too.
        let (home, moving) = with_second(ctx.state(), |second| {
            (
                second.home.unwrap_or(formed.journals.cell_id),
                second.moving_to,
            )
        });
        let serves = |cell_id: u64| cell_id == home || moving == Some(cell_id);
        // A resolver may answer from the directory it last read (static
        // stability): after a move, it may still send the client to the
        // cell the tenant left, whose frontend answers the call moved
        // (#785). Only a cell the tenant lived in is such a hint.
        let lived_in = lived_in(ctx.state(), formed.journals.cell_id);
        let lived = |cell_id: u64| serves(cell_id) || lived_in.contains(&cell_id);
        let resolution = paros::client::resolve::resolve(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            initial_members,
            NAME,
            self.patience,
        )
        .await;
        if let Resolution::Resolved(resolved) = &resolution {
            assert_always!(
                serves(resolved.cell_id),
                "resolve: a tenant resolves to the cell that serves it",
                { "cell" => resolved.cell_id, "known" => home }
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
            initial_members,
            (NAME, b""),
            &own,
            self.patience,
        )
        .await;
        if let paros::client::resolve::Redirection::Redirected(redirect) = &redirection
            && with_second(ctx.state(), |second| second.tenant) == Some(redirect.tenant)
        {
            if !serves(redirect.cell_id) && lived(redirect.cell_id) {
                assert_reachable!("move: a resolver sends a client to the cell a tenant left");
            }
            assert_always!(
                lived(redirect.cell_id),
                "resolver: a tenant is sent to the cell it was created in",
                { "cell" => redirect.cell_id, "known" => home }
            );
            if let Resolution::Resolved(resolved) = &resolution {
                assert_always!(
                    serves(resolved.cell_id),
                    "resolve: a tenant resolves to the cell that serves it",
                    { "cell" => resolved.cell_id, "known" => home }
                );
                assert_reachable!(
                    "resolver: a tenant of a second cell resolves through a resolver"
                );
            }
        }
        if home != formed.journals.cell_id && moving.is_none() {
            self.follow_from_second(ctx, formed, draw).await;
        }
        // The run's frontends front the run's cell: a tenant that moved
        // there, or may have, is theirs to serve.
        if home == formed.journals.cell_id && moving.is_none() {
            let run_id = self.cell.as_ref().map(|cell| cell.journals.cell_id);
            self.read_through_run(ctx, formed, draw, (home, run_id.is_some_and(lived)))
                .await;
        }
    }

    /// Read the second cell's tenant's journal through the run's
    /// frontends, which do not serve it, then through a client that follows
    /// it. `lived_in_run`: a move took the tenant to the run's cell and back.
    async fn read_through_run(
        &self,
        ctx: &SimContext,
        formed: &CellJoin,
        draw: u64,
        (home, lived_in_run): (u64, bool),
    ) {
        let run = self.run_frontends(formed);
        if run.is_empty() {
            return;
        }
        let Some(pass) = self.journal_pass(ctx) else {
            return;
        };
        let client = self.connector.through(&run, &pass);
        let first = usize::try_from(draw % run.len() as u64).unwrap_or(0);
        let request = paros::client::Reader::new(LOCAL, 0).request(1, 0);
        let outcome = client.read_any(&request, first).await.outcome;
        // A frontend caches the tenant hop and drops it only once a machine
        // refuses the journal as unknown (§3.5, static stability). After a
        // move back, a frontend of the run's cell still holds the hop it
        // cached while the tenant lived there, and its machines serve the
        // journal until the source released the moved tenant's slots and
        // they forgot it (`Journals::forget`): the read is served from the
        // cell the tenant left. The tail proves the frontends then answer
        // moved (`final_follow`). Witness: replay-recipe
        // 14238023912774140646 1028534:10429108689412708984 on 3674597
        // (moved back at 77.8 s, a truncated read served at 78.5 s).
        assert_always!(
            !matches!(
                outcome,
                ReadOutcome::Page { .. } | ReadOutcome::Truncated { .. }
            ) || lived_in_run,
            "frontend: a frontend never serves a tenant of another cell",
            { "cell" => home, "lived_in_run" => lived_in_run }
        );
        if outcome == ReadOutcome::Moved {
            assert_reachable!("frontend: a call for a tenant of another cell is answered moved");
        }
        // The same call through a client that follows the tenant (#785
        // (moved record)): it resolves again, and asks the cell that
        // serves it.
        if with_second(ctx.state(), |second| second.journal.is_some()) {
            let (outcome, _) = self.follow(&run, &pass, draw).await;
            if matches!(outcome, ReadOutcome::Page { .. }) {
                assert_reachable!("move: a client of another cell follows the tenant to its cell");
            }
        }
    }

    /// A tenant that moved to the run's cell: a client of the second
    /// cell's frontends follows it there (#785 (moved record)).
    async fn follow_from_second(&self, ctx: &SimContext, formed: &CellJoin, draw: u64) {
        if !with_second(ctx.state(), |second| second.journal.is_some()) {
            return;
        }
        let Some(pass) = self.journal_pass(ctx) else {
            return;
        };
        let second: Vec<_> = formed.endpoint.iter().map(|(_, a)| a.clone()).collect();
        let (outcome, _) = self.follow(&second, &pass, draw).await;
        if matches!(outcome, ReadOutcome::Page { .. }) {
            assert_reachable!("move: a client of the cell a tenant left follows it");
        }
    }

    /// The frontends of the run's cell: every frontend but the second
    /// cell's machines, which front the second cell.
    fn run_frontends(&self, formed: &CellJoin) -> Vec<paros::model::address::Address> {
        // The run's cell as this operator learned it: an address of the
        // layout may serve another cell now (an idle cell formed where a
        // wiped initial member was, #216), whose frontend knows no tenant.
        let Some(cell) = &self.cell else {
            return Vec::new();
        };
        cell.servers
            .iter()
            .map(|(_, addr)| addr)
            .filter(|addr| !formed.endpoint.iter().any(|(_, a)| a == *addr))
            .cloned()
            .collect()
    }

    /// The pass a client of the second cell's tenant presents, its journal
    /// name bound to [`LOCAL`]. `None` when the name is not a valid one.
    fn journal_pass(&self, ctx: &SimContext) -> Option<Arc<Pass>> {
        let named = JournalName::new(
            &String::from_utf8_lossy(NAME),
            &String::from_utf8_lossy(JOURNAL),
        )
        .ok()?;
        let now = self.connector.providers().time().now();
        let pass = Arc::new(Pass::new(crate::frontend::token(
            ctx.state(),
            &String::from_utf8_lossy(NAME),
            "",
            Presented::Own,
            now,
        )));
        pass.bind(LOCAL, named);
        Some(pass)
    }

    /// Read the second cell's tenant's journal through the frontends at
    /// `frontends`, with a client that follows the tenant (#785 (moved
    /// record)): on a `moved` answer it resolves the tenant again through
    /// the machines it calls and asks the cell that serves it, once. The
    /// read's outcome, and whether a frontend of the cell the tenant lives
    /// in answered the journal unknown ([`Followed`]).
    async fn follow(
        &self,
        frontends: &[paros::model::address::Address],
        pass: &Arc<Pass>,
        draw: u64,
    ) -> (ReadOutcome, bool) {
        if frontends.is_empty() {
            return (ReadOutcome::Ambiguous, false);
        }
        let first = usize::try_from(draw % frontends.len() as u64).unwrap_or(0);
        // The client resolves through the machines it calls, from the one it
        // calls first, never through the layout's initial members: an address a
        // wiped machine left may serve another cell, with no universe, and it
        // refuses every resolution (#216; a witness of #785 (moved record) on
        // b646f4f).
        let mut entry = frontends.to_vec();
        entry.rotate_left(first);
        let rehome = paros::client::rehome::Resolving::new(
            (
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
            ),
            entry,
            NAME.to_vec(),
            pass.clone(),
            self.patience,
        );
        let through = self.connector.through(frontends, pass);
        let followed = Arc::new(Followed::over(through.observer().clone()));
        let client = through
            .with_observer(followed.clone())
            .with_rehome(Arc::new(rehome));
        let request = paros::client::Reader::new(LOCAL, 0).request(1, 0);
        let outcome = client.read_any(&request, first).await.outcome;
        (outcome, followed.gone())
    }

    /// Create the journal [`JOURNAL`] in the second cell's tenant `tenant`,
    /// through its cell's tenant coordinator (#785 (moved record)): a
    /// client of the other cell follows the tenant to it.
    async fn journal_in_second(&mut self, ctx: &SimContext, formed: &CellJoin, tenant: TenantId) {
        let Some(election) = formed.journals.election else {
            return;
        };
        let client = self.connect(ctx, &formed.endpoint);
        let request = paros::client::journals::JournalRequest {
            request: moonpool_sim::sim_random_range(1..u64::MAX),
            tenant,
            op: paros::client::journals::JournalOp::Create {
                name: JOURNAL.to_vec(),
                writer: paros::core::journal::WriterMode::Single,
                desired: None,
                tags: paros::model::tags::UserTags::default(),
                store: paros::model::tenant::StoreDurability::Ordered,
            },
        };
        let answer = paros::client::journals::request(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &client,
            election,
            &request,
            self.patience,
        )
        .await;
        if let paros::client::journals::JournalAnswer::Created { id, .. }
        | paros::client::journals::JournalAnswer::NameTaken { id } = answer
        {
            with_second(ctx.state(), |second| second.journal = Some(id));
        }
    }

    /// Remove the second cell: first while it still hosts its tenant, which
    /// must be refused, then its tenant, then the cell; then add it again,
    /// which must be refused.
    /// Keep a removal of the tenant this phase cut short as this
    /// operator's own pending operation when the tenant lives in the run's
    /// cell (#784 (coordinator handover)): it moved there, so the run's
    /// cell judges it after chaos, and the phase is never taken again once
    /// the chaos window closed. The operator finishes it in the recovery
    /// tail like any removal it stopped in ([`FleetOps::settle`]).
    /// Witness: replay-main 3587726640808527649 on 45e0926: `hooli` moved
    /// to the run's cell, its removal marked it `REMOVING` and waited past
    /// its patience for the cell's `DropTenant`, and no operator finished
    /// it: red on "fleet: no tenant is left mid-operation after chaos".
    fn pend_moved_removal(&mut self, session: &FleetSession, run_cell: u64) {
        if self.pending.is_none() && removal_owed_by_run(session.directory(), run_cell) {
            self.pending = Some(Pending::Remove(NAME.to_vec()));
        }
    }

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
            self.pend_moved_removal(&session, cell.journals.cell_id);
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
    pub(super) async fn final_second(
        &mut self,
        ctx: &SimContext,
        directory: &FleetDirectory,
        policy: CheckpointPolicy,
    ) {
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
                super::listed_under(directory, registry, tenant, cell_id),
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
        // A renamed initial member of the universe cell leaves the second
        // cell's universe pointer naming nowhere: by design the cell reads the
        // directory only through it, so its `Resolve` refuses, and a client
        // that follows a moved tenant there fails (#349,
        // `docs/architecture/03-7`). The tail's operator sends `universe
        // add-cell` again, which repoints it, before the follow check. Witness:
        // seed 5196045004897168212 on 1689868, the moved-member scenario
        // renamed the universe initial member at 16.9 s and no tenant operation
        // stalled, so no earlier repoint ran.
        if !crate::machine::renamed_initial_members(ctx.state(), self.layout.initial_members)
            .is_empty()
            && let Some(cell) = self.learn(ctx).await
        {
            assert_reachable!(
                "universe: the tail repoints a second cell after an initial member moved"
            );
            self.repoint_second(ctx, &cell, policy, self.client_id)
                .await;
        }
        self.final_follow(ctx, directory, &formed).await;
    }

    /// After chaos, a client of the cell the second cell's tenant does not
    /// live in reaches its journal (#785 (moved record)): the frontends
    /// there answer `moved`, and the client follows the tenant. No client
    /// call fails for good across a move.
    async fn final_follow(&self, ctx: &SimContext, directory: &FleetDirectory, formed: &CellJoin) {
        let Some(journal) = with_second(ctx.state(), |second| {
            second
                .tenant
                .zip(second.journal)
                .map(|(tenant, journal)| JournalIdentifier::new(tenant, journal))
        }) else {
            return;
        };
        let Some((_, entry)) = directory.named(NAME) else {
            return;
        };
        // An open move, a removal or a tenant mid-operation owes no answer.
        let run = self.run_frontends(formed);
        if entry.moving_to.is_some()
            || entry.state != paros::model::fleet::TenantState::Ready
            || run.is_empty()
        {
            return;
        }
        let other: Vec<_> = if entry.cell_id == formed.journals.cell_id {
            run
        } else {
            formed.endpoint.iter().map(|(_, a)| a.clone()).collect()
        };
        let Some(pass) = self.journal_pass(ctx) else {
            return;
        };
        let deadline = ctx.time().now() + super::FLEET_SETTLE;
        let mut draw = self.client_id;
        let mut last = ReadOutcome::Ambiguous;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            let (outcome, gone) = self.follow(&other, &pass, draw).await;
            last = outcome;
            // A truncated read is served too: the call reached the journal.
            if matches!(
                last,
                ReadOutcome::Page { .. } | ReadOutcome::Truncated { .. }
            ) {
                assert_reachable!("move: a client follows a moved tenant after chaos");
                return;
            }
            // An operator deleted the journal (`DELETE_JOURNAL` names a
            // journal of any READY tenant, the moved one included): the
            // name is gone (replay-main 16860049563827136429 on 31fb5d9:
            // deleted at 93.6 s, the follow asked from 105 s on). Judged
            // by what the frontends answered (`Followed`), never by the
            // read's outcome: the client reports the journal unknown only
            // when every frontend answered so, and a silent one leaves the
            // read ambiguous with the name gone all the same (replay-recipe
            // 9860678694894022772 99365:15397153939371774380 on c4ed4ab:
            // deleted at 255.2 s, two of the four machines silent in the
            // tail). An `unknown` answer is not the proof of the delete,
            // because the cell the tenant left answers `unknown` too
            // (#885 (moved tenant)): the tenant board is (#1003 (uncounted
            // skips from fix-main)).
            if follow_excused(ctx.state(), journal, gone) {
                crate::excuses::note(
                    ctx.state(),
                    crate::excuses::Excuse::SkipFollowDeleted,
                    Some(journal),
                );
                return;
            }
            draw = draw.wrapping_add(1);
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return;
            }
        }
        assert_always_or_unreachable!(
            ctx.shutdown().is_cancelled() || crate::excuses::cell_lost(ctx.state(), crate::excuses::Excuse::SkipLateCellLost),
            "move: no client call fails for good across a move",
            { "journal" => journal.journal.0, "last" => format!("{last:?}") }
        );
    }
}

/// Whether the follow oracle owes no verdict on `journal`
/// ([`FleetOps::final_follow`]): a frontend answered it unknown after one
/// answered `moved` (`gone`), and a machine folded its tombstone. A delete
/// that was only asked, or an `unknown` from the cell the tenant left
/// (#885 (moved tenant)), excuses nothing (#1003 (uncounted skips from
/// fix-main)).
fn follow_excused(
    state: &moonpool_sim::StateHandle,
    journal: JournalIdentifier,
    gone: bool,
) -> bool {
    let board = crate::audit::tenants::tenant_board(state);
    gone && crate::audit::tenants::lock(&board).tombstoned(journal)
}

/// The observer of a client that follows the tenant ([`FleetOps::follow`]):
/// the actor's fence the client came with, and what the frontends
/// answered. A follow starts at the cell the tenant does not live in,
/// whose frontends answer `moved`; a read answered `unknown` after a
/// `moved` one is most likely the tenant's cell's, but the cell the tenant
/// left answers `unknown` too (#885 (moved tenant)), so `gone` is not the
/// proof of a delete ([`follow_excused`]).
struct Followed {
    fence: Arc<dyn CallObserver>,
    inner: Mutex<FollowedInner>,
}

#[derive(Default)]
struct FollowedInner {
    /// The fence's token behind each token handed out, if it took one.
    fenced: BTreeMap<u64, Option<u64>>,
    next: u64,
    /// A frontend answered `moved`: the client follows the tenant.
    moved: bool,
    /// A frontend of the tenant's cell answered the journal unknown.
    gone: bool,
}

impl Followed {
    /// Observe over `fence`, which judges the fenced calls.
    fn over(fence: Arc<dyn CallObserver>) -> Self {
        Self {
            fence,
            inner: Mutex::new(FollowedInner::default()),
        }
    }

    /// Whether a frontend of the tenant's cell answered the journal unknown.
    fn gone(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .gone
    }
}

impl CallObserver for Followed {
    fn invoked(&self, attempt: Attempted<'_>) -> Option<u64> {
        let fenced = self.fence.invoked(attempt);
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let token = inner.next;
        inner.next += 1;
        inner.fenced.insert(token, fenced);
        Some(token)
    }

    fn answered(&self, token: u64, answer: Answered<'_>) {
        let fenced = {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            if let Answered::Read(outcome) = answer {
                match outcome {
                    ReadOutcome::Moved => inner.moved = true,
                    ReadOutcome::UnknownJournal if inner.moved => inner.gone = true,
                    _ => {}
                }
            }
            inner.fenced.remove(&token).flatten()
        };
        if let Some(fenced) = fenced {
            self.fence.answered(fenced, answer);
        }
    }

    fn resolving(&self, write: &paros::wire::Write) -> Option<u64> {
        self.fence.resolving(write)
    }

    fn resolved(&self, token: u64, report: &paros::client::ResolveReport) {
        self.fence.resolved(token, report);
    }

    fn merged(&self, write: &paros::wire::Write) {
        self.fence.merged(write);
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
    use std::sync::{Arc, Mutex};

    use paros::model::fleet::buckets::bucket_of;

    use super::{Answered, Attempted, CallObserver, Followed, LOCAL, ReadOutcome};

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

    /// Only the second cell's tenant `REMOVING` in the run's cell is a
    /// removal the run owes an end: not one still in the second cell, not
    /// one the directory holds `READY`.
    #[test]
    fn a_removal_is_owed_only_for_the_tenant_removing_in_the_run_cell() {
        use paros::client::checkpoint::Checkpointable as _;
        use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
        use paros::model::fleet::{FleetCommand, FleetDirectory, FleetEntry, TenantState};
        let apply = |directory: &mut FleetDirectory, seq: u64, command: FleetCommand| {
            directory.apply(seq, &FleetEntry::new(42, command).encode());
        };
        let tenant = TenantId(20);
        let held = |cell_id: u64, state: Option<TenantState>| {
            let mut directory = FleetDirectory::default();
            apply(
                &mut directory,
                0,
                FleetCommand::FormFleet {
                    control: JournalIdentifier::new(TenantId(7), JournalId(8)),
                    name: Vec::new(),
                },
            );
            apply(
                &mut directory,
                1,
                FleetCommand::AddCell {
                    cell_id,
                    control: JournalIdentifier::new(TenantId(5), JournalId(6)),
                    name: Vec::new(),
                    kind: paros::model::fleet::CellKind::default(),
                    endpoint: Vec::new(),
                },
            );
            apply(
                &mut directory,
                2,
                FleetCommand::MarkCell {
                    cell_id,
                    state: paros::model::fleet::CellState::Ready,
                },
            );
            apply(
                &mut directory,
                3,
                FleetCommand::RegisterTenant {
                    control: JournalIdentifier::new(tenant, JournalId(21)),
                    name: super::NAME.to_vec(),
                    cell_id,
                    survives: paros::model::tenant::Survives::Az,
                    tags: paros::model::tags::UserTags::none(),
                    placement: paros::model::fleet::buckets::Placement::Bucket,
                },
            );
            if let Some(state) = state {
                apply(
                    &mut directory,
                    4,
                    FleetCommand::MarkTenant { tenant, state },
                );
            }
            directory
        };
        let removing = Some(TenantState::Removing);
        assert!(super::removal_owed_by_run(&held(9, removing), 9));
        assert!(!super::removal_owed_by_run(&held(10, removing), 9));
        assert!(!super::removal_owed_by_run(
            &held(9, Some(TenantState::Ready)),
            9
        ));
        assert!(!super::removal_owed_by_run(&FleetDirectory::default(), 9));
    }

    /// A fence that takes a token for every attempt and counts the answers
    /// handed back to it.
    #[derive(Default)]
    struct Counting(Mutex<(u64, Vec<u64>)>);

    impl CallObserver for Counting {
        fn invoked(&self, _: Attempted<'_>) -> Option<u64> {
            let mut inner = self.0.lock().unwrap();
            inner.0 += 100;
            Some(inner.0)
        }

        fn answered(&self, token: u64, _: Answered<'_>) {
            self.0.lock().unwrap().1.push(token);
        }

        fn resolving(&self, _: &paros::wire::Write) -> Option<u64> {
            None
        }

        fn resolved(&self, _: u64, _: &paros::client::ResolveReport) {}
    }

    /// The follow oracle skips only on a journal a machine folded the
    /// tombstone of, and only when the follow found the name gone: a delete
    /// that was only asked is still judged (#1003 (uncounted skips from
    /// fix-main)).
    #[test]
    fn a_follow_is_excused_only_by_a_folded_tombstone() {
        use paros::core::NodeId;
        use paros::core::journal::{JournalId, JournalIdentifier, TenantId, WriterMode};
        use paros::core::membership::{AcceptorConfig, QuorumSystem};
        use paros::model::tenant::{Desired, StoreDurability, TenantEvent};

        let state = moonpool_sim::StateHandle::new();
        let control = JournalIdentifier::new(TenantId(5), JournalId(1));
        let journal = JournalIdentifier::new(TenantId(5), JournalId(7));
        let fold = |lsn: u64, event: &TenantEvent| {
            let board = crate::audit::tenants::tenant_board(&state);
            crate::audit::tenants::lock(&board).folded(NodeId(10), control, lsn, event);
        };
        fold(0, &TenantEvent::Described);
        fold(
            1,
            &TenantEvent::Created {
                request: 100,
                id: journal.journal,
                name: super::JOURNAL.to_vec(),
                config: AcceptorConfig::new(
                    vec![NodeId(1), NodeId(2), NodeId(3)],
                    QuorumSystem::Majority,
                ),
                writer: WriterMode::Single,
                store: StoreDurability::Ordered,
                desired: Desired::DEFAULT,
            },
        );
        // A delete asked, not applied: no machine folded a tombstone, so an
        // `unknown` answer (the cell the tenant left) excuses nothing.
        assert!(!super::follow_excused(&state, journal, true));
        fold(
            2,
            &TenantEvent::Deleted {
                request: 101,
                id: journal.journal,
            },
        );
        assert!(super::follow_excused(&state, journal, true));
        // The follow must still find the name gone.
        assert!(!super::follow_excused(&state, journal, false));
        // Another journal's tombstone excuses nothing.
        let other = JournalIdentifier::new(TenantId(6), journal.journal);
        assert!(!super::follow_excused(&state, other, true));
    }

    /// The name is gone only when a frontend answered the journal unknown
    /// after one answered `moved` (the tenant's cell's answer), and every
    /// answer still reaches the fence under the fence's own token
    /// (replay-recipe 9860678694894022772 99365:15397153939371774380 on
    /// c4ed4ab).
    #[test]
    fn a_follow_is_gone_only_when_the_tenants_cell_answers_unknown() {
        let fence = Arc::new(Counting::default());
        let followed = Followed::over(fence.clone());
        let read = paros::client::Reader::new(LOCAL, 0).request(1, 0);
        let answer = |outcome: &ReadOutcome| {
            let token = followed.invoked(Attempted::Read(&read)).expect("a token");
            followed.answered(token, Answered::Read(outcome));
        };
        // The cell the tenant left answers unknown (a stale resolution):
        // not the tenant's cell's word.
        answer(&ReadOutcome::UnknownJournal);
        assert!(!followed.gone());
        answer(&ReadOutcome::Moved);
        answer(&ReadOutcome::Ambiguous);
        assert!(!followed.gone());
        answer(&ReadOutcome::UnknownJournal);
        assert!(followed.gone());
        let inner = fence.0.lock().unwrap();
        assert_eq!(
            inner.1,
            vec![100, 200, 300, 400],
            "every answer under its fence token"
        );
    }
}
