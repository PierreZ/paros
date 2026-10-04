//! Fleet operations (#229, `docs/architecture.md` §3.7): registering the
//! cell in meta at `init`, and creating and removing tenants — each an
//! **idempotent state machine** over two journals, meta (`1/1`) and the
//! cell control journal (`2/1`), as FDB's metacluster runs them.
//!
//! An operation is a sequence of steps, one entry each. A step reads both
//! journals, decides the next entry the operation is missing from what they
//! hold — never from what the caller remembers — then claims the journal it
//! writes and writes that entry. So an operation that stopped anywhere (a
//! crash, a lost answer, a rival) is resumed by running it again: the next
//! step finds what is already there and moves on.
//!
//! - **Cell registration** ([`CellRegistration`]), `init`'s fleet steps:
//!   meta records the cell `REGISTERING` (the first cell names the fleet,
//!   with the id the caller drew), the cell records its half
//!   (`RegisterFleet`), meta marks the cell `READY`.
//! - **Tenant creation** ([`TenantCreation`]): meta records the tenant
//!   `REGISTERING` under the id the caller drew, assigned to the `READY`
//!   cell; the cell hosts it; meta marks it `READY`. A name already
//!   `REGISTERING` is resumed, whoever started it.
//! - **Tenant removal** ([`TenantRemoval`]): meta marks the tenant
//!   `REMOVING`, the cell unhosts it (a fence: the cell never hosts that id
//!   again), meta forgets it.
//!
//! **Registration on both sides, verified on every step.** Every entry
//! names the fleet and the cell the operation believes it talks to
//! ([`FleetContext`]), fixed at its first step; the journal refuses an entry
//! naming another one at apply, and the step refuses before writing when
//! what it reads disagrees ([`FleetRefusal::ContextChanged`]). A caller that
//! resumes an operation with the context it saved from an earlier run
//! ([`TenantCreation::resume`]) is refused if the fleet or the cell changed
//! since.
//!
//! Like the rest of the client: provider-generic, wasm-safe, no randomness
//! (the caller draws every id), every outcome typed.

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalKey, TenantId};

use super::Client;
use super::checkpoint::{Applied, Checkpointable, Checkpointer, Folder, LoadOutcome, OpenOutcome};
use crate::system::{
    CellState, FleetContext, META, METADATA_VERSION, Meta, MetaEvent, MetaRefusal, REGISTRY,
    Registry, RegistryEvent, RegistryRefusal, SystemCommand, TenantState,
};

/// The entry a step wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FleetAction {
    /// Meta: `RegisterCell`.
    RegisterCell,
    /// Cell: `RegisterFleet`.
    RegisterFleet,
    /// Meta: `CellReady`.
    CellReady,
    /// Meta: `RegisterTenant`.
    RegisterTenant,
    /// Cell: `HostTenant`.
    HostTenant,
    /// Meta: `TenantReady`.
    TenantReady,
    /// Meta: `RemoveTenant`.
    RemoveTenant,
    /// Cell: `UnhostTenant`.
    UnhostTenant,
    /// Meta: `ForgetTenant`.
    ForgetTenant,
}

/// Why an operation refused to go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetRefusal {
    /// Meta names no fleet: `init` has not registered a cell.
    NoFleet,
    /// No cell is `READY` to receive a tenant.
    NoReadyCell,
    /// The cell control journal is registered as another cell, or to
    /// another fleet than meta's: `init` talks to the wrong place.
    OtherCell,
    /// The fleet or the cell is not the one this operation started against.
    ContextChanged,
    /// The tenant is being removed.
    TenantRemoving {
        /// The tenant.
        tenant: TenantId,
    },
    /// No tenant has the name (or the tenant this operation started on is
    /// gone).
    UnknownTenant,
    /// Meta refused the step's entry.
    Meta(MetaRefusal),
    /// The cell control journal refused the step's entry.
    Cell(RegistryRefusal),
}

/// What one step came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetStep {
    /// The step's entry was written (`Some`), or another writer had already
    /// moved the operation on (`None`): step again.
    Stepped(Option<FleetAction>),
    /// The operation is complete.
    Done(FleetContext),
    /// The tenant id the caller drew is reserved or taken: redraw, then step
    /// again.
    IdTaken,
    /// The operation cannot go on.
    Refused(FleetRefusal),
    /// A journal was not read, not claimed, or the entry is not known
    /// written: step again later (the operation resumes).
    Unavailable,
}

/// The next entry an operation is missing, or its end.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Next {
    Write {
        journal: JournalKey,
        command: SystemCommand,
        action: FleetAction,
    },
    Done(FleetContext),
}

fn write(journal: JournalKey, action: FleetAction, command: SystemCommand) -> Next {
    Next::Write {
        journal,
        command,
        action,
    }
}

/// Read `journal` from its floor to its tail into `state`.
async fn read<P: Providers, S: Checkpointable + Clone>(
    client: &Client<P>,
    journal: JournalKey,
    state: S,
    first: usize,
) -> Option<S> {
    let mut folder = Folder::new(state);
    match super::checkpoint::load(&mut folder, journal, client, first, 0).await {
        LoadOutcome::Loaded { .. } => Some(folder.state().clone()),
        _ => None,
    }
}

/// What one written entry folded to, as the step reports it.
enum Verdict {
    Applied,
    IdTaken,
    NameTaken,
    Refused(FleetRefusal),
}

fn meta_verdict(event: &MetaEvent) -> Verdict {
    match event {
        MetaEvent::Refused(MetaRefusal::IdTaken { .. } | MetaRefusal::Reserved { .. }) => {
            Verdict::IdTaken
        }
        MetaEvent::Refused(MetaRefusal::NameTaken { .. }) => Verdict::NameTaken,
        MetaEvent::Refused(refusal) => Verdict::Refused(FleetRefusal::Meta(refusal.clone())),
        _ => Verdict::Applied,
    }
}

fn cell_verdict(event: &RegistryEvent) -> Verdict {
    match event {
        RegistryEvent::Refused(refusal) => Verdict::Refused(FleetRefusal::Cell(refusal.clone())),
        _ => Verdict::Applied,
    }
}

/// Run one step: read both journals, decide with `decide`, and write the
/// entry it names to the journal it names — claimed as `owner`, folded to
/// its tail, and decided again on what that fold holds, so the entry is
/// judged against exactly the state it was decided on. Returns the outcome
/// and the freshest meta the step folded.
async fn step<P: Providers>(
    client: &Client<P>,
    owner: u64,
    first: usize,
    decide: impl Fn(&Meta, &Registry) -> Result<Next, FleetRefusal>,
) -> (FleetStep, Option<Meta>) {
    let Some(meta) = read(client, META, Meta::new(), first).await else {
        return (FleetStep::Unavailable, None);
    };
    let Some(cell) = read(client, REGISTRY, Registry::new([]), first).await else {
        return (FleetStep::Unavailable, Some(meta));
    };
    let journal = match decide(&meta, &cell) {
        Err(refusal) => return (FleetStep::Refused(refusal), Some(meta)),
        Ok(Next::Done(context)) => return (FleetStep::Done(context), Some(meta)),
        Ok(Next::Write { journal, .. }) => journal,
    };
    let policy = client.tunables().checkpoint_policy();
    if journal == META {
        let mut writer = Checkpointer::new(META, owner, Meta::new(), policy);
        if !matches!(writer.open(client, first).await, OpenOutcome::Open { .. }) {
            return (FleetStep::Unavailable, Some(meta));
        }
        let next = decide(writer.state(), &cell);
        let outcome = written(&mut writer, client, first, journal, next, meta_verdict).await;
        (outcome, Some(writer.state().clone()))
    } else {
        let mut writer = Checkpointer::new(REGISTRY, owner, Registry::new([]), policy);
        if !matches!(writer.open(client, first).await, OpenOutcome::Open { .. }) {
            return (FleetStep::Unavailable, Some(meta));
        }
        let next = decide(&meta, writer.state());
        let outcome = written(&mut writer, client, first, journal, next, cell_verdict).await;
        (outcome, Some(meta))
    }
}

/// Write the entry `next` names, if it still targets `journal` (the owner
/// holds it, folded to its tail).
async fn written<P: Providers, S: Checkpointable>(
    writer: &mut Checkpointer<S>,
    client: &Client<P>,
    first: usize,
    journal: JournalKey,
    next: Result<Next, FleetRefusal>,
    verdict: impl Fn(&S::Event) -> Verdict,
) -> FleetStep {
    match next {
        Err(refusal) => FleetStep::Refused(refusal),
        Ok(Next::Done(context)) => FleetStep::Done(context),
        // Another writer moved the operation on: the next step is elsewhere.
        Ok(Next::Write { journal: other, .. }) if other != journal => FleetStep::Stepped(None),
        Ok(Next::Write {
            command, action, ..
        }) => match writer.apply(client, command.encode(), first).await {
            Applied::Folded(event) => match verdict(&event) {
                Verdict::Applied => FleetStep::Stepped(Some(action)),
                Verdict::IdTaken => FleetStep::IdTaken,
                // A rival creator holds the name: the next step adopts it.
                Verdict::NameTaken => FleetStep::Stepped(None),
                Verdict::Refused(refusal) => FleetStep::Refused(refusal),
            },
            Applied::NotFolded(_) => FleetStep::Unavailable,
        },
    }
}

/// The most steps one `run` takes: every operation ends in a handful, and
/// a rival that keeps moving it on is bounded by the patience anyway.
const MAX_STEPS: usize = 64;

/// `init`'s fleet steps (§3.1, steps 2 and 3): register cell `cell_id` in
/// meta — the first cell names the fleet — and record the cell's own half,
/// then mark the cell `READY`.
#[derive(Clone, Debug)]
pub struct CellRegistration {
    owner: u64,
    cell_id: u64,
    fleet_draw: u64,
}

impl CellRegistration {
    /// Register cell `cell_id`, writing as client `owner`. `fleet_draw` is
    /// the fleet id to mint if meta names none yet (non-zero; a resumed run
    /// keeps the fleet already recorded, on either side).
    #[must_use]
    pub fn new(owner: u64, cell_id: u64, fleet_draw: u64) -> Self {
        Self {
            owner,
            cell_id,
            fleet_draw: fleet_draw.max(1),
        }
    }

    fn decide(&self, meta: &Meta, cell: &Registry) -> Result<Next, FleetRefusal> {
        let registered = cell.registration().map(|r| r.context);
        if registered.is_some_and(|r| r.cell_id != self.cell_id) {
            return Err(FleetRefusal::OtherCell);
        }
        let fleet_id = match (meta.fleet_id(), registered) {
            (Some(fleet), Some(r)) if r.fleet_id != fleet => return Err(FleetRefusal::OtherCell),
            (Some(fleet), _) => fleet,
            (None, Some(r)) => r.fleet_id,
            (None, None) => self.fleet_draw,
        };
        let context = FleetContext {
            fleet_id,
            cell_id: self.cell_id,
        };
        let Some(entry) = meta.cell(self.cell_id) else {
            return Ok(write(
                META,
                FleetAction::RegisterCell,
                SystemCommand::RegisterCell {
                    context,
                    metadata_version: METADATA_VERSION,
                },
            ));
        };
        if registered.is_none() {
            return Ok(write(
                REGISTRY,
                FleetAction::RegisterFleet,
                SystemCommand::RegisterFleet {
                    context,
                    metadata_version: METADATA_VERSION,
                },
            ));
        }
        match entry.state {
            CellState::Registering | CellState::Restoring => Ok(write(
                META,
                FleetAction::CellReady,
                SystemCommand::CellReady { context },
            )),
            CellState::Ready => Ok(Next::Done(context)),
            state @ CellState::Removing => Err(FleetRefusal::Meta(MetaRefusal::CellState {
                cell_id: self.cell_id,
                state,
            })),
        }
    }

    /// One step (see the module docs).
    #[tracing::instrument(level = "debug", skip_all, fields(cell = self.cell_id))]
    pub async fn step<P: Providers>(&mut self, client: &Client<P>, first: usize) -> FleetStep {
        step(client, self.owner, first, |meta, cell| {
            self.decide(meta, cell)
        })
        .await
        .0
    }

    /// Step until the cell is registered or refused, retrying an
    /// unavailable step for up to `patience`.
    pub async fn run<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        patience: Duration,
    ) -> FleetStep {
        let deadline = client.now() + patience;
        let mut last = FleetStep::Unavailable;
        for _ in 0..MAX_STEPS {
            last = self.step(client, first).await;
            match last {
                FleetStep::Stepped(_) => {}
                FleetStep::Unavailable => {
                    if client.now() >= deadline
                        || !client.pause(client.tunables.retry_backoff).await
                    {
                        return last;
                    }
                }
                _ => return last,
            }
        }
        last
    }
}

/// Creating a tenant (§3.7): `REGISTERING` in meta, hosted by its cell,
/// then `READY`.
#[derive(Clone, Debug)]
pub struct TenantCreation {
    owner: u64,
    name: Vec<u8>,
    candidate: TenantId,
    context: Option<FleetContext>,
    tenant: Option<TenantId>,
    adopted: bool,
}

impl TenantCreation {
    /// Create the tenant `name` under the id `candidate` (the caller's draw,
    /// from the user range), writing as client `owner`.
    #[must_use]
    pub fn new(owner: u64, name: Vec<u8>, candidate: TenantId) -> Self {
        Self {
            owner,
            name,
            candidate,
            context: None,
            tenant: None,
            adopted: false,
        }
    }

    /// Resume a creation with the context an earlier run of it reported:
    /// refused at the first step if the fleet or the cell changed since.
    #[must_use]
    pub fn resume(mut self, context: FleetContext) -> Self {
        self.context = Some(context);
        self
    }

    /// The fleet and cell this creation runs against, once its first step
    /// fixed them.
    #[must_use]
    pub fn context(&self) -> Option<FleetContext> {
        self.context
    }

    /// The tenant's id, once meta records it.
    #[must_use]
    pub fn tenant(&self) -> Option<TenantId> {
        self.tenant
    }

    /// Whether this creation took over a tenant it did not register itself:
    /// a name another run had left `REGISTERING` (resumed), or `READY`
    /// already.
    #[must_use]
    pub fn adopted(&self) -> bool {
        self.adopted
    }

    /// Draw again after [`FleetStep::IdTaken`].
    pub fn redraw(&mut self, candidate: TenantId) {
        self.candidate = candidate;
    }

    fn decide(&self, meta: &Meta, cell: &Registry) -> Result<Next, FleetRefusal> {
        let fleet_id = meta.fleet_id().ok_or(FleetRefusal::NoFleet)?;
        let context = match self.context {
            Some(context) => {
                if context.fleet_id != fleet_id || meta.cell(context.cell_id).is_none() {
                    return Err(FleetRefusal::ContextChanged);
                }
                context
            }
            None => FleetContext {
                fleet_id,
                cell_id: meta.ready_cell().ok_or(FleetRefusal::NoReadyCell)?,
            },
        };
        // The cell control journal read is this context's cell's.
        if cell.registration().map(|r| r.context) != Some(context) {
            return Err(FleetRefusal::ContextChanged);
        }
        // A name another tenant took after ours was removed is not ours.
        let found = meta
            .by_name(&self.name)
            .filter(|t| self.tenant.is_none_or(|ours| ours == *t));
        let Some(tenant) = found else {
            if self.tenant.is_some() {
                // Ours, and gone: removed under us.
                return Err(FleetRefusal::UnknownTenant);
            }
            return Ok(write(
                META,
                FleetAction::RegisterTenant,
                SystemCommand::RegisterTenant {
                    context,
                    tenant: self.candidate,
                    name: self.name.clone(),
                },
            ));
        };
        let entry = meta.tenant(tenant).ok_or(FleetRefusal::UnknownTenant)?;
        if entry.cell_id != context.cell_id {
            return Err(FleetRefusal::ContextChanged);
        }
        match entry.state {
            TenantState::Registering if cell.tenant(tenant).is_none() => Ok(write(
                REGISTRY,
                FleetAction::HostTenant,
                SystemCommand::HostTenant {
                    context,
                    tenant,
                    name: self.name.clone(),
                },
            )),
            TenantState::Registering => Ok(write(
                META,
                FleetAction::TenantReady,
                SystemCommand::TenantReady { context, tenant },
            )),
            TenantState::Ready => Ok(Next::Done(context)),
            TenantState::Removing => Err(FleetRefusal::TenantRemoving { tenant }),
            state => Err(FleetRefusal::Meta(MetaRefusal::TenantState {
                tenant,
                state,
            })),
        }
    }

    /// One step (see the module docs).
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = self.candidate.0))]
    pub async fn step<P: Providers>(&mut self, client: &Client<P>, first: usize) -> FleetStep {
        let (outcome, meta) = step(client, self.owner, first, |meta, cell| {
            self.decide(meta, cell)
        })
        .await;
        if let Some(meta) = meta {
            self.observe(&meta);
        }
        outcome
    }

    /// Learn what meta fixed: the tenant's id and the context.
    fn observe(&mut self, meta: &Meta) {
        let Some(tenant) = meta.by_name(&self.name) else {
            return;
        };
        let Some(entry) = meta.tenant(tenant) else {
            return;
        };
        if self.tenant.is_none() {
            self.adopted = tenant != self.candidate || entry.state != TenantState::Registering;
            self.tenant = Some(tenant);
        }
        if self.context.is_none() && meta.fleet_id().is_some() {
            self.context = meta.fleet_id().map(|fleet_id| FleetContext {
                fleet_id,
                cell_id: entry.cell_id,
            });
        }
    }

    /// Step until the tenant is `READY` or refused, retrying an
    /// unavailable step for up to `patience`; a taken id is redrawn through
    /// `redraw`.
    pub async fn run<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        patience: Duration,
        mut redraw: impl FnMut() -> TenantId,
    ) -> FleetStep {
        let deadline = client.now() + patience;
        let mut last = FleetStep::Unavailable;
        for _ in 0..MAX_STEPS {
            last = self.step(client, first).await;
            match last {
                FleetStep::Stepped(_) => {}
                FleetStep::IdTaken => self.redraw(redraw()),
                FleetStep::Unavailable => {
                    if client.now() >= deadline
                        || !client.pause(client.tunables.retry_backoff).await
                    {
                        return last;
                    }
                }
                _ => return last,
            }
        }
        last
    }
}

/// Removing a tenant (§3.7): `REMOVING` in meta, unhosted by its cell (a
/// fence), then forgotten.
#[derive(Clone, Debug)]
pub struct TenantRemoval {
    owner: u64,
    name: Vec<u8>,
    tenant: Option<TenantId>,
}

impl TenantRemoval {
    /// Remove the tenant `name`, writing as client `owner`.
    #[must_use]
    pub fn new(owner: u64, name: Vec<u8>) -> Self {
        Self {
            owner,
            name,
            tenant: None,
        }
    }

    /// The tenant's id, once a step found it.
    #[must_use]
    pub fn tenant(&self) -> Option<TenantId> {
        self.tenant
    }

    fn decide(&self, meta: &Meta, cell: &Registry) -> Result<Next, FleetRefusal> {
        let fleet_id = meta.fleet_id().ok_or(FleetRefusal::NoFleet)?;
        let found = meta
            .by_name(&self.name)
            .filter(|t| self.tenant.is_none_or(|ours| ours == *t));
        let Some(tenant) = found else {
            return match self.tenant {
                // Ours, and forgotten: removed.
                Some(_) => Ok(Next::Done(FleetContext {
                    fleet_id,
                    cell_id: cell.registration().map_or(0, |r| r.context.cell_id),
                })),
                None => Err(FleetRefusal::UnknownTenant),
            };
        };
        let entry = meta.tenant(tenant).ok_or(FleetRefusal::UnknownTenant)?;
        let context = FleetContext {
            fleet_id,
            cell_id: entry.cell_id,
        };
        if entry.state != TenantState::Removing {
            return Ok(write(
                META,
                FleetAction::RemoveTenant,
                SystemCommand::RemoveTenant { fleet_id, tenant },
            ));
        }
        if cell.registration().map(|r| r.context) != Some(context) {
            return Err(FleetRefusal::ContextChanged);
        }
        if !cell.is_unhosted(tenant) {
            return Ok(write(
                REGISTRY,
                FleetAction::UnhostTenant,
                SystemCommand::UnhostTenant { context, tenant },
            ));
        }
        Ok(write(
            META,
            FleetAction::ForgetTenant,
            SystemCommand::ForgetTenant { fleet_id, tenant },
        ))
    }

    /// One step (see the module docs).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn step<P: Providers>(&mut self, client: &Client<P>, first: usize) -> FleetStep {
        if self.tenant.is_none()
            && let Some(meta) = read(client, META, Meta::new(), first).await
        {
            self.tenant = meta.by_name(&self.name);
        }
        step(client, self.owner, first, |meta, cell| {
            self.decide(meta, cell)
        })
        .await
        .0
    }

    /// Step until the tenant is forgotten or refused, retrying an
    /// unavailable step for up to `patience`.
    pub async fn run<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        patience: Duration,
    ) -> FleetStep {
        let deadline = client.now() + patience;
        let mut last = FleetStep::Unavailable;
        for _ in 0..MAX_STEPS {
            last = self.step(client, first).await;
            match last {
                FleetStep::Stepped(_) => {}
                FleetStep::Unavailable => {
                    if client.now() >= deadline
                        || !client.pause(client.tunables.retry_backoff).await
                    {
                        return last;
                    }
                }
                _ => return last,
            }
        }
        last
    }
}

/// Meta, read from its floor to its tail (`parosctl tenant list`); `None`
/// when no server served it.
pub async fn load_meta<P: Providers>(client: &Client<P>, first: usize) -> Option<Meta> {
    read(client, META, Meta::new(), first).await
}

/// The cell control journal's fold, read from its floor to its tail.
pub async fn load_cell<P: Providers>(client: &Client<P>, first: usize) -> Option<Registry> {
    read(client, REGISTRY, Registry::new([]), first).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Meta and the cell control journal, folded in memory.
    struct World {
        meta: Meta,
        cell: Registry,
        seq: (u64, u64),
    }

    impl World {
        fn new() -> Self {
            Self {
                meta: Meta::new(),
                cell: Registry::new([]),
                seq: (0, 0),
            }
        }

        /// Write what `decide` names; `None` once it is done.
        fn step(
            &mut self,
            decide: impl Fn(&Meta, &Registry) -> Result<Next, FleetRefusal>,
        ) -> Result<Option<FleetAction>, FleetRefusal> {
            match decide(&self.meta, &self.cell)? {
                Next::Done(_) => Ok(None),
                Next::Write {
                    journal,
                    command,
                    action,
                } => {
                    let record = command.encode();
                    if journal == META {
                        self.meta.fold(self.seq.0, &record);
                        self.seq.0 += 1;
                    } else {
                        self.cell.fold(self.seq.1, &record);
                        self.seq.1 += 1;
                    }
                    Ok(Some(action))
                }
            }
        }

        fn run(
            &mut self,
            decide: impl Fn(&Meta, &Registry) -> Result<Next, FleetRefusal>,
        ) -> Result<Vec<FleetAction>, FleetRefusal> {
            let mut actions = Vec::new();
            for _ in 0..16 {
                match self.step(&decide)? {
                    Some(action) => actions.push(action),
                    None => return Ok(actions),
                }
            }
            panic!("an operation ends in a few steps");
        }
    }

    fn registered() -> World {
        let mut world = World::new();
        let init = CellRegistration::new(1, 5, 77);
        assert_eq!(
            world.run(|m, c| init.decide(m, c)),
            Ok(vec![
                FleetAction::RegisterCell,
                FleetAction::RegisterFleet,
                FleetAction::CellReady
            ])
        );
        world
    }

    #[test]
    fn init_registers_the_cell_on_both_sides_and_a_rerun_resumes_or_refuses() {
        let mut world = registered();
        assert_eq!(world.meta.fleet_id(), Some(77));
        assert_eq!(world.meta.ready_cell(), Some(5));
        // A re-run with another draw keeps the fleet recorded.
        let again = CellRegistration::new(1, 5, 99);
        assert_eq!(world.run(|m, c| again.decide(m, c)), Ok(vec![]));
        // Another cell id against this cell control journal is the wrong
        // place.
        let wrong = CellRegistration::new(1, 6, 77);
        assert_eq!(
            world.run(|m, c| wrong.decide(m, c)),
            Err(FleetRefusal::OtherCell)
        );
        // A crash after the first step resumes on the cell's half.
        let mut half = World::new();
        let init = CellRegistration::new(1, 5, 77);
        assert_eq!(
            half.step(|m, c| init.decide(m, c)),
            Ok(Some(FleetAction::RegisterCell))
        );
        let rerun = CellRegistration::new(1, 5, 123);
        assert_eq!(
            half.run(|m, c| rerun.decide(m, c)),
            Ok(vec![FleetAction::RegisterFleet, FleetAction::CellReady])
        );
        assert_eq!(half.meta.fleet_id(), Some(77));
    }

    #[test]
    fn a_tenant_creation_resumes_from_any_step() {
        let mut world = registered();
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300));
        assert_eq!(
            world.step(|m, c| create.decide(m, c)),
            Ok(Some(FleetAction::RegisterTenant))
        );
        // Another run under another draw resumes the same tenant.
        let rerun = TenantCreation::new(2, b"acme".to_vec(), TenantId(400));
        assert_eq!(
            world.run(|m, c| rerun.decide(m, c)),
            Ok(vec![FleetAction::HostTenant, FleetAction::TenantReady])
        );
        assert_eq!(
            world.meta.tenant(TenantId(300)).map(|t| t.state),
            Some(TenantState::Ready)
        );
        assert!(world.cell.tenant(TenantId(300)).is_some());
        // A resumed run whose saved context names another fleet is refused.
        let stale = TenantCreation::new(1, b"acme".to_vec(), TenantId(300)).resume(FleetContext {
            fleet_id: 78,
            cell_id: 5,
        });
        assert_eq!(
            world.run(|m, c| stale.decide(m, c)),
            Err(FleetRefusal::ContextChanged)
        );
    }

    #[test]
    fn a_removal_fences_a_creation_it_overtakes() {
        let mut world = registered();
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300));
        world.step(|m, c| create.decide(m, c)).expect("registered");
        // The creator decides to host, and stalls before writing.
        let late = create.decide(&world.meta, &world.cell);
        // A removal runs to the end meanwhile.
        let remove = TenantRemoval::new(2, b"acme".to_vec());
        let removal = TenantRemoval {
            tenant: Some(TenantId(300)),
            ..remove
        };
        assert_eq!(
            world.run(|m, c| removal.decide(m, c)),
            Ok(vec![
                FleetAction::RemoveTenant,
                FleetAction::UnhostTenant,
                FleetAction::ForgetTenant
            ])
        );
        // The creator's late host is refused by the fence: the cell never
        // hosts a tenant meta forgot.
        let Ok(Next::Write { command, .. }) = late else {
            panic!("the creator was about to host");
        };
        assert_eq!(
            world.cell.fold(world.seq.1, &command.encode()),
            RegistryEvent::Refused(RegistryRefusal::TenantGone {
                tenant: TenantId(300)
            })
        );
        assert!(world.cell.tenant(TenantId(300)).is_none());
        assert!(world.meta.tenant(TenantId(300)).is_none());
    }

    #[test]
    fn a_tenant_needs_a_ready_cell() {
        let mut world = World::new();
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300));
        assert_eq!(
            world.run(|m, c| create.decide(m, c)),
            Err(FleetRefusal::NoFleet)
        );
        let init = CellRegistration::new(1, 5, 77);
        world.step(|m, c| init.decide(m, c)).expect("registered");
        assert_eq!(
            world.run(|m, c| create.decide(m, c)),
            Err(FleetRefusal::NoReadyCell)
        );
    }
}
