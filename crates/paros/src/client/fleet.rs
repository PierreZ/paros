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
//!   cell; the cell hosts it (naming where its control journal runs, #210);
//!   its control journal describes it; meta marks it `READY`. A name already
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
use paros_core::{AcceptorConfig, JournalKey, TenantId};

use super::Client;
use super::checkpoint::{Applied, Checkpointable, Checkpointer, Folder, LoadOutcome, OpenOutcome};
use crate::system::{
    CellState, Directory, DirectoryEvent, DirectoryRefusal, FleetContext, META, METADATA_VERSION,
    Meta, MetaEvent, MetaRefusal, REGISTRY, Registry, RegistryEvent, RegistryRefusal,
    SystemCommand, TenantState,
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
    /// The tenant's control journal: `DescribeTenant` (#210).
    DescribeTenant,
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
    /// The tenant's control journal refused the step's entry (#210).
    Control(DirectoryRefusal),
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

/// Read `journal` from its floor to its tail into `state`: only a whole
/// fold (from position 0, or restored from the checkpoint at the floor)
/// comes back — `load` reports a fold a non-checkpoint floor left blind as
/// `Unhealed`, and a step never decides on one.
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
    Refused(FleetRefusal),
}

fn meta_verdict(event: &MetaEvent) -> Verdict {
    match event {
        MetaEvent::Refused(MetaRefusal::IdTaken { .. } | MetaRefusal::Reserved { .. }) => {
            Verdict::IdTaken
        }
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

fn control_verdict(event: &DirectoryEvent) -> Verdict {
    match event {
        DirectoryEvent::Refused(refusal) => {
            Verdict::Refused(FleetRefusal::Control(refusal.clone()))
        }
        _ => Verdict::Applied,
    }
}

/// What a step decides on: meta, the cell control journal, and — when the
/// operation needs it — the tenant's control journal (#210).
struct View<'a> {
    meta: &'a Meta,
    cell: &'a Registry,
    control: Option<&'a Directory>,
}

/// What one step saw and did.
struct StepReport {
    outcome: FleetStep,
    /// Meta as the step first read it.
    read: Option<Meta>,
    /// Meta after the step's own write, when it wrote meta.
    written: Option<Meta>,
}

impl StepReport {
    fn of(outcome: FleetStep, read: Option<Meta>) -> Self {
        Self {
            outcome,
            read,
            written: None,
        }
    }
}

/// Run one step: read meta and the cell control journal — and the tenant
/// control journal `wants` names, if any — decide with `decide`, and write
/// the entry it names to the journal it names: claimed as `owner`, folded to
/// its tail, and decided again on what that fold holds, so the entry is
/// judged against exactly the state it was decided on.
async fn step<P: Providers>(
    client: &Client<P>,
    owner: u64,
    first: usize,
    wants: impl Fn(&Meta, &Registry) -> Option<JournalKey>,
    decide: impl Fn(&View<'_>) -> Result<Next, FleetRefusal>,
) -> StepReport {
    let Some(meta) = read(client, META, Meta::new(), first).await else {
        return StepReport::of(FleetStep::Unavailable, None);
    };
    let Some(cell) = read(client, REGISTRY, Registry::new([]), first).await else {
        return StepReport::of(FleetStep::Unavailable, Some(meta));
    };
    let wanted = wants(&meta, &cell);
    let control = match wanted {
        Some(key) => match read(client, key, Directory::new([]), first).await {
            Some(directory) => Some(directory),
            // Not started yet where the step asked (its nodes fold the
            // hosting first): a later step reads it.
            None => return StepReport::of(FleetStep::Unavailable, Some(meta)),
        },
        None => None,
    };
    let view = View {
        meta: &meta,
        cell: &cell,
        control: control.as_ref(),
    };
    let journal = match decide(&view) {
        Err(refusal) => return StepReport::of(FleetStep::Refused(refusal), Some(meta)),
        Ok(Next::Done(context)) => return StepReport::of(FleetStep::Done(context), Some(meta)),
        Ok(Next::Write { journal, .. }) => journal,
    };
    let policy = client.tunables().checkpoint_policy();
    if journal == META {
        let mut writer = Checkpointer::new(META, owner, Meta::new(), policy);
        if !matches!(writer.open(client, first).await, OpenOutcome::Open { .. }) {
            return StepReport::of(FleetStep::Unavailable, Some(meta));
        }
        let next = decide(&View {
            meta: writer.state(),
            ..view
        });
        let outcome = written(&mut writer, client, first, journal, next, meta_verdict).await;
        StepReport {
            outcome,
            read: Some(meta.clone()),
            written: Some(writer.state().clone()),
        }
    } else if journal == REGISTRY {
        let mut writer = Checkpointer::new(REGISTRY, owner, Registry::new([]), policy);
        if !matches!(writer.open(client, first).await, OpenOutcome::Open { .. }) {
            return StepReport::of(FleetStep::Unavailable, Some(meta));
        }
        let next = decide(&View {
            cell: writer.state(),
            ..view
        });
        let outcome = written(&mut writer, client, first, journal, next, cell_verdict).await;
        StepReport::of(outcome, Some(meta.clone()))
    } else {
        let mut writer = Checkpointer::new(journal, owner, Directory::new([]), policy);
        if !matches!(writer.open(client, first).await, OpenOutcome::Open { .. }) {
            return StepReport::of(FleetStep::Unavailable, Some(meta));
        }
        let next = decide(&View {
            control: Some(writer.state()),
            ..view
        });
        let outcome = written(&mut writer, client, first, journal, next, control_verdict).await;
        StepReport::of(outcome, Some(meta.clone()))
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

    fn decide(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
        let (meta, cell) = (view.meta, view.cell);
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
        step(
            client,
            self.owner,
            first,
            |_, _| None,
            |view| self.decide(view),
        )
        .await
        .outcome
    }

    /// Step until the cell is registered or refused, retrying an
    /// unavailable step for up to `patience`.
    #[tracing::instrument(level = "debug", skip_all, fields(cell = self.cell_id))]
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
    /// Where the tenant's control journal runs (#210).
    control: AcceptorConfig,
    context: Option<FleetContext>,
    tenant: Option<TenantId>,
    adopted: bool,
}

impl TenantCreation {
    /// Create the tenant `name` under the id `candidate` (the caller's draw,
    /// from the user range), its control journal over `control` (the
    /// caller's placement, until the cell coordinator places it, #212),
    /// writing as client `owner`.
    #[must_use]
    pub fn new(owner: u64, name: Vec<u8>, candidate: TenantId, control: AcceptorConfig) -> Self {
        Self {
            owner,
            name,
            candidate,
            control,
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

    fn decide(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
        let (meta, cell) = (view.meta, view.cell);
        let fleet_id = meta.fleet_id().ok_or(FleetRefusal::NoFleet)?;
        let context = match self.context {
            Some(context) => {
                if context.fleet_id != fleet_id || meta.cell(context.cell_id).is_none() {
                    return Err(FleetRefusal::ContextChanged);
                }
                context
            }
            // A name already registered runs in its own cell; a new one is
            // assigned the ready cell.
            None => FleetContext {
                fleet_id,
                cell_id: match meta.by_name(&self.name).and_then(|t| meta.tenant(t)) {
                    Some(entry) => entry.cell_id,
                    None => meta.ready_cell().ok_or(FleetRefusal::NoReadyCell)?,
                },
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
                    control: self.control.clone(),
                },
            )),
            // Hosted: the tenant's control journal describes it before the
            // tenant is ready, so it is self-describing from its first
            // ready moment (#210).
            TenantState::Registering => match view.control.and_then(Directory::name) {
                None => Ok(write(
                    JournalKey::control(tenant),
                    FleetAction::DescribeTenant,
                    SystemCommand::DescribeTenant {
                        name: self.name.clone(),
                    },
                )),
                Some(name) if name == self.name.as_slice() => Ok(write(
                    META,
                    FleetAction::TenantReady,
                    SystemCommand::TenantReady { context, tenant },
                )),
                Some(_) => Err(FleetRefusal::Control(DirectoryRefusal::Described)),
            },
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
        let report = step(
            client,
            self.owner,
            first,
            |meta, cell| self.wants(meta, cell),
            |view| self.decide(view),
        )
        .await;
        if let Some(meta) = report.written.as_ref().or(report.read.as_ref()) {
            self.observe(meta);
        }
        report.outcome
    }

    /// The tenant control journal a step reads: the tenant's, once its cell
    /// hosts it and it is still registering (the describe step's input).
    fn wants(&self, meta: &Meta, cell: &Registry) -> Option<JournalKey> {
        let tenant = meta
            .by_name(&self.name)
            .filter(|t| self.tenant.is_none_or(|ours| ours == *t))?;
        let registering = meta
            .tenant(tenant)
            .is_some_and(|entry| entry.state == TenantState::Registering);
        (registering && cell.tenant(tenant).is_some()).then_some(JournalKey::control(tenant))
    }

    /// Learn what meta fixed: the tenant's id and the context.
    fn observe(&mut self, meta: &Meta) {
        let Some(tenant) = meta.by_name(&self.name) else {
            return;
        };
        let Some(entry) = meta.tenant(tenant) else {
            return;
        };
        // Only a tenant this creation may finish: registering or ready (a
        // rival's tenant being removed is not one).
        if self.tenant.is_none()
            && matches!(entry.state, TenantState::Registering | TenantState::Ready)
        {
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
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = self.candidate.0))]
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

    fn decide(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
        let (meta, cell) = (view.meta, view.cell);
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
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = self.tenant.map(|t| t.0)))]
    pub async fn step<P: Providers>(&mut self, client: &Client<P>, first: usize) -> FleetStep {
        let report = step(
            client,
            self.owner,
            first,
            |_, _| None,
            |view| self.decide(view),
        )
        .await;
        // The tenant this removal works on, from what the step read before
        // it wrote: a forget leaves nothing to learn it from afterwards.
        if self.tenant.is_none()
            && let Some(meta) = &report.read
        {
            self.tenant = meta.by_name(&self.name);
        }
        report.outcome
    }

    /// Step until the tenant is forgotten or refused, retrying an
    /// unavailable step for up to `patience`.
    #[tracing::instrument(level = "debug", skip_all)]
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
#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_meta<P: Providers>(client: &Client<P>, first: usize) -> Option<Meta> {
    read(client, META, Meta::new(), first).await
}

/// The cell control journal's fold, read from its floor to its tail.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_cell<P: Providers>(client: &Client<P>, first: usize) -> Option<Registry> {
    read(client, REGISTRY, Registry::new([]), first).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    /// An operation's decision, as the in-memory world drives it.
    trait Operation {
        fn wants(&self, _meta: &Meta, _cell: &Registry) -> Option<JournalKey> {
            None
        }
        fn decide_on(&self, view: &View<'_>) -> Result<Next, FleetRefusal>;
    }

    impl Operation for CellRegistration {
        fn decide_on(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
            self.decide(view)
        }
    }

    impl Operation for TenantCreation {
        fn wants(&self, meta: &Meta, cell: &Registry) -> Option<JournalKey> {
            TenantCreation::wants(self, meta, cell)
        }
        fn decide_on(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
            self.decide(view)
        }
    }

    impl Operation for TenantRemoval {
        fn decide_on(&self, view: &View<'_>) -> Result<Next, FleetRefusal> {
            self.decide(view)
        }
    }

    /// Meta, the cell control journal and the tenant control journals,
    /// folded in memory.
    struct World {
        meta: Meta,
        cell: Registry,
        controls: BTreeMap<JournalKey, Directory>,
        seq: BTreeMap<JournalKey, u64>,
    }

    impl World {
        fn new() -> Self {
            Self {
                meta: Meta::new(),
                cell: Registry::new([]),
                controls: BTreeMap::new(),
                seq: BTreeMap::new(),
            }
        }

        /// What `op` would write next, decided on the world as it stands.
        fn next(&self, op: &impl Operation) -> Result<Next, FleetRefusal> {
            let wanted = op.wants(&self.meta, &self.cell);
            let empty = Directory::new([]);
            let control = wanted.map(|key| self.controls.get(&key).unwrap_or(&empty));
            op.decide_on(&View {
                meta: &self.meta,
                cell: &self.cell,
                control,
            })
        }

        /// Fold `next`'s entry into the journal it names.
        fn apply(&mut self, journal: JournalKey, command: &SystemCommand) {
            let record = command.encode();
            let seq = self.seq.entry(journal).or_default();
            let at = *seq;
            *seq += 1;
            if journal == META {
                self.meta.fold(at, &record);
            } else if journal == REGISTRY {
                self.cell.fold(at, &record);
            } else {
                self.controls
                    .entry(journal)
                    .or_insert_with(|| Directory::new([]))
                    .fold(at, &record);
            }
        }

        /// Write what `op` names; `None` once it is done.
        fn step(&mut self, op: &impl Operation) -> Result<Option<FleetAction>, FleetRefusal> {
            match self.next(op)? {
                Next::Done(_) => Ok(None),
                Next::Write {
                    journal,
                    command,
                    action,
                } => {
                    self.apply(journal, &command);
                    Ok(Some(action))
                }
            }
        }

        fn run(&mut self, op: &impl Operation) -> Result<Vec<FleetAction>, FleetRefusal> {
            let mut actions = Vec::new();
            for _ in 0..16 {
                match self.step(op)? {
                    Some(action) => actions.push(action),
                    None => return Ok(actions),
                }
            }
            panic!("an operation ends in a few steps");
        }
    }

    fn control() -> AcceptorConfig {
        AcceptorConfig::new(
            vec![paros_core::NodeId(0)],
            paros_core::QuorumSystem::Majority,
        )
    }

    fn registered() -> World {
        let mut world = World::new();
        let init = CellRegistration::new(1, 5, 77);
        assert_eq!(
            world.run(&init),
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
        assert_eq!(world.run(&again), Ok(vec![]));
        // Another cell id against this cell control journal is the wrong
        // place.
        let wrong = CellRegistration::new(1, 6, 77);
        assert_eq!(world.run(&wrong), Err(FleetRefusal::OtherCell));
        // A crash after the first step resumes on the cell's half.
        let mut half = World::new();
        let init = CellRegistration::new(1, 5, 77);
        assert_eq!(half.step(&init), Ok(Some(FleetAction::RegisterCell)));
        let rerun = CellRegistration::new(1, 5, 123);
        assert_eq!(
            half.run(&rerun),
            Ok(vec![FleetAction::RegisterFleet, FleetAction::CellReady])
        );
        assert_eq!(half.meta.fleet_id(), Some(77));
    }

    #[test]
    fn a_tenant_creation_resumes_from_any_step() {
        let mut world = registered();
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300), control());
        assert_eq!(world.step(&create), Ok(Some(FleetAction::RegisterTenant)));
        // Another run under another draw resumes the same tenant.
        let rerun = TenantCreation::new(2, b"acme".to_vec(), TenantId(400), control());
        assert_eq!(
            world.run(&rerun),
            Ok(vec![
                FleetAction::HostTenant,
                FleetAction::DescribeTenant,
                FleetAction::TenantReady
            ])
        );
        assert_eq!(
            world.meta.tenant(TenantId(300)).map(|t| t.state),
            Some(TenantState::Ready)
        );
        assert!(world.cell.tenant(TenantId(300)).is_some());
        // A resumed run whose saved context names another fleet is refused.
        let stale = TenantCreation::new(1, b"acme".to_vec(), TenantId(300), control()).resume(
            FleetContext {
                fleet_id: 78,
                cell_id: 5,
            },
        );
        assert_eq!(world.run(&stale), Err(FleetRefusal::ContextChanged));
    }

    #[test]
    fn a_removal_fences_a_creation_it_overtakes() {
        let mut world = registered();
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300), control());
        world.step(&create).expect("registered");
        // The creator decides to host, and stalls before writing.
        let late = world.next(&create);
        // A removal runs to the end meanwhile.
        let remove = TenantRemoval::new(2, b"acme".to_vec());
        let removal = TenantRemoval {
            tenant: Some(TenantId(300)),
            ..remove
        };
        assert_eq!(
            world.run(&removal),
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
            world.cell.fold(
                world.seq.get(&REGISTRY).copied().unwrap_or(0),
                &command.encode()
            ),
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
        let create = TenantCreation::new(1, b"acme".to_vec(), TenantId(300), control());
        assert_eq!(world.run(&create), Err(FleetRefusal::NoFleet));
        let init = CellRegistration::new(1, 5, 77);
        world.step(&init).expect("registered");
        assert_eq!(world.run(&create), Err(FleetRefusal::NoReadyCell));
    }
}
