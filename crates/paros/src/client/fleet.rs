//! **Fleet operations** (#229, `docs/architecture.md` §3.7): `init`'s fleet
//! steps, and creating and removing a tenant, each an **idempotent state
//! machine** over two control journals — meta's ([`META`], the fleet's
//! directory) and the cell's ([`crate::machine::CELL_CONTROL`], the cell's
//! tenant list).
//!
//! An operation is a sequence of steps, each **one** write to one of the two
//! journals, and the next step is decided from what the journals hold, never
//! from what the operator remembers. So a crash at any step is answered by
//! running the same operation again: it reads where the journals stand and
//! resumes there (FDB's metacluster operations). [`FleetSession::init_step`],
//! [`FleetSession::create_step`] and [`FleetSession::remove_step`] take one
//! step; [`FleetSession::init`], [`FleetSession::create_tenant`] and
//! [`FleetSession::remove_tenant`] run steps until the operation ends.
//!
//! - **`init`** (steps 2 and 3 of §3.1): the cell records the fleet on its
//!   side (`JoinFleet`), meta records the fleet's id (`FormFleet`), the cell
//!   joins meta's directory `REGISTERING` (`AddCell`), and meta marks it
//!   `READY`. The cell's side goes first, against the order §3.1 lists the
//!   steps in, because it is the one place a re-run can learn the cell's id
//!   from: a formed seed no longer answers `Init`, and `Inspect` does not
//!   name its cell.
//! - **Creating a tenant**: meta registers it `REGISTERING` with its cell
//!   assignment (the one `READY` cell in M9), the cell hosts it
//!   (`HostTenant`), meta marks it `READY`. A name meta holds in
//!   `REGISTERING` — an earlier run's, whoever ran it — is resumed under its
//!   id; a name held `READY` ends the operation at once. Booking the
//!   tenant's footprint and writing its own control journal are #210 and
//!   #225.
//! - **Removing a tenant**: meta marks it `REMOVING`, the cell drops it,
//!   meta removes it. Any state may be removed.
//!
//! **Every step checks it still talks to the same fleet and the same cell**
//! (FDB's `MetaclusterOperationContext`): every entry names the fleet meta
//! formed, and both folds refuse another; before a cell step, the cell's own
//! registration must name meta's fleet and the cell the tenant is assigned to
//! ([`FleetRefusal::CellMismatch`]).
//!
//! **Who writes.** Meta is written by its coordinator — whoever claims it with
//! `SetLeader` (the session's `operator`) — and the cell's control journal by
//! the cell coordinator (`coordinator`, the cell's first coordinator in M9:
//! the cell coordinator of #225 is not built). Each journal has one writer
//! per generation, so two operators running at once fence each other: the
//! loser's write is refused, its run is [`Step::Interrupted`], and running it
//! again resumes from what the winner wrote. Meta is checkpointed as its
//! owner writes it ([`crate::client::checkpoint`], #227); the cell's journal
//! is never checkpointed here — its owner's fold is the registry's, over a
//! genesis pool only the deployment knows.
//!
//! Like the rest of the client it draws no randomness: a tenant id is the
//! caller's draw, and an id meta holds already is a refusal the caller
//! redraws on ([`FleetRefusal::IdTaken`]).

use moonpool_core::Providers;
use paros_core::{JournalKey, NodeId, TenantId};

use super::Client;
use super::checkpoint::{
    AppendOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome, OpenOutcome,
};
use super::outcome::ClaimOutcome;
use super::writer::WriterOutcome;
use crate::machine::CELL_CONTROL;
use crate::meta::{CellState, META, METADATA_VERSION, Meta, MetaCommand, MetaEntry, TenantState};
use crate::system::{FleetRegistration, Registry, SystemCommand};

/// One step a fleet operation wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// The cell recorded the fleet on its side.
    JoinFleet,
    /// Meta recorded the fleet's id.
    FormFleet,
    /// The cell joined meta's directory, `REGISTERING`.
    AddCell,
    /// Meta marked the cell `READY`.
    CellReady,
    /// Meta registered the tenant, `REGISTERING`.
    RegisterTenant,
    /// The cell hosts the tenant.
    HostTenant,
    /// Meta marked the tenant `READY`.
    TenantReady,
    /// Meta marked the tenant `REMOVING`.
    TenantRemoving,
    /// The cell dropped the tenant.
    DropTenant,
    /// Meta removed the tenant.
    RemoveTenant,
}

/// Why a fleet operation cannot go on: running it again changes nothing
/// until something else does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetRefusal {
    /// `init` was asked to form a fleet, or a cell, under the id `0`.
    NoFleetId,
    /// `init` found the cell's side unwritten and was not told the cell's
    /// id.
    CellIdUnknown,
    /// Meta holds no fleet, or no `READY` cell: run `init` first.
    NotInitialized,
    /// The cell's side of the registration names another fleet or another
    /// cell than meta, or than the tenant's assignment: this operation
    /// talks to the wrong cell.
    CellMismatch {
        /// What meta expects: `(fleet_id, cell_id)`.
        expected: (u64, u64),
        /// What the cell recorded, if anything.
        found: Option<FleetRegistration>,
    },
    /// Meta's entry for the cell is being removed.
    CellRemoving,
    /// The tenant id is reserved (below 256): draw another.
    ReservedId,
    /// The tenant id is held by another entry, or was removed: draw another.
    IdTaken,
    /// The name is held by a tenant in a state creation cannot resume.
    NameBusy {
        /// The tenant holding it.
        tenant: TenantId,
        /// Its state.
        state: TenantState,
    },
}

/// What stopped a step's write from landing: run the operation again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Interrupted {
    /// `journal` could not be claimed (see [`ClaimOutcome`]).
    NotClaimed {
        /// The journal.
        journal: JournalKey,
        /// The claim's outcome.
        outcome: ClaimOutcome,
    },
    /// `journal` was claimed but not folded to its tail (see
    /// [`LoadOutcome`]).
    Behind {
        /// The journal.
        journal: JournalKey,
        /// The fold's outcome.
        outcome: LoadOutcome,
    },
    /// A step's write is not known written (see [`WriterOutcome`]): refused,
    /// fenced by another writer, unanswered or ambiguous.
    NotWritten {
        /// The journal.
        journal: JournalKey,
        /// The write's verdict.
        outcome: WriterOutcome,
    },
}

/// What one step came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step<T> {
    /// One step was written; take the next.
    Advanced(Stage),
    /// The operation is over, with the step that ended it, if any.
    Done {
        /// The operation's result.
        result: T,
        /// The step written on the way, `None` when the journals already
        /// held the end.
        last: Option<Stage>,
    },
    /// The operation cannot go on.
    Refused(FleetRefusal),
    /// A write did not land: run the operation again, it resumes.
    Interrupted(Interrupted),
}

/// What running an operation to its end came to: the outcome (never
/// [`Step::Advanced`]) and every step written on the way, in order. An
/// operation that wrote nothing found its end already in the journals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run<T> {
    /// How it ended.
    pub outcome: Step<T>,
    /// The steps written, in order.
    pub steps: Vec<Stage>,
}

/// The most steps one run takes: the longest operation is four writes, and
/// every step is decided afresh, so a run that needs more is going round.
const MAX_STEPS: usize = 8;

/// An operator's handle on the two control journals a fleet operation
/// writes: meta, as `operator`, and the cell's, as its coordinator.
#[derive(Clone, Debug)]
pub struct FleetSession {
    meta: Checkpointer<Meta>,
    cell: Checkpointer<Registry>,
    meta_open: bool,
    cell_open: bool,
}

impl FleetSession {
    /// A session writing meta as client `operator` (checkpointing it under
    /// `policy`) and the cell's control journal as the cell coordinator
    /// `coordinator`, folding the cell's journal into `cell` — the empty
    /// registry over the deployment's genesis pool.
    #[must_use]
    pub fn new(
        operator: u64,
        coordinator: NodeId,
        cell: Registry,
        policy: CheckpointPolicy,
    ) -> Self {
        Self {
            meta: Checkpointer::new(META, operator, Meta::default(), policy),
            cell: Checkpointer::new(CELL_CONTROL, coordinator.0, cell, policy),
            meta_open: false,
            cell_open: false,
        }
    }

    /// Meta as this session last folded it.
    #[must_use]
    pub fn meta(&self) -> &Meta {
        self.meta.state()
    }

    /// The cell's control journal as this session last folded it.
    #[must_use]
    pub fn cell(&self) -> &Registry {
        self.cell.state()
    }

    /// One step of `init`'s fleet half. The cell's side is written first
    /// (`JoinFleet`, naming `cell_id` and the fleet `fleet_id`), so a re-run
    /// learns both ids from the cell's own journal: `cell_id` may then be
    /// `None`, and an id the cell recorded is kept over the ones given — a
    /// re-run never mints a second fleet. Then meta records the fleet, adds
    /// the cell and marks it `READY`. Ends with `(fleet_id, cell_id)`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn init_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        cell_id: Option<u64>,
        fleet_id: u64,
    ) -> Step<(u64, u64)> {
        if let Err(stop) = self.open_cell(client, first).await {
            return Step::Interrupted(stop);
        }
        let joined = match self.cell.state().fleet() {
            None => {
                let Some(cell_id) = cell_id else {
                    return Step::Refused(FleetRefusal::CellIdUnknown);
                };
                if fleet_id == 0 || cell_id == 0 {
                    return Step::Refused(FleetRefusal::NoFleetId);
                }
                let join = SystemCommand::JoinFleet {
                    fleet_id,
                    cell_id,
                    version: METADATA_VERSION,
                };
                return self
                    .write_cell(client, first, &join, Stage::JoinFleet)
                    .await;
            }
            Some(found) if cell_id.is_some_and(|c| c != found.cell_id) => {
                return Step::Refused(FleetRefusal::CellMismatch {
                    expected: (found.fleet_id, cell_id.unwrap_or(0)),
                    found: Some(found),
                });
            }
            Some(found) => found,
        };
        let (fleet, cell_id) = (joined.fleet_id, joined.cell_id);
        if let Err(stop) = self.open_meta(client, first).await {
            return Step::Interrupted(stop);
        }
        match self.meta.state().fleet() {
            None => {
                return self
                    .write_meta(
                        client,
                        first,
                        fleet,
                        MetaCommand::FormFleet,
                        Stage::FormFleet,
                    )
                    .await;
            }
            // Meta belongs to another fleet than the one this cell joined.
            Some(other) if other != fleet => {
                return Step::Refused(FleetRefusal::CellMismatch {
                    expected: (other, cell_id),
                    found: Some(joined),
                });
            }
            Some(_) => {}
        }
        let Some(cell) = self.meta.state().cell(cell_id) else {
            return self
                .write_meta(
                    client,
                    first,
                    fleet,
                    MetaCommand::AddCell { cell_id },
                    Stage::AddCell,
                )
                .await;
        };
        match cell.state {
            CellState::Ready => Step::Done {
                result: (fleet, cell_id),
                last: None,
            },
            CellState::Removing => Step::Refused(FleetRefusal::CellRemoving),
            CellState::Registering | CellState::Restoring => {
                let ready = MetaCommand::MarkCell {
                    cell_id,
                    state: CellState::Ready,
                };
                self.finish_meta(
                    client,
                    first,
                    fleet,
                    ready,
                    Stage::CellReady,
                    (fleet, cell_id),
                )
                .await
            }
        }
    }

    /// One step of creating the tenant `name`, under the id `draw` unless
    /// meta holds the name in `REGISTERING` already (whose id is resumed).
    /// Ends with the tenant's id once meta holds it `READY`.
    #[tracing::instrument(level = "debug", skip_all, fields(tenant = draw.0))]
    pub async fn create_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        draw: TenantId,
    ) -> Step<TenantId> {
        if let Err(stop) = self.open_meta(client, first).await {
            return Step::Interrupted(stop);
        }
        let meta = self.meta.state();
        let Some(fleet) = meta.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some((tenant, entry)) = meta.named(name) else {
            if !draw.is_user() {
                return Step::Refused(FleetRefusal::ReservedId);
            }
            if meta.tenant(draw).is_some() || meta.is_removed(draw) {
                return Step::Refused(FleetRefusal::IdTaken);
            }
            let Some((cell_id, _)) = meta.cells().find(|(_, c)| c.state == CellState::Ready) else {
                return Step::Refused(FleetRefusal::NotInitialized);
            };
            let register = MetaCommand::RegisterTenant {
                tenant: draw,
                name: name.to_vec(),
                cell_id,
            };
            return self
                .write_meta(client, first, fleet, register, Stage::RegisterTenant)
                .await;
        };
        let cell_id = match entry.state {
            TenantState::Ready => {
                return Step::Done {
                    result: tenant,
                    last: None,
                };
            }
            TenantState::Registering => entry.cell_id,
            state => return Step::Refused(FleetRefusal::NameBusy { tenant, state }),
        };
        if let Err(stop) = self.cell_of(client, first, fleet, cell_id).await {
            return stop;
        }
        if !self.cell.state().hosts(tenant) {
            let host = SystemCommand::HostTenant {
                fleet_id: fleet,
                tenant,
            };
            return self
                .write_cell(client, first, &host, Stage::HostTenant)
                .await;
        }
        let ready = MetaCommand::MarkTenant {
            tenant,
            state: TenantState::Ready,
        };
        self.finish_meta(client, first, fleet, ready, Stage::TenantReady, tenant)
            .await
    }

    /// One step of removing the tenant `name`. Ends with its id once meta
    /// removed it, or with `None` when meta holds no tenant by that name
    /// (never created, or removed already).
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
    ) -> Step<Option<TenantId>> {
        if let Err(stop) = self.open_meta(client, first).await {
            return Step::Interrupted(stop);
        }
        let meta = self.meta.state();
        let Some(fleet) = meta.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some((tenant, entry)) = meta.named(name) else {
            return Step::Done {
                result: None,
                last: None,
            };
        };
        let (state, cell_id) = (entry.state, entry.cell_id);
        if state != TenantState::Removing {
            let removing = MetaCommand::MarkTenant {
                tenant,
                state: TenantState::Removing,
            };
            return self
                .write_meta(client, first, fleet, removing, Stage::TenantRemoving)
                .await;
        }
        if let Err(stop) = self.cell_of(client, first, fleet, cell_id).await {
            return stop;
        }
        if self.cell.state().hosts(tenant) {
            let drop = SystemCommand::DropTenant {
                fleet_id: fleet,
                tenant,
            };
            return self
                .write_cell(client, first, &drop, Stage::DropTenant)
                .await;
        }
        let remove = MetaCommand::RemoveTenant { tenant };
        self.finish_meta(
            client,
            first,
            fleet,
            remove,
            Stage::RemoveTenant,
            Some(tenant),
        )
        .await
    }

    /// Run `init`'s fleet half to its end (see [`FleetSession::init_step`]).
    pub async fn init<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        cell_id: Option<u64>,
        fleet_id: u64,
    ) -> Run<(u64, u64)> {
        let mut steps = Vec::new();
        for _ in 0..MAX_STEPS {
            let step = self.init_step(client, first, cell_id, fleet_id).await;
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Create the tenant `name` to its end (see
    /// [`FleetSession::create_step`]).
    pub async fn create_tenant<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        draw: TenantId,
    ) -> Run<TenantId> {
        let mut steps = Vec::new();
        for _ in 0..MAX_STEPS {
            let step = self.create_step(client, first, name, draw).await;
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Remove the tenant `name` to its end (see
    /// [`FleetSession::remove_step`]).
    pub async fn remove_tenant<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
    ) -> Run<Option<TenantId>> {
        let mut steps = Vec::new();
        for _ in 0..MAX_STEPS {
            let step = self.remove_step(client, first, name).await;
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Claim meta and fold it to its tail, unless this session holds it.
    async fn open_meta<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        if !self.meta_open {
            open(&mut self.meta, client, first).await?;
            self.meta_open = true;
        }
        Ok(())
    }

    /// Claim the cell's control journal and fold it to its tail, unless
    /// this session holds it.
    async fn open_cell<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        if !self.cell_open {
            open(&mut self.cell, client, first).await?;
            self.cell_open = true;
        }
        Ok(())
    }

    /// Open the cell's journal and check it is cell `cell_id` of `fleet`.
    async fn cell_of<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        cell_id: u64,
    ) -> Result<(), Step<T>> {
        self.open_cell(client, first)
            .await
            .map_err(Step::Interrupted)?;
        let found = self.cell.state().fleet();
        if found.is_none_or(|f| (f.fleet_id, f.cell_id) != (fleet, cell_id)) {
            return Err(Step::Refused(FleetRefusal::CellMismatch {
                expected: (fleet, cell_id),
                found,
            }));
        }
        Ok(())
    }

    /// Write one meta entry as its owner, and checkpoint meta when its
    /// policy finds it due.
    async fn write_meta<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: MetaCommand,
        stage: Stage,
    ) -> Step<T> {
        match self.append_meta(client, first, fleet, command).await {
            Ok(()) => Step::Advanced(stage),
            Err(stop) => Step::Interrupted(stop),
        }
    }

    /// [`FleetSession::write_meta`] for the step that ends the operation.
    async fn finish_meta<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: MetaCommand,
        stage: Stage,
        result: T,
    ) -> Step<T> {
        match self.append_meta(client, first, fleet, command).await {
            Ok(()) => Step::Done {
                result,
                last: Some(stage),
            },
            Err(stop) => Step::Interrupted(stop),
        }
    }

    async fn append_meta<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: MetaCommand,
    ) -> Result<(), Interrupted> {
        let record = MetaEntry::new(fleet, command).encode();
        if let Err(stop) = append(&mut self.meta, client, first, record).await {
            self.meta_open = false;
            return Err(stop);
        }
        if self.meta.due(client.now()) {
            // A checkpoint that does not land leaves the owner's belief
            // unsure: the next step opens meta afresh.
            let checkpointed = self.meta.checkpoint(client, first).await;
            if !matches!(
                checkpointed,
                super::checkpoint::CheckpointOutcome::Checkpointed { .. }
            ) {
                self.meta_open = false;
            }
        }
        Ok(())
    }

    async fn write_cell<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        command: &SystemCommand,
        stage: Stage,
    ) -> Step<T> {
        match append(&mut self.cell, client, first, command.encode()).await {
            Ok(()) => Step::Advanced(stage),
            Err(stop) => {
                self.cell_open = false;
                Step::Interrupted(stop)
            }
        }
    }
}

/// Fold one step into a run: `None` to take the next, or the run's end.
fn settle<T>(step: Step<T>, steps: &mut Vec<Stage>) -> Option<Run<T>> {
    match step {
        Step::Advanced(stage) => {
            steps.push(stage);
            None
        }
        Step::Done { result, last } => {
            steps.extend(last);
            Some(Run {
                outcome: Step::Done {
                    result,
                    last: steps.last().copied(),
                },
                steps: std::mem::take(steps),
            })
        }
        outcome => Some(Run {
            outcome,
            steps: std::mem::take(steps),
        }),
    }
}

/// A run that took [`MAX_STEPS`] steps without ending: its last write is
/// reported as not known to have settled anything.
fn going_round<T>(steps: Vec<Stage>) -> Run<T> {
    Run {
        outcome: Step::Interrupted(Interrupted::NotWritten {
            journal: META,
            outcome: WriterOutcome::Ambiguous,
        }),
        steps,
    }
}

/// Claim `owner`'s journal and fold it to its tail.
async fn open<P: Providers, S: crate::client::checkpoint::Checkpointable>(
    owner: &mut Checkpointer<S>,
    client: &Client<P>,
    first: usize,
) -> Result<(), Interrupted> {
    let journal = owner.writer().journal();
    match owner.open(client, first).await {
        OpenOutcome::Open { .. } => Ok(()),
        OpenOutcome::NotClaimed(outcome) => Err(Interrupted::NotClaimed { journal, outcome }),
        OpenOutcome::Behind(outcome) => Err(Interrupted::Behind { journal, outcome }),
    }
}

/// Append `record` as `owner` and make sure the fold holds it: a write the
/// journal took at another position than the fold's next (a resolved
/// ambiguity) is folded by reading up to it.
async fn append<P: Providers, S: crate::client::checkpoint::Checkpointable>(
    owner: &mut Checkpointer<S>,
    client: &Client<P>,
    first: usize,
    record: Vec<u8>,
) -> Result<(), Interrupted> {
    let journal = owner.writer().journal();
    match owner.append(client, record, first).await {
        AppendOutcome::Written(WriterOutcome::Written { .. }) => {}
        AppendOutcome::Written(outcome) => {
            return Err(Interrupted::NotWritten { journal, outcome });
        }
        // A meta or system entry is protobuf, which never starts with the
        // checkpoint magic's zero byte.
        AppendOutcome::ReservedPrefix => {
            return Err(Interrupted::NotWritten {
                journal,
                outcome: WriterOutcome::NotOwner,
            });
        }
    }
    let tail = owner.writer().next_seq();
    if owner.folder().next_seq() < tail {
        match owner.load(client, first, tail).await {
            LoadOutcome::Loaded { up_to, .. } if up_to >= tail => {}
            outcome => return Err(Interrupted::Behind { journal, outcome }),
        }
    }
    Ok(())
}

/// Read meta to its tail, as a reader that owns nothing (`parosctl tenant
/// list`).
///
/// # Errors
///
/// The fold did not reach the tail (see [`LoadOutcome`]).
pub async fn read_meta<P: Providers>(
    client: &Client<P>,
    first: usize,
) -> Result<Meta, LoadOutcome> {
    let mut folder = Folder::new(Meta::default());
    match super::checkpoint::load(&mut folder, META, client, first, 0).await {
        LoadOutcome::Loaded { .. } => Ok(folder.state().clone()),
        outcome => Err(outcome),
    }
}
