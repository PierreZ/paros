//! **Fleet operations** (#229, `docs/architecture.md` §3.1, §3.7): `init`'s
//! fleet steps, and creating and removing a tenant, each an **idempotent
//! state machine** over two control journals — meta's (the fleet's
//! directory) and the cell tenant's (the cell's tenant list). No frame is
//! fixed (§3.8): the caller hands the session both, read from the cell plan
//! or from any machine's `Inspect` ([`FleetFrames`]).
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
//! - **`init`** (steps 2 and 3 of §3.1): meta records the fleet's id and
//!   itself (`FormFleet`: meta is the fleet's first tenant, `internal` and
//!   movable), the cell joins meta's directory `REGISTERING` with its cell
//!   tenant (`AddCell`: `internal` and pinned), the cell records the fleet on
//!   its side (`JoinFleet`), and meta marks the cell `READY`.
//! - **Creating a tenant**: a `users` tenant only — the tenant API cannot
//!   create an `internal` one. Meta registers it `REGISTERING` with its
//!   frame, its cell assignment (the one `READY` cell in M9) and its
//!   placement, the cell hosts it (`HostTenant`), meta marks it `READY`. A
//!   name meta holds in `REGISTERING` — an earlier run's, whoever ran it — is
//!   resumed under its id; a name held `READY` ends the operation at once.
//!   Booking the tenant's footprint and writing its own control journal are
//!   #210 and #225.
//! - **Removing a tenant**: meta marks it `REMOVING`, the cell drops it (a
//!   tombstone, written whether the cell hosted it or not), meta removes it.
//!
//! **Every step checks it still talks to the same fleet and the same cell**
//! (FDB's `MetaclusterOperationContext`): every entry names the fleet meta
//! formed, and both folds refuse another; before a cell step, the cell's own
//! registration must name meta's fleet and the cell the tenant is assigned to
//! ([`FleetRefusal::CellMismatch`]).
//!
//! **A cell step decided from a stale meta fold is judged by the cell.** A
//! step reads meta, then writes the cell; between the two another operator
//! may remove the tenant from meta (fencing this one's meta, not its claim
//! of the cell). Such a removal always tombstones the tenant on the cell
//! first, and the cell refuses to host a tombstoned tenant at apply, so the
//! stale `HostTenant` changes nothing ([`FleetRefusal::Removed`]).
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
//! Like the rest of the client it draws no randomness: tenant frames are the
//! caller's draws, and [`FleetSession::create_tenant`] moves to the next one
//! when meta holds a draw already.

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalKey, NodeId, TenantId};

use super::Client;
use super::checkpoint::{
    AppendOutcome, CheckpointOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome,
    OpenOutcome,
};
use super::outcome::ClaimOutcome;
use super::writer::{Writer, WriterOutcome};
use crate::meta::{
    CellState, METADATA_VERSION, Meta, MetaCommand, MetaEntry, Placement, TenantState,
};
use crate::system::{FleetRegistration, Registry, SystemCommand};

/// The frames a fleet operation works on: no frame is fixed (§3.8), so the
/// caller learns them from the cell plan or from any machine's `Inspect`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetFrames {
    /// The cell's id.
    pub cell_id: u64,
    /// The cell tenant's control journal.
    pub cell: JournalKey,
    /// Meta's control journal.
    pub meta: JournalKey,
}

/// One step a fleet operation wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Meta recorded the fleet's id, and itself.
    FormFleet,
    /// The cell joined meta's directory, `REGISTERING`, with its cell
    /// tenant.
    AddCell,
    /// The cell recorded the fleet on its side.
    JoinFleet,
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
    /// `init` was given an unset fleet id, cell id or frame.
    Unset,
    /// Meta holds no fleet, or no `READY` cell: run `init` first.
    NotInitialized,
    /// The cell's side of the registration names another fleet or another
    /// cell than meta, or than the tenant's assignment, or meta names
    /// another fleet than the cell: this operation talks to the wrong cell.
    CellMismatch {
        /// What the operation expected: `(fleet_id, cell_id)`.
        expected: (u64, u64),
        /// What the cell recorded, if anything.
        found: Option<FleetRegistration>,
    },
    /// Meta's entry for the cell is being removed.
    CellRemoving,
    /// Every tenant frame drawn is held by an entry or was removed: draw
    /// more.
    IdTaken,
    /// The name is held by another creation's tenant, in any state: a
    /// tenant is created once, and only a run carrying its frame resumes it.
    NameTaken {
        /// The tenant holding it.
        tenant: TenantId,
        /// Its state.
        state: TenantState,
    },
    /// The tenant being created was removed meanwhile: a removal overtook
    /// the creation (meta holds it `REMOVING`, or the cell dropped it, and
    /// never hosts it again).
    Removed {
        /// The tenant.
        tenant: TenantId,
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
    /// The run took its step budget without ending: other operators keep
    /// moving the journals under it.
    GoingRound {
        /// The steps it wrote.
        steps: usize,
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
    frames: FleetFrames,
    meta: Checkpointer<Meta>,
    cell: Checkpointer<Registry>,
    meta_open: bool,
    cell_open: bool,
    /// The first checkpoint a fold of this session found unequal to its own
    /// state, with its journal (see [`LoadOutcome::Loaded`]).
    diverged: Option<(JournalKey, u64)>,
}

impl FleetSession {
    /// A session over `frames`, writing meta as client `operator`
    /// (checkpointing it under `policy`) and the cell's control journal as
    /// the cell coordinator `coordinator`, folding the cell's journal into
    /// `cell` — the empty registry over the deployment's genesis pool.
    #[must_use]
    pub fn new(
        frames: FleetFrames,
        operator: u64,
        coordinator: NodeId,
        cell: Registry,
        policy: CheckpointPolicy,
    ) -> Self {
        Self {
            frames,
            meta: Checkpointer::new(frames.meta, operator, Meta::default(), policy),
            cell: Checkpointer::new(frames.cell, coordinator.0, cell, policy),
            meta_open: false,
            cell_open: false,
            diverged: None,
        }
    }

    /// The first checkpoint any fold of this session — meta's or the cell's,
    /// opening or catching up after a write — verified and found **unequal**
    /// to its own state, with its journal: an owner wrote a state that is not
    /// the fold of its journal (`None` while every one matched).
    #[must_use]
    pub fn diverged(&self) -> Option<(JournalKey, u64)> {
        self.diverged
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

    /// The writers of meta and of the cell's control journal: the
    /// generation each owns and the position it writes next.
    #[must_use]
    pub fn writers(&self) -> (&Writer, &Writer) {
        (self.meta.writer(), self.cell.writer())
    }

    /// Whether this session holds both journals, claimed and folded.
    #[must_use]
    pub fn holds_both(&self) -> bool {
        self.meta_open && self.cell_open
    }

    /// One step of `init`'s fleet half. The fleet is formed under `fleet_id`
    /// unless meta or the cell recorded one already, which is then kept — a
    /// re-run never mints a second fleet. Ends with the fleet's id once meta
    /// holds the cell `READY` and the cell names the fleet.
    #[tracing::instrument(level = "trace", skip_all, fields(cell = self.frames.cell_id))]
    pub async fn init_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet_id: u64,
    ) -> Step<u64> {
        let FleetFrames {
            cell_id,
            cell: control,
            meta: meta_frame,
        } = self.frames;
        if cell_id == 0 || !control.is_set() || !meta_frame.is_set() {
            return Step::Refused(FleetRefusal::Unset);
        }
        if let Err(stop) = self.open_meta(client, first).await {
            return Step::Interrupted(stop);
        }
        let Some(fleet) = self.meta.state().fleet() else {
            // The cell may have joined a fleet meta lost (a recovery, #231):
            // its id is kept.
            if let Err(stop) = self.open_cell(client, first).await {
                return Step::Interrupted(stop);
            }
            let fleet = self
                .cell
                .state()
                .fleet()
                .map_or(fleet_id, |joined| joined.fleet_id);
            if fleet == 0 {
                return Step::Refused(FleetRefusal::Unset);
            }
            let form = MetaCommand::FormFleet { meta: meta_frame };
            return self
                .write_meta(client, first, fleet, form, Stage::FormFleet)
                .await;
        };
        let Some(cell) = self.meta.state().cell(cell_id) else {
            // The cell tenant is meta's already, under another cell: this
            // operation names the wrong cell (meta would refuse the entry).
            if self.meta.state().tenant(control.tenant).is_some() {
                return Step::Refused(FleetRefusal::CellMismatch {
                    expected: (fleet, cell_id),
                    found: None,
                });
            }
            let add = MetaCommand::AddCell { cell_id, control };
            return self
                .write_meta(client, first, fleet, add, Stage::AddCell)
                .await;
        };
        if let Err(stop) = self.open_cell(client, first).await {
            return Step::Interrupted(stop);
        }
        match self.cell.state().fleet() {
            None => {
                let join = SystemCommand::JoinFleet {
                    fleet_id: fleet,
                    cell_id,
                    version: METADATA_VERSION,
                };
                return self
                    .write_cell(client, first, &join, Stage::JoinFleet)
                    .await;
            }
            Some(found) if (found.fleet_id, found.cell_id) != (fleet, cell_id) => {
                return Step::Refused(FleetRefusal::CellMismatch {
                    expected: (fleet, cell_id),
                    found: Some(found),
                });
            }
            Some(_) => {}
        }
        match cell.state {
            CellState::Ready => Step::Done {
                result: fleet,
                last: None,
            },
            CellState::Removing => Step::Refused(FleetRefusal::CellRemoving),
            CellState::Registering | CellState::Restoring => {
                let ready = MetaCommand::MarkCell {
                    cell_id,
                    state: CellState::Ready,
                };
                self.finish_meta(client, first, fleet, ready, Stage::CellReady, fleet)
                    .await
            }
        }
    }

    /// One step of creating the `users` tenant `name` with `placement`
    /// under the frame `draw` (its id and its control journal's). The frame
    /// names the creation: when meta holds `name` under `draw`, the step
    /// resumes it; under any other frame, in any state, it is refused
    /// ([`FleetRefusal::NameTaken`]), since a tenant is created once
    /// (`docs/architecture.md` §3.7). Ends with the tenant's id once meta
    /// holds it `READY`.
    #[tracing::instrument(level = "trace", skip_all, fields(tenant = draw.tenant.0))]
    pub async fn create_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        placement: Placement,
        draw: JournalKey,
    ) -> Step<TenantId> {
        if let Err(stop) = self.open_meta(client, first).await {
            return Step::Interrupted(stop);
        }
        let meta = self.meta.state();
        let Some(fleet) = meta.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some((tenant, entry)) = meta.named(name) else {
            if !draw.is_set() {
                return Step::Refused(FleetRefusal::Unset);
            }
            if meta.tenant(draw.tenant).is_some() || meta.is_removed(draw.tenant) {
                return Step::Refused(FleetRefusal::IdTaken);
            }
            let Some((cell_id, _)) = meta.cells().find(|(_, c)| c.state == CellState::Ready) else {
                return Step::Refused(FleetRefusal::NotInitialized);
            };
            let register = MetaCommand::RegisterTenant {
                control: draw,
                name: name.to_vec(),
                cell_id,
                placement,
            };
            return self
                .write_meta(client, first, fleet, register, Stage::RegisterTenant)
                .await;
        };
        if tenant != draw.tenant || entry.control != draw.journal {
            return Step::Refused(FleetRefusal::NameTaken {
                tenant,
                state: entry.state,
            });
        }
        let cell_id = match entry.state {
            TenantState::Ready => {
                return Step::Done {
                    result: tenant,
                    last: None,
                };
            }
            TenantState::Registering => entry.cell_id,
            // This creation's own tenant, which a removal overtook.
            TenantState::Removing => return Step::Refused(FleetRefusal::Removed { tenant }),
            state => return Step::Refused(FleetRefusal::NameTaken { tenant, state }),
        };
        if let Err(stop) = self.cell_of(client, first, fleet, cell_id).await {
            return stop;
        }
        if self.cell.state().dropped(tenant) {
            // Removed under this operation: its meta fold is stale.
            self.meta_open = false;
            return Step::Refused(FleetRefusal::Removed { tenant });
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

    /// One step of removing the `users` tenant `name`. Ends with its id once
    /// meta removed it, or with `None` when meta holds no tenant by that
    /// name (never created, or removed already).
    #[tracing::instrument(level = "trace", skip_all)]
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
        // The tombstone goes down whether the cell hosts the tenant or not:
        // a creation still running from a stale meta can then never host it.
        if !self.cell.state().dropped(tenant) {
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

    /// Run `init`'s fleet half to its end (see [`FleetSession::init_step`]),
    /// taking an interrupted step again for up to `patience` (a cell still
    /// electing its leaders, an operator racing this one).
    #[tracing::instrument(level = "debug", skip_all, fields(cell = self.frames.cell_id))]
    pub async fn init<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet_id: u64,
        patience: Duration,
    ) -> Run<u64> {
        let deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.init_step(client, first, fleet_id).await;
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Create the `users` tenant `name` to its end (see
    /// [`FleetSession::create_step`]), under the first of `draws` meta does
    /// not hold: a draw meta holds already is skipped for the next one. An
    /// interrupted step is taken again for up to `patience`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn create_tenant<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        placement: Placement,
        draws: impl IntoIterator<Item = JournalKey>,
        patience: Duration,
    ) -> Run<TenantId> {
        let deadline = client.now() + patience;
        let mut draws = draws.into_iter();
        let mut draw = draws.next().unwrap_or(JournalKey::UNSET);
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.create_step(client, first, name, placement, draw).await;
            if step == Step::Refused(FleetRefusal::IdTaken)
                && let Some(next) = draws.next()
            {
                draw = next;
                continue;
            }
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Remove the `users` tenant `name` to its end (see
    /// [`FleetSession::remove_step`]), taking an interrupted step again for
    /// up to `patience`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_tenant<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        patience: Duration,
    ) -> Run<Option<TenantId>> {
        let deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.remove_step(client, first, name).await;
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Claim and fold both journals — the cell's, then meta — unless this
    /// session holds them already: for a caller that reads the two folds
    /// side by side (a consistency check).
    ///
    /// # Errors
    ///
    /// A claim or a fold that did not reach its tail.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn open<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        self.open_cell(client, first).await?;
        self.open_meta(client, first).await
    }

    /// Claim meta and fold it to its tail, unless this session holds it.
    async fn open_meta<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        if !self.meta_open {
            let diverged = open(&mut self.meta, client, first).await?;
            self.note_diverged(self.frames.meta, diverged);
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
            let diverged = open(&mut self.cell, client, first).await?;
            self.note_diverged(self.frames.cell, diverged);
            self.cell_open = true;
        }
        Ok(())
    }

    /// Keep the first divergence a fold of `journal` reported.
    fn note_diverged(&mut self, journal: JournalKey, diverged: Option<u64>) {
        if self.diverged.is_none() {
            self.diverged = diverged.map(|seq| (journal, seq));
        }
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
        match append(&mut self.meta, client, first, record).await {
            Ok(diverged) => self.note_diverged(self.frames.meta, diverged),
            Err(stop) => {
                self.meta_open = false;
                return Err(stop);
            }
        }
        if self.meta.due(client.now()) {
            // A checkpoint that does not land leaves the owner's belief
            // unsure: the next step opens meta afresh.
            let checkpointed = self.meta.checkpoint(client, first).await;
            if !matches!(checkpointed, CheckpointOutcome::Checkpointed { .. }) {
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
            Ok(diverged) => {
                self.note_diverged(self.frames.cell, diverged);
                Step::Advanced(stage)
            }
            Err(stop) => {
                self.cell_open = false;
                Step::Interrupted(stop)
            }
        }
    }
}

/// Whether a run takes `step` again: it was interrupted (a claim no server
/// decided, a leader not ready yet, a write fenced by another operator) and
/// the run's patience allows one more `retry_backoff`. The step is decided
/// afresh from the journals, so a retry is always a resumption.
async fn retry<P: Providers, T>(client: &Client<P>, step: &Step<T>, deadline: Duration) -> bool {
    matches!(step, Step::Interrupted(_))
        && client.now() < deadline
        && client.pause(client.tunables().retry_backoff).await
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

/// A run that took [`MAX_STEPS`] steps without ending.
fn going_round<T>(steps: Vec<Stage>) -> Run<T> {
    Run {
        outcome: Step::Interrupted(Interrupted::GoingRound { steps: steps.len() }),
        steps,
    }
}

/// Claim `owner`'s journal and fold it to its tail: the first checkpoint
/// the fold found diverged, if any.
async fn open<P: Providers, S: crate::client::checkpoint::Checkpointable>(
    owner: &mut Checkpointer<S>,
    client: &Client<P>,
    first: usize,
) -> Result<Option<u64>, Interrupted> {
    let journal = owner.writer().journal();
    match owner.open(client, first).await {
        OpenOutcome::Open { diverged, .. } => Ok(diverged),
        OpenOutcome::NotClaimed(outcome) => Err(Interrupted::NotClaimed { journal, outcome }),
        OpenOutcome::Behind(outcome) => Err(Interrupted::Behind { journal, outcome }),
    }
}

/// Append `record` as `owner` and make sure the fold holds it: a write the
/// journal took at another position than the fold's next (a resolved
/// ambiguity) is folded by reading up to it — the first checkpoint that
/// read found diverged, if any.
async fn append<P: Providers, S: crate::client::checkpoint::Checkpointable>(
    owner: &mut Checkpointer<S>,
    client: &Client<P>,
    first: usize,
    record: Vec<u8>,
) -> Result<Option<u64>, Interrupted> {
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
        return match owner.load(client, first, tail).await {
            LoadOutcome::Loaded {
                up_to, diverged, ..
            } if up_to >= tail => Ok(diverged),
            outcome => Err(Interrupted::Behind { journal, outcome }),
        };
    }
    Ok(None)
}

/// Read meta (at `frame`) to its tail, as a reader that owns nothing
/// (`parosctl tenant list`).
///
/// # Errors
///
/// The fold did not reach the tail (see [`LoadOutcome`]).
#[tracing::instrument(level = "debug", skip_all, fields(meta = %frame))]
pub async fn read_meta<P: Providers>(
    client: &Client<P>,
    first: usize,
    frame: JournalKey,
) -> Result<Meta, LoadOutcome> {
    let mut folder = Folder::new(Meta::default());
    match super::checkpoint::load(&mut folder, frame, client, first, 0).await {
        LoadOutcome::Loaded { .. } => Ok(folder.state().clone()),
        outcome => Err(outcome),
    }
}
