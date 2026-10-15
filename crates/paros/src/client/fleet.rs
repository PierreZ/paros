//! **Fleet operations** (#229, `docs/architecture.md` §3.1, §3.7): `init`'s
//! fleet steps, and creating and removing a tenant, each an **idempotent
//! state machine** over two control journals — the fleet tenant's (the fleet's
//! directory) and the cell tenant's (the cell's tenant list). No identifier is
//! fixed (§3.8): the caller hands the session both, read from the cell plan
//! or from any machine's node-only `Inspect` ([`ControlJournals`]).
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
//! - **`init`** (steps 2 and 3 of §3.1): the fleet tenant records the fleet's id and
//!   itself (`FormFleet`: the fleet tenant is the fleet's first tenant, `{internal,
//!   fleet}`), the cell joins the fleet directory `REGISTERING` with its cell
//!   tenant (`AddCell`: `{internal, cell}`), the cell records the fleet on
//!   its side (`JoinFleet`), and the fleet tenant marks the cell `READY`.
//!   The cell coordinator writes `JoinFleet`, not the operator
//!   ([`crate::machine`], #513 (fleet Init duel)): `init` reads the cell
//!   control journal and waits for the join ([`Interrupted::NotJoined`]).
//!   The coordinator also books the cell tenant's and the fleet tenant's
//!   footprints (#476 (tenant footprint)).
//! - **Creating a tenant**: a `{users}` tenant only — the tenant API cannot
//!   create an `internal` one. The fleet tenant registers it `REGISTERING` with its
//!   identifier and its cell assignment (the one `READY` cell in M9). The
//!   cell coordinator reads the entry, books the tenant's footprint
//!   (`GrantCapacity`, [`crate::footprint`], #476 (tenant footprint)) and
//!   hosts the tenant (`HostTenant`, [`crate::machine`], #225). The operator
//!   waits for both ([`Interrupted::NotHosted`]; while a founding member
//!   is not registered, the cell refuses no footprint, so the creation
//!   waits). Then the fleet tenant marks it `READY`. A footprint the cell refused makes the
//!   operator mark the tenant `ERROR` and end the creation `NoFootprint`.
//!   A creation is resumed only under its own identifier; a name held
//!   `READY` under it ends the operation at once.
//! - **Removing a tenant**: the fleet tenant marks it `REMOVING`. The cell
//!   coordinator reads the entry and drops the tenant (`DropTenant`, a
//!   tombstone, written whether the cell hosted it or not): the operator
//!   waits for it ([`Interrupted::NotDropped`]). Then the fleet tenant
//!   removes it.
//! - **Root keys** (#245 (Biscuit tokens)): `init` writes its root public key
//!   into the universe entry right after `FormFleet`, when the directory holds
//!   no key yet ([`FleetSession::with_root_key`]); `universe key add` and
//!   `universe key remove` are one write each ([`FleetSession::add_root_key`],
//!   [`FleetSession::remove_root_key`]).
//!
//! **Every step checks it still talks to the same fleet and the same cell**
//! (FDB's `MetaclusterOperationContext`): every entry names the fleet the fleet tenant
//! formed, and both folds refuse another; before it waits on a cell step, the
//! cell's own registration must name the fleet directory's fleet and the cell
//! the tenant is assigned to ([`FleetRefusal::CellMismatch`]).
//!
//! **A cell step decided from a stale fleet directory fold is judged by the cell.** The
//! cell coordinator reads the fleet tenant, then writes the cell; between the two an
//! operator may move the tenant to `REMOVING`. A removal always tombstones the
//! tenant on the cell before the fleet tenant forgets it, and the cell
//! refuses to host a tombstoned tenant at apply, so a stale `HostTenant`
//! changes nothing ([`FleetRefusal::Removed`]).
//!
//! **Who writes.** The fleet tenant's journal is written by the session that
//! last claimed it with `SetLeader`, under a leader uuid drawn from the
//! session's seed (#241). It has one leader per term, so two operators
//! running at once fence each other: the loser's write is refused, its run
//! is [`Step::Interrupted`], and running it again resumes from what the
//! winner wrote. No fleet operation claims the cell control journal: it
//! reads it, and the elected cell coordinator, its one writer, writes the
//! cell's step of each operation (`JoinFleet`, #513 (fleet Init duel);
//! a tenant's footprint, `HostTenant` and `DropTenant`, #225). A claim
//! there fenced the
//! coordinator, whose next term installed a fresh uuid and fenced the
//! operator in turn; with slow links each claim lost to the next install,
//! and `init` did not end (witness 1522711066683701314 on a0c4aa6, red on
//! "fleet: an operation an operator stopped in ends in the recovery
//! tail"). A tenant's creation and removal claimed it the same way until
//! #225; the sweep of 07424f8 left two creations `REGISTERING` on that
//! oracle (seed 15189977391382984753, green when replayed alone). The
//! fleet tenant is checkpointed as its owner writes it
//! ([`crate::client::checkpoint`], #227); the cell's journal is never
//! checkpointed here — its owner's fold is the registry's, over a genesis
//! pool only the deployment knows.
//!
//! Like the rest of the client it draws no randomness: tenant identifiers are the
//! caller's draws, and [`FleetSession::create_tenant`] moves to the next one
//! when the fleet tenant holds a draw already.

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalIdentifier, NodeId, TenantId};

use super::Client;
use super::checkpoint::{
    AppendOutcome, CheckpointOutcome, CheckpointPolicy, Checkpointer, Folder, LoadOutcome,
    OpenOutcome, load,
};
use super::outcome::ClaimOutcome;
use super::writer::{Writer, WriterOutcome};
use crate::fleet::RootKeyEntry;
use crate::fleet::{CellState, FleetCommand, FleetDirectory, FleetEntry, TenantState};
use crate::machine::ControlJournals;
use crate::system::{FleetRegistration, Registry};
use crate::tenant::Survives;

/// One step a fleet operation wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// The fleet tenant recorded the fleet's id, and itself.
    FormFleet,
    /// The cell joined the fleet directory, `REGISTERING`, with its cell
    /// tenant.
    AddCell,
    /// The fleet tenant marked the cell `READY`.
    CellReady,
    /// The fleet tenant registered the tenant, `REGISTERING`.
    RegisterTenant,
    /// The fleet tenant marked the tenant `ERROR`: its cell refused its
    /// footprint (#476).
    TenantError,
    /// The fleet tenant marked the tenant `READY`.
    TenantReady,
    /// The fleet tenant marked the tenant `REMOVING`.
    TenantRemoving,
    /// The fleet tenant removed the tenant.
    RemoveTenant,
    /// The cell registered a machine it admits (`RegisterNode`, #216).
    RegisterMachine,
    /// The machine recorded its admission (`Admit`, #216).
    Admit,
    /// The universe entry took a root public key (#245).
    AddRootKey,
    /// A root public key left the universe entry (#245).
    RemoveRootKey,
}

/// Why a fleet operation cannot go on: running it again changes nothing
/// until something else does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetRefusal {
    /// `init` was given an unset fleet id, cell id or identifier.
    Unset,
    /// The fleet tenant holds no fleet, or no `READY` cell: run `init` first.
    NotInitialized,
    /// The cell's side of the registration names another fleet or another
    /// cell than the fleet directory, or than the tenant's assignment, or the directory names
    /// another fleet than the cell: this operation talks to the wrong cell.
    CellMismatch {
        /// What the operation expected: `(fleet_id, cell_id)`.
        expected: (u64, u64),
        /// What the cell recorded, if anything.
        found: Option<FleetRegistration>,
    },
    /// The fleet tenant's entry for the cell is being removed.
    CellRemoving,
    /// Every tenant identifier drawn is held by an entry or was removed: draw
    /// more.
    IdTaken,
    /// The name is held by another creation's tenant, in any state: a
    /// tenant is created once, and only a run carrying its identifier resumes it.
    NameTaken {
        /// The tenant holding it.
        tenant: TenantId,
        /// Its state.
        state: TenantState,
    },
    /// The machine being admitted belongs to another cell (#216): founded
    /// or admitted there.
    OtherCell {
        /// The machine.
        node: NodeId,
        /// Its cell, when it said (0 when its `Admit` refusal named none).
        cell_id: u64,
    },
    /// The machine being admitted promised in a `cell init` and has no cell
    /// (#216): a cell may still form over it, so it is admitted nowhere.
    InCellInit {
        /// The machine.
        node: NodeId,
    },
    /// The machine being admitted was retired from this cell: a retired id
    /// never comes back (#216).
    Retired {
        /// The machine.
        node: NodeId,
    },
    /// The cell has too few free slots for the tenant's footprint (#476):
    /// the cell refused it, and a creation marks the tenant `ERROR` (remove
    /// it to free its name). For `init`, the cell tenant's or the universe
    /// tenant's.
    NoFootprint {
        /// The tenant.
        tenant: TenantId,
    },
    /// The tenant being created was removed meanwhile: a removal overtook
    /// the creation (the fleet tenant holds it `REMOVING`, or the cell dropped it, and
    /// never hosts it again).
    Removed {
        /// The tenant.
        tenant: TenantId,
    },
    /// Another root key, in force or removed, holds the key's id or its
    /// label (#245): neither is reused.
    RootKeyTaken,
    /// No root key in force has the label (#245).
    UnknownRootKey,
    /// The label names the universe's last root key, which stays (#245).
    LastRootKey,
    /// The root key is not one: an unset id, no label, or a text that is
    /// not an Ed25519 public key (#245).
    MalformedRootKey,
    /// The machine being admitted checks an `admin` token against its
    /// pinned root key, and refused the one sent (#245).
    Denied {
        /// The machine.
        node: NodeId,
        /// Why.
        denial: crate::frontend::Denial,
    },
}

/// What stopped a step's write from landing: run the operation again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Interrupted {
    /// `journal` could not be claimed (see [`ClaimOutcome`]).
    NotClaimed {
        /// The journal.
        journal: JournalIdentifier,
        /// The claim's outcome.
        outcome: ClaimOutcome,
    },
    /// `journal` was claimed but not folded to its tail (see
    /// [`LoadOutcome`]).
    Behind {
        /// The journal.
        journal: JournalIdentifier,
        /// The fold's outcome.
        outcome: LoadOutcome,
    },
    /// A step's write is not known written (see [`WriterOutcome`]): refused,
    /// fenced by another writer, unanswered or ambiguous.
    NotWritten {
        /// The journal.
        journal: JournalIdentifier,
        /// The write's verdict.
        outcome: WriterOutcome,
    },
    /// The cell has not joined the fleet yet: the directory holds the cell,
    /// and the cell coordinator writes the join into `journal`, the cell
    /// control journal (#513 (fleet Init duel)). Nothing was written.
    NotJoined {
        /// The cell control journal.
        journal: JournalIdentifier,
    },
    /// The cell does not host the tenant being created yet: the directory
    /// holds it `REGISTERING`, and the cell coordinator writes `HostTenant`
    /// into `journal`, the cell control journal (#225). Nothing was written.
    NotHosted {
        /// The cell control journal.
        journal: JournalIdentifier,
        /// The tenant.
        tenant: TenantId,
    },
    /// The cell has not dropped the tenant being removed yet: the directory
    /// holds it `REMOVING`, and the cell coordinator writes `DropTenant`
    /// into `journal`, the cell control journal (#225). Nothing was written.
    NotDropped {
        /// The cell control journal.
        journal: JournalIdentifier,
        /// The tenant.
        tenant: TenantId,
    },
    /// The machine being admitted did not answer, or failed to record its
    /// admission (#216). Its admission may have landed.
    MachineUnreachable {
        /// The machine's address.
        addr: crate::Address,
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
/// uses: it writes the fleet tenant's as its owner, and it only reads the
/// cell's, which the cell coordinator writes (#513 (fleet Init duel), #225).
#[derive(Clone, Debug)]
pub struct FleetSession {
    journals: ControlJournals,
    /// The fleet tenant's control journal, read off `journals` at
    /// construction: a session exists only over a cell that hosts it.
    fleet: JournalIdentifier,
    directory: Checkpointer<FleetDirectory>,
    /// The cell control journal's fold, read as a reader that owns nothing.
    cell: Folder<Registry>,
    directory_open: bool,
    /// The first checkpoint a fold of this session found unequal to its own
    /// state, with its journal (see [`LoadOutcome::Loaded`]).
    diverged: Option<(JournalIdentifier, u64)>,
    /// What a tenant this session registers survives (#252).
    survives: Survives,
    /// The names `init` gives the universe and the cell (#252, #399):
    /// empty is unnamed.
    names: (Vec<u8>, Vec<u8>),
    /// The root public key `init` writes into the universe entry (#245).
    root_key: Option<RootKeyEntry>,
}

impl FleetSession {
    /// A session over `journals`, writing the fleet tenant (checkpointing it
    /// under `policy`) under leader uuids drawn from `seed`
    /// ([`Writer::new`]), and folding the cell's journal into `cell` — the
    /// empty registry over the deployment's genesis pool.
    /// `None` when `journals` names no fleet journal: a cell that does not
    /// host the fleet tenant runs no fleet operation (no identifier has a
    /// default, §3.8).
    #[must_use]
    pub fn new(
        journals: ControlJournals,
        seed: u128,
        cell: Registry,
        policy: CheckpointPolicy,
    ) -> Option<Self> {
        let fleet = journals.fleet?;
        Some(Self {
            journals,
            fleet,
            directory: Checkpointer::new(fleet, seed, FleetDirectory::default(), policy),
            cell: Folder::new(cell),
            directory_open: false,
            diverged: None,
            survives: Survives::Az,
            names: (Vec::new(), Vec::new()),
            root_key: None,
        })
    }

    /// Give the universe `key` as its first root public key when `init`
    /// forms it (#245): written after `FormFleet` while the universe entry
    /// holds no key. A re-run over a universe that holds keys writes none.
    #[must_use]
    pub fn with_root_key(mut self, key: RootKeyEntry) -> Self {
        self.root_key = Some(key);
        self
    }

    /// Name the universe `universe` and the cell `cell` when `init` forms
    /// them (#252, #399). A re-run never renames: the first name written
    /// stays.
    #[must_use]
    pub fn with_names(mut self, universe: &[u8], cell: &[u8]) -> Self {
        self.names = (universe.to_vec(), cell.to_vec());
        self
    }

    /// Register every tenant this session creates as surviving `survives`
    /// (#252; `az` otherwise).
    #[must_use]
    pub fn with_survives(mut self, survives: Survives) -> Self {
        self.survives = survives;
        self
    }

    /// The first checkpoint any fold of this session — the fleet
    /// directory's or the cell's, opening or catching up after a write —
    /// verified and found **unequal** to its own state, with its journal: an
    /// owner wrote a state that is not the fold of its journal (`None` while
    /// every one matched).
    #[must_use]
    pub fn diverged(&self) -> Option<(JournalIdentifier, u64)> {
        self.diverged
    }

    /// The fleet directory as this session last folded it.
    #[must_use]
    pub fn directory(&self) -> &FleetDirectory {
        self.directory.state()
    }

    /// The cell's control journal as this session last folded it.
    #[must_use]
    pub fn cell(&self) -> &Registry {
        self.cell.state()
    }

    /// The writer of the fleet tenant's control journal: the uuid it leads
    /// under and the position it writes next.
    #[must_use]
    pub fn writer(&self) -> &Writer {
        self.directory.writer()
    }

    /// Whether this session holds the fleet tenant's journal, claimed and
    /// folded.
    #[must_use]
    pub fn holds_directory(&self) -> bool {
        self.directory_open
    }

    /// One step of `init`'s fleet half. The fleet is formed under `fleet_id`
    /// unless the fleet directory or the cell recorded one already, which is then kept — a
    /// re-run never mints a second fleet. Ends with the fleet's id once the directory
    /// holds the cell `READY` and the cell names the fleet.
    #[tracing::instrument(level = "trace", skip_all, fields(cell = self.journals.cell_id))]
    pub async fn init_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet_id: u64,
    ) -> Step<u64> {
        let ControlJournals {
            cell_id,
            cell: control,
            ..
        } = self.journals;
        let fleet_control = self.fleet;
        if cell_id == 0 || !control.is_set() || !fleet_control.is_set() {
            return Step::Refused(FleetRefusal::Unset);
        }
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let Some(fleet) = self.directory.state().fleet() else {
            // The cell may have joined a fleet the fleet tenant lost (a recovery, #231):
            // its id is kept.
            if let Err(stop) = self.read_cell(client, first).await {
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
            let form = FleetCommand::FormFleet {
                control: fleet_control,
                name: self.names.0.clone(),
            };
            return self
                .write_directory(client, first, fleet, form, Stage::FormFleet)
                .await;
        };
        if let Some(key) = &self.root_key
            && self.directory.state().root_keys().next().is_none()
        {
            if !key.is_well_formed() {
                return Step::Refused(FleetRefusal::MalformedRootKey);
            }
            let add = FleetCommand::AddRootKey(key.clone());
            return self
                .write_directory(client, first, fleet, add, Stage::AddRootKey)
                .await;
        }
        let Some(cell) = self.directory.state().cell(cell_id) else {
            // The cell tenant is the fleet tenant's already, under another cell: this
            // operation names the wrong cell (the fleet tenant would refuse the entry).
            if self.directory.state().tenant(control.tenant).is_some() {
                return Step::Refused(FleetRefusal::CellMismatch {
                    expected: (fleet, cell_id),
                    found: None,
                });
            }
            let add = FleetCommand::AddCell {
                cell_id,
                control,
                name: self.names.1.clone(),
            };
            return self
                .write_directory(client, first, fleet, add, Stage::AddCell)
                .await;
        };
        if let Err(stop) = self.read_cell(client, first).await {
            return Step::Interrupted(stop);
        }
        match self.cell.state().fleet() {
            // The cell coordinator, the cell control journal's one writer,
            // joins the cell once it reads the cell in the directory
            // (#513 (fleet Init duel)): wait for it, claiming nothing.
            None => {
                moonpool_assertions::reachable!("fleet: an init waits for the cell to join");
                return Step::Interrupted(Interrupted::NotJoined { journal: control });
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
                let ready = FleetCommand::MarkCell {
                    cell_id,
                    state: CellState::Ready,
                };
                self.finish_directory(client, first, fleet, ready, Stage::CellReady, fleet)
                    .await
            }
        }
    }

    /// One step of creating the `users` tenant `name` under the identifier `draw` (its id and its control journal's). The identifier
    /// names the creation: when the fleet tenant holds `name` under `draw`, the step
    /// resumes it; under any other identifier, in any state, it is refused
    /// ([`FleetRefusal::NameTaken`]), since a tenant is created once
    /// (`docs/architecture.md` §3.7). Ends with the tenant's id once the directory
    /// holds it `READY`.
    #[tracing::instrument(level = "trace", skip_all, fields(tenant = draw.tenant.0))]
    pub async fn create_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        draw: JournalIdentifier,
    ) -> Step<TenantId> {
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let directory = self.directory.state();
        let Some(fleet) = directory.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some((tenant, entry)) = directory.named(name) else {
            if !draw.is_set() {
                return Step::Refused(FleetRefusal::Unset);
            }
            if directory.tenant(draw.tenant).is_some() || directory.is_removed(draw.tenant) {
                return Step::Refused(FleetRefusal::IdTaken);
            }
            let Some((cell_id, _)) = directory.cells().find(|(_, c)| c.state == CellState::Ready)
            else {
                return Step::Refused(FleetRefusal::NotInitialized);
            };
            let register = FleetCommand::RegisterTenant {
                control: draw,
                name: name.to_vec(),
                cell_id,
                survives: self.survives,
            };
            return self
                .write_directory(client, first, fleet, register, Stage::RegisterTenant)
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
            // Marked by this creation once its cell refused its footprint.
            TenantState::Error => return Step::Refused(FleetRefusal::NoFootprint { tenant }),
            state @ TenantState::UpdatingConfiguration => {
                return Step::Refused(FleetRefusal::NameTaken { tenant, state });
            }
        };
        if let Err(stop) = self.cell_of(client, first, fleet, cell_id).await {
            return stop;
        }
        let cell = self.cell.state();
        if cell.dropped(tenant) {
            // Removed under this operation: its fleet directory fold is stale.
            self.directory_open = false;
            return Step::Refused(FleetRefusal::Removed { tenant });
        }
        if cell.hosts(tenant) {
            let ready = FleetCommand::MarkTenant {
                tenant,
                state: TenantState::Ready,
            };
            return self
                .finish_directory(client, first, fleet, ready, Stage::TenantReady, tenant)
                .await;
        }
        // The cell coordinator, the cell control journal's one writer,
        // books a `REGISTERING` tenant's footprint, then hosts it, once it
        // reads it in the directory (#476, #225): the operator waits,
        // claiming nothing. A refused footprint ends the creation: the
        // tenant is marked `ERROR`.
        match cell.footprint(tenant) {
            Some(grant) if !grant.granted() => {
                let error = FleetCommand::MarkTenant {
                    tenant,
                    state: TenantState::Error,
                };
                self.write_directory(client, first, fleet, error, Stage::TenantError)
                    .await
            }
            _ => {
                moonpool_assertions::reachable!("fleet: a creation waits for the cell to host it");
                Step::Interrupted(Interrupted::NotHosted {
                    journal: self.journals.cell,
                    tenant,
                })
            }
        }
    }

    /// One step of removing the `users` tenant `name`. Ends with its id once
    /// the fleet tenant removed it, or with `None` when the fleet tenant holds no tenant by that
    /// name (never created, or removed already).
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn remove_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
    ) -> Step<Option<TenantId>> {
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let directory = self.directory.state();
        let Some(fleet) = directory.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some((tenant, entry)) = directory.named(name) else {
            return Step::Done {
                result: None,
                last: None,
            };
        };
        let (state, cell_id) = (entry.state, entry.cell_id);
        if state != TenantState::Removing {
            let removing = FleetCommand::MarkTenant {
                tenant,
                state: TenantState::Removing,
            };
            return self
                .write_directory(client, first, fleet, removing, Stage::TenantRemoving)
                .await;
        }
        if let Err(stop) = self.cell_of(client, first, fleet, cell_id).await {
            return stop;
        }
        // The cell coordinator tombstones a `REMOVING` tenant once it reads
        // it in the directory, whether the cell hosts it or not (#225): wait
        // for it, claiming nothing.
        if !self.cell.state().dropped(tenant) {
            moonpool_assertions::reachable!("fleet: a removal waits for the cell to drop it");
            return Step::Interrupted(Interrupted::NotDropped {
                journal: self.journals.cell,
                tenant,
            });
        }
        let remove = FleetCommand::RemoveTenant { tenant };
        self.finish_directory(
            client,
            first,
            fleet,
            remove,
            Stage::RemoveTenant,
            Some(tenant),
        )
        .await
    }

    /// One step of `universe key add` (#245): ends once the universe entry
    /// holds `key`.
    #[tracing::instrument(level = "trace", skip_all, fields(key = %key.label))]
    pub async fn add_key_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        key: &RootKeyEntry,
    ) -> Step<u32> {
        if !key.is_well_formed() {
            return Step::Refused(FleetRefusal::MalformedRootKey);
        }
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let directory = self.directory.state();
        let Some(fleet) = directory.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        if directory.root_keys().any(|held| held == key) {
            return Step::Done {
                result: key.key_id,
                last: None,
            };
        }
        if directory
            .root_keys()
            .any(|held| held.key_id == key.key_id || held.label == key.label)
        {
            return Step::Refused(FleetRefusal::RootKeyTaken);
        }
        let add = FleetCommand::AddRootKey(key.clone());
        let step = self
            .finish_directory(client, first, fleet, add, Stage::AddRootKey, key.key_id)
            .await;
        self.judge_key(step, FleetRefusal::RootKeyTaken, |directory| {
            directory.root_keys().any(|h| h == key)
        })
    }

    /// One step of `universe key remove` (#245): ends once no key in force
    /// has the label `label`, with the key id it removed (`None`: none was
    /// in force, an earlier run removed it).
    #[tracing::instrument(level = "trace", skip_all, fields(key = label))]
    pub async fn remove_key_step<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        label: &str,
    ) -> Step<Option<u32>> {
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let directory = self.directory.state();
        let Some(fleet) = directory.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        let Some(key_id) = directory
            .root_keys()
            .find(|key| key.label == label)
            .map(|key| key.key_id)
        else {
            return Step::Done {
                result: None,
                last: None,
            };
        };
        if directory.root_keys().count() == 1 {
            return Step::Refused(FleetRefusal::LastRootKey);
        }
        let remove = FleetCommand::RemoveRootKey {
            label: label.to_string(),
        };
        let step = self
            .finish_directory(
                client,
                first,
                fleet,
                remove,
                Stage::RemoveRootKey,
                Some(key_id),
            )
            .await;
        self.judge_key(step, FleetRefusal::LastRootKey, |directory| {
            directory.root_keys().all(|key| key.label != label)
        })
    }

    /// A key step the fold refused although the session's look allowed it
    /// (another operator's key landed between): the step is refused for the
    /// key with `refusal`, as the directory now stands.
    fn judge_key<T>(
        &self,
        step: Step<T>,
        refusal: FleetRefusal,
        holds: impl Fn(&FleetDirectory) -> bool,
    ) -> Step<T> {
        if matches!(step, Step::Done { .. }) && !holds(self.directory.state()) {
            moonpool_assertions::reachable!("fleet: a root key step lost a race at apply");
            return Step::Refused(refusal);
        }
        step
    }

    /// Add `key` to the universe entry to its end (see
    /// [`FleetSession::add_key_step`]), taking an interrupted step again for
    /// up to `patience`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn add_root_key<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        key: &RootKeyEntry,
        patience: Duration,
    ) -> Run<u32> {
        let deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.add_key_step(client, first, key).await;
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Remove the root key `label` from the universe entry to its end (see
    /// [`FleetSession::remove_key_step`]), taking an interrupted step again
    /// for up to `patience`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_root_key<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        label: &str,
        patience: Duration,
    ) -> Run<Option<u32>> {
        let deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.remove_key_step(client, first, label).await;
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }

    /// Run `init`'s fleet half to its end (see [`FleetSession::init_step`]),
    /// taking an interrupted step again for up to `patience` (a cell still
    /// electing its leaders, an operator racing this one).
    #[tracing::instrument(level = "debug", skip_all, fields(cell = self.journals.cell_id))]
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
    /// [`FleetSession::create_step`]), under the first of `draws` the fleet tenant does
    /// not hold: a draw the fleet tenant holds already is skipped for the next one. An
    /// interrupted step is taken again for up to `patience`.
    ///
    /// The creation is named by its whole draw set, so a resume passes the same
    /// `draws` and picks up under whichever of them the last run reached: a name
    /// held under one of this creation's own draws is this creation, not a
    /// `NameTaken` refusal. Found by the main campaign after the moonpool pin
    /// advance: a registration under the first draw was overtaken by a
    /// removal, the run moved on to the second draw and was interrupted again,
    /// and the next resume, starting from the first draw, refused its own
    /// tenant and left it registering for good (red on the
    /// "fleet: a second creation of a name is refused" gate, green after).
    ///
    /// # Panics
    ///
    /// Never on any input: the one assertion checks that a switch to another
    /// of this creation's draws moves to a different one.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn create_tenant<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        name: &[u8],
        draws: impl IntoIterator<Item = JournalIdentifier>,
        patience: Duration,
    ) -> Run<TenantId> {
        let deadline = client.now() + patience;
        let draws: Vec<JournalIdentifier> = draws.into_iter().collect();
        let mut at = 0;
        let mut draw = draws.first().copied().unwrap_or(JournalIdentifier::UNSET);
        // A switch is no step, so the directory moving between two looks could
        // bounce between draws: each look may switch at most this often.
        let mut switches = 2 * draws.len();
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.create_step(client, first, name, draw).await;
            if step == Step::Refused(FleetRefusal::IdTaken) && at + 1 < draws.len() && switches > 0
            {
                switches -= 1;
                at += 1;
                draw = draws[at];
                continue;
            }
            if let Step::Refused(FleetRefusal::NameTaken { tenant, .. }) = &step
                && let Some(own) = draws.iter().position(|d| d.tenant == *tenant && *d != draw)
                && switches > 0
            {
                switches -= 1;
                // `create_step` still checks the holder's control journal under
                // the draw it switches to, and the draw it holds now is never
                // switched to again, so a foreign holder is still refused.
                assert!(own != at, "a switch moves to another draw");
                at = own;
                draw = draws[at];
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

    /// Claim the fleet tenant and fold it to its tail, unless this session holds it.
    async fn open_directory<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        if !self.directory_open {
            let diverged = open(&mut self.directory, client, first).await?;
            self.note_diverged(self.fleet, diverged);
            self.directory_open = true;
        }
        Ok(())
    }

    /// Fold the cell's control journal to its tail as a reader, claiming
    /// nothing (#513 (fleet Init duel), #225): a fleet operation only reads
    /// the cell's side, which the cell coordinator writes. A claim here
    /// would fence the coordinator, whose next term fences this session in
    /// turn.
    ///
    /// # Errors
    ///
    /// The fold did not reach the tail.
    ///
    /// # Panics
    ///
    /// Never on any input: the one assertion checks the fold's own report.
    pub async fn read_cell<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        let journal = self.journals.cell;
        match load(&mut self.cell, journal, client, first, 0).await {
            LoadOutcome::Loaded {
                up_to, diverged, ..
            } => {
                assert_eq!(
                    up_to,
                    self.cell.next_seq(),
                    "a loaded fold stands at the position it reached"
                );
                self.note_diverged(journal, diverged);
                Ok(())
            }
            outcome => Err(Interrupted::Behind { journal, outcome }),
        }
    }

    /// Keep the first divergence a fold of `journal` reported.
    fn note_diverged(&mut self, journal: JournalIdentifier, diverged: Option<u64>) {
        if self.diverged.is_none() {
            self.diverged = diverged.map(|seq| (journal, seq));
        }
    }

    /// Read the cell's journal to its tail and check it is cell `cell_id`
    /// of `fleet`.
    async fn cell_of<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        cell_id: u64,
    ) -> Result<(), Step<T>> {
        self.read_cell(client, first)
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

    /// Write one fleet entry as its owner, and checkpoint the fleet directory when its
    /// policy finds it due.
    async fn write_directory<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: FleetCommand,
        stage: Stage,
    ) -> Step<T> {
        match self.append_directory(client, first, fleet, command).await {
            Ok(()) => Step::Advanced(stage),
            Err(stop) => Step::Interrupted(stop),
        }
    }

    /// [`FleetSession::write_directory`] for the step that ends the operation.
    async fn finish_directory<P: Providers, T>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: FleetCommand,
        stage: Stage,
        result: T,
    ) -> Step<T> {
        match self.append_directory(client, first, fleet, command).await {
            Ok(()) => Step::Done {
                result,
                last: Some(stage),
            },
            Err(stop) => Step::Interrupted(stop),
        }
    }

    async fn append_directory<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
        fleet: u64,
        command: FleetCommand,
    ) -> Result<(), Interrupted> {
        let record = FleetEntry::new(fleet, command).encode();
        match append(&mut self.directory, client, first, record).await {
            Ok(diverged) => self.note_diverged(self.fleet, diverged),
            Err(stop) => {
                self.directory_open = false;
                return Err(stop);
            }
        }
        if self.directory.due(client.now()) {
            // A checkpoint that does not land leaves the owner's belief
            // unsure: the next step opens the fleet tenant afresh.
            let checkpointed = self.directory.checkpoint(client, first).await;
            if !matches!(checkpointed, CheckpointOutcome::Checkpointed { .. }) {
                self.directory_open = false;
            }
        }
        Ok(())
    }
}

/// Whether a run takes `step` again: it was interrupted (a claim no server
/// decided, a leader not ready yet, a write fenced by another operator) and
/// the run's patience allows one more `retry_backoff`. The step is decided
/// afresh from the journals, so a retry is always a resumption.
pub(super) async fn retry<P: Providers, T>(
    client: &Client<P>,
    step: &Step<T>,
    deadline: Duration,
) -> bool {
    matches!(step, Step::Interrupted(_))
        && client.now() < deadline
        && client.pause(client.tunables().retry_backoff).await
}

/// Fold one step into a run: `None` to take the next, or the run's end.
pub(super) fn settle<T>(step: Step<T>, steps: &mut Vec<Stage>) -> Option<Run<T>> {
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
pub(super) fn going_round<T>(steps: Vec<Stage>) -> Run<T> {
    Run {
        outcome: Step::Interrupted(Interrupted::GoingRound { steps: steps.len() }),
        steps,
    }
}

/// Claim `owner`'s journal and fold it to its tail: the first checkpoint
/// the fold found diverged, if any.
pub(super) async fn open<P: Providers, S: crate::client::checkpoint::Checkpointable>(
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
pub(super) async fn append<P: Providers, S: crate::client::checkpoint::Checkpointable>(
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
        // A fleet or system entry is protobuf, which never starts with the
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

/// Read the fleet directory (the fleet tenant's control journal `journal`) to its tail, as
/// a reader that owns nothing (`parosctl tenant list`).
///
/// # Errors
///
/// The fold did not reach the tail (see [`LoadOutcome`]).
#[tracing::instrument(level = "debug", skip_all, fields(fleet = %journal))]
pub async fn read_directory<P: Providers>(
    client: &Client<P>,
    first: usize,
    journal: JournalIdentifier,
) -> Result<FleetDirectory, LoadOutcome> {
    let mut folder = Folder::new(FleetDirectory::default());
    match load(&mut folder, journal, client, first, 0).await {
        LoadOutcome::Loaded { .. } => Ok(folder.state().clone()),
        outcome => Err(outcome),
    }
}
