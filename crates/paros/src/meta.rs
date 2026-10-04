//! The **meta tenant** (#229, `docs/architecture.md` §3.7): the fleet's
//! directory. Its control journal answers one question — which tenant lives
//! in which cell — plus the cell entries; quotas, billing and status live
//! elsewhere. Its frame is random like every frame (§3.8): `init` draws it
//! and records it in the cell plan, and meta records it in its own first
//! entry.
//!
//! This module is the one reading of meta's entries: the typed
//! [`MetaEntry`] a fleet operation writes (one record per position) and the
//! pure fold [`Meta`] every reader runs over them in position order. Like the
//! system journals' folds ([`crate::system`]) it is a function of the log
//! alone, so every reader that folded a prefix agrees on it; and like them it
//! is not an application (#186): the core keeps the records opaque.
//!
//! - **Every entry names its fleet and its metadata version.** The first
//!   entry, [`MetaCommand::FormFleet`], records the fleet's id once; any later
//!   entry naming another fleet is refused ([`MetaRefusal::OtherFleet`]), so
//!   a step of a fleet operation that talks to another fleet than its
//!   previous step did changes nothing (FDB's `MetaclusterOperationContext`).
//!   An entry whose version this fold does not understand is refused
//!   ([`MetaRefusal::UnknownVersion`]).
//! - **Every tenant has a group and a placement**, fixed when it is
//!   registered (§3.7). The group only separates the tenants paros
//!   administrates itself with ([`Group::Internal`]: meta, registered by
//!   `FormFleet`, and each cell's cell tenant, registered by `AddCell`) from
//!   the ones it serves ([`Group::Users`]): `RegisterTenant` — the tenant
//!   API's entry — cannot express another group, and an internal tenant is
//!   never marked or removed through it ([`MetaRefusal::Internal`]). The
//!   placement ([`Placement::Movable`] or [`Placement::Pinned`]) is the
//!   creator's: a cell tenant is registered pinned, meta movable, a user
//!   tenant as asked.
//! - **Every fleet operation is an idempotent state machine.** A tenant is
//!   written in `REGISTERING` with its cell assignment, created in the cell,
//!   then marked `READY` ([`crate::client::fleet`]); `init` takes its cell
//!   entry through the same `REGISTERING` → `READY`. An entry that asks for
//!   what the directory already holds folds to [`MetaEvent::Unchanged`], so
//!   re-running a step after a crash is harmless. The transitions are
//!   [`TenantState::may_become`] and [`CellState::may_become`].
//! - **No id is fixed** (§3.8). A tenant's id and its control journal's are
//!   random, drawn by its creator and checked here, the tenant ids' single
//!   writer: an id any entry holds — meta's own and the cell tenants'
//!   included — or that was ever removed is taken
//!   ([`MetaRefusal::TenantIdTaken`]), and the creator redraws; an unset id
//!   is malformed. A live name is held by one tenant at a time
//!   ([`MetaRefusal::NameTaken`]).
//! - **Only a `READY` cell receives new tenants.**
//! - **The directory is a pointer, never the authority** (#225): a tenant's
//!   control journal's generation wins over it, and the directory is
//!   rebuilt from the cells' tenant lists when lost (the recovery issue,
//!   #231). Meta is checkpointed with `paros::client::checkpoint` (#227),
//!   `Inline` in M9: [`Meta`] is [`Checkpointable`].
//!
//! Every malformed entry folds to a refusal, never a panic: the entries are
//! external input.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{JournalId, JournalKey, TenantId};
use prost::Message as _;

use crate::client::checkpoint::{Checkpointable, Folded};
use crate::rpc::meta as wire;

/// The metadata version this fold speaks: written into every entry, and
/// into the cell's side of the registration (`crate::system`).
pub const METADATA_VERSION: u32 = 1;

/// Which tenants a tenant is with (§3.7): the ones paros administrates
/// itself with, or the ones it serves. Fixed at registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Group {
    /// Meta and the cell tenants: created by `init` and by adding a cell,
    /// never through the tenant API.
    Internal,
    /// Every tenant the tenant API creates.
    Users,
}

/// Whether a tenant may move to another cell (§3.7). Fixed at
/// registration, chosen by its creator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Placement {
    /// It may move (M12).
    Movable,
    /// It lives and dies with its cell: a move is refused.
    Pinned,
}

impl Group {
    fn to_wire(self) -> u32 {
        match self {
            Group::Internal => 1,
            Group::Users => 2,
        }
    }

    fn from_wire(group: u32) -> Option<Self> {
        match group {
            1 => Some(Group::Internal),
            2 => Some(Group::Users),
            _ => None,
        }
    }

    /// The group's label (`internal`, `users`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Group::Internal => "internal",
            Group::Users => "users",
        }
    }
}

impl Placement {
    fn to_wire(self) -> u32 {
        match self {
            Placement::Movable => 1,
            Placement::Pinned => 2,
        }
    }

    fn from_wire(placement: u32) -> Option<Self> {
        match placement {
            1 => Some(Placement::Movable),
            2 => Some(Placement::Pinned),
            _ => None,
        }
    }

    /// The placement's label (`movable`, `pinned`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Placement::Movable => "movable",
            Placement::Pinned => "pinned",
        }
    }
}

/// Where a cell entry stands (§3.7). Only a [`CellState::Ready`] cell
/// receives new tenants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CellState {
    /// Written by `init` (or a cell addition); not serving new tenants yet.
    Registering,
    /// Serving.
    Ready,
    /// Being removed (M12).
    Removing,
    /// Being restored by a recovery (#231).
    Restoring,
}

/// Where a tenant entry stands (§3.7, FDB's metacluster tenant states).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TenantState {
    /// Written with its cell assignment; the cell may not host it yet.
    Registering,
    /// Hosted by its cell and serving.
    Ready,
    /// Being removed: the cell may still host it.
    Removing,
    /// Its configuration is being changed (its sequence number moved).
    UpdatingConfiguration,
    /// Being renamed (M12).
    Renaming,
    /// An operation on it failed in a way a re-run cannot resume.
    Error,
}

impl CellState {
    fn to_wire(self) -> u32 {
        match self {
            CellState::Registering => 0,
            CellState::Ready => 1,
            CellState::Removing => 2,
            CellState::Restoring => 3,
        }
    }

    fn from_wire(state: u32) -> Option<Self> {
        Some(match state {
            0 => CellState::Registering,
            1 => CellState::Ready,
            2 => CellState::Removing,
            3 => CellState::Restoring,
            _ => return None,
        })
    }

    /// Whether a cell in `self` may be marked `to`. A cell is never marked
    /// `REGISTERING` (only [`MetaCommand::AddCell`] writes that); it becomes
    /// `READY` from `REGISTERING` or `RESTORING`, `RESTORING` from `READY`,
    /// and `REMOVING` from any state.
    #[must_use]
    pub fn may_become(self, to: CellState) -> bool {
        match to {
            CellState::Registering => false,
            CellState::Ready => matches!(self, CellState::Registering | CellState::Restoring),
            CellState::Restoring => self == CellState::Ready,
            CellState::Removing => true,
        }
    }
}

impl TenantState {
    fn to_wire(self) -> u32 {
        match self {
            TenantState::Registering => 0,
            TenantState::Ready => 1,
            TenantState::Removing => 2,
            TenantState::UpdatingConfiguration => 3,
            TenantState::Renaming => 4,
            TenantState::Error => 5,
        }
    }

    fn from_wire(state: u32) -> Option<Self> {
        Some(match state {
            0 => TenantState::Registering,
            1 => TenantState::Ready,
            2 => TenantState::Removing,
            3 => TenantState::UpdatingConfiguration,
            4 => TenantState::Renaming,
            5 => TenantState::Error,
            _ => return None,
        })
    }

    /// Whether a tenant in `self` may be marked `to` (§3.7). A tenant is
    /// never marked `REGISTERING` (only [`MetaCommand::RegisterTenant`]
    /// writes that). Any tenant may be removed; only a `READY` one is
    /// reconfigured or renamed; an operation's end returns it to `READY`;
    /// a tenant being removed takes no other state.
    #[must_use]
    pub fn may_become(self, to: TenantState) -> bool {
        match to {
            TenantState::Registering => false,
            TenantState::Removing => true,
            TenantState::Ready => matches!(
                self,
                TenantState::Registering
                    | TenantState::UpdatingConfiguration
                    | TenantState::Renaming
            ),
            TenantState::UpdatingConfiguration | TenantState::Renaming => {
                self == TenantState::Ready
            }
            TenantState::Error => self != TenantState::Removing,
        }
    }

    /// The state's label (`REGISTERING`, `READY`, …), as `parosctl` prints it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TenantState::Registering => "REGISTERING",
            TenantState::Ready => "READY",
            TenantState::Removing => "REMOVING",
            TenantState::UpdatingConfiguration => "UPDATING_CONFIGURATION",
            TenantState::Renaming => "RENAMING",
            TenantState::Error => "ERROR",
        }
    }
}

impl CellState {
    /// The state's label (`REGISTERING`, `READY`, …).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            CellState::Registering => "REGISTERING",
            CellState::Ready => "READY",
            CellState::Removing => "REMOVING",
            CellState::Restoring => "RESTORING",
        }
    }
}

/// What one meta entry asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaCommand {
    /// `init`: record the entry's fleet as meta's, once, and register meta
    /// itself (`internal`, movable) under its own frame.
    FormFleet {
        /// Meta's control journal: its tenant is meta's id.
        meta: JournalKey,
    },
    /// `init` (and adding a cell, M12): cell `cell_id` joins the directory,
    /// `REGISTERING`, and its cell tenant is registered (`internal`,
    /// pinned) under the frame of its control journal.
    AddCell {
        /// The cell's id (random, minted at `init`).
        cell_id: u64,
        /// The cell tenant's control journal.
        control: JournalKey,
    },
    /// Move cell `cell_id` to `state`.
    MarkCell {
        /// The cell.
        cell_id: u64,
        /// Its new state.
        state: CellState,
    },
    /// Tenant creation's first step: a `users` tenant named `name`, its
    /// control journal `control` (both ids its creator drew), assigned to
    /// `cell_id`, `REGISTERING`.
    RegisterTenant {
        /// The tenant's control journal: its tenant is the tenant's id.
        control: JournalKey,
        /// Opaque bytes; paros never interprets them.
        name: Vec<u8>,
        /// The cell it is assigned to.
        cell_id: u64,
        /// Whether it may move.
        placement: Placement,
    },
    /// Move `tenant` to `state`.
    MarkTenant {
        /// The tenant.
        tenant: TenantId,
        /// Its new state.
        state: TenantState,
    },
    /// Tenant removal's last step: a `REMOVING` tenant leaves the
    /// directory for good.
    RemoveTenant {
        /// The tenant.
        tenant: TenantId,
    },
}

/// One meta entry: a command, framed by the fleet it was written for and
/// the metadata version its writer speaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaEntry {
    /// The fleet the writer believes it talks to.
    pub fleet_id: u64,
    /// The writer's metadata version.
    pub version: u32,
    /// What it asks for.
    pub command: MetaCommand,
}

fn frame(tenant: u64, journal: u64) -> JournalKey {
    JournalKey::new(TenantId(tenant), JournalId(journal))
}

impl MetaEntry {
    /// `command` for fleet `fleet_id` at this fold's [`METADATA_VERSION`].
    #[must_use]
    pub fn new(fleet_id: u64, command: MetaCommand) -> Self {
        Self {
            fleet_id,
            version: METADATA_VERSION,
            command,
        }
    }

    /// The record a fleet operation writes: exactly one per position.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::meta_entry::Kind;
        let kind = match &self.command {
            MetaCommand::FormFleet { meta } => Kind::FormFleet(wire::FormFleet {
                meta_tenant: meta.tenant.0,
                meta_journal: meta.journal.0,
            }),
            MetaCommand::AddCell { cell_id, control } => Kind::AddCell(wire::AddCell {
                cell_id: *cell_id,
                control_tenant: control.tenant.0,
                control_journal: control.journal.0,
            }),
            MetaCommand::MarkCell { cell_id, state } => Kind::MarkCell(wire::MarkCell {
                cell_id: *cell_id,
                state: state.to_wire(),
            }),
            MetaCommand::RegisterTenant {
                control,
                name,
                cell_id,
                placement,
            } => Kind::RegisterTenant(wire::RegisterTenant {
                tenant: control.tenant.0,
                name: name.clone(),
                cell_id: *cell_id,
                control_journal: control.journal.0,
                placement: placement.to_wire(),
            }),
            MetaCommand::MarkTenant { tenant, state } => Kind::MarkTenant(wire::MarkTenant {
                tenant: tenant.0,
                state: state.to_wire(),
            }),
            MetaCommand::RemoveTenant { tenant } => {
                Kind::RemoveTenant(wire::RemoveTenant { tenant: tenant.0 })
            }
        };
        wire::MetaEntry {
            fleet_id: self.fleet_id,
            version: self.version,
            kind: Some(kind),
        }
        .encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a meta entry, names no kind, or names a state or a
    /// placement no version knows.
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::meta_entry::Kind;
        let entry = wire::MetaEntry::decode(record).map_err(|_| "not a meta entry")?;
        let command = match entry.kind.ok_or("a meta entry names no kind")? {
            Kind::FormFleet(form) => MetaCommand::FormFleet {
                meta: frame(form.meta_tenant, form.meta_journal),
            },
            Kind::AddCell(add) => MetaCommand::AddCell {
                cell_id: add.cell_id,
                control: frame(add.control_tenant, add.control_journal),
            },
            Kind::MarkCell(mark) => MetaCommand::MarkCell {
                cell_id: mark.cell_id,
                state: CellState::from_wire(mark.state).ok_or("an unknown cell state")?,
            },
            Kind::RegisterTenant(register) => MetaCommand::RegisterTenant {
                control: frame(register.tenant, register.control_journal),
                name: register.name,
                cell_id: register.cell_id,
                placement: Placement::from_wire(register.placement)
                    .ok_or("an unknown placement")?,
            },
            Kind::MarkTenant(mark) => MetaCommand::MarkTenant {
                tenant: TenantId(mark.tenant),
                state: TenantState::from_wire(mark.state).ok_or("an unknown tenant state")?,
            },
            Kind::RemoveTenant(remove) => MetaCommand::RemoveTenant {
                tenant: TenantId(remove.tenant),
            },
        };
        Ok(Self {
            fleet_id: entry.fleet_id,
            version: entry.version,
            command,
        })
    }
}

/// A cell entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellEntry {
    /// Where it stands.
    pub state: CellState,
    /// The metadata version it was added under.
    pub version: u32,
    /// Its cell tenant (an `internal`, pinned tenant entry).
    pub control_tenant: TenantId,
}

/// A tenant entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantEntry {
    /// Its name (opaque bytes; empty for an `internal` tenant, which is
    /// found by its frame, never by name).
    pub name: Vec<u8>,
    /// The cell it lives in (`0` for meta until its hosting cell is added).
    pub cell_id: u64,
    /// Its control journal's id, inside the tenant.
    pub control: JournalId,
    /// Where it stands.
    pub state: TenantState,
    /// Its configuration sequence number: moved each time it enters
    /// `UPDATING_CONFIGURATION`.
    pub config_seq: u64,
    /// Its group.
    pub group: Group,
    /// Whether it may move.
    pub placement: Placement,
}

/// What one meta record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaEvent {
    /// Meta recorded its fleet, and itself as its first tenant.
    FleetFormed {
        /// The fleet's id.
        fleet_id: u64,
    },
    /// A cell joined the directory, `REGISTERING`, with its cell tenant.
    CellAdded {
        /// The cell.
        cell_id: u64,
    },
    /// A cell moved.
    CellMarked {
        /// The cell.
        cell_id: u64,
        /// Its new state.
        state: CellState,
    },
    /// A `users` tenant was registered, `REGISTERING`.
    TenantRegistered {
        /// The tenant.
        tenant: TenantId,
        /// Its cell.
        cell_id: u64,
    },
    /// A tenant moved.
    TenantMarked {
        /// The tenant.
        tenant: TenantId,
        /// Its new state.
        state: TenantState,
    },
    /// A tenant left the directory.
    TenantRemoved {
        /// The tenant.
        tenant: TenantId,
    },
    /// The entry asked for what the directory already holds: a re-run step.
    Unchanged,
    /// A checkpoint (#227): every position below `covers_up_to` is in the
    /// state this fold now holds.
    Checkpoint {
        /// The checkpoint's horizon, its own position.
        covers_up_to: u64,
    },
    /// The entry changed nothing.
    Refused(MetaRefusal),
}

/// Why a meta entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaRefusal {
    /// Not one decodable meta entry (or a checkpoint this fold cannot use),
    /// or an unset id or frame where one must be named.
    Malformed,
    /// Written at a metadata version this fold does not understand.
    UnknownVersion {
        /// The entry's version.
        version: u32,
    },
    /// No fleet is formed yet: only `FormFleet` may come first.
    NoFleet,
    /// The entry names another fleet than the one meta formed (or another
    /// frame for meta).
    OtherFleet {
        /// The fleet the entry names.
        fleet_id: u64,
    },
    /// A cell the directory does not hold.
    UnknownCell {
        /// The cell named.
        cell_id: u64,
    },
    /// The cell is in the directory under another cell tenant.
    OtherCellTenant {
        /// The cell named.
        cell_id: u64,
    },
    /// A cell transition [`CellState::may_become`] forbids.
    CellTransition {
        /// The cell's state.
        from: CellState,
        /// The state asked for.
        to: CellState,
    },
    /// A tenant registered to a cell that is not `READY`.
    CellNotReady {
        /// The cell named.
        cell_id: u64,
    },
    /// The id names another tenant entry — meta, a cell tenant or a user
    /// tenant — or one removed (ids are never reused): the creator redraws.
    TenantIdTaken {
        /// The id asked for.
        tenant: TenantId,
    },
    /// A live tenant already holds the name.
    NameTaken {
        /// The tenant holding it.
        holder: TenantId,
    },
    /// A tenant the directory does not hold.
    UnknownTenant {
        /// The tenant named.
        tenant: TenantId,
    },
    /// An `internal` tenant is moved or removed only by the fleet
    /// operations that created it, never through the tenant API.
    Internal {
        /// The tenant named.
        tenant: TenantId,
    },
    /// A tenant transition [`TenantState::may_become`] forbids (a removal
    /// of a tenant that is not `REMOVING` included).
    TenantTransition {
        /// The tenant's state.
        from: TenantState,
        /// The state asked for (`REMOVING` for a removal).
        to: TenantState,
    },
}

/// The fleet as meta recorded it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fleet {
    id: u64,
    version: u32,
    /// Meta's own control journal.
    meta: JournalKey,
}

/// Meta's fold: the fleet, its cells and its tenants.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    fleet: Option<Fleet>,
    cells: BTreeMap<u64, CellEntry>,
    tenants: BTreeMap<TenantId, TenantEntry>,
    /// Live `users` names, to the tenant holding each.
    names: BTreeMap<Vec<u8>, TenantId>,
    /// Ids removed from the directory: never reused.
    removed: BTreeSet<TenantId>,
    next_seq: u64,
}

impl Meta {
    /// The next position this fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The fleet's id, once formed.
    #[must_use]
    pub fn fleet(&self) -> Option<u64> {
        self.fleet.map(|fleet| fleet.id)
    }

    /// Meta's own control journal, once the fleet is formed.
    #[must_use]
    pub fn meta(&self) -> Option<JournalKey> {
        self.fleet.map(|fleet| fleet.meta)
    }

    /// The cell entry `cell_id`.
    #[must_use]
    pub fn cell(&self, cell_id: u64) -> Option<CellEntry> {
        self.cells.get(&cell_id).copied()
    }

    /// Every cell entry, in id order.
    pub fn cells(&self) -> impl Iterator<Item = (u64, CellEntry)> + '_ {
        self.cells.iter().map(|(id, c)| (*id, *c))
    }

    /// The tenant entry `tenant`.
    #[must_use]
    pub fn tenant(&self, tenant: TenantId) -> Option<&TenantEntry> {
        self.tenants.get(&tenant)
    }

    /// The `users` tenant holding `name`.
    #[must_use]
    pub fn named(&self, name: &[u8]) -> Option<(TenantId, &TenantEntry)> {
        let id = *self.names.get(name)?;
        self.tenants.get(&id).map(|t| (id, t))
    }

    /// Every tenant entry, in id order.
    pub fn tenants(&self) -> impl Iterator<Item = (TenantId, &TenantEntry)> {
        self.tenants.iter().map(|(id, t)| (*id, t))
    }

    /// Whether `tenant` was removed from the directory.
    #[must_use]
    pub fn is_removed(&self, tenant: TenantId) -> bool {
        self.removed.contains(&tenant)
    }

    /// Whether `tenant` is held by an entry or was ever removed.
    fn taken(&self, tenant: TenantId) -> bool {
        self.tenants.contains_key(&tenant) || self.removed.contains(&tenant)
    }

    /// Fold the record at position `seq` of meta (in position order; a gap
    /// is simply skipped). A checkpoint record is not an entry: fold through
    /// a [`Folder`](crate::client::checkpoint::Folder).
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (a programmer error of
    /// the caller).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> MetaEvent {
        assert!(seq >= self.next_seq, "meta folds in position order");
        self.next_seq = seq + 1;
        let Ok(entry) = MetaEntry::decode(record) else {
            return MetaEvent::Refused(MetaRefusal::Malformed);
        };
        match self.judge(&entry) {
            Ok(event) => event,
            Err(refusal) => MetaEvent::Refused(refusal),
        }
    }

    /// The operation context (§3.7): the entry's version is one this fold
    /// speaks, and it names the fleet meta formed.
    fn judge(&mut self, entry: &MetaEntry) -> Result<MetaEvent, MetaRefusal> {
        if entry.fleet_id == 0 || entry.version == 0 {
            return Err(MetaRefusal::Malformed);
        }
        if entry.version > METADATA_VERSION {
            return Err(MetaRefusal::UnknownVersion {
                version: entry.version,
            });
        }
        if let MetaCommand::FormFleet { meta } = entry.command {
            return self.form_fleet(entry.fleet_id, entry.version, meta);
        }
        match self.fleet {
            None => return Err(MetaRefusal::NoFleet),
            Some(fleet) if fleet.id != entry.fleet_id => {
                return Err(MetaRefusal::OtherFleet {
                    fleet_id: entry.fleet_id,
                });
            }
            Some(_) => {}
        }
        match &entry.command {
            MetaCommand::FormFleet { .. } => unreachable!("judged above"),
            MetaCommand::AddCell { cell_id, control } => {
                self.add_cell(*cell_id, *control, entry.version)
            }
            MetaCommand::MarkCell { cell_id, state } => self.mark_cell(*cell_id, *state),
            MetaCommand::RegisterTenant {
                control,
                name,
                cell_id,
                placement,
            } => self.register_tenant(*control, name, *cell_id, *placement),
            MetaCommand::MarkTenant { tenant, state } => self.mark_tenant(*tenant, *state),
            MetaCommand::RemoveTenant { tenant } => self.remove_tenant(*tenant),
        }
    }

    fn form_fleet(
        &mut self,
        fleet_id: u64,
        version: u32,
        meta: JournalKey,
    ) -> Result<MetaEvent, MetaRefusal> {
        if !meta.is_set() {
            return Err(MetaRefusal::Malformed);
        }
        match self.fleet {
            None => {
                self.fleet = Some(Fleet {
                    id: fleet_id,
                    version,
                    meta,
                });
                self.tenants.insert(
                    meta.tenant,
                    TenantEntry {
                        name: Vec::new(),
                        cell_id: 0,
                        control: meta.journal,
                        state: TenantState::Ready,
                        config_seq: 0,
                        group: Group::Internal,
                        placement: Placement::Movable,
                    },
                );
                Ok(MetaEvent::FleetFormed { fleet_id })
            }
            Some(fleet) if fleet.id == fleet_id && fleet.meta == meta => Ok(MetaEvent::Unchanged),
            Some(_) => Err(MetaRefusal::OtherFleet { fleet_id }),
        }
    }

    fn add_cell(
        &mut self,
        cell_id: u64,
        control: JournalKey,
        version: u32,
    ) -> Result<MetaEvent, MetaRefusal> {
        if cell_id == 0 || !control.is_set() {
            return Err(MetaRefusal::Malformed);
        }
        if let Some(cell) = self.cells.get(&cell_id) {
            let same = cell.control_tenant == control.tenant
                && self
                    .tenants
                    .get(&control.tenant)
                    .is_some_and(|t| t.control == control.journal);
            return if same {
                Ok(MetaEvent::Unchanged)
            } else {
                Err(MetaRefusal::OtherCellTenant { cell_id })
            };
        }
        if self.taken(control.tenant) {
            return Err(MetaRefusal::TenantIdTaken {
                tenant: control.tenant,
            });
        }
        self.cells.insert(
            cell_id,
            CellEntry {
                state: CellState::Registering,
                version,
                control_tenant: control.tenant,
            },
        );
        self.tenants.insert(
            control.tenant,
            TenantEntry {
                name: Vec::new(),
                cell_id,
                control: control.journal,
                state: TenantState::Ready,
                config_seq: 0,
                group: Group::Internal,
                placement: Placement::Pinned,
            },
        );
        // The fleet's first cell hosts meta (#226).
        if let Some(meta) = self.fleet.map(|fleet| fleet.meta.tenant)
            && let Some(entry) = self.tenants.get_mut(&meta)
            && entry.cell_id == 0
        {
            entry.cell_id = cell_id;
        }
        Ok(MetaEvent::CellAdded { cell_id })
    }

    fn mark_cell(&mut self, cell_id: u64, state: CellState) -> Result<MetaEvent, MetaRefusal> {
        let cell = self
            .cells
            .get_mut(&cell_id)
            .ok_or(MetaRefusal::UnknownCell { cell_id })?;
        if cell.state == state {
            return Ok(MetaEvent::Unchanged);
        }
        if !cell.state.may_become(state) {
            return Err(MetaRefusal::CellTransition {
                from: cell.state,
                to: state,
            });
        }
        cell.state = state;
        Ok(MetaEvent::CellMarked { cell_id, state })
    }

    fn register_tenant(
        &mut self,
        control: JournalKey,
        name: &[u8],
        cell_id: u64,
        placement: Placement,
    ) -> Result<MetaEvent, MetaRefusal> {
        let tenant = control.tenant;
        if !control.is_set() {
            return Err(MetaRefusal::Malformed);
        }
        if let Some(existing) = self.tenants.get(&tenant) {
            // The same registration again: a re-run of the first step.
            let same = existing.group == Group::Users
                && existing.name == name
                && existing.cell_id == cell_id
                && existing.control == control.journal
                && existing.placement == placement;
            return if same {
                Ok(MetaEvent::Unchanged)
            } else {
                Err(MetaRefusal::TenantIdTaken { tenant })
            };
        }
        if self.removed.contains(&tenant) {
            return Err(MetaRefusal::TenantIdTaken { tenant });
        }
        if let Some(&holder) = self.names.get(name) {
            return Err(MetaRefusal::NameTaken { holder });
        }
        if self.cells.get(&cell_id).map(|c| c.state) != Some(CellState::Ready) {
            return Err(MetaRefusal::CellNotReady { cell_id });
        }
        self.names.insert(name.to_vec(), tenant);
        self.tenants.insert(
            tenant,
            TenantEntry {
                name: name.to_vec(),
                cell_id,
                control: control.journal,
                state: TenantState::Registering,
                config_seq: 0,
                group: Group::Users,
                placement,
            },
        );
        Ok(MetaEvent::TenantRegistered { tenant, cell_id })
    }

    fn mark_tenant(
        &mut self,
        tenant: TenantId,
        state: TenantState,
    ) -> Result<MetaEvent, MetaRefusal> {
        let entry = self
            .tenants
            .get_mut(&tenant)
            .ok_or(MetaRefusal::UnknownTenant { tenant })?;
        if entry.group == Group::Internal {
            return Err(MetaRefusal::Internal { tenant });
        }
        if entry.state == state {
            return Ok(MetaEvent::Unchanged);
        }
        if !entry.state.may_become(state) {
            return Err(MetaRefusal::TenantTransition {
                from: entry.state,
                to: state,
            });
        }
        if state == TenantState::UpdatingConfiguration {
            entry.config_seq += 1;
        }
        entry.state = state;
        Ok(MetaEvent::TenantMarked { tenant, state })
    }

    fn remove_tenant(&mut self, tenant: TenantId) -> Result<MetaEvent, MetaRefusal> {
        if self.removed.contains(&tenant) {
            return Ok(MetaEvent::Unchanged);
        }
        let entry = self
            .tenants
            .get(&tenant)
            .ok_or(MetaRefusal::UnknownTenant { tenant })?;
        if entry.group == Group::Internal {
            return Err(MetaRefusal::Internal { tenant });
        }
        if entry.state != TenantState::Removing {
            return Err(MetaRefusal::TenantTransition {
                from: entry.state,
                to: TenantState::Removing,
            });
        }
        let name = entry.name.clone();
        self.tenants.remove(&tenant);
        self.names.remove(&name);
        self.removed.insert(tenant);
        Ok(MetaEvent::TenantRemoved { tenant })
    }

    fn state_to_wire(&self) -> wire::MetaState {
        let fleet = self.fleet;
        wire::MetaState {
            fleet_id: fleet.map_or(0, |f| f.id),
            version: fleet.map_or(0, |f| f.version),
            meta_tenant: fleet.map_or(0, |f| f.meta.tenant.0),
            meta_journal: fleet.map_or(0, |f| f.meta.journal.0),
            cells: self
                .cells
                .iter()
                .map(|(id, c)| wire::CellEntry {
                    cell_id: *id,
                    state: c.state.to_wire(),
                    version: c.version,
                    control_tenant: c.control_tenant.0,
                })
                .collect(),
            tenants: self
                .tenants
                .iter()
                .map(|(id, t)| wire::TenantEntry {
                    tenant: id.0,
                    name: t.name.clone(),
                    cell_id: t.cell_id,
                    state: t.state.to_wire(),
                    config_seq: t.config_seq,
                    control_journal: t.control.0,
                    group: t.group.to_wire(),
                    placement: t.placement.to_wire(),
                })
                .collect(),
            removed: self.removed.iter().map(|id| id.0).collect(),
        }
    }
}

impl Checkpointable for Meta {
    type Event = MetaEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> MetaEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state = wire::MetaState::decode(state).map_err(|_| "a meta state does not decode")?;
        if state.version > METADATA_VERSION {
            return Err("a meta state of an unknown metadata version");
        }
        let fleet = match state.fleet_id {
            0 => None,
            id => Some(Fleet {
                id,
                version: state.version,
                meta: frame(state.meta_tenant, state.meta_journal),
            }),
        };
        if fleet.is_some_and(|f| !f.meta.is_set()) {
            return Err("a meta state names no frame for meta");
        }
        let mut cells = BTreeMap::new();
        for c in state.cells {
            let cell_state = CellState::from_wire(c.state).ok_or("an unknown cell state")?;
            cells.insert(
                c.cell_id,
                CellEntry {
                    state: cell_state,
                    version: c.version,
                    control_tenant: TenantId(c.control_tenant),
                },
            );
        }
        let mut tenants = BTreeMap::new();
        let mut names = BTreeMap::new();
        for t in state.tenants {
            let id = TenantId(t.tenant);
            let group = Group::from_wire(t.group).ok_or("an unknown tenant group")?;
            if !id.is_set() || t.control_journal == 0 {
                return Err("a meta state names an unset tenant frame");
            }
            if group == Group::Users && names.insert(t.name.clone(), id).is_some() {
                return Err("a meta state names one tenant name twice");
            }
            tenants.insert(
                id,
                TenantEntry {
                    name: t.name,
                    cell_id: t.cell_id,
                    control: JournalId(t.control_journal),
                    state: TenantState::from_wire(t.state).ok_or("an unknown tenant state")?,
                    config_seq: t.config_seq,
                    group,
                    placement: Placement::from_wire(t.placement).ok_or("an unknown placement")?,
                },
            );
        }
        let removed: BTreeSet<TenantId> = state.removed.into_iter().map(TenantId).collect();
        if removed.iter().any(|id| tenants.contains_key(id)) {
            return Err("a meta state holds a removed tenant");
        }
        self.fleet = fleet;
        self.cells = cells;
        self.tenants = tenants;
        self.names = names;
        self.removed = removed;
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The event a [`Folded`] meta record is reported as (see
/// [`crate::system::registry_event`]). `None` for a record the fold skipped
/// or is waiting on.
#[must_use]
pub fn meta_event(folded: Folded<MetaEvent>) -> Option<MetaEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(MetaEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(MetaEvent::Refused(MetaRefusal::Malformed)),
        Folded::NeedsRef(_) | Folded::Skipped => None,
    }
}

#[cfg(test)]
mod tests;
