//! The **fleet tenant** (#229, `docs/architecture.md` §3.7): the fleet
//! directory. Its control journal answers one question — which tenant lives
//! in which cell — plus the cell entries; quotas, billing and status live
//! elsewhere. Its identifier is random like every identifier (§3.8): `init`
//! draws it and records it in the cell plan, and the fleet tenant records it
//! in its own first entry.
//!
//! This module is the one reading of the fleet tenant's entries: the typed
//! [`FleetEntry`] a fleet operation writes (one record per position) and the
//! pure fold [`FleetDirectory`] every reader runs over them in position order. Like the
//! system journals' folds ([`crate::system`]) it is a function of the log
//! alone, so every reader that folded a prefix agrees on it; and like them it
//! is not an application (#186): the core keeps the records opaque.
//!
//! - **Every entry names its fleet and its metadata version.** The first
//!   entry, [`FleetCommand::FormFleet`], records the fleet's id once; any later
//!   entry naming another fleet is refused ([`FleetDirectoryRefusal::OtherFleet`]), so
//!   a step of a fleet operation that talks to another fleet than its
//!   previous step did changes nothing (FDB's `MetaclusterOperationContext`).
//!   An entry whose version this fold does not understand is refused
//!   ([`FleetDirectoryRefusal::UnknownVersion`]).
//! - **Every tenant belongs to a set of groups** ([`Groups`]), fixed when it
//!   is registered (§3.7, decided on 2026-10-04): the fleet tenant is `{internal, fleet}`
//!   (registered by `FormFleet`), each cell's cell tenant `{internal, cell}`
//!   (registered by `AddCell`), and a served tenant `{users}`.
//!   `RegisterTenant` — the tenant API's entry — cannot express another
//!   set, and an `internal` tenant is never marked or removed through it
//!   ([`FleetDirectoryRefusal::Internal`]). The groups alone decide whether a tenant
//!   may move ([`Groups::may_move`]): only `cell` forbids it. There is no
//!   per-tenant placement flag.
//! - **Every fleet operation is an idempotent state machine.** A tenant is
//!   written in `REGISTERING` with its cell assignment, created in the cell,
//!   then marked `READY` ([`crate::client::fleet`]); `init` takes its cell
//!   entry through the same `REGISTERING` → `READY`. An entry that asks for
//!   what the directory already holds folds to [`FleetEvent::Unchanged`], so
//!   re-running a step after a crash is harmless. The transitions are
//!   [`TenantState::may_become`] and [`CellState::may_become`].
//! - **No id is fixed** (§3.8). A tenant's id and its control journal's are
//!   random, drawn by its creator and checked here, the tenant ids' single
//!   writer: an id any entry holds — the fleet tenant's own and the cell tenants'
//!   included — or that was ever removed is taken
//!   ([`FleetDirectoryRefusal::TenantIdTaken`]), and the creator redraws; an unset id
//!   is malformed. A live name is held by one tenant at a time
//!   ([`FleetDirectoryRefusal::NameTaken`]).
//! - **Only a `READY` cell receives new tenants.**
//! - **The directory is a pointer, never the authority** (#225): a tenant's
//!   control journal's generation wins over it, and the directory is
//!   rebuilt from the cells' tenant lists when lost (the recovery issue,
//!   #231). The fleet directory is checkpointed with `paros::client::checkpoint` (#227),
//!   a run of small records (#353): [`FleetDirectory`] is [`Checkpointable`].
//!
//! Every malformed entry folds to a refusal, never a panic: the entries are
//! external input.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{JournalId, JournalIdentifier, TenantId};
use prost::Message as _;

use crate::client::checkpoint::{Checkpointable, Folded};
use crate::rpc::fleet as wire;
pub use crate::tenant::Survives;

/// The metadata version this fold speaks: written into every entry, and
/// into the cell's side of the registration (`crate::system`). `2` since
/// the tenant entry carries a set of groups and no placement (#243).
pub const METADATA_VERSION: u32 = 2;

/// One tenant group (§3.7): a rule every member obeys. The set of groups is
/// fixed by paros for now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Group {
    /// Created only by paros's own operations (`init`, adding a cell), never
    /// through the tenant API: the fleet tenant and every cell tenant.
    Internal,
    /// Never leaves its cell: it *is* its cell. Each cell's cell tenant.
    Cell,
    /// Holds the fleet directory and moves with its coordinator: the fleet tenant.
    Fleet,
    /// Created by the tenant API and served by its own proxies: every
    /// served tenant.
    Users,
}

impl Group {
    /// Every group, in label order.
    pub const ALL: [Group; 4] = [Group::Internal, Group::Cell, Group::Fleet, Group::Users];

    /// The group's bit in a [`Groups`] set (and on the wire).
    const fn bit(self) -> u32 {
        match self {
            Group::Internal => 1,
            Group::Cell => 2,
            Group::Fleet => 4,
            Group::Users => 8,
        }
    }

    /// The group's label (`internal`, `cell`, `fleet`, `users`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Group::Internal => "internal",
            Group::Cell => "cell",
            Group::Fleet => "fleet",
            Group::Users => "users",
        }
    }
}

/// The **set of groups** a tenant belongs to (§3.7, decided on 2026-10-04):
/// recorded when the tenant is registered and never changed. A tenant obeys
/// the rule of every group it is in. Only three sets exist —
/// [`Groups::FLEET_TENANT`], [`Groups::CELL_TENANT`] and
/// [`Groups::SERVED`] — and the wire refuses any other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Groups(u32);

impl Groups {
    /// The fleet tenant: `{internal, fleet}`.
    pub const FLEET_TENANT: Self = Self(Group::Internal.bit() | Group::Fleet.bit());
    /// A cell tenant: `{internal, cell}`.
    pub const CELL_TENANT: Self = Self(Group::Internal.bit() | Group::Cell.bit());
    /// A served tenant: `{users}`.
    pub const SERVED: Self = Self(Group::Users.bit());

    /// Whether the set holds `group`.
    #[must_use]
    pub fn contains(self, group: Group) -> bool {
        self.0 & group.bit() != 0
    }

    /// Whether a tenant with these groups may move to another cell (M12):
    /// it moves unless one of its groups forbids it, and only `cell` does.
    #[must_use]
    pub fn may_move(self) -> bool {
        !self.contains(Group::Cell)
    }

    /// The groups, in label order.
    pub fn iter(self) -> impl Iterator<Item = Group> {
        Group::ALL
            .into_iter()
            .filter(move |group| self.contains(*group))
    }

    /// The set's label, its groups comma-separated (`internal,fleet`).
    #[must_use]
    pub fn label(self) -> String {
        self.iter().map(Group::as_str).collect::<Vec<_>>().join(",")
    }

    fn to_wire(self) -> u32 {
        self.0
    }

    /// One of the three sets paros defines; `None` for any other bits.
    fn from_wire(bits: u32) -> Option<Self> {
        [Self::FLEET_TENANT, Self::CELL_TENANT, Self::SERVED]
            .into_iter()
            .find(|groups| groups.0 == bits)
    }
}

const _: () = assert!(Groups::FLEET_TENANT.0 & Groups::CELL_TENANT.0 == Group::Internal.bit());
const _: () = assert!(Groups::SERVED.0 & (Groups::FLEET_TENANT.0 | Groups::CELL_TENANT.0) == 0);

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
    /// `REGISTERING` (only [`FleetCommand::AddCell`] writes that); it becomes
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
            // 4 was RENAMING, dropped from §3.7 on 2026-10-04 (#243).
            TenantState::Error => 5,
        }
    }

    fn from_wire(state: u32) -> Option<Self> {
        Some(match state {
            0 => TenantState::Registering,
            1 => TenantState::Ready,
            2 => TenantState::Removing,
            3 => TenantState::UpdatingConfiguration,
            5 => TenantState::Error,
            _ => return None,
        })
    }

    /// Whether a tenant in `self` may be marked `to` (§3.7). A tenant is
    /// never marked `REGISTERING` (only [`FleetCommand::RegisterTenant`]
    /// writes that). Any tenant may be removed; only a `READY` one is
    /// reconfigured; an operation's end returns it to `READY`;
    /// a tenant being removed takes no other state.
    #[must_use]
    pub fn may_become(self, to: TenantState) -> bool {
        match to {
            TenantState::Registering => false,
            TenantState::Removing => true,
            TenantState::Ready => matches!(
                self,
                TenantState::Registering | TenantState::UpdatingConfiguration
            ),
            TenantState::UpdatingConfiguration => self == TenantState::Ready,
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

/// What one fleet entry asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetCommand {
    /// `init`: record the entry's fleet as the fleet tenant's, once, and
    /// register the fleet tenant itself (`{internal, fleet}`) under its own
    /// identifier.
    FormFleet {
        /// The fleet tenant's control journal: its tenant is the fleet
        /// tenant's id.
        control: JournalIdentifier,
    },
    /// `init` (and adding a cell, M12): cell `cell_id` joins the directory,
    /// `REGISTERING`, and its cell tenant is registered (`{internal,
    /// cell}`) under the identifier of its control journal.
    AddCell {
        /// The cell's id (random, minted at `init`).
        cell_id: u64,
        /// The cell tenant's control journal.
        control: JournalIdentifier,
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
        control: JournalIdentifier,
        /// Opaque bytes; paros never interprets them.
        name: Vec<u8>,
        /// The cell it is assigned to.
        cell_id: u64,
        /// What it survives (#252).
        survives: Survives,
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

/// One fleet entry: a command, stamped with the fleet it was written for and
/// the metadata version its writer speaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetEntry {
    /// The fleet the writer believes it talks to.
    pub fleet_id: u64,
    /// The writer's metadata version.
    pub version: u32,
    /// What it asks for.
    pub command: FleetCommand,
}

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

impl FleetEntry {
    /// `command` for fleet `fleet_id` at this fold's [`METADATA_VERSION`].
    #[must_use]
    pub fn new(fleet_id: u64, command: FleetCommand) -> Self {
        Self {
            fleet_id,
            version: METADATA_VERSION,
            command,
        }
    }

    /// The record a fleet operation writes: exactly one per position.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::fleet_entry::Kind;
        let kind = match &self.command {
            FleetCommand::FormFleet { control } => Kind::FormFleet(wire::FormFleet {
                fleet_tenant: control.tenant.0,
                fleet_journal: control.journal.0,
            }),
            FleetCommand::AddCell { cell_id, control } => Kind::AddCell(wire::AddCell {
                cell_id: *cell_id,
                control_tenant: control.tenant.0,
                control_journal: control.journal.0,
            }),
            FleetCommand::MarkCell { cell_id, state } => Kind::MarkCell(wire::MarkCell {
                cell_id: *cell_id,
                state: state.to_wire(),
            }),
            FleetCommand::RegisterTenant {
                control,
                name,
                cell_id,
                survives,
            } => Kind::RegisterTenant(wire::RegisterTenant {
                tenant: control.tenant.0,
                name: name.clone(),
                cell_id: *cell_id,
                control_journal: control.journal.0,
                survives: survives.to_wire(),
            }),
            FleetCommand::MarkTenant { tenant, state } => Kind::MarkTenant(wire::MarkTenant {
                tenant: tenant.0,
                state: state.to_wire(),
            }),
            FleetCommand::RemoveTenant { tenant } => {
                Kind::RemoveTenant(wire::RemoveTenant { tenant: tenant.0 })
            }
        };
        wire::FleetEntry {
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
    /// The record is not a fleet entry, names no kind, or names a state no
    /// version knows.
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::fleet_entry::Kind;
        let entry = wire::FleetEntry::decode(record).map_err(|_| "not a fleet entry")?;
        let command = match entry.kind.ok_or("a fleet entry names no kind")? {
            Kind::FormFleet(form) => FleetCommand::FormFleet {
                control: identifier(form.fleet_tenant, form.fleet_journal),
            },
            Kind::AddCell(add) => FleetCommand::AddCell {
                cell_id: add.cell_id,
                control: identifier(add.control_tenant, add.control_journal),
            },
            Kind::MarkCell(mark) => FleetCommand::MarkCell {
                cell_id: mark.cell_id,
                state: CellState::from_wire(mark.state).ok_or("an unknown cell state")?,
            },
            Kind::RegisterTenant(register) => FleetCommand::RegisterTenant {
                control: identifier(register.tenant, register.control_journal),
                name: register.name,
                cell_id: register.cell_id,
                survives: Survives::from_wire(register.survives)?,
            },
            Kind::MarkTenant(mark) => FleetCommand::MarkTenant {
                tenant: TenantId(mark.tenant),
                state: TenantState::from_wire(mark.state).ok_or("an unknown tenant state")?,
            },
            Kind::RemoveTenant(remove) => FleetCommand::RemoveTenant {
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
    /// Its cell tenant (a `{internal, cell}` tenant entry).
    pub control_tenant: TenantId,
}

/// How a person reads a tenant ([`FleetDirectory::label`], #239).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantLabel<'a> {
    /// A `users` tenant: its name.
    Named(&'a [u8]),
    /// The fleet tenant (internal: it has no name).
    Fleet,
    /// The cell tenant of this cell (internal: it has no name).
    Cell(u64),
}

/// A tenant entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantEntry {
    /// Its name (opaque bytes; empty for an `internal` tenant, which is
    /// found by its identifier, never by name).
    pub name: Vec<u8>,
    /// The cell it lives in (`0` for the fleet tenant until its hosting cell is added).
    pub cell_id: u64,
    /// Its control journal's id, inside the tenant.
    pub control: JournalId,
    /// Where it stands.
    pub state: TenantState,
    /// Its configuration sequence number: moved each time it enters
    /// `UPDATING_CONFIGURATION`.
    pub config_seq: u64,
    /// Its groups, fixed at registration.
    pub groups: Groups,
    /// What it survives (#252), recorded at `REGISTERING` and mirrored into
    /// its control journal (#210). An `internal` tenant survives a zone.
    pub survives: Survives,
}

/// What one fleet record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetEvent {
    /// The fleet tenant recorded its fleet, and itself as its first tenant.
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
        /// The checkpoint's horizon, the position of its run's `Begin`.
        covers_up_to: u64,
    },
    /// The entry changed nothing.
    Refused(FleetDirectoryRefusal),
}

/// Why a fleet entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetDirectoryRefusal {
    /// Not one decodable fleet entry (or a checkpoint this fold cannot use),
    /// or an unset id or identifier where one must be named.
    Malformed,
    /// Written at a metadata version this fold does not understand.
    UnknownVersion {
        /// The entry's version.
        version: u32,
    },
    /// No fleet is formed yet: only `FormFleet` may come first.
    NoFleet,
    /// The entry names another fleet than the one the fleet tenant formed (or
    /// another control journal for the fleet tenant).
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
    /// The id names another tenant entry — the fleet tenant, a cell tenant or a user
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

/// The fleet as the fleet tenant recorded it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Formed {
    id: u64,
    version: u32,
    /// The fleet tenant's own control journal.
    control: JournalIdentifier,
}

/// The fleet directory's fold: the fleet, its cells and its tenants.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetDirectory {
    fleet: Option<Formed>,
    cells: BTreeMap<u64, CellEntry>,
    tenants: BTreeMap<TenantId, TenantEntry>,
    /// Live `users` names, to the tenant holding each.
    names: BTreeMap<Vec<u8>, TenantId>,
    /// Ids removed from the directory: never reused.
    removed: BTreeSet<TenantId>,
    next_seq: u64,
}

impl FleetDirectory {
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

    /// The fleet tenant's own control journal, once the fleet is formed.
    #[must_use]
    pub fn control(&self) -> Option<JournalIdentifier> {
        self.fleet.map(|fleet| fleet.control)
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

    /// How a person reads `tenant` (#239): a `users` tenant by its name, an
    /// `internal` one by the group that makes it internal, `fleet` or `cell`
    /// with its cell. A label is display only: never a name a caller can
    /// resolve, and never an id (§3.8: no well-known names).
    ///
    /// # Panics
    ///
    /// If a tenant's groups are none of the three sets the wire admits.
    #[must_use]
    pub fn label(&self, tenant: TenantId) -> Option<TenantLabel<'_>> {
        let entry = self.tenants.get(&tenant)?;
        if !entry.groups.contains(Group::Internal) {
            assert!(entry.groups.contains(Group::Users));
            return Some(TenantLabel::Named(&entry.name));
        }
        if entry.groups.contains(Group::Fleet) {
            return Some(TenantLabel::Fleet);
        }
        assert!(
            entry.groups.contains(Group::Cell),
            "an internal tenant is the fleet's or a cell's"
        );
        let cell = self
            .cells
            .iter()
            .find(|(_, cell)| cell.control_tenant == tenant)
            .map_or(entry.cell_id, |(id, _)| *id);
        Some(TenantLabel::Cell(cell))
    }

    /// Whether `tenant` is held by an entry or was ever removed.
    fn taken(&self, tenant: TenantId) -> bool {
        self.tenants.contains_key(&tenant) || self.removed.contains(&tenant)
    }

    /// Fold the record at position `seq` of the fleet tenant's journal (in position order; a gap
    /// is simply skipped). A checkpoint record is not an entry: fold through
    /// a [`Folder`](crate::client::checkpoint::Folder).
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (a programmer error of
    /// the caller).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> FleetEvent {
        assert!(
            seq >= self.next_seq,
            "the fleet directory folds in position order"
        );
        self.next_seq = seq + 1;
        let Ok(entry) = FleetEntry::decode(record) else {
            return FleetEvent::Refused(FleetDirectoryRefusal::Malformed);
        };
        match self.judge(&entry) {
            Ok(event) => event,
            Err(refusal) => FleetEvent::Refused(refusal),
        }
    }

    /// The operation context (§3.7): the entry's version is one this fold
    /// speaks, and it names the fleet the fleet tenant formed.
    fn judge(&mut self, entry: &FleetEntry) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if entry.fleet_id == 0 || entry.version == 0 {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if entry.version > METADATA_VERSION {
            return Err(FleetDirectoryRefusal::UnknownVersion {
                version: entry.version,
            });
        }
        if let FleetCommand::FormFleet { control } = entry.command {
            return self.form_fleet(entry.fleet_id, entry.version, control);
        }
        match self.fleet {
            None => return Err(FleetDirectoryRefusal::NoFleet),
            Some(fleet) if fleet.id != entry.fleet_id => {
                return Err(FleetDirectoryRefusal::OtherFleet {
                    fleet_id: entry.fleet_id,
                });
            }
            Some(_) => {}
        }
        match &entry.command {
            FleetCommand::FormFleet { .. } => unreachable!("judged above"),
            FleetCommand::AddCell { cell_id, control } => {
                self.add_cell(*cell_id, *control, entry.version)
            }
            FleetCommand::MarkCell { cell_id, state } => self.mark_cell(*cell_id, *state),
            FleetCommand::RegisterTenant {
                control,
                name,
                cell_id,
                survives,
            } => self.register_tenant(*control, name, *cell_id, *survives),
            FleetCommand::MarkTenant { tenant, state } => self.mark_tenant(*tenant, *state),
            FleetCommand::RemoveTenant { tenant } => self.remove_tenant(*tenant),
        }
    }

    fn form_fleet(
        &mut self,
        fleet_id: u64,
        version: u32,
        control: JournalIdentifier,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if !control.is_set() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        match self.fleet {
            None => {
                self.fleet = Some(Formed {
                    id: fleet_id,
                    version,
                    control,
                });
                self.tenants.insert(
                    control.tenant,
                    TenantEntry {
                        name: Vec::new(),
                        cell_id: 0,
                        control: control.journal,
                        state: TenantState::Ready,
                        config_seq: 0,
                        groups: Groups::FLEET_TENANT,
                        survives: Survives::Az,
                    },
                );
                Ok(FleetEvent::FleetFormed { fleet_id })
            }
            Some(fleet) if fleet.id == fleet_id && fleet.control == control => {
                Ok(FleetEvent::Unchanged)
            }
            Some(_) => Err(FleetDirectoryRefusal::OtherFleet { fleet_id }),
        }
    }

    fn add_cell(
        &mut self,
        cell_id: u64,
        control: JournalIdentifier,
        version: u32,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if cell_id == 0 || !control.is_set() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if let Some(cell) = self.cells.get(&cell_id) {
            let same = cell.control_tenant == control.tenant
                && self
                    .tenants
                    .get(&control.tenant)
                    .is_some_and(|t| t.control == control.journal);
            return if same {
                Ok(FleetEvent::Unchanged)
            } else {
                Err(FleetDirectoryRefusal::OtherCellTenant { cell_id })
            };
        }
        if self.taken(control.tenant) {
            return Err(FleetDirectoryRefusal::TenantIdTaken {
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
                groups: Groups::CELL_TENANT,
                survives: Survives::Az,
            },
        );
        // The fleet's first cell hosts the fleet tenant (#226).
        if let Some(fleet_tenant) = self.fleet.map(|fleet| fleet.control.tenant)
            && let Some(entry) = self.tenants.get_mut(&fleet_tenant)
            && entry.cell_id == 0
        {
            entry.cell_id = cell_id;
        }
        Ok(FleetEvent::CellAdded { cell_id })
    }

    fn mark_cell(
        &mut self,
        cell_id: u64,
        state: CellState,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        let cell = self
            .cells
            .get_mut(&cell_id)
            .ok_or(FleetDirectoryRefusal::UnknownCell { cell_id })?;
        if cell.state == state {
            return Ok(FleetEvent::Unchanged);
        }
        if !cell.state.may_become(state) {
            return Err(FleetDirectoryRefusal::CellTransition {
                from: cell.state,
                to: state,
            });
        }
        cell.state = state;
        Ok(FleetEvent::CellMarked { cell_id, state })
    }

    fn register_tenant(
        &mut self,
        control: JournalIdentifier,
        name: &[u8],
        cell_id: u64,
        survives: Survives,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        let tenant = control.tenant;
        if !control.is_set() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if let Some(existing) = self.tenants.get(&tenant) {
            // The same registration again: a re-run of the first step.
            let same = existing.groups == Groups::SERVED
                && existing.name == name
                && existing.cell_id == cell_id
                && existing.control == control.journal
                && existing.survives == survives;
            return if same {
                Ok(FleetEvent::Unchanged)
            } else {
                Err(FleetDirectoryRefusal::TenantIdTaken { tenant })
            };
        }
        if self.removed.contains(&tenant) {
            return Err(FleetDirectoryRefusal::TenantIdTaken { tenant });
        }
        if let Some(&holder) = self.names.get(name) {
            return Err(FleetDirectoryRefusal::NameTaken { holder });
        }
        if self.cells.get(&cell_id).map(|c| c.state) != Some(CellState::Ready) {
            return Err(FleetDirectoryRefusal::CellNotReady { cell_id });
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
                groups: Groups::SERVED,
                survives,
            },
        );
        Ok(FleetEvent::TenantRegistered { tenant, cell_id })
    }

    fn mark_tenant(
        &mut self,
        tenant: TenantId,
        state: TenantState,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        let entry = self
            .tenants
            .get_mut(&tenant)
            .ok_or(FleetDirectoryRefusal::UnknownTenant { tenant })?;
        if entry.groups.contains(Group::Internal) {
            return Err(FleetDirectoryRefusal::Internal { tenant });
        }
        if entry.state == state {
            return Ok(FleetEvent::Unchanged);
        }
        if !entry.state.may_become(state) {
            return Err(FleetDirectoryRefusal::TenantTransition {
                from: entry.state,
                to: state,
            });
        }
        if state == TenantState::UpdatingConfiguration {
            entry.config_seq += 1;
        }
        entry.state = state;
        Ok(FleetEvent::TenantMarked { tenant, state })
    }

    fn remove_tenant(&mut self, tenant: TenantId) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if self.removed.contains(&tenant) {
            return Ok(FleetEvent::Unchanged);
        }
        let entry = self
            .tenants
            .get(&tenant)
            .ok_or(FleetDirectoryRefusal::UnknownTenant { tenant })?;
        if entry.groups.contains(Group::Internal) {
            return Err(FleetDirectoryRefusal::Internal { tenant });
        }
        if entry.state != TenantState::Removing {
            return Err(FleetDirectoryRefusal::TenantTransition {
                from: entry.state,
                to: TenantState::Removing,
            });
        }
        let name = entry.name.clone();
        self.tenants.remove(&tenant);
        self.names.remove(&name);
        self.removed.insert(tenant);
        Ok(FleetEvent::TenantRemoved { tenant })
    }

    fn state_to_wire(&self) -> wire::FleetDirectoryState {
        let fleet = self.fleet;
        wire::FleetDirectoryState {
            fleet_id: fleet.map_or(0, |f| f.id),
            version: fleet.map_or(0, |f| f.version),
            fleet_tenant: fleet.map_or(0, |f| f.control.tenant.0),
            fleet_journal: fleet.map_or(0, |f| f.control.journal.0),
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
                    groups: t.groups.to_wire(),
                    survives: t.survives.to_wire(),
                })
                .collect(),
            removed: self.removed.iter().map(|id| id.0).collect(),
        }
    }
}

impl Checkpointable for FleetDirectory {
    type Event = FleetEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> FleetEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state = wire::FleetDirectoryState::decode(state)
            .map_err(|_| "a fleet directory state does not decode")?;
        if state.version > METADATA_VERSION {
            return Err("a fleet directory state of an unknown metadata version");
        }
        let fleet = match state.fleet_id {
            0 => None,
            id => Some(Formed {
                id,
                version: state.version,
                control: identifier(state.fleet_tenant, state.fleet_journal),
            }),
        };
        if fleet.is_some_and(|f| !f.control.is_set()) {
            return Err("a fleet directory state names no fleet tenant");
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
            let groups = Groups::from_wire(t.groups).ok_or("an unknown set of tenant groups")?;
            if !id.is_set() || t.control_journal == 0 {
                return Err("a fleet directory state names an unset tenant identifier");
            }
            if groups.contains(Group::Users) && names.insert(t.name.clone(), id).is_some() {
                return Err("a fleet directory state names one tenant name twice");
            }
            tenants.insert(
                id,
                TenantEntry {
                    name: t.name,
                    cell_id: t.cell_id,
                    control: JournalId(t.control_journal),
                    state: TenantState::from_wire(t.state).ok_or("an unknown tenant state")?,
                    config_seq: t.config_seq,
                    groups,
                    survives: Survives::from_wire(t.survives)?,
                },
            );
        }
        let removed: BTreeSet<TenantId> = state.removed.into_iter().map(TenantId).collect();
        if removed.iter().any(|id| tenants.contains_key(id)) {
            return Err("a fleet directory state holds a removed tenant");
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

/// The event a [`Folded`] fleet record is reported as (see
/// [`crate::system::registry_event`]). `None` for a record the fold skipped
/// or a checkpoint run record.
#[must_use]
pub fn fleet_event(folded: Folded<FleetEvent>) -> Option<FleetEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(FleetEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(FleetEvent::Refused(FleetDirectoryRefusal::Malformed)),
        Folded::Run | Folded::Skipped => None,
    }
}

#[cfg(test)]
mod tests;
