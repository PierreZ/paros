//! The **fleet tenant** (#229, `docs/architecture/03-7-universe.md` §3.7): the fleet
//! directory. Its control journal answers one question — which tenant lives
//! in which cell — plus the cell entries; quotas, billing and status live
//! elsewhere. Its identifier is random like every identifier (§3.8): `init`
//! draws it and records it in the cell plan, and the fleet tenant records it
//! in its own first entry.
//!
//! This module is the one reading of the fleet tenant's entries: the typed
//! [`FleetEntry`] a fleet operation writes (one record per position) and the
//! pure fold [`FleetDirectory`] every reader runs over them in position order. Like the
//! system journals' folds ([`crate::registry`]) it is a function of the log
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
//! - **Every tenant has a kind, a reserved tag** ([`TenantKind`], #575
//!   (key:value tags)), fixed when it is registered (§3.7, decided on
//!   2026-10-04, as groups until #575): the universe tenant is
//!   `paros/kind:internal`, `paros/role:universe` (registered by
//!   `FormFleet`), each cell's cell tenant `paros/kind:internal`,
//!   `paros/role:cell` (registered by `AddCell`), and a served tenant
//!   `paros/kind:users`. `RegisterTenant` — the tenant API's entry — cannot
//!   express another kind, and an internal tenant is never marked or
//!   removed through it ([`FleetDirectoryRefusal::Internal`]). Whether a
//!   tenant may move is its reserved tag `paros/movable` ([`Movable`]): a
//!   cell tenant is always pinned. A user tenant's name never holds a `:`
//!   ([`FleetDirectoryRefusal::NameRefused`]): an internal tenant shows as
//!   `<role>:<name>`.
//! - **Tenants and cells carry user tags** (#575): `RegisterTenant` sets a
//!   tenant's first ones, `SetTenantTags` and `SetCellTags` replace them,
//!   each a request applied at most once under its idempotency id. A
//!   reserved key or a set over the cap is refused at apply.
//! - **Every fleet operation is an idempotent state machine.** A tenant is
//!   written in `REGISTERING` with its cell assignment, created in the cell,
//!   then marked `READY` ([`paros_client::fleet`]); `init` takes its cell
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

use crate::address::Address;
use crate::checkpoint::{Checkpointable, Folded};
use crate::tags::{Tag, TagError, Tags, UserTags, reserved};
pub use crate::tenant::Survives;
use paros_wire::fleet as wire;

mod keys;

pub use keys::RootKeyEntry;
use keys::{KeyChange, RootKeys};

/// The metadata version this fold speaks: written into every entry, and
/// into the cell's side of the registration (`crate::registry`). `2` since
/// the tenant entry carries a set of groups and no placement (#243); `3`
/// since the universe entry holds root public keys (#245 (Biscuit
/// tokens)); `4` since tenants and cells carry tags and the tenant
/// entry's groups became reserved tags (#575 (key:value tags)); `5` since a
/// cell entry carries its entry endpoint and a removed cell's id is
/// tombstoned (#232 (multiple cells)).
pub const METADATA_VERSION: u32 = 5;

/// What a tenant is (§3.7, #575 (key:value tags)): its reserved tags
/// `paros/kind` and `paros/role`, fixed when it is registered and never
/// changed. Only paros's own operations create an internal tenant; the
/// tenant API creates only a [`TenantKind::Users`] one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TenantKind {
    /// The universe tenant (`paros/kind:internal`, `paros/role:universe`):
    /// it holds the universe directory and moves with its coordinator.
    /// Registered by `FormFleet`.
    Universe,
    /// A cell's cell tenant (`paros/kind:internal`, `paros/role:cell`): it
    /// *is* its cell, so it is always pinned. Registered by `AddCell`.
    Cell,
    /// A tenant the tenant API created (`paros/kind:users`), served by its
    /// own proxies.
    Users,
}

impl TenantKind {
    /// Whether paros's own operations created it (`paros/kind:internal`):
    /// never moved, marked or removed through the tenant API, never
    /// resolved by name.
    #[must_use]
    pub fn is_internal(self) -> bool {
        self != TenantKind::Users
    }

    /// The kind's label: `universe`, `cell` or `users`. An internal
    /// tenant shows as `<label>:<name>` (#575).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TenantKind::Universe => "universe",
            TenantKind::Cell => "cell",
            TenantKind::Users => "users",
        }
    }

    /// The movability its kind imposes, if any: a cell tenant is pinned.
    #[must_use]
    pub fn forced(self) -> Option<Movable> {
        (self == TenantKind::Cell).then_some(Movable::Pinned)
    }

    /// Its reserved tags, with the movability a tenant of this kind is
    /// registered with (a view of a tenant the directory does not hold).
    #[must_use]
    pub fn default_tags(self) -> Tags {
        let movable = self.forced().unwrap_or(Movable::Movable);
        let mut tags = self.tags();
        tags.push(movable.tag());
        Tags::new().with(tags)
    }

    /// Its reserved tags.
    fn tags(self) -> Vec<Tag> {
        match self {
            TenantKind::Users => vec![Tag::reserved(reserved::KIND, "users")],
            internal => vec![
                Tag::reserved(reserved::KIND, "internal"),
                Tag::reserved(reserved::ROLE, internal.as_str()),
            ],
        }
    }

    /// Parse the kind from reserved tags, taking the keys it reads.
    fn take(held: &mut BTreeMap<String, String>) -> Result<Self, TagError> {
        let kind = held.remove(reserved::KIND);
        let role = held.remove(reserved::ROLE);
        match (kind.as_deref(), role.as_deref()) {
            (Some("users"), None) => Ok(TenantKind::Users),
            (Some("internal"), Some("universe")) => Ok(TenantKind::Universe),
            (Some("internal"), Some("cell")) => Ok(TenantKind::Cell),
            (_, Some(_)) => Err(TagError::UnknownReserved(reserved::ROLE.into())),
            _ => Err(TagError::UnknownReserved(reserved::KIND.into())),
        }
    }
}

/// Whether a tenant may move to another cell (M12, #575): its reserved tag
/// `paros/movable`. A cell tenant is always [`Movable::Pinned`]; every
/// other tenant is [`Movable::Movable`] until a command pins it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Movable {
    /// It may move (`paros/movable:true`).
    Movable,
    /// It never leaves its cell (`paros/movable:false`).
    Pinned,
}

impl Movable {
    fn tag(self) -> Tag {
        Tag::reserved(
            reserved::MOVABLE,
            match self {
                Movable::Movable => "true",
                Movable::Pinned => "false",
            },
        )
    }

    fn take(held: &mut BTreeMap<String, String>) -> Result<Self, TagError> {
        match held.remove(reserved::MOVABLE).as_deref() {
            Some("true") => Ok(Movable::Movable),
            Some("false") => Ok(Movable::Pinned),
            _ => Err(TagError::UnknownReserved(reserved::MOVABLE.into())),
        }
    }
}

/// The kind of a cell (#253, §3.7): its reserved tags (#575) `paros/kind`
/// and its regions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellKind {
    /// One region, across at least three zones (`paros/kind:regional`,
    /// `paros/region`).
    Regional {
        /// The region's label (empty while the cell has none: no
        /// `paros/region` tag).
        region: String,
    },
    /// Three regions, one of them the witness (M12;
    /// `paros/kind:multi-region`, `paros/regions`, `paros/witness`).
    MultiRegion {
        /// The regions.
        regions: Vec<String>,
        /// The witness region.
        witness: String,
    },
}

impl Default for CellKind {
    fn default() -> Self {
        CellKind::Regional {
            region: String::new(),
        }
    }
}

impl CellKind {
    /// Its reserved tags.
    ///
    /// # Panics
    ///
    /// If a region holds a character no tag value may hold (a programmer
    /// error: [`CellKind::from_tags`] refuses one).
    #[must_use]
    pub fn tags(&self) -> Vec<Tag> {
        match self {
            CellKind::Regional { region } => {
                let mut tags = vec![Tag::reserved(reserved::KIND, "regional")];
                if !region.is_empty() {
                    tags.push(Tag::reserved(reserved::REGION, region.clone()));
                }
                tags
            }
            CellKind::MultiRegion { regions, witness } => vec![
                Tag::reserved(reserved::KIND, "multi-region"),
                Tag::reserved(reserved::REGIONS, regions.join("+")),
                Tag::reserved(reserved::WITNESS, witness.clone()),
            ],
        }
    }

    /// Parse the kind from a cell's reserved tags.
    ///
    /// # Errors
    ///
    /// A key no cell carries, or a kind no cell has.
    pub fn from_tags(mut held: BTreeMap<String, String>) -> Result<Self, TagError> {
        let kind = match held.remove(reserved::KIND).as_deref() {
            Some("regional") => CellKind::Regional {
                region: held.remove(reserved::REGION).unwrap_or_default(),
            },
            Some("multi-region") => {
                let regions = held
                    .remove(reserved::REGIONS)
                    .ok_or_else(|| TagError::UnknownReserved(reserved::REGIONS.into()))?;
                let witness = held
                    .remove(reserved::WITNESS)
                    .ok_or_else(|| TagError::UnknownReserved(reserved::WITNESS.into()))?;
                CellKind::MultiRegion {
                    regions: regions.split('+').map(str::to_string).collect(),
                    witness,
                }
            }
            _ => return Err(TagError::UnknownReserved(reserved::KIND.into())),
        };
        match held.into_keys().next() {
            Some(key) => Err(TagError::UnknownReserved(key)),
            None => Ok(kind),
        }
    }

    /// The kind's label (`regional`, `multi-region`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CellKind::Regional { .. } => "regional",
            CellKind::MultiRegion { .. } => "multi-region",
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
    /// register the fleet tenant itself ([`TenantKind::Universe`]) under its
    /// own identifier.
    FormFleet {
        /// The fleet tenant's control journal: its tenant is the fleet
        /// tenant's id.
        control: JournalIdentifier,
        /// The universe's name, for people (#252, #399; empty: unnamed).
        name: Vec<u8>,
    },
    /// `init` (and adding a cell, M12): cell `cell_id` joins the directory,
    /// `REGISTERING`, and its cell tenant is registered
    /// ([`TenantKind::Cell`], pinned) under the identifier of its control
    /// journal.
    AddCell {
        /// The cell's id (random, minted at `init`).
        cell_id: u64,
        /// The cell tenant's control journal.
        control: JournalIdentifier,
        /// The cell's name, for people (#252, #399; empty: unnamed).
        name: Vec<u8>,
        /// The cell's kind: its reserved tags (#575).
        kind: CellKind,
        /// The cell's entry endpoint (#232 (multiple cells)): its founding
        /// members by node id and advertised address, where a caller that
        /// reached another cell is pointed. The first endpoint written stays.
        endpoint: Vec<(u64, Address)>,
    },
    /// `universe remove-cell` (#232 (multiple cells)): a `REMOVING` cell
    /// that hosts no tenant but its cell tenant leaves the directory, with
    /// its cell tenant. Its id is tombstoned: never added again.
    RemoveCell {
        /// The cell.
        cell_id: u64,
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
        /// Its first user tags (#575).
        tags: UserTags,
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
    /// A root public key joins the universe entry (#245 (Biscuit tokens)).
    AddRootKey(RootKeyEntry),
    /// The root key `label` leaves the universe entry, for good.
    RemoveRootKey {
        /// The key's label.
        label: String,
    },
    /// Request `request`: `tenant`'s user tags become `tags` (#575).
    SetTenantTags {
        /// The caller's idempotency id: applied once.
        request: u64,
        /// The tenant.
        tenant: TenantId,
        /// Its user tags, all of them.
        tags: UserTags,
    },
    /// Request `request`: cell `cell_id`'s user tags become `tags` (#575).
    SetCellTags {
        /// The caller's idempotency id: applied once.
        request: u64,
        /// The cell.
        cell_id: u64,
        /// Its user tags, all of them.
        tags: UserTags,
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

/// A wire list of user tags, judged: a malformed or reserved tag, or a set
/// over the cap, is refused (#575).
fn user_tags(tags: Vec<paros_wire::common::Tag>) -> Result<UserTags, &'static str> {
    UserTags::from_wire(tags).map_err(|_| "a user tag set is malformed, reserved or over the cap")
}

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn members_to_wire(members: &[(u64, Address)]) -> Vec<wire::CellMember> {
    members
        .iter()
        .map(|(node_id, addr)| wire::CellMember {
            node_id: *node_id,
            addr: addr.to_string(),
        })
        .collect()
}

fn members_from_wire(members: Vec<wire::CellMember>) -> Result<Vec<(u64, Address)>, &'static str> {
    members
        .into_iter()
        .map(|m| {
            let addr =
                Address::parse(&m.addr).map_err(|_| "a cell member's address does not parse")?;
            Ok((m.node_id, addr))
        })
        .collect()
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
            FleetCommand::FormFleet { control, name } => Kind::FormFleet(wire::FormFleet {
                fleet_tenant: control.tenant.0,
                fleet_journal: control.journal.0,
                name: name.clone(),
            }),
            FleetCommand::AddCell {
                cell_id,
                control,
                name,
                kind,
                endpoint,
            } => Kind::AddCell(wire::AddCell {
                cell_id: *cell_id,
                control_tenant: control.tenant.0,
                control_journal: control.journal.0,
                name: name.clone(),
                tags: kind.tags().iter().map(Tag::to_wire).collect(),
                endpoint: members_to_wire(endpoint),
            }),
            FleetCommand::RemoveCell { cell_id } => {
                Kind::RemoveCell(wire::RemoveCell { cell_id: *cell_id })
            }
            FleetCommand::MarkCell { cell_id, state } => Kind::MarkCell(wire::MarkCell {
                cell_id: *cell_id,
                state: state.to_wire(),
            }),
            FleetCommand::RegisterTenant {
                control,
                name,
                cell_id,
                survives,
                tags,
            } => Kind::RegisterTenant(wire::RegisterTenant {
                tenant: control.tenant.0,
                name: name.clone(),
                cell_id: *cell_id,
                control_journal: control.journal.0,
                survives: survives.to_wire(),
                tags: tags.to_wire(),
            }),
            FleetCommand::MarkTenant { tenant, state } => Kind::MarkTenant(wire::MarkTenant {
                tenant: tenant.0,
                state: state.to_wire(),
            }),
            FleetCommand::RemoveTenant { tenant } => {
                Kind::RemoveTenant(wire::RemoveTenant { tenant: tenant.0 })
            }
            FleetCommand::AddRootKey(key) => Kind::AddRootKey(wire::AddRootKey {
                key_id: key.key_id,
                label: key.label.clone(),
                public_key: key.public_key.clone(),
            }),
            FleetCommand::RemoveRootKey { label } => Kind::RemoveRootKey(wire::RemoveRootKey {
                label: label.clone(),
            }),
            FleetCommand::SetTenantTags {
                request,
                tenant,
                tags,
            } => Kind::SetTenantTags(wire::SetTenantTags {
                request: *request,
                tenant: tenant.0,
                tags: tags.to_wire(),
            }),
            FleetCommand::SetCellTags {
                request,
                cell_id,
                tags,
            } => Kind::SetCellTags(wire::SetCellTags {
                request: *request,
                cell_id: *cell_id,
                tags: tags.to_wire(),
            }),
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
    /// The record is not a fleet entry, names no kind, names a state no
    /// version knows, or carries a malformed or reserved user tag, a set
    /// over the cap, or a reserved tag its entity does not carry (#575).
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::fleet_entry::Kind;
        let entry = wire::FleetEntry::decode(record).map_err(|_| "not a fleet entry")?;
        let command = match entry.kind.ok_or("a fleet entry names no kind")? {
            Kind::FormFleet(form) => FleetCommand::FormFleet {
                control: identifier(form.fleet_tenant, form.fleet_journal),
                name: form.name,
            },
            Kind::AddCell(add) => {
                let (held, user) = Tags::from_wire(add.tags)
                    .and_then(Tags::split)
                    .map_err(|_| "a cell's tags are malformed")?;
                if !user.is_empty() {
                    return Err("a cell is added with its reserved tags only");
                }
                FleetCommand::AddCell {
                    cell_id: add.cell_id,
                    control: identifier(add.control_tenant, add.control_journal),
                    name: add.name,
                    kind: CellKind::from_tags(held).map_err(|_| "an unknown cell kind")?,
                    endpoint: members_from_wire(add.endpoint)?,
                }
            }
            Kind::RemoveCell(remove) => FleetCommand::RemoveCell {
                cell_id: remove.cell_id,
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
                tags: user_tags(register.tags)?,
            },
            Kind::MarkTenant(mark) => FleetCommand::MarkTenant {
                tenant: TenantId(mark.tenant),
                state: TenantState::from_wire(mark.state).ok_or("an unknown tenant state")?,
            },
            Kind::RemoveTenant(remove) => FleetCommand::RemoveTenant {
                tenant: TenantId(remove.tenant),
            },
            Kind::AddRootKey(add) => FleetCommand::AddRootKey(RootKeyEntry {
                key_id: add.key_id,
                label: add.label,
                public_key: add.public_key,
            }),
            Kind::RemoveRootKey(remove) => FleetCommand::RemoveRootKey {
                label: remove.label,
            },
            Kind::SetTenantTags(set) => FleetCommand::SetTenantTags {
                request: set.request,
                tenant: TenantId(set.tenant),
                tags: user_tags(set.tags)?,
            },
            Kind::SetCellTags(set) => FleetCommand::SetCellTags {
                request: set.request,
                cell_id: set.cell_id,
                tags: user_tags(set.tags)?,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellEntry {
    /// Where it stands.
    pub state: CellState,
    /// The metadata version it was added under.
    pub version: u32,
    /// Its cell tenant (a [`TenantKind::Cell`] tenant entry).
    pub control_tenant: TenantId,
    /// Its kind: its reserved tags, fixed when it was added (#575).
    pub kind: CellKind,
    /// Its user tags (#575).
    pub tags: UserTags,
}

impl CellEntry {
    /// Every tag it carries: its reserved kind tags and its user tags.
    #[must_use]
    pub fn all_tags(&self) -> Tags {
        self.tags.tags().clone().with(self.kind.tags())
    }
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
    /// Its name (opaque bytes). An `internal` tenant is found by its
    /// identifier, never by name: the fleet tenant's entry keeps the
    /// universe's name and a cell tenant's its cell's (#252, #399), display
    /// only (empty when unnamed).
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
    /// What it is, fixed at registration (#575: `paros/kind`,
    /// `paros/role`).
    pub kind: TenantKind,
    /// Whether it may move to another cell (`paros/movable`): a cell
    /// tenant is always pinned.
    pub movable: Movable,
    /// What it survives (#252), recorded at `REGISTERING` and mirrored into
    /// its control journal (#210). An `internal` tenant survives a zone.
    pub survives: Survives,
    /// Its user tags (#575).
    pub tags: UserTags,
}

impl TenantEntry {
    /// Every tag it carries: its reserved tags and its user tags.
    #[must_use]
    pub fn all_tags(&self) -> Tags {
        let mut reserved = self.kind.tags();
        reserved.push(self.movable.tag());
        self.tags.tags().clone().with(reserved)
    }

    /// Whether it may move to another cell (M12).
    #[must_use]
    pub fn may_move(&self) -> bool {
        self.movable == Movable::Movable
    }

    /// The reserved tags and user tags of a checkpointed entry.
    fn from_tags(tags: Tags) -> Result<(TenantKind, Movable, UserTags), TagError> {
        let (mut held, user) = tags.split()?;
        let kind = TenantKind::take(&mut held)?;
        let movable = Movable::take(&mut held)?;
        if let Some(key) = held.into_keys().next() {
            return Err(TagError::UnknownReserved(key));
        }
        if kind.forced().is_some_and(|forced| forced != movable) {
            return Err(TagError::UnknownReserved(reserved::MOVABLE.into()));
        }
        Ok((kind, movable, user))
    }
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
    /// A cell left the directory, its id tombstoned (#232).
    CellRemoved {
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
    /// A root public key joined the universe entry (#245).
    RootKeyAdded {
        /// Its key id.
        key_id: u32,
    },
    /// A root public key left the universe entry (#245).
    RootKeyRemoved {
        /// Its key id.
        key_id: u32,
    },
    /// A tenant's user tags changed (#575).
    TenantTagged {
        /// The tenant.
        tenant: TenantId,
    },
    /// A cell's user tags changed (#575).
    CellTagged {
        /// The cell.
        cell_id: u64,
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
    /// The cell id was removed from the directory (#232 (multiple
    /// cells)): a removed cell never comes back.
    CellTombstoned {
        /// The cell named.
        cell_id: u64,
    },
    /// The cell still hosts a tenant other than its cell tenant (#232
    /// (multiple cells)): remove or move it first.
    CellNotEmpty {
        /// The cell named.
        cell_id: u64,
        /// A tenant it hosts.
        tenant: TenantId,
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
    /// Another cell holds the name (#399).
    CellNameTaken {
        /// The cell holding it.
        holder: u64,
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
    /// A `users` tenant's name holds a `:`: an internal tenant shows as
    /// `<role>:<name>` (#575), so a user's never does.
    NameRefused,
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
    /// A root key, in force or removed, holds the key id or the label
    /// (#245): neither is ever reused.
    RootKeyTaken {
        /// The key id asked for.
        key_id: u32,
        /// The label asked for.
        label: String,
    },
    /// No root key in force holds the label.
    UnknownRootKey {
        /// The label named.
        label: String,
    },
    /// The universe's last root key stays: no token could administer the
    /// universe again.
    LastRootKey {
        /// The label named.
        label: String,
    },
}

/// What a tag request names (#575).
#[derive(Clone, Copy, Debug)]
enum Tagged {
    Tenant(TenantId),
    Cell(u64),
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
    /// Each cell's entry endpoint (#232 (multiple cells)).
    endpoints: BTreeMap<u64, Vec<(u64, Address)>>,
    /// Cell ids removed from the directory: never added again (#232).
    removed_cells: BTreeSet<u64>,
    tenants: BTreeMap<TenantId, TenantEntry>,
    /// Live `users` names, to the tenant holding each.
    names: BTreeMap<Vec<u8>, TenantId>,
    /// Ids removed from the directory: never reused.
    removed: BTreeSet<TenantId>,
    /// The universe entry's root public keys (#245).
    keys: RootKeys,
    /// Every tag request applied (#575): a retry never applies one again.
    tagged: BTreeSet<u64>,
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
    pub fn cell(&self, cell_id: u64) -> Option<&CellEntry> {
        self.cells.get(&cell_id)
    }

    /// Cell `cell_id`'s entry endpoint (#232 (multiple cells)): its
    /// founding members by node id and advertised address; empty for an
    /// unknown cell or one added before endpoints (metadata version 3).
    #[must_use]
    pub fn endpoint(&self, cell_id: u64) -> &[(u64, Address)] {
        self.endpoints.get(&cell_id).map_or(&[], Vec::as_slice)
    }

    /// Whether cell `cell_id` was removed from the directory (#232).
    #[must_use]
    pub fn is_cell_removed(&self, cell_id: u64) -> bool {
        self.removed_cells.contains(&cell_id)
    }

    /// The cell that hosts the universe tenant: the cell its entry names,
    /// once its hosting cell was added.
    #[must_use]
    pub fn universe_cell(&self) -> Option<u64> {
        let fleet = self.fleet?;
        let cell = self.tenants.get(&fleet.control.tenant)?.cell_id;
        (cell != 0).then_some(cell)
    }

    /// Every cell entry, in id order.
    pub fn cells(&self) -> impl Iterator<Item = (u64, &CellEntry)> + '_ {
        self.cells.iter().map(|(id, c)| (*id, c))
    }

    /// Whether the tag request `request` was applied (#575).
    #[must_use]
    pub fn tagged(&self, request: u64) -> bool {
        self.tagged.contains(&request)
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

    /// The universe's name (#252, #399): empty when unnamed or not formed.
    #[must_use]
    pub fn universe_name(&self) -> &[u8] {
        self.fleet
            .and_then(|fleet| self.tenants.get(&fleet.control.tenant))
            .map_or(&[], |entry| entry.name.as_slice())
    }

    /// Cell `cell_id`'s name (#252, #399): empty when unnamed or unknown.
    #[must_use]
    pub fn cell_name(&self, cell_id: u64) -> &[u8] {
        self.cells
            .get(&cell_id)
            .and_then(|cell| self.tenants.get(&cell.control_tenant))
            .map_or(&[], |entry| entry.name.as_slice())
    }

    /// The cell named `name` (#399).
    #[must_use]
    pub fn cell_named(&self, name: &[u8]) -> Option<u64> {
        if name.is_empty() {
            return None;
        }
        self.cells()
            .map(|(id, _)| id)
            .find(|id| self.cell_name(*id) == name)
    }

    /// The universe entry's root public keys in force, in key id order
    /// (#245): what an entry role verifies a token against.
    pub fn root_keys(&self) -> impl Iterator<Item = &RootKeyEntry> {
        self.keys.live()
    }

    /// Whether `tenant` was removed from the directory.
    #[must_use]
    pub fn is_removed(&self, tenant: TenantId) -> bool {
        self.removed.contains(&tenant)
    }

    /// How a person reads `tenant` (#239): a `users` tenant by its name, an
    /// internal one by its kind, the universe's or a cell's, with its cell.
    /// A label is display only: never a name a caller can resolve, and
    /// never an id (§3.8: no well-known names).
    #[must_use]
    pub fn label(&self, tenant: TenantId) -> Option<TenantLabel<'_>> {
        let entry = self.tenants.get(&tenant)?;
        match entry.kind {
            TenantKind::Users => return Some(TenantLabel::Named(&entry.name)),
            TenantKind::Universe => return Some(TenantLabel::Fleet),
            TenantKind::Cell => {}
        }
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
    /// a [`Folder`](crate::checkpoint::Folder).
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
        if let FleetCommand::FormFleet { control, name } = &entry.command {
            return self.form_fleet(entry.fleet_id, entry.version, *control, name);
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
            FleetCommand::AddCell {
                cell_id,
                control,
                name,
                kind,
                endpoint,
            } => self.add_cell(*cell_id, *control, name, kind, endpoint, entry.version),
            FleetCommand::RemoveCell { cell_id } => self.remove_cell(*cell_id),
            FleetCommand::MarkCell { cell_id, state } => self.mark_cell(*cell_id, *state),
            FleetCommand::RegisterTenant {
                control,
                name,
                cell_id,
                survives,
                tags,
            } => self.register_tenant(*control, name, *cell_id, *survives, tags),
            FleetCommand::MarkTenant { tenant, state } => self.mark_tenant(*tenant, *state),
            FleetCommand::RemoveTenant { tenant } => self.remove_tenant(*tenant),
            FleetCommand::AddRootKey(key) => Ok(self.keys.add(key)?.into()),
            FleetCommand::RemoveRootKey { label } => Ok(self.keys.remove(label)?.into()),
            FleetCommand::SetTenantTags {
                request,
                tenant,
                tags,
            } => self.set_tags(*request, Tagged::Tenant(*tenant), tags),
            FleetCommand::SetCellTags {
                request,
                cell_id,
                tags,
            } => self.set_tags(*request, Tagged::Cell(*cell_id), tags),
        }
    }

    /// A tag request (#575): judged at apply, applied at most once, all the
    /// user tags at once. The tags were judged at decode: no reserved key,
    /// within the cap.
    fn set_tags(
        &mut self,
        request: u64,
        target: Tagged,
        tags: &UserTags,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if request == 0 {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if self.tagged.contains(&request) {
            return Ok(FleetEvent::Unchanged);
        }
        let (held, event) = match target {
            Tagged::Tenant(tenant) => {
                let entry = self
                    .tenants
                    .get_mut(&tenant)
                    .ok_or(FleetDirectoryRefusal::UnknownTenant { tenant })?;
                (&mut entry.tags, FleetEvent::TenantTagged { tenant })
            }
            Tagged::Cell(cell_id) => {
                let entry = self
                    .cells
                    .get_mut(&cell_id)
                    .ok_or(FleetDirectoryRefusal::UnknownCell { cell_id })?;
                (&mut entry.tags, FleetEvent::CellTagged { cell_id })
            }
        };
        held.clone_from(tags);
        self.tagged.insert(request);
        Ok(event)
    }

    fn form_fleet(
        &mut self,
        fleet_id: u64,
        version: u32,
        control: JournalIdentifier,
        name: &[u8],
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
                        name: name.to_vec(),
                        cell_id: 0,
                        control: control.journal,
                        state: TenantState::Ready,
                        config_seq: 0,
                        kind: TenantKind::Universe,
                        movable: Movable::Movable,
                        survives: Survives::Az,
                        tags: UserTags::none(),
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
        name: &[u8],
        kind: &CellKind,
        endpoint: &[(u64, Address)],
        version: u32,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if cell_id == 0 || !control.is_set() || endpoint.iter().any(|(id, _)| *id == 0) {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if self.removed_cells.contains(&cell_id) {
            return Err(FleetDirectoryRefusal::CellTombstoned { cell_id });
        }
        if let Some(cell) = self.cells.get(&cell_id) {
            let same = cell.control_tenant == control.tenant
                && cell.kind == *kind
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
        if let Some(holder) = self.cell_named(name) {
            return Err(FleetDirectoryRefusal::CellNameTaken { holder });
        }
        self.cells.insert(
            cell_id,
            CellEntry {
                state: CellState::Registering,
                version,
                control_tenant: control.tenant,
                kind: kind.clone(),
                tags: UserTags::none(),
            },
        );
        if !endpoint.is_empty() {
            self.endpoints.insert(cell_id, endpoint.to_vec());
        }
        self.tenants.insert(
            control.tenant,
            TenantEntry {
                name: name.to_vec(),
                cell_id,
                control: control.journal,
                state: TenantState::Ready,
                config_seq: 0,
                kind: TenantKind::Cell,
                movable: Movable::Pinned,
                survives: Survives::Az,
                tags: UserTags::none(),
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

    fn remove_cell(&mut self, cell_id: u64) -> Result<FleetEvent, FleetDirectoryRefusal> {
        if self.removed_cells.contains(&cell_id) {
            return Ok(FleetEvent::Unchanged);
        }
        let (state, control_tenant) = self
            .cells
            .get(&cell_id)
            .map(|cell| (cell.state, cell.control_tenant))
            .ok_or(FleetDirectoryRefusal::UnknownCell { cell_id })?;
        if state != CellState::Removing {
            return Err(FleetDirectoryRefusal::CellTransition {
                from: state,
                to: CellState::Removing,
            });
        }
        if let Some((tenant, _)) = self
            .tenants
            .iter()
            .find(|(id, t)| t.cell_id == cell_id && **id != control_tenant)
        {
            return Err(FleetDirectoryRefusal::CellNotEmpty {
                cell_id,
                tenant: *tenant,
            });
        }
        self.cells.remove(&cell_id);
        self.endpoints.remove(&cell_id);
        self.tenants.remove(&control_tenant);
        self.removed.insert(control_tenant);
        self.removed_cells.insert(cell_id);
        Ok(FleetEvent::CellRemoved { cell_id })
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
        tags: &UserTags,
    ) -> Result<FleetEvent, FleetDirectoryRefusal> {
        let tenant = control.tenant;
        if !control.is_set() {
            return Err(FleetDirectoryRefusal::Malformed);
        }
        if name.contains(&b':') {
            return Err(FleetDirectoryRefusal::NameRefused);
        }
        if let Some(existing) = self.tenants.get(&tenant) {
            // The same registration again: a re-run of the first step. Its
            // tags are the first ones only: a later `SetTenantTags` may
            // have changed them since.
            let same = existing.kind == TenantKind::Users
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
                kind: TenantKind::Users,
                movable: Movable::Movable,
                survives,
                tags: tags.clone(),
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
        if entry.kind.is_internal() {
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
        if entry.kind.is_internal() {
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
        let (root_keys, removed_keys) = self.keys.to_wire();
        wire::FleetDirectoryState {
            root_keys,
            removed_keys,
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
                    tags: c.all_tags().to_wire(),
                    endpoint: members_to_wire(self.endpoint(*id)),
                })
                .collect(),
            removed_cells: self.removed_cells.iter().copied().collect(),
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
                    survives: t.survives.to_wire(),
                    tags: t.all_tags().to_wire(),
                })
                .collect(),
            removed: self.removed.iter().map(|id| id.0).collect(),
            tagged: self.tagged.iter().copied().collect(),
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
        let mut endpoints = BTreeMap::new();
        for c in state.cells {
            let cell_state = CellState::from_wire(c.state).ok_or("an unknown cell state")?;
            let (held, tags) = Tags::from_wire(c.tags)
                .and_then(Tags::split)
                .map_err(|_| "a cell entry's tags are malformed")?;
            let endpoint = members_from_wire(c.endpoint)?;
            if !endpoint.is_empty() {
                endpoints.insert(c.cell_id, endpoint);
            }
            cells.insert(
                c.cell_id,
                CellEntry {
                    state: cell_state,
                    version: c.version,
                    control_tenant: TenantId(c.control_tenant),
                    kind: CellKind::from_tags(held).map_err(|_| "an unknown cell kind")?,
                    tags,
                },
            );
        }
        let mut tenants = BTreeMap::new();
        let mut names = BTreeMap::new();
        for t in state.tenants {
            let id = TenantId(t.tenant);
            let (kind, movable, tags) = Tags::from_wire(t.tags)
                .and_then(TenantEntry::from_tags)
                .map_err(|_| "a tenant entry's tags are malformed or unknown")?;
            if !id.is_set() || t.control_journal == 0 {
                return Err("a fleet directory state names an unset tenant identifier");
            }
            if kind == TenantKind::Users && names.insert(t.name.clone(), id).is_some() {
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
                    kind,
                    movable,
                    survives: Survives::from_wire(t.survives)?,
                    tags,
                },
            );
        }
        let removed: BTreeSet<TenantId> = state.removed.into_iter().map(TenantId).collect();
        if removed.iter().any(|id| tenants.contains_key(id)) {
            return Err("a fleet directory state holds a removed tenant");
        }
        let keys = RootKeys::restore(state.root_keys, state.removed_keys)?;
        let removed_cells: BTreeSet<u64> = state.removed_cells.into_iter().collect();
        if removed_cells.iter().any(|id| cells.contains_key(id)) {
            return Err("a fleet directory state holds a removed cell");
        }
        self.fleet = fleet;
        self.cells = cells;
        self.endpoints = endpoints;
        self.removed_cells = removed_cells;
        self.tenants = tenants;
        self.names = names;
        self.removed = removed;
        self.keys = keys;
        self.tagged = state.tagged.into_iter().collect();
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

impl From<KeyChange> for FleetEvent {
    fn from(change: KeyChange) -> Self {
        match change {
            KeyChange::Added(key_id) => Self::RootKeyAdded { key_id },
            KeyChange::Removed(key_id) => Self::RootKeyRemoved { key_id },
            KeyChange::Unchanged => Self::Unchanged,
        }
    }
}

/// The event a [`Folded`] fleet record is reported as (see
/// [`crate::registry::registry_event`]). `None` for a record the fold skipped
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
