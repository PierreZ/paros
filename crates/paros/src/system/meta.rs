//! **Meta** (#229, `docs/architecture.md` §3.7): the meta tenant's control
//! journal, the fleet's directory. It answers one question — which tenant
//! lives in which cell — plus the cell entries, and stays that small.
//!
//! - **Cells.** A cell is `REGISTERING` once `init` records it, `READY` once
//!   its own half of the registration is written (`RegisterFleet` in its
//!   registry); only a `READY` cell receives new tenants. The first cell
//!   registered names the fleet: every later step must name the same
//!   fleet, or it is refused at apply ([`MetaRefusal::OtherFleet`]).
//! - **Tenants.** A tenant's id is random, drawn by its creator and checked
//!   here: a reserved id, or one ever registered (a forgotten one
//!   included), is refused and the creator redraws. A tenant is
//!   `REGISTERING` with its cell assignment, then `READY` once its cell
//!   hosts it; it may be removed from any state (`REMOVING`), then
//!   forgotten. A step naming another cell than the tenant's is refused
//!   ([`MetaRefusal::WrongCell`]).
//!
//! `UPDATING_CONFIGURATION`, `RENAMING` and `ERROR` are part of the
//! recorded format from M9 (§3.7) but no M9 entry enters them:
//! configuration is #214, renaming and the error path come with M12. So are
//! the configuration sequence number (0 until #214) and `tenant_group`
//! (reserved, unused).
//!
//! The directory is a pointer, never the authority: where it disagrees with
//! a tenant's control journal, the journal wins, and the directory is
//! rebuilt from the cells (§3.10). It is [`Checkpointable`] like the
//! registry: its state is every cell and tenant, never their history.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::TenantId;
use prost::Message as _;

use super::{FleetContext, SystemCommand};
use crate::client::checkpoint::{Checkpointable, Folded};
use crate::rpc::system as wire;

/// The metadata format this fold reads and writes (§3.7): a registration in
/// a higher version is refused, so a reader never folds a format it does
/// not understand.
pub const METADATA_VERSION: u32 = 1;

/// A tenant's state in meta (FDB's metacluster tenant states).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TenantState {
    /// Recorded with its cell assignment; not created in the cell yet.
    Registering,
    /// Created in its cell.
    Ready,
    /// Being removed.
    Removing,
    /// Being reconfigured (#214; no M9 entry enters it).
    UpdatingConfiguration,
    /// Being renamed (no M9 entry enters it).
    Renaming,
    /// A failed operation left it here (no M9 entry enters it).
    Error,
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

    fn from_wire(state: u32) -> Result<Self, &'static str> {
        Ok(match state {
            0 => TenantState::Registering,
            1 => TenantState::Ready,
            2 => TenantState::Removing,
            3 => TenantState::UpdatingConfiguration,
            4 => TenantState::Renaming,
            5 => TenantState::Error,
            _ => return Err("a tenant state is one of six"),
        })
    }
}

/// A cell's state in meta.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CellState {
    /// Recorded by `init`; its own half of the registration may be missing.
    Registering,
    /// Registered on both sides: it receives new tenants.
    Ready,
    /// Being removed (M12).
    Removing,
    /// Being restored (`init --recover`, #231).
    Restoring,
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

    fn from_wire(state: u32) -> Result<Self, &'static str> {
        Ok(match state {
            0 => CellState::Registering,
            1 => CellState::Ready,
            2 => CellState::Removing,
            3 => CellState::Restoring,
            _ => return Err("a cell state is one of four"),
        })
    }
}

/// A cell meta records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellEntry {
    /// Where it stands.
    pub state: CellState,
    /// The metadata version it registered in.
    pub metadata_version: u32,
}

/// A tenant meta records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantEntry {
    /// Its name, unique among the fleet's tenants.
    pub name: Vec<u8>,
    /// The cell it is assigned to.
    pub cell_id: u64,
    /// Where it stands.
    pub state: TenantState,
    /// Its configuration sequence number (0 until #214).
    pub config_seq: u64,
    /// Reserved, unused in M9.
    pub tenant_group: Option<u64>,
}

/// What one meta record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaEvent {
    /// A cell was registered (`REGISTERING`).
    CellRegistered {
        /// The fleet and the cell.
        context: FleetContext,
    },
    /// A cell is `READY`.
    CellReady {
        /// The cell.
        cell_id: u64,
    },
    /// A tenant was registered (`REGISTERING`).
    TenantRegistered {
        /// The tenant.
        tenant: TenantId,
        /// Its name.
        name: Vec<u8>,
        /// Its cell.
        cell_id: u64,
    },
    /// A tenant is `READY`.
    TenantReady {
        /// The tenant.
        tenant: TenantId,
    },
    /// A tenant is `REMOVING`.
    TenantRemoving {
        /// The tenant.
        tenant: TenantId,
    },
    /// A tenant is gone; its id is never used again.
    TenantForgotten {
        /// The tenant.
        tenant: TenantId,
    },
    /// A checkpoint (#230): see [`super::RegistryEvent::Checkpoint`].
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
    /// Not exactly one decodable meta entry (or a checkpoint this fold
    /// cannot use).
    Malformed,
    /// A registration in a metadata version this fold does not understand.
    UnsupportedVersion {
        /// The version named.
        version: u32,
    },
    /// The step names another fleet than the one meta records (or names one
    /// before any cell registered).
    OtherFleet,
    /// The cell is registered already.
    CellKnown {
        /// The cell named.
        cell_id: u64,
    },
    /// The cell is not registered.
    UnknownCell {
        /// The cell named.
        cell_id: u64,
    },
    /// The cell receives no new tenant: it is not `READY`.
    CellNotReady {
        /// The cell named.
        cell_id: u64,
    },
    /// The cell is not in a state this step moves it from.
    CellState {
        /// The cell named.
        cell_id: u64,
        /// Its state.
        state: CellState,
    },
    /// The id is a system tenant's (`0..=255`).
    Reserved {
        /// The id asked for.
        tenant: TenantId,
    },
    /// The id was registered before (forgotten or not): the creator redraws.
    IdTaken {
        /// The id asked for.
        tenant: TenantId,
    },
    /// A tenant holds the name.
    NameTaken {
        /// The tenant that holds it.
        winner: TenantId,
    },
    /// The tenant is not registered.
    UnknownTenant {
        /// The tenant named.
        tenant: TenantId,
    },
    /// The step names another cell than the tenant's.
    WrongCell {
        /// The tenant named.
        tenant: TenantId,
    },
    /// The tenant is not in a state this step moves it from.
    TenantState {
        /// The tenant named.
        tenant: TenantId,
        /// Its state.
        state: TenantState,
    },
}

/// Meta's fold: the fleet's id, every cell and every tenant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    fleet_id: Option<u64>,
    cells: BTreeMap<u64, CellEntry>,
    tenants: BTreeMap<TenantId, TenantEntry>,
    names: BTreeMap<Vec<u8>, TenantId>,
    forgotten: BTreeSet<TenantId>,
    next_seq: u64,
}

impl Meta {
    /// An empty meta: no fleet yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The next position this fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The fleet's id, once the first cell registered.
    #[must_use]
    pub fn fleet_id(&self) -> Option<u64> {
        self.fleet_id
    }

    /// The cell registered as `cell_id`.
    #[must_use]
    pub fn cell(&self, cell_id: u64) -> Option<&CellEntry> {
        self.cells.get(&cell_id)
    }

    /// Every cell, in id order.
    pub fn cells(&self) -> impl Iterator<Item = (u64, &CellEntry)> {
        self.cells.iter().map(|(id, c)| (*id, c))
    }

    /// The cell a new tenant is assigned to: the first `READY` one in id
    /// order (M9 has one; placement across cells is M12).
    #[must_use]
    pub fn ready_cell(&self) -> Option<u64> {
        self.cells
            .iter()
            .find(|(_, c)| c.state == CellState::Ready)
            .map(|(id, _)| *id)
    }

    /// The tenant registered as `tenant`.
    #[must_use]
    pub fn tenant(&self, tenant: TenantId) -> Option<&TenantEntry> {
        self.tenants.get(&tenant)
    }

    /// The tenant holding `name`.
    #[must_use]
    pub fn by_name(&self, name: &[u8]) -> Option<TenantId> {
        self.names.get(name).copied()
    }

    /// Every tenant, in id order.
    pub fn tenants(&self) -> impl Iterator<Item = (TenantId, &TenantEntry)> {
        self.tenants.iter().map(|(id, t)| (*id, t))
    }

    /// Whether `tenant` was registered and then forgotten.
    #[must_use]
    pub fn is_forgotten(&self, tenant: TenantId) -> bool {
        self.forgotten.contains(&tenant)
    }

    /// Fold the record at position `seq` (in position order). A checkpoint
    /// record is not an entry: fold through a
    /// [`Folder`](crate::client::checkpoint::Folder).
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (see
    /// [`super::Directory::fold`]).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> MetaEvent {
        assert!(seq >= self.next_seq, "meta folds in position order");
        self.next_seq = seq + 1;
        let refused = MetaEvent::Refused;
        match SystemCommand::decode(record).ok() {
            Some(SystemCommand::RegisterCell {
                context,
                metadata_version,
            }) => self.register_cell(context, metadata_version),
            Some(SystemCommand::CellReady { context }) => {
                if let Err(refusal) = self.check_fleet(context.fleet_id) {
                    return refused(refusal);
                }
                let cell_id = context.cell_id;
                match self.cells.get_mut(&cell_id) {
                    None => refused(MetaRefusal::UnknownCell { cell_id }),
                    Some(cell)
                        if matches!(cell.state, CellState::Registering | CellState::Restoring) =>
                    {
                        cell.state = CellState::Ready;
                        MetaEvent::CellReady { cell_id }
                    }
                    Some(cell) => refused(MetaRefusal::CellState {
                        cell_id,
                        state: cell.state,
                    }),
                }
            }
            Some(SystemCommand::RegisterTenant {
                context,
                tenant,
                name,
            }) => self.register_tenant(context, tenant, name),
            Some(SystemCommand::TenantReady { context, tenant }) => {
                if let Err(refusal) = self.check_fleet(context.fleet_id) {
                    return refused(refusal);
                }
                match self.tenants.get_mut(&tenant) {
                    None => refused(MetaRefusal::UnknownTenant { tenant }),
                    Some(entry) if entry.cell_id != context.cell_id => {
                        refused(MetaRefusal::WrongCell { tenant })
                    }
                    Some(entry)
                        if matches!(
                            entry.state,
                            TenantState::Registering
                                | TenantState::UpdatingConfiguration
                                | TenantState::Renaming
                        ) =>
                    {
                        entry.state = TenantState::Ready;
                        MetaEvent::TenantReady { tenant }
                    }
                    Some(entry) => refused(MetaRefusal::TenantState {
                        tenant,
                        state: entry.state,
                    }),
                }
            }
            Some(SystemCommand::RemoveTenant { fleet_id, tenant }) => {
                if let Err(refusal) = self.check_fleet(fleet_id) {
                    return refused(refusal);
                }
                match self.tenants.get_mut(&tenant) {
                    None => refused(MetaRefusal::UnknownTenant { tenant }),
                    Some(entry) if entry.state == TenantState::Removing => {
                        refused(MetaRefusal::TenantState {
                            tenant,
                            state: entry.state,
                        })
                    }
                    Some(entry) => {
                        entry.state = TenantState::Removing;
                        MetaEvent::TenantRemoving { tenant }
                    }
                }
            }
            Some(SystemCommand::ForgetTenant { fleet_id, tenant }) => {
                if let Err(refusal) = self.check_fleet(fleet_id) {
                    return refused(refusal);
                }
                match self.tenants.get(&tenant) {
                    None => refused(MetaRefusal::UnknownTenant { tenant }),
                    Some(entry) if entry.state != TenantState::Removing => {
                        refused(MetaRefusal::TenantState {
                            tenant,
                            state: entry.state,
                        })
                    }
                    Some(_) => {
                        if let Some(entry) = self.tenants.remove(&tenant) {
                            self.names.remove(&entry.name);
                        }
                        self.forgotten.insert(tenant);
                        MetaEvent::TenantForgotten { tenant }
                    }
                }
            }
            _ => refused(MetaRefusal::Malformed),
        }
    }

    /// A step names the fleet meta records.
    fn check_fleet(&self, fleet_id: u64) -> Result<(), MetaRefusal> {
        if self.fleet_id == Some(fleet_id) {
            Ok(())
        } else {
            Err(MetaRefusal::OtherFleet)
        }
    }

    fn register_cell(&mut self, context: FleetContext, version: u32) -> MetaEvent {
        if version == 0 || version > METADATA_VERSION {
            return MetaEvent::Refused(MetaRefusal::UnsupportedVersion { version });
        }
        if context.fleet_id == 0 || context.cell_id == 0 {
            return MetaEvent::Refused(MetaRefusal::Malformed);
        }
        if self.fleet_id.is_some_and(|fleet| fleet != context.fleet_id) {
            return MetaEvent::Refused(MetaRefusal::OtherFleet);
        }
        if self.cells.contains_key(&context.cell_id) {
            return MetaEvent::Refused(MetaRefusal::CellKnown {
                cell_id: context.cell_id,
            });
        }
        self.fleet_id = Some(context.fleet_id);
        self.cells.insert(
            context.cell_id,
            CellEntry {
                state: CellState::Registering,
                metadata_version: version,
            },
        );
        MetaEvent::CellRegistered { context }
    }

    fn register_tenant(
        &mut self,
        context: FleetContext,
        tenant: TenantId,
        name: Vec<u8>,
    ) -> MetaEvent {
        let refused = MetaEvent::Refused;
        if let Err(refusal) = self.check_fleet(context.fleet_id) {
            return refused(refusal);
        }
        if !tenant.is_user() {
            return refused(MetaRefusal::Reserved { tenant });
        }
        if self.tenants.contains_key(&tenant) || self.forgotten.contains(&tenant) {
            return refused(MetaRefusal::IdTaken { tenant });
        }
        if let Some(&winner) = self.names.get(&name) {
            return refused(MetaRefusal::NameTaken { winner });
        }
        let cell_id = context.cell_id;
        match self.cells.get(&cell_id) {
            None => return refused(MetaRefusal::UnknownCell { cell_id }),
            Some(cell) if cell.state != CellState::Ready => {
                return refused(MetaRefusal::CellNotReady { cell_id });
            }
            Some(_) => {}
        }
        self.names.insert(name.clone(), tenant);
        self.tenants.insert(
            tenant,
            TenantEntry {
                name: name.clone(),
                cell_id,
                state: TenantState::Registering,
                config_seq: 0,
                tenant_group: None,
            },
        );
        MetaEvent::TenantRegistered {
            tenant,
            name,
            cell_id,
        }
    }

    fn state_to_wire(&self) -> wire::MetaState {
        wire::MetaState {
            fleet_id: self.fleet_id.unwrap_or(0),
            cells: self
                .cells
                .iter()
                .map(|(id, c)| wire::CellEntryState {
                    cell_id: *id,
                    state: c.state.to_wire(),
                    metadata_version: c.metadata_version,
                })
                .collect(),
            tenants: self
                .tenants
                .iter()
                .map(|(id, t)| wire::TenantEntryState {
                    tenant: id.0,
                    name: t.name.clone(),
                    cell_id: t.cell_id,
                    state: t.state.to_wire(),
                    config_seq: t.config_seq,
                    tenant_group: t.tenant_group.unwrap_or(0),
                })
                .collect(),
            forgotten: self.forgotten.iter().map(|id| id.0).collect(),
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
        let mut cells = BTreeMap::new();
        for c in state.cells {
            if c.metadata_version > METADATA_VERSION {
                return Err("a meta state names a metadata version this fold does not read");
            }
            cells.insert(
                c.cell_id,
                CellEntry {
                    state: CellState::from_wire(c.state)?,
                    metadata_version: c.metadata_version,
                },
            );
        }
        let mut tenants = BTreeMap::new();
        let mut names = BTreeMap::new();
        for t in state.tenants {
            let id = TenantId(t.tenant);
            if names.insert(t.name.clone(), id).is_some() {
                return Err("a meta state names one tenant name twice");
            }
            tenants.insert(
                id,
                TenantEntry {
                    name: t.name,
                    cell_id: t.cell_id,
                    state: TenantState::from_wire(t.state)?,
                    config_seq: t.config_seq,
                    tenant_group: (t.tenant_group != 0).then_some(t.tenant_group),
                },
            );
        }
        self.fleet_id = (state.fleet_id != 0).then_some(state.fleet_id);
        self.cells = cells;
        self.tenants = tenants;
        self.names = names;
        self.forgotten = state.forgotten.into_iter().map(TenantId).collect();
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The event a [`Folded`] meta record is reported as (see
/// [`super::registry_event`]).
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
mod tests {
    use super::*;
    use crate::client::checkpoint::{CheckpointRecord, Folder};

    const FLEET: u64 = 77;
    const CELL: u64 = 5;
    const HERE: FleetContext = FleetContext {
        fleet_id: FLEET,
        cell_id: CELL,
    };

    fn rec(command: &SystemCommand) -> Vec<u8> {
        command.encode()
    }

    fn register_cell(context: FleetContext, version: u32) -> Vec<u8> {
        rec(&SystemCommand::RegisterCell {
            context,
            metadata_version: version,
        })
    }

    fn register(tenant: u64, name: &[u8], context: FleetContext) -> Vec<u8> {
        rec(&SystemCommand::RegisterTenant {
            context,
            tenant: TenantId(tenant),
            name: name.to_vec(),
        })
    }

    fn ready(tenant: u64, context: FleetContext) -> Vec<u8> {
        rec(&SystemCommand::TenantReady {
            context,
            tenant: TenantId(tenant),
        })
    }

    /// A meta with the one cell `READY`, at position 2.
    fn ready_fleet() -> Meta {
        let mut meta = Meta::new();
        meta.fold(0, &register_cell(HERE, METADATA_VERSION));
        meta.fold(1, &rec(&SystemCommand::CellReady { context: HERE }));
        meta
    }

    #[test]
    fn the_first_cell_names_the_fleet_and_receives_tenants_once_ready() {
        let mut meta = Meta::new();
        assert_eq!(
            meta.fold(0, &register_cell(HERE, METADATA_VERSION + 1)),
            MetaEvent::Refused(MetaRefusal::UnsupportedVersion {
                version: METADATA_VERSION + 1
            })
        );
        assert_eq!(
            meta.fold(1, &register_cell(HERE, METADATA_VERSION)),
            MetaEvent::CellRegistered { context: HERE }
        );
        assert_eq!(meta.fleet_id(), Some(FLEET));
        assert_eq!(meta.ready_cell(), None);
        // A cell that is not ready receives no tenant.
        assert_eq!(
            meta.fold(2, &register(300, b"acme", HERE)),
            MetaEvent::Refused(MetaRefusal::CellNotReady { cell_id: CELL })
        );
        // Another fleet is refused on every step.
        let elsewhere = FleetContext {
            fleet_id: FLEET + 1,
            cell_id: CELL,
        };
        assert_eq!(
            meta.fold(3, &register_cell(elsewhere, METADATA_VERSION)),
            MetaEvent::Refused(MetaRefusal::OtherFleet)
        );
        assert_eq!(
            meta.fold(4, &rec(&SystemCommand::CellReady { context: elsewhere })),
            MetaEvent::Refused(MetaRefusal::OtherFleet)
        );
        assert_eq!(
            meta.fold(5, &register_cell(HERE, METADATA_VERSION)),
            MetaEvent::Refused(MetaRefusal::CellKnown { cell_id: CELL })
        );
        assert_eq!(
            meta.fold(6, &rec(&SystemCommand::CellReady { context: HERE })),
            MetaEvent::CellReady { cell_id: CELL }
        );
        assert_eq!(meta.ready_cell(), Some(CELL));
        assert_eq!(
            meta.fold(7, &rec(&SystemCommand::CellReady { context: HERE })),
            MetaEvent::Refused(MetaRefusal::CellState {
                cell_id: CELL,
                state: CellState::Ready
            })
        );
    }

    #[test]
    fn a_tenant_registers_then_goes_ready_and_its_id_is_never_reused() {
        let mut meta = ready_fleet();
        assert_eq!(
            meta.fold(2, &register(7, b"sys", HERE)),
            MetaEvent::Refused(MetaRefusal::Reserved {
                tenant: TenantId(7)
            })
        );
        assert!(matches!(
            meta.fold(3, &register(300, b"acme", HERE)),
            MetaEvent::TenantRegistered { .. }
        ));
        assert_eq!(meta.by_name(b"acme"), Some(TenantId(300)));
        assert_eq!(
            meta.fold(4, &register(300, b"other", HERE)),
            MetaEvent::Refused(MetaRefusal::IdTaken {
                tenant: TenantId(300)
            })
        );
        assert_eq!(
            meta.fold(5, &register(301, b"acme", HERE)),
            MetaEvent::Refused(MetaRefusal::NameTaken {
                winner: TenantId(300)
            })
        );
        // A step naming another cell than the tenant's is refused.
        let other_cell = FleetContext {
            fleet_id: FLEET,
            cell_id: CELL + 1,
        };
        assert_eq!(
            meta.fold(6, &ready(300, other_cell)),
            MetaEvent::Refused(MetaRefusal::WrongCell {
                tenant: TenantId(300)
            })
        );
        assert_eq!(
            meta.fold(7, &ready(300, HERE)),
            MetaEvent::TenantReady {
                tenant: TenantId(300)
            }
        );
        // Removed from any state, then forgotten: the name frees up, the id
        // never does.
        let remove = rec(&SystemCommand::RemoveTenant {
            fleet_id: FLEET,
            tenant: TenantId(300),
        });
        let forget = rec(&SystemCommand::ForgetTenant {
            fleet_id: FLEET,
            tenant: TenantId(300),
        });
        assert_eq!(
            meta.fold(8, &forget),
            MetaEvent::Refused(MetaRefusal::TenantState {
                tenant: TenantId(300),
                state: TenantState::Ready
            })
        );
        assert_eq!(
            meta.fold(9, &remove),
            MetaEvent::TenantRemoving {
                tenant: TenantId(300)
            }
        );
        assert_eq!(
            meta.fold(10, &ready(300, HERE)),
            MetaEvent::Refused(MetaRefusal::TenantState {
                tenant: TenantId(300),
                state: TenantState::Removing
            })
        );
        assert_eq!(
            meta.fold(11, &forget),
            MetaEvent::TenantForgotten {
                tenant: TenantId(300)
            }
        );
        assert!(meta.is_forgotten(TenantId(300)));
        assert_eq!(
            meta.fold(12, &register(300, b"acme", HERE)),
            MetaEvent::Refused(MetaRefusal::IdTaken {
                tenant: TenantId(300)
            })
        );
        assert!(matches!(
            meta.fold(13, &register(302, b"acme", HERE)),
            MetaEvent::TenantRegistered { .. }
        ));
    }

    #[test]
    fn meta_restored_from_its_checkpoint_is_meta_folded_whole() {
        let records = [
            register_cell(HERE, METADATA_VERSION),
            rec(&SystemCommand::CellReady { context: HERE }),
            register(300, b"a", HERE),
            register(301, b"b", HERE),
            ready(300, HERE),
            rec(&SystemCommand::RemoveTenant {
                fleet_id: FLEET,
                tenant: TenantId(301),
            }),
            rec(&SystemCommand::ForgetTenant {
                fleet_id: FLEET,
                tenant: TenantId(301),
            }),
        ];
        let mut whole = Folder::new(Meta::new());
        for (seq, record) in (0..).zip(&records) {
            assert!(matches!(whole.fold(seq, record), Some(Folded::Entry(_))));
        }
        let at = records.len() as u64;
        let checkpoint = CheckpointRecord::Inline {
            covers_up_to: at,
            chunks: vec![whole.state().checkpoint()],
        }
        .encode();
        assert_eq!(
            whole.fold(at, &checkpoint),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: Some(true)
            })
        );
        let mut restored = Folder::new(Meta::new());
        restored.jump(at);
        assert!(matches!(
            restored.fold(at, &checkpoint),
            Some(Folded::Checkpoint { verified: None, .. })
        ));
        assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
        // The forgotten id stays taken across the checkpoint.
        let next = register(301, b"c", HERE);
        assert_eq!(whole.fold(at + 1, &next), restored.fold(at + 1, &next));
        assert_eq!(restored.state(), whole.state());
    }
}
