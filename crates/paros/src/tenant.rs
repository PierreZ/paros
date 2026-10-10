//! **A tenant's control journal** (#210, `docs/architecture.md` §3.1, §3.3,
//! §3.8): the tenant describes itself in it, and every journal it creates or
//! deletes is an entry in it. Every index above it (the cell's tenant list,
//! the fleet directory) can be rebuilt from it.
//!
//! Its single writer is the **tenant coordinator**. A caller never writes it:
//! it sends a request to the coordinator (`paros::client::journals`), which
//! picks the journal's id and members and writes the request here. This
//! module is the one reading of its entries: the typed [`TenantCommand`]
//! (one record per position) and the pure fold [`TenantControl`] that every
//! machine and every reader runs over the chosen entries in position order.
//!
//! - **Describe.** The first coordinator writes the tenant's id, name,
//!   [`Survives`], the name and [`CellKind`] of its cell and its default
//!   [`Desired`] mode. It is written once; a repeat with the same value
//!   changes nothing and any other one is refused.
//! - **Requests.** A create or a delete carries the caller's idempotency id.
//!   The fold judges it at apply and records its [`RequestOutcome`] under
//!   that id; a later entry with an answered id changes nothing and folds to
//!   [`TenantEvent::Repeated`] with the recorded outcome. So a retry that
//!   crosses a coordinator change acts once.
//! - **Ids.** A journal's id is random, drawn by the coordinator and checked
//!   here: an id the tenant ever used (its control journal's own id, a live
//!   journal's, a deleted one's) folds to [`TenantRefusal::IdTaken`], which
//!   records nothing, and the coordinator redraws under the same request. Ids
//!   are tombstoned on delete and never reused; names are free again once
//!   the delete applied.
//!
//! Every malformed entry folds to a refusal, never a panic: the entries are
//! external input. The outcomes and the tombstones are the part of the
//! state that grows with history (§3.9); everything else is bounded by the
//! live journals.

use std::collections::BTreeMap;

use paros_core::{AcceptorConfig, JournalId, QuorumSystem, TenantId, WriterMode};
use prost::Message as _;

use crate::client::checkpoint::{Checkpointable, Folded};
use crate::rpc::tenant as wire;
use crate::rpc::{
    config_from_proto, config_to_proto, writer_mode_from_proto, writer_mode_to_proto,
};

/// What a tenant survives (#252, §3.4): it picks the kind of cell the tenant
/// may live in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Survives {
    /// The loss of an availability zone: a regional cell.
    #[default]
    Az,
    /// The loss of a region: a multi-region cell.
    Region,
}

impl Survives {
    /// The wire value (`0` is unset).
    #[must_use]
    pub fn to_wire(self) -> u32 {
        match self {
            Survives::Az => 1,
            Survives::Region => 2,
        }
    }

    /// From the wire.
    ///
    /// # Errors
    ///
    /// An unset or unknown value.
    pub fn from_wire(value: u32) -> Result<Self, &'static str> {
        match value {
            1 => Ok(Survives::Az),
            2 => Ok(Survives::Region),
            _ => Err("survives is az or region"),
        }
    }

    /// Its label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Survives::Az => "az",
            Survives::Region => "region",
        }
    }
}

impl std::str::FromStr for Survives {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "az" => Ok(Survives::Az),
            "region" => Ok(Survives::Region),
            _ => Err("survives is az or region"),
        }
    }
}

/// The kind of a cell (#253, §3.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellKind {
    /// One region, across at least three zones.
    Regional {
        /// The region's label (empty while the cell has none).
        region: String,
    },
    /// Three regions, one of them the witness (M12).
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
    fn to_wire(&self) -> wire::CellKind {
        match self {
            CellKind::Regional { region } => wire::CellKind {
                kind: 1,
                regions: vec![region.clone()],
                witness: String::new(),
            },
            CellKind::MultiRegion { regions, witness } => wire::CellKind {
                kind: 2,
                regions: regions.clone(),
                witness: witness.clone(),
            },
        }
    }

    fn from_wire(kind: Option<wire::CellKind>) -> Result<Self, &'static str> {
        let kind = kind.ok_or("a description names no cell kind")?;
        match kind.kind {
            1 => match kind.regions.as_slice() {
                [region] => Ok(CellKind::Regional {
                    region: region.clone(),
                }),
                _ => Err("a regional cell names one region"),
            },
            2 => Ok(CellKind::MultiRegion {
                regions: kind.regions,
                witness: kind.witness,
            }),
            _ => Err("a cell kind is regional or multi-region"),
        }
    }
}

/// A redundancy mode (§3.4): a majority over one, three or five acceptors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Redundancy {
    /// One acceptor: survives no zone (#215), refused for control journals.
    Single,
    /// Three acceptors.
    Double,
    /// Five acceptors.
    Triple,
}

impl Redundancy {
    /// The acceptors it asks for.
    #[must_use]
    pub fn acceptors(self) -> usize {
        match self {
            Redundancy::Single => 1,
            Redundancy::Double => 3,
            Redundancy::Triple => 5,
        }
    }

    /// Its label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Redundancy::Single => "single",
            Redundancy::Double => "double",
            Redundancy::Triple => "triple",
        }
    }
}

/// A desired mode (§3.4): a redundancy or a grid, plus the replica count. A
/// caller names this, never members or an `AcceptorConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Desired {
    /// The acceptor layout.
    pub mode: DesiredMode,
    /// Replicas per journal (recorded; placed by #212).
    pub replicas: u32,
}

/// The acceptor layout of a [`Desired`] mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesiredMode {
    /// A majority over one, three or five acceptors.
    Redundancy(Redundancy),
    /// The opt-in throughput mode: a `rows × cols` grid.
    Grid {
        /// Rows, at least 2.
        rows: u32,
        /// Columns, at least 2.
        cols: u32,
    },
}

impl Desired {
    /// The `double` default.
    pub const DOUBLE: Desired = Desired {
        mode: DesiredMode::Redundancy(Redundancy::Double),
        replicas: 0,
    };

    /// The acceptors it asks for.
    #[must_use]
    pub fn acceptors(&self) -> usize {
        match self.mode {
            DesiredMode::Redundancy(r) => r.acceptors(),
            DesiredMode::Grid { rows, cols } => {
                usize::try_from(rows.saturating_mul(cols)).unwrap_or(usize::MAX)
            }
        }
    }

    /// The quorum system over `members` acceptors this mode asks for.
    #[must_use]
    pub fn quorum_system(&self) -> QuorumSystem {
        match self.mode {
            DesiredMode::Redundancy(_) => QuorumSystem::Majority,
            DesiredMode::Grid { rows, cols } => QuorumSystem::Grid {
                rows: rows as usize,
                cols: cols as usize,
            },
        }
    }

    /// Its label (`double`, `grid 2x3`).
    #[must_use]
    pub fn label(&self) -> String {
        match self.mode {
            DesiredMode::Redundancy(r) => r.as_str().to_string(),
            DesiredMode::Grid { rows, cols } => format!("grid {rows}x{cols}"),
        }
    }

    /// The wire form.
    #[must_use]
    pub fn to_wire(&self) -> wire::Desired {
        let (redundancy, rows, cols) = match self.mode {
            DesiredMode::Redundancy(Redundancy::Single) => (1, 0, 0),
            DesiredMode::Redundancy(Redundancy::Double) => (2, 0, 0),
            DesiredMode::Redundancy(Redundancy::Triple) => (3, 0, 0),
            DesiredMode::Grid { rows, cols } => (0, rows, cols),
        };
        wire::Desired {
            redundancy,
            rows,
            cols,
            replicas: self.replicas,
        }
    }

    /// From the wire.
    ///
    /// # Errors
    ///
    /// No mode, both modes, or a grid under 2×2.
    pub fn from_wire(desired: Option<wire::Desired>) -> Result<Self, &'static str> {
        let desired = desired.ok_or("no desired mode")?;
        let mode = match (desired.redundancy, desired.rows, desired.cols) {
            (1, 0, 0) => DesiredMode::Redundancy(Redundancy::Single),
            (2, 0, 0) => DesiredMode::Redundancy(Redundancy::Double),
            (3, 0, 0) => DesiredMode::Redundancy(Redundancy::Triple),
            (0, rows, cols) if rows >= 2 && cols >= 2 => DesiredMode::Grid { rows, cols },
            _ => return Err("a desired mode is a redundancy or a grid of at least 2x2"),
        };
        Ok(Desired {
            mode,
            replicas: desired.replicas,
        })
    }
}

impl std::str::FromStr for Desired {
    type Err = &'static str;

    /// `single`, `double`, `triple` or `grid:RxC`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s {
            "single" => DesiredMode::Redundancy(Redundancy::Single),
            "double" => DesiredMode::Redundancy(Redundancy::Double),
            "triple" => DesiredMode::Redundancy(Redundancy::Triple),
            grid => {
                let (rows, cols) = grid
                    .strip_prefix("grid:")
                    .and_then(|g| g.split_once('x'))
                    .ok_or("a mode is single, double, triple or grid:RxC")?;
                let rows: u32 = rows.parse().map_err(|_| "grid rows are a number")?;
                let cols: u32 = cols.parse().map_err(|_| "grid columns are a number")?;
                if rows < 2 || cols < 2 {
                    return Err("a grid is at least 2x2");
                }
                DesiredMode::Grid { rows, cols }
            }
        };
        Ok(Desired { mode, replicas: 0 })
    }
}

/// The tenant's own description (the `Describe` entry).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Description {
    /// The tenant's id.
    pub tenant: TenantId,
    /// Its name (opaque bytes).
    pub name: Vec<u8>,
    /// What it survives.
    pub survives: Survives,
    /// The name of the cell that hosts it (empty while the cell has none).
    pub cell_name: Vec<u8>,
    /// The kind of that cell.
    pub cell_kind: CellKind,
    /// The default desired mode of its journals.
    pub desired: Desired,
}

impl Description {
    fn to_wire(&self) -> wire::Describe {
        wire::Describe {
            tenant: self.tenant.0,
            name: self.name.clone(),
            survives: self.survives.to_wire(),
            cell_name: self.cell_name.clone(),
            cell_kind: Some(self.cell_kind.to_wire()),
            desired: Some(self.desired.to_wire()),
        }
    }

    fn from_wire(describe: wire::Describe) -> Result<Self, &'static str> {
        if describe.tenant == 0 {
            return Err("a description names its tenant");
        }
        Ok(Description {
            tenant: TenantId(describe.tenant),
            name: describe.name,
            survives: Survives::from_wire(describe.survives)?,
            cell_name: describe.cell_name,
            cell_kind: CellKind::from_wire(describe.cell_kind)?,
            desired: Desired::from_wire(describe.desired)?,
        })
    }
}

/// One control-journal entry, as the coordinator writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantCommand {
    /// The tenant describes itself, once.
    Describe(Description),
    /// Request `request`: create journal `id`.
    CreateJournal {
        /// The caller's idempotency id.
        request: u64,
        /// The id the coordinator drew.
        id: JournalId,
        /// Its name.
        name: Vec<u8>,
        /// Who may write it (#241), fixed for its life.
        writer: WriterMode,
        /// The mode the caller asked for.
        desired: Desired,
        /// The members the coordinator picked.
        config: AcceptorConfig,
    },
    /// Request `request`: delete journal `id` (`0`: the name named none).
    DeleteJournal {
        /// The caller's idempotency id.
        request: u64,
        /// The journal.
        id: JournalId,
    },
}

impl TenantCommand {
    /// The record the coordinator writes: exactly one per position.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::tenant_entry::Kind;
        let kind = match self {
            TenantCommand::Describe(description) => Kind::Describe(description.to_wire()),
            TenantCommand::CreateJournal {
                request,
                id,
                name,
                writer,
                desired,
                config,
            } => Kind::CreateJournal(wire::CreateJournal {
                request: *request,
                id: id.0,
                name: name.clone(),
                writer: writer_mode_to_proto(*writer).into(),
                desired: Some(desired.to_wire()),
                config: Some(config_to_proto(config)),
            }),
            TenantCommand::DeleteJournal { request, id } => {
                Kind::DeleteJournal(wire::DeleteJournal {
                    request: *request,
                    id: id.0,
                })
            }
        };
        wire::TenantEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a tenant entry, names no kind, or carries a field
    /// out of range.
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::tenant_entry::Kind;
        let entry = wire::TenantEntry::decode(record).map_err(|_| "not a tenant entry")?;
        Ok(match entry.kind.ok_or("a tenant entry names no kind")? {
            Kind::Describe(describe) => TenantCommand::Describe(Description::from_wire(describe)?),
            Kind::CreateJournal(create) => TenantCommand::CreateJournal {
                request: create.request,
                id: JournalId(create.id),
                name: create.name,
                writer: writer_mode_from_proto(create.writer)?,
                desired: Desired::from_wire(create.desired)?,
                config: config_from_proto(create.config)?
                    .ok_or("a created journal names no configuration")?,
            },
            Kind::DeleteJournal(delete) => TenantCommand::DeleteJournal {
                request: delete.request,
                id: JournalId(delete.id),
            },
        })
    }
}

/// A journal the tenant created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantJournal {
    /// Its name.
    pub name: Vec<u8>,
    /// Who may write it.
    pub writer: WriterMode,
    /// The mode it was created under.
    pub desired: Desired,
    /// Its placement: the members the coordinator picked.
    pub config: AcceptorConfig,
    /// The position of the delete that tombstoned it, if any.
    pub deleted_at: Option<u64>,
}

/// What a request came to, recorded under its idempotency id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestOutcome {
    /// The journal was created with this id.
    Created(JournalId),
    /// The journal was deleted.
    Deleted(JournalId),
    /// A live journal holds the name.
    NameTaken(JournalId),
    /// The delete named no live journal.
    UnknownJournal,
}

impl RequestOutcome {
    fn to_wire(self, request: u64) -> wire::OutcomeState {
        let (kind, id) = match self {
            RequestOutcome::Created(id) => (1, id.0),
            RequestOutcome::Deleted(id) => (2, id.0),
            RequestOutcome::NameTaken(id) => (3, id.0),
            RequestOutcome::UnknownJournal => (4, 0),
        };
        wire::OutcomeState { request, kind, id }
    }

    fn from_wire(state: &wire::OutcomeState) -> Result<Self, &'static str> {
        Ok(match (state.kind, state.id) {
            (1, id) if id != 0 => RequestOutcome::Created(JournalId(id)),
            (2, id) if id != 0 => RequestOutcome::Deleted(JournalId(id)),
            (3, id) if id != 0 => RequestOutcome::NameTaken(JournalId(id)),
            (4, 0) => RequestOutcome::UnknownJournal,
            _ => return Err("an outcome state is out of range"),
        })
    }
}

/// What one control-journal record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantEvent {
    /// The tenant described itself.
    Described,
    /// A journal was created.
    Created {
        /// The request.
        request: u64,
        /// Its id.
        id: JournalId,
        /// Its name.
        name: Vec<u8>,
        /// Its placement.
        config: AcceptorConfig,
        /// Who may write it.
        writer: WriterMode,
    },
    /// A journal was tombstoned.
    Deleted {
        /// The request.
        request: u64,
        /// The journal.
        id: JournalId,
    },
    /// A request was answered with a refusal, recorded.
    Answered {
        /// The request.
        request: u64,
        /// Its outcome.
        outcome: RequestOutcome,
    },
    /// A request already answered: nothing changed.
    Repeated {
        /// The request.
        request: u64,
        /// The outcome recorded for it.
        outcome: RequestOutcome,
    },
    /// A checkpoint (#230): every position below `covers_up_to` is in the
    /// state the fold now holds. Never folded from an entry: a reader of the
    /// journal through a [`crate::client::checkpoint::Folder`] reports it.
    Checkpoint {
        /// The checkpoint's horizon, the position of its run's `Begin`.
        covers_up_to: u64,
    },
    /// The entry changed nothing and recorded nothing.
    Refused(TenantRefusal),
}

/// The event a [`Folded`] tenant record is reported as: an entry's own
/// event, a checkpoint's, or a refusal for a record the fold cannot use.
/// `None` for a record the fold skipped (above a gap) or a checkpoint run record.
#[must_use]
pub fn tenant_event(folded: Folded<TenantEvent>) -> Option<TenantEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(TenantEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(TenantEvent::Refused(TenantRefusal::Malformed)),
        Folded::Run | Folded::Skipped => None,
    }
}

/// Why a control-journal entry changed nothing and recorded nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantRefusal {
    /// Not one decodable tenant entry, or an unset id.
    Malformed,
    /// The tenant used this id already: the coordinator redraws.
    IdTaken {
        /// The id asked for.
        id: JournalId,
    },
    /// A second description that differs from the first, or one of another
    /// tenant.
    Redescribed,
}

/// The tenant control journal's fold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantControl {
    /// The tenant (named by the journal's identifier).
    tenant: TenantId,
    /// The control journal's own id: taken from birth.
    control: JournalId,
    description: Option<Description>,
    journals: BTreeMap<JournalId, TenantJournal>,
    /// Live names, to the journal holding each.
    names: BTreeMap<Vec<u8>, JournalId>,
    outcomes: BTreeMap<u64, RequestOutcome>,
    next_seq: u64,
}

impl TenantControl {
    /// The empty fold of tenant `tenant`'s control journal `control`.
    ///
    /// # Panics
    ///
    /// When either id is unset (no id has a default, §3.8).
    #[must_use]
    pub fn new(tenant: TenantId, control: JournalId) -> Self {
        assert!(tenant.is_set(), "a tenant control journal names its tenant");
        assert!(control.is_set(), "a tenant control journal has an id");
        Self {
            tenant,
            control,
            description: None,
            journals: BTreeMap::new(),
            names: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The control journal's own id.
    #[must_use]
    pub fn control(&self) -> JournalId {
        self.control
    }

    /// The next position this fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The tenant's description, once written.
    #[must_use]
    pub fn description(&self) -> Option<&Description> {
        self.description.as_ref()
    }

    /// Whether the tenant used `id` already (its control journal's, or any
    /// journal's, deleted ones included).
    #[must_use]
    pub fn is_taken(&self, id: JournalId) -> bool {
        id == self.control || self.journals.contains_key(&id)
    }

    /// The journal created as `id`, deleted or not.
    #[must_use]
    pub fn get(&self, id: JournalId) -> Option<&TenantJournal> {
        self.journals.get(&id)
    }

    /// Whether `id` was created and then deleted.
    #[must_use]
    pub fn is_deleted(&self, id: JournalId) -> bool {
        self.journals
            .get(&id)
            .is_some_and(|j| j.deleted_at.is_some())
    }

    /// The live journal named `name`.
    #[must_use]
    pub fn named(&self, name: &[u8]) -> Option<JournalId> {
        self.names.get(name).copied()
    }

    /// The recorded outcome of `request`.
    #[must_use]
    pub fn outcome(&self, request: u64) -> Option<RequestOutcome> {
        self.outcomes.get(&request).copied()
    }

    /// Every journal created so far, deleted ones included, in id order.
    pub fn journals(&self) -> impl Iterator<Item = (JournalId, &TenantJournal)> {
        self.journals.iter().map(|(id, j)| (*id, j))
    }

    /// The live journals, in id order.
    pub fn live(&self) -> impl Iterator<Item = (JournalId, &TenantJournal)> {
        self.journals().filter(|(_, j)| j.deleted_at.is_none())
    }

    /// Fold the record at position `seq` (in position order; a gap is
    /// skipped).
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (a programmer error).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> TenantEvent {
        assert!(
            seq >= self.next_seq,
            "a tenant control journal folds in position order"
        );
        self.next_seq = seq + 1;
        let event = match TenantCommand::decode(record) {
            Ok(TenantCommand::Describe(description)) => self.describe(description),
            Ok(TenantCommand::CreateJournal {
                request,
                id,
                name,
                writer,
                desired,
                config,
            }) => self.create(request, id, name, writer, desired, config),
            Ok(TenantCommand::DeleteJournal { request, id }) => self.delete(seq, request, id),
            Err(_) => TenantEvent::Refused(TenantRefusal::Malformed),
        };
        self.assert_invariants();
        event
    }

    fn describe(&mut self, description: Description) -> TenantEvent {
        if description.tenant != self.tenant {
            return TenantEvent::Refused(TenantRefusal::Redescribed);
        }
        match &self.description {
            None => {
                self.description = Some(description);
                TenantEvent::Described
            }
            // A repeat of the same description changes nothing.
            Some(held) if *held == description => TenantEvent::Described,
            Some(_) => TenantEvent::Refused(TenantRefusal::Redescribed),
        }
    }

    fn create(
        &mut self,
        request: u64,
        id: JournalId,
        name: Vec<u8>,
        writer: WriterMode,
        desired: Desired,
        config: AcceptorConfig,
    ) -> TenantEvent {
        if request == 0 || !id.is_set() {
            return TenantEvent::Refused(TenantRefusal::Malformed);
        }
        if let Some(outcome) = self.outcome(request) {
            return TenantEvent::Repeated { request, outcome };
        }
        if self.is_taken(id) {
            return TenantEvent::Refused(TenantRefusal::IdTaken { id });
        }
        if let Some(holder) = self.named(&name) {
            let outcome = RequestOutcome::NameTaken(holder);
            self.outcomes.insert(request, outcome);
            return TenantEvent::Answered { request, outcome };
        }
        self.names.insert(name.clone(), id);
        self.journals.insert(
            id,
            TenantJournal {
                name: name.clone(),
                writer,
                desired,
                config: config.clone(),
                deleted_at: None,
            },
        );
        self.outcomes.insert(request, RequestOutcome::Created(id));
        TenantEvent::Created {
            request,
            id,
            name,
            config,
            writer,
        }
    }

    fn delete(&mut self, seq: u64, request: u64, id: JournalId) -> TenantEvent {
        if request == 0 {
            return TenantEvent::Refused(TenantRefusal::Malformed);
        }
        if let Some(outcome) = self.outcome(request) {
            return TenantEvent::Repeated { request, outcome };
        }
        let Some(journal) = self
            .journals
            .get_mut(&id)
            .filter(|j| j.deleted_at.is_none())
        else {
            let outcome = RequestOutcome::UnknownJournal;
            self.outcomes.insert(request, outcome);
            return TenantEvent::Answered { request, outcome };
        };
        journal.deleted_at = Some(seq);
        let name = journal.name.clone();
        self.names.remove(&name);
        self.outcomes.insert(request, RequestOutcome::Deleted(id));
        TenantEvent::Deleted { request, id }
    }

    /// The fold's invariants: every live name maps to a live journal of that
    /// name, every created outcome to a journal it created, and the control
    /// journal is never a created one.
    fn assert_invariants(&self) {
        assert!(
            !self.journals.contains_key(&self.control),
            "the control journal is never a created journal"
        );
        for (name, id) in &self.names {
            let journal = self.journals.get(id).expect("a live name holds a journal");
            assert!(
                journal.deleted_at.is_none(),
                "a live name holds a live journal"
            );
            assert!(journal.name == *name, "a live name is its journal's name");
        }
        assert!(
            self.journals
                .values()
                .filter(|j| j.deleted_at.is_none())
                .count()
                == self.names.len(),
            "every live journal holds its name"
        );
    }

    fn state_to_wire(&self) -> wire::TenantState {
        wire::TenantState {
            describe: self.description.as_ref().map(Description::to_wire),
            journals: self
                .journals
                .iter()
                .map(|(id, j)| wire::JournalState {
                    id: id.0,
                    name: j.name.clone(),
                    writer: writer_mode_to_proto(j.writer).into(),
                    desired: Some(j.desired.to_wire()),
                    config: Some(config_to_proto(&j.config)),
                    deleted_at: j.deleted_at.map_or(0, |at| at + 1),
                })
                .collect(),
            outcomes: self
                .outcomes
                .iter()
                .map(|(request, outcome)| outcome.to_wire(*request))
                .collect(),
        }
    }
}

impl Checkpointable for TenantControl {
    type Event = TenantEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> TenantEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state =
            wire::TenantState::decode(state).map_err(|_| "a tenant state does not decode")?;
        let description = state.describe.map(Description::from_wire).transpose()?;
        if description
            .as_ref()
            .is_some_and(|d| d.tenant != self.tenant)
        {
            return Err("a tenant state describes another tenant");
        }
        let mut journals = BTreeMap::new();
        let mut names = BTreeMap::new();
        for j in state.journals {
            let id = JournalId(j.id);
            if !id.is_set() || id == self.control {
                return Err("a tenant state names an id it cannot hold");
            }
            let deleted_at = j.deleted_at.checked_sub(1);
            if deleted_at.is_none() && names.insert(j.name.clone(), id).is_some() {
                return Err("a tenant state holds a live name twice");
            }
            journals.insert(
                id,
                TenantJournal {
                    name: j.name,
                    writer: writer_mode_from_proto(j.writer)?,
                    desired: Desired::from_wire(j.desired)?,
                    config: config_from_proto(j.config)?
                        .ok_or("a tenant state journal names no configuration")?,
                    deleted_at,
                },
            );
        }
        let mut outcomes = BTreeMap::new();
        for o in &state.outcomes {
            if o.request == 0 {
                return Err("an outcome names its request");
            }
            outcomes.insert(o.request, RequestOutcome::from_wire(o)?);
        }
        self.description = description;
        self.journals = journals;
        self.names = names;
        self.outcomes = outcomes;
        self.next_seq = covers_up_to + 1;
        self.assert_invariants();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::NodeId;

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        )
    }

    fn create(request: u64, id: u64, name: &[u8]) -> Vec<u8> {
        TenantCommand::CreateJournal {
            request,
            id: JournalId(id),
            name: name.to_vec(),
            writer: WriterMode::Single,
            desired: Desired::DOUBLE,
            config: config(&[1, 2, 3]),
        }
        .encode()
    }

    fn delete(request: u64, id: u64) -> Vec<u8> {
        TenantCommand::DeleteJournal {
            request,
            id: JournalId(id),
        }
        .encode()
    }

    fn fold() -> TenantControl {
        TenantControl::new(TenantId(7), JournalId(70))
    }

    #[test]
    fn every_command_round_trips() {
        let commands = [
            TenantCommand::Describe(Description {
                tenant: TenantId(7),
                name: b"acme".to_vec(),
                survives: Survives::Region,
                cell_name: b"west-1".to_vec(),
                cell_kind: CellKind::MultiRegion {
                    regions: vec!["w".into(), "c".into(), "n".into()],
                    witness: "n".into(),
                },
                desired: Desired {
                    mode: DesiredMode::Grid { rows: 2, cols: 3 },
                    replicas: 2,
                },
            }),
            TenantCommand::CreateJournal {
                request: 9,
                id: JournalId(0x9e37),
                name: b"orders".to_vec(),
                writer: WriterMode::Multi,
                desired: Desired::DOUBLE,
                config: config(&[1, 2, 3]),
            },
            TenantCommand::DeleteJournal {
                request: 10,
                id: JournalId(0x9e37),
            },
        ];
        for command in commands {
            assert_eq!(TenantCommand::decode(&command.encode()), Ok(command));
        }
        assert!(TenantCommand::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn a_create_is_recorded_and_a_retry_acts_once() {
        let mut t = fold();
        assert!(matches!(
            t.fold(0, &create(1, 11, b"a")),
            TenantEvent::Created { .. }
        ));
        assert_eq!(t.outcome(1), Some(RequestOutcome::Created(JournalId(11))));
        // The same request under a fresh draw changes nothing.
        assert_eq!(
            t.fold(1, &create(1, 12, b"a")),
            TenantEvent::Repeated {
                request: 1,
                outcome: RequestOutcome::Created(JournalId(11))
            }
        );
        assert!(t.get(JournalId(12)).is_none());
    }

    #[test]
    fn a_taken_id_records_nothing_and_a_taken_name_is_answered() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        // The control journal's own id is taken from birth.
        assert_eq!(
            t.fold(1, &create(2, 70, b"b")),
            TenantEvent::Refused(TenantRefusal::IdTaken { id: JournalId(70) })
        );
        assert_eq!(t.outcome(2), None);
        assert_eq!(
            t.fold(2, &create(2, 11, b"b")),
            TenantEvent::Refused(TenantRefusal::IdTaken { id: JournalId(11) })
        );
        assert_eq!(
            t.fold(3, &create(3, 13, b"a")),
            TenantEvent::Answered {
                request: 3,
                outcome: RequestOutcome::NameTaken(JournalId(11))
            }
        );
    }

    #[test]
    fn a_delete_tombstones_the_id_and_frees_the_name() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        assert_eq!(
            t.fold(1, &delete(2, 11)),
            TenantEvent::Deleted {
                request: 2,
                id: JournalId(11)
            }
        );
        assert!(t.is_deleted(JournalId(11)));
        assert_eq!(
            t.fold(2, &delete(3, 11)),
            TenantEvent::Answered {
                request: 3,
                outcome: RequestOutcome::UnknownJournal
            }
        );
        // The id stays taken; the name is free.
        assert!(matches!(
            t.fold(3, &create(4, 11, b"a")),
            TenantEvent::Refused(TenantRefusal::IdTaken { .. })
        ));
        assert!(matches!(
            t.fold(4, &create(4, 14, b"a")),
            TenantEvent::Created { .. }
        ));
    }

    #[test]
    fn a_description_is_written_once() {
        let mut t = fold();
        let description = Description {
            tenant: TenantId(7),
            name: b"acme".to_vec(),
            survives: Survives::Az,
            cell_name: Vec::new(),
            cell_kind: CellKind::default(),
            desired: Desired::DOUBLE,
        };
        let entry = TenantCommand::Describe(description.clone()).encode();
        assert_eq!(t.fold(0, &entry), TenantEvent::Described);
        assert_eq!(t.fold(1, &entry), TenantEvent::Described);
        let other = TenantCommand::Describe(Description {
            name: b"other".to_vec(),
            ..description
        })
        .encode();
        assert_eq!(
            t.fold(2, &other),
            TenantEvent::Refused(TenantRefusal::Redescribed)
        );
    }

    #[test]
    fn a_checkpoint_restores_the_same_state() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        t.fold(1, &create(2, 12, b"b"));
        t.fold(2, &delete(3, 11));
        t.fold(3, &create(4, 13, b"b"));
        let mut restored = fold();
        restored
            .restore(4, &t.checkpoint())
            .expect("a checkpoint restores");
        assert_eq!(restored.checkpoint(), t.checkpoint());
        assert_eq!(restored.next_seq(), 5);
        assert_eq!(restored.named(b"b"), Some(JournalId(12)));
        assert_eq!(
            restored.outcome(4),
            Some(RequestOutcome::NameTaken(JournalId(12)))
        );
    }

    #[test]
    fn desired_modes_parse() {
        assert_eq!("double".parse::<Desired>(), Ok(Desired::DOUBLE));
        assert_eq!("grid:2x3".parse::<Desired>().map(|d| d.acceptors()), Ok(6));
        assert!("grid:1x3".parse::<Desired>().is_err());
        assert!("quad".parse::<Desired>().is_err());
    }
}
