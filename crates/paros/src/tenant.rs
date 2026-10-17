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
//!   [`Survives`], the name of its cell and its default [`Desired`] mode
//!   (the cell's kind is the cell's reserved tags in the universe
//!   directory, #575 (key:value tags)). It is written once; a repeat with the same value
//!   changes nothing and any other one is refused.
//! - **Requests.** A create, a delete or a tag request (`SetJournalTags`,
//!   #575: the journal's user tags, all at once; a journal stores no copy of
//!   its tenant's tags) carries the caller's idempotency id.
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
use crate::tags::UserTags;

mod scrub;

pub use scrub::{ScrubCounts, ScrubProgress};

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

/// How a journal's stores commit a batch (#599 (durability mode per
/// journal)): chosen at its create, like its writer mode, recorded in its
/// `CreateJournal` entry, and fixed for its life. Every machine that serves
/// the journal opens its store under it.
///
/// Both are safe: a store reports an ambiguous entry faulty, like a corrupt
/// one, and the journal repairs it from its peers. `Batched` saves one sync
/// per commit, and a crash inside a commit can then leave the last batch
/// ambiguous. A journal repairs a lost batch only while each slot keeps a
/// Phase-1 quorum of clean copies, so a `Batched` journal with one acceptor
/// per partition (`failures_per_partition` 0) can stop for good after a
/// power loss inside a commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoreDurability {
    /// Write the entries, sync, write the persist records, sync: a crash
    /// never leaves a batch ambiguous. The default.
    #[default]
    Ordered,
    /// Write the entries and the persist records, sync once (CLSTORE's
    /// protocol): a crash inside a commit can leave its batch ambiguous.
    Batched,
}

impl StoreDurability {
    /// Every mode, in wire order.
    pub const ALL: [StoreDurability; 2] = [StoreDurability::Ordered, StoreDurability::Batched];

    /// The store's commit protocol.
    #[must_use]
    pub fn journal(self) -> crate::journal::Durability {
        match self {
            StoreDurability::Ordered => crate::journal::Durability::Ordered,
            StoreDurability::Batched => crate::journal::Durability::Batched,
        }
    }

    /// The wire value (`0` is `Ordered`, the default).
    #[must_use]
    pub fn to_proto(self) -> crate::rpc::common::StoreDurability {
        match self {
            StoreDurability::Ordered => crate::rpc::common::StoreDurability::Ordered,
            StoreDurability::Batched => crate::rpc::common::StoreDurability::Batched,
        }
    }

    /// From the wire. An unknown value is refused, never read as ordered.
    ///
    /// # Errors
    ///
    /// The value names no store durability.
    pub fn from_proto(value: i32) -> Result<Self, &'static str> {
        match crate::rpc::common::StoreDurability::try_from(value) {
            Ok(crate::rpc::common::StoreDurability::Ordered) => Ok(StoreDurability::Ordered),
            Ok(crate::rpc::common::StoreDurability::Batched) => Ok(StoreDurability::Batched),
            Err(_) => Err("an unknown store durability"),
        }
    }

    /// Its label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StoreDurability::Ordered => "ordered",
            StoreDurability::Batched => "batched",
        }
    }
}

impl std::str::FromStr for StoreDurability {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StoreDurability::ALL
            .into_iter()
            .find(|mode| mode.as_str() == s)
            .ok_or("store durability is ordered or batched")
    }
}

/// The largest `failures_per_partition` a journal may ask for: seven
/// acceptors per acceptor partition (#523, decided on 2026-10-14).
pub const MAX_FAILURES_PER_PARTITION: u32 = 3;

/// A journal's durability (§3.4, #523): how many failures each acceptor
/// partition survives, and over how many acceptor partitions its writes
/// spread.
///
/// Each acceptor partition has `2F+1` acceptors, so the journal survives any
/// `F` failures, and up to `F` per partition when they are spread. One
/// partition is the plain majority. With more than one, each slot goes to
/// one partition and is chosen by a majority of it; an election and a tail
/// read need a majority of every partition (#532 (acceptor partitions),
/// [`Durability::quorum_system`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Durability {
    /// `F`: the failures each acceptor partition survives, `0..=3`.
    pub failures_per_partition: u32,
    /// `P`: the acceptor partitions, at least 1.
    pub partitions: u32,
}

impl Durability {
    /// Three acceptors in one partition: the default.
    pub const DEFAULT: Durability = Durability {
        failures_per_partition: 1,
        partitions: 1,
    };

    /// A durability of `failures_per_partition` over `partitions`.
    ///
    /// # Errors
    ///
    /// `failures_per_partition` above [`MAX_FAILURES_PER_PARTITION`], or
    /// `partitions` of 0.
    pub fn new(failures_per_partition: u32, partitions: u32) -> Result<Self, &'static str> {
        if failures_per_partition > MAX_FAILURES_PER_PARTITION {
            return Err("failures-per-partition is at most 3");
        }
        if partitions == 0 {
            return Err("partitions is at least 1");
        }
        Ok(Durability {
            failures_per_partition,
            partitions,
        })
    }

    /// The acceptors of one partition: `2F+1`.
    #[must_use]
    pub fn acceptors_per_partition(self) -> usize {
        2 * self.failures_per_partition as usize + 1
    }

    /// The acceptors it asks for: `(2F+1) × P`.
    #[must_use]
    pub fn acceptors(self) -> usize {
        self.acceptors_per_partition()
            .saturating_mul(self.partitions as usize)
    }

    /// The quorum system over its acceptors: a majority for one partition,
    /// else the acceptor partitions (#532 (acceptor partitions)): each
    /// slot's Phase 2 goes to one partition and a majority of it chooses.
    #[must_use]
    pub fn quorum_system(self) -> QuorumSystem {
        if self.partitions == 1 {
            QuorumSystem::Majority
        } else {
            QuorumSystem::partitions(
                self.failures_per_partition as usize,
                self.partitions as usize,
            )
        }
    }

    /// Its label, with the derived numbers and always the partition count:
    /// `1 per partition · 1 partition (any 1) · 3 acceptors`.
    #[must_use]
    pub fn label(self) -> String {
        let f = self.failures_per_partition;
        let p = self.partitions;
        let partitions = if p == 1 { "partition" } else { "partitions" };
        let survives = if p == 1 {
            format!("any {f}")
        } else {
            format!("any {f}, up to {} spread", f.saturating_mul(p))
        };
        let n = self.acceptors();
        let acceptors = if n == 1 { "acceptor" } else { "acceptors" };
        format!("{f} per partition · {p} {partitions} ({survives}) · {n} {acceptors}")
    }
}

/// A desired mode (§3.4): a [`Durability`] plus the replica count. A caller
/// names this, never members or an `AcceptorConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Desired {
    /// The acceptors' durability.
    pub durability: Durability,
    /// Replicas per journal (recorded; placed by #212).
    pub replicas: u32,
}

impl Desired {
    /// The default: one failure in one partition, three acceptors.
    pub const DEFAULT: Desired = Desired {
        durability: Durability::DEFAULT,
        replicas: 0,
    };

    /// The acceptors it asks for.
    #[must_use]
    pub fn acceptors(&self) -> usize {
        self.durability.acceptors()
    }

    /// The quorum system its acceptors are placed under.
    #[must_use]
    pub fn quorum_system(&self) -> QuorumSystem {
        self.durability.quorum_system()
    }

    /// Its label ([`Durability::label`]).
    #[must_use]
    pub fn label(&self) -> String {
        self.durability.label()
    }

    /// The wire form.
    #[must_use]
    pub fn to_wire(&self) -> wire::Desired {
        wire::Desired {
            failures_per_partition: self.durability.failures_per_partition,
            partitions: self.durability.partitions,
            replicas: self.replicas,
        }
    }

    /// From the wire.
    ///
    /// # Errors
    ///
    /// No mode, `failures_per_partition` above 3 or no partition.
    pub fn from_wire(desired: Option<wire::Desired>) -> Result<Self, &'static str> {
        let desired = desired.ok_or("no desired mode")?;
        Ok(Desired {
            durability: Durability::new(desired.failures_per_partition, desired.partitions)?,
            replicas: desired.replicas,
        })
    }
}

impl std::str::FromStr for Desired {
    type Err = &'static str;

    /// `failures-per-partition=F,partitions=P`, either part optional (the
    /// default is `F=1`, `P=1`), in any order.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut f = Durability::DEFAULT.failures_per_partition;
        let mut p = Durability::DEFAULT.partitions;
        for part in s.split(',').map(str::trim).filter(|part| !part.is_empty()) {
            let (key, value) = part
                .split_once('=')
                .ok_or("a mode is failures-per-partition=F,partitions=P")?;
            let value: u32 = value
                .trim()
                .parse()
                .map_err(|_| "a mode's value is a number")?;
            match key.trim() {
                "failures-per-partition" => f = value,
                "partitions" => p = value,
                _ => return Err("a mode names failures-per-partition and partitions"),
            }
        }
        Ok(Desired {
            durability: Durability::new(f, p)?,
            replicas: 0,
        })
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
        /// Its first user tags (#575).
        tags: UserTags,
        /// How its stores commit a batch (#599), fixed for its life.
        store: StoreDurability,
    },
    /// Request `request`: delete journal `id` (`0`: the name named none).
    DeleteJournal {
        /// The caller's idempotency id.
        request: u64,
        /// The journal.
        id: JournalId,
    },
    /// Request `request`: a create of `name` the cell cannot place (#212
    /// (placement)), recorded so a retry reads the refusal back. A live
    /// `name` answers `NameTaken` instead.
    Unplaceable {
        /// The caller's idempotency id.
        request: u64,
        /// The name the create asked for.
        name: Vec<u8>,
    },
    /// Request `request`: live journal `id`'s user tags become `tags`
    /// (#575; `0`: the name named none).
    SetJournalTags {
        /// The caller's idempotency id.
        request: u64,
        /// The journal.
        id: JournalId,
        /// Its user tags, all of them.
        tags: UserTags,
    },
    /// Request `request`: change a desired mode (#214 (tenant modes)), the
    /// tenant's default or a live journal's. A journal target that names no
    /// live journal answers `UnknownJournal`.
    Configure {
        /// The caller's idempotency id.
        request: u64,
        /// Whose mode changes.
        target: ConfigureTarget,
        /// The new mode.
        desired: Desired,
    },
    /// The configuration in force of journal `id`, once the coordinator
    /// applied its desired mode (#214 (tenant modes)). Not a request: it
    /// records no outcome.
    Placed {
        /// The journal.
        id: JournalId,
        /// Its members and quorum system now.
        config: AcceptorConfig,
    },
    /// The scrubber's progress (#362 (background scrubbing)). Not a
    /// request: it records no outcome.
    Scrubbed(ScrubProgress),
}

/// Whose desired mode a `Configure` changes (#214 (tenant modes)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigureTarget {
    /// The default a create that names no mode takes.
    TenantDefault,
    /// A journal of the tenant (`0`: the name named none).
    Journal(JournalId),
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
                tags,
                store,
            } => Kind::CreateJournal(wire::CreateJournal {
                request: *request,
                id: id.0,
                name: name.clone(),
                writer: writer_mode_to_proto(*writer).into(),
                desired: Some(desired.to_wire()),
                config: Some(config_to_proto(config)),
                tags: tags.to_wire(),
                store_durability: store.to_proto().into(),
            }),
            TenantCommand::DeleteJournal { request, id } => {
                Kind::DeleteJournal(wire::DeleteJournal {
                    request: *request,
                    id: id.0,
                })
            }
            TenantCommand::Unplaceable { request, name } => Kind::Unplaceable(wire::Unplaceable {
                request: *request,
                name: name.clone(),
            }),
            TenantCommand::SetJournalTags { request, id, tags } => {
                Kind::SetJournalTags(wire::SetJournalTags {
                    request: *request,
                    id: id.0,
                    tags: tags.to_wire(),
                })
            }
            TenantCommand::Configure {
                request,
                target,
                desired,
            } => {
                let (id, tenant_default) = match target {
                    ConfigureTarget::TenantDefault => (0, true),
                    ConfigureTarget::Journal(id) => (id.0, false),
                };
                Kind::Configure(wire::Configure {
                    request: *request,
                    id,
                    tenant_default,
                    desired: Some(desired.to_wire()),
                })
            }
            TenantCommand::Placed { id, config } => Kind::Placed(wire::Placed {
                id: id.0,
                config: Some(config_to_proto(config)),
            }),
            TenantCommand::Scrubbed(progress) => Kind::Scrubbed(progress.to_wire()),
        };
        wire::TenantEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a tenant entry, names no kind, or carries a field
    /// out of range: a malformed or reserved user tag, or a set over the cap
    /// (#575), included.
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
                tags: user_tags(create.tags)?,
                store: StoreDurability::from_proto(create.store_durability)?,
            },
            Kind::DeleteJournal(delete) => TenantCommand::DeleteJournal {
                request: delete.request,
                id: JournalId(delete.id),
            },
            Kind::Unplaceable(refused) => TenantCommand::Unplaceable {
                request: refused.request,
                name: refused.name,
            },
            Kind::SetJournalTags(set) => TenantCommand::SetJournalTags {
                request: set.request,
                id: JournalId(set.id),
                tags: user_tags(set.tags)?,
            },
            Kind::Configure(configure) => TenantCommand::Configure {
                request: configure.request,
                target: if configure.tenant_default {
                    if configure.id != 0 {
                        return Err("a tenant default names no journal");
                    }
                    ConfigureTarget::TenantDefault
                } else {
                    ConfigureTarget::Journal(JournalId(configure.id))
                },
                desired: Desired::from_wire(configure.desired)?,
            },
            Kind::Placed(placed) => TenantCommand::Placed {
                id: JournalId(placed.id),
                config: config_from_proto(placed.config)?
                    .ok_or("a placed journal names its configuration")?,
            },
            Kind::Scrubbed(scrubbed) => {
                TenantCommand::Scrubbed(ScrubProgress::from_wire(&scrubbed)?)
            }
        })
    }
}

/// A wire list of user tags, judged (#575).
fn user_tags(tags: Vec<crate::rpc::common::Tag>) -> Result<UserTags, &'static str> {
    UserTags::from_wire(tags).map_err(|_| "a user tag set is malformed, reserved or over the cap")
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
    /// Its placement: the members the coordinator picked at its create,
    /// its bootstrap configuration for life.
    pub config: AcceptorConfig,
    /// The configuration in force the coordinator last recorded (#214
    /// (tenant modes)): the create's `config` until a `Placed` entry.
    pub placed: AcceptorConfig,
    /// The position of the delete that tombstoned it, if any.
    pub deleted_at: Option<u64>,
    /// Its user tags (#575): never a copy of its tenant's.
    pub tags: UserTags,
    /// How its stores commit a batch (#599), fixed at its create.
    pub store: StoreDurability,
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
    /// The cell could not place the create (#212 (placement)).
    Unplaceable,
    /// The journal's user tags were set (#575).
    Tagged(JournalId),
    /// A desired mode changed (#214 (tenant modes)): a journal's, or the
    /// tenant default's (`None`).
    Configured(Option<JournalId>),
}

impl RequestOutcome {
    fn to_wire(self, request: u64) -> wire::OutcomeState {
        let (kind, id) = match self {
            RequestOutcome::Created(id) => (1, id.0),
            RequestOutcome::Deleted(id) => (2, id.0),
            RequestOutcome::NameTaken(id) => (3, id.0),
            RequestOutcome::UnknownJournal => (4, 0),
            RequestOutcome::Unplaceable => (5, 0),
            RequestOutcome::Configured(id) => (6, id.map_or(0, |id| id.0)),
            RequestOutcome::Tagged(id) => (7, id.0),
        };
        wire::OutcomeState { request, kind, id }
    }

    fn from_wire(state: &wire::OutcomeState) -> Result<Self, &'static str> {
        Ok(match (state.kind, state.id) {
            (1, id) if id != 0 => RequestOutcome::Created(JournalId(id)),
            (2, id) if id != 0 => RequestOutcome::Deleted(JournalId(id)),
            (3, id) if id != 0 => RequestOutcome::NameTaken(JournalId(id)),
            (4, 0) => RequestOutcome::UnknownJournal,
            (5, 0) => RequestOutcome::Unplaceable,
            (7, id) if id != 0 => RequestOutcome::Tagged(JournalId(id)),
            (6, 0) => RequestOutcome::Configured(None),
            (6, id) => RequestOutcome::Configured(Some(JournalId(id))),
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
        /// How its stores commit a batch (#599).
        store: StoreDurability,
    },
    /// A journal was tombstoned.
    Deleted {
        /// The request.
        request: u64,
        /// The journal.
        id: JournalId,
    },
    /// A journal's user tags were set (#575).
    Tagged {
        /// The request.
        request: u64,
        /// The journal.
        id: JournalId,
    },
    /// A desired mode changed (#214 (tenant modes)): a journal's, or the
    /// tenant default's (`id` `None`).
    Configured {
        /// The request.
        request: u64,
        /// The journal, or `None` for the tenant default.
        id: Option<JournalId>,
        /// The new mode.
        desired: Desired,
    },
    /// The coordinator recorded journal `id`'s configuration in force.
    Placed {
        /// The journal.
        id: JournalId,
        /// Its members and quorum system now.
        config: AcceptorConfig,
    },
    /// The scrubber recorded its progress (#362 (background scrubbing)).
    Scrubbed(ScrubProgress),
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
    /// A `Placed` entry for a journal that is not live.
    NotPlaceable {
        /// The journal it named.
        id: JournalId,
    },
    /// A scrub record behind the one the fold holds (#362): a stale term's
    /// flush.
    ScrubBehind,
}

/// The tenant control journal's fold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantControl {
    /// The tenant (named by the journal's identifier).
    tenant: TenantId,
    /// The control journal's own id: taken from birth.
    control: JournalId,
    description: Option<Description>,
    /// The default desired mode a `Configure` set (#214 (tenant modes)).
    default: Option<Desired>,
    journals: BTreeMap<JournalId, TenantJournal>,
    /// Live names, to the journal holding each.
    names: BTreeMap<Vec<u8>, JournalId>,
    outcomes: BTreeMap<u64, RequestOutcome>,
    /// The scrubber's newest progress (#362 (background scrubbing)).
    scrub: Option<ScrubProgress>,
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
            default: None,
            journals: BTreeMap::new(),
            names: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            scrub: None,
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

    /// The scrubber's newest progress, once it recorded some (#362
    /// (background scrubbing)).
    #[must_use]
    pub fn scrub(&self) -> Option<&ScrubProgress> {
        self.scrub.as_ref()
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

    /// The desired mode a create that names none takes (#214 (tenant
    /// modes)): the last `Configure` of the tenant default, else the
    /// description's, else [`Desired::DEFAULT`].
    #[must_use]
    pub fn default_desired(&self) -> Desired {
        self.default
            .or_else(|| self.description.as_ref().map(|d| d.desired))
            .unwrap_or(Desired::DEFAULT)
    }

    /// The largest `failures_per_partition` of the tenant: its default and
    /// every live journal's (§3.4: its matchmaker set has `2F+1`
    /// matchmakers, #214 (tenant modes)).
    #[must_use]
    pub fn largest_failures(&self) -> u32 {
        self.live()
            .map(|(_, j)| j.desired.durability.failures_per_partition)
            .chain(std::iter::once(
                self.default_desired().durability.failures_per_partition,
            ))
            .max()
            .unwrap_or(Durability::DEFAULT.failures_per_partition)
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
                tags,
                store,
            }) => self.create(request, id, name, (writer, desired, config, store), tags),
            Ok(TenantCommand::DeleteJournal { request, id }) => self.delete(seq, request, id),
            Ok(TenantCommand::Unplaceable { request, name }) => self.refuse(request, &name),
            Ok(TenantCommand::SetJournalTags { request, id, tags }) => {
                self.set_tags(request, id, tags)
            }
            Ok(TenantCommand::Configure {
                request,
                target,
                desired,
            }) => self.configure(request, target, desired),
            Ok(TenantCommand::Placed { id, config }) => self.place(id, config),
            Ok(TenantCommand::Scrubbed(progress)) => self.scrubbed(progress),
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
        (writer, desired, config, store): (WriterMode, Desired, AcceptorConfig, StoreDurability),
        tags: UserTags,
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
                placed: config.clone(),
                deleted_at: None,
                tags,
                store,
            },
        );
        self.outcomes.insert(request, RequestOutcome::Created(id));
        TenantEvent::Created {
            request,
            id,
            name,
            config,
            writer,
            store,
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

    /// A tag request (#575): judged at apply like a create, its outcome
    /// recorded under its id. The tags were judged at decode.
    fn set_tags(&mut self, request: u64, id: JournalId, tags: UserTags) -> TenantEvent {
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
        journal.tags = tags;
        self.outcomes.insert(request, RequestOutcome::Tagged(id));
        TenantEvent::Tagged { request, id }
    }

    /// A create the cell could not place: recorded `Unplaceable`, or
    /// `NameTaken` when a live journal holds the name (the fold judges the
    /// name as a create would).
    fn refuse(&mut self, request: u64, name: &[u8]) -> TenantEvent {
        if request == 0 {
            return TenantEvent::Refused(TenantRefusal::Malformed);
        }
        if let Some(outcome) = self.outcome(request) {
            return TenantEvent::Repeated { request, outcome };
        }
        let outcome = match self.named(name) {
            Some(holder) => RequestOutcome::NameTaken(holder),
            None => RequestOutcome::Unplaceable,
        };
        self.outcomes.insert(request, outcome);
        TenantEvent::Answered { request, outcome }
    }

    /// A `Configure` (#214 (tenant modes)): the tenant default changes, or
    /// a live journal's desired mode does. Its writer mode and its
    /// bootstrap configuration never change.
    fn configure(
        &mut self,
        request: u64,
        target: ConfigureTarget,
        desired: Desired,
    ) -> TenantEvent {
        if request == 0 {
            return TenantEvent::Refused(TenantRefusal::Malformed);
        }
        if let Some(outcome) = self.outcome(request) {
            return TenantEvent::Repeated { request, outcome };
        }
        let id = match target {
            ConfigureTarget::TenantDefault => {
                self.default = Some(desired);
                None
            }
            ConfigureTarget::Journal(id) => {
                let Some(journal) = self
                    .journals
                    .get_mut(&id)
                    .filter(|j| j.deleted_at.is_none())
                else {
                    let outcome = RequestOutcome::UnknownJournal;
                    self.outcomes.insert(request, outcome);
                    return TenantEvent::Answered { request, outcome };
                };
                journal.desired = desired;
                Some(id)
            }
        };
        self.outcomes
            .insert(request, RequestOutcome::Configured(id));
        TenantEvent::Configured {
            request,
            id,
            desired,
        }
    }

    /// A `Placed` (#214 (tenant modes)): the configuration in force of a
    /// live journal, recorded once the coordinator applied its mode.
    fn place(&mut self, id: JournalId, config: AcceptorConfig) -> TenantEvent {
        let Some(journal) = self
            .journals
            .get_mut(&id)
            .filter(|j| j.deleted_at.is_none())
        else {
            return TenantEvent::Refused(TenantRefusal::NotPlaceable { id });
        };
        journal.placed = config.clone();
        TenantEvent::Placed { id, config }
    }

    /// A `Scrubbed` (#362): the newest progress, never one behind it.
    fn scrubbed(&mut self, progress: ScrubProgress) -> TenantEvent {
        if self
            .scrub
            .is_some_and(|held| progress.cursor() < held.cursor())
        {
            return TenantEvent::Refused(TenantRefusal::ScrubBehind);
        }
        self.scrub = Some(progress);
        TenantEvent::Scrubbed(progress)
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
                    tags: j.tags.to_wire(),
                    placed: (j.placed != j.config).then(|| config_to_proto(&j.placed)),
                    store_durability: j.store.to_proto().into(),
                })
                .collect(),
            outcomes: self
                .outcomes
                .iter()
                .map(|(request, outcome)| outcome.to_wire(*request))
                .collect(),
            default_desired: self.default.as_ref().map(Desired::to_wire),
            scrub: self.scrub.map(ScrubProgress::to_wire),
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
            let config = config_from_proto(j.config)?
                .ok_or("a tenant state journal names no configuration")?;
            let placed = config_from_proto(j.placed)?.unwrap_or_else(|| config.clone());
            journals.insert(
                id,
                TenantJournal {
                    name: j.name,
                    writer: writer_mode_from_proto(j.writer)?,
                    desired: Desired::from_wire(j.desired)?,
                    config,
                    placed,
                    deleted_at,
                    tags: user_tags(j.tags)?,
                    store: StoreDurability::from_proto(j.store_durability)?,
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
        let default = state
            .default_desired
            .map(|d| Desired::from_wire(Some(d)))
            .transpose()?;
        self.description = description;
        self.default = default;
        self.journals = journals;
        self.names = names;
        self.outcomes = outcomes;
        self.scrub = state
            .scrub
            .as_ref()
            .map(ScrubProgress::from_wire)
            .transpose()?;
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
            desired: Desired::DEFAULT,
            config: config(&[1, 2, 3]),
            tags: UserTags::none(),
            store: StoreDurability::Ordered,
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
                desired: Desired {
                    durability: Durability {
                        failures_per_partition: 2,
                        partitions: 3,
                    },
                    replicas: 2,
                },
            }),
            TenantCommand::CreateJournal {
                request: 9,
                id: JournalId(0x9e37),
                name: b"orders".to_vec(),
                writer: WriterMode::Multi,
                desired: Desired::DEFAULT,
                config: config(&[1, 2, 3]),
                tags: UserTags::parse(&["team:payments"]).unwrap(),
                store: StoreDurability::Batched,
            },
            TenantCommand::SetJournalTags {
                request: 12,
                id: JournalId(0x9e37),
                tags: UserTags::parse(&["env:prod", "app:orders"]).unwrap(),
            },
            TenantCommand::DeleteJournal {
                request: 10,
                id: JournalId(0x9e37),
            },
            TenantCommand::Unplaceable {
                request: 11,
                name: b"orders".to_vec(),
            },
            TenantCommand::Configure {
                request: 12,
                target: ConfigureTarget::Journal(JournalId(0x9e37)),
                desired: "failures-per-partition=0,partitions=3"
                    .parse()
                    .expect("a mode"),
            },
            TenantCommand::Configure {
                request: 13,
                target: ConfigureTarget::TenantDefault,
                desired: Desired::DEFAULT,
            },
            TenantCommand::Placed {
                id: JournalId(0x9e37),
                config: config(&[1, 2, 4]),
            },
            TenantCommand::Scrubbed(ScrubProgress::default().next_pass().advanced(
                ScrubCounts {
                    checked: 6,
                    damaged: 1,
                    repaired: 1,
                    mismatched: 0,
                },
                JournalId(0x9e37),
                4,
            )),
        ];
        for command in commands {
            assert_eq!(TenantCommand::decode(&command.encode()), Ok(command));
        }
        assert!(TenantCommand::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn a_tag_request_is_judged_at_apply_and_acts_once() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        let set = |request: u64, id: u64, texts: &[&str]| {
            TenantCommand::SetJournalTags {
                request,
                id: JournalId(id),
                tags: UserTags::parse(texts).unwrap(),
            }
            .encode()
        };
        assert_eq!(
            t.fold(1, &set(2, 11, &["team:payments"])),
            TenantEvent::Tagged {
                request: 2,
                id: JournalId(11)
            }
        );
        assert_eq!(
            t.fold(2, &set(3, 11, &[])),
            TenantEvent::Tagged {
                request: 3,
                id: JournalId(11)
            }
        );
        // The first request again changes nothing: the later one stays.
        assert!(matches!(
            t.fold(3, &set(2, 11, &["team:payments"])),
            TenantEvent::Repeated { .. }
        ));
        assert!(t.get(JournalId(11)).unwrap().tags.is_empty());
        assert_eq!(
            t.fold(4, &set(4, 12, &["a:b"])),
            TenantEvent::Answered {
                request: 4,
                outcome: RequestOutcome::UnknownJournal
            }
        );
        // A reserved key is refused at apply and records nothing.
        let mut forged = wire::TenantEntry::decode(set(5, 11, &["a:b"]).as_slice()).unwrap();
        if let Some(wire::tenant_entry::Kind::SetJournalTags(set)) = &mut forged.kind {
            set.tags[0].key = "paros/movable".into();
        }
        assert_eq!(
            t.fold(5, &forged.encode_to_vec()),
            TenantEvent::Refused(TenantRefusal::Malformed)
        );
        assert_eq!(t.outcome(5), None);
        // A deleted journal is unknown to a tag request.
        t.fold(6, &delete(6, 11));
        assert_eq!(
            t.fold(7, &set(7, 11, &["a:b"])),
            TenantEvent::Answered {
                request: 7,
                outcome: RequestOutcome::UnknownJournal
            }
        );
        let mut restored = fold();
        restored.restore(7, &t.checkpoint()).unwrap();
        assert_eq!(restored, t);
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

    /// #599 (durability mode per journal): a create records its store
    /// durability, a configure keeps it, and a checkpoint carries it.
    #[test]
    fn a_store_durability_is_fixed_at_create() {
        let mut t = fold();
        let batched = TenantCommand::CreateJournal {
            request: 1,
            id: JournalId(11),
            name: b"fast".to_vec(),
            writer: WriterMode::Single,
            desired: Desired::DEFAULT,
            config: config(&[1, 2, 3]),
            tags: UserTags::none(),
            store: StoreDurability::Batched,
        };
        assert!(matches!(
            t.fold(0, &batched.encode()),
            TenantEvent::Created {
                store: StoreDurability::Batched,
                ..
            }
        ));
        t.fold(1, &create(2, 12, b"plain"));
        t.fold(
            2,
            &configure(3, ConfigureTarget::Journal(JournalId(11)), "partitions=2"),
        );
        assert_eq!(
            t.get(JournalId(11)).unwrap().store,
            StoreDurability::Batched
        );
        assert_eq!(
            t.get(JournalId(12)).unwrap().store,
            StoreDurability::Ordered
        );
        let mut restored = fold();
        restored.restore(3, &t.checkpoint()).unwrap();
        assert_eq!(restored.checkpoint(), t.checkpoint());
        assert_eq!(
            restored.get(JournalId(11)).unwrap().store,
            StoreDurability::Batched
        );
        // Ordered is the zero value: a create that names none is ordered.
        assert_eq!(StoreDurability::default(), StoreDurability::Ordered);
        assert_eq!(StoreDurability::from_proto(0), Ok(StoreDurability::Ordered));
        assert!(StoreDurability::from_proto(7).is_err());
        for mode in StoreDurability::ALL {
            assert_eq!(mode.as_str().parse(), Ok(mode));
        }
        assert!("fast".parse::<StoreDurability>().is_err());
    }

    fn configure(request: u64, target: ConfigureTarget, desired: &str) -> Vec<u8> {
        TenantCommand::Configure {
            request,
            target,
            desired: desired.parse().expect("a mode"),
        }
        .encode()
    }

    #[test]
    fn a_configure_changes_a_live_journal_or_the_default_and_a_retry_acts_once() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        let wide = "failures-per-partition=2";
        assert_eq!(
            t.fold(
                1,
                &configure(2, ConfigureTarget::Journal(JournalId(11)), wide)
            ),
            TenantEvent::Configured {
                request: 2,
                id: Some(JournalId(11)),
                desired: wide.parse().expect("a mode"),
            }
        );
        let journal = t.get(JournalId(11)).expect("created");
        assert_eq!(journal.desired.durability.failures_per_partition, 2);
        // The writer mode and the bootstrap configuration never change.
        assert_eq!(journal.writer, WriterMode::Single);
        assert_eq!(journal.config, config(&[1, 2, 3]));
        assert_eq!(t.largest_failures(), 2);
        // A retry under the same id reads back its outcome.
        assert_eq!(
            t.fold(
                2,
                &configure(2, ConfigureTarget::Journal(JournalId(11)), "partitions=3")
            ),
            TenantEvent::Repeated {
                request: 2,
                outcome: RequestOutcome::Configured(Some(JournalId(11)))
            }
        );
        // A name that named no live journal is answered unknown.
        assert_eq!(
            t.fold(
                3,
                &configure(3, ConfigureTarget::Journal(JournalId(0)), wide)
            ),
            TenantEvent::Answered {
                request: 3,
                outcome: RequestOutcome::UnknownJournal
            }
        );
        // The tenant default changes what a create that names none takes.
        assert_eq!(t.default_desired(), Desired::DEFAULT);
        t.fold(
            4,
            &configure(4, ConfigureTarget::TenantDefault, "partitions=2"),
        );
        assert_eq!(t.default_desired().durability.partitions, 2);
        assert_eq!(t.outcome(4), Some(RequestOutcome::Configured(None)));
        // A deleted journal is configured no more.
        t.fold(5, &delete(5, 11));
        assert_eq!(
            t.fold(
                6,
                &configure(6, ConfigureTarget::Journal(JournalId(11)), wide)
            ),
            TenantEvent::Answered {
                request: 6,
                outcome: RequestOutcome::UnknownJournal
            }
        );
        assert_eq!(t.largest_failures(), 1);
    }

    #[test]
    fn a_placement_is_recorded_for_a_live_journal_and_kept_across_a_checkpoint() {
        let mut t = fold();
        t.fold(0, &create(1, 11, b"a"));
        let placed = TenantCommand::Placed {
            id: JournalId(11),
            config: config(&[1, 2, 4]),
        };
        assert_eq!(
            t.fold(1, &placed.encode()),
            TenantEvent::Placed {
                id: JournalId(11),
                config: config(&[1, 2, 4]),
            }
        );
        assert_eq!(
            t.get(JournalId(11)).expect("created").placed,
            config(&[1, 2, 4])
        );
        assert_eq!(
            t.get(JournalId(11)).expect("created").config,
            config(&[1, 2, 3])
        );
        t.fold(
            2,
            &configure(
                2,
                ConfigureTarget::TenantDefault,
                "failures-per-partition=3",
            ),
        );
        let mut restored = fold();
        restored
            .restore(2, &t.checkpoint())
            .expect("a checkpoint restores");
        assert_eq!(restored, t);
        // A placement for a journal that is not live changes nothing.
        let stray = TenantCommand::Placed {
            id: JournalId(99),
            config: config(&[1]),
        };
        assert_eq!(
            t.fold(3, &stray.encode()),
            TenantEvent::Refused(TenantRefusal::NotPlaceable { id: JournalId(99) })
        );
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
    fn an_unplaceable_create_is_recorded_and_a_live_name_answers_name_taken() {
        let mut t = fold();
        let refuse = |request: u64, name: &[u8]| {
            TenantCommand::Unplaceable {
                request,
                name: name.to_vec(),
            }
            .encode()
        };
        assert_eq!(
            t.fold(0, &refuse(1, b"a")),
            TenantEvent::Answered {
                request: 1,
                outcome: RequestOutcome::Unplaceable
            }
        );
        // A retry reads the refusal back, even as a create.
        assert_eq!(
            t.fold(1, &create(1, 11, b"a")),
            TenantEvent::Repeated {
                request: 1,
                outcome: RequestOutcome::Unplaceable
            }
        );
        t.fold(2, &create(2, 11, b"a"));
        assert_eq!(
            t.fold(3, &refuse(3, b"a")),
            TenantEvent::Answered {
                request: 3,
                outcome: RequestOutcome::NameTaken(JournalId(11))
            }
        );
        let mut restored = fold();
        restored
            .restore(3, &t.checkpoint())
            .expect("a checkpoint restores");
        assert_eq!(restored.outcome(1), Some(RequestOutcome::Unplaceable));
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
            desired: Desired::DEFAULT,
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
    fn a_scrub_record_moves_forward_and_survives_a_checkpoint() {
        let mut t = fold();
        let first = ScrubProgress::default().next_pass();
        let ahead = first.advanced(ScrubCounts::default(), JournalId(11), 8);
        assert_eq!(
            t.fold(0, &TenantCommand::Scrubbed(ahead).encode()),
            TenantEvent::Scrubbed(ahead)
        );
        // A stale term's flush behind it changes nothing.
        assert_eq!(
            t.fold(1, &TenantCommand::Scrubbed(first).encode()),
            TenantEvent::Refused(TenantRefusal::ScrubBehind)
        );
        assert_eq!(t.scrub(), Some(&ahead));
        let mut restored = fold();
        restored
            .restore(1, &t.checkpoint())
            .expect("a checkpoint restores");
        assert_eq!(restored.scrub(), Some(&ahead));
        assert_eq!(restored.checkpoint(), t.checkpoint());
    }

    #[test]
    fn desired_modes_parse() {
        assert_eq!("".parse::<Desired>(), Ok(Desired::DEFAULT));
        assert_eq!(
            "failures-per-partition=1".parse::<Desired>(),
            Ok(Desired::DEFAULT)
        );
        assert_eq!(
            "partitions=3,failures-per-partition=1"
                .parse::<Desired>()
                .map(|d| d.acceptors()),
            Ok(9)
        );
        assert_eq!(
            "failures-per-partition=3"
                .parse::<Desired>()
                .map(|d| d.acceptors()),
            Ok(7)
        );
        assert_eq!(
            "failures-per-partition=0"
                .parse::<Desired>()
                .map(|d| d.acceptors()),
            Ok(1)
        );
        assert!("failures-per-partition=4".parse::<Desired>().is_err());
        assert!("partitions=0".parse::<Desired>().is_err());
        assert!("double".parse::<Desired>().is_err());
        assert!("copies=3".parse::<Desired>().is_err());
    }

    #[test]
    fn a_durability_shows_its_partition_count() {
        assert_eq!(
            Durability::DEFAULT.label(),
            "1 per partition · 1 partition (any 1) · 3 acceptors"
        );
        let spread = Durability::new(1, 3).expect("a durability");
        assert_eq!(
            spread.label(),
            "1 per partition · 3 partitions (any 1, up to 3 spread) · 9 acceptors"
        );
        assert_eq!(
            spread.quorum_system(),
            QuorumSystem::Grid {
                rows: 3,
                cols: 3,
                acks: 2
            }
        );
        assert_eq!(Durability::DEFAULT.quorum_system(), QuorumSystem::Majority);
        let lone = Durability::new(0, 1).expect("a durability");
        assert_eq!(
            lone.label(),
            "0 per partition · 1 partition (any 0) · 1 acceptor"
        );
    }

    #[test]
    fn a_durability_round_trips_the_wire() {
        for f in 0..=MAX_FAILURES_PER_PARTITION {
            for p in 1..=4 {
                let desired = Desired {
                    durability: Durability::new(f, p).expect("a durability"),
                    replicas: p,
                };
                assert_eq!(Desired::from_wire(Some(desired.to_wire())), Ok(desired));
            }
        }
        let mut wire = Desired::DEFAULT.to_wire();
        wire.partitions = 0;
        assert!(Desired::from_wire(Some(wire)).is_err());
    }
}
