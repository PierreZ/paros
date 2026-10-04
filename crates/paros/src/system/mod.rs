//! The **system journals** (#189): the directory (the user tenant's control
//! journal), the node registry (the cell tenant's control journal, #235) and
//! meta (the meta tenant's control journal, #229).
//! A service must create and delete journals, and add
//! and retire nodes, while it runs; paros already has the right tool for
//! both — a replicated log — so both lists are journals of their own, and
//! every node learns them by reading them.
//!
//! This module is the one reading of their entries: the typed
//! [`SystemCommand`] a client writes (one record per position, framed by
//! [`SystemCommand::encode`]), and the three pure folds — [`Directory`],
//! [`Registry`] and [`Meta`] — that every node, and every client reading back its own
//! request, runs over the chosen entries in position order. A fold is a function
//! of the log alone, so every reader that has folded a prefix agrees on it.
//!
//! **This is not an application** (#186): paros still decides nothing about
//! the bytes of a user journal. The system journals are paros's own control
//! plane, like the matchmaker registry; the core keeps their entries as
//! opaque as any other, and only this module and the driver read them.
//!
//! - **Directory.** A created journal's id is random (#226, #235): its
//!   creator draws it from the user range ([`JournalId::FIRST_USER`] and up)
//!   and this fold, the tenant's single writer of journal ids, checks it at
//!   apply. An id outside the user range or naming a journal the deployment
//!   was booted with (its *genesis* journals) folds to
//!   [`DirectoryRefusal::Reserved`]; an id the directory already created —
//!   deleted or not, ids are never reused — folds to
//!   [`DirectoryRefusal::IdTaken`], and the creator redraws. Never a log
//!   position: an id must not change when its tenant moves. Of two creates
//!   with one name the lower position wins; the other folds to
//!   [`DirectoryRefusal::NameTaken`], which its creator reads back. Names
//!   are opaque bytes.
//! - **Registry** (#211), keyed by `node_id` (random, minted at format). The
//!   node pool is the genesis pool plus every registered node not yet
//!   retired ([`Registry::pool`]). A node registers with its class
//!   (`storage` or `stateless`) and its capacity (role slots of its class);
//!   a reboot registers the same id again, updating address and capacity,
//!   never class. It is drained, then retired — an id is never reused, a
//!   retired one included. Capacity **bookings** (`BookCapacity`, written
//!   by the cell coordinator) are judged at apply: a slot of the node's own
//!   class only, never past its capacity. The registry is checkpointed with
//!   `paros::client::checkpoint` (#230): its state is the latest entry per
//!   `node_id` and the live bookings.
//!
//!   Not yet (follow-ups of #211): a machine registering itself at start
//!   and on a cadence (it needs the cell coordinator of #225 as the
//!   registry's single writer), the `InterfaceRef` of #216, and placement
//!   by booking (#212). The directory is not checkpointed yet (#229).
//! - **Meta** (#229, `docs/architecture.md` §3.7): the fleet's directory,
//!   the meta tenant's control journal. Every cell with its state, every
//!   tenant with its cell assignment and state. A fleet operation is an
//!   idempotent state machine over meta and the cell (FDB's metacluster):
//!   a tenant is `REGISTERING` in meta, hosted by its cell, then `READY`;
//!   `paros::client::fleet` runs it, and a re-run resumes. The registration
//!   is recorded on both sides — meta's cell entry, the cell's
//!   `RegisterFleet` in the registry — and every step names the fleet and
//!   cell it believes in, refused at apply when they differ. Meta is
//!   checkpointed like the registry.
//!
//! Every malformed entry — a record that does not decode,
//! a configuration that does not admit its quorum system, a registry entry
//! in the directory — folds to a refusal, never a panic: the entries are
//! external input.

mod command;
mod directory;
mod meta;
mod registry;

pub use command::{FleetContext, SystemCommand};
pub use directory::{CreatedJournal, Directory, DirectoryEvent, DirectoryRefusal};
pub use meta::{
    CellEntry, CellState, METADATA_VERSION, Meta, MetaEvent, MetaRefusal, TenantEntry, TenantState,
    meta_event,
};
pub use registry::{
    Booking, FleetRegistration, HostedTenant, NodeStanding, RegisteredNode, Registry,
    RegistryEvent, RegistryRefusal, registry_event,
};

use paros_core::{JournalKey, TenantId};

pub use crate::machine::Class;

/// The directory: the journals created and deleted at runtime — the user
/// tenant's own control journal, which holds its journal names (#235,
/// `docs/architecture.md` §3.1). One user tenant today ([`TenantId::default`]);
/// tenant creation is #210.
pub const DIRECTORY: JournalKey = JournalKey::control(TenantId::FIRST_USER);

/// The node registry: the nodes registered, drained and retired at runtime —
/// the cell tenant's control journal (#235, §3.1).
pub const REGISTRY: JournalKey = JournalKey::control(TenantId::CELL);

/// Meta: the fleet's directory of cells and tenants — the meta tenant's
/// control journal (#229, §3.7).
pub const META: JournalKey = JournalKey::control(TenantId::META);

/// Whether `journal` is one of the system journals.
#[must_use]
pub fn is_system(journal: JournalKey) -> bool {
    journal == DIRECTORY || journal == REGISTRY || journal == META
}

/// What one system-journal record folded to — the directory's, the
/// registry's or meta's event — as the driver reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    /// A directory record.
    Directory(DirectoryEvent),
    /// A registry record.
    Registry(RegistryEvent),
    /// A meta record.
    Meta(MetaEvent),
}
