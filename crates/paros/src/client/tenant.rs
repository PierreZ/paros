//! A tenant's journals (#210): creating and deleting them through the
//! tenant's **control journal** (`tenant/1`), the self-describing directory
//! of its journals that every node follows — a node a created journal's
//! configuration names starts it.
//!
//! Every call claims the control journal as the caller (the tenant
//! coordinator's single writer, until #212 elects one), folds it to the tail
//! through [`Checkpointer`] — so a checkpointed control journal is read from
//! its floor — and writes one entry, whose verdict is what the writer's own
//! fold of it says: the verdict every node folds at that position. A name
//! race between two creators is decided by position: the later one reads
//! back `NameTaken`. Ids are the caller's to draw, from the user range; a
//! taken one is reported, never redrawn here.
//!
//! Like the rest of the client: provider-generic, wasm-safe, no randomness,
//! every outcome typed.

use moonpool_core::Providers;
use paros_core::{AcceptorConfig, JournalId, JournalKey, TenantId};

use super::Client;
use super::checkpoint::{Applied, Checkpointer, Folder, LoadOutcome, OpenOutcome};
use crate::system::{Directory, DirectoryEvent, DirectoryRefusal, SystemCommand};

/// What [`create_journal`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateJournalOutcome {
    /// The tenant's directory created the journal under the drawn id.
    Created {
        /// The journal, framed by its tenant.
        journal: JournalKey,
    },
    /// A live journal of the tenant holds the name already.
    NameTaken {
        /// The journal that holds it.
        winner: JournalKey,
    },
    /// The id is reserved or was used before in this tenant: draw again.
    IdTaken,
    /// The directory refused the entry for another reason.
    Refused(DirectoryRefusal),
    /// The control journal was not claimed, not folded, or the entry is not
    /// known written: nothing is known to have happened (a write whose
    /// answer was lost may still land; a re-run reads it back).
    Unavailable,
}

/// What [`delete_journal`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeleteJournalOutcome {
    /// The journal is tombstoned: it is gone on every node, its id is never
    /// reused.
    Deleted,
    /// No live journal of the tenant has the name.
    Unknown,
    /// See [`CreateJournalOutcome::Unavailable`].
    Unavailable,
}

/// Claim and fold `tenant`'s control journal as `owner`.
async fn open<P: Providers>(
    client: &Client<P>,
    owner: u64,
    tenant: TenantId,
    first: usize,
) -> Option<Checkpointer<Directory>> {
    let mut writer = Checkpointer::new(
        JournalKey::control(tenant),
        owner,
        Directory::new([]),
        client.tunables().checkpoint_policy(),
    );
    matches!(writer.open(client, first).await, OpenOutcome::Open { .. }).then_some(writer)
}

/// Create the journal `name` in `tenant` under the id `id` (the caller's
/// draw), over the static configuration `config`, writing as `owner`.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = tenant.0, journal = id.0))]
pub async fn create_journal<P: Providers>(
    client: &Client<P>,
    owner: u64,
    tenant: TenantId,
    id: JournalId,
    name: Vec<u8>,
    config: AcceptorConfig,
    first: usize,
) -> CreateJournalOutcome {
    let Some(mut writer) = open(client, owner, tenant, first).await else {
        return CreateJournalOutcome::Unavailable;
    };
    let command = SystemCommand::CreateJournal { id, name, config };
    match writer.apply(client, command.encode(), first).await {
        Applied::Folded(DirectoryEvent::Created { id, .. }) => CreateJournalOutcome::Created {
            journal: JournalKey::new(tenant, id),
        },
        Applied::Folded(DirectoryEvent::Refused(DirectoryRefusal::NameTaken { winner })) => {
            CreateJournalOutcome::NameTaken {
                winner: JournalKey::new(tenant, winner),
            }
        }
        Applied::Folded(DirectoryEvent::Refused(
            DirectoryRefusal::IdTaken { .. } | DirectoryRefusal::Reserved { .. },
        )) => CreateJournalOutcome::IdTaken,
        Applied::Folded(DirectoryEvent::Refused(refusal)) => CreateJournalOutcome::Refused(refusal),
        Applied::Folded(_) | Applied::NotFolded(_) => CreateJournalOutcome::Unavailable,
    }
}

/// Delete `tenant`'s live journal named `name`, writing as `owner`.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = tenant.0))]
pub async fn delete_journal<P: Providers>(
    client: &Client<P>,
    owner: u64,
    tenant: TenantId,
    name: &[u8],
    first: usize,
) -> DeleteJournalOutcome {
    let Some(mut writer) = open(client, owner, tenant, first).await else {
        return DeleteJournalOutcome::Unavailable;
    };
    let Some(id) = writer
        .state()
        .journals()
        .find(|(_, j)| j.deleted_at.is_none() && j.name == name)
        .map(|(id, _)| id)
    else {
        return DeleteJournalOutcome::Unknown;
    };
    let command = SystemCommand::DeleteJournal { id };
    match writer.apply(client, command.encode(), first).await {
        Applied::Folded(DirectoryEvent::Deleted { .. }) => DeleteJournalOutcome::Deleted,
        Applied::Folded(_) => DeleteJournalOutcome::Unknown,
        Applied::NotFolded(_) => DeleteJournalOutcome::Unavailable,
    }
}

/// `tenant`'s control journal, folded from its floor to its tail; `None`
/// when no server served it whole.
#[tracing::instrument(level = "debug", skip_all, fields(tenant = tenant.0))]
pub async fn load_directory<P: Providers>(
    client: &Client<P>,
    tenant: TenantId,
    first: usize,
) -> Option<Directory> {
    let mut folder = Folder::new(Directory::new([]));
    match super::checkpoint::load(&mut folder, JournalKey::control(tenant), client, first, 0).await
    {
        LoadOutcome::Loaded { .. } => Some(folder.state().clone()),
        _ => None,
    }
}

/// The tenant meta records as `name`, when it is `READY`.
#[tracing::instrument(level = "debug", skip_all)]
pub async fn resolve_tenant<P: Providers>(
    client: &Client<P>,
    name: &[u8],
    first: usize,
) -> Option<TenantId> {
    let meta = super::fleet::load_meta(client, first).await?;
    let tenant = meta.by_name(name)?;
    meta.tenant(tenant)
        .filter(|entry| entry.state == crate::system::TenantState::Ready)
        .map(|_| tenant)
}
