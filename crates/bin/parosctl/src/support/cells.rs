//! Commands that cross cells (#232 (multiple cells)): `--servers` names
//! one cell, and a tenant may live in another.
//!
//! The universe directory lives in one cell. A cell that does not host it
//! records a **universe pointer** in its registry (§3.7): the directory's
//! journal and the machines of the cell that hosts it. [`universe`] reads
//! the directory from either cell. [`tenant_cell`] then gives a client over
//! the tenant's own cell, through the entry endpoint the directory records
//! for it. So a command given the machines of any cell of the universe
//! follows the tenant to its cell, as a resolver does.

use moonpool_core::CompioProviders;
use paros::client::Client;
use paros::client::bootstrap::{cell_members, control_journals};
use paros::client::checkpoint::{LoadOutcome, load};
use paros::client::fleet::read_directory;
use paros::core::NodeId;
use paros::core::journal::JournalIdentifier;
use paros::model::checkpoint::Folder;
use paros::model::fleet::FleetDirectory;
use paros::model::machine::ControlJournals;
use paros::model::registry::{Registry, UniversePointer};

use crate::support::connect::Ending;
use crate::support::output::{note, record_text};

type ParosClient = Client<CompioProviders>;

/// The universe directory, as read from the machines `--servers` names.
pub struct Universe {
    /// The servers' own cell.
    pub own: ControlJournals,
    /// The directory's journal.
    pub directory: JournalIdentifier,
    /// A client over the machines of the cell that hosts the directory: the
    /// servers themselves when their cell hosts it.
    pub client: ParosClient,
    /// The directory, folded to its tail.
    pub state: FleetDirectory,
}

/// The universe pointer cell `own`'s registry records, folded from its
/// control journal through `client`: `None` for a cell that hosts the
/// directory, never joined a universe, or could not be read.
pub async fn pointer(client: &ParosClient, own: &ControlJournals) -> Option<UniversePointer> {
    let members = cell_members(client, own.cell).await?;
    let mut fold = Folder::new(Registry::new(members.into_iter().map(NodeId)));
    match load(&mut fold, own.cell, client, 0, 0).await {
        LoadOutcome::Loaded { .. } => fold.state().universe().cloned(),
        _ => None,
    }
}

/// How a cell that does not host the universe directory names it, for
/// people: `demo (directory in cell cell-1)`, read through the universe
/// pointer `own`'s registry records. `None` for a cell in no universe, or
/// when the directory could not be read.
pub async fn universe_label(client: &ParosClient, own: &ControlJournals) -> Option<String> {
    let pointer = pointer(client, own).await.filter(|p| !p.hosts.is_empty())?;
    let state = read_directory(&client.elsewhere(&pointer.hosts), 0, pointer.directory)
        .await
        .ok()?;
    let host = state
        .universe_cell()
        .map(|cell| record_text(state.cell_name(cell)))
        .unwrap_or_default();
    Some(format!(
        "{} (directory in cell {host})",
        record_text(state.universe_name())
    ))
}

/// The universe directory, from the servers' cell or through its universe
/// pointer; or how the command ends, with a note.
pub async fn universe(client: &ParosClient) -> Result<Universe, Ending> {
    let Some(own) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    let (directory, host) = match own.fleet {
        Some(directory) => (directory, client.clone()),
        None => match pointer(client, &own).await {
            Some(pointer) if !pointer.hosts.is_empty() => {
                (pointer.directory, client.elsewhere(&pointer.hosts))
            }
            _ => {
                note("the cell is in no universe: join it with `parosctl universe add-cell`");
                return Err(Ending::Refused);
            }
        },
    };
    match read_directory(&host, 0, directory).await {
        Ok(state) => Ok(Universe {
            own,
            directory,
            client: host,
            state,
        }),
        Err(outcome) => {
            note(&format!(
                "the universe directory could not be read to its tail: {outcome:?}"
            ));
            Err(Ending::Unreachable)
        }
    }
}

/// A client over cell `cell_id`: `client` itself for the servers' own
/// cell, else the entry endpoint the universe directory records for it.
pub fn tenant_cell(
    client: &ParosClient,
    universe: &Universe,
    cell_id: u64,
) -> Result<ParosClient, Ending> {
    if cell_id == universe.own.cell_id {
        return Ok(client.clone());
    }
    let endpoint = universe.state.endpoint(cell_id);
    if endpoint.is_empty() {
        note("the universe directory records no entry endpoint for the tenant's cell");
        return Err(Ending::Refused);
    }
    Ok(client.elsewhere(endpoint))
}
