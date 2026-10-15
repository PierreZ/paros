//! **The cell's side of the universe** (#513 (fleet Init duel),
//! `docs/architecture.md` §3.1, §3.7): the cell coordinator writes the
//! cell's `JoinFleet` into the cell control journal, its one writer.
//!
//! `init`'s fleet half adds the cell to the universe directory (`AddCell`).
//! The cell coordinator reads that directory, which the cell hosts, and
//! writes `JoinFleet` with the directory's fleet id once the directory names
//! this cell and its control journal. The operator then waits for the join
//! and marks the cell `READY` ([`crate::client::fleet`]). So `init` never
//! claims the cell control journal and never fences the coordinator.
//!
//! Before #513 the operator claimed the cell control journal for this one
//! write. The coordinator it fenced resigned, the next term installed a
//! fresh uuid, and that uuid fenced the operator in turn. With slow links in
//! the recovery tail, each claim lost to the next install, and `init` did not
//! end (witness 1522711066683701314 on a0c4aa6: the cell write came back
//! `Superseded`, red on "fleet: an operation an operator stopped in ends in
//! the recovery tail").
//!
//! The coordinator joins the cell at its install (an interrupted `init`) and
//! at every watch round until the cell joined. A read of the directory that
//! does not reach its tail writes nothing: the next round reads again.

use moonpool_core::Providers;

use crate::client::Client;
use crate::client::cell::CellSession;
use crate::client::checkpoint::{Folder, LoadOutcome, load};
use crate::client::fleet::Interrupted;
use crate::fleet::{CellState, FleetDirectory, METADATA_VERSION};
use crate::machine::ControlJournals;
use crate::system::SystemCommand;

/// The `JoinFleet` the universe directory `directory` asks of the cell
/// `journals` names: the directory holds a fleet and an entry for this
/// cell, not being removed, whose cell tenant has this cell's control
/// journal. `None` while the directory asks nothing of this cell.
#[must_use]
pub(crate) fn joining(
    directory: &FleetDirectory,
    journals: ControlJournals,
) -> Option<SystemCommand> {
    let fleet_id = directory.fleet()?;
    assert_ne!(fleet_id, 0, "a formed fleet has an id");
    let entry = directory.cell(journals.cell_id)?;
    if entry.state == CellState::Removing {
        return None;
    }
    if entry.control_tenant != journals.cell.tenant {
        return None;
    }
    let tenant = directory.tenant(entry.control_tenant)?;
    if tenant.control != journals.cell.journal {
        return None;
    }
    assert_ne!(journals.cell_id, 0, "a cell entry names a set cell id");
    Some(SystemCommand::JoinFleet {
        fleet_id,
        cell_id: journals.cell_id,
        version: METADATA_VERSION,
    })
}

/// Join the cell to the universe through the term's `session`, when the
/// cell has not joined and the universe directory (folded into `directory`,
/// from where it stands) holds this cell. Whether it wrote `JoinFleet`.
///
/// # Errors
///
/// The write did not land: the term writes nothing more.
///
/// # Panics
///
/// If the session is not open (a programmer error), or a written join is
/// not in the session's fold.
pub(crate) async fn join<P: Providers>(
    client: &Client<P>,
    session: &mut CellSession,
    directory: &mut Folder<FleetDirectory>,
) -> Result<bool, Interrupted> {
    assert!(session.is_open(), "only an open session joins the cell");
    let journals = session.journals();
    let Some(universe) = journals.fleet else {
        return Ok(false);
    };
    if session.registry().fleet().is_some() {
        return Ok(false);
    }
    if !matches!(
        load(directory, universe, client, 0, 0).await,
        LoadOutcome::Loaded { .. }
    ) {
        return Ok(false);
    }
    let Some(join) = joining(directory.state(), journals) else {
        return Ok(false);
    };
    session.record(client, 0, &join).await?;
    // The fold judges the join at apply: another fleet's id is refused, so
    // the cell names this fleet or none.
    let joined = session.registry().fleet();
    assert!(
        joined.is_none_or(|f| f.cell_id == journals.cell_id),
        "a cell joins under its own id"
    );
    moonpool_assertions::reachable!("fleet: a step joins the cell to the fleet");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use paros_core::{JournalId, JournalIdentifier, TenantId};

    use super::*;
    use crate::client::checkpoint::Checkpointable;
    use crate::fleet::{FleetCommand, FleetEntry};

    fn journals() -> ControlJournals {
        ControlJournals {
            cell_id: 9,
            cell: JournalIdentifier::new(TenantId(5), JournalId(6)),
            election: None,
            fleet: Some(JournalIdentifier::new(TenantId(7), JournalId(8))),
        }
    }

    fn apply(directory: &mut FleetDirectory, seq: u64, command: FleetCommand) {
        directory.apply(seq, &FleetEntry::new(42, command).encode());
    }

    #[test]
    fn the_directory_asks_a_join_only_once_it_holds_this_cell() {
        let journals = journals();
        let mut directory = FleetDirectory::default();
        assert_eq!(joining(&directory, journals), None, "no fleet yet");
        apply(
            &mut directory,
            0,
            FleetCommand::FormFleet {
                control: journals.fleet.expect("a fleet journal"),
                name: Vec::new(),
            },
        );
        assert_eq!(joining(&directory, journals), None, "no cell entry yet");
        // Another cell's entry asks nothing of this one.
        let mut other = directory.clone();
        apply(
            &mut other,
            1,
            FleetCommand::AddCell {
                cell_id: 10,
                control: JournalIdentifier::new(TenantId(11), JournalId(12)),
                name: Vec::new(),
            },
        );
        assert_eq!(joining(&other, journals), None);
        // An entry for this cell id with another control journal neither.
        let mut moved = directory.clone();
        apply(
            &mut moved,
            1,
            FleetCommand::AddCell {
                cell_id: journals.cell_id,
                control: JournalIdentifier::new(TenantId(5), JournalId(13)),
                name: Vec::new(),
            },
        );
        assert_eq!(joining(&moved, journals), None);
        apply(
            &mut directory,
            1,
            FleetCommand::AddCell {
                cell_id: journals.cell_id,
                control: journals.cell,
                name: Vec::new(),
            },
        );
        assert_eq!(
            joining(&directory, journals),
            Some(SystemCommand::JoinFleet {
                fleet_id: 42,
                cell_id: journals.cell_id,
                version: METADATA_VERSION,
            })
        );
        // A cell being removed is not joined again.
        apply(
            &mut directory,
            2,
            FleetCommand::MarkCell {
                cell_id: journals.cell_id,
                state: CellState::Removing,
            },
        );
        assert_eq!(joining(&directory, journals), None);
    }
}
