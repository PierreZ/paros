//! **Offline store inspection** (#365): what a machine's disk holds, read
//! without a running `parosd` and without changing a byte of it.
//!
//! Exit code 78 (amnesia, a changed class, a lost identity, an edited
//! `Config`) tells the operator not to restart blindly. This module is the
//! tool that looks at the disk then, like Kafka's `kafka-dump-log`:
//! `parosctl store inspect <data-dir>` prints it.
//!
//! [`inspect`] reads the machine record and every store under the data
//! directory's layout ([`ProviderDisk`]): each journal store under
//! `journals/<tenant>/<journal>/` (its format marker, promise, chosen index,
//! floor, sealed journal state, faulty slots and accepted slots) and each
//! matchmaker set's store under `matchmakers/<tenant>/` (its format marker,
//! scalars and registrations).
//!
//! # Read-only by construction
//!
//! Opening a `moonpool-journal` journal recovers it, and recovery writes: it
//! syncs, rebuilds a torn record, starts a new generation and removes dead
//! segments. So the inspection never opens a store where it lies. It copies
//! the store's files to a **scratch** directory, on a provider of the
//! caller's choice, opens the copy with the shipped stores'
//! own boot scan ([`JournalStorage`], [`JournalMatchmakerStorage`]), and
//! deletes the copy. The source is only listed and read
//! (`OpenOptions::read_only`). What the copy reports is what the next
//! boot of `parosd` would find.
//!
//! # Names
//!
//! Hex ids are for local debugging only. [`names`] folds the cell's journals
//! this disk holds (the registry, the universe directory, the election
//! journal, the hosted tenants' control journals) over each store's
//! accepted prefix, and answers the administrative cell view ([`crate::model::view`]) from
//! them, so the inspection names machines, tenants and journals as
//! `parosctl` does online. A disk that holds none of them (an admitted
//! machine that serves only tenant journals) names nothing, and its ids
//! print as short hex.

mod names;
mod scratch;

#[cfg(test)]
mod tests;

use moonpool_core::LocalStorageProvider;
use paros_core::{
    Ballot, Command, Config, JournalId, JournalIdentifier, JournalState, MatchmakerConfig,
    MatchmakerHardState, NodeId, RegistryStorage, Slot, Storage, TenantId,
};

use crate::machine::{MachineRecord, ProviderDisk};
use crate::storage::LogStorage;
use crate::storage::journal::JournalMatchmakerStorage;
use crate::storage::journal::JournalStorage;
use crate::storage::journal::JournalStoreConfig;
use crate::storage::matchmaker::MatchmakerStorage;

pub use names::names;
use scratch::Scratch;

/// What a machine's disk holds.
#[derive(Clone, Debug)]
pub struct DiskImage {
    /// The data directory read.
    pub root: String,
    /// The machine record: `None` where there is none (an idle machine
    /// never started, or a disk that lost its identity), an error where it
    /// does not read or parse.
    pub record: Option<Result<MachineRecord, String>>,
    /// Every journal store, by identifier.
    pub journals: Vec<JournalImage>,
    /// Every matchmaker set's store, by tenant.
    pub matchmakers: Vec<SetImage>,
}

/// One journal store.
#[derive(Clone, Debug)]
pub struct JournalImage {
    /// The journal the store's directory names.
    pub journal: JournalIdentifier,
    /// What its boot scan found, or why it failed.
    pub store: Result<StoreImage, String>,
}

/// What a journal store's boot scan found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreImage {
    /// The format marker and the `Config` it records (#147, #207); `None`
    /// on a store that carries no marker.
    pub formatted: Option<Config>,
    /// The promised ballot.
    pub promise: Ballot,
    /// The chosen index as of the last metainfo write (relaxed: the real
    /// one may be higher).
    pub chosen: Option<Slot>,
    /// The first slot the store holds: every slot below it was truncated
    /// or jumped (#186).
    pub floor: Slot,
    /// What the slots below the floor folded to (#204).
    pub sealed: JournalState,
    /// The slots whose entry is damaged and whose ballot survived: CTRL's
    /// recoverable class, repaired from peers.
    pub faulty: Vec<(Slot, Ballot)>,
    /// Every accepted slot, in slot order.
    pub accepted: Vec<(Slot, Ballot, Command)>,
}

/// One matchmaker set's store.
#[derive(Clone, Debug)]
pub struct SetImage {
    /// The tenant the set serves.
    pub tenant: TenantId,
    /// What its boot scan found, or why it failed.
    pub store: Result<SetStoreImage, String>,
}

/// What a matchmaker store's boot scan found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetStoreImage {
    /// The format marker and the configuration it records (#183); `None`
    /// on a store that carries no marker.
    pub formatted: Option<MatchmakerConfig>,
    /// The set's scalars: generation, members, phase, each journal's GC
    /// watermark and effective configuration.
    pub scalars: MatchmakerHardState,
    /// Every live registration, by journal and ballot.
    pub registered: Vec<(JournalId, Ballot)>,
}

/// Read the disk under `root` on `source`, a machine's data directory, with
/// its stores laid out by `layout` (the default geometry in `parosd`). Every store is
/// copied under `scratch_root` on `scratch` before it is opened, and the
/// copy is deleted after (see the [module docs](self)): nothing under
/// `root` is written.
///
/// # Errors
///
/// The data directory cannot be listed, or the scratch directory cannot
/// be made. A store that fails to read is reported in its own image.
pub async fn inspect<P, S>(
    source: &P,
    root: &str,
    layout: JournalStoreConfig,
    scratch: &S,
    scratch_root: &str,
) -> Result<DiskImage, String>
where
    P: LocalStorageProvider + Clone,
    S: LocalStorageProvider + Clone,
{
    let disk = ProviderDisk::new(source.clone(), root, layout);
    if !source
        .exists(root)
        .await
        .map_err(|e| format!("{root}: {e}"))?
    {
        return Err(format!("{root}: no such directory"));
    }
    let record = match disk.read_record().await {
        Ok(None) => None,
        Ok(Some(text)) => Some(MachineRecord::parse(&text)),
        Err(error) => Some(Err(error)),
    };
    let scratch = Scratch::new(scratch.clone(), scratch_root).await?;
    let mut journals = Vec::new();
    for journal in journal_dirs(source, root).await? {
        let dir = disk.journal_dir(journal);
        let name = format!("journal-{}-{}", journal.tenant.0, journal.journal.0);
        let store = match scratch.copy(source, &dir, &name).await {
            Ok(copy) => {
                let read = read_journal(scratch.provider(), &copy, journal, layout).await;
                scratch.remove(&copy).await;
                read
            }
            Err(error) => Err(error),
        };
        journals.push(JournalImage { journal, store });
    }
    let mut matchmakers = Vec::new();
    for tenant in set_dirs(source, root).await? {
        let dir = disk.matchmaker_dir(tenant);
        let store = match scratch
            .copy(source, &dir, &format!("set-{}", tenant.0))
            .await
        {
            Ok(copy) => {
                let read = read_set(scratch.provider(), &copy, tenant, layout).await;
                scratch.remove(&copy).await;
                read
            }
            Err(error) => Err(error),
        };
        matchmakers.push(SetImage { tenant, store });
    }
    Ok(DiskImage {
        root: root.to_string(),
        record,
        journals,
        matchmakers,
    })
}

/// The names under `dir` that parse as a nonzero id, sorted; none where
/// `dir` does not exist. A name that is not an id is not a store paros
/// wrote, and is left out.
async fn ids<P: LocalStorageProvider>(source: &P, dir: &str) -> Result<Vec<u64>, String> {
    let names = match source.list_dir(dir).await {
        Ok(names) => names,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("{dir}: {error}")),
    };
    let mut ids: Vec<u64> = names
        .iter()
        .filter_map(|name| name.parse().ok())
        .filter(|id| *id != 0)
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Every journal store's identifier under `root/journals/`.
async fn journal_dirs<P: LocalStorageProvider>(
    source: &P,
    root: &str,
) -> Result<Vec<JournalIdentifier>, String> {
    let mut journals = Vec::new();
    for tenant in ids(source, &format!("{root}/journals")).await? {
        for journal in ids(source, &format!("{root}/journals/{tenant}")).await? {
            journals.push(JournalIdentifier::new(TenantId(tenant), JournalId(journal)));
        }
    }
    assert!(
        journals.iter().all(|j| j.is_set()),
        "a store directory names a set identifier"
    );
    Ok(journals)
}

/// Every matchmaker set's tenant under `root/matchmakers/`.
async fn set_dirs<P: LocalStorageProvider>(
    source: &P,
    root: &str,
) -> Result<Vec<TenantId>, String> {
    Ok(ids(source, &format!("{root}/matchmakers"))
        .await?
        .into_iter()
        .map(TenantId)
        .collect())
}

/// Boot-scan the journal store in `dir` (a scratch copy) and read it back.
async fn read_journal<S: LocalStorageProvider + Clone>(
    scratch: &S,
    dir: &str,
    journal: JournalIdentifier,
    layout: JournalStoreConfig,
) -> Result<StoreImage, String> {
    // The node id only names the store in its own logs; the boot scan reads
    // the journal's identity from the directory's identifier.
    let config = Config::new(NodeId(0), journal);
    let mut store = JournalStorage::new(scratch.clone(), dir, config, layout);
    store.boot_scan().await.map_err(|e| e.to_string())?;
    let (hard, _) = store.initial_state();
    let floor = store.first_slot();
    // `last_slot` is the highest slot held (inclusive), 0 when none is.
    let accepted: Vec<(Slot, Ballot, Command)> = (floor.0..=store.last_slot().0)
        .filter_map(|s| {
            store
                .accepted(Slot(s))
                .map(|(ballot, command)| (Slot(s), ballot, command))
        })
        .collect();
    let image = StoreImage {
        formatted: store.formatted_config(),
        promise: hard.max_promised_ballot,
        chosen: hard.chosen_index,
        floor,
        sealed: store.sealed_state(),
        faulty: store.faulty_entries(),
        accepted,
    };
    // Pair of the boot scan: every slot it holds is at or above its floor.
    assert!(
        image.accepted.iter().all(|(s, _, _)| *s >= image.floor),
        "an accepted slot lies at or above the store's floor"
    );
    assert!(
        image.faulty.iter().all(|(s, _)| *s >= image.floor),
        "a faulty slot lies at or above the store's floor"
    );
    Ok(image)
}

/// Boot-scan the matchmaker store in `dir` (a scratch copy) and read it
/// back.
async fn read_set<S: LocalStorageProvider + Clone>(
    scratch: &S,
    dir: &str,
    tenant: TenantId,
    layout: JournalStoreConfig,
) -> Result<SetStoreImage, String> {
    let id = JournalIdentifier::new(tenant, JournalId::UNSET);
    let mut store = JournalMatchmakerStorage::new(scratch.clone(), dir, id, layout);
    store.boot_scan().await.map_err(|e| e.to_string())?;
    Ok(SetStoreImage {
        formatted: store.formatted_config(),
        scalars: store.initial_state(),
        registered: store.registered(),
    })
}
