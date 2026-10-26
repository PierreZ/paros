//! The **cold-rot scenario**'s injector (#362 (background scrubbing)): on a
//! seed that draws it (`crate::shape::cold_rot`), a machine's disk loses
//! bits at rest. Inside the chaos window, now and then, a task beside the
//! machine's shipped `run_machine` rots the entry of one cold record of a
//! journal a tenant created, on the machine's own disk, while the machine
//! runs. The record's copy in the node's memory stays whole, so only a read
//! of the disk finds the damage: the tenant scrubber's read
//! (`paros::machine` scrub), or the boot scan after a reboot, which reports
//! the slot faulty to CTRL.
//!
//! The aim comes from the journal's own layout, never from a format the
//! harness knows: the task copies the store's files to a scratch directory
//! on the same disk, opens the copy with the shipped store's boot scan, and
//! reads `JournalStorage::layout` of a held slot there. A record's place in
//! a segment never moves, so the copy's layout is the original's. The task
//! flips bytes near the entry's start (the injector's own `flip`), so the
//! entry fails its checksum and its persist record stays whole: the slot
//! is CTRL's recoverable class, never a double fault.
//!
//! **Budget.** At most one machine rots each journal, at most
//! `ROTS_PER_JOURNAL` of its records, and
//! only a journal its tenant created on three members or more under a
//! majority (the audit's tenant board): every slot keeps a clean copy on a
//! quorum. The scenario's runs last to the top of the `run_end` knob, since
//! created journals come late in a run. The moonpool fault families that rot
//! a disk at random stay masked (#176): they would need a ledger of every
//! machine store.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    OpenOptions, SimStorageProvider, StateHandle, StorageFile, StorageProvider, TimeProvider,
    assert_reachable,
};
use paros::core::Config;
use paros::core::JournalId;
use paros::core::JournalIdentifier;
use paros::core::NodeId;
use paros::core::QuorumSystem;
use paros::core::Slot;
use paros::core::Storage;
use paros::core::TenantId;
use paros::storage::LogStorage;
use tokio_util::sync::CancellationToken;

/// The key of the run's rot budget.
const BUDGET_KEY: &str = "paros-cold-rot";

/// How long the task waits between two tries, in milliseconds.
const PAUSE_MS: std::ops::Range<u64> = 1_500..4_001;

/// How many records the one machine that rots a journal may rot in it.
const ROTS_PER_JOURNAL: u32 = 4;

/// The run's rot budget: for each journal, the machine that rots it and
/// how many of its records it rotted.
fn budget(state: &StateHandle) -> Arc<Mutex<BTreeMap<JournalIdentifier, (usize, u32)>>> {
    crate::state::published(state, BUDGET_KEY, BTreeMap::new)
}

/// The ids under `dir` that parse as a nonzero id.
async fn ids(provider: &SimStorageProvider, dir: &str) -> Vec<u64> {
    provider.list_dir(dir).await.map_or_else(
        |_| Vec::new(),
        |names| {
            names
                .iter()
                .filter_map(|name| name.parse().ok())
                .filter(|id| *id != 0)
                .collect()
        },
    )
}

/// Every byte of the file at `path`, or `None` on any error.
async fn read_file(provider: &SimStorageProvider, path: &str) -> Option<Vec<u8>> {
    let file = provider.open(path, OpenOptions::read_only()).await.ok()?;
    let size = usize::try_from(file.size().await.ok()?).ok()?;
    let mut bytes = vec![0_u8; size];
    let mut done = 0;
    while done < size {
        match file.read_at(done as u64, &mut bytes[done..]).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => done += read,
        }
    }
    Some(bytes)
}

/// Write `bytes` as the new file `path`, or `false` on any error.
async fn write_file(provider: &SimStorageProvider, path: &str, bytes: &[u8]) -> bool {
    let Ok(file) = provider.open(path, OpenOptions::create_write()).await else {
        return false;
    };
    let mut done = 0;
    while done < bytes.len() {
        match file.write_at(done as u64, &bytes[done..]).await {
            Ok(0) | Err(_) => return false,
            Ok(wrote) => done += wrote,
        }
    }
    true
}

/// Delete every file of `dir`, best effort.
async fn remove(provider: &SimStorageProvider, dir: &str) {
    let Ok(names) = provider.list_dir(dir).await else {
        return;
    };
    for name in names {
        let _ = provider.delete(&format!("{dir}/{name}")).await;
    }
}

/// The cold-rot task of the machine whose disk is `provider` under `root`,
/// its stores laid out as `layout` (see the module doc). It returns at the
/// end of the chaos window or at `shutdown`.
#[tracing::instrument(level = "debug", skip_all, fields(rank))]
pub(crate) async fn run<T: TimeProvider>(
    provider: SimStorageProvider,
    time: T,
    state: StateHandle,
    (root, layout, rank): (
        &'static str,
        paros::storage::journal::JournalStoreConfig,
        usize,
    ),
    shutdown: CancellationToken,
) {
    while !shutdown.is_cancelled() {
        let pause = Duration::from_millis(moonpool_sim::sim_random_range(PAUSE_MS));
        if time.sleep(pause).await.is_err() || shutdown.is_cancelled() {
            return;
        }
        if crate::past_cutoff(time.now(), Duration::ZERO) {
            return;
        }
        let journals = format!("{root}/journals");
        let mut candidates = Vec::new();
        for tenant in ids(&provider, &journals).await {
            for journal in ids(&provider, &format!("{journals}/{tenant}")).await {
                let id = JournalIdentifier::new(TenantId(tenant), JournalId(journal));
                // Only a journal placed on three members or more under a
                // majority: every slot keeps a clean copy on a quorum.
                let created =
                    crate::audit::tenants::lock(&crate::audit::tenants::tenant_board(&state))
                        .live_config(id)
                        .is_some_and(|config| {
                            config.members().len() >= 3
                                && config.quorum_system() == QuorumSystem::Majority
                        });
                let spent = budget(&state)
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&id)
                    .is_some_and(|(by, rots)| *by != rank || *rots >= ROTS_PER_JOURNAL);
                if created && !spent {
                    candidates.push(id);
                }
            }
        }
        if candidates.is_empty() {
            continue;
        }
        let at = moonpool_sim::sim_random_range(0..candidates.len());
        let journal = candidates[at];
        if rot(&provider, &state, (root, layout, rank), journal).await {
            assert_reachable!("scrub: a machine's disk rots a cold record");
        }
    }
}

/// Rot one cold record of `journal` on this disk, if its budget allows.
/// Whether the flip landed.
async fn rot(
    provider: &SimStorageProvider,
    state: &StateHandle,
    (root, layout, rank): (
        &'static str,
        paros::storage::journal::JournalStoreConfig,
        usize,
    ),
    journal: JournalIdentifier,
) -> bool {
    let dir = format!("{root}/journals/{}/{}", journal.tenant.0, journal.journal.0);
    let copy = format!("{root}/cold-rot-scratch");
    if provider.create_dir_all(&copy).await.is_err() {
        return false;
    }
    remove(provider, &copy).await;
    let Ok(names) = provider.list_dir(&dir).await else {
        return false;
    };
    for name in &names {
        let Some(bytes) = read_file(provider, &format!("{dir}/{name}")).await else {
            remove(provider, &copy).await;
            return false;
        };
        if !write_file(provider, &format!("{copy}/{name}"), &bytes).await {
            remove(provider, &copy).await;
            return false;
        }
    }
    // The copy, opened with the shipped store's boot scan: its layout is
    // the original's for every record whose place a later write never
    // moved.
    let mut store = paros::storage::journal::JournalStorage::new(
        provider.clone(),
        &copy,
        Config::new(NodeId(0), journal),
        layout,
    );
    let target = match store.boot_scan().await {
        Ok(()) => aim(&store),
        Err(_) => None,
    };
    drop(store);
    remove(provider, &copy).await;
    let Some((slot, region)) = target else {
        return false;
    };
    // The budget: one machine per journal, a few records each, decided
    // before a byte changes.
    {
        let budget = budget(state);
        let mut spent = budget.lock().unwrap_or_else(PoisonError::into_inner);
        let (by, rots) = spent.entry(journal).or_insert((rank, 0));
        if *by != rank || *rots >= ROTS_PER_JOURNAL {
            return false;
        }
        *rots += 1;
    }
    let original = paros::storage::journal::LayoutRegion {
        path: region.path.replacen(&copy, &dir, 1),
        ..region
    };
    tracing::info!(
        rank,
        journal = %journal,
        slot = slot.0,
        path = %original.path,
        "cold_rot_strikes"
    );
    crate::world::injector::flip(provider, &original).await
}

/// A cold slot of the scanned `store` and its entry's region: one of the
/// lower half of the held slots, so the next batches never write near it.
fn aim(
    store: &paros::storage::journal::JournalStorage<SimStorageProvider>,
) -> Option<(Slot, paros::storage::journal::LayoutRegion)> {
    let first = store.first_slot().0;
    let last = store.last_slot().0;
    let held: Vec<Slot> = (first..=last)
        .map(Slot)
        .filter(|slot| store.accepted(*slot).is_some())
        .collect();
    if held.len() < 2 {
        return None;
    }
    let slot = held[moonpool_sim::sim_random_range(0..held.len() / 2)];
    let layout = store.layout(slot)?;
    Some((slot, layout.entry))
}
