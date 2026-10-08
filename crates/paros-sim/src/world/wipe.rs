//! The wipe coin's physical half on a journal-store seed (#176): the disk a
//! restarted identity comes back on is empty. The world's half (parking the
//! identity, the dead-node budget) is [`super::StorageWorld::wipe`] and
//! [`super::StorageWorld::wipe_matchmaker`]; this one deletes the journal's
//! files and makes the deletion durable, so no crash brings them back.

use moonpool_sim::{SimStorageProvider, StorageProvider, assert_always};

/// How many times a directory sync is retried: moonpool's storage chaos fails
/// a sync now and then, never for long (a failed disk is masked, #176), so
/// the bound is a hang guard far above any streak, not a fault budget.
const SYNC_ATTEMPTS: usize = 1024;

/// Delete every file in `dir` and sync `dir` and its parent, so the
/// deletions are durable. A directory that does not exist is already empty.
#[tracing::instrument(level = "debug", skip_all, fields(dir = %dir))]
pub(crate) async fn wipe_dir(provider: &SimStorageProvider, dir: &str) {
    let Ok(names) = provider.list_dir(dir).await else {
        return;
    };
    for name in &names {
        let _ = provider.delete(&format!("{dir}/{name}")).await;
    }
    let parent = dir.rsplit_once('/').map_or(".", |(parent, _)| parent);
    let mut durable = false;
    for _ in 0..SYNC_ATTEMPTS {
        if provider.sync_dir(dir).await.is_ok() && provider.sync_dir(parent).await.is_ok() {
            durable = true;
            break;
        }
    }
    // A wipe that could not make itself durable would let a crash bring the
    // old journal back under a parked identity.
    assert_always!(
        durable,
        "journal store: a wipe's deletions are made durable",
        { "files" => names.len() }
    );
    let left = provider.list_dir(dir).await.map_or(0, |left| left.len());
    assert_always!(
        left == 0,
        "journal store: a wiped journal directory is empty",
        { "files" => left }
    );
}
