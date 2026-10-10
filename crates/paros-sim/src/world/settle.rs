//! The operator's durable probe (#348): before it reads a store's format
//! marker to resolve an interrupted provisioning, it makes the store's
//! files durable as they stand ([`paros::journal::settle`]). A marker a
//! living process reads after a failed format sync is staged, not landed;
//! once settled, the marker the probe reads is the one a crash keeps.
//!
//! Witness of the bug (let it go): a genesis journal's format sync failed
//! twice, the quarantined journal re-opened in the same process, the probe
//! read the unsynced marker and confirmed the provisioning, and a kill then
//! dropped the marker: the restart was refused as `Amnesia` with no wipe
//! (`storage: an amnesia refusal names a wiped identity`, 1 seed in a
//! 3,000-seed hunt at c0a8e41).

use moonpool_sim::{SimStorageProvider, assert_always, assert_reachable};

use super::wipe::SYNC_ATTEMPTS;

/// Settle the store under `dir`, retrying a failed sync (moonpool's storage
/// chaos fails one now and then, never for long). `false` only past the
/// hang guard, which the assertion reports.
#[tracing::instrument(level = "debug", skip_all, fields(dir = %dir))]
pub(crate) async fn settle_store(provider: &SimStorageProvider, dir: &str) -> bool {
    for attempt in 0..SYNC_ATTEMPTS {
        if paros::journal::settle(provider, dir).await.is_ok() {
            if attempt > 0 {
                assert_reachable!("journal store: a provisioning probe retries a failed sync");
            }
            return true;
        }
    }
    assert_always!(
        false,
        "journal store: a provisioning probe makes the store durable",
        { "attempts" => SYNC_ATTEMPTS }
    );
    false
}
