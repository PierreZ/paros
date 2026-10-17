//! Spare segments (#600 (journal commit stalls), moonpool#338): a journal
//! store prepares its next segment in the background, so the commit that
//! fills a segment rolls onto the spare with a header write and a rename,
//! not a zero-fill of a whole segment on the commit path.
//!
//! [`Spares`] carries the task provider that prepares them, its type erased
//! so a store stays generic over its storage alone
//! ([`JournalStorage::with_spares`](super::JournalStorage::with_spares)).
//! `run_machine` hands its own task provider to every store it serves on, so
//! `parosd` and the simulation prepare spares the same way: in production on
//! Tokio, in the simulation as a task of the machine's process, which dies
//! with it. A spare is never evidence: the next open deletes any spare it
//! finds, whole or half-written, and loses nothing.
//!
//! A matchmaker store prepares none: its registrations fill a segment
//! rarely, and a spare would hold a whole segment of disk per set.

use std::sync::Arc;

use moonpool_core::{StorageProvider, TaskProvider};
use moonpool_journal::Journal;

/// What [`Spares`] does to a journal: hand it the task provider.
type Start<P> = dyn Fn(&mut Journal<P>) + Send + Sync;

/// Starts a journal's spare-segment preparation on a task provider
/// (`Journal::prepare_spares_with`). See the [module docs](self).
pub struct Spares<P: StorageProvider> {
    start: Arc<Start<P>>,
}

impl<P: StorageProvider> Spares<P> {
    /// Spares prepared on `tasks`.
    #[must_use]
    pub fn on<T: TaskProvider>(tasks: T) -> Self {
        Self {
            start: Arc::new(move |journal: &mut Journal<P>| {
                journal.prepare_spares_with(tasks.clone());
            }),
        }
    }

    /// Prepare `journal`'s spares from now on: the journal starts one in
    /// the background whenever its active segment is half full.
    pub(crate) fn start(&self, journal: &mut Journal<P>) {
        (self.start)(journal);
    }
}

impl<P: StorageProvider> Clone for Spares<P> {
    fn clone(&self) -> Self {
        Self {
            start: Arc::clone(&self.start),
        }
    }
}

impl<P: StorageProvider> std::fmt::Debug for Spares<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spares").finish_non_exhaustive()
    }
}
