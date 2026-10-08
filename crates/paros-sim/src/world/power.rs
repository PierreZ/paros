//! A power loss **inside** a journal commit (#176), for any journal store a
//! process runs: a node's `JournalStorage` or a matchmaker's
//! `JournalMatchmakerStorage`.
//!
//! A crash from attrition lands at a random instant, and a commit's
//! unsynced window (entries written, records or the sync not yet done) is
//! microseconds wide, so the journal's crash recovery (torn records, records
//! rebuilt from their entries, an ambiguous last batch) would almost never
//! run. This BUGGIFY site makes it likely: a buggified commit races a random
//! timer spanning a commit's duration, and when the timer wins the process
//! cuts its own power through moonpool's [`SelfCrash`]: the kill lands
//! before the commit's next storage completion, every unsynced sector
//! resolves by the disk's crash physics, and the process restarts after a
//! delay. Only inside the chaos window, so the tail is a genuine recovery.

use std::future::Future;
use std::time::Duration;

use futures::future::{Either, select};
use moonpool_sim::{
    RebootKind, SelfCrash, SimTimeProvider, TimeProvider, assert_reachable, buggify_with_prob,
};

/// Whose store a [`PowerCut`] cuts: each owner's cut is its own reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    /// An acceptor's `JournalStorage`.
    Node,
    /// A matchmaker's `JournalMatchmakerStorage`.
    Matchmaker,
}

/// The power-cut site of one process's journal store (see the module doc).
pub(crate) struct PowerCut {
    crash: SelfCrash,
    time: SimTimeProvider,
    cutoff: Duration,
    owner: Owner,
    /// How long this store's last whole commit took: the span a cut is
    /// drawn in, so it lands between a write and the sync that would have
    /// covered it.
    last_commit: Duration,
}

impl PowerCut {
    pub(crate) fn new(
        crash: SelfCrash,
        time: SimTimeProvider,
        cutoff: Duration,
        owner: Owner,
    ) -> Self {
        Self {
            crash,
            time,
            cutoff,
            owner,
            last_commit: Duration::from_millis(1),
        }
    }

    /// The delay after which this commit loses power, if it is one that
    /// does: uniform over the store's last commit's duration. `writes` is
    /// whether the commit carries anything a crash can tear.
    fn draw(&self, writes: bool, permit: impl FnOnce() -> bool) -> Option<Duration> {
        // A commit without entries (a format, a promise) has no persist
        // record to tear: the metainfo's own copies cover it. The permit is
        // asked only once the site fired: it draws nothing, and a refusal
        // is the budget's, not the coin's.
        if !writes || self.time.now() >= self.cutoff {
            return None;
        }
        // One location per owner, so the sweep can select a node's cuts and
        // a matchmaker's apart.
        let fired = match self.owner {
            Owner::Node => buggify_with_prob!(0.25),
            Owner::Matchmaker => buggify_with_prob!(0.25),
        };
        if !fired || !permit() {
            return None;
        }
        let span = u64::try_from(self.last_commit.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        Some(Duration::from_micros(moonpool_sim::sim_random_range(
            0..span,
        )))
    }

    /// Run `commit`, cutting the process's power partway through it when
    /// the site fires and `permit` (the store's damage budget, see
    /// [`super::StorageWorld::permit_power_cut`]) allows (`on_cut` runs then,
    /// before the kill: nothing after the commit's await is sure to). The
    /// commit is awaited either way: the kill lands before its next storage
    /// completion, and an uncut commit times the next draw's span.
    #[tracing::instrument(level = "trace", skip_all, fields(owner = ?self.owner, writes))]
    pub(crate) async fn around<F: Future>(
        &mut self,
        writes: bool,
        permit: impl FnOnce() -> bool,
        on_cut: impl FnOnce(),
        commit: F,
    ) -> F::Output {
        let Some(after) = self.draw(writes, permit) else {
            let start = self.time.now();
            let done = commit.await;
            self.last_commit = self.time.now().saturating_sub(start);
            return done;
        };
        let commit = Box::pin(commit);
        let timer = Box::pin(self.time.sleep(after));
        match select(commit, timer).await {
            Either::Left((done, _)) => done,
            Either::Right((_, commit)) => {
                // Paired with the recovery gates the journal reports at
                // the next boot.
                match self.owner {
                    Owner::Node => {
                        assert_reachable!("journal store: a node loses power mid-commit");
                    }
                    Owner::Matchmaker => {
                        assert_reachable!("journal store: a matchmaker loses power mid-commit");
                    }
                }
                on_cut();
                let restart = Duration::from_millis(moonpool_sim::sim_random_range(500..2500));
                let _ = self.crash.crash(RebootKind::Crash, Some(restart));
                commit.await
            }
        }
    }
}
