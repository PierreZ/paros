//! A power loss **inside** a journal commit (#176), for any journal store a
//! process runs: a node's `JournalStorage` or a matchmaker's
//! `JournalMatchmakerStorage`.
//!
//! A crash from attrition lands at a random instant, and a commit's
//! unsynced window (entries written, records or the sync not yet done) is
//! microseconds wide, so the journal's crash recovery (torn records, records
//! rebuilt from their entries, an ambiguous last batch) would almost never
//! run. The commit itself names the moments: `moonpool_journal` asks its
//! `CommitHooks` at each `CommitPoint`, with writes issued and not yet
//! synced (fault injection lives in the shipped code, decided on
//! 2026-10-09). [`PowerCut`] is those hooks under simulation: one BUGGIFY
//! location per owner and point, and when one fires the process cuts its own
//! power through moonpool's [`SelfCrash`]. The kill lands before the
//! commit's next storage completion, every unsynced sector resolves by the
//! disk's crash physics, and the process restarts after a delay. Only inside
//! the chaos window, so the tail is a genuine recovery.
//!
//! The store arms the cut around each sync ([`PowerCut::arm`]) with the
//! world's damage budget and what to note when the power goes; a point
//! reached with nothing armed (a boot's own repairs) never cuts.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{
    RebootKind, SelfCrash, SimTimeProvider, TimeProvider, assert_reachable, buggify_with_prob,
};
use paros::journal::{CommitHooks, CommitPoint};

/// Whose store a [`PowerCut`] cuts: each owner's cut is its own reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    /// An acceptor's `JournalStorage`.
    Node,
    /// A matchmaker's `JournalMatchmakerStorage`.
    Matchmaker,
}

/// What one sync's cut asks of its store: the budget, asked only once a
/// location fired (a refusal is the budget's, not the coin's), and what to
/// note before the kill.
struct Armed {
    permit: Box<dyn FnOnce() -> bool + Send>,
    on_cut: Box<dyn FnOnce() + Send>,
}

/// The power-cut hooks of one process's journal store (see the module doc).
pub(crate) struct PowerCut {
    crash: SelfCrash,
    time: SimTimeProvider,
    cutoff: Duration,
    owner: Owner,
    armed: Mutex<Option<Armed>>,
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
            armed: Mutex::new(None),
        }
    }

    /// Whether the chaos window is still open: the injector's damage lands
    /// only inside it, like the cut itself.
    pub(crate) fn in_chaos(&self) -> bool {
        self.time.now() < self.cutoff
    }

    /// Arm the next commit points, up to [`Self::disarm`]: a cut there asks
    /// `permit` (the store's damage budget, see
    /// [`super::StorageWorld::permit_power_cut`]) and runs `on_cut` before
    /// the kill (nothing after the commit's await is sure to run).
    pub(crate) fn arm(
        &self,
        permit: impl FnOnce() -> bool + Send + 'static,
        on_cut: impl FnOnce() + Send + 'static,
    ) {
        *self.armed.lock().unwrap_or_else(PoisonError::into_inner) = Some(Armed {
            permit: Box::new(permit),
            on_cut: Box::new(on_cut),
        });
    }

    /// The sync returned: no later point is this sync's.
    pub(crate) fn disarm(&self) {
        self.armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    fn armed(&self) -> bool {
        self.armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Whether this point's location fires. One location per owner and
    /// point, so the sweep can select each apart; three points per commit at
    /// most, so each fires about a third as often as the one per-commit site
    /// it replaced.
    fn fires(&self, point: CommitPoint) -> bool {
        match (self.owner, point) {
            (Owner::Node, CommitPoint::EntriesWritten) => buggify_with_prob!(0.1),
            (Owner::Node, CommitPoint::RecordsWritten) => buggify_with_prob!(0.1),
            (Owner::Node, CommitPoint::BeforeMeta) => buggify_with_prob!(0.1),
            (Owner::Matchmaker, CommitPoint::EntriesWritten) => buggify_with_prob!(0.1),
            (Owner::Matchmaker, CommitPoint::RecordsWritten) => buggify_with_prob!(0.1),
            (Owner::Matchmaker, CommitPoint::BeforeMeta) => buggify_with_prob!(0.1),
        }
    }
}

impl CommitHooks for PowerCut {
    #[tracing::instrument(level = "trace", skip_all, fields(owner = ?self.owner, point = ?point))]
    fn at(&self, point: CommitPoint) {
        if !self.in_chaos() || !self.armed() || !self.fires(point) {
            return;
        }
        let Some(armed) = self
            .armed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };
        if !(armed.permit)() {
            return;
        }
        // Paired with the recovery gates the journal reports at the next
        // boot.
        match self.owner {
            Owner::Node => assert_reachable!("journal store: a node loses power mid-commit"),
            Owner::Matchmaker => {
                assert_reachable!("journal store: a matchmaker loses power mid-commit");
            }
        }
        match point {
            CommitPoint::EntriesWritten => {
                assert_reachable!("journal store: the power goes with a commit's entries written");
            }
            CommitPoint::RecordsWritten => {
                assert_reachable!("journal store: the power goes with a commit's records written");
            }
            CommitPoint::BeforeMeta => {
                assert_reachable!("journal store: the power goes before a commit's metainfo");
            }
        }
        (armed.on_cut)();
        let restart = Duration::from_millis(moonpool_sim::sim_random_range(500..2500));
        let _ = self.crash.crash(RebootKind::Crash, Some(restart));
    }
}
