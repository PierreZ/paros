//! Simulation answers to the driver's per-seed latches (`paros::DriverHooks`):
//! withholding GC requests, holding a journal, and the lost-verdict reply
//! drop. Each is drawn once per seed in `crate::shape` and coupled to a
//! scenario there, and each turns off with the chaos window, leaving the
//! recovery tail quiet for convergence. Every other driver choice is an
//! inline BUGGIFY site in `paros` (#294, #318); these move there too once
//! moonpool can force a location's activation per seed (#318 E).
//!
//! Every method is consulted from the driver's node loop and nowhere else.
//! That is load-bearing for replay, not incidental: a draw taken inside a
//! detached task can outlive its simulation and shift the *next* run's
//! stream (see `PeerMailbox` in `paros::driver`).
//!
//! | latch | fired gate | recovery gate |
//! |---|---|---|
//! | `withhold_gc_requests` | inline ("gc: a seed withholds its GC requests for the chaos window"), drawn per seed in `crate::shape::withhold_gc` | "storage: a departed straggler's slot is recovered through the prior configuration" (#263) |
//! | `hold_journal` | the journal board (`crate::audit::journals`), drawn per seed in `crate::shape::journals` | "a sibling keeps committing while one is held" |
//! | `drop_client_reply` (the lost-verdict latch) | inline ("client: a write's verdict is lost on a lost-verdict seed"), drawn per seed in `crate::shape::lost_verdict` | "…retry takes the dedup path" |

use std::time::Duration;

use moonpool_sim::{TimeProvider, assert_reachable};

use paros::{DriverHooks, JournalIdentifier};

/// The driver's `DriverHooks` under simulation (see the module doc).
pub(crate) struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
    /// Whether this run's nodes withhold their GC requests for the chaos
    /// window (`crate::shape::withhold_gc`, drawn once per seed).
    withhold_gc: bool,
    /// The journal held on every node for the chaos window (#188), drawn
    /// once per seed (`crate::shape::journals`); `None` on most seeds.
    held_journal: Option<JournalIdentifier>,
    /// Whether this run draws the lost-verdict scenario
    /// (`crate::shape::lost_verdict`, drawn once per seed): a write's reply
    /// is dropped at its own rate on every node, not only where the
    /// location fires.
    lose_verdicts: bool,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    pub(crate) fn new(time: T, cutoff: Duration) -> Self {
        Self {
            time,
            cutoff,
            withhold_gc: false,
            held_journal: None,
            lose_verdicts: false,
        }
    }

    /// Hold `held` on this node for the chaos window
    /// (`DriverHooks::hold_journal`, #188).
    pub(crate) fn holding_journal(mut self, held: Option<JournalIdentifier>) -> Self {
        self.held_journal = held;
        self
    }

    /// Withhold every GC request these hooks' node would send for the chaos
    /// window when `withhold` (see `DriverHooks::withhold_gc_requests`).
    pub(crate) fn withholding_gc(mut self, withhold: bool) -> Self {
        self.withhold_gc = withhold;
        self
    }

    /// Drop write replies at the lost-verdict scenario's rate
    /// (`crate::shape::lost_verdict`).
    pub(crate) fn losing_verdicts(mut self, lose: bool) -> Self {
        self.lose_verdicts = lose;
        self
    }

    fn active(&self) -> bool {
        self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn withhold_gc_requests(&self) -> bool {
        // Drawn once per seed (`crate::shape::withhold_gc`, its own
        // location), never per call; only inside the chaos window, so GC
        // resumes in the recovery tail. The driver asks only when a request
        // is due, so a withheld answer here is a request withheld.
        let withheld = self.active() && self.withhold_gc;
        if withheld {
            assert_reachable!("gc: a seed withholds its GC requests for the chaos window");
        }
        withheld
    }

    fn hold_journal(&self, journal: JournalIdentifier) -> bool {
        // Drawn once per seed (the plan's own BUGGIFY location and its
        // reachable), never per call: a deterministic answer is safe to ask
        // per inbound message. Only inside the chaos window, so the held
        // journal recovers in the tail like any partition.
        self.active() && self.held_journal == Some(journal)
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        // The reply seam's per-kind drops are inline in `paros` (#318). What
        // is left is the lost-verdict scenario's latch: on such a seed every
        // node drops a write's verdict at the same rate, so a retry meets a
        // committed write wherever it lands (#318 E moves it).
        if !self.active() || !self.lose_verdicts || reply != paros::Reply::Write {
            return false;
        }
        let lost = moonpool_sim::sim_random_bool(0.10);
        if lost {
            assert_reachable!("client: a write's verdict is lost on a lost-verdict seed");
        }
        lost
    }
}
