//! Driver fault-injection hooks.
//!
//! `drain_ready` runs synchronously (no `.await`), so process-granularity chaos
//! (moonpool's attrition) can only crash a node *between* batches — never at the
//! persist/send seam within one. [`DriverHooks`] also exposes the driver's
//! optional policy decisions: delaying an `Accept` re-send, resigning
//! leadership, and choosing the shortest valid election timeout. Production
//! passes [`NoHooks`], whose defaults never perturb the driver.

/// A durability seam within one `Ready` batch where a crash can be injected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seam {
    /// After the batch's durable writes are staged but **before** the fsync. A
    /// crash here loses the whole un-synced batch — and no message was sent yet
    /// (sends come after the fsync), so it is a clean "the step never happened".
    BeforeSync,
    /// After the batch is fsync-durable but **before** its messages are sent
    /// (this subsumes the after-accept-before-`Accepted`-reply seam). A crash
    /// here keeps the durable writes but drops the batch's outbound messages;
    /// the peers must recover from the restarted node re-deriving them.
    AfterSyncBeforeSend,
}

/// Optional driver-level fault and policy hooks.
///
/// Each method corresponds to one independent `BUGGIFY` location in simulation.
/// The default implementation is production behavior: never crash, always
/// re-send pending accepts, retain leadership, and use normal randomized
/// election timeouts.
pub trait DriverHooks {
    /// Whether to simulate a crash at `seam` right now.
    fn crash_at(&self, _seam: Seam) -> bool {
        false
    }

    /// Whether to skip a re-send that has pending `Accept`s to send.
    fn skip_accept_resend(&self) -> bool {
        false
    }

    /// Whether the current leader should voluntarily step down.
    fn resign_leadership(&self) -> bool {
        false
    }

    /// Whether the next election timeout should use the shortest valid value.
    fn shortest_election_timeout(&self) -> bool {
        false
    }
}

/// Inert production hooks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHooks;

impl DriverHooks for NoHooks {}
