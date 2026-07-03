//! Crash-seam injection: the hook the driver calls at the durability seams
//! *inside* a `Ready` batch, where a fault injector can simulate a crash.
//!
//! `drain_ready` runs synchronously (no `.await`), so process-granularity chaos
//! (moonpool's attrition) can only crash a node *between* batches — never at the
//! persist/send seam within one. [`CrashSeam`] is the seam the simulation drives
//! with `buggify!()` to crash *inside* a batch. Production ships [`NoCrash`],
//! which never fires, so the seam is zero-cost and inert outside the simulation.

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

/// The driver's crash-seam hook. Called at each [`Seam`]; returning `true` tells
/// the driver to abandon the current batch — a simulated crash at that seam. The
/// caller (the node loop's owner) then recovers durable state and re-runs.
pub trait CrashSeam {
    /// Whether to simulate a crash at `seam` right now.
    fn crash_at(&self, seam: Seam) -> bool;
}

/// The production crash seam: never crashes. The driver ships this, so the seam
/// hook is inert outside the deterministic simulation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCrash;

impl CrashSeam for NoCrash {
    fn crash_at(&self, _seam: Seam) -> bool {
        false
    }
}
