//! The client's fault-injection port: the points where an operator process
//! can die between two durable writes, asked by the library's own loops.
//!
//! A multi-step operation (a fleet operation, a checkpoint and its truncate)
//! is safe only if an operator that dies after any step leaves state the next
//! run resumes from. The library asks [`ClientHooks::stop_at`] at each such
//! point, after the write landed. A `true` answer makes the operation stop
//! there, as if the process died: the caller sees
//! [`Interrupted::Stopped`](super::fleet::Interrupted::Stopped) or
//! [`CheckpointOutcome::Stopped`](super::checkpoint::CheckpointOutcome::Stopped)
//! and runs it again later. The simulation answers with BUGGIFY; production
//! passes [`NoClientHooks`], which never stops.
//!
//! The same rule as the driver's [`crate::DriverHooks`]: the fault is
//! decided where the code path is, in the shipped code, and the harness only
//! answers the question.

use super::fleet::Stage;

/// Where an operator can stop (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopPoint {
    /// A fleet operation wrote this step, and another step follows.
    Step(Stage),
    /// A checkpoint is written, and its truncate is not sent yet: the
    /// checkpoint stays mid-log.
    BeforeTruncate,
}

/// The client's fault-injection hooks (see the module doc). Every method has
/// a no-op default.
pub trait ClientHooks: Send + Sync + 'static {
    /// Whether the operator stops at `point`, as if its process died there.
    fn stop_at(&self, point: StopPoint) -> bool {
        let _ = point;
        false
    }
}

/// The production hooks: the operator never stops on purpose.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoClientHooks;

impl ClientHooks for NoClientHooks {}
