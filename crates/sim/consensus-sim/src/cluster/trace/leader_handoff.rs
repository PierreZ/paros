//! The events of a cluster walk that `specs/tla/node/LeaderHandoffTrace.tla`
//! keeps beside the log: the cooperative handoff. `node.rs` writes the
//! constants of the trace.

use super::ClusterEvent;

/// The steps of the handoff in `Node.tla`: a leader that steps down, a
/// handoff with its payload, and its install.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::StepDown { .. }
            | ClusterEvent::Relinquish { .. }
            | ClusterEvent::Install { .. }
    )
}
