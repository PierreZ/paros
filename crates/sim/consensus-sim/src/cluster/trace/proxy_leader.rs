//! The events of a cluster walk that `specs/tla/node/ProxyLeaderTrace.tla`
//! keeps beside the log: the rounds of the proxy leaders. `node.rs`
//! writes the constants of the trace.

use super::ClusterEvent;

/// The steps of the proxy leaders in `Node.tla`: the leader's own
/// `Accept` (`Direct`), a delegation, a fan-out, a `Commit`, a close and a
/// crash.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Direct { .. }
            | ClusterEvent::Delegate { .. }
            | ClusterEvent::Fanout { .. }
            | ClusterEvent::Commit { .. }
            | ClusterEvent::Close { .. }
            | ClusterEvent::Crash { .. }
    )
}
