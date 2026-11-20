//! The events of a cluster walk that `specs/tla/node/MultiPaxosTrace.tla`
//! keeps: the log of every flavor without matchmakers.

use super::ClusterEvent;

/// The log steps of `Node.tla`: Phase 1 and Phase 2 of the leader, the
/// votes, the learns and the restarts. The proxy's steps and the leader's
/// audiences are not in it: a delegated round is a round of its ballot.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Prepare { .. }
            | ClusterEvent::Promise { .. }
            | ClusterEvent::Win { .. }
            | ClusterEvent::Propose { .. }
            | ClusterEvent::Accept { .. }
            | ClusterEvent::Record { .. }
            | ClusterEvent::Learn { .. }
            | ClusterEvent::Restart { .. }
    )
}

