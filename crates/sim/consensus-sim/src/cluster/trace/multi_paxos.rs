//! The events of a cluster walk as a trace of
//! `specs/tla/multi-paxos/MultiPaxosTrace.tla`: the log of every flavor
//! without matchmakers.

use moonpool_tla::Value;
use paros_consensus::NodeId;
use paros_consensus::membership::QuorumSystem;

use super::{ClusterEvent, Sizes, encode, grid, ranks};
use crate::trace::{Trace, int, node};

/// The steps of `MultiPaxos.tla`: Phase 1 and Phase 2 of the leader, the
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

