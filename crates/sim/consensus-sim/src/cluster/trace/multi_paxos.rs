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

/// The events of one cluster walk as a trace of `MultiPaxosTrace.tla`, for
/// a cluster of `acceptors` nodes under `quorum`.
pub(super) fn trace(acceptors: u64, quorum: QuorumSystem, events: &[ClusterEvent]) -> Trace {
    let (ballots, ranks) = ranks(events);
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in events {
        encode::write(&mut trace, &ranks, 0, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..acceptors).map(|id| node(NodeId(id))).collect()),
    );
    trace.define("TraceCommands", sizes.commands(false));
    trace.define("TraceNB", ranks.count());
    trace.define("TraceNS", int(sizes.slots));
    // The owner of each ballot, in the order of the ranks.
    trace.define(
        "TraceOwner",
        Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    trace.define("TraceRestarts", int(sizes.restarts));
    grid(&mut trace, quorum);
    trace
}
