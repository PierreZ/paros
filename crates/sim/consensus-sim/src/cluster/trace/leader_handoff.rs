//! The events of a cluster walk as a trace of
//! `specs/tla/leader-handoff/LeaderHandoffTrace.tla`: the guards of the
//! cooperative handoff, on the handoff flavor.

use moonpool_tla::Value;
use paros_consensus::NodeId;

use super::{ClusterEvent, Sizes, encode, ranks};
use crate::trace::{Trace, int, node};

/// The steps of `LeaderHandoff.tla` and the steps of the trace spec. A
/// round a leader sends to the acceptors or to a proxy is a round of its
/// ballot.
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
            | ClusterEvent::StepDown { .. }
            | ClusterEvent::Relinquish { .. }
            | ClusterEvent::Install { .. }
    )
}

/// The events of one cluster walk of `acceptors` nodes on a majority as a
/// trace of `LeaderHandoffTrace.tla`.
pub(super) fn trace(acceptors: u64, events: &[ClusterEvent]) -> Trace {
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
    trace.define("TraceNS", int(sizes.slots.max(1)));
    trace.define(
        "TraceOwner",
        Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    trace.define("TraceRestarts", int(sizes.restarts));
    trace
}
