//! The events of a cluster walk as a trace of
//! `specs/tla/proxy-leader/ProxyLeaderTrace.tla`: the proxy's `Commit`
//! rule, on the flavors with proxy leaders.

use moonpool_tla::Value;
use paros_consensus::NodeId;
use paros_consensus::membership::ProxyId;

use super::super::Flavor;
use super::{ClusterEvent, Sizes, encode, grid, ranks};
use crate::trace::{Trace, int, node};

/// The ballot ranks of the code start at 2: rank 1 is the bootstrap, that
/// no node runs (see the header of the trace spec).
const SHIFT: i64 = 1;

/// The steps of `ProxyLeader.tla` and the colocated steps of the trace
/// spec. A restart changes nothing in this model, and a leadership is a
/// ballot's, not a node's.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    !matches!(
        event,
        ClusterEvent::Restart { .. }
            | ClusterEvent::StepDown { .. }
            | ClusterEvent::Relinquish { .. }
            | ClusterEvent::Install { .. }
            | ClusterEvent::Select { .. }
            | ClusterEvent::Register { .. }
            | ClusterEvent::GcAsk { .. }
            | ClusterEvent::GcAck { .. }
    )
}

/// The events of one cluster walk of `flavor` as a trace of
/// `ProxyLeaderTrace.tla`.
pub(super) fn trace(flavor: &Flavor, events: &[ClusterEvent]) -> Trace {
    let (ballots, ranks) = ranks(events);
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in events {
        encode::write(&mut trace, &ranks, SHIFT, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..flavor.acceptors).map(|id| node(NodeId(id))).collect()),
    );
    trace.define(
        "TraceProxies",
        Value::Set(
            (0..flavor.proxies)
                .map(|id| encode::proxy(ProxyId(id)))
                .collect(),
        ),
    );
    trace.define("TraceCommands", sizes.commands(true));
    trace.define("TraceNB", int(ballots.len() + 1));
    // `NS` is at least 1: a model's slots are `0..NS-1`.
    trace.define("TraceNS", int(sizes.slots.max(1)));
    // The owner of each ballot: the bootstrap, then the ranks.
    let bootstrap = node(NodeId(0));
    trace.define(
        "TraceOwner",
        Value::Seq(
            std::iter::once(bootstrap)
                .chain(ballots.iter().map(|ballot| node(ballot.node)))
                .collect(),
        ),
    );
    trace.define("TraceCrashes", int(sizes.crashes));
    grid(&mut trace, flavor.quorum);
    trace
}
