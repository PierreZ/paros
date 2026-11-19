//! The events of a cell walk as a trace of
//! `specs/tla/multi-paxos/MultiPaxosTrace.tla`: the log of every flavor
//! without matchmakers.

use moonpool_tla::Value;
use paros_consensus::NodeId;
use paros_consensus::membership::QuorumSystem;

use super::{CellEvent, Sizes, encode, grid, ranks};
use crate::trace::{Trace, int, node};

/// The steps of `MultiPaxos.tla`: Phase 1 and Phase 2 of the leader, the
/// votes, the learns and the restarts. The proxy's steps and the leader's
/// audiences are not in it: a delegated round is a round of its ballot.
pub(super) fn keeps(event: &CellEvent) -> bool {
    matches!(
        event,
        CellEvent::Prepare { .. }
            | CellEvent::Promise { .. }
            | CellEvent::Win { .. }
            | CellEvent::Propose { .. }
            | CellEvent::Accept { .. }
            | CellEvent::Record { .. }
            | CellEvent::Learn { .. }
            | CellEvent::Restart { .. }
    )
}

/// The events of one cell walk as a trace of `MultiPaxosTrace.tla`, for
/// a cell of `acceptors` nodes under `quorum`.
pub(super) fn trace(acceptors: u64, quorum: QuorumSystem, events: &[CellEvent]) -> Trace {
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
