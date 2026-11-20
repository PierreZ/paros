//! The events of a cluster walk as a trace of
//! `specs/tla/matchmaker/MatchmakerTrace.tla`: matchmaking, Phase 1 over
//! every configuration of `H_b`, Phase 2 in `C_b` and the GC, on the
//! flavors with matchmakers.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::membership::MatchmakerId;
use paros_consensus::{Ballot, NodeId};

use super::{ClusterEvent, Sizes, encode};
use crate::trace::{Ranks, Trace, int, node};

/// The steps of `Matchmaker.tla` and the log steps of the trace spec. A
/// step down changes nothing in it: a leader that stepped down asks for
/// no GC, and its rounds stay open as a message the code never delivers.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::Select { .. }
            | ClusterEvent::Register { .. }
            | ClusterEvent::Prepare { .. }
            | ClusterEvent::Promise { .. }
            | ClusterEvent::Win { .. }
            | ClusterEvent::Propose { .. }
            | ClusterEvent::Accept { .. }
            | ClusterEvent::Record { .. }
            | ClusterEvent::Learn { .. }
            | ClusterEvent::Restart { .. }
            | ClusterEvent::GcAsk { .. }
            | ClusterEvent::GcAck { .. }
    )
}

/// Every ballot that a walk names: a campaign registers a ballot before
/// it prepares it, and a ballot that never prepared is in the histories.
fn ballots(events: &[ClusterEvent]) -> BTreeSet<Ballot> {
    events
        .iter()
        .filter_map(|event| match event {
            ClusterEvent::Select { ballot, .. }
            | ClusterEvent::Register { ballot, .. }
            | ClusterEvent::Prepare { ballot, .. }
            | ClusterEvent::GcAsk { ballot }
            | ClusterEvent::GcAck { ballot, .. }
            | ClusterEvent::Basis { ballot, .. }
            | ClusterEvent::ReadStart { ballot, .. }
            | ClusterEvent::ReadAck {
                fence: Some(ballot),
                ..
            } => Some(*ballot),
            _ => None,
        })
        .collect()
}

/// The events of one cluster walk of a pool of `nodes` and `matchmakers`
/// matchmakers as a trace of `MatchmakerTrace.tla`.
pub(super) fn trace(nodes: u64, matchmakers: u64, events: &[ClusterEvent]) -> Trace {
    let ballots = ballots(events);
    let ranks = Ranks::new(ballots.iter().copied());
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    let mut configs = BTreeSet::new();
    for event in events {
        if let ClusterEvent::Select { config: c, .. } = event {
            configs.insert(encode::config(c));
        }
        encode::write(&mut trace, &ranks, 0, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..nodes).map(|id| node(NodeId(id))).collect()),
    );
    trace.define(
        "TraceMatchmakers",
        Value::Set(
            (0..matchmakers)
                .map(|id| encode::matchmaker(MatchmakerId(id)))
                .collect(),
        ),
    );
    trace.define("TraceConfigs", Value::Set(configs.into_iter().collect()));
    trace.define("TraceValues", sizes.commands(true));
    trace.define("TraceNB", ranks.count());
    trace.define("TraceNS", int(sizes.slots.max(1)));
    trace.define(
        "TraceOwner",
        Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    trace
}
