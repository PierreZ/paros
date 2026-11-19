//! The events of a cell walk as a trace of
//! `specs/tla/matchmaker/MatchmakerTrace.tla`: matchmaking, Phase 1 over
//! every configuration of `H_b`, Phase 2 in `C_b` and the GC, on the
//! flavors with matchmakers.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::membership::MatchmakerId;
use paros_consensus::{Ballot, NodeId};

use super::{CellEvent, Sizes, encode};
use crate::trace::{Ranks, Trace, int, node};

/// The steps of `Matchmaker.tla` and the log steps of the trace spec. A
/// step down changes nothing in it: a leader that stepped down asks for
/// no GC, and its rounds stay open as a message the code never delivers.
pub(super) fn keeps(event: &CellEvent) -> bool {
    matches!(
        event,
        CellEvent::Select { .. }
            | CellEvent::Register { .. }
            | CellEvent::Prepare { .. }
            | CellEvent::Promise { .. }
            | CellEvent::Win { .. }
            | CellEvent::Propose { .. }
            | CellEvent::Accept { .. }
            | CellEvent::Record { .. }
            | CellEvent::Learn { .. }
            | CellEvent::Restart { .. }
            | CellEvent::GcAsk { .. }
            | CellEvent::GcAck { .. }
    )
}

/// Every ballot that a walk names: a campaign registers a ballot before
/// it prepares it, and a ballot that never prepared is in the histories.
fn ballots(events: &[CellEvent]) -> BTreeSet<Ballot> {
    events
        .iter()
        .filter_map(|event| match event {
            CellEvent::Select { ballot, .. }
            | CellEvent::Register { ballot, .. }
            | CellEvent::Prepare { ballot, .. }
            | CellEvent::GcAsk { ballot }
            | CellEvent::GcAck { ballot, .. }
            | CellEvent::Basis { ballot, .. }
            | CellEvent::ReadStart { ballot, .. }
            | CellEvent::ReadAck {
                fence: Some(ballot),
                ..
            } => Some(*ballot),
            _ => None,
        })
        .collect()
}

/// The events of one cell walk of a pool of `nodes` and `matchmakers`
/// matchmakers as a trace of `MatchmakerTrace.tla`.
pub(super) fn trace(nodes: u64, matchmakers: u64, events: &[CellEvent]) -> Trace {
    let ballots = ballots(events);
    let ranks = Ranks::new(ballots.iter().copied());
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    let mut configs = BTreeSet::new();
    for event in events {
        if let CellEvent::Select { config: c, .. } = event {
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
