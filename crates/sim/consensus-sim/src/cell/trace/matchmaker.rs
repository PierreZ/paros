//! The events of a cell walk as a trace of
//! `specs/tla/matchmaker/MatchmakerTrace.tla`: matchmaking, Phase 1 over
//! every configuration of `H_b`, Phase 2 in `C_b` and the GC, on the
//! flavors with matchmakers.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::membership::MatchmakerId;
use paros_consensus::{Ballot, NodeId};

use super::{CellEvent, Sizes, report, slot};
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

/// The name of matchmaker `id` in a trace: `"m<id>"`.
fn matchmaker(id: MatchmakerId) -> Value {
    Value::Str(format!("m{}", id.0))
}

/// A configuration: the set of its members.
fn config(members: &[NodeId]) -> Value {
    Value::Set(members.iter().map(|id| node(*id)).collect())
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
            | CellEvent::GcAck { ballot, .. } => Some(*ballot),
            _ => None,
        })
        .collect()
}

/// Write one event: its name and its fields, ballots as ranks.
fn write(trace: &mut Trace, ranks: &Ranks, sizes: &mut Sizes, event: &CellEvent) {
    let bal = |b| ranks.of(b);
    match event {
        CellEvent::Select { ballot, config: c } => {
            trace.event("select", [("bal", bal(*ballot)), ("cfg", config(c))]);
        }
        CellEvent::Register { matchmaker: m, ballot } => {
            trace.event("register", [("mm", matchmaker(*m)), ("bal", bal(*ballot))]);
        }
        CellEvent::Prepare {
            ballot,
            from,
            prior,
        } => {
            sizes.bound(*from);
            trace.event(
                "prepare",
                [
                    ("bal", bal(*ballot)),
                    ("owner", node(ballot.node)),
                    ("from", slot(*from)),
                    ("hist", Value::Set(prior.iter().map(|c| config(c)).collect())),
                ],
            );
        }
        CellEvent::Promise {
            acceptor,
            ballot,
            from,
            votes,
        } => {
            let rep = report(ranks, 0, sizes, votes);
            trace.event(
                "promise",
                [
                    ("acc", node(*acceptor)),
                    ("bal", bal(*ballot)),
                    ("from", slot(*from)),
                    ("rep", rep),
                ],
            );
        }
        CellEvent::Win {
            ballot, frontier, ..
        } => {
            sizes.bound(*frontier);
            trace.event("win", [("bal", bal(*ballot)), ("front", slot(*frontier))]);
        }
        CellEvent::Propose {
            ballot,
            slot: s,
            cmd,
            ..
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "propose",
                [
                    ("bal", bal(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Accept {
            acceptor: id,
            ballot,
            slot: s,
            cmd,
            ..
        }
        | CellEvent::Record {
            node: id,
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            let (name, who) = if matches!(event, CellEvent::Accept { .. }) {
                ("accept", "acc")
            } else {
                ("record", "node")
            };
            trace.event(
                name,
                [
                    (who, node(*id)),
                    ("bal", bal(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Learn {
            node: id,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "learn",
                [
                    ("node", node(*id)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Restart { node: id } => {
            trace.event("restart", [("node", node(*id))]);
        }
        CellEvent::GcAsk { ballot } => {
            trace.event("gcask", [("bal", bal(*ballot))]);
        }
        CellEvent::GcAck { matchmaker: m, ballot } => {
            trace.event("gcack", [("mm", matchmaker(*m)), ("bal", bal(*ballot))]);
        }
        other => unreachable!("Matchmaker keeps no {other:?}"),
    }
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
            configs.insert(config(c));
        }
        write(&mut trace, &ranks, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..nodes).map(|id| node(NodeId(id))).collect()),
    );
    trace.define(
        "TraceMatchmakers",
        Value::Set((0..matchmakers).map(|id| matchmaker(MatchmakerId(id))).collect()),
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
