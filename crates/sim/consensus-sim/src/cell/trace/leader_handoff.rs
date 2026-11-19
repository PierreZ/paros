//! The events of a cell walk as a trace of
//! `specs/tla/leader-handoff/LeaderHandoffTrace.tla`: the guards of the
//! cooperative handoff, on the handoff flavor.

use moonpool_tla::Value;
use paros_consensus::{NodeId, Slot};

use super::{CellEvent, Cmd, Sizes, rank, ranks, report, slot};
use crate::trace::{Ranks, Trace, int, node};

/// The steps of `LeaderHandoff.tla` and the steps of the trace spec. A
/// round a leader sends to the acceptors or to a proxy is a round of its
/// ballot.
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
            | CellEvent::StepDown { .. }
            | CellEvent::Relinquish { .. }
            | CellEvent::Install { .. }
    )
}

/// The slots of a payload, as a set of `[slot, val]` records.
fn tail(sizes: &mut Sizes, slots: &[(Slot, Cmd)]) -> Value {
    Value::Set(
        slots
            .iter()
            .map(|(s, cmd)| {
                sizes.at(*s, *cmd);
                Value::record([("slot", slot(*s)), ("val", cmd.value())])
            })
            .collect(),
    )
}

/// Write one event: its name and its fields, ballots as ranks.
fn write(trace: &mut Trace, ranks: &Ranks, sizes: &mut Sizes, event: &CellEvent) {
    let bal = |b| rank(ranks, 0, b);
    match event {
        CellEvent::Prepare { ballot, from, .. } => {
            sizes.bound(*from);
            trace.event(
                "prepare",
                [
                    ("bal", bal(*ballot)),
                    ("owner", node(ballot.node)),
                    ("from", slot(*from)),
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
            ballot,
            frontier,
            known,
        } => {
            sizes.bound(*frontier);
            let known = tail(sizes, known);
            trace.event(
                "win",
                [
                    ("bal", bal(*ballot)),
                    ("front", slot(*frontier)),
                    ("known", known),
                ],
            );
        }
        CellEvent::Propose {
            by,
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "propose",
                [
                    ("by", node(*by)),
                    ("bal", bal(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Accept {
            acceptor,
            ballot,
            slot: s,
            cmd,
            own,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "accept",
                [
                    ("acc", node(*acceptor)),
                    ("bal", bal(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                    ("own", Value::Bool(*own)),
                ],
            );
        }
        CellEvent::Record {
            node: id,
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "record",
                [
                    ("node", node(*id)),
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
            sizes.restarts += 1;
            trace.event("restart", [("node", node(*id))]);
        }
        CellEvent::StepDown { node: id } => {
            trace.event("stepdown", [("node", node(*id))]);
        }
        CellEvent::Relinquish {
            from,
            to,
            ballot,
            lo,
            next,
            decided,
            pending,
        } => {
            sizes.bound(*next);
            let dec = tail(sizes, decided);
            let pen = tail(sizes, pending);
            trace.event(
                "relinquish",
                [
                    ("from", node(*from)),
                    ("to", node(*to)),
                    ("bal", bal(*ballot)),
                    ("lo", slot(*lo)),
                    ("nxt", slot(*next)),
                    ("dec", dec),
                    ("pen", pen),
                ],
            );
        }
        CellEvent::Install { node: id, ballot } => {
            trace.event("install", [("node", node(*id)), ("bal", bal(*ballot))]);
        }
        other => unreachable!("LeaderHandoff keeps no {other:?}"),
    }
}

/// The events of one cell walk of `acceptors` nodes on a majority as a
/// trace of `LeaderHandoffTrace.tla`.
pub(super) fn trace(acceptors: u64, events: &[CellEvent]) -> Trace {
    let (ballots, ranks) = ranks(events);
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in events {
        write(&mut trace, &ranks, &mut sizes, event);
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
