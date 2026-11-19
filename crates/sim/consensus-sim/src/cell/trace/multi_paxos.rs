//! The events of a cell walk as a trace of
//! `specs/tla/multi-paxos/MultiPaxosTrace.tla`: the log of every flavor
//! without matchmakers.

use paros_consensus::NodeId;
use paros_consensus::membership::QuorumSystem;

use super::{CellEvent, Sizes, grid, rank, ranks, report, slot};
use crate::trace::{Ranks, Trace, int, node};

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
            sizes.restarts += 1;
            trace.event("restart", [("node", node(*id))]);
        }
        other => unreachable!("MultiPaxos keeps no {other:?}"),
    }
}

/// The events of one cell walk as a trace of `MultiPaxosTrace.tla`, for
/// a cell of `acceptors` nodes under `quorum`.
pub(super) fn trace(acceptors: u64, quorum: QuorumSystem, events: &[CellEvent]) -> Trace {
    let (ballots, ranks) = ranks(events);
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in events {
        write(&mut trace, &ranks, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        moonpool_tla::Value::Set((0..acceptors).map(|id| node(NodeId(id))).collect()),
    );
    trace.define("TraceCommands", sizes.commands(false));
    trace.define("TraceNB", ranks.count());
    trace.define("TraceNS", int(sizes.slots));
    // The owner of each ballot, in the order of the ranks.
    trace.define(
        "TraceOwner",
        moonpool_tla::Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    trace.define("TraceRestarts", int(sizes.restarts));
    grid(&mut trace, quorum);
    trace
}
