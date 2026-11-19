//! The events of a cell walk as a trace of
//! `specs/tla/proxy-leader/ProxyLeaderTrace.tla`: the proxy's `Commit`
//! rule, on the flavors with proxy leaders.

use moonpool_tla::Value;
use paros_consensus::NodeId;
use paros_consensus::membership::ProxyId;

use super::{CellEvent, Sizes, grid, rank, ranks, report, slot};
use super::super::Flavor;
use crate::trace::{Ranks, Trace, int, node};

/// The ballot ranks of the code start at 2: rank 1 is the bootstrap, that
/// no node runs (see the header of the trace spec).
const SHIFT: i64 = 1;

/// The steps of `ProxyLeader.tla` and the colocated steps of the trace
/// spec. A restart changes nothing in this model, and a leadership is a
/// ballot's, not a node's.
pub(super) fn keeps(event: &CellEvent) -> bool {
    !matches!(
        event,
        CellEvent::Restart { .. }
            | CellEvent::StepDown { .. }
            | CellEvent::Relinquish { .. }
            | CellEvent::Install { .. }
            | CellEvent::Select { .. }
            | CellEvent::Register { .. }
            | CellEvent::GcAsk { .. }
            | CellEvent::GcAck { .. }
    )
}

/// The name of proxy `id` in a trace: `"p<id>"`.
fn proxy(id: ProxyId) -> Value {
    Value::Str(format!("p{}", id.0))
}

/// The fields of a round: its ballot, its slot and its value.
fn round(
    ranks: &Ranks,
    sizes: &mut Sizes,
    ballot: paros_consensus::Ballot,
    s: paros_consensus::Slot,
    cmd: super::Cmd,
) -> [(&'static str, Value); 3] {
    sizes.at(s, cmd);
    [
        ("bal", rank(ranks, SHIFT, ballot)),
        ("slot", slot(s)),
        ("val", cmd.value()),
    ]
}

/// Write one event: its name and its fields, ballots as ranks.
fn write(trace: &mut Trace, ranks: &Ranks, sizes: &mut Sizes, event: &CellEvent) {
    match event {
        CellEvent::Prepare { ballot, from, .. } => {
            sizes.bound(*from);
            trace.event(
                "prepare",
                [
                    ("bal", rank(ranks, SHIFT, *ballot)),
                    ("owner", node(ballot.node)),
                ],
            );
        }
        CellEvent::Promise {
            acceptor,
            ballot,
            from,
            votes,
        } => {
            let rep = report(ranks, SHIFT, sizes, votes);
            trace.event(
                "promise",
                [
                    ("acc", node(*acceptor)),
                    ("bal", rank(ranks, SHIFT, *ballot)),
                    ("from", slot(*from)),
                    ("rep", rep),
                ],
            );
        }
        CellEvent::Win { ballot, .. } => {
            trace.event("win", [("bal", rank(ranks, SHIFT, *ballot))]);
        }
        CellEvent::Propose {
            ballot, slot, cmd, ..
        } => {
            trace.event("propose", round(ranks, sizes, *ballot, *slot, *cmd));
        }
        CellEvent::Direct { ballot, slot, cmd } => {
            trace.event("direct", round(ranks, sizes, *ballot, *slot, *cmd));
        }
        CellEvent::Delegate {
            ballot,
            slot,
            cmd,
            proxy: p,
        } => {
            let [b, s, v] = round(ranks, sizes, *ballot, *slot, *cmd);
            trace.event("delegate", [b, s, v, ("proxy", proxy(*p))]);
        }
        CellEvent::Fanout {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = round(ranks, sizes, *ballot, *slot, *cmd);
            trace.event("fanout", [b, s, v, ("proxy", proxy(*p))]);
        }
        CellEvent::Commit {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = round(ranks, sizes, *ballot, *slot, *cmd);
            trace.event("commit", [b, s, v, ("proxy", proxy(*p))]);
        }
        CellEvent::Crash { proxy: p } => {
            sizes.crashes += 1;
            trace.event("crash", [("proxy", proxy(*p))]);
        }
        CellEvent::Accept {
            acceptor,
            ballot,
            slot,
            cmd,
            own,
        } => {
            let [b, s, v] = round(ranks, sizes, *ballot, *slot, *cmd);
            trace.event(
                "accept",
                [("acc", node(*acceptor)), b, s, v, ("own", Value::Bool(*own))],
            );
        }
        CellEvent::Record {
            node: id,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = round(ranks, sizes, *ballot, *slot, *cmd);
            trace.event("record", [("node", node(*id)), b, s, v]);
        }
        CellEvent::Learn { node: id, slot: s, cmd } => {
            sizes.at(*s, *cmd);
            trace.event(
                "learn",
                [("node", node(*id)), ("slot", slot(*s)), ("val", cmd.value())],
            );
        }
        other => unreachable!("ProxyLeader keeps no {other:?}"),
    }
}

/// The events of one cell walk of `flavor` as a trace of
/// `ProxyLeaderTrace.tla`.
pub(super) fn trace(flavor: &Flavor, events: &[CellEvent]) -> Trace {
    let (ballots, ranks) = ranks(events);
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in events {
        write(&mut trace, &ranks, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..flavor.acceptors).map(|id| node(NodeId(id))).collect()),
    );
    trace.define(
        "TraceProxies",
        Value::Set((0..flavor.proxies).map(|id| proxy(ProxyId(id))).collect()),
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
