//! One encoding of the events for every trace spec: each event is a record
//! with its name and its fields, ballots as ranks. A spec reads the fields
//! it needs and ignores the others, so the four writers share it.

use moonpool_tla::Value;
use paros_consensus::membership::{MatchmakerId, ProxyId};
use paros_consensus::{Ballot, NodeId, Slot};

use super::{CellEvent, Cmd, Sizes, rank, report, slot};
use crate::trace::{Ranks, Trace, node};

/// The fields of one event.
type Fields = Vec<(&'static str, Value)>;

/// The name of proxy `id` in a trace: `"p<id>"`.
pub(super) fn proxy(id: ProxyId) -> Value {
    Value::Str(format!("p{}", id.0))
}

/// The name of matchmaker `id` in a trace: `"m<id>"`.
pub(super) fn matchmaker(id: MatchmakerId) -> Value {
    Value::Str(format!("m{}", id.0))
}

/// A configuration: the set of its members.
pub(super) fn config(members: &[NodeId]) -> Value {
    Value::Set(members.iter().map(|id| node(*id)).collect())
}

/// Slots with their commands, as a set of `[slot, val]` records.
fn rounds(sizes: &mut Sizes, slots: &[(Slot, Cmd)]) -> Value {
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

/// Write `event` into `trace`, ballots ranked from `shift + 1`.
pub(super) fn write(
    trace: &mut Trace,
    ranks: &Ranks,
    shift: i64,
    sizes: &mut Sizes,
    event: &CellEvent,
) {
    let bal = |b: Ballot| rank(ranks, shift, b);
    let (name, fields) = leader(&bal, ranks, shift, sizes, event)
        .or_else(|| round(&bal, sizes, event))
        .or_else(|| membership(&bal, event))
        .expect("every event has an encoding");
    trace.event(name, fields);
}

/// The steps of a leader: Phase 1, the proposals and the handoff.
fn leader(
    bal: &impl Fn(Ballot) -> Value,
    ranks: &Ranks,
    shift: i64,
    sizes: &mut Sizes,
    event: &CellEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        CellEvent::Prepare {
            ballot,
            from,
            prior,
        } => {
            sizes.bound(*from);
            let hist = Value::Set(prior.iter().map(|c| config(c)).collect());
            (
                "prepare",
                vec![
                    ("bal", bal(*ballot)),
                    ("owner", node(ballot.node)),
                    ("from", slot(*from)),
                    ("hist", hist),
                ],
            )
        }
        CellEvent::Promise {
            acceptor,
            ballot,
            from,
            votes,
        } => {
            let rep = report(ranks, shift, sizes, votes);
            (
                "promise",
                vec![
                    ("acc", node(*acceptor)),
                    ("bal", bal(*ballot)),
                    ("from", slot(*from)),
                    ("rep", rep),
                ],
            )
        }
        CellEvent::Win {
            ballot,
            frontier,
            known,
        } => {
            sizes.bound(*frontier);
            (
                "win",
                vec![
                    ("bal", bal(*ballot)),
                    ("front", slot(*frontier)),
                    ("known", rounds(sizes, known)),
                ],
            )
        }
        CellEvent::Propose {
            by,
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            (
                "propose",
                vec![
                    ("by", node(*by)),
                    ("bal", bal(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            )
        }
        CellEvent::StepDown { node: id } => ("stepdown", vec![("node", node(*id))]),
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
            (
                "relinquish",
                vec![
                    ("from", node(*from)),
                    ("to", node(*to)),
                    ("bal", bal(*ballot)),
                    ("lo", slot(*lo)),
                    ("nxt", slot(*next)),
                    ("dec", rounds(sizes, decided)),
                    ("pen", rounds(sizes, pending)),
                ],
            )
        }
        CellEvent::Install { node: id, ballot } => {
            ("install", vec![("node", node(*id)), ("bal", bal(*ballot))])
        }
        _ => return None,
    })
}

/// The steps of a round: the proxies, the votes, the learns, the crashes.
fn round(
    bal: &impl Fn(Ballot) -> Value,
    sizes: &mut Sizes,
    event: &CellEvent,
) -> Option<(&'static str, Fields)> {
    let mut at = |ballot: Ballot, s: Slot, cmd: Cmd| {
        sizes.at(s, cmd);
        [
            ("bal", bal(ballot)),
            ("slot", slot(s)),
            ("val", cmd.value()),
        ]
    };
    Some(match event {
        CellEvent::Direct { ballot, slot, cmd } => ("direct", at(*ballot, *slot, *cmd).to_vec()),
        CellEvent::Delegate {
            ballot,
            slot,
            cmd,
            proxy: p,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("delegate", vec![b, s, v, ("proxy", proxy(*p))])
        }
        CellEvent::Fanout {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("fanout", vec![b, s, v, ("proxy", proxy(*p))])
        }
        CellEvent::Commit {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("commit", vec![b, s, v, ("proxy", proxy(*p))])
        }
        CellEvent::Accept {
            acceptor,
            ballot,
            slot,
            cmd,
            own,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            (
                "accept",
                vec![
                    ("acc", node(*acceptor)),
                    b,
                    s,
                    v,
                    ("own", Value::Bool(*own)),
                ],
            )
        }
        CellEvent::Record {
            node: id,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("record", vec![("node", node(*id)), b, s, v])
        }
        CellEvent::Learn {
            node: id,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            (
                "learn",
                vec![
                    ("node", node(*id)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            )
        }
        CellEvent::Crash { proxy: p } => {
            sizes.crashes += 1;
            ("crash", vec![("proxy", proxy(*p))])
        }
        CellEvent::Restart { node: id, upto } => {
            sizes.restarts += 1;
            ("restart", vec![("node", node(*id)), ("upto", slot(*upto))])
        }
        _ => return None,
    })
}

/// The steps of matchmaking and of the GC.
fn membership(bal: &impl Fn(Ballot) -> Value, event: &CellEvent) -> Option<(&'static str, Fields)> {
    Some(match event {
        CellEvent::Select { ballot, config: c } => {
            ("select", vec![("bal", bal(*ballot)), ("cfg", config(c))])
        }
        CellEvent::Register {
            matchmaker: m,
            ballot,
        } => (
            "register",
            vec![("mm", matchmaker(*m)), ("bal", bal(*ballot))],
        ),
        CellEvent::GcAsk { ballot } => ("gcask", vec![("bal", bal(*ballot))]),
        CellEvent::GcAck {
            matchmaker: m,
            ballot,
        } => ("gcack", vec![("mm", matchmaker(*m)), ("bal", bal(*ballot))]),
        _ => return None,
    })
}
