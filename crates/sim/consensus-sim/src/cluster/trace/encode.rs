//! One encoding of the events for every trace spec: each event is a record
//! with its name and its fields, ballots as ranks. A spec reads the fields
//! it needs and ignores the others, so the four writers share it.

use moonpool_tla::Value;
use paros_consensus::membership::{MatchmakerId, ProxyId};
use paros_consensus::{Ballot, NodeId, Slot};

use super::{ClusterEvent, Cmd, Sizes, rank, report, slot};
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
    event: &ClusterEvent,
) {
    let bal = |b: Ballot| rank(ranks, shift, b);
    let (name, fields) = leader(&bal, ranks, shift, sizes, event)
        .or_else(|| round(&bal, sizes, event))
        .or_else(|| fault(&bal, sizes, event))
        .or_else(|| membership(&bal, event))
        .or_else(|| read(&bal, sizes, event))
        .or_else(|| floor(sizes, event))
        .or_else(|| replica(&bal, sizes, event))
        .or_else(|| faults(&bal, ranks, shift, sizes, event))
        .expect("every event has an encoding");
    trace.event(name, fields);
}

/// The steps of a leader: Phase 1, the proposals and the handoff.
fn leader(
    bal: &impl Fn(Ballot) -> Value,
    ranks: &Ranks,
    shift: i64,
    sizes: &mut Sizes,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Prepare {
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
        ClusterEvent::Promise {
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
        ClusterEvent::Win {
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
        ClusterEvent::Propose {
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
        ClusterEvent::StepDown { node: id } => ("stepdown", vec![("node", node(*id))]),
        ClusterEvent::Relinquish {
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
        ClusterEvent::Install { node: id, ballot } => {
            ("install", vec![("node", node(*id)), ("bal", bal(*ballot))])
        }
        _ => return None,
    })
}

/// The steps of a round: the proxies, the votes, the learns.
fn round(
    bal: &impl Fn(Ballot) -> Value,
    sizes: &mut Sizes,
    event: &ClusterEvent,
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
        ClusterEvent::Direct { ballot, slot, cmd } => ("direct", at(*ballot, *slot, *cmd).to_vec()),
        ClusterEvent::Delegate {
            ballot,
            slot,
            cmd,
            proxy: p,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("delegate", vec![b, s, v, ("proxy", proxy(*p))])
        }
        ClusterEvent::Fanout {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("fanout", vec![b, s, v, ("proxy", proxy(*p))])
        }
        ClusterEvent::Commit {
            proxy: p,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("commit", vec![b, s, v, ("proxy", proxy(*p))])
        }
        ClusterEvent::Close { proxy: p, slot: s } => {
            sizes.bound(Slot(s.0 + 1));
            ("close", vec![("proxy", proxy(*p)), ("slot", slot(*s))])
        }
        ClusterEvent::Accept {
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
        ClusterEvent::Record {
            node: id,
            ballot,
            slot,
            cmd,
        } => {
            let [b, s, v] = at(*ballot, *slot, *cmd);
            ("record", vec![("node", node(*id)), b, s, v])
        }
        ClusterEvent::Learn {
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
        _ => return None,
    })
}

/// The faults: a proxy crash, a node restart.
fn fault(
    bal: &impl Fn(Ballot) -> Value,
    sizes: &mut Sizes,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Crash { proxy: p } => {
            sizes.crashes += 1;
            ("crash", vec![("proxy", proxy(*p))])
        }
        ClusterEvent::Restart {
            node: id,
            upto,
            led,
        } => {
            sizes.restarts += 1;
            (
                "restart",
                vec![
                    ("node", node(*id)),
                    ("upto", slot(*upto)),
                    ("led", bal(led.unwrap_or_else(Ballot::zero))),
                ],
            )
        }
        _ => return None,
    })
}

/// The steps of matchmaking and of the GC.
fn membership(
    bal: &impl Fn(Ballot) -> Value,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Select { ballot, config: c } => {
            ("select", vec![("bal", bal(*ballot)), ("cfg", config(c))])
        }
        ClusterEvent::Register {
            matchmaker: m,
            ballot,
        } => (
            "register",
            vec![("mm", matchmaker(*m)), ("bal", bal(*ballot))],
        ),
        ClusterEvent::GcAsk { ballot } => ("gcask", vec![("bal", bal(*ballot))]),
        ClusterEvent::GcAck {
            matchmaker: m,
            ballot,
        } => ("gcack", vec![("mm", matchmaker(*m)), ("bal", bal(*ballot))]),
        ClusterEvent::Retire { node: id } => ("retire", vec![("node", node(*id))]),
        ClusterEvent::Rejoin { node: id } => ("rejoin", vec![("node", node(*id))]),
        _ => return None,
    })
}

/// A slot that may be none: `-1` for none.
fn maybe_slot(sizes: &mut Sizes, s: Option<Slot>) -> Value {
    s.map_or(Value::Int(-1), |s| {
        sizes.bound(Slot(s.0 + 1));
        slot(s)
    })
}

/// The steps of a leaderless read, and the writes that completed.
fn read(
    bal: &impl Fn(Ballot) -> Value,
    sizes: &mut Sizes,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Basis {
            node: id,
            ballot,
            fence,
        } => (
            "basis",
            vec![
                ("node", node(*id)),
                ("bal", bal(*ballot)),
                ("fence", maybe_slot(sizes, *fence)),
            ],
        ),
        ClusterEvent::ReadStart {
            node: id,
            ctx,
            ballot,
            fence,
            own,
        } => {
            let mut fields = vec![
                ("node", node(*id)),
                ("ctx", Value::Int(i64::try_from(*ctx).expect("a token"))),
                ("bal", bal(*ballot)),
                ("fence", maybe_slot(sizes, *fence)),
            ];
            if let Some(own) = own {
                fields.push(("own", maybe_slot(sizes, own.watermark)));
            }
            ("readstart", fields)
        }
        ClusterEvent::ReadAck {
            acceptor,
            ctx,
            watermark,
            fence,
        } => (
            "readack",
            vec![
                ("acc", node(*acceptor)),
                ("ctx", Value::Int(i64::try_from(*ctx).expect("a token"))),
                ("wm", maybe_slot(sizes, *watermark)),
                ("fence", fence.map_or(Value::Int(-1), bal)),
            ],
        ),
        ClusterEvent::ReadFold {
            node: id,
            acceptor,
            ctx,
        } => (
            "readfold",
            vec![
                ("node", node(*id)),
                ("acc", node(*acceptor)),
                ("ctx", Value::Int(i64::try_from(*ctx).expect("a token"))),
            ],
        ),
        ClusterEvent::ReadServe {
            node: id,
            ctx,
            index,
        } => (
            "readserve",
            vec![
                ("node", node(*id)),
                ("ctx", Value::Int(i64::try_from(*ctx).expect("a token"))),
                ("index", maybe_slot(sizes, *index)),
            ],
        ),
        ClusterEvent::Applied { slot: s } => {
            ("applied", vec![("slot", maybe_slot(sizes, Some(*s)))])
        }
        _ => return None,
    })
}

/// A journal position.
fn seq(position: u64) -> Value {
    Value::Int(i64::try_from(position).expect("a position"))
}

/// The steps of the compaction floor: the fold, a compaction and a jump.
fn floor(sizes: &mut Sizes, event: &ClusterEvent) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Fold {
            slot: s,
            high,
            first,
        } => {
            sizes.bound(Slot(s.0 + 1));
            (
                "fold",
                vec![
                    ("slot", slot(*s)),
                    ("high", seq(*high)),
                    ("first", seq(*first)),
                ],
            )
        }
        ClusterEvent::Compact { node: id, floor } | ClusterEvent::Trim { node: id, floor } => {
            sizes.bound(*floor);
            let name = if matches!(event, ClusterEvent::Compact { .. }) {
                "compact"
            } else {
                "trim"
            };
            (name, vec![("node", node(*id)), ("floor", slot(*floor))])
        }
        _ => return None,
    })
}

/// The steps of a read replica.
fn replica(
    bal: &impl Fn(Ballot) -> Value,
    sizes: &mut Sizes,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    let token = |ctx: u64| Value::Int(i64::try_from(ctx).expect("a token"));
    Some(match event {
        ClusterEvent::ReplicaLearn {
            replica,
            slot: s,
            ballot,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            (
                "rlearn",
                vec![
                    ("rep", node(*replica)),
                    ("slot", slot(*s)),
                    ("bal", bal(*ballot)),
                    ("val", cmd.value()),
                ],
            )
        }
        ClusterEvent::ReplicaApply { replica, slot: s } => {
            sizes.bound(Slot(s.0 + 1));
            ("rapply", vec![("rep", node(*replica)), ("slot", slot(*s))])
        }
        ClusterEvent::ReplicaFloor {
            replica,
            floor,
            jump,
        } => {
            sizes.bound(*floor);
            (
                if *jump { "rtrim" } else { "rcompact" },
                vec![("rep", node(*replica)), ("floor", slot(*floor))],
            )
        }
        ClusterEvent::ReplicaBoot { replica, from } => {
            sizes.bound(*from);
            (
                "rboot",
                vec![("rep", node(*replica)), ("from", slot(*from))],
            )
        }
        ClusterEvent::ReplicaRead { replica, ctx } => {
            ("rread", vec![("rep", node(*replica)), ("ctx", token(*ctx))])
        }
        ClusterEvent::ReplicaServe {
            replica,
            ctx,
            index,
        } => (
            "rserve",
            vec![
                ("rep", node(*replica)),
                ("ctx", token(*ctx)),
                ("index", maybe_slot(sizes, *index)),
            ],
        ),
        _ => return None,
    })
}

/// The steps of the faulty entries of CTRL: a page of a promise, which
/// reports a faulty record as a vote of the value `"lost"`, and a boot
/// that reads a faulty record.
fn faults(
    bal: &impl Fn(Ballot) -> Value,
    ranks: &Ranks,
    shift: i64,
    sizes: &mut Sizes,
    event: &ClusterEvent,
) -> Option<(&'static str, Fields)> {
    Some(match event {
        ClusterEvent::Page {
            acceptor,
            ballot,
            from,
            next,
            votes,
            faulty,
        } => {
            sizes.bound(*from);
            let Value::Set(mut rep) = report(ranks, shift, sizes, votes) else {
                unreachable!("a report is a set");
            };
            rep.extend(faulty.iter().map(|(s, at)| {
                sizes.bound(Slot(s.0 + 1));
                Value::record([
                    ("slot", slot(*s)),
                    ("bal", bal(*at)),
                    ("val", Value::Str(LOST.to_owned())),
                ])
            }));
            (
                "promise",
                vec![
                    ("acc", node(*acceptor)),
                    ("bal", bal(*ballot)),
                    ("from", slot(*from)),
                    ("upto", maybe_slot(sizes, *next)),
                    ("rep", Value::Set(rep)),
                ],
            )
        }
        ClusterEvent::Hole {
            node: id,
            slot: hole,
        } => (
            "hole",
            vec![("node", node(*id)), ("slot", maybe_slot(sizes, *hole))],
        ),
        ClusterEvent::Rot {
            node: id,
            slot: s,
            ballot,
        } => {
            sizes.bound(Slot(s.0 + 1));
            (
                "rot",
                vec![
                    ("node", node(*id)),
                    ("slot", slot(*s)),
                    ("bal", bal(*ballot)),
                ],
            )
        }
        _ => return None,
    })
}

/// The value of a faulty record in a trace: `Lost` of `Truncation.tla`.
const LOST: &str = "lost";
