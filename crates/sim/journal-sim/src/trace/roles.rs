//! Model-based trace checking of the roles walk: its events as a trace of
//! `specs/tla/journal-ctrl/RolesTrace.tla` (the actions of `Journal.tla`,
//! of the compaction in `JournalCompact.tla` and of CTRL in
//! `JournalCtrl.tla`),
//! and the named edits of recorded roles traces (#1308 (mutate recorded
//! traces)).
//!
//! The encoding: an acceptor, a replica, a segment and a slot are their
//! numbers; a ballot is its round; a command is written as in
//! `FoldTrace.tla` (`op`, then its fields); an answer is its name in the
//! model. The trace defines the model's sizes: `TraceGrids` (the acceptors
//! of each segment), `TraceSlots`, `TraceBallots`, `TraceCrashes`,
//! `TraceRots`, `TraceFresh` and `TraceValues` (the commands other than `Noop` that a
//! leader proposed at each slot).

use std::collections::{BTreeMap, BTreeSet};

use moonpool_tla::Value;
use paros_journal::{Command, Leader};

use super::mutate::{Edit, change_first, fields, is, num, text, with};
use super::{Trace, TraceCheck, command_fields, int, leader, specs_dir};
use crate::roles::RolesEvent;

/// The check of `RolesTrace` in `specs/tla/journal-ctrl/`.
#[must_use]
pub fn roles_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("journal-ctrl"), "RolesTrace")
}

/// The grid of the first segment of every roles walk.
const FIRST_GRID: [u64; 3] = [1, 2, 3];

fn set(items: impl IntoIterator<Item = u64>) -> Value {
    Value::Set(items.into_iter().map(int).collect())
}

fn command(command: &Command) -> Value {
    let mut fields = Vec::new();
    command_fields(&mut fields, command);
    Value::record(fields)
}

/// The sizes of the model that the events of a walk give.
struct Sizes {
    grids: Vec<Value>,
    slots: u64,
    ballots: u64,
    crashes: u64,
    rots: u64,
    values: BTreeMap<u64, BTreeSet<Value>>,
}

/// The events of one roles walk, whose fresh journal is `fresh`, as a
/// trace of `RolesTrace.tla`.
///
/// # Panics
///
/// When an event holds what the model does not have (an `Upgrade`).
#[must_use]
pub fn trace(fresh: Leader, events: &[RolesEvent]) -> Trace {
    let mut sizes = Sizes {
        grids: vec![set(FIRST_GRID)],
        slots: 1,
        ballots: 1,
        crashes: 0,
        rots: 0,
        values: BTreeMap::new(),
    };
    let encoded: Vec<_> = events.iter().map(|e| encode(e, &mut sizes)).collect();
    let mut trace = Trace::new();
    trace.define("TraceGrids", Value::Seq(sizes.grids));
    trace.define("TraceSlots", int(sizes.slots));
    trace.define("TraceBallots", int(sizes.ballots));
    trace.define("TraceCrashes", int(sizes.crashes));
    trace.define("TraceRots", int(sizes.rots));
    trace.define("TraceFresh", leader(fresh));
    trace.define(
        "TraceValues",
        Value::Seq(
            (0..sizes.slots)
                .map(|s| {
                    Value::Set(
                        sizes
                            .values
                            .remove(&s)
                            .unwrap_or_default()
                            .into_iter()
                            .collect(),
                    )
                })
                .collect(),
        ),
    );
    for (name, fields) in encoded {
        trace.event(name, fields);
    }
    trace
}

/// One event as an event of `RolesTrace.tla`, and the sizes it gives.
fn encode(event: &RolesEvent, sizes: &mut Sizes) -> (&'static str, Vec<(&'static str, Value)>) {
    match event {
        RolesEvent::Reshape { g, grid } => {
            assert_eq!(
                usize::try_from(*g).ok(),
                Some(sizes.grids.len() + 1),
                "segments open in order"
            );
            sizes.grids.push(set(grid.iter().copied()));
            (
                "reshape",
                vec![("g", int(*g)), ("grid", set(grid.iter().copied()))],
            )
        }
        RolesEvent::Register { b, from } => {
            sizes.ballots = sizes.ballots.max(*b);
            ("register", vec![("b", int(*b)), ("from", int(*from))])
        }
        RolesEvent::Promise { a, b } => ("promise", vec![("a", int(*a)), ("b", int(*b))]),
        RolesEvent::Accept { a, b, s } => {
            sizes.slots = sizes.slots.max(*s + 1);
            (
                "accept",
                vec![("a", int(*a)), ("b", int(*b)), ("s", int(*s))],
            )
        }
        RolesEvent::Flush { a } => ("flush", vec![("a", int(*a))]),
        RolesEvent::Crash { a } => {
            sizes.crashes += 1;
            ("crash", vec![("a", int(*a))])
        }
        RolesEvent::Phase1 { b, q, end } => {
            let mut fields = vec![("b", int(*b)), ("q", set(q.iter().copied()))];
            fields.extend(end.map(|end| ("end", int(end))));
            ("phase1", fields)
        }
        RolesEvent::Seal { b } => ("seal", vec![("b", int(*b))]),
        RolesEvent::Open { b, start } => ("open", vec![("b", int(*b)), ("start", int(*start))]),
        RolesEvent::Propose { b, s, command: c } => {
            sizes.slots = sizes.slots.max(*s + 1);
            if *c != Command::Noop {
                sizes.values.entry(*s).or_default().insert(command(c));
            }
            let mut fields = vec![("b", int(*b)), ("s", int(*s))];
            command_fields(&mut fields, c);
            ("propose", fields)
        }
        RolesEvent::Base { r } => ("base", vec![("r", int(*r))]),
        RolesEvent::Apply {
            r,
            s,
            command: c,
            res,
        } => {
            sizes.slots = sizes.slots.max(*s + 1);
            let mut fields = vec![("r", int(*r)), ("s", int(*s))];
            command_fields(&mut fields, c);
            // The model has one refusal for a truncate. A blind fold gives
            // no answer: the event has no `res`.
            if let Some(res) = res {
                let res = if matches!(c, Command::Truncate { .. }) && *res == "wrong" {
                    "fenced"
                } else {
                    res
                };
                fields.push(("res", Value::from(res)));
            }
            ("apply", fields)
        }
        RolesEvent::Boundary { r } => ("boundary", vec![("r", int(*r))]),
        RolesEvent::Retire { p } => ("retire", vec![("p", int(*p))]),
        RolesEvent::Trim { p, r } => ("trim", vec![("p", int(*p)), ("r", int(*r))]),
        RolesEvent::Drop { r } => ("drop", vec![("r", int(*r))]),
        RolesEvent::Compact { a, p, x } => (
            "compact",
            vec![("a", int(*a)), ("p", int(*p)), ("x", int(*x))],
        ),
        RolesEvent::CompactReplica { r, slot } => (
            "compact_replica",
            vec![("r", int(*r)), ("slot", int(*slot))],
        ),
        RolesEvent::Jump { r, slot, floor } => (
            "jump",
            vec![("r", int(*r)), ("slot", int(*slot)), ("floor", int(*floor))],
        ),
        RolesEvent::Rot { .. }
        | RolesEvent::Probe { .. }
        | RolesEvent::Skip { .. }
        | RolesEvent::Learn { .. }
        | RolesEvent::Repair { .. } => encode_ctrl(event, sizes),
        RolesEvent::Resolve { s, r, res } => (
            "resolve",
            vec![("s", int(*s)), ("r", int(*r)), ("res", Value::from(*res))],
        ),
    }
}

/// One event of CTRL (`JournalCtrl.tla`) as an event of `RolesTrace.tla`.
fn encode_ctrl(
    event: &RolesEvent,
    sizes: &mut Sizes,
) -> (&'static str, Vec<(&'static str, Value)>) {
    match event {
        RolesEvent::Rot { a, s } => {
            sizes.rots += 1;
            ("rot", vec![("a", int(*a)), ("s", int(*s))])
        }
        RolesEvent::Probe { b, s, q } => (
            "probe",
            vec![
                ("b", int(*b)),
                ("s", int(*s)),
                ("q", set(q.iter().copied())),
            ],
        ),
        RolesEvent::Skip { b, s, a } => {
            ("skip", vec![("b", int(*b)), ("s", int(*s)), ("a", int(*a))])
        }
        RolesEvent::Learn { b, s, command: c } => {
            let mut fields = vec![("b", int(*b)), ("s", int(*s))];
            command_fields(&mut fields, c);
            ("learn", fields)
        }
        RolesEvent::Repair {
            a,
            s,
            command: c,
            c: ballot,
        } => {
            let mut fields = vec![("a", int(*a)), ("s", int(*s)), ("c", int(*ballot))];
            command_fields(&mut fields, c);
            ("repair", fields)
        }
        _ => unreachable!("an event of CTRL"),
    }
}

/// The first flush that makes a promise durable and is the last flush of
/// its acceptor before a Phase 1 that counts that promise: without it, the
/// promise of that Phase 1 is still queued. The queue of each acceptor is
/// played from the events (`Flush` takes its head, `Crash` empties it).
fn promise_flush(events: &[Value]) -> Option<usize> {
    let mut queues: BTreeMap<i64, Vec<(bool, i64)>> = BTreeMap::new();
    for (i, event) in events.iter().enumerate() {
        let a = num(event, "a");
        if is(event, "promise") || is(event, "accept") {
            let a = a.expect("an acceptor");
            let b = num(event, "b").expect("a ballot");
            queues.entry(a).or_default().push((is(event, "promise"), b));
        } else if is(event, "compact") || is(event, "repair") {
            queues
                .entry(a.expect("an acceptor"))
                .or_default()
                .push((false, 0));
        } else if is(event, "crash") || is(event, "rot") {
            queues.remove(&a.expect("an acceptor"));
        } else if is(event, "flush") {
            let a = a.expect("an acceptor");
            let queue = queues.entry(a).or_default();
            if queue.is_empty() {
                continue;
            }
            let (promise, b) = queue.remove(0);
            if !promise {
                continue;
            }
            let counted = events[i + 1..]
                .iter()
                .take_while(|e| !(is(e, "flush") && num(e, "a") == Some(a)))
                .any(|e| {
                    is(e, "phase1")
                        && num(e, "b") == Some(b)
                        && matches!(fields(e).get("q"), Some(Value::Set(q)) if q.contains(&Value::Int(a)))
                });
            if counted {
                return Some(i);
            }
        }
    }
    None
}

fn remove(events: &[Value], at: usize) -> Vec<Value> {
    let mut edited = events.to_vec();
    edited.remove(at);
    edited
}

fn repeat(events: &[Value], at: usize) -> Vec<Value> {
    let mut edited = events.to_vec();
    edited.insert(at + 1, events[at].clone());
    edited
}

fn plus(event: &Value, name: &str, by: i64) -> Value {
    with(event, name, Value::Int(num(event, name).unwrap_or(0) + by))
}

fn same(a: &Value, b: &Value, names: &[&str]) -> bool {
    names.iter().all(|n| fields(a).get(*n) == fields(b).get(*n))
}

/// The answer of the first apply answered `from` becomes `to`.
macro_rules! answer_edit {
    ($name:expr, $from:expr, $to:expr) => {
        Edit::new($name, |events| {
            change_first(
                events,
                |e, i| is(&e[i], "apply") && text(&e[i], "res") == $from,
                |e| with(e, "res", Value::from($to)),
            )
        })
    };
}

/// The first event of `kind` that `pick` selects, repeated.
macro_rules! repeat_edit {
    ($name:expr, $kind:expr) => {
        Edit::new($name, |events| {
            let at = events.iter().position(|e| is(e, $kind))?;
            Some((at + 1, repeat(events, at)))
        })
    };
}

/// The named edits of recorded roles traces.
pub const EDITS: &[Edit] = &[
    answer_edit!("an accepted command refused", "ok", "fenced"),
    answer_edit!("a duplicate called taken", "dup", "taken"),
    answer_edit!("a Below called a duplicate", "below", "dup"),
    answer_edit!("a Noop answered as a write", "noop", "ok"),
    Edit::new("an apply at the next slot", |events| {
        change_first(events, |e, i| is(&e[i], "apply"), |e| plus(e, "s", 1))
    }),
    Edit::new("an apply of another write", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply") && text(&e[i], "op") == "w",
            |e| plus(e, "id", 100),
        )
    }),
    Edit::new("an apply dropped", |events| {
        let at = (0..events.len()).find(|&i| {
            is(&events[i], "apply")
                && events[i + 1..]
                    .iter()
                    .any(|e| is(e, "apply") && same(e, &events[i], &["r"]))
        })?;
        Some((at, remove(events, at)))
    }),
    Edit::new("a campaign that writes from one slot more", |events| {
        change_first(events, |e, i| is(&e[i], "register"), |e| plus(e, "from", 1))
    }),
    repeat_edit!("a campaign repeated", "register"),
    Edit::new("a seal end one higher", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "phase1") && num(&e[i], "end").is_some(),
            |e| plus(e, "end", 1),
        )
    }),
    Edit::new("a first leader with no seal end", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "phase1") && num(&e[i], "end").is_some(),
            |e| {
                let mut changed = fields(e).clone();
                changed.remove("end");
                Value::Record(changed)
            },
        )
    }),
    Edit::new("a Phase 1 on one report", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "phase1"),
            |e| {
                let Some(Value::Set(q)) = fields(e).get("q") else {
                    panic!("a Phase 1 names its reports");
                };
                with(e, "q", Value::Set(q[..1].to_vec()))
            },
        )
    }),
    Edit::new("a Phase 1 before its promises", |events| {
        let phase1 = events.iter().position(|e| is(e, "phase1"))?;
        let at = events[..phase1]
            .iter()
            .position(|e| is(e, "promise") && same(e, &events[phase1], &["b"]))?;
        let mut edited = events.to_vec();
        let moved = edited.remove(phase1);
        edited.insert(at, moved);
        Some((at, edited))
    }),
    Edit::new("an open at the next slot", |events| {
        change_first(events, |e, i| is(&e[i], "open"), |e| plus(e, "start", 1))
    }),
    Edit::new("a seal dropped", |events| {
        let at = events.iter().position(|e| is(e, "seal"))?;
        Some((at, remove(events, at)))
    }),
    Edit::new("a seal of another ballot", |events| {
        // Another ballot of the walk, so that the edit names a ballot of the
        // model: one that registers after the seal, so it has no Phase 1 at
        // the seal (a ballot of the same segment that already read the old
        // grid may seal there too).
        let at = events.iter().position(|e| is(e, "seal"))?;
        let other = events[at..].iter().find_map(|e| {
            (is(e, "register") && num(e, "b") != num(&events[at], "b"))
                .then(|| fields(e)["b"].clone())
        })?;
        let mut edited = events.to_vec();
        edited[at] = with(&events[at], "b", other);
        Some((at, edited))
    }),
    Edit::new("a proposal that skips a slot", |events| {
        change_first(events, |e, i| is(&e[i], "propose"), |e| plus(e, "s", 1))
    }),
    Edit::new("a Noop proposed where a write was", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "propose") && text(&e[i], "op") == "w",
            |e| {
                Value::record([
                    ("event", Value::from("propose")),
                    ("b", fields(e)["b"].clone()),
                    ("s", fields(e)["s"].clone()),
                    ("op", Value::from("noop")),
                ])
            },
        )
    }),
    Edit::new("a vote before its proposal", |events| {
        let accept = events.iter().position(|e| is(e, "accept"))?;
        let at = events[..accept]
            .iter()
            .position(|e| is(e, "propose") && same(e, &events[accept], &["b", "s"]))?;
        let mut edited = events.to_vec();
        let moved = edited.remove(accept);
        edited.insert(at, moved);
        Some((at, edited))
    }),
    repeat_edit!("a promise repeated", "promise"),
    Edit::new("a crash before a flush", |events| {
        let at = events.iter().position(|e| is(e, "flush"))?;
        let mut edited = events.to_vec();
        edited.insert(
            at,
            Value::record([
                ("event", Value::from("crash")),
                ("a", fields(&events[at])["a"].clone()),
            ]),
        );
        Some((at, edited))
    }),
    Edit::new("a flush dropped", |events| {
        let at = promise_flush(events)?;
        Some((at, remove(events, at)))
    }),
    Edit::new("a base dropped", |events| {
        let at = (0..events.len()).find(|&i| {
            is(&events[i], "base")
                && events[i + 1..]
                    .iter()
                    .any(|e| is(e, "apply") && same(e, &events[i], &["r"]))
        })?;
        Some((at, remove(events, at)))
    }),
    repeat_edit!("a base taken twice", "base"),
    repeat_edit!("a boundary written twice", "boundary"),
    repeat_edit!("a retire repeated", "retire"),
    repeat_edit!("a trim repeated", "trim"),
    repeat_edit!("a drop repeated", "drop"),
    Edit::new("a drop before its trim", |events| {
        let at = events.iter().position(|e| is(e, "trim"))?;
        let r = fields(&events[at])["p"].clone();
        let mut edited = events.to_vec();
        // A replica of the trimmed segment: replica 10p + 1.
        let Value::Int(p) = r else { return None };
        edited.insert(
            at,
            Value::record([
                ("event", Value::from("drop")),
                ("r", Value::Int(10 * p + 1)),
            ]),
        );
        Some((at, edited))
    }),
    Edit::new("a trim of the open segment", |events| {
        let at = events.iter().position(|e| is(e, "trim"))?;
        let newest = events[..at]
            .iter()
            .rev()
            .find(|e| is(e, "reshape"))
            .map(|e| fields(e)["g"].clone())?;
        let mut edited = events.to_vec();
        edited[at] = with(&events[at], "p", newest);
        Some((at, edited))
    }),
    repeat_edit!("a reshape repeated", "reshape"),
    Edit::new("an acceptor compaction one slot higher", |events| {
        change_first(events, |e, i| is(&e[i], "compact"), |e| plus(e, "x", 1))
    }),
    Edit::new("a replica compaction one slot higher", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "compact_replica"),
            |e| plus(e, "slot", 1),
        )
    }),
    Edit::new("a jump one slot higher", |events| {
        change_first(events, |e, i| is(&e[i], "jump"), |e| plus(e, "slot", 1))
    }),
    Edit::new("a jump with a floor one higher", |events| {
        change_first(events, |e, i| is(&e[i], "jump"), |e| plus(e, "floor", 1))
    }),
    Edit::new("a blind fold that answers", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply") && !fields(&e[i]).contains_key("res"),
            |e| with(e, "res", Value::from("ok")),
        )
    }),
    repeat_edit!("a rot repeated", "rot"),
    Edit::new("a rot of the next slot", |events| {
        change_first(events, |e, i| is(&e[i], "rot"), |e| plus(e, "s", 1))
    }),
    repeat_edit!("a probe repeated", "probe"),
    Edit::new("a probe on one answer", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "probe"),
            |e| {
                let Some(Value::Set(q)) = fields(e).get("q") else {
                    panic!("a probe names its answers");
                };
                with(e, "q", Value::Set(q[..1].to_vec()))
            },
        )
    }),
    repeat_edit!("a skip repeated", "skip"),
    Edit::new("a skip by an acceptor that did not answer", |events| {
        change_first(events, |e, i| is(&e[i], "skip"), |e| with(e, "a", int(99)))
    }),
    repeat_edit!("a learn repeated", "learn"),
    Edit::new("a learn of another command", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "learn"),
            |e| {
                if text(e, "op") == "w" {
                    plus(e, "id", 100)
                } else {
                    Value::record([
                        ("event", Value::from("learn")),
                        ("b", fields(e)["b"].clone()),
                        ("s", fields(e)["s"].clone()),
                        ("op", Value::from("w")),
                        ("u", Value::from("none")),
                        ("e", Value::Int(0)),
                        ("id", Value::Int(100)),
                    ])
                }
            },
        )
    }),
    repeat_edit!("a repair repeated", "repair"),
    Edit::new("a repair at a ballot one higher", |events| {
        change_first(events, |e, i| is(&e[i], "repair"), |e| plus(e, "c", 1))
    }),
    Edit::new("a written Resolve called not written", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "resolve") && text(&e[i], "res") == "dup",
            |e| with(e, "res", Value::from("taken")),
        )
    }),
];
