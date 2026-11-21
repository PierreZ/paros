//! Model-based trace checking of the register walk: its events as a trace
//! of `specs/tla/segment-records/RegisterTrace.tla` (the actions of
//! `SegmentRecords.tla`), and the named edits of recorded register traces
//! (#1308 (mutate recorded traces)).
//!
//! The encoding: a machine is its name in the model (`"X"`, `"Y"`, `"Z"`,
//! `"W"`); an operation is its index, from 1, which is also its generation;
//! a record value is `<<st, end, term, cfg>>` ([`Abstract`]); a successor
//! state is the generation `wop` and the value `val` of its write, and its
//! read generation `rg`. The trace defines the model's constants `TraceOps`
//! (the kind of each operation) and `TraceStart` (the record before the
//! first operation).

use moonpool_tla::Value;

use super::mutate::{Edit, change_first, is, num, text, with};
use super::{Trace, TraceCheck, int, specs_dir};
use crate::register::{Abstract, AbstractSuccessor, Kind, RegisterEvent, name};

/// The check of `RegisterTrace` in `specs/tla/segment-records/`.
#[must_use]
pub fn register_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("segment-records"), "RegisterTrace")
}

fn value(value: Abstract) -> Value {
    let (st, end, term, cfg) = value;
    Value::Seq(vec![int(st), int(end), int(term), int(cfg)])
}

fn machine(id: paros_journal::register::MatchmakerId) -> Value {
    Value::from(name(id))
}

fn machines(ids: &[paros_journal::register::MatchmakerId]) -> Value {
    Value::str_set(ids.iter().map(|id| name(*id)))
}

fn successor(fields: &mut Vec<(&'static str, Value)>, (wop, val, rg): AbstractSuccessor) {
    fields.push(("wop", int(wop)));
    fields.push(("val", value(val)));
    fields.push(("rg", int(rg)));
}

/// The events of one traced register walk, with its operations `kinds` and
/// its `start` value, as a trace of `RegisterTrace.tla`.
#[must_use]
pub fn trace<'a>(
    kinds: &[Kind],
    start: Abstract,
    events: impl IntoIterator<Item = &'a RegisterEvent>,
) -> Trace {
    let mut trace = Trace::new();
    for event in events {
        let mut fields: Vec<(&'static str, Value)> = Vec::new();
        let kind = match event {
            RegisterEvent::Read {
                i,
                g,
                q,
                refused,
                seen,
            } => {
                fields.push(("i", int(*i)));
                fields.push(("g", int(*g)));
                fields.push(("q", machines(q)));
                fields.push((
                    "res",
                    Value::from(if *refused { "refused" } else { "read" }),
                ));
                fields.push(("seen", value(*seen)));
                "read"
            }
            RegisterEvent::Write { i, m, value: val } => {
                fields.push(("i", int(*i)));
                fields.push(("m", machine(*m)));
                fields.push(("val", value(*val)));
                "write"
            }
            RegisterEvent::Retire => "retire",
            RegisterEvent::Drop => "drop",
            RegisterEvent::Stop(m) => {
                fields.push(("m", machine(*m)));
                "stop"
            }
            RegisterEvent::Close { q, successor: s } => {
                fields.push(("q", machines(q)));
                successor(&mut fields, *s);
                "close"
            }
            RegisterEvent::Bootstrap(m, s) => {
                fields.push(("m", machine(*m)));
                successor(&mut fields, *s);
                "bootstrap"
            }
            RegisterEvent::Decide(s) => {
                successor(&mut fields, *s);
                "decide"
            }
            RegisterEvent::Activate(m) => {
                fields.push(("m", machine(*m)));
                "activate"
            }
            RegisterEvent::Crash(m) => {
                fields.push(("m", machine(*m)));
                "crash"
            }
        };
        trace.event(kind, fields);
    }
    trace.define(
        "TraceOps",
        Value::Seq(kinds.iter().map(|k| Value::from(k.name())).collect()),
    );
    trace.define("TraceStart", value(start));
    trace
}

/// An event named `kind` with `fields`, as a recorded trace holds it.
fn event(kind: &str, fields: &[(&str, Value)]) -> Value {
    Value::record(
        [("event", Value::from(kind))]
            .into_iter()
            .chain(fields.iter().cloned())
            .map(|(k, v)| (k.to_owned(), v)),
    )
}

/// Insert `new` at `at`.
fn insert(events: &[Value], at: usize, new: Value) -> (usize, Vec<Value>) {
    let mut edited = events.to_vec();
    edited.insert(at, new);
    (at, edited)
}

/// Move the first event of `kind` to just after the first event of
/// `after`, if it is later.
fn move_after(events: &[Value], kind: &str, after: &str) -> Option<(usize, Vec<Value>)> {
    let from = events.iter().position(|e| is(e, kind))?;
    let to = events.iter().position(|e| is(e, after))? + 1;
    if to >= from {
        return None;
    }
    let mut edited = events.to_vec();
    let moved = edited.remove(from);
    edited.insert(to, moved);
    Some((to, edited))
}

/// The field `field` of a `<<st, end, term, cfg>>` value, plus one.
fn bump(event: &Value, name: &str, field: usize) -> Value {
    let Some(Value::Seq(mut items)) = super::mutate::fields(event).get(name).cloned() else {
        panic!("a value field");
    };
    let Value::Int(n) = items[field] else {
        panic!("an integer");
    };
    items[field] = Value::Int(n + 1);
    with(event, name, Value::Seq(items))
}

/// The members of a `q` field.
fn members(event: &Value) -> Vec<Value> {
    match super::mutate::fields(event).get("q") {
        Some(Value::Set(items)) => items.clone(),
        _ => Vec::new(),
    }
}

/// The named edits of recorded register traces.
pub const EDITS: &[Edit] = &[
    Edit::new("a read saw a higher term", |events| {
        change_first(events, |e, i| is(&e[i], "read"), |e| bump(e, "seen", 2))
    }),
    Edit::new("a read saw a later state", |events| {
        change_first(events, |e, i| is(&e[i], "read"), |e| bump(e, "seen", 0))
    }),
    Edit::new("a refused change read", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "read") && text(&e[i], "res") == "refused",
            |e| with(e, "res", Value::from("read")),
        )
    }),
    Edit::new("a change refused", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "read") && text(&e[i], "res") == "read",
            |e| with(e, "res", Value::from("refused")),
        )
    }),
    Edit::new("a read at a minority", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "read"),
            |e| with(e, "q", Value::Set(members(e)[..1].to_vec())),
        )
    }),
    Edit::new("a read at a set not yet chosen", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "read") && num(&e[i], "g") == Some(0),
            |e| with(e, "g", Value::Int(1)),
        )
    }),
    Edit::new("a write of another value", |events| {
        change_first(events, |e, i| is(&e[i], "write"), |e| bump(e, "val", 2))
    }),
    Edit::new("a write of an operation that never ran", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "write"),
            |e| with(e, "i", Value::Int(99)),
        )
    }),
    Edit::new("a member took a write twice", |events| {
        let at = events.iter().position(|e| is(e, "write"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("the acceptors retired before any write", |events| {
        Some(insert(events, 0, event("retire", &[])))
    }),
    Edit::new("a replica dropped before any write", |events| {
        Some(insert(events, 0, event("drop", &[])))
    }),
    Edit::new("a member of the next set froze", |events| {
        Some(insert(events, 0, event("stop", &[("m", Value::from("W"))])))
    }),
    Edit::new("a member froze twice", |events| {
        let at = events.iter().position(|e| is(e, "stop"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("a close at a member that never froze", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "close"),
            |e| with(e, "q", Value::str_set(["Y", "W"])),
        )
    }),
    Edit::new("a close took a lower write", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "close"),
            |e| with(e, "wop", Value::Int(num(e, "wop").unwrap_or(0) + 7)),
        )
    }),
    Edit::new("a close took a higher read generation", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "close"),
            |e| with(e, "rg", Value::Int(num(e, "rg").unwrap_or(0) + 1)),
        )
    }),
    Edit::new("a member of the old set only stored the state", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "bootstrap"),
            |e| with(e, "m", Value::from("X")),
        )
    }),
    Edit::new("the decree chose before every bootstrap", |events| {
        move_after(events, "decide", "close")
    }),
    Edit::new("the decree chose another state", |events| {
        change_first(events, |e, i| is(&e[i], "decide"), |e| bump(e, "val", 2))
    }),
    Edit::new(
        "a member of the old set only served the next one",
        |events| {
            change_first(
                events,
                |e, i| is(&e[i], "activate"),
                |e| with(e, "m", Value::from("X")),
            )
        },
    ),
    Edit::new("a member served before the decree", |events| {
        move_after(events, "activate", "close")
    }),
    Edit::new("two machines down", |events| {
        let mut edited = events.to_vec();
        edited.insert(0, event("crash", &[("m", Value::from("Y"))]));
        edited.insert(0, event("crash", &[("m", Value::from("X"))]));
        Some((0, edited))
    }),
    Edit::new("a member wrote after it crashed", |events| {
        let at = events.iter().position(|e| is(e, "write"))?;
        let m = text(&events[at], "m").to_owned();
        Some(insert(events, at, event("crash", &[("m", Value::from(m))])))
    }),
];
