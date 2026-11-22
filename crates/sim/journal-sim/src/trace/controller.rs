//! Model-based trace checking of the controller walk: its events as a
//! trace of `specs/tla/grid-controller/ControllerTrace.tla` (the actions of
//! `GridController.tla`), and the named edits of recorded controller traces
//! (#1308 (mutate recorded traces)).
//!
//! The encoding: controller `c` is `"c<c + 1>"`; acceptor `n` is
//! `"a<n>"`; a call is its term `tm`, its expected configuration `exp` and
//! its members `mem` (the model's grid is `mem` with two acks); an answer
//! is the model's name (`"ok"`, `"term"`, `"exp"`, `"unretired"`).

use moonpool_tla::Value;

use super::mutate::{Edit, change_first, is, num, text, with};
use super::{Trace, TraceCheck, int, specs_dir};
use crate::controller::{Call, ControllerEvent};

/// The check of `ControllerTrace` in `specs/tla/grid-controller/`.
#[must_use]
pub fn controller_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("grid-controller"), "ControllerTrace")
}

fn ctl(c: usize) -> Value {
    Value::from(format!("c{}", c + 1))
}

fn acceptor(n: u64) -> String {
    format!("a{n}")
}

fn call(fields: &mut Vec<(&'static str, Value)>, c: usize, call: &Call) {
    fields.push(("c", ctl(c)));
    fields.push(("tm", int(call.tm)));
    fields.push(("exp", int(call.exp)));
    fields.push(("mem", Value::str_set(call.mem.iter().map(|n| acceptor(*n)))));
}

/// The events of one traced controller walk as a trace of
/// `ControllerTrace.tla`.
#[must_use]
pub fn trace<'a>(events: impl IntoIterator<Item = &'a ControllerEvent>) -> Trace {
    let mut trace = Trace::new();
    for event in events {
        let mut fields: Vec<(&'static str, Value)> = Vec::new();
        let kind = match event {
            ControllerEvent::ReadTerm(c) => {
                fields.push(("c", ctl(*c)));
                "read_term"
            }
            ControllerEvent::Elect { c, won } => {
                fields.push(("c", ctl(*c)));
                fields.push(("won", Value::Bool(*won)));
                "elect"
            }
            ControllerEvent::Plan(c, planned) => {
                call(&mut fields, *c, planned);
                "plan"
            }
            ControllerEvent::Call(c) => {
                fields.push(("c", ctl(*c)));
                "call"
            }
            ControllerEvent::Stand(c) => {
                fields.push(("c", ctl(*c)));
                "stand"
            }
            ControllerEvent::Process(c, sent, ans) => {
                call(&mut fields, *c, sent);
                fields.push(("ans", Value::from(*ans)));
                "process"
            }
            ControllerEvent::Receive(c, sent, ans) => {
                call(&mut fields, *c, sent);
                fields.push(("ans", Value::from(*ans)));
                "receive"
            }
            ControllerEvent::LoseReq(c, sent) => {
                call(&mut fields, *c, sent);
                "lose_req"
            }
            ControllerEvent::LoseRep(c, sent, ans) => {
                call(&mut fields, *c, sent);
                fields.push(("ans", Value::from(*ans)));
                "lose_rep"
            }
            ControllerEvent::Seal(g) => {
                fields.push(("g", int(*g)));
                "seal"
            }
            ControllerEvent::Retire(g) => {
                fields.push(("g", int(*g)));
                "retire"
            }
            ControllerEvent::Crash(c) => {
                fields.push(("c", ctl(*c)));
                "crash"
            }
            ControllerEvent::Restart(c) => {
                fields.push(("c", ctl(*c)));
                "restart"
            }
            ControllerEvent::Fail(a) => {
                fields.push(("a", Value::from(acceptor(*a))));
                "fail"
            }
        };
        trace.event(kind, fields);
    }
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

/// Remove the first event that `pick` finds.
fn remove(events: &[Value], pick: impl Fn(&Value) -> bool) -> Option<(usize, Vec<Value>)> {
    let at = events.iter().position(pick)?;
    let mut edited = events.to_vec();
    edited.remove(at);
    Some((at, edited))
}

/// The field `name` plus one.
fn bump(event: &Value, name: &str) -> Value {
    with(event, name, Value::Int(num(event, name).unwrap_or(0) + 1))
}

/// The other controller.
fn other(event: &Value) -> Value {
    Value::from(if text(event, "c") == "c1" { "c2" } else { "c1" })
}

/// The named edits of recorded controller traces.
pub const EDITS: &[Edit] = &[
    Edit::new("a candidate won a race it lost", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "elect") && fields_bool(&e[i], "won") == Some(false),
            |e| with(e, "won", Value::Bool(true)),
        )
    }),
    Edit::new("a candidate lost a race it won", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "elect") && fields_bool(&e[i], "won") == Some(true),
            |e| with(e, "won", Value::Bool(false)),
        )
    }),
    Edit::new("an election with no read of the term", |events| {
        remove(events, |e| is(e, "read_term"))
    }),
    Edit::new("the other controller elected", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "elect"),
            |e| with(e, "c", other(e)),
        )
    }),
    Edit::new("a plan by a controller with no term", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| with(e, "c", other(e)))
    }),
    Edit::new("a plan at another term", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| bump(e, "tm"))
    }),
    Edit::new("a plan from a configuration not the newest", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| bump(e, "exp"))
    }),
    Edit::new("a call with no plan", |events| {
        remove(events, |e| is(e, "plan"))
    }),
    Edit::new("a call sent while one is in flight", |events| {
        let at = events.iter().position(|e| is(e, "call"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("a request taken twice", |events| {
        let at = events.iter().position(|e| is(e, "process"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("an ok answered as a term refusal", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "process") && text(&e[i], "ans") == "ok",
            |e| with(e, "ans", Value::from("term")),
        )
    }),
    Edit::new("a term refusal answered ok", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "process") && text(&e[i], "ans") == "term",
            |e| with(e, "ans", Value::from("ok")),
        )
    }),
    Edit::new("a request of another grid taken", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "process"),
            |e| with(e, "mem", Value::str_set(["a1", "a2", "a4"])),
        )
    }),
    Edit::new("a reply of a request never taken", |events| {
        remove(events, |e| is(e, "process"))
    }),
    Edit::new("a reply received twice", |events| {
        let at = events.iter().position(|e| is(e, "receive"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("a request lost before it was sent", |events| {
        let at = events.iter().position(|e| is(e, "call"))?;
        let plan = events[..at].iter().rev().find(|e| is(e, "plan"))?;
        let mut lost = plan.clone();
        lost = with(&lost, "event", Value::from("lose_req"));
        Some(insert(events, at, lost))
    }),
    Edit::new("a stand by a holder of the term", |events| {
        let at = events
            .iter()
            .position(|e| is(e, "elect") && fields_bool(e, "won") == Some(true))?;
        let c = super::mutate::fields(&events[at])["c"].clone();
        Some(insert(events, at + 1, event("stand", &[("c", c)])))
    }),
    Edit::new("a seal before any reshape", |events| {
        Some(insert(events, 0, event("seal", &[("g", int(2))])))
    }),
    Edit::new("a retire of the configuration in force", |events| {
        Some(insert(events, 0, event("retire", &[("g", int(1))])))
    }),
    Edit::new("a seal skipped a configuration", |events| {
        change_first(events, |e, i| is(&e[i], "seal"), |e| bump(e, "g"))
    }),
    Edit::new("a restart of a controller that is up", |events| {
        Some(insert(
            events,
            0,
            event("restart", &[("c", Value::from("c1"))]),
        ))
    }),
    Edit::new("two acceptors failed", |events| {
        let at = events.iter().position(|e| is(e, "fail"))?;
        let next = if text(&events[at], "a") == "a1" {
            "a2"
        } else {
            "a1"
        };
        Some(insert(
            events,
            at + 1,
            event("fail", &[("a", Value::from(next))]),
        ))
    }),
    Edit::new("a candidate that is down read the term", |events| {
        let at = events.iter().position(|e| is(e, "read_term"))?;
        let c = super::mutate::fields(&events[at])["c"].clone();
        Some(insert(events, at, event("crash", &[("c", c)])))
    }),
];

/// The boolean field `name`.
fn fields_bool(event: &Value, name: &str) -> Option<bool> {
    match super::mutate::fields(event).get(name) {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    }
}
