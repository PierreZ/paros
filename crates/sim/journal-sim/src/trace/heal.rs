//! Model-based trace checking of the heal walk: its events as a trace of
//! `specs/tla/heal-reuse/HealTrace.tla` (the actions of `HealReuse.tla`),
//! and the named edits of recorded heal traces (#1308 (mutate recorded
//! traces)).
//!
//! The encoding: machine `n` is `"n<n>"`; controller `c` is `"c<c + 1>"`; a
//! copy is its term `tm` and its number `num` among the plans of all terms,
//! counted from 1 in the order of the plans, as the model counts them (the
//! walk numbers the copies of each term from 0); a set is a set of
//! machines.

use std::collections::BTreeMap;

use moonpool_tla::Value;
use paros_journal::NodeId;
use paros_journal::roles::CopyId;

use super::mutate::{Edit, change_first, fields, is, num, text, with};
use super::{Trace, TraceCheck, int, specs_dir};
use crate::heal::HealEvent;

/// The check of `HealTrace` in `specs/tla/heal-reuse/`.
#[must_use]
pub fn heal_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("heal-reuse"), "HealTrace")
}

fn node(n: NodeId) -> Value {
    Value::from(format!("n{}", n.0))
}

fn set(nodes: &[NodeId]) -> Value {
    Value::str_set(nodes.iter().map(|n| format!("n{}", n.0)))
}

fn ctl(c: usize) -> Value {
    Value::from(format!("c{}", c + 1))
}

/// The events of one traced heal walk as a trace of `HealTrace.tla`.
///
/// # Panics
///
/// When a node starts a copy that no plan asked.
#[must_use]
pub fn trace<'a>(events: impl IntoIterator<Item = &'a HealEvent>) -> Trace {
    let mut numbers: BTreeMap<CopyId, u64> = BTreeMap::new();
    let mut trace = Trace::new();
    let copy =
        |fields: &mut Vec<(&'static str, Value)>, numbers: &BTreeMap<CopyId, u64>, id: CopyId| {
            fields.push(("tm", int(id.term)));
            fields.push((
                "num",
                int(*numbers.get(&id).expect("a copy that a plan asked")),
            ));
        };
    for event in events {
        let mut fields: Vec<(&'static str, Value)> = Vec::new();
        match event {
            HealEvent::Read(n)
            | HealEvent::CopyStep(n)
            | HealEvent::Report(n)
            | HealEvent::Abandon(n)
            | HealEvent::Ends(n)
            | HealEvent::Drop(n)
            | HealEvent::Crash(n)
            | HealEvent::Restart(n) => fields.push(("r", node(*n))),
            HealEvent::Join(n, id) => {
                fields.push(("r", node(*n)));
                copy(&mut fields, &numbers, *id);
            }
            HealEvent::Elect(c)
            | HealEvent::CtlCrash(c)
            | HealEvent::Send(c)
            | HealEvent::Done(c)
            | HealEvent::Abort(c) => fields.push(("c", ctl(*c))),
            HealEvent::Plan {
                c,
                read,
                heals,
                remove,
                add,
                copy: id,
            } => {
                let next = numbers.len() as u64 + 1;
                assert!(
                    numbers.insert(*id, next).is_none(),
                    "each plan asks a new copy"
                );
                fields.push(("c", ctl(*c)));
                fields.push(("rd", set(read)));
                fields.push(("heals", int(*heals)));
                fields.push(("d", node(*remove)));
                fields.push(("n", node(*add)));
                copy(&mut fields, &numbers, *id);
            }
            HealEvent::Apply {
                term,
                read,
                heals,
                remove,
                add,
                ok,
                set: after,
            } => {
                fields.push(("tm", int(*term)));
                fields.push(("rd", set(read)));
                fields.push(("heals", int(*heals)));
                fields.push(("d", node(*remove)));
                fields.push(("n", node(*add)));
                fields.push(("ok", Value::Bool(*ok)));
                fields.push(("set", set(after)));
            }
            HealEvent::Answer(c, ok) => {
                fields.push(("c", ctl(*c)));
                fields.push(("ok", Value::Bool(*ok)));
            }
        }
        trace.event(kind(event), fields);
    }
    trace
}

/// The name of an event in `HealTrace.tla`.
fn kind(event: &HealEvent) -> &'static str {
    match event {
        HealEvent::Read(_) => "read",
        HealEvent::Join(..) => "join",
        HealEvent::CopyStep(_) => "copy",
        HealEvent::Report(_) => "report",
        HealEvent::Abandon(_) => "abandon",
        HealEvent::Ends(_) => "ends",
        HealEvent::Drop(_) => "drop",
        HealEvent::Crash(_) => "crash",
        HealEvent::Restart(_) => "restart",
        HealEvent::Elect(_) => "elect",
        HealEvent::CtlCrash(_) => "ctl_crash",
        HealEvent::Plan { .. } => "plan",
        HealEvent::Send(_) => "send",
        HealEvent::Apply { .. } => "apply",
        HealEvent::Answer(..) => "answer",
        HealEvent::Done(_) => "done",
        HealEvent::Abort(_) => "abort",
    }
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

/// The event at `at` again, right after it.
fn twice(events: &[Value], pick: impl Fn(&Value) -> bool) -> Option<(usize, Vec<Value>)> {
    let at = events.iter().position(pick)?;
    Some(insert(events, at + 1, events[at].clone()))
}

/// The field `name` plus one.
fn bump(event: &Value, name: &str) -> Value {
    with(event, name, Value::Int(num(event, name).unwrap_or(0) + 1))
}

/// The other controller.
fn other(event: &Value) -> Value {
    Value::from(if text(event, "c") == "c1" { "c2" } else { "c1" })
}

/// The boolean field `name`.
fn flag(event: &Value, name: &str) -> Option<bool> {
    match fields(event).get(name) {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// A spare that is not `n`.
fn another(event: &Value, name: &str) -> Value {
    Value::from(if text(event, name) == "n25" {
        "n24"
    } else {
        "n25"
    })
}

/// The named edits of recorded heal traces.
pub const EDITS: &[Edit] = &[
    Edit::new("a copy started with no read of the register", |events| {
        // A join whose plan came after a write of the set that the node
        // read just before the join: without that read, the node's view is
        // older than the ask.
        events.iter().enumerate().find_map(|(at, join)| {
            if !is(join, "join") {
                return None;
            }
            let r = &fields(join)["r"];
            let plan = events[..at].iter().rposition(|e| {
                is(e, "plan")
                    && num(e, "num") == num(join, "num")
                    && num(e, "tm") == num(join, "tm")
            })?;
            let read = events[..at]
                .iter()
                .rposition(|e| is(e, "read") && &fields(e)["r"] == r)?;
            let written = events[..plan]
                .iter()
                .rposition(|e| is(e, "apply") && flag(e, "ok") == Some(true))?;
            let between = events[written + 1..read]
                .iter()
                .any(|e| is(e, "read") && &fields(e)["r"] == r);
            (read > plan && !between).then(|| {
                let mut edited = events.to_vec();
                edited.remove(read);
                (read, edited)
            })
        })
    }),
    Edit::new("a node started a copy no plan asked", |events| {
        change_first(events, |e, i| is(&e[i], "join"), |e| bump(e, "num"))
    }),
    Edit::new("a node started its copy twice", |events| {
        twice(events, |e| is(e, "join"))
    }),
    Edit::new("another node started the copy", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "join"),
            |e| with(e, "r", another(e, "r")),
        )
    }),
    Edit::new("a copy step with no copy", |events| {
        let at = events.iter().position(|e| is(e, "join"))?;
        let r = fields(&events[at])["r"].clone();
        Some(insert(events, at, event("copy", &[("r", r)])))
    }),
    Edit::new("a report of a copy that is not complete", |events| {
        let at = events.iter().position(|e| is(e, "join"))?;
        let r = fields(&events[at])["r"].clone();
        Some(insert(events, at + 1, event("report", &[("r", r)])))
    }),
    Edit::new("a compare-and-set sent with no report", |events| {
        let send = events.iter().position(|e| is(e, "send"))?;
        let report = send.checked_sub(1)?;
        is(&events[report], "report").then(|| {
            let mut edited = events.to_vec();
            edited.remove(report);
            (report, edited)
        })
    }),
    Edit::new("a plan by a controller with no term", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| with(e, "c", other(e)))
    }),
    Edit::new("a plan at another term", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| bump(e, "tm"))
    }),
    Edit::new("a plan of a member that is up", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "plan"),
            |e| {
                let d = if text(e, "d") == "n11" { "n12" } else { "n11" };
                with(e, "d", Value::from(d))
            },
        )
    }),
    Edit::new("a plan on a read with another heal count", |events| {
        change_first(events, |e, i| is(&e[i], "plan"), |e| bump(e, "heals"))
    }),
    Edit::new("an election with a heal in flight", |events| {
        let at = events.iter().position(|e| is(e, "plan"))?;
        let c = fields(&events[at])["c"].clone();
        Some(insert(events, at + 1, event("elect", &[("c", c)])))
    }),
    Edit::new("an ok answered as a refusal", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply") && flag(&e[i], "ok") == Some(true),
            |e| with(e, "ok", Value::Bool(false)),
        )
    }),
    Edit::new("a refusal answered ok", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply") && flag(&e[i], "ok") == Some(false),
            |e| with(e, "ok", Value::Bool(true)),
        )
    }),
    Edit::new("a compare-and-set never sent applied", |events| {
        let at = events.iter().position(|e| is(e, "apply"))?;
        Some(insert(
            events,
            at,
            with(&events[at], "heals", Value::Int(7)),
        ))
    }),
    Edit::new("an applied heal wrote another set", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply") && flag(&e[i], "ok") == Some(true),
            |e| with(e, "set", Value::str_set(["n11", "n12", "n13"]).clone()),
        )
        .filter(|(at, edited)| edited[*at] != events[*at])
    }),
    Edit::new("an answer of the other kind", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "answer"),
            |e| with(e, "ok", Value::Bool(flag(e, "ok") != Some(true))),
        )
    }),
    Edit::new("an answer to the other controller", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "answer"),
            |e| with(e, "c", other(e)),
        )
    }),
    Edit::new("a heal answered before it was sent", |events| {
        let at = events.iter().position(|e| is(e, "answer"))?;
        let c = fields(&events[at])["c"].clone();
        let send = events[..at]
            .iter()
            .rposition(|e| is(e, "send") && fields(e)["c"] == c)?;
        let mut edited = events.to_vec();
        let answer = edited.remove(at);
        edited.insert(send, answer);
        Some((send, edited))
    }),
    Edit::new("a view showed a heal done that was not", |events| {
        let at = events.iter().position(|e| is(e, "plan"))?;
        let c = fields(&events[at])["c"].clone();
        Some(insert(events, at + 1, event("done", &[("c", c)])))
    }),
    Edit::new("a sent heal ended with no answer", |events| {
        let at = events.iter().position(|e| is(e, "send"))?;
        let c = fields(&events[at])["c"].clone();
        Some(insert(events, at + 1, event("abort", &[("c", c)])))
    }),
    Edit::new("an abandon of a copy the node does not run", |events| {
        let at = events.iter().position(|e| is(e, "abandon"))?;
        Some(insert(events, at + 1, events[at].clone()))
    }),
    Edit::new("a node ended the copy of the term in force", |events| {
        let at = events.iter().position(|e| is(e, "join"))?;
        let r = fields(&events[at])["r"].clone();
        Some(insert(events, at + 1, event("ends", &[("r", r)])))
    }),
    Edit::new("a node dropped the copy it runs", |events| {
        let at = events.iter().position(|e| is(e, "join"))?;
        let r = fields(&events[at])["r"].clone();
        Some(insert(events, at + 1, event("drop", &[("r", r)])))
    }),
    Edit::new("a member dropped its copy", |events| {
        Some(insert(
            events,
            0,
            event("drop", &[("r", Value::from("n11"))]),
        ))
    }),
    Edit::new("a restart of a machine that is up", |events| {
        Some(insert(
            events,
            0,
            event("restart", &[("r", Value::from("n11"))]),
        ))
    }),
    Edit::new("a machine that is down copied", |events| {
        let at = events.iter().position(|e| is(e, "copy"))?;
        let r = fields(&events[at])["r"].clone();
        Some(insert(events, at, event("crash", &[("r", r)])))
    }),
];
