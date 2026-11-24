//! Model-based trace checking of the election walk: its events as a trace
//! of `specs/tla/election/ElectionTrace.tla` (the actions of
//! `Election.tla`), and the named edits of recorded election traces
//! (#1308 (mutate recorded traces)).
//!
//! The encoding: actor `a` is `"a<a + 1>"`; a uuid is its number, the
//! unset leader is 0 and a multi-writer leader is the model's `None`,
//! [`MAX_UUIDS`] + 1; a lost message is its kind in the model (`"sl"`,
//! `"w"`, `"slans"`, `"wans"`).

use moonpool_tla::Value;

use super::mutate::{Edit, change_first, fields, is, num, text, with};
use super::{Trace, TraceCheck, int, specs_dir};
use crate::election::ElectionEvent;

/// The uuids of the model, `MaxUuids` in `ElectionTrace.cfg`.
pub const MAX_UUIDS: u128 = 1000;

/// The check of `ElectionTrace` in `specs/tla/election/`.
#[must_use]
pub fn election_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("election"), "ElectionTrace")
}

fn actor(a: usize) -> Value {
    Value::from(format!("a{}", a + 1))
}

/// A leader value of the walk as the model's number.
///
/// # Panics
///
/// When a uuid is beyond the model's uuids.
fn uuid(u: u128) -> Value {
    if u == u128::MAX {
        return int(i64::try_from(MAX_UUIDS + 1).expect("a small bound"));
    }
    assert!(
        u <= MAX_UUIDS,
        "uuid {u} is beyond MaxUuids of ElectionTrace.cfg"
    );
    int(i64::try_from(u).expect("a small uuid"))
}

/// The events of one traced election walk as a trace of
/// `ElectionTrace.tla`.
#[must_use]
pub fn trace<'a>(events: impl IntoIterator<Item = &'a ElectionEvent>) -> Trace {
    let mut trace = Trace::new();
    for event in events {
        let mut f: Vec<(&'static str, Value)> = Vec::new();
        let kind = match event {
            ElectionEvent::Challenge { a, u, old } => {
                f.push(("a", actor(*a)));
                f.push(("u", uuid(*u)));
                f.push(("old", uuid(*old)));
                "challenge"
            }
            ElectionEvent::Retry { a } => {
                f.push(("a", actor(*a)));
                "retry"
            }
            ElectionEvent::CampaignAnswer { a, won, t } => {
                f.push(("a", actor(*a)));
                f.push(("won", Value::Bool(*won)));
                f.push(("t", int(*t)));
                "campaign_answer"
            }
            ElectionEvent::Renew { a } => {
                f.push(("a", actor(*a)));
                "renew"
            }
            ElectionEvent::Timeout { a } => {
                f.push(("a", actor(*a)));
                "timeout"
            }
            ElectionEvent::WriteAnswer { a, ok } => {
                f.push(("a", actor(*a)));
                f.push(("ok", Value::Bool(*ok)));
                "write_answer"
            }
            ElectionEvent::HandOn { a, b, u } => {
                f.push(("a", actor(*a)));
                f.push(("b", actor(*b)));
                f.push(("u", uuid(*u)));
                "hand_on"
            }
            ElectionEvent::HandAnswer { a } => {
                f.push(("a", actor(*a)));
                "hand_answer"
            }
            ElectionEvent::Adopt { b, took, t } => {
                f.push(("b", actor(*b)));
                f.push(("took", Value::Bool(*took)));
                f.push(("t", int(*t)));
                "adopt"
            }
            ElectionEvent::ApplySetLeader { a, inc, u, ok } => {
                f.push(("a", actor(*a)));
                f.push(("inc", int(*inc)));
                f.push(("u", uuid(*u)));
                f.push(("ok", Value::Bool(*ok)));
                "apply_sl"
            }
            ElectionEvent::ApplyWrite { a, inc, u, ok } => {
                f.push(("a", actor(*a)));
                f.push(("inc", int(*inc)));
                f.push(("u", uuid(*u)));
                f.push(("ok", Value::Bool(*ok)));
                "apply_w"
            }
            ElectionEvent::Lose { kind, a, inc, u } => {
                f.push(("kind", Value::from(*kind)));
                f.push(("a", actor(*a)));
                f.push(("inc", int(*inc)));
                f.push(("u", uuid(*u)));
                "lose"
            }
            ElectionEvent::Crash { a } => {
                f.push(("a", actor(*a)));
                "crash"
            }
            ElectionEvent::UserMulti => "user_multi",
        };
        trace.event(kind, f);
    }
    trace
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

/// The field `name` plus `by`.
fn bump(event: &Value, name: &str, by: i64) -> Value {
    with(event, name, Value::Int(num(event, name).unwrap_or(0) + by))
}

/// The boolean field `name`.
fn flag(event: &Value, name: &str) -> Option<bool> {
    match fields(event).get(name) {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// Another actor than the one in field `name`.
fn other(event: &Value, name: &str) -> Value {
    Value::from(if text(event, name) == "a1" {
        "a2"
    } else {
        "a1"
    })
}

/// The named edits of recorded election traces.
pub const EDITS: &[Edit] = &[
    Edit::new("a claim lost that won", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "campaign_answer") && flag(&e[i], "won") == Some(true),
            |e| with(e, "won", Value::Bool(false)),
        )
    }),
    Edit::new("a claim won that lost", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "campaign_answer") && flag(&e[i], "won") == Some(false),
            |e| with(e, "won", Value::Bool(true)),
        )
    }),
    Edit::new("a term number one too high", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "campaign_answer") && flag(&e[i], "won") == Some(true),
            |e| bump(e, "t", 1),
        )
    }),
    Edit::new("a term number taken again", |events| {
        change_first(
            events,
            |e, i| {
                is(&e[i], "campaign_answer")
                    && flag(&e[i], "won") == Some(true)
                    && num(&e[i], "t").is_some_and(|t| t >= 2)
            },
            |e| bump(e, "t", -1),
        )
    }),
    Edit::new("a claim names a uuid drawn before", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "challenge") && num(&e[i], "u").is_some_and(|u| u >= 2),
            |e| bump(e, "u", -1),
        )
    }),
    Edit::new("a claim replaces a leader never in force", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "challenge"),
            |e| with(e, "old", int(900)),
        )
    }),
    Edit::new("a claim sent while one is in flight", |events| {
        let at = events.iter().position(|e| is(e, "challenge"))?;
        let mut again = bump(&events[at], "u", 1);
        again = with(&again, "old", fields(&events[at])["old"].clone());
        Some(insert(events, at + 1, again))
    }),
    Edit::new("a resend while the call is in flight", |events| {
        let at = events.iter().position(|e| is(e, "challenge"))?;
        let a = fields(&events[at])["a"].clone();
        Some(insert(events, at + 1, event("retry", &[("a", a)])))
    }),
    Edit::new("a write of an actor that never won", |events| {
        let at = events.iter().position(|e| is(e, "challenge"))?;
        let a = fields(&events[at])["a"].clone();
        Some(insert(events, at, event("renew", &[("a", a)])))
    }),
    Edit::new("a write applied that was never sent", |events| {
        remove(events, |e| is(e, "renew"))
    }),
    Edit::new(
        "an answer to a write before the journal took it",
        |events| {
            let at = events.iter().position(|e| is(e, "renew"))?;
            let a = fields(&events[at])["a"].clone();
            Some(insert(
                events,
                at + 1,
                event("write_answer", &[("a", a), ("ok", Value::Bool(true))]),
            ))
        },
    ),
    Edit::new("a write taken under a uuid not in force", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply_w") && flag(&e[i], "ok") == Some(false),
            |e| with(e, "ok", Value::Bool(true)),
        )
    }),
    Edit::new("a write of the holder refused", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply_w") && flag(&e[i], "ok") == Some(true),
            |e| with(e, "ok", Value::Bool(false)),
        )
    }),
    Edit::new("a write applied for another incarnation", |events| {
        change_first(events, |e, i| is(&e[i], "apply_w"), |e| bump(e, "inc", 1))
    }),
    Edit::new("a compare-and-set taken with a stale old", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply_sl") && flag(&e[i], "ok") == Some(false),
            |e| with(e, "ok", Value::Bool(true)),
        )
    }),
    Edit::new("a compare-and-set refused that matched", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "apply_sl") && flag(&e[i], "ok") == Some(true),
            |e| with(e, "ok", Value::Bool(false)),
        )
    }),
    Edit::new("a handed term with a number one too high", |events| {
        change_first(events, |e, i| is(&e[i], "adopt"), |e| bump(e, "t", 1))
    }),
    Edit::new("a handed term taken by a busy actor", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "adopt") && flag(&e[i], "took") == Some(false),
            |e| with(e, "took", Value::Bool(true)),
        )
    }),
    Edit::new("a handed term refused by an idle actor", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "adopt") && flag(&e[i], "took") == Some(true),
            |e| with(e, "took", Value::Bool(false)),
        )
    }),
    Edit::new("a handed term adopted by another actor", |events| {
        change_first(
            events,
            |e, i| is(&e[i], "adopt"),
            |e| with(e, "b", other(e, "b")),
        )
    }),
    Edit::new("an answer that reaches a stopped actor", |events| {
        let at = events.iter().position(|e| is(e, "write_answer"))?;
        let a = fields(&events[at])["a"].clone();
        Some(insert(events, at, event("crash", &[("a", a)])))
    }),
    Edit::new("a loss of an answer never sent", |events| {
        let at = events.iter().position(|e| is(e, "challenge"))?;
        let a = fields(&events[at])["a"].clone();
        Some(insert(
            events,
            at,
            event(
                "lose",
                &[
                    ("kind", Value::from("slans")),
                    ("a", a),
                    ("inc", int(1)),
                    ("u", int(0)),
                ],
            ),
        ))
    }),
];

/// An event named `kind` with `fields`, as a recorded trace holds it.
fn event(kind: &str, fields: &[(&str, Value)]) -> Value {
    Value::record(
        [("event", Value::from(kind))]
            .into_iter()
            .chain(fields.iter().cloned())
            .map(|(k, v)| (k.to_owned(), v)),
    )
}
