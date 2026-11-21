//! Edited recorded traces of the fold walk: a test of the trace check
//! itself, as `consensus-sim`'s `trace/mutate.rs` (#1308 (mutate recorded
//! traces)).
//!
//! A control shows that `FoldTrace` can fail; the edits show that its
//! binding refuses what it must. Each named [`Edit`] finds its own place in
//! a recorded trace (the first event where the edit makes a trace that no
//! correct fold records) and TLC must refuse the edited trace at the edited
//! event or after it. A place is a fact of the trace, never a seed or an
//! index.

use std::collections::BTreeMap;

use moonpool_tla::Value;

use super::Trace;

/// One edited trace.
#[derive(Debug, Clone)]
pub struct Mutant {
    /// The name of the edit.
    pub edit: &'static str,
    /// The index of the first event the edit changed. TLC must refuse an
    /// event at this index or after it.
    pub at: usize,
    /// The edited trace, with the definitions of the recorded one.
    pub trace: Trace,
}

/// Where an edit goes in the events of a trace, and the edited events.
pub type Find = fn(&[Value]) -> Option<(usize, Vec<Value>)>;

/// A named edit of a recorded trace.
pub struct Edit {
    /// The name of the edit.
    pub name: &'static str,
    find: Find,
}

impl std::fmt::Debug for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

impl Edit {
    /// The edit `name`, placed by `find`.
    #[must_use]
    pub const fn new(name: &'static str, find: Find) -> Self {
        Self { name, find }
    }

    /// The edit at its first place in `trace`, or `None`.
    ///
    /// # Panics
    ///
    /// When the edit changes nothing, or changes an event before its place.
    #[must_use]
    pub fn apply(&self, trace: &Trace) -> Option<Mutant> {
        let (at, events) = (self.find)(trace.events())?;
        assert!(
            at < events.len(),
            "{}: the edited event is in the trace",
            self.name
        );
        assert_ne!(
            events.as_slice(),
            trace.events(),
            "{}: an edit changes the trace",
            self.name
        );
        assert_eq!(
            events[..at],
            trace.events()[..at],
            "{}: the events before the edited one are the recorded ones",
            self.name
        );
        let mut edited = trace.prefix(0);
        for event in events {
            let Value::Record(mut fields) = event else {
                panic!("{}: an event is a record", self.name);
            };
            let Some(Value::Str(name)) = fields.remove("event") else {
                panic!("{}: an event has a string name", self.name);
            };
            edited.event(&name, fields);
        }
        Some(Mutant {
            edit: self.name,
            at,
            trace: edited,
        })
    }
}

/// Every edit.
#[must_use]
pub fn edits() -> &'static [Edit] {
    EDITS
}

pub(crate) fn fields(event: &Value) -> &BTreeMap<String, Value> {
    let Value::Record(fields) = event else {
        panic!("an event is a record");
    };
    fields
}

pub(crate) fn text<'a>(event: &'a Value, name: &str) -> &'a str {
    match fields(event).get(name) {
        Some(Value::Str(s)) => s,
        _ => "",
    }
}

pub(crate) fn num(event: &Value, name: &str) -> Option<i64> {
    match fields(event).get(name) {
        Some(Value::Int(n)) => Some(*n),
        _ => None,
    }
}

pub(crate) fn with(event: &Value, name: &str, value: Value) -> Value {
    let mut changed = fields(event).clone();
    changed.insert(name.to_owned(), value);
    Value::Record(changed)
}

pub(crate) fn is(event: &Value, kind: &str) -> bool {
    text(event, "event") == kind
}

fn applied(event: &Value, res: &str) -> bool {
    is(event, "apply") && text(event, "res") == res
}

/// Replace the first event that `pick` selects with `change` of it.
pub(crate) fn change_first(
    events: &[Value],
    pick: impl Fn(&[Value], usize) -> bool,
    change: impl Fn(&Value) -> Value,
) -> Option<(usize, Vec<Value>)> {
    let at = (0..events.len()).find(|&i| pick(events, i))?;
    let mut edited = events.to_vec();
    edited[at] = change(&events[at]);
    Some((at, edited))
}

/// The answer of the first apply answered `from` becomes `to`.
macro_rules! answer_edit {
    ($name:expr, $from:expr, $to:expr) => {
        Edit {
            name: $name,
            find: |events| {
                change_first(
                    events,
                    |e, i| applied(&e[i], $from),
                    |e| with(e, "res", Value::from($to)),
                )
            },
        }
    };
}

const EDITS: &[Edit] = &[
    answer_edit!("an accepted command refused", "ok", "fenced"),
    answer_edit!("a duplicate called taken", "dup", "taken"),
    answer_edit!("a taken write called a duplicate", "taken", "dup"),
    answer_edit!("a Below called a duplicate", "below", "dup"),
    answer_edit!("a fenced write accepted", "fenced", "ok"),
    answer_edit!("an early write accepted", "early", "ok"),
    answer_edit!("a truncated write called Below", "trunc", "below"),
    answer_edit!("a write of the wrong mode accepted", "wrong", "ok"),
    answer_edit!("a refused SetLeader accepted", "refused", "ok"),
    answer_edit!("a Noop accepted", "noop", "ok"),
    Edit {
        name: "next_seq moved",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "apply"),
                |e| with(e, "next", Value::Int(num(e, "next").unwrap_or(0) + 1)),
            )
        },
    },
    Edit {
        name: "first_seq moved",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "apply") && num(&e[i], "first") < num(&e[i], "next"),
                |e| with(e, "first", Value::Int(num(e, "first").unwrap_or(0) + 1)),
            )
        },
    },
    Edit {
        name: "the leader changed",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "apply") && text(&e[i], "leader") != "none",
                |e| with(e, "leader", Value::from("none")),
            )
        },
    },
    Edit {
        name: "an accepted write dropped",
        find: |events| {
            let at = (0..events.len()).find(|&i| {
                applied(&events[i], "ok")
                    && text(&events[i], "op") == "w"
                    && i + 1 < events.len()
                    && !is(&events[i + 1], "resolve")
            })?;
            let mut edited = events.to_vec();
            edited.remove(at);
            Some((at, edited))
        },
    },
    Edit {
        name: "an accepted fenced write repeated",
        find: |events| {
            let at = (0..events.len()).find(|&i| {
                applied(&events[i], "ok")
                    && text(&events[i], "op") == "w"
                    && text(&events[i], "u") != "none"
            })?;
            let mut edited = events.to_vec();
            edited.insert(at + 1, events[at].clone());
            Some((at + 1, edited))
        },
    },
    Edit {
        name: "the seals before a Below dropped",
        find: |events| {
            // Every seal before it: with one left, the position can still
            // be below the start of an older segment.
            let below = events.iter().position(|e| applied(e, "below"))?;
            let at = (0..below).find(|&i| is(&events[i], "seal"))?;
            let edited = events
                .iter()
                .enumerate()
                .filter(|(i, e)| *i >= below || !is(e, "seal"))
                .map(|(_, e)| e.clone())
                .collect();
            Some((at, edited))
        },
    },
    Edit {
        name: "a seal before a duplicate",
        find: |events| {
            let at = events.iter().position(|e| applied(e, "dup"))?;
            let mut seal = BTreeMap::new();
            seal.insert("event".to_owned(), Value::from("seal"));
            for name in ["next", "first", "leader"] {
                seal.insert(name.to_owned(), fields(&events[at])[name].clone());
            }
            let mut edited = events.to_vec();
            edited.insert(at, Value::Record(seal));
            Some((at, edited))
        },
    },
    Edit {
        name: "a written Resolve called not written",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "resolve") && text(&e[i], "res") == "dup",
                |e| with(e, "res", Value::from("taken")),
            )
        },
    },
    Edit {
        name: "a not written Resolve called written",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "resolve") && text(&e[i], "res") == "taken",
                |e| with(e, "res", Value::from("dup")),
            )
        },
    },
    Edit {
        name: "a Resolve at the segment in force",
        find: |events| {
            change_first(
                events,
                |e, i| is(&e[i], "resolve"),
                |e| with(e, "seg", Value::Int(num(e, "seg").unwrap_or(0) + 1_000)),
            )
        },
    },
    Edit {
        name: "a Resolve before its Below",
        find: |events| {
            let resolve = events.iter().position(|e| is(e, "resolve"))?;
            let below = events[..resolve].iter().position(|e| {
                applied(e, "below")
                    && ["u", "e", "id"]
                        .iter()
                        .all(|f| fields(e).get(*f) == fields(&events[resolve]).get(*f))
            })?;
            let mut edited = events.to_vec();
            let moved = edited.remove(resolve);
            edited.insert(below, moved);
            Some((below, edited))
        },
    },
    Edit {
        name: "an accepted write of another uuid",
        find: |events| {
            change_first(
                events,
                |e, i| {
                    applied(&e[i], "ok") && text(&e[i], "op") == "w" && text(&e[i], "u") != "none"
                },
                |e| with(e, "u", Value::from("u9999")),
            )
        },
    },
    Edit {
        name: "an accepted write at another position",
        find: |events| {
            change_first(
                events,
                |e, i| {
                    applied(&e[i], "ok") && text(&e[i], "op") == "w" && text(&e[i], "u") != "none"
                },
                |e| with(e, "e", Value::Int(num(e, "e").unwrap_or(0) + 1)),
            )
        },
    },
    Edit {
        name: "a won SetLeader names another old",
        find: |events| {
            change_first(
                events,
                |e, i| applied(&e[i], "ok") && text(&e[i], "op") == "sl",
                |e| with(e, "old", Value::from("u9999")),
            )
        },
    },
    Edit {
        name: "a truncate that raised the floor stops one short",
        find: |events| {
            // The floor before the truncate: the last event that names it.
            let floor_before = |i: usize| {
                events[..i]
                    .iter()
                    .rev()
                    .find_map(|e| num(e, "first"))
                    .unwrap_or(0)
            };
            change_first(
                events,
                |e, i| {
                    applied(&e[i], "ok")
                        && text(&e[i], "op") == "tr"
                        && num(&e[i], "first").is_some_and(|first| first > floor_before(i))
                },
                |e| with(e, "up", Value::Int(num(e, "first").unwrap_or(0) - 1)),
            )
        },
    },
];
