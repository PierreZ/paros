//! Mutated recorded traces: a test of the trace check itself (#1308 (mutate
//! recorded traces)).
//!
//! A trace spec has one control, a bug planted in the driver that TLC
//! refuses. That shows that the check can fail; it does not show that the
//! binding refuses all that it must refuse. A binding that accepts too much
//! passes every honest walk. So a recorded trace is edited, and TLC must
//! refuse each edited trace at or after the edited event, never before: the
//! events before it are the recorded ones, which TLC accepts.
//!
//! An [`Edit`] is named, and it finds its own place in a trace: the first
//! event where the edit makes a trace that no correct run records. A place
//! is a fact of the trace (a promise that a later win needs, a vote that a
//! later promise reports, a slot that no ballot proposed), never a seed or
//! an index picked by hand. Each edit works on the one encoding that every
//! trace spec shares (`cluster/trace/encode.rs`, `decree::trace`,
//! `batcher::trace`): an event is a record with its name in the field
//! `event`, ballots are ranks from 1, slots start at 0, and nodes, values
//! and matchmakers are strings.

use std::collections::BTreeSet;

use moonpool_tla::Value;

use super::Trace;

/// One edited trace.
#[derive(Debug, Clone)]
pub struct Mutant {
    /// The name of the edit.
    pub edit: &'static str,
    /// The index of the first event that the edit changed. TLC must refuse
    /// an event at this index or after it.
    pub at: usize,
    /// The edited trace, with the definitions of the recorded one.
    pub trace: Trace,
}

/// The first place of an edit in the events, and the edited events: the
/// index of the first event it changed, and every event after the edit.
type Find = fn(&[Value]) -> Option<(usize, Vec<Value>)>;

/// A named edit of a recorded trace.
pub struct Edit {
    /// The name of the edit, as a test reports it.
    pub name: &'static str,
    /// The first place of the edit in the events, and the edited events.
    find: Find,
}

impl std::fmt::Debug for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

impl Edit {
    /// The edit at its first place in `trace`, or `None` when `trace` has
    /// no place for it.
    ///
    /// # Panics
    ///
    /// When an event of `trace` is not a record with a string `event`.
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

/// Every edit, in the order a test runs them.
#[must_use]
pub fn edits() -> &'static [Edit] {
    EDITS
}

// ---------------------------------------------------------------------------
// Reading and writing events.

/// The name of `event`.
fn kind(event: &Value) -> &str {
    match field(event, "event") {
        Some(Value::Str(name)) => name,
        _ => "",
    }
}

fn is(event: &Value, name: &str) -> bool {
    kind(event) == name
}

fn field<'a>(event: &'a Value, name: &str) -> Option<&'a Value> {
    match event {
        Value::Record(fields) => fields.get(name),
        _ => None,
    }
}

fn num(event: &Value, name: &str) -> Option<i64> {
    match field(event, name) {
        Some(Value::Int(n)) => Some(*n),
        _ => None,
    }
}

fn text<'a>(event: &'a Value, name: &str) -> Option<&'a str> {
    match field(event, name) {
        Some(Value::Str(s)) => Some(s),
        _ => None,
    }
}

fn elements<'a>(event: &'a Value, name: &str) -> &'a [Value] {
    match field(event, name) {
        Some(Value::Set(items) | Value::Seq(items)) => items,
        _ => &[],
    }
}

/// `event` with its field `name` set to `value`.
fn with(event: &Value, name: &str, value: Value) -> Value {
    let mut event = event.clone();
    if let Value::Record(fields) = &mut event {
        fields.insert(name.to_owned(), value);
    }
    event
}

/// The ballot of `event`.
fn bal(event: &Value) -> Option<i64> {
    num(event, "bal")
}

/// The slot of `event`. A single-decree event has no slot: every event of
/// such a trace is at the one slot.
fn slot(event: &Value) -> Option<i64> {
    num(event, "slot")
}

/// Whether `a` and `b` name the same ballot and slot.
fn same_round(a: &Value, b: &Value) -> bool {
    bal(a).is_some() && bal(a) == bal(b) && slot(a) == slot(b)
}

/// `events` with event `k` removed.
fn drop(events: &[Value], k: usize) -> (usize, Vec<Value>) {
    let mut out = events.to_vec();
    out.remove(k);
    (k, out)
}

/// `events` with event `k` recorded twice in a row.
fn twice(events: &[Value], k: usize) -> (usize, Vec<Value>) {
    let mut out = events.to_vec();
    out.insert(k + 1, events[k].clone());
    (k + 1, out)
}

/// `events` with event `j` moved to index `i`, before event `i`.
fn hoist(events: &[Value], j: usize, i: usize) -> (usize, Vec<Value>) {
    assert!(i < j, "an event moves to an earlier place");
    let mut out = events.to_vec();
    let moved = out.remove(j);
    out.insert(i, moved);
    (i, out)
}

/// `events` with event `k` replaced by `event`.
fn replace(events: &[Value], k: usize, event: Value) -> (usize, Vec<Value>) {
    let mut out = events.to_vec();
    out[k] = event;
    (k, out)
}

/// The first event at or after `from` of `name` that `pred` takes.
fn find_from(
    events: &[Value],
    from: usize,
    name: &str,
    pred: impl Fn(&Value) -> bool,
) -> Option<usize> {
    (from..events.len()).find(|&k| is(&events[k], name) && pred(&events[k]))
}

/// The first event of `name` that `pred` takes.
fn first(events: &[Value], name: &str, pred: impl Fn(&Value) -> bool) -> Option<usize> {
    find_from(events, 0, name, pred)
}

/// The first event before `upto` of `name` that `pred` takes.
fn first_before(
    events: &[Value],
    upto: usize,
    name: &str,
    pred: impl Fn(&Value) -> bool,
) -> Option<usize> {
    (0..upto).find(|&k| is(&events[k], name) && pred(&events[k]))
}

/// For each event of `name` in order, the edit that `place` finds there.
fn each(
    events: &[Value],
    name: &str,
    place: impl Fn(usize) -> Option<(usize, Vec<Value>)>,
) -> Option<(usize, Vec<Value>)> {
    (0..events.len())
        .filter(|&k| is(&events[k], name))
        .find_map(place)
}

/// The commands of the trace: every `val` field and every reported vote.
fn values(events: &[Value]) -> BTreeSet<Value> {
    let mut out = BTreeSet::new();
    for event in events {
        for name in ["val", "vval"] {
            if let Some(v @ Value::Str(s)) = field(event, name)
                && s != "none"
            {
                out.insert(v.clone());
            }
        }
        for report in elements(event, "rep").iter().chain(elements(event, "pen")) {
            if let Some(v) = field(report, "val") {
                out.insert(v.clone());
            }
        }
    }
    out
}

/// A command of the trace that is not `value`.
fn other_value(events: &[Value], value: &Value) -> Option<Value> {
    values(events).into_iter().find(|v| v != value)
}

/// The acceptors that the trace names.
fn nodes(events: &[Value]) -> BTreeSet<String> {
    events
        .iter()
        .flat_map(|e| ["acc", "node", "owner", "by"].map(|f| text(e, f)))
        .flatten()
        .map(str::to_owned)
        .collect()
}

/// The ballots opened at or before `k`.
fn opened(events: &[Value], k: usize) -> BTreeSet<i64> {
    events[..=k]
        .iter()
        .filter(|e| is(e, "prepare"))
        .filter_map(bal)
        .collect()
}

/// A ballot whose first `prepare` comes after `k`: no event at or before
/// `k` can be of it.
fn unopened(events: &[Value], k: usize) -> Option<i64> {
    let before = opened(events, k);
    events[k + 1..]
        .iter()
        .filter(|e| is(e, "prepare"))
        .filter_map(bal)
        .find(|b| !before.contains(b))
}

/// The frontier of ballot `b`'s win before `k`.
fn frontier(events: &[Value], k: usize, b: i64) -> Option<i64> {
    events[..k]
        .iter()
        .find(|e| is(e, "win") && bal(e) == Some(b))
        .and_then(|e| num(e, "front"))
}

/// Whether the `propose` at `k` is a fresh command: a single-decree
/// proposal, or a slot at or above its ballot's frontier (the recovery
/// slots below it were sent by the win).
fn fresh(events: &[Value], k: usize) -> bool {
    let e = &events[k];
    match (slot(e), bal(e)) {
        (None, Some(_)) => true,
        (Some(s), Some(b)) => frontier(events, k, b).is_some_and(|f| s >= f),
        _ => false,
    }
}

/// The ballot that chose `value` at slot `s` or above, as far as the
/// events before `k` tell: the lowest ballot of a vote for it.
fn lowest_vote(events: &[Value], k: usize, s: Option<i64>) -> Option<i64> {
    events[..k]
        .iter()
        .filter(|e| is(e, "accept") && slot(e) == s)
        .filter_map(bal)
        .min()
}

// ---------------------------------------------------------------------------
// The edits.

/// Every edit. Each says in one line what a correct run never records.
const EDITS: &[Edit] = &[
    // --- Phase 1 ---
    Edit {
        // A promise answers a `Prepare` that the trace no longer holds.
        name: "drop a prepare that a promise answers",
        find: |ev| {
            each(ev, "prepare", |k| {
                find_from(ev, k + 1, "promise", |p| bal(p) == bal(&ev[k]))
                    .is_some_and(|_| {
                        first_before(ev, k, "prepare", |p| bal(p) == bal(&ev[k])).is_none()
                    })
                    .then(|| drop(ev, k))
            })
        },
    },
    Edit {
        // A ballot opens its campaign once.
        name: "repeat a prepare",
        find: |ev| first(ev, "prepare", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A ballot belongs to one owner.
        name: "give a prepare another owner",
        find: |ev| {
            let nodes = nodes(ev);
            each(ev, "prepare", |k| {
                let owner = text(&ev[k], "owner")?;
                let other = nodes.iter().find(|n| *n != owner)?;
                Some(replace(
                    ev,
                    k,
                    with(&ev[k], "owner", Value::Str(other.clone())),
                ))
            })
        },
    },
    Edit {
        // A promise answers a `Prepare` sent before it.
        name: "move a promise before its prepare",
        find: |ev| {
            each(ev, "prepare", |i| {
                let j = find_from(ev, i + 1, "promise", |p| bal(p) == bal(&ev[i]))?;
                first_before(ev, i, "prepare", |p| bal(p) == bal(&ev[i]))
                    .is_none()
                    .then(|| hoist(ev, j, i))
            })
        },
    },
    Edit {
        // An acceptor promises a ballot once: a second promise raises
        // nothing. A page of a paged promise (it carries `upto`) is not
        // such a promise: an acceptor re-affirms its promise to the repair
        // probe and to a dropped answer asked again, so a repeated page is a
        // valid walk.
        name: "repeat a promise",
        find: |ev| first(ev, "promise", |p| field(p, "upto").is_none()).map(|k| twice(ev, k)),
    },
    Edit {
        // A promise answers a ballot that no `Prepare` opened yet.
        name: "promise a ballot not opened yet",
        find: |ev| {
            each(ev, "promise", |k| {
                let b = unopened(ev, k)?;
                Some(replace(ev, k, with(&ev[k], "bal", Value::Int(b))))
            })
        },
    },
    Edit {
        // A promise reports every vote of its acceptor at or above `from`.
        name: "hide a vote from a promise",
        find: |ev| {
            each(ev, "promise", |k| {
                let e = &ev[k];
                if let Some(vbal) = num(e, "vbal") {
                    return (vbal > 0).then(|| {
                        let hidden = with(e, "vbal", Value::Int(0));
                        replace(ev, k, with(&hidden, "vval", Value::from("none")))
                    });
                }
                let rep = elements(e, "rep");
                let kept: Vec<Value> = rep.iter().skip(1).cloned().collect();
                (!rep.is_empty()).then(|| replace(ev, k, with(e, "rep", Value::Set(kept))))
            })
        },
    },
    Edit {
        // A promise reports no vote its acceptor never cast: here a vote at
        // a slot that no ballot proposed before it.
        name: "forge a vote in a promise",
        find: |ev| {
            let some = values(ev).into_iter().next()?;
            each(ev, "promise", |k| {
                let e = &ev[k];
                if let Some(vbal) = num(e, "vbal") {
                    let b = bal(e)?;
                    return (vbal == 0 && b > 1).then(|| {
                        let forged = with(e, "vbal", Value::Int(b - 1));
                        replace(ev, k, with(&forged, "vval", some.clone()))
                    });
                }
                field(e, "rep")?;
                let from = num(e, "from")?;
                let top = ev[..k]
                    .iter()
                    .filter(|p| is(p, "propose") || is(p, "accept"))
                    .filter_map(slot)
                    .max()
                    .unwrap_or(-1);
                let s = top.max(from - 1) + 1;
                let mut rep = elements(e, "rep").to_vec();
                rep.push(Value::record([
                    ("slot", Value::Int(s)),
                    ("bal", Value::Int(1)),
                    ("val", some.clone()),
                ]));
                (bal(e)? > 1).then(|| replace(ev, k, with(e, "rep", Value::Set(rep))))
            })
        },
    },
    Edit {
        // A promise reports the value of its acceptor's vote.
        name: "change the value a promise reports",
        find: |ev| {
            each(ev, "promise", |k| {
                let e = &ev[k];
                if num(e, "vbal").is_some_and(|b| b > 0) {
                    let v = other_value(ev, field(e, "vval")?)?;
                    return Some(replace(ev, k, with(e, "vval", v)));
                }
                let rep = elements(e, "rep");
                let first = rep.first()?;
                let v = other_value(ev, field(first, "val")?)?;
                let mut changed = rep.to_vec();
                changed[0] = with(first, "val", v);
                Some(replace(ev, k, with(e, "rep", Value::Set(changed))))
            })
        },
    },
    Edit {
        // A promise reports the ballot of its acceptor's vote.
        name: "raise the ballot a promise reports",
        find: |ev| {
            each(ev, "promise", |k| {
                let e = &ev[k];
                if let Some(vbal) = num(e, "vbal") {
                    return (vbal > 0 && vbal + 1 < bal(e)?)
                        .then(|| replace(ev, k, with(e, "vbal", Value::Int(vbal + 1))));
                }
                let rep = elements(e, "rep");
                let first = rep.first()?;
                let b = bal(first)?;
                let mut changed = rep.to_vec();
                changed[0] = with(first, "bal", Value::Int(b + 1));
                (b + 1 < bal(e)?).then(|| replace(ev, k, with(e, "rep", Value::Set(changed))))
            })
        },
    },
    Edit {
        // A single-decree proposal needs the promises of a majority: here
        // the proposal had a bare majority, and one of its promises is gone.
        name: "drop a promise that a proposal needs",
        find: |ev| {
            if ev.iter().any(|e| is(e, "win")) {
                return None;
            }
            let majority = nodes(ev).len() / 2 + 1;
            each(ev, "propose", |j| {
                let b = bal(&ev[j]);
                let promises: Vec<usize> = (0..j)
                    .filter(|&i| is(&ev[i], "promise") && bal(&ev[i]) == b)
                    .collect();
                (promises.len() == majority).then(|| drop(ev, promises[0]))
            })
        },
    },
    Edit {
        // A promise answers the `from` of the `Prepare` it answers. A page
        // of a paged promise (it carries `upto`) from above that slot is the
        // valid answer to a keep-alive, so the first page moves below it.
        name: "move the from of a promise",
        find: |ev| {
            each(ev, "promise", |k| {
                let from = num(&ev[k], "from")?;
                if field(&ev[k], "upto").is_none() {
                    return Some(replace(ev, k, with(&ev[k], "from", Value::Int(from + 1))));
                }
                let prepare = first_before(ev, k, "prepare", |p| bal(p) == bal(&ev[k]))?;
                (num(&ev[prepare], "from") == Some(from))
                    .then(|| replace(ev, k, with(&ev[k], "from", Value::Int(from - 1))))
            })
        },
    },
    // --- The win ---
    Edit {
        // A ballot proposes only once it won Phase 1.
        name: "drop a win that a proposal needs",
        find: |ev| {
            each(ev, "win", |k| {
                find_from(ev, k + 1, "propose", |p| bal(p) == bal(&ev[k])).map(|_| drop(ev, k))
            })
        },
    },
    Edit {
        // A ballot wins once.
        name: "repeat a win",
        find: |ev| first(ev, "win", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // The frontier of a win is the one its promises give.
        name: "move the frontier of a win",
        find: |ev| {
            each(ev, "win", |k| {
                let front = num(&ev[k], "front")?;
                Some(replace(ev, k, with(&ev[k], "front", Value::Int(front + 1))))
            })
        },
    },
    // --- Phase 2 ---
    Edit {
        // A proposal of a ballot comes after its campaign: its win, or in
        // single-decree Paxos its `Prepare`.
        name: "move a proposal before its campaign",
        find: |ev| {
            each(ev, "propose", |j| {
                let b = bal(&ev[j]);
                let i = first_before(ev, j, "win", |w| bal(w) == b)
                    .or_else(|| first_before(ev, j, "prepare", |p| bal(p) == b))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A vote answers a proposal that the trace no longer holds.
        name: "drop a fresh proposal that a vote answers",
        find: |ev| {
            each(ev, "propose", |k| {
                (fresh(ev, k)
                    && find_from(ev, k + 1, "accept", |a| same_round(a, &ev[k])).is_some())
                .then(|| drop(ev, k))
            })
        },
    },
    Edit {
        // A ballot proposes a fresh command once at a slot.
        name: "repeat a fresh proposal",
        find: |ev| each(ev, "propose", |k| fresh(ev, k).then(|| twice(ev, k))),
    },
    Edit {
        // The walk records the first `Accept` of a (ballot, slot) once: a
        // slot of the recovery too.
        name: "repeat a recovery proposal",
        find: |ev| {
            each(ev, "propose", |k| {
                (slot(&ev[k]).is_some() && !fresh(ev, k)).then(|| twice(ev, k))
            })
        },
    },
    Edit {
        // A leader allocates its fresh slots in order, with no gap.
        name: "skip a slot in a fresh proposal",
        find: |ev| {
            each(ev, "propose", |k| {
                let s = slot(&ev[k])?;
                fresh(ev, k).then(|| replace(ev, k, with(&ev[k], "slot", Value::Int(s + 1))))
            })
        },
    },
    Edit {
        // A vote answers a proposal sent before it.
        name: "move a vote before its fresh proposal",
        find: |ev| {
            each(ev, "propose", |i| {
                if !fresh(ev, i) {
                    return None;
                }
                let j = find_from(ev, i + 1, "accept", |a| same_round(a, &ev[i]))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // An acceptor casts a vote once.
        name: "repeat a vote",
        find: |ev| first(ev, "accept", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A vote holds the value that its ballot proposed.
        name: "change the value of a vote",
        find: |ev| {
            each(ev, "accept", |k| {
                let v = other_value(ev, field(&ev[k], "val")?)?;
                Some(replace(ev, k, with(&ev[k], "val", v)))
            })
        },
    },
    Edit {
        // A vote is at a ballot that a `Prepare` opened before it.
        name: "vote at a ballot not opened yet",
        find: |ev| {
            each(ev, "accept", |k| {
                let b = unopened(ev, k)?;
                Some(replace(ev, k, with(&ev[k], "bal", Value::Int(b))))
            })
        },
    },
    Edit {
        // A vote is at a slot its ballot proposed: here a slot above every
        // slot the ballot proposed and above its frontier.
        name: "vote at a slot its ballot never proposed",
        find: |ev| {
            each(ev, "accept", |k| {
                let e = &ev[k];
                let (s, b) = (slot(e)?, bal(e)?);
                let top = ev
                    .iter()
                    .filter(|p| is(p, "propose") && bal(p) == Some(b))
                    .filter_map(slot)
                    .chain(frontier(ev, k, b))
                    .chain(std::iter::once(s))
                    .max()?;
                Some(replace(ev, k, with(e, "slot", Value::Int(top + 1))))
            })
        },
    },
    Edit {
        // An acceptor that promised a higher ballot refuses to vote.
        name: "vote by an acceptor that promised higher",
        find: |ev| {
            each(ev, "accept", |k| {
                let e = &ev[k];
                let b = bal(e)?;
                let me = text(e, "acc")?;
                let other = ev[..k].iter().rev().find_map(|p| {
                    let who = if is(p, "promise") {
                        text(p, "acc")
                    } else if is(p, "prepare") {
                        text(p, "owner")
                    } else {
                        None
                    }?;
                    (who != me && bal(p)? > b).then_some(who)
                })?;
                // The other acceptor did not boot since: a boot keeps only the
                // durable promise, and the walk records a promise when it is
                // durable, but the edit keeps to the plain case.
                let since = ev[..k].iter().rposition(|p| {
                    (is(p, "promise") && text(p, "acc") == Some(other))
                        || (is(p, "prepare") && text(p, "owner") == Some(other))
                })?;
                ev[since..k]
                    .iter()
                    .all(|p| !(is(p, "restart") && text(p, "node") == Some(other)))
                    .then(|| replace(ev, k, with(e, "acc", Value::Str(other.to_owned()))))
            })
        },
    },
    Edit {
        // A later promise reports the vote, so the vote happened. A
        // leader's own vote is left out: a model that records the round as
        // the leader's vote when it opens it (`LeaderHandoff`) holds that
        // vote with no "accept" step.
        name: "drop a vote that a promise reports",
        find: |ev| {
            each(ev, "accept", |k| {
                let e = &ev[k];
                if field(e, "own") == Some(&Value::Bool(true)) {
                    return None;
                }
                let acc = text(e, "acc")?;
                let next = (k + 1..ev.len()).find(|&j| {
                    let p = &ev[j];
                    (is(p, "accept") || is(p, "promise") || is(p, "record"))
                        && (text(p, "acc") == Some(acc) || text(p, "node") == Some(acc))
                        && (is(p, "promise") || slot(p) == slot(e))
                })?;
                let p = &ev[next];
                if !is(p, "promise") {
                    return None;
                }
                let reported = num(p, "vbal") == bal(e) && field(p, "vval") == field(e, "val")
                    || elements(p, "rep").iter().any(|r| {
                        slot(r) == slot(e) && bal(r) == bal(e) && field(r, "val") == field(e, "val")
                    });
                reported.then(|| drop(ev, k))
            })
        },
    },
    // --- Learning ---
    Edit {
        // A slot is learned only once a quorum voted for it.
        name: "move a learn before every vote of its slot",
        find: |ev| {
            ["learn", "chosen", "record"].iter().find_map(|name| {
                each(ev, name, |j| {
                    let s = slot(&ev[j]);
                    let i = first_before(ev, j, "accept", |a| slot(a) == s)?;
                    Some(hoist(ev, j, i))
                })
            })
        },
    },
    Edit {
        // A slot is learned with the value chosen there.
        name: "change the value of a learn",
        find: |ev| {
            ["learn", "chosen"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let v = other_value(ev, field(&ev[k], "val")?)?;
                    Some(replace(ev, k, with(&ev[k], "val", v)))
                })
            })
        },
    },
    Edit {
        // A record holds a value chosen at its ballot or below: here below
        // every vote of its slot.
        name: "lower the ballot of a record",
        find: |ev| {
            each(ev, "record", |k| {
                let low = lowest_vote(ev, k, slot(&ev[k]))?;
                (low > 1).then(|| replace(ev, k, with(&ev[k], "bal", Value::Int(low - 1))))
            })
        },
    },
    Edit {
        // A node boots with a chosen prefix that it learned.
        name: "boot past the learned prefix",
        find: |ev| {
            let slots: BTreeSet<i64> = ev
                .iter()
                .filter(|e| is(e, "propose"))
                .filter_map(slot)
                .collect();
            each(ev, "restart", |k| {
                let e = &ev[k];
                let n = text(e, "node")?;
                let upto = num(e, "upto")?;
                let mut learned = BTreeSet::new();
                for p in &ev[..k] {
                    if is(p, "learn") && text(p, "node") == Some(n) {
                        learned.extend(slot(p));
                    } else if is(p, "restart") && text(p, "node") == Some(n) {
                        let cut = num(p, "upto")?;
                        learned.retain(|s| *s < cut);
                    }
                }
                let hole = slots.iter().find(|s| !learned.contains(s))?;
                (hole + 1 > upto).then(|| replace(ev, k, with(e, "upto", Value::Int(hole + 1))))
            })
        },
    },
    // --- Matchmaking and GC ---
    Edit {
        // A configuration is selected once for its ballot.
        name: "repeat a selection",
        find: |ev| first(ev, "select", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A matchmaker registers the configuration that the ballot selected.
        name: "move a registration before its selection",
        find: |ev| {
            each(ev, "select", |i| {
                let j = find_from(ev, i + 1, "register", |r| bal(r) == bal(&ev[i]))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A matchmaker registers a ballot once.
        name: "repeat a registration",
        find: |ev| first(ev, "register", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // Phase 1 starts after a matchmaker quorum registered the ballot.
        name: "move a prepare before its registrations",
        find: |ev| {
            each(ev, "prepare", |j| {
                let i = first_before(ev, j, "register", |r| bal(r) == bal(&ev[j]))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // Phase 2 addresses the configuration of the ballot.
        name: "drop a voter from a selection",
        find: |ev| {
            each(ev, "select", |k| {
                let e = &ev[k];
                let voter = find_from(ev, k + 1, "accept", |a| bal(a) == bal(e))?;
                let acc = field(&ev[voter], "acc")?;
                let cfg: Vec<Value> = elements(e, "cfg")
                    .iter()
                    .filter(|n| *n != acc)
                    .cloned()
                    .collect();
                (cfg.len() < elements(e, "cfg").len())
                    .then(|| replace(ev, k, with(e, "cfg", Value::Set(cfg))))
            })
        },
    },
    Edit {
        // A matchmaker acks a GC floor that a leader asked for.
        name: "move a gc ack before its request",
        find: |ev| {
            each(ev, "gcask", |i| {
                let j = find_from(ev, i + 1, "gcack", |a| bal(a) == bal(&ev[i]))?;
                first_before(ev, i, "gcask", |a| bal(a) == bal(&ev[i]))
                    .is_none()
                    .then(|| hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A leader asks for its GC floor once.
        name: "repeat a gc request",
        find: |ev| first(ev, "gcask", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A matchmaker acks a GC floor once.
        name: "repeat a gc ack",
        find: |ev| first(ev, "gcack", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A node retires once.
        name: "repeat a retirement",
        find: |ev| first(ev, "retire", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A node rejoins after it retired.
        name: "move a rejoin before its retirement",
        find: |ev| {
            each(ev, "retire", |i| {
                let j = find_from(ev, i + 1, "rejoin", |r| {
                    field(r, "node") == field(&ev[i], "node")
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    // --- Reads, folds and replicas ---
    Edit {
        // The fold applies the chosen slots in order.
        name: "skip a slot in an apply",
        find: |ev| {
            ["applied", "rapply"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let s = slot(&ev[k])?;
                    Some(replace(ev, k, with(&ev[k], "slot", Value::Int(s + 1))))
                })
            })
        },
    },
    Edit {
        // The fold applies a slot once.
        name: "repeat an apply",
        find: |ev| {
            ["applied", "rapply"]
                .iter()
                .find_map(|name| first(ev, name, |_| true).map(|k| twice(ev, k)))
        },
    },
    Edit {
        // The fold applies a slot once it is chosen.
        name: "move an apply before every vote of its slot",
        find: |ev| {
            ["applied", "rapply", "rlearn", "fold", "commit"]
                .iter()
                .find_map(|name| {
                    each(ev, name, |j| {
                        let s = slot(&ev[j]);
                        let i = first_before(ev, j, "accept", |a| slot(a) == s)?;
                        Some(hoist(ev, j, i))
                    })
                })
        },
    },
    Edit {
        // A replica learns the value chosen at its slot.
        name: "change the value of a replica learn",
        find: |ev| {
            ["rlearn", "commit", "delegate", "fanout", "direct"]
                .iter()
                .find_map(|name| {
                    each(ev, name, |k| {
                        let v = other_value(ev, field(&ev[k], "val")?)?;
                        Some(replace(ev, k, with(&ev[k], "val", v)))
                    })
                })
        },
    },
    Edit {
        // A read is served at the index its answers give.
        name: "move the index of a served read",
        find: |ev| {
            ["readserve", "rserve"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let index = num(&ev[k], "index")?;
                    Some(replace(ev, k, with(&ev[k], "index", Value::Int(index + 1))))
                })
            })
        },
    },
    Edit {
        // A read answer reports a watermark at or above every vote of its
        // acceptor: here one below a vote it cast before. (A watermark may
        // be higher: a compaction raises it to the floor.)
        name: "lower the watermark of a read answer below a vote",
        find: |ev| {
            each(ev, "readack", |k| {
                let e = &ev[k];
                let wm = num(e, "wm")?;
                let voted = ev[..k].iter().any(|a| {
                    is(a, "accept") && field(a, "acc") == field(e, "acc") && slot(a) == Some(wm)
                });
                voted.then(|| replace(ev, k, with(e, "wm", Value::Int(wm - 1))))
            })
        },
    },
    Edit {
        // A read basis carries the fence of its leadership.
        name: "move the fence of a read basis",
        find: |ev| {
            ["basis", "readstart"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let fence = num(&ev[k], "fence")?;
                    Some(replace(ev, k, with(&ev[k], "fence", Value::Int(fence + 1))))
                })
            })
        },
    },
    Edit {
        // A read is served after it started.
        name: "move a served read before its start",
        find: |ev| {
            [("readstart", "readserve"), ("rread", "rserve")]
                .iter()
                .find_map(|(start, serve)| {
                    each(ev, start, |i| {
                        let e = &ev[i];
                        let j =
                            find_from(ev, i + 1, serve, |s| field(s, "ctx") == field(e, "ctx"))?;
                        Some(hoist(ev, j, i))
                    })
                })
        },
    },
    Edit {
        // A replica boots from the floor its disk holds.
        name: "move the floor of a replica boot",
        find: |ev| {
            each(ev, "rboot", |k| {
                let from = num(&ev[k], "from")?;
                Some(replace(ev, k, with(&ev[k], "from", Value::Int(from + 1))))
            })
        },
    },
    Edit {
        // The records of a slot end at or above the end of the slots
        // before it (`next_seq`): here below it.
        name: "end the records of a fold below the slots before",
        find: |ev| {
            each(ev, "fold", |k| {
                let high = num(&ev[k], "high")?;
                let next = ev[..k]
                    .iter()
                    .filter(|f| is(f, "fold"))
                    .filter_map(|f| num(f, "high"))
                    .max()?;
                (high > 0 && next > 1)
                    .then(|| replace(ev, k, with(&ev[k], "high", Value::Int(next - 1))))
            })
        },
    },
    Edit {
        // `first_seq` never goes down.
        name: "lower first_seq in a fold",
        find: |ev| {
            each(ev, "fold", |k| {
                let first = num(&ev[k], "first")?;
                (first > 0).then(|| replace(ev, k, with(&ev[k], "first", Value::Int(first - 1))))
            })
        },
    },
    Edit {
        // The fold takes the slots in order.
        name: "fold a slot out of order",
        find: |ev| {
            each(ev, "fold", |k| {
                let s = slot(&ev[k])?;
                Some(replace(ev, k, with(&ev[k], "slot", Value::Int(s + 1))))
            })
        },
    },
    Edit {
        // A compaction keeps every record at or above `first_seq`: here
        // it also drops the slot at its floor, which holds records (its
        // fold ends above 0). A compaction stops at the first such slot.
        name: "compact past a slot that holds records",
        find: |ev| {
            ["compact", "rcompact"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let floor = num(&ev[k], "floor")?;
                    let holds = ev[..k].iter().any(|f| {
                        is(f, "fold") && slot(f) == Some(floor) && num(f, "high") > Some(0)
                    });
                    holds.then(|| replace(ev, k, with(&ev[k], "floor", Value::Int(floor + 1))))
                })
            })
        },
    },
    Edit {
        // A jump moves to the floor of a peer: here above every floor,
        // past a slot that holds records.
        name: "trim above every floor",
        find: |ev| {
            ["trim", "rtrim"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let top = ev[..=k]
                        .iter()
                        .filter(|f| ["compact", "trim", "rcompact", "rtrim"].contains(&kind(f)))
                        .filter_map(|f| num(f, "floor"))
                        .max()?;
                    let holds = ev[..k]
                        .iter()
                        .any(|f| is(f, "fold") && slot(f) == Some(top) && num(f, "high") > Some(0));
                    holds.then(|| replace(ev, k, with(&ev[k], "floor", Value::Int(top + 1))))
                })
            })
        },
    },
    Edit {
        // A faulty record is at the ballot its disk held.
        name: "change the ballot of a faulty record",
        find: |ev| {
            each(ev, "rot", |k| {
                let b = bal(&ev[k])?;
                Some(replace(ev, k, with(&ev[k], "bal", Value::Int(b + 1))))
            })
        },
    },
    // --- The handoff and the proxies ---
    Edit {
        // A handoff carries every pending round of the leader.
        name: "drop a pending round from a handoff",
        find: |ev| {
            each(ev, "relinquish", |k| {
                let pen = elements(&ev[k], "pen");
                (!pen.is_empty())
                    .then(|| replace(ev, k, with(&ev[k], "pen", Value::Set(pen[1..].to_vec()))))
            })
        },
    },
    Edit {
        // A handoff carries the leader's next slot.
        name: "move the next slot of a handoff",
        find: |ev| {
            each(ev, "relinquish", |k| {
                let nxt = num(&ev[k], "nxt")?;
                Some(replace(ev, k, with(&ev[k], "nxt", Value::Int(nxt + 1))))
            })
        },
    },
    Edit {
        // A successor installs a ballot handed to it.
        name: "move an install before its handoff",
        find: |ev| {
            each(ev, "relinquish", |i| {
                let e = &ev[i];
                let j = find_from(ev, i + 1, "install", |s| {
                    bal(s) == bal(e) && field(s, "node") == field(e, "to")
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A proxy commits a slot once a quorum voted for it.
        name: "move a proxy round before its delegation",
        find: |ev| {
            each(ev, "delegate", |i| {
                let e = &ev[i];
                let j = find_from(ev, i + 1, "fanout", |f| {
                    same_round(f, e) && field(f, "proxy") == field(e, "proxy")
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    // --- The matchmaker set handover ---
    Edit {
        // A matchmaker stops its generation once.
        name: "repeat a stop",
        find: |ev| first(ev, "stop", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A matchmaker stops the generation it serves.
        name: "stop a generation the matchmaker does not serve",
        find: |ev| {
            each(ev, "stop", |k| {
                let g = num(&ev[k], "gen")?;
                Some(replace(ev, k, with(&ev[k], "gen", Value::Int(g + 1))))
            })
        },
    },
    Edit {
        // A matchmaker of a later generation stops it once it activated.
        name: "move a stop before its activation",
        find: |ev| {
            each(ev, "stop", |j| {
                let e = &ev[j];
                let i = first_before(ev, j, "activate", |a| {
                    field(a, "mm") == field(e, "mm") && num(a, "gen") == num(e, "gen")
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A matchmaker boots into a set that holds it.
        name: "bootstrap a set without the matchmaker",
        find: |ev| {
            each(ev, "boot", |k| {
                let e = &ev[k];
                let mm = field(e, "mm")?;
                let set: Vec<Value> = elements(e, "set")
                    .iter()
                    .filter(|m| *m != mm)
                    .cloned()
                    .collect();
                Some(replace(ev, k, with(e, "set", Value::Set(set))))
            })
        },
    },
    Edit {
        // A decree votes for a set whose members booted it.
        name: "vote for a set that did not boot",
        find: |ev| {
            each(ev, "vote", |k| {
                let set = elements(&ev[k], "set");
                (set.len() > 1)
                    .then(|| replace(ev, k, with(&ev[k], "set", Value::Set(set[1..].to_vec()))))
            })
        },
    },
    Edit {
        // A matchmaker votes in the decree of the generation it serves.
        name: "vote in a generation the matchmaker does not serve",
        find: |ev| {
            each(ev, "vote", |k| {
                let g = num(&ev[k], "gen")?;
                Some(replace(ev, k, with(&ev[k], "gen", Value::Int(g + 1))))
            })
        },
    },
    Edit {
        // A matchmaker activates the set its decree chose.
        name: "activate another set",
        find: |ev| {
            each(ev, "activate", |k| {
                let set = elements(&ev[k], "set");
                (set.len() > 1)
                    .then(|| replace(ev, k, with(&ev[k], "set", Value::Set(set[1..].to_vec()))))
            })
        },
    },
    Edit {
        // A matchmaker registers no ballot below its floor.
        name: "register below the floor",
        find: |ev| {
            each(ev, "register", |k| {
                let e = &ev[k];
                let floor = ev[..k]
                    .iter()
                    .filter(|a| is(a, "gcack") && field(a, "mm") == field(e, "mm"))
                    .filter_map(bal)
                    .max()?;
                (floor > 1).then(|| replace(ev, k, with(e, "bal", Value::Int(floor - 1))))
            })
        },
    },
    Edit {
        // A successor boots from a generation that stopped.
        name: "move a bootstrap before every stop",
        find: |ev| {
            each(ev, "boot", |j| {
                let g = num(&ev[j], "gen")?;
                let i = first_before(ev, j, "stop", |s| num(s, "gen") == Some(g - 1))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A bootstrap carries every registration that its stop quorum kept.
        name: "drop a registration from a bootstrap",
        find: |ev| {
            ["boot", "activate"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let regs = elements(&ev[k], "regs");
                    (!regs.is_empty()).then(|| {
                        replace(ev, k, with(&ev[k], "regs", Value::Set(regs[1..].to_vec())))
                    })
                })
            })
        },
    },
    Edit {
        // A bootstrap carries the floor that its stop quorum made effective.
        name: "move the floor of a bootstrap",
        find: |ev| {
            ["boot", "activate"].iter().find_map(|name| {
                each(ev, name, |k| {
                    let wm = num(&ev[k], "wm")?;
                    Some(replace(ev, k, with(&ev[k], "wm", Value::Int(wm + 1))))
                })
            })
        },
    },
    Edit {
        // A successor decree votes for a set that booted.
        name: "move a decree vote before its bootstrap",
        find: |ev| {
            each(ev, "vote", |j| {
                let g = num(&ev[j], "gen")?;
                let i = first_before(ev, j, "boot", |b| num(b, "gen") == Some(g + 1))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A matchmaker votes once in a decree ballot.
        name: "repeat a decree vote",
        find: |ev| first(ev, "vote", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A set activates once its decree chose it.
        name: "move an activation before every decree vote",
        find: |ev| {
            each(ev, "activate", |j| {
                let g = num(&ev[j], "gen")?;
                let i = first_before(ev, j, "vote", |v| num(v, "gen") == Some(g - 1))?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A matchmaker activates its generation once.
        name: "repeat an activation",
        find: |ev| first(ev, "activate", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A matchmaker of a stopped generation registers nothing.
        name: "move a registration after its matchmaker stopped",
        find: |ev| {
            each(ev, "register", |i| {
                let e = &ev[i];
                let j = find_from(ev, i + 1, "stop", |s| field(s, "mm") == field(e, "mm"))?;
                // No activation of the matchmaker between the two: it stays
                // stopped where the registration lands.
                if ev[i..j]
                    .iter()
                    .any(|a| is(a, "activate") && field(a, "mm") == field(e, "mm"))
                {
                    return None;
                }
                let mut out = ev.to_vec();
                let moved = out.remove(i);
                out.insert(j, moved);
                Some((i, out))
            })
        },
    },
    Edit {
        // A matchmaker of a stopped generation acks no floor.
        name: "move a gc ack after its matchmaker stopped",
        find: |ev| {
            each(ev, "gcack", |i| {
                let e = &ev[i];
                let j = find_from(ev, i + 1, "stop", |s| field(s, "mm") == field(e, "mm"))?;
                // No activation of the matchmaker between the two: it stays
                // stopped where the ack lands.
                if ev[i..j]
                    .iter()
                    .any(|a| is(a, "activate") && field(a, "mm") == field(e, "mm"))
                {
                    return None;
                }
                let mut out = ev.to_vec();
                let moved = out.remove(i);
                out.insert(j, moved);
                Some((i, out))
            })
        },
    },
    Edit {
        // A matchmaker that a later set boots, and that took no step
        // before, is not active yet: it registers nothing.
        name: "register at a matchmaker not active yet",
        find: |ev| {
            each(ev, "register", |k| {
                let late = (k + 1..ev.len())
                    .filter(|&j| is(&ev[j], "boot"))
                    .find_map(|j| {
                        let mm = field(&ev[j], "mm")?;
                        let seen = ev[..j].iter().any(|a| field(a, "mm") == Some(mm));
                        let stopped = ev
                            .iter()
                            .any(|a| is(a, "stop") && field(a, "mm") == Some(mm));
                        (!seen && !stopped).then_some(mm)
                    })?;
                Some(replace(ev, k, with(&ev[k], "mm", late.clone())))
            })
        },
    },
    // --- The batcher ---
    Edit {
        // A writer pushes a write that it made.
        name: "move a push before its write",
        find: |ev| {
            each(ev, "write", |i| {
                let j = find_from(ev, i + 1, "push", |p| {
                    field(p, "tok") == field(&ev[i], "tok")
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A flush sends the writes its lane holds: a write pushed before.
        name: "drop a push that a flush sends",
        find: |ev| {
            each(ev, "push", |k| {
                let tok = field(&ev[k], "tok")?;
                let next = (k + 1..ev.len()).find(|&j| {
                    (is(&ev[j], "push") && field(&ev[j], "tok") == Some(tok))
                        || (is(&ev[j], "flush") && elements(&ev[j], "ws").contains(tok))
                })?;
                is(&ev[next], "flush").then(|| drop(ev, k))
            })
        },
    },
    Edit {
        // A flush sends a lane after its pushes.
        name: "move a flush before a push it sends",
        find: |ev| {
            each(ev, "flush", |j| {
                let ws = elements(&ev[j], "ws");
                let i = first_before(ev, j, "push", |p| {
                    field(p, "tok").is_some_and(|t| ws.contains(t))
                })?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A flush sends the writes of its lane: here one more, made later.
        name: "add a later write to a flush",
        find: |ev| {
            each(ev, "flush", |k| {
                let later = find_from(ev, k + 1, "write", |w| {
                    field(w, "jr") == field(&ev[k], "jr")
                })?;
                let mut ws = elements(&ev[k], "ws").to_vec();
                ws.push(field(&ev[later], "tok")?.clone());
                Some(replace(ev, k, with(&ev[k], "ws", Value::Seq(ws))))
            })
        },
    },
    Edit {
        // A flush of a whole lane empties it: the same lane cannot go twice.
        name: "repeat a flush of a lane",
        find: |ev| {
            each(ev, "flush", |k| {
                (elements(&ev[k], "ws").len() > 1).then(|| twice(ev, k))
            })
        },
    },
    Edit {
        // An answer counts the records the write made.
        name: "change the count of a write",
        find: |ev| {
            each(ev, "write", |k| {
                let n = num(&ev[k], "n")?;
                find_from(ev, k + 1, "answer", |a| {
                    field(a, "tok") == field(&ev[k], "tok")
                })?;
                Some(replace(ev, k, with(&ev[k], "n", Value::Int(n + 1))))
            })
        },
    },
    Edit {
        // A batch lands at the next position of its journal.
        name: "move the position of a landing",
        find: |ev| {
            each(ev, "land", |k| {
                let seq = num(&ev[k], "seq")?;
                Some(replace(ev, k, with(&ev[k], "seq", Value::Int(seq + 1))))
            })
        },
    },
    Edit {
        // A write is answered once a batch that holds it landed: here the
        // answer names a write that no batch landed with yet.
        name: "answer a write that did not land",
        find: |ev| {
            each(ev, "answer", |k| {
                let landed: BTreeSet<&Value> = ev[..k]
                    .iter()
                    .filter(|l| is(l, "land"))
                    .filter_map(|l| num(l, "id"))
                    .filter_map(|id| {
                        let flush = ev
                            .iter()
                            .filter(|f| is(f, "flush"))
                            .nth(usize::try_from(id - 1).ok()?)?;
                        Some(elements(flush, "ws"))
                    })
                    .flatten()
                    .collect();
                let other = ev[..k]
                    .iter()
                    .filter(|w| is(w, "write"))
                    .filter_map(|w| field(w, "tok"))
                    .find(|t| !landed.contains(t))?;
                Some(replace(ev, k, with(&ev[k], "tok", other.clone())))
            })
        },
    },
    Edit {
        // A writer makes each write once.
        name: "repeat a write",
        find: |ev| first(ev, "write", |_| true).map(|k| twice(ev, k)),
    },
    Edit {
        // A lane goes out in push order.
        name: "reverse a lane",
        find: |ev| {
            each(ev, "flush", |k| {
                let mut ws = elements(&ev[k], "ws").to_vec();
                (ws.len() > 1).then(|| {
                    ws.reverse();
                    replace(ev, k, with(&ev[k], "ws", Value::Seq(ws)))
                })
            })
        },
    },
    Edit {
        // A flush sends every write of its lane.
        name: "drop a write from a flush",
        find: |ev| {
            each(ev, "flush", |k| {
                let ws = elements(&ev[k], "ws");
                (ws.len() > 1)
                    .then(|| replace(ev, k, with(&ev[k], "ws", Value::Seq(ws[1..].to_vec()))))
            })
        },
    },
    Edit {
        // A flush sends the lane of its journal.
        name: "change the journal of a flush",
        find: |ev| {
            each(ev, "flush", |k| {
                let jr = num(&ev[k], "jr")?;
                Some(replace(ev, k, with(&ev[k], "jr", Value::Int(jr + 1))))
            })
        },
    },
    Edit {
        // A batch lands after it was flushed.
        name: "move a landing before every flush",
        find: |ev| {
            each(ev, "land", |j| {
                let i = first_before(ev, j, "flush", |_| true)?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A batch lands at the position the journal gave it.
        name: "move the position of an answer",
        find: |ev| {
            each(ev, "answer", |k| {
                let seq = num(&ev[k], "seq")?;
                Some(replace(ev, k, with(&ev[k], "seq", Value::Int(seq + 1))))
            })
        },
    },
    Edit {
        // An answer counts the records of its write.
        name: "change the count of an answer",
        find: |ev| {
            each(ev, "answer", |k| {
                let n = num(&ev[k], "n")?;
                Some(replace(ev, k, with(&ev[k], "n", Value::Int(n + 1))))
            })
        },
    },
    Edit {
        // A write is answered once its batch landed.
        name: "move an answer before every landing",
        find: |ev| {
            each(ev, "answer", |j| {
                let i = first_before(ev, j, "land", |_| true)?;
                Some(hoist(ev, j, i))
            })
        },
    },
    Edit {
        // A write is answered once a batch that holds it landed: here the
        // only landing of such a batch is gone.
        name: "drop a landing that an answer needs",
        find: |ev| {
            let flushes: Vec<&Value> = ev.iter().filter(|f| is(f, "flush")).collect();
            each(ev, "answer", |k| {
                let tok = field(&ev[k], "tok")?;
                let holds = |l: &Value| {
                    num(l, "id")
                        .and_then(|id| usize::try_from(id - 1).ok())
                        .and_then(|i| flushes.get(i))
                        .is_some_and(|f| elements(f, "ws").contains(tok))
                };
                let lands: Vec<usize> = (0..k)
                    .filter(|&j| is(&ev[j], "land") && holds(&ev[j]))
                    .collect();
                (lands.len() == 1).then(|| drop(ev, lands[0]))
            })
        },
    },
    Edit {
        // A write is answered once.
        name: "repeat an answer",
        find: |ev| first(ev, "answer", |_| true).map(|k| twice(ev, k)),
    },
    // --- Any trace ---
    Edit {
        // Every event is a step of the model: an unknown name is none.
        name: "rename an event",
        find: |ev| {
            (!ev.is_empty()).then(|| replace(ev, 0, with(&ev[0], "event", Value::from("unknown"))))
        },
    },
    Edit {
        // An event carries every field its step reads.
        name: "drop a field of an event",
        find: |ev| {
            let e = ev.first()?;
            let Value::Record(fields) = e else {
                return None;
            };
            let name = fields.keys().find(|k| *k != "event")?;
            let mut fields = fields.clone();
            fields.remove(name);
            Some(replace(ev, 0, Value::Record(fields)))
        },
    },
];
