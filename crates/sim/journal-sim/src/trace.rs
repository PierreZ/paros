//! Model-based trace checking of the fold walk: its steps as a trace of
//! `specs/tla/journal-state/FoldTrace.tla`, judged by TLC with
//! [`moonpool_tla`].
//!
//! The encoding: a leader value is a string, `"unset"`, `"none"`
//! (multi-writer) or `"u<n>"`; a write holds one record and its `id` is the
//! first byte of its payload (the walk's payload is that byte, and its
//! client headers are a function of it); an unfenced write has `e = 0`; a
//! verdict is the name the model gives it (`"ok"`, `"dup"`, `"taken"`,
//! `"fenced"`, `"early"`, `"trunc"`, `"below"`, `"wrong"`, `"refused"`,
//! `"noop"`). The model has one refusal for a truncate, `"fenced"`: the
//! code's `WrongMode` of a truncate is written so.

pub mod mutate;
pub mod register;
pub mod roles;

use std::path::PathBuf;

use moonpool_tla::Value;
pub use moonpool_tla::{Rejection, Trace, TraceCheck, Verdict};
use paros_journal::resolve::ResolveAnswer;
use paros_journal::{Command, Fence, Leader, LeaderUuid, Write};

use crate::fold::FoldEvent;

/// The folder of the TLA+ models, from the root of the workspace.
#[must_use]
pub fn specs_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../specs/tla")
}

/// The check of `FoldTrace` in `specs/tla/journal-state/`.
#[must_use]
pub fn fold_spec() -> TraceCheck {
    TraceCheck::new(specs_dir().join("journal-state"), "FoldTrace")
}

/// TLC must run here. TLC (`tlc`) is in the nix dev shell, and every test
/// runs in it. A trace test never skips: a missing TLC fails the test (no
/// excuse in simulation, `AGENTS.md`).
///
/// # Panics
///
/// When TLC is missing.
pub fn require_tlc(check: &TraceCheck) {
    assert!(
        check.tlc_available(),
        "TLC (`tlc`) is missing: run the tests in the nix dev shell (`nix develop`)"
    );
}

/// A count or a position as a TLA+ integer.
///
/// # Panics
///
/// When `n` does not fit an `i64` (never, for a walk).
#[must_use]
pub fn int(n: impl TryInto<i64>) -> Value {
    Value::Int(n.try_into().ok().expect("a walk's numbers fit an i64"))
}

pub(crate) fn uuid(uuid: LeaderUuid) -> Value {
    Value::Str(format!("u{}", uuid.0))
}

pub(crate) fn leader(leader: Leader) -> Value {
    match leader {
        Leader::Unset => Value::from("unset"),
        Leader::Multi => Value::from("none"),
        Leader::Uuid(id) => uuid(id),
    }
}

fn state(fields: &mut Vec<(&'static str, Value)>, (lead, next, first): (Leader, u64, u64)) {
    fields.push(("leader", leader(lead)));
    fields.push(("next", int(next)));
    fields.push(("first", int(first)));
}

pub(crate) fn write_fields(fields: &mut Vec<(&'static str, Value)>, write: &Write) {
    assert!(write.records.len() == 1, "a traced write holds one record");
    let id = write.records[0].payload[0];
    fields.push(("op", Value::from("w")));
    fields.push(("id", int(id)));
    match write.fence {
        Fence::Leader {
            uuid: u,
            expected_seq,
        } => {
            fields.push(("u", uuid(u)));
            fields.push(("e", int(expected_seq.0)));
        }
        Fence::Open => {
            fields.push(("u", Value::from("none")));
            fields.push(("e", int(0)));
        }
    }
}

/// The fields of `command` as the model writes a command (`Cmd` of
/// `FoldTrace.tla` and `RolesTrace.tla`).
///
/// # Panics
///
/// On an `Upgrade`, which the model does not have.
pub(crate) fn command_fields(fields: &mut Vec<(&'static str, Value)>, command: &Command) {
    match command {
        Command::Noop => fields.push(("op", Value::from("noop"))),
        Command::Write(write) => write_fields(fields, write),
        Command::Truncate { fence, up_to } => {
            fields.push(("op", Value::from("tr")));
            fields.push(("u", fence.map_or(Value::from("none"), uuid)));
            fields.push(("up", int(up_to.0)));
        }
        Command::SetLeader { new, old } => {
            fields.push(("op", Value::from("sl")));
            fields.push(("new", new.map_or(Value::from("none"), uuid)));
            fields.push(("old", leader(*old)));
        }
        Command::Upgrade { .. } => panic!("the model has no Upgrade"),
    }
}

/// The events of one fold walk, whose fresh journal is `fresh`, as a trace
/// of `FoldTrace.tla`.
///
/// # Panics
///
/// When an event holds what the model does not have (see [`encode`]).
#[must_use]
pub fn trace<'a>(fresh: Leader, events: impl IntoIterator<Item = &'a FoldEvent>) -> Trace {
    let mut trace = start(fresh);
    for event in events {
        let (name, fields) = encode(event);
        trace.event(name, fields);
    }
    trace
}

/// The trace of no event of a fold walk whose fresh journal is `fresh`:
/// the definitions of `FoldTrace.tla`, and the start of a live check.
#[must_use]
pub fn start(fresh: Leader) -> Trace {
    let mut trace = Trace::new();
    trace.define("TraceFresh", leader(fresh));
    trace
}

/// One event of a fold walk as an event of `FoldTrace.tla`: its name and
/// its fields.
///
/// # Panics
///
/// When the event holds what the model does not have: a write of more or
/// fewer than one record, an `Upgrade`, an answer `Empty` or `Reserved`, a
/// `Resolve` that was not covered.
#[must_use]
pub fn encode(event: &FoldEvent) -> (&'static str, Vec<(&'static str, Value)>) {
    let mut fields: Vec<(&'static str, Value)> = Vec::new();
    let name = match event {
        FoldEvent::Apply {
            command,
            answer,
            state: after,
        } => {
            let mut res = *answer;
            command_fields(&mut fields, command);
            if matches!(command, Command::Truncate { .. }) && res == "wrong" {
                res = "fenced";
            }
            assert!(
                !matches!(res, "empty" | "reserved"),
                "the model has no answer {res}"
            );
            fields.push(("res", Value::from(res)));
            state(&mut fields, *after);
            "apply"
        }
        FoldEvent::Seal { state: boundary } => {
            state(&mut fields, *boundary);
            "seal"
        }
        FoldEvent::Resolve {
            segment,
            write,
            answer,
        } => {
            write_fields(&mut fields, write);
            fields.push(("seg", int(*segment)));
            let res = match answer {
                ResolveAnswer::Written { .. } => "dup",
                ResolveAnswer::NotWritten => "taken",
                ResolveAnswer::Truncated => "trunc",
                ResolveAnswer::NotCovered => panic!("a Resolve that was not covered"),
            };
            fields.push(("res", Value::from(res)));
            "resolve"
        }
    };
    (name, fields)
}
