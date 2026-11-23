//! Model-based trace checking of the roles walk: TLC checks that each
//! checked walk is a behaviour of `specs/tla/journal-ctrl/RolesTrace.tla`
//! (the actions of `Journal.tla`, `JournalCompact.tla` and
//! `JournalCtrl.tla`), refuses the
//! trace control, and refuses
//! every edit of the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::roles::{RolesEvent, RolesMutant, Shape, walk};
use journal_sim::trace::roles::{EDITS, roles_spec, trace};
use journal_sim::trace::{Trace, Verdict, require_tlc};
use paros_journal::{Command, Leader};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The roles walks TLC always checks.
const ROLES_SEEDS: u64 = 4;

/// The chaos steps of a traced walk.
fn chaos() -> u64 {
    knob("TRACE_CHAOS", 800)
}

/// One traced walk: its trace and its events.
fn replay(mutant: RolesMutant, seed: u64) -> (Trace, Vec<RolesEvent>) {
    let run = walk(Shape::NARROW, mutant, 1, chaos())
        .replay(seed)
        .unwrap_or_else(|failure| panic!("seed {seed}: {failure}"));
    let events: Vec<RolesEvent> = run.events().cloned().collect();
    (trace(run.model.fresh(), &events), events)
}

/// A rule of `RolesTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[RolesEvent]) -> bool);

fn has(events: &[RolesEvent], pick: impl Fn(&RolesEvent) -> bool) -> bool {
    events.iter().any(pick)
}

/// The start of the segment of ballot `b`, once its leader opened.
fn start_of(events: &[RolesEvent], b: u64) -> Option<u64> {
    events.iter().find_map(|e| match e {
        RolesEvent::Open { b: o, start } if *o == b => Some(*start),
        _ => None,
    })
}

/// A proposal of ballot `b` at `s`, below the start of its segment, that
/// `pick` selects.
fn below_start(events: &[RolesEvent], pick: fn(&Command) -> bool) -> bool {
    has(events, |e| {
        matches!(e, RolesEvent::Propose { b, s, command }
            if pick(command) && start_of(events, *b).is_some_and(|start| *s < start))
    })
}

fn applied(events: &[RolesEvent], pick: impl Fn(&Command, &str) -> bool) -> bool {
    has(
        events,
        |e| matches!(e, RolesEvent::Apply { command, res: Some(res), .. } if pick(command, res)),
    )
}

/// The rules of `RolesTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a seal", |e| {
        has(e, |e| matches!(e, RolesEvent::Seal { .. }))
    }),
    ("a first leader whose seal lost", |events| {
        has(events, |e| {
            matches!(e, RolesEvent::Phase1 { b, end: Some(_), .. }
                if !events.contains(&RolesEvent::Seal { b: *b }))
        })
    }),
    ("a Noop below the start of a segment", |e| {
        below_start(e, |c| *c == Command::Noop)
    }),
    ("an old vote proposed again below the start", |e| {
        below_start(e, |c| *c != Command::Noop)
    }),
    (
        "a Noop above the start, below a vote that Phase 1 saw",
        |events| {
            has(events, |e| {
                matches!(e, RolesEvent::Propose { b, s, command: Command::Noop }
                if start_of(events, *b).is_some_and(|start| *s >= start))
            })
        },
    ),
    ("a promise after its Phase 1 closed", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, RolesEvent::Phase1 { b, .. }
                if events[at..].iter().any(|later| matches!(later, RolesEvent::Promise { b: p, .. } if p == b)))
        })
    }),
    ("a crash that lost a queued write", |events| {
        events.iter().enumerate().any(|(at, e)| {
            let RolesEvent::Crash { a } = e else {
                return false;
            };
            let queued = events[..at]
                .iter()
                .filter(|e| matches!(e, RolesEvent::Promise { a: x, .. } | RolesEvent::Accept { a: x, .. } if x == a))
                .count();
            let flushed = events[..at]
                .iter()
                .filter(|e| matches!(e, RolesEvent::Flush { a: x } if x == a))
                .count();
            let crashed = events[..at]
                .iter()
                .any(|e| matches!(e, RolesEvent::Crash { a: x } if x == a));
            !crashed && queued > flushed
        })
    }),
    ("a third segment", |e| {
        has(e, |e| matches!(e, RolesEvent::Reshape { g: 3, .. }))
    }),
    ("a base taken", |e| {
        has(e, |e| matches!(e, RolesEvent::Base { .. }))
    }),
    ("a boundary state written", |e| {
        has(e, |e| matches!(e, RolesEvent::Boundary { .. }))
    }),
    ("a segment retired", |e| {
        has(e, |e| matches!(e, RolesEvent::Retire { .. }))
    }),
    ("a sealed segment trimmed", |e| {
        has(e, |e| matches!(e, RolesEvent::Trim { .. }))
    }),
    ("a copy of a trimmed segment dropped", |e| {
        has(e, |e| matches!(e, RolesEvent::Drop { .. }))
    }),
    ("a resend answered Duplicate", |e| {
        applied(e, |_, res| res == "dup")
    }),
    ("a resend across a seal answered Below", |e| {
        applied(e, |_, res| res == "below")
    }),
    ("a Resolve answered written", |e| {
        has(e, |e| matches!(e, RolesEvent::Resolve { res: "dup", .. }))
    }),
    ("a SetLeader to a multi-writer mode applied", |e| {
        applied(e, |c, res| {
            matches!(
                c,
                Command::SetLeader {
                    new: Leader::Multi(_),
                    ..
                }
            ) && res == "ok"
        })
    }),
    ("a truncate applied", |e| {
        applied(e, |c, res| {
            matches!(c, Command::Truncate { .. }) && res == "ok"
        })
    }),
    ("a replica compacted", |e| {
        has(e, |e| matches!(e, RolesEvent::CompactReplica { .. }))
    }),
    (
        "an acceptor dropped votes, then flushed the drop",
        |events| {
            events.iter().enumerate().any(|(at, e)| {
            matches!(e, RolesEvent::Compact { a, .. }
                if events[at..].iter().any(|later| matches!(later, RolesEvent::Flush { a: f } if f == a)))
        })
        },
    ),
    ("a Phase 1 after an acceptor dropped votes", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, RolesEvent::Compact { .. })
                && events[at..]
                    .iter()
                    .any(|later| matches!(later, RolesEvent::Phase1 { .. }))
        })
    }),
    ("a laggard jumped", |e| {
        has(e, |e| matches!(e, RolesEvent::Jump { .. }))
    }),
    ("a blind fold applied a slot", |e| {
        has(e, |e| matches!(e, RolesEvent::Apply { res: None, .. }))
    }),
    ("an acceptor booted with a damaged vote", |e| {
        has(e, |e| matches!(e, RolesEvent::Rot { .. }))
    }),
    ("a late answer decided a blocked slot", |e| {
        has(e, |e| matches!(e, RolesEvent::Probe { .. }))
    }),
    ("a replica gave a blocked slot its command", |e| {
        has(e, |e| matches!(e, RolesEvent::Learn { .. }))
    }),
    ("an acceptor repaired its faulty vote", |e| {
        has(e, |e| matches!(e, RolesEvent::Repair { .. }))
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<RolesEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= ROLES_SEEDS {
            break;
        }
        let (traced, events) = replay(RolesMutant::None, seed);
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= ROLES_SEEDS {
            continue;
        }
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the roles walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_roles_walks() {
    let check = roles_spec();
    require_tlc(&check);
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("roles seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "roles seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => panic!("roles seed {seed}: {other:?}"),
        }
    }
    assert!(unmet.is_empty(), "roles: no walk meets {unmet:?}");
}

/// The driver runs a boundary write before its batch is durable. The
/// oracles pass it (`the_oracles_pass_an_early_boundary_write`); TLC
/// refuses the write.
#[test]
fn tlc_refuses_a_boundary_write_before_its_batch() {
    let check = roles_spec();
    require_tlc(&check);
    let refused = (1..=100).find_map(|seed| {
        let (traced, events) = replay(RolesMutant::BoundaryBeforeDurable, seed);
        match check.check(&traced).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the control");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, RolesEvent::Boundary { .. }),
        "seed {seed}: TLC refused {event:?}, not the boundary write"
    );
}

/// The trace control passes every oracle.
#[test]
fn the_oracles_pass_an_early_boundary_write() {
    try_campaign(
        &walk(
            Shape::NARROW,
            RolesMutant::BoundaryBeforeDurable,
            100,
            chaos(),
        ),
        &[],
    )
    .expect("the oracles pass the trace control");
}

/// The least number of edits that find a place in the recorded traces.
const MIN_EDITS: usize = 20;

/// TLC refuses every edit of the recorded traces at or after the edited
/// event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_roles_traces() {
    let check = roles_spec();
    require_tlc(&check);
    let (mut walks, _) = checked();
    // The shortest traces first: TLC checks each edit on the first trace
    // where it finds a place.
    walks.sort_by_key(|(_, events, _)| events.len());
    let recorded: Vec<&Trace> = walks.iter().map(|(_, _, traced)| traced).collect();
    let mut placed = 0;
    let mut wrong = Vec::new();
    for edit in EDITS {
        let Some(mutant) = recorded.iter().find_map(|traced| edit.apply(traced)) else {
            eprintln!("edit {:?}: no place", edit.name);
            continue;
        };
        placed += 1;
        match check.check(&mutant.trace).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) if rejection.index >= mutant.at => {}
            Verdict::InvariantViolated { matched, .. } if matched >= mutant.at => {}
            other => wrong.push(format!("{}: at {}, {other:?}", edit.name, mutant.at)),
        }
    }
    assert!(
        wrong.is_empty(),
        "edits TLC did not refuse where it must: {wrong:#?}"
    );
    assert!(
        placed >= MIN_EDITS,
        "only {placed} edits found a place, {MIN_EDITS} must"
    );
}
