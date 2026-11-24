//! Model-based trace checking of the controller walk: TLC checks that each
//! checked walk is a behaviour of
//! `specs/tla/grid-controller/ControllerTrace.tla` (the actions of
//! `GridController.tla`), refuses the trace control, and refuses every
//! edit of the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::controller::{ControllerEvent, ControllerMutant, Shape, walk};
use journal_sim::trace::controller::{EDITS, controller_spec, trace};
use journal_sim::trace::{Trace, Verdict, mutate, require_tlc};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The controller walks TLC always checks.
const CONTROLLER_SEEDS: u64 = 10;

/// One traced walk: its trace and its events.
fn replay(mutant: ControllerMutant, seed: u64) -> Option<(Trace, Vec<ControllerEvent>)> {
    let run = walk(Shape::TRACED, mutant, 1, knob("TRACE_CHAOS", 300))
        .replay(seed)
        .ok()?;
    let events: Vec<ControllerEvent> = run.events().cloned().collect();
    Some((trace(&events), events))
}

/// A rule of `ControllerTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[ControllerEvent]) -> bool);

fn has(events: &[ControllerEvent], pick: fn(&ControllerEvent) -> bool) -> bool {
    events.iter().any(pick)
}

/// The rules of `ControllerTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a candidate lost the race", |e| {
        has(e, |e| {
            matches!(e, ControllerEvent::Elect { won: false, .. })
        })
    }),
    ("two terms won", |e| {
        e.iter()
            .filter(|e| matches!(e, ControllerEvent::Elect { won: true, .. }))
            .count()
            >= 2
    }),
    ("a deposed holder stood down", |e| {
        has(e, |e| matches!(e, ControllerEvent::Stand(_)))
    }),
    ("a call of an old term refused", |e| {
        has(e, |e| matches!(e, ControllerEvent::Process(_, _, "term")))
    }),
    ("a reshape opened a segment", |e| {
        has(e, |e| matches!(e, ControllerEvent::Process(_, _, "ok")))
    }),
    ("an ok reply lost", |e| {
        has(e, |e| matches!(e, ControllerEvent::LoseRep(_, _, "ok")))
    }),
    ("a request lost", |e| {
        has(e, |e| matches!(e, ControllerEvent::LoseReq(..)))
    }),
    ("a call answered ok twice", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, ControllerEvent::Process(c, call, "ok")
                if events[at + 1..].iter().any(|later| matches!(later,
                    ControllerEvent::Process(d, again, "ok") if d == c && again == call)))
        })
    }),
    ("the journal sealed", |e| {
        has(e, |e| matches!(e, ControllerEvent::Seal(_)))
    }),
    ("the journal retired", |e| {
        has(e, |e| matches!(e, ControllerEvent::Retire(_)))
    }),
    ("an acceptor failed", |e| {
        has(e, |e| matches!(e, ControllerEvent::Fail(_)))
    }),
    ("a crash between plan and call", |events| {
        events.iter().enumerate().any(|(at, e)| {
            let ControllerEvent::Plan(c, _) = e else {
                return false;
            };
            events[at + 1..]
                .iter()
                .find(|later| {
                    matches!(later, ControllerEvent::Call(d) | ControllerEvent::Crash(d) if d == c)
                })
                .is_some_and(|later| matches!(later, ControllerEvent::Crash(_)))
        })
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<ControllerEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= CONTROLLER_SEEDS {
            break;
        }
        let (traced, events) = replay(ControllerMutant::None, seed).expect("the walk passes");
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= CONTROLLER_SEEDS {
            continue;
        }
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the controller walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_controller_walks() {
    let check = controller_spec();
    require_tlc(&check);
    try_campaign(
        &walk(
            Shape::TRACED,
            ControllerMutant::None,
            knob("TRACE_SEARCH", 300),
            knob("TRACE_CHAOS", 300),
        ),
        &[],
    )
    .expect("the walks pass");
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("controller seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "controller seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => {
                panic!("controller seed {seed}: {other:?}")
            }
        }
    }
    assert!(unmet.is_empty(), "controller: no walk meets {unmet:?}");
}

/// The driver elects with no compare. The oracles pass it
/// (`the_oracles_pass_an_election_with_no_compare`); TLC refuses the
/// election of a candidate that read an old term.
#[test]
fn tlc_refuses_an_election_with_no_compare() {
    let check = controller_spec();
    require_tlc(&check);
    let refused = (1..=100).find_map(|seed| {
        let (traced, events) = replay(ControllerMutant::ElectNoCas, seed)?;
        match check.check(&traced).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, ControllerEvent::Elect { won: true, .. }),
        "seed {seed}: TLC refused {event:?}, not the election"
    );
}

/// The trace control passes every oracle.
#[test]
fn the_oracles_pass_an_election_with_no_compare() {
    try_campaign(
        &walk(Shape::TRACED, ControllerMutant::ElectNoCas, 300, 300),
        &[],
    )
    .expect("the oracles pass the trace control");
}

/// Every edit finds a place in a walk, and TLC refuses it at or after the
/// edited event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_controller_traces() {
    let check = controller_spec();
    require_tlc(&check);
    let (walks, _) = checked();
    let recorded: Vec<&Trace> = walks.iter().map(|(_, _, traced)| traced).collect();
    let from = walks
        .iter()
        .map(|(seed, ..)| seed + 1)
        .max()
        .expect("a checked walk");
    mutate::judge(&check, EDITS, &recorded, from, |seed| {
        replay(ControllerMutant::None, seed)
            .expect("the walk passes")
            .0
    });
}
