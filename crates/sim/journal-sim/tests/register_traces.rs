//! Model-based trace checking of the register walk: TLC checks that each
//! checked walk is a behaviour of
//! `specs/tla/segment-records/RegisterTrace.tla` (the actions of
//! `SegmentRecords.tla`), refuses the trace control, and refuses every edit
//! of the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::register::{Kind, RegisterEvent, RegisterMutant, Shape, walk};
use journal_sim::trace::register::{EDITS, register_spec, trace};
use journal_sim::trace::{Trace, Verdict, require_tlc};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The register walks TLC always checks.
const REGISTER_SEEDS: u64 = 10;

/// One traced walk: its trace and its events.
fn replay(mutant: RegisterMutant, seed: u64) -> Option<(Trace, Vec<RegisterEvent>)> {
    let run = walk(Shape::TRACED, mutant, 1, knob("TRACE_CHAOS", 120))
        .replay(seed)
        .ok()?;
    let events: Vec<RegisterEvent> = run.events().cloned().collect();
    let traced = trace(&run.model.kinds(), run.model.start(), &events);
    Some((traced, events))
}

/// A rule of `RegisterTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[RegisterEvent]) -> bool);

fn has(events: &[RegisterEvent], pick: fn(&RegisterEvent) -> bool) -> bool {
    events.iter().any(pick)
}

/// The rules of `RegisterTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a refused change", |e| {
        has(e, |e| {
            matches!(e, RegisterEvent::Read { refused: true, .. })
        })
    }),
    ("a read at the next set", |e| {
        has(e, |e| matches!(e, RegisterEvent::Read { g: 1, .. }))
    }),
    ("the acceptors retired", |e| {
        has(e, |e| matches!(e, RegisterEvent::Retire))
    }),
    ("a replica dropped its copy", |e| {
        has(e, |e| matches!(e, RegisterEvent::Drop))
    }),
    ("a handover closed", |e| {
        has(e, |e| matches!(e, RegisterEvent::Close { .. }))
    }),
    ("the decree chose", |e| {
        has(e, |e| matches!(e, RegisterEvent::Decide(_)))
    }),
    ("a member served the next set", |e| {
        has(e, |e| matches!(e, RegisterEvent::Activate(_)))
    }),
    ("a machine crashed", |e| {
        has(e, |e| matches!(e, RegisterEvent::Crash(_)))
    }),
    ("a write to a member that froze later", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, RegisterEvent::Write { m, .. }
                if events[at..].contains(&RegisterEvent::Stop(*m)))
        })
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<RegisterEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= REGISTER_SEEDS {
            break;
        }
        let (traced, events) = replay(RegisterMutant::None, seed).expect("the walk passes");
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= REGISTER_SEEDS {
            continue;
        }
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the register walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_register_walks() {
    let check = register_spec();
    require_tlc(&check);
    try_campaign(
        &walk(
            Shape::TRACED,
            RegisterMutant::None,
            knob("TRACE_SEARCH", 300),
            knob("TRACE_CHAOS", 120),
        ),
        &[],
    )
    .expect("the walks pass");
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("register seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "register seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => panic!("register seed {seed}: {other:?}"),
        }
    }
    assert!(unmet.is_empty(), "register: no walk meets {unmet:?}");
}

/// The driver forges W's bootstrap. The oracles pass it
/// (`the_oracles_pass_a_forged_bootstrap`); TLC refuses the decree.
#[test]
fn tlc_refuses_a_decree_before_every_bootstrap() {
    let check = register_spec();
    require_tlc(&check);
    let refused = (1..=100).find_map(|seed| {
        let (traced, events) = replay(RegisterMutant::ForgedBootstrap, seed)?;
        match check.check(&traced).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, RegisterEvent::Decide(_)),
        "seed {seed}: TLC refused {event:?}, not the decree"
    );
}

/// The trace control passes every oracle.
#[test]
fn the_oracles_pass_a_forged_bootstrap() {
    try_campaign(
        &walk(Shape::TRACED, RegisterMutant::ForgedBootstrap, 100, 120),
        &[],
    )
    .expect("the oracles pass the trace control");
}

/// The least number of edits that find a place in the recorded traces.
const MIN_EDITS: usize = 20;

/// TLC refuses every edit of the recorded traces at or after the edited
/// event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_register_traces() {
    let check = register_spec();
    require_tlc(&check);
    let (walks, _) = checked();
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

#[test]
fn every_kind_has_its_name_in_the_model() {
    let names: Vec<&str> = [
        Kind::Seal1,
        Kind::Seal2,
        Kind::Retire,
        Kind::Offload,
        Kind::Drop,
        Kind::Elect,
        Kind::Reshape1,
    ]
    .map(Kind::name)
    .to_vec();
    assert_eq!(
        names,
        [
            "seal1", "seal2", "retire", "offload", "drop", "elect", "reshape1"
        ]
    );
}
