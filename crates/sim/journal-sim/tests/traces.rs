//! Model-based trace checking of the fold walk: TLC checks that each
//! checked walk is a behaviour of `specs/tla/journal-state/FoldTrace.tla`
//! (the fold of `Journal.tla`), refuses the trace control, and refuses
//! every edit of the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::fold::{FoldEvent, FoldModel, FoldMutant, Shape};
use journal_sim::trace::{Trace, Verdict, fold_spec, mutate, require_tlc, trace};
use moonpool_walk::Walk;
use paros_journal::resolve::ResolveAnswer;
use paros_journal::{Command, Fence, Leader};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The fold walks TLC always checks.
const FOLD_SEEDS: u64 = 10;

/// A traced fold walk with `mutant` planted.
fn fold_walk(mutant: FoldMutant) -> Walk<FoldModel> {
    Walk::<FoldModel>::new()
        .chaos_steps(knob("TRACE_CHAOS", 100))
        .env_prefix(None)
        .model(move |cx| FoldModel::build(Shape::TRACED, mutant, cx))
}

/// One walk: its fresh leader and its events.
fn replay(mutant: FoldMutant, seed: u64) -> Option<(Leader, Vec<FoldEvent>)> {
    let run = fold_walk(mutant).replay(seed).ok()?;
    Some((run.model.fresh(), run.events().cloned().collect()))
}

/// A rule of `FoldTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[FoldEvent]) -> bool);

fn answered(events: &[FoldEvent], name: &str) -> bool {
    events
        .iter()
        .any(|e| matches!(e, FoldEvent::Apply { answer, .. } if *answer == name))
}

fn resolved(events: &[FoldEvent], written: bool) -> bool {
    events.iter().any(|e| {
        matches!(e, FoldEvent::Resolve { answer, .. }
            if matches!(answer, ResolveAnswer::Written { .. }) == written
                && *answer != ResolveAnswer::Truncated)
    })
}

/// The rules of `FoldTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a resend across a seal answered Below", |e| {
        answered(e, "below")
    }),
    ("a Resolve answered written", |e| resolved(e, true)),
    ("a Resolve answered not written", |e| resolved(e, false)),
    ("a resend answered Duplicate", |e| answered(e, "dup")),
    ("a write at a taken position", |e| answered(e, "taken")),
    ("an early write", |e| answered(e, "early")),
    ("a fenced write", |e| answered(e, "fenced")),
    ("a call of the wrong mode", |e| answered(e, "wrong")),
    ("a write below the floor", |e| answered(e, "trunc")),
    ("a refused SetLeader", |e| answered(e, "refused")),
    ("a Noop", |e| answered(e, "noop")),
    ("a SetLeader to a multi-writer mode", |events| {
        events.iter().any(|e| {
            matches!(e, FoldEvent::Apply { command: Command::SetLeader { new: Leader::Multi(_), .. }, answer, .. } if *answer == "ok")
        })
    }),
    ("a SetLeader from multi-writer", |events| {
        events.iter().any(|e| {
            matches!(e, FoldEvent::Apply { command: Command::SetLeader { old: Leader::Multi(_), .. }, answer, .. } if *answer == "ok")
        })
    }),
    (
        "a SetLeader that names another multi-writer mode, refused",
        |events| {
            events.iter().any(|e| {
            matches!(e, FoldEvent::Apply { command: Command::SetLeader { old: old @ Leader::Multi(_), .. }, answer, state }
                if *answer == "refused" && state.0.is_multi() && state.0 != *old)
        })
        },
    ),
    ("a truncate that raised the floor", |events| {
        events.iter().any(|e| {
            matches!(e, FoldEvent::Apply { command: Command::Truncate { .. }, answer, state, .. } if *answer == "ok" && state.2 > 0)
        })
    }),
    ("an unfenced write accepted", |events| {
        events.iter().any(|e| {
            matches!(e, FoldEvent::Apply { command: Command::Write(w), answer, .. } if *answer == "ok" && w.fence == Fence::Open)
        })
    }),
    ("two seals", |events| {
        events
            .iter()
            .filter(|e| matches!(e, FoldEvent::Seal { .. }))
            .count()
            >= 2
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<FoldEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before. Each with its trace.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let max_events = usize::try_from(knob("TRACE_MAX_EVENTS", 200)).expect("a size");
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= FOLD_SEEDS {
            break;
        }
        let (fresh, events) = replay(FoldMutant::None, seed).expect("the walk passes");
        if events.len() > max_events {
            continue;
        }
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= FOLD_SEEDS {
            continue;
        }
        let traced = trace(fresh, &events);
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the fold walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_fold_walks() {
    let check = fold_spec();
    require_tlc(&check);
    // The walks pass their oracles: the campaign owns the assertion table.
    try_campaign(
        &fold_walk(FoldMutant::None).seeds(knob("TRACE_SEARCH", 300)),
        &[],
    )
    .expect("the walks pass");
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("fold seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "fold seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => panic!("fold seed {seed}: {other:?}"),
        }
    }
    assert!(unmet.is_empty(), "fold: no walk meets {unmet:?}");
}

/// The driver answers `Below` for a write taken in the segment in force.
/// The oracles pass it (`tests/mutants.rs`); TLC refuses the answer.
#[test]
fn tlc_refuses_a_below_in_the_segment_in_force() {
    let check = fold_spec();
    require_tlc(&check);
    let refused = (1..=100).find_map(|seed| {
        let (fresh, events) = replay(FoldMutant::BelowInSegment, seed)?;
        match check
            .check(&trace(fresh, &events))
            .expect("TLC gives a verdict")
        {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(&event, FoldEvent::Apply { answer, .. } if *answer == "below"),
        "seed {seed}: TLC refused {event:?}, not a Below"
    );
}

/// Every edit finds a place in a walk, and TLC refuses it at or after the
/// edited event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_fold_traces() {
    let check = fold_spec();
    require_tlc(&check);
    let (walks, _) = checked();
    let recorded: Vec<&Trace> = walks.iter().map(|(_, _, traced)| traced).collect();
    let from = walks
        .iter()
        .map(|(seed, ..)| seed + 1)
        .max()
        .expect("a checked walk");
    mutate::judge(&check, mutate::edits(), &recorded, from, |seed| {
        let (fresh, events) = replay(FoldMutant::None, seed).expect("the walk passes");
        trace(fresh, &events)
    });
}
