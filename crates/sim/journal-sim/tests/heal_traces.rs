//! Model-based trace checking of the heal walk (#1314 (re-replication of a
//! sealed segment's replica set)): TLC checks that each checked walk is a
//! behaviour of `specs/tla/heal-reuse/HealTrace.tla` (the actions of
//! `HealReuse.tla`), refuses the trace control, and refuses every edit of
//! the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::heal::{HealEvent, HealMutant, traced};
use journal_sim::trace::heal::{EDITS, heal_spec, trace};
use journal_sim::trace::{Trace, Verdict, mutate, require_tlc};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The heal walks TLC always checks.
const HEAL_SEEDS: u64 = 10;

/// One traced walk: its trace and its events.
fn replay(mutant: HealMutant, seed: u64) -> Option<(Trace, Vec<HealEvent>)> {
    let run = traced(mutant, 1, knob("TRACE_CHAOS", 300))
        .replay(seed)
        .ok()?;
    let events: Vec<HealEvent> = run.events().cloned().collect();
    Some((trace(&events), events))
}

/// A rule of `HealTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[HealEvent]) -> bool);

fn has(events: &[HealEvent], pick: fn(&HealEvent) -> bool) -> bool {
    events.iter().any(pick)
}

/// The rules of `HealTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a heal written", |e| {
        has(e, |e| matches!(e, HealEvent::Answer(_, true)))
    }),
    ("a heal refused", |e| {
        has(e, |e| matches!(e, HealEvent::Answer(_, false)))
    }),
    ("a compare-and-set answered twice", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, HealEvent::Apply { .. })
                && events[at + 1..].iter().any(|later| match (e, later) {
                    (
                        HealEvent::Apply {
                            term,
                            read,
                            heals,
                            remove,
                            add,
                            ..
                        },
                        HealEvent::Apply {
                            term: t,
                            read: r,
                            heals: h,
                            remove: d,
                            add: n,
                            ..
                        },
                    ) => (term, read, heals, remove, add) == (t, r, h, d, n),
                    _ => false,
                })
        })
    }),
    ("a set came back to the first set", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, HealEvent::Apply { ok: true, set, .. } if set.iter().any(|n| n.0 >= 20))
                && events[at + 1..].iter().any(|later| {
                    matches!(later, HealEvent::Apply { ok: true, set, .. }
                        if set.iter().all(|n| n.0 < 20))
                })
        })
    }),
    ("a view showed a heal done", |e| {
        has(e, |e| matches!(e, HealEvent::Done(_)))
    }),
    ("a heal ended with no write", |e| {
        has(e, |e| matches!(e, HealEvent::Abort(_)))
    }),
    ("an abandon ended a copy", |e| {
        has(e, |e| matches!(e, HealEvent::Abandon(_)))
    }),
    ("an orphan copy ended", |e| {
        has(e, |e| matches!(e, HealEvent::Ends(_)))
    }),
    ("a removed member dropped its copy", |e| {
        has(e, |e| matches!(e, HealEvent::Drop(n) if n.0 < 20))
    }),
    ("a node took a higher copy over its copy", |events| {
        events.iter().enumerate().any(|(at, e)| {
            let HealEvent::Join(n, first) = e else {
                return false;
            };
            events[at + 1..]
                .iter()
                .take_while(
                    |later| !matches!(later, HealEvent::Abandon(m) | HealEvent::Ends(m) if m == n),
                )
                .any(
                    |later| matches!(later, HealEvent::Join(m, second) if m == n && second > first),
                )
        })
    }),
    ("two terms won", |e| {
        e.iter()
            .filter(|e| matches!(e, HealEvent::Elect(_)))
            .count()
            >= 2
    }),
    ("a controller crashed", |e| {
        has(e, |e| matches!(e, HealEvent::CtlCrash(_)))
    }),
    ("a machine came back", |e| {
        has(e, |e| matches!(e, HealEvent::Restart(_)))
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<HealEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= HEAL_SEEDS {
            break;
        }
        let (traced, events) = replay(HealMutant::None, seed).expect("the walk passes");
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= HEAL_SEEDS {
            continue;
        }
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the heal walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_heal_walks() {
    let check = heal_spec();
    require_tlc(&check);
    try_campaign(
        &traced(
            HealMutant::None,
            knob("TRACE_SEARCH", 300),
            knob("TRACE_CHAOS", 300),
        ),
        &[],
    )
    .expect("the walks pass");
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("heal seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "heal seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => {
                panic!("heal seed {seed}: {other:?}")
            }
        }
    }
    assert!(unmet.is_empty(), "heal: no walk meets {unmet:?}");
}

/// A node takes the ask of the copy it runs again. The oracles pass it
/// (`the_oracles_pass_a_copy_started_again`); TLC refuses the join.
#[test]
fn tlc_refuses_a_copy_started_again() {
    let check = heal_spec();
    require_tlc(&check);
    let refused = (1..=300).find_map(|seed| {
        let (traced, events) = replay(HealMutant::SameCopyAgain, seed)?;
        match check.check(&traced).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, HealEvent::Join(..)),
        "seed {seed}: TLC refused {event:?}, not the join"
    );
}

/// The trace control passes every oracle.
#[test]
fn the_oracles_pass_a_copy_started_again() {
    try_campaign(&traced(HealMutant::SameCopyAgain, 3000, 300), &[])
        .expect("the oracles pass the trace control");
}

/// Every edit finds a place in a walk, and TLC refuses it at or after the
/// edited event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_heal_traces() {
    let check = heal_spec();
    require_tlc(&check);
    let (walks, _) = checked();
    let recorded: Vec<&Trace> = walks.iter().map(|(_, _, traced)| traced).collect();
    let from = walks
        .iter()
        .map(|(seed, ..)| seed + 1)
        .max()
        .expect("a checked walk");
    mutate::judge(&check, EDITS, &recorded, from, |seed| {
        replay(HealMutant::None, seed).expect("the walk passes").0
    });
}
