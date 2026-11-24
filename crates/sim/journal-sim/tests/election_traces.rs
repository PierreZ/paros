//! Model-based trace checking of the election walk: TLC checks that each
//! checked walk is a behaviour of `specs/tla/election/ElectionTrace.tla`
//! (the actions of `Election.tla`), refuses the trace control, and refuses
//! every edit of the recorded walks at or after the edited event.

use journal_sim::campaign::try_campaign;
use journal_sim::election::{ElectionEvent, ElectionMutant, walk};
use journal_sim::trace::election::{EDITS, election_spec, trace};
use journal_sim::trace::{Trace, Verdict, mutate, require_tlc};

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The election walks TLC always checks.
const ELECTION_SEEDS: u64 = 10;

/// One traced walk: its trace and its events.
fn replay(mutant: ElectionMutant, seed: u64) -> Option<(Trace, Vec<ElectionEvent>)> {
    let run = walk(mutant, 1, knob("TRACE_CHAOS", 300))
        .replay(seed)
        .ok()?;
    let events: Vec<ElectionEvent> = run.events().cloned().collect();
    Some((trace(&events), events))
}

/// A rule of `ElectionTrace`: its name, and whether a walk meets it.
type Rule = (&'static str, fn(&[ElectionEvent]) -> bool);

fn has(events: &[ElectionEvent], pick: fn(&ElectionEvent) -> bool) -> bool {
    events.iter().any(pick)
}

/// The rules of `ElectionTrace`: the steps some checked walk must take.
const RULES: &[Rule] = &[
    ("a claim lost the compare-and-set", |e| {
        has(e, |e| {
            matches!(e, ElectionEvent::CampaignAnswer { won: false, .. })
        })
    }),
    ("a claim won after a resend", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, ElectionEvent::Retry { a }
                if events[at + 1..].iter().find(|later| matches!(later,
                    ElectionEvent::CampaignAnswer { a: b, .. } | ElectionEvent::HandAnswer { a: b }
                        if b == a))
                    .is_some_and(|later| matches!(later,
                        ElectionEvent::CampaignAnswer { won: true, .. })))
        })
    }),
    ("a third term", |e| {
        has(
            e,
            |e| matches!(e, ElectionEvent::CampaignAnswer { won: true, t, .. } | ElectionEvent::Adopt { took: true, t, .. } if *t >= 3),
        )
    }),
    ("a refused write", |e| {
        has(e, |e| {
            matches!(e, ElectionEvent::WriteAnswer { ok: false, .. })
        })
    }),
    ("a handed term adopted", |e| {
        has(e, |e| matches!(e, ElectionEvent::Adopt { took: true, .. }))
    }),
    ("a handed term refused", |e| {
        has(e, |e| matches!(e, ElectionEvent::Adopt { took: false, .. }))
    }),
    ("a write given up", |e| {
        has(e, |e| matches!(e, ElectionEvent::Timeout { .. }))
    }),
    ("a crash", |e| {
        has(e, |e| matches!(e, ElectionEvent::Crash { .. }))
    }),
    ("a multi-writer journal", |e| {
        has(e, |e| matches!(e, ElectionEvent::UserMulti))
    }),
    ("a lost answer", |e| {
        has(e, |e| {
            matches!(
                e,
                ElectionEvent::Lose {
                    kind: "slans" | "wans",
                    ..
                }
            )
        })
    }),
    ("a call of a stopped incarnation applied", |events| {
        events.iter().enumerate().any(|(at, e)| {
            matches!(e, ElectionEvent::Crash { a }
                if events[..at].iter().any(|before| matches!(before,
                    ElectionEvent::Challenge { a: b, .. } | ElectionEvent::Renew { a: b }
                        if b == a))
                    && events[at + 1..].iter().any(|later| matches!(later,
                        ElectionEvent::ApplySetLeader { a: b, inc: 1, .. }
                        | ElectionEvent::ApplyWrite { a: b, inc: 1, .. } if b == a)))
        })
    }),
];

/// A checked walk: its seed, its events and its trace.
type Checked = (u64, Vec<ElectionEvent>, Trace);

/// The checked walks: the first seeds, and each seed that meets a rule no
/// checked walk met before.
fn checked() -> (Vec<Checked>, Vec<&'static str>) {
    let mut unmet: Vec<&Rule> = RULES.iter().collect();
    let mut walks = Vec::new();
    let first = knob("TRACE_FROM", 1);
    for seed in first..first + knob("TRACE_SEARCH", 300) {
        if unmet.is_empty() && walks.len() as u64 >= ELECTION_SEEDS {
            break;
        }
        let (traced, events) = replay(ElectionMutant::None, seed).expect("the walk passes");
        let before = unmet.len();
        unmet.retain(|(_, met)| !met(&events));
        if unmet.len() == before && walks.len() as u64 >= ELECTION_SEEDS {
            continue;
        }
        walks.push((seed, events, traced));
    }
    (walks, unmet.iter().map(|(name, _)| *name).collect())
}

/// TLC accepts the election walks, and the checked walks meet every rule.
#[test]
fn tlc_accepts_the_election_walks() {
    let check = election_spec();
    require_tlc(&check);
    try_campaign(
        &walk(
            ElectionMutant::None,
            knob("TRACE_SEARCH", 300),
            knob("TRACE_CHAOS", 300),
        ),
        &[],
    )
    .expect("the walks pass");
    let (walks, unmet) = checked();
    for (seed, events, traced) in &walks {
        eprintln!("election seed {seed}: {} events", events.len());
        match check.check(traced).expect("TLC gives a verdict") {
            Verdict::Accepted => {}
            Verdict::Rejected(rejection) => panic!(
                "election seed {seed}: TLC refused event {} {:?}\n{}",
                rejection.index, events[rejection.index], rejection.state
            ),
            other @ Verdict::InvariantViolated { .. } => {
                panic!("election seed {seed}: {other:?}")
            }
        }
    }
    assert!(unmet.is_empty(), "election: no walk meets {unmet:?}");
}

/// The program hands a term on with a number one too high. The oracles pass
/// it (`the_oracles_pass_a_skipped_hand_number` in `tests/election.rs`); TLC
/// refuses the adoption.
#[test]
fn tlc_refuses_a_skipped_hand_number() {
    let check = election_spec();
    require_tlc(&check);
    let refused = (1..=100).find_map(|seed| {
        let (traced, events) = replay(ElectionMutant::HandSkip, seed)?;
        match check.check(&traced).expect("TLC gives a verdict") {
            Verdict::Rejected(rejection) => Some((seed, events[rejection.index].clone())),
            _ => None,
        }
    });
    let (seed, event) = refused.expect("TLC refuses a seed of the mutant");
    eprintln!("seed {seed}: TLC refused {event:?}");
    assert!(
        matches!(event, ElectionEvent::Adopt { .. }),
        "seed {seed}: TLC refused {event:?}, not the adoption"
    );
}

/// Every edit finds a place in a walk, and TLC refuses it at or after the
/// edited event (#1308 (mutate recorded traces)).
#[test]
fn tlc_refuses_the_edited_election_traces() {
    let check = election_spec();
    require_tlc(&check);
    let (walks, _) = checked();
    let recorded: Vec<&Trace> = walks.iter().map(|(_, _, traced)| traced).collect();
    let from = walks
        .iter()
        .map(|(seed, ..)| seed + 1)
        .max()
        .expect("a checked walk");
    mutate::judge(&check, EDITS, &recorded, from, |seed| {
        replay(ElectionMutant::None, seed)
            .expect("the walk passes")
            .0
    });
}
