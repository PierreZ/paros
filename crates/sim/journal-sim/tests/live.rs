//! The live trace check of long fold walks (#1310 (check long runs live)):
//! the walk reports each event on its events port while it runs, and
//! moonpool-tla's `Live` checks the stream against
//! `specs/tla/journal-state/FoldTrace.tla` in chunks, each chunk starting
//! from every state the chunk before it ended in. A walk far longer than
//! the walks of `tests/traces.rs` is accepted, and the trace control
//! (`BelowInSegment`) is stopped at its first wrong answer, before the
//! walk ends.
//!
//! The knobs: `LIVE_STEPS` (the chaos steps of the walk), `LIVE_CHUNK`
//! (the events of one chunk), `LIVE_SEED`, and `LIVE_MINUTES`: one walk
//! with no end, checked live for that many minutes (the long run of the
//! `Journal live` workflow, `.github/workflows/journal-live.yml`).

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use journal_sim::fold::{FoldEvent, FoldModel, FoldMutant, Reported, STOPPED, Shape};
use journal_sim::trace::{Verdict, encode, fold_spec, require_tlc, start};
use moonpool_walk::Walk;

/// The value of the knob `name`, or `default`.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// The full chunks that may wait for TLC.
const BACKLOG: usize = 4;
/// The events the port holds before the walk waits.
const PORT: usize = 256;

/// A walk that runs on its own thread and reports on a port. Its result is
/// `Err` with the walk's failure, a [`STOPPED`] failure when the live check
/// stopped it.
struct Running {
    events: Receiver<Reported>,
    walk: JoinHandle<Result<(), String>>,
}

/// One fold walk of `seed`, `steps` chaos steps long, with `mutant`.
fn run(mutant: FoldMutant, seed: u64, steps: u64) -> Running {
    // A small port: the walk waits for the check, as a run waits for its
    // `Events` consumer.
    let (port, events) = sync_channel(PORT);
    let walk = std::thread::spawn(move || {
        Walk::<FoldModel>::new()
            .chaos_steps(steps)
            .env_prefix(None)
            .model(move |cx| FoldModel::build_reported(Shape::LONG, mutant, port.clone(), cx))
            .replay(seed)
            .map(|_| ())
            .map_err(|failure| format!("{failure}"))
    });
    Running { events, walk }
}

/// What the live check of one walk saw.
struct Checked {
    verdict: Verdict,
    /// How many events the walk reported to the check.
    reported: usize,
    /// The seals among them.
    seals: usize,
    /// The last events reported, from event `first` on: every event the
    /// check had not yet judged when it stopped is here.
    window: VecDeque<FoldEvent>,
    first: usize,
    /// The walk's result.
    walk: Result<(), String>,
}

impl Checked {
    /// The event at `index` of the stream, if the window still holds it.
    fn event(&self, index: usize) -> Option<&FoldEvent> {
        self.window.get(index.checked_sub(self.first)?)
    }
}

/// Check one walk live, in chunks of `chunk` events. At `deadline`, the
/// check stops reading: the walk stops, and the verdict is the verdict of
/// the events reported so far.
fn check_live(running: Running, chunk: usize, deadline: Option<Instant>) -> Checked {
    let Ok(Reported::Fresh(fresh)) = running.events.recv() else {
        panic!("a walk reports its fresh journal first");
    };
    let mut live = fold_spec()
        .live("TraceNext")
        .chunk(chunk)
        .backlog(BACKLOG)
        .start(&start(fresh))
        .expect("FoldTrace keeps the contract of a live check");
    // The events not yet judged: the chunks in the backlog, the chunk that
    // TLC checks, the chunk that fills, and the port.
    let keep = (BACKLOG + 2) * chunk + PORT;
    let mut window = VecDeque::with_capacity(keep + 1);
    let (mut reported, mut seals, mut first) = (0, 0, 0);
    for event in &running.events {
        let Reported::Event(event) = event else {
            panic!("a walk reports its fresh journal once");
        };
        let (name, fields) = encode(&event);
        reported += 1;
        seals += usize::from(matches!(event, FoldEvent::Seal { .. }));
        window.push_back(event);
        if window.len() > keep {
            window.pop_front();
            first += 1;
        }
        let late = deadline.is_some_and(|d| Instant::now() >= d);
        if !live.event(name, fields) || late {
            // Closing the port stops the walk.
            break;
        }
    }
    drop(running.events);
    let verdict = live.finish().expect("TLC gives a verdict");
    let walk = running.walk.join().expect("the walk thread returns");
    Checked {
        verdict,
        reported,
        seals,
        window,
        first,
        walk,
    }
}

/// The peak memory of the test process, as Linux reports it: a walk of
/// many hours keeps its log, so the nightly run prints it.
fn peak_memory() -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))
                .map(|line| {
                    line.split_whitespace()
                        .skip(1)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
        })
        .map_or_else(
            || "peak memory unknown".to_owned(),
            |peak| format!("peak memory {peak}"),
        )
}

/// TLC follows a long fold walk while it runs: the walk passes its
/// oracles, and the live check accepts every chunk. With `LIVE_MINUTES`,
/// one walk with no end runs for that long, and the check follows it.
#[test]
fn tlc_follows_a_long_fold_walk_live() {
    require_tlc(&fold_spec());
    let chunk = usize::try_from(knob("LIVE_CHUNK", 500)).expect("a size");
    let seed = knob("LIVE_SEED", 1);
    let minutes = knob("LIVE_MINUTES", 0);
    let (steps, deadline) = if minutes == 0 {
        (knob("LIVE_STEPS", 20_000), None)
    } else {
        (
            u64::MAX,
            Some(Instant::now() + Duration::from_secs(minutes * 60)),
        )
    };
    let started = Instant::now();
    let checked = check_live(run(FoldMutant::None, seed, steps), chunk, deadline);
    eprintln!(
        "fold seed {seed}: {} events, {} seals, checked live in {:?}; {}",
        checked.reported,
        checked.seals,
        started.elapsed(),
        peak_memory()
    );
    match &checked.walk {
        Ok(()) => assert!(deadline.is_none(), "a walk with no end ended"),
        // At the deadline the check closed the port, and the walk stopped.
        Err(failure) if deadline.is_some() && failure.contains(STOPPED) => {}
        Err(failure) => panic!("fold seed {seed}: the walk fails: {failure}"),
    }
    match checked.verdict {
        Verdict::Accepted => {}
        Verdict::Rejected(ref rejection) => panic!(
            "fold seed {seed}: TLC refused event {} {:?}\n{}",
            rejection.index,
            checked.event(rejection.index),
            rejection.state
        ),
        ref other @ Verdict::InvariantViolated { .. } => {
            panic!("fold seed {seed}: {other:?}")
        }
    }
    // A long walk: many chunks, many seals. A shorter walk would not test
    // the hand-off between chunks.
    assert!(
        checked.reported >= 8 * chunk,
        "fold seed {seed}: only {} events, the live check needs at least {}",
        checked.reported,
        8 * chunk
    );
    assert!(
        checked.seals >= 50,
        "fold seed {seed}: only {} seals",
        checked.seals
    );
}

/// The trace control, live: the driver answers `Below` for a write taken
/// in the segment in force. The oracles pass it; the live check refuses the
/// first such answer and stops the walk before its end.
#[test]
fn tlc_stops_a_below_in_the_segment_in_force_live() {
    require_tlc(&fold_spec());
    let steps = knob("LIVE_STEPS", 20_000);
    let chunk = usize::try_from(knob("LIVE_CHUNK", 500)).expect("a size");
    let seed = knob("LIVE_SEED", 1);
    let checked = check_live(run(FoldMutant::BelowInSegment, seed, steps), chunk, None);
    let Verdict::Rejected(rejection) = &checked.verdict else {
        panic!("seed {seed}: the live check did not refuse the mutant");
    };
    let event = checked
        .event(rejection.index)
        .expect("the window holds the events not yet judged");
    assert!(
        matches!(event, FoldEvent::Apply { answer, .. } if *answer == "below"),
        "seed {seed}: TLC refused {event:?}, not a Below"
    );
    // The first wrong answer: no earlier answer is a Below of the segment in
    // force (every earlier event was accepted).
    let walk = checked.walk.expect_err("the live check stops the walk");
    assert!(
        walk.contains(STOPPED),
        "seed {seed}: the walk failed: {walk}"
    );
    eprintln!(
        "seed {seed}: refused event {} of {} reported, the walk stopped",
        rejection.index, checked.reported
    );
}
