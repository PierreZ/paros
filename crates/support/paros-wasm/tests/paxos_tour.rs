//! The rules of a tour run (tour plan §3.2): every run that the recorder
//! marks for the tour of the Paxos flavors (`Recorder::tour`) has 10 to 30
//! beats, one sentence of at most 140 characters each so that the stage,
//! the sentence and the player fit one phone screen, no question (the tour is
//! for watching), and only lanes of its world in `enter`. The site checks the
//! other half at build time (`website/site/src/tour.ts`): every chapter
//! names runs that exist, and a tour run is in one chapter.

use paros_wasm::paxos::trace::traces;
use paros_wasm::steps::story::{TOUR_SAYS, Tour, tour_problems};

#[test]
fn every_tour_run_keeps_the_rules_of_the_tour() {
    for trace in traces() {
        assert_eq!(
            tour_problems(&trace),
            Vec::<String>::new(),
            "{}",
            trace.page
        );
    }
}

/// A run of the single-decree page that has a question, marked for the
/// tour: each rule that it breaks is named.
#[test]
fn the_check_names_each_broken_rule() {
    let mut trace = traces()
        .into_iter()
        .find(|trace| trace.page == "single-decree")
        .expect("the single-decree page");
    let asked = trace
        .scenarios
        .iter()
        .position(|run| run.beats.iter().any(|beat| beat.question.is_some()))
        .expect("a run that asks");
    trace.scenarios.truncate(asked + 1);
    trace.scenarios.drain(..asked);
    let run = &mut trace.scenarios[0];
    run.tour = Some(Tour {
        chapter: "one-value".to_string(),
        title: "One value".to_string(),
    });
    run.beats[0].says = "a".repeat(TOUR_SAYS + 1);
    run.beats[0].enter.push("n99".to_string());
    let problems = tour_problems(&trace).join("\n");
    assert!(problems.contains("beats, not 10 to 30"), "{problems}");
    assert!(problems.contains("characters, more than 140"), "{problems}");
    assert!(problems.contains("a question"), "{problems}");
    assert!(
        problems.contains("n99 enters, but is no lane"),
        "{problems}"
    );
}
