//! Write `website/site/src/traces/` from the engine (#811 (Paxos flavor
//! pages)).
//!
//! Each page of `/paxos/` draws a step-through diagram from one JSON trace:
//! a recorded run of the real `paros-core` through the game's engine. The
//! files are committed and CI diffs them, so this test is the only way they
//! are produced: change a run or the engine, run
//! `cargo test -p paros-wasm --test paxos_traces`, commit what changed.

use std::path::PathBuf;

use paros_wasm::paxos::trace::{trace_of, traces};

fn out_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../website/site/src/traces")
        .canonicalize()
        .expect("website/site/src/traces exists and is committed")
}

#[test]
fn traces_are_written() {
    for trace in traces() {
        assert!(!trace.scenarios.is_empty(), "{} has a base run", trace.page);
        for run in &trace.scenarios {
            assert!(!run.steps.is_empty(), "{}/{} has steps", trace.page, run.id);
            for step in &run.steps {
                assert_eq!(step.state.len(), trace.lanes.len(), "one state per lane");
                for lane in [&step.from, &step.to].into_iter().flatten() {
                    assert!(
                        trace.lanes.iter().any(|known| &known.id == lane),
                        "{}/{}: {lane} is a lane",
                        trace.page,
                        run.id
                    );
                }
            }
        }
        let mut json = serde_json::to_string_pretty(&trace).expect("a trace serializes");
        json.push('\n');
        std::fs::write(out_dir().join(format!("{}.json", trace.page)), json)
            .expect("the trace is written");
    }
}

#[test]
fn traces_are_reproducible() {
    assert_eq!(traces(), traces(), "a trace draws no randomness");
}

#[test]
fn a_page_is_recorded_alone_as_in_the_whole_set() {
    // The browser records one page (`paxos_trace`); the committed files come
    // from the whole set. They must be the same run.
    for trace in traces() {
        assert_eq!(
            trace_of(&trace.page).as_ref(),
            Some(&trace),
            "{}",
            trace.page
        );
    }
    assert_eq!(trace_of("no-such-page"), None);
}

// ---- what the step-through says (Pierre, 2026-11-01: "I don't understand the flow") ----

#[test]
fn every_step_has_a_stage_a_sentence_and_lines() {
    for trace in traces() {
        for run in &trace.scenarios {
            assert_eq!(
                run.start.len(),
                trace.lanes.len(),
                "{}/{}: one start state per lane",
                trace.page,
                run.id
            );
            for (n, step) in run.steps.iter().enumerate() {
                let at = format!("{}/{} step {}", trace.page, run.id, n + 1);
                let stage = step
                    .stage
                    .as_ref()
                    .unwrap_or_else(|| panic!("{at} has a stage"));
                assert!(
                    !stage.title.is_empty() && !stage.about.is_empty(),
                    "{at}: the stage has words"
                );
                assert!(
                    step.says.ends_with('.'),
                    "{at}: one plain sentence, got {:?}",
                    step.says
                );
                assert!(
                    !step.says.contains("re-affirm"),
                    "{at}: no jargon in the sentence"
                );
                for lines in &step.state {
                    assert!(!lines.is_empty(), "{at}: every lane has a line");
                }
            }
        }
    }
}

fn page(name: &str) -> paros_wasm::paxos::trace::Trace {
    trace_of(name).expect("a page")
}

fn run<'a>(
    trace: &'a paros_wasm::paxos::trace::Trace,
    id: &str,
) -> &'a paros_wasm::paxos::trace::Scenario {
    trace
        .scenarios
        .iter()
        .find(|run| run.id == id)
        .expect("a run")
}

#[test]
fn the_classic_run_names_its_two_phases_in_order() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let mut titles: Vec<&str> = base
        .steps
        .iter()
        .map(|step| step.stage.as_ref().unwrap().title.as_str())
        .collect();
    titles.dedup();
    assert_eq!(
        titles,
        ["Phase 1 · ballot 1.5", "Phase 2 · ballot 1.5", "Commit"]
    );
}

#[test]
fn the_classic_run_does_not_use_the_second_proposer() {
    // The page draws only the lanes a run uses; the base run never talks to
    // proposer 8, the second-proposer run does.
    let trace = page("single-decree");
    let uses = |run: &paros_wasm::paxos::trace::Scenario, lane: &str| {
        run.steps
            .iter()
            .any(|step| step.from.as_deref() == Some(lane) || step.to.as_deref() == Some(lane))
    };
    assert!(!uses(run(&trace, "base"), "n8"));
    assert!(uses(run(&trace, "second-proposer"), "n8"));
}

#[test]
fn the_value_is_chosen_at_the_vote_that_makes_a_majority() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let first = base
        .steps
        .iter()
        .position(|step| !step.chosen.is_empty())
        .expect("alpha is chosen");
    let step = &base.steps[first];
    assert_eq!(step.label, "Accepted 1.5");
    assert_eq!(step.chosen, ["alpha"]);
    assert!(
        step.says
            .contains("A majority of the acceptors voted for it, so alpha is chosen."),
        "{}",
        step.says
    );
    let votes = base.steps[..=first]
        .iter()
        .filter(|step| step.label.starts_with("Accepted"))
        .count();
    assert_eq!(votes, 2, "2 of 3 votes choose the value");
    // A one-decree label carries no slot.
    assert!(base.steps.iter().all(|step| !step.label.contains("slot")));
}

#[test]
fn an_acceptor_card_shows_its_promise_and_its_vote() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let a1 = trace
        .lanes
        .iter()
        .position(|lane| lane.id == "n1")
        .expect("acceptor 1");
    assert_eq!(trace.lanes[a1].label, "acceptor 1");
    assert_eq!(base.start[a1], ["no promise", "no vote"]);
    let promised = base
        .steps
        .iter()
        .find(|step| step.label == "Prepare 1.5")
        .unwrap();
    assert_eq!(promised.state[a1], ["promise 1.5", "no vote"]);
    assert_eq!(
        base.steps.last().unwrap().state[a1],
        ["promise 1.5", "vote alpha", "at 1.5"]
    );
}

#[test]
fn the_second_proposer_learns_the_earlier_vote_from_a_promise() {
    let trace = page("single-decree");
    let second = run(&trace, "second-proposer");
    let promise = second
        .steps
        .iter()
        .find(|step| step.label == "Promise 1.8" && step.from.as_deref() == Some("n1"))
        .expect("acceptor 1 promises 1.8");
    assert!(
        promise
            .says
            .contains("carries a vote for alpha at ballot 1.5"),
        "{}",
        promise.says
    );
    let refused = second
        .steps
        .iter()
        .find(|step| {
            step.label == "Accept 1.5" && step.kind == "deliver" && step.to.as_deref() == Some("n2")
        })
        .expect("acceptor 2 gets the old Accept");
    assert!(refused.says.contains("does not vote"), "{}", refused.says);
    assert_eq!(second.steps.last().unwrap().chosen, ["alpha"]);
}

#[test]
fn a_log_names_the_slot_that_is_chosen() {
    let trace = page("multi-paxos");
    let base = run(&trace, "base");
    let end = base.steps.last().unwrap();
    assert_eq!(end.chosen, ["slot 0 = alpha", "slot 1 = bravo"]);
    let leader = base
        .steps
        .iter()
        .find(|step| step.says.contains("is now the leader"))
        .expect("an election");
    assert!(
        leader
            .says
            .contains("has promises from a majority of the nodes"),
        "{}",
        leader.says
    );
    let dropped = run(&trace, "leader-crash")
        .steps
        .iter()
        .find(|step| step.kind == "drop")
        .expect("a lost message");
    assert!(dropped.says.contains("is lost"), "{}", dropped.says);
}
