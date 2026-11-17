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
            let lanes = run.lanes.as_ref().unwrap_or(&trace.lanes);
            for step in &run.steps {
                assert_eq!(step.state.len(), lanes.len(), "one state per lane");
                for lane in [&step.from, &step.to].into_iter().flatten() {
                    assert!(
                        lanes.iter().any(|known| &known.id == lane),
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
                run.lanes.as_ref().unwrap_or(&trace.lanes).len(),
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

// ---- the quorum and the log (#947 (quorum and log in the step-through)) ----

#[test]
fn a_tally_says_when_the_core_has_a_quorum() {
    let trace = page("multi-paxos");
    let base = run(&trace, "base");
    let first = base
        .steps
        .iter()
        .find_map(|step| step.tally.as_ref())
        .expect("a tally");
    assert_eq!(first.what, "promises");
    assert_eq!(first.need, "2 of 3");
    // Node 0 counts its own promise and node 1's: a majority.
    assert_eq!(first.members, ["n0", "n1"]);
    assert!(first.quorum);
    let end = base.steps.last().unwrap();
    assert!(
        end.logs.iter().all(|log| log
            .iter()
            .map(|cell| cell.value.as_str())
            .eq(["alpha", "bravo"])),
        "every node holds both slots"
    );
    assert!(
        end.state.iter().all(|lines| lines.len() == 1),
        "a node's slots are in its log strip, not in its lines"
    );
}

#[test]
fn the_grid_counts_every_column_for_phase_1_and_lights_the_column_of_phase_2() {
    let trace = page("quorums");
    let grid = trace.grid.expect("the quorums page runs a grid");
    assert_eq!((grid.rows, grid.cols), (2, 3));
    assert!(trace.lanes.iter().all(|lane| lane.cell.is_some()));
    let base = run(&trace, "base");
    let tallies: Vec<_> = base.steps.iter().filter_map(|s| s.tally.as_ref()).collect();
    let elected = tallies
        .iter()
        .find(|tally| tally.quorum && tally.what == "promises")
        .expect("an election");
    // One acceptor of each of the three columns: no single line.
    assert_eq!(elected.need, "one acceptor of every column");
    assert_eq!(elected.line, None);
    assert_eq!(elected.members, ["n0", "n1", "n2"]);
    let slot0 = tallies
        .iter()
        .find(|tally| tally.quorum && tally.slot == Some(0))
        .expect("slot 0 is chosen");
    let line = slot0.line.as_ref().expect("a full column");
    assert_eq!((line.axis.as_str(), line.index), ("column", 0));
}

#[test]
fn a_single_decree_commit_names_the_proposer() {
    let trace = page("single-decree");
    let commit = run(&trace, "base")
        .steps
        .iter()
        .filter_map(|step| step.stage.as_ref())
        .find(|stage| stage.title == "Commit")
        .expect("a commit");
    assert!(commit.about.contains("proposer"), "{}", commit.about);
    assert!(!commit.about.contains("leader"), "{}", commit.about);
}

/// The lanes a run's message steps touch.
fn touched(run: &paros_wasm::paxos::trace::Scenario) -> std::collections::BTreeSet<String> {
    run.steps
        .iter()
        .flat_map(|step| [step.from.clone(), step.to.clone()])
        .flatten()
        .collect()
}

#[test]
fn the_compartmentalized_page_moves_the_work_off_the_leader() {
    let trace = page("compartmentalized");
    // Proxy leaders send the Accepts, by slot: slot 0 to proxy 0, slot 1 to proxy 1.
    let base = run(&trace, "base");
    for (proxy, slot) in [("p0", 0), ("p1", 1)] {
        assert!(base.steps.iter().any(|step| {
            step.from.as_deref() == Some(proxy)
                && step.label.starts_with("Accept ")
                && step.label.ends_with(&format!("slot {slot}"))
        }));
    }
    // The leader sends no Accept to an acceptor and counts no vote.
    assert!(!base.steps.iter().any(|step| {
        step.from.as_deref() == Some("n0")
            && step.label.starts_with("Accept ")
            && step.to.as_deref() != Some("p0")
            && step.to.as_deref() != Some("p1")
    }));
    assert!(base.steps.iter().all(|step| {
        step.tally
            .as_ref()
            .is_none_or(|tally| tally.at.starts_with('p'))
    }));
    // A replica learns the log and votes on nothing.
    assert!(
        base.steps
            .iter()
            .any(|step| step.to.as_deref() == Some("r0") && step.label.starts_with("Commit"))
    );
    assert!(
        !base
            .steps
            .iter()
            .any(|step| step.from.as_deref() == Some("r0"))
    );
    // Before: the same writes, the leader does it all.
    let before = touched(run(&trace, "colocated"));
    assert!(
        before.iter().all(|lane| lane.starts_with('n')),
        "{before:?}"
    );
    // Two writers, one slot.
    let batcher = run(&trace, "batcher");
    assert_eq!(
        batcher
            .steps
            .iter()
            .filter(|s| s.from.as_deref() == Some("b0"))
            .count(),
        1
    );
    assert_eq!(
        batcher.steps.last().expect("steps").chosen,
        ["slot 0 = alpha, bravo"]
    );
    // A read on a replica, served with no leader.
    let read = run(&trace, "read");
    assert!(
        read.steps
            .iter()
            .any(|step| step.from.as_deref() == Some("r1") && step.label == "PreRead")
    );
    assert!(
        read.steps
            .iter()
            .any(|step| step.says.contains("serves the read"))
    );
    // A dead proxy loses its slot's Accept; the leader takes the slot back.
    let crash = run(&trace, "proxy-crash");
    assert!(
        crash
            .steps
            .iter()
            .any(|step| step.kind == "drop" && step.to.as_deref() == Some("p1"))
    );
    assert!(
        crash
            .steps
            .last()
            .expect("steps")
            .chosen
            .contains(&"slot 1 = bravo".to_string())
    );
    // The handoff keeps its own run, on its own lanes.
    assert!(run(&trace, "handoff").lanes.is_some());
}

// ---- what the story mode reads (#1146 (recorder for the animated chapters)) ----

#[test]
fn every_step_names_its_message_on_the_wire_before_it() {
    for trace in traces() {
        for run in &trace.scenarios {
            let lanes = run.lanes.as_ref().unwrap_or(&trace.lanes);
            assert_eq!(
                run.start_nodes.len(),
                lanes.len(),
                "{}/{}",
                trace.page,
                run.id
            );
            let mut wire = run.start_wire.clone();
            for (n, step) in run.steps.iter().enumerate() {
                let at = format!("{}/{} step {}", trace.page, run.id, n + 1);
                assert_eq!(step.nodes.len(), lanes.len(), "{at}: one state per lane");
                if let Some(id) = step.message {
                    let flight = wire
                        .iter()
                        .find(|flight| flight.id == id)
                        .unwrap_or_else(|| panic!("{at}: message {id} was on the wire"));
                    assert_eq!(Some(&flight.from), step.from.as_ref(), "{at}");
                    // A copy step leaves the message on the wire and adds a copy.
                    assert!(
                        step.kind == "copy" || !step.wire.iter().any(|flight| flight.id == id),
                        "{at}: a delivered message leaves the wire"
                    );
                }
                for flight in &step.wire {
                    assert!(
                        flight.fan <= flight.id,
                        "{at}: a broadcast opens at its first message"
                    );
                    for lane in [&flight.from, &flight.to] {
                        assert!(lanes.iter().any(|known| &known.id == lane), "{at}: {lane}");
                    }
                }
                wire.clone_from(&step.wire);
            }
        }
    }
}

#[test]
fn a_broadcast_leaves_as_one_and_arrives_one_by_one() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let opened = &base.steps[0];
    assert_eq!(opened.kind, "event");
    let prepares: Vec<_> = opened
        .wire
        .iter()
        .filter(|flight| flight.label == "Prepare 1.5")
        .collect();
    assert_eq!(prepares.len(), 3, "one Prepare to each acceptor");
    assert!(prepares.iter().all(|flight| flight.fan == prepares[0].fan));
    // The first Prepare arrives; the two others are still in flight.
    let first = &base.steps[1];
    assert_eq!(first.message, Some(prepares[0].id));
    assert_eq!(
        first
            .wire
            .iter()
            .filter(|flight| flight.fan == prepares[0].fan)
            .count(),
        2
    );
    // The leader of the compartmentalized page sends one Accept to a proxy
    // leader, which sends it to the three acceptors as one broadcast.
    let trace = page("compartmentalized");
    let base = run(&trace, "base");
    let to_acceptors = base
        .steps
        .iter()
        .find(|step| {
            step.wire
                .iter()
                .filter(|flight| flight.from == "p0" && flight.label.starts_with("Accept "))
                .count()
                == 3
        })
        .expect("proxy leader 0 sends three Accepts");
    let fans: std::collections::BTreeSet<u64> = to_acceptors
        .wire
        .iter()
        .filter(|flight| flight.from == "p0")
        .map(|flight| flight.fan)
        .collect();
    assert_eq!(fans.len(), 1, "one broadcast");
}

#[test]
fn an_acceptor_writes_its_promise_and_its_vote_at_the_step_that_makes_them() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let a1 = trace
        .lanes
        .iter()
        .position(|lane| lane.id == "n1")
        .expect("acceptor 1");
    let writes: Vec<(&str, &[String])> = base
        .steps
        .iter()
        .filter(|step| !step.nodes[a1].writes.is_empty())
        .map(|step| (step.label.as_str(), step.nodes[a1].writes.as_slice()))
        .collect();
    assert_eq!(
        writes,
        [
            ("Prepare 1.5", &["promise 1.5".to_string()][..]),
            ("Accept 1.5", &["vote alpha at 1.5".to_string()][..]),
        ]
    );
    let end = &base.steps.last().expect("steps").nodes[a1];
    assert_eq!(end.promise.as_deref(), Some("1.5"));
    let vote = end.vote.as_ref().expect("a vote");
    assert_eq!(
        (vote.value.as_str(), vote.ballot.as_str()),
        ("alpha", "1.5")
    );
    // A crashed acceptor is drawn dark, and its restart writes nothing.
    let restart = run(&trace, "acceptor-restart");
    let crashed = restart
        .steps
        .iter()
        .find(|step| step.label == "acceptor 1 crashes")
        .expect("a crash");
    assert!(!crashed.nodes[a1].alive);
    let back = restart
        .steps
        .iter()
        .find(|step| step.label == "acceptor 1 restarts")
        .expect("a restart");
    assert!(back.nodes[a1].alive && back.nodes[a1].writes.is_empty());
}

#[test]
fn a_log_node_has_a_role_and_shows_its_holes() {
    let trace = page("multi-paxos");
    let base = run(&trace, "base");
    let end = base.steps.last().expect("steps");
    assert_eq!(end.nodes[0].role.as_deref(), Some("leader"));
    assert!(
        end.nodes[1..]
            .iter()
            .all(|node| node.role.as_deref() == Some("follower"))
    );
    // The gap run loses the Accepts of slot 1 while slot 2 is chosen: the
    // followers hold a hole until the next leader fills it with a no-op.
    let gap = run(&trace, "gap");
    assert!(
        gap.steps
            .iter()
            .any(|step| step.nodes.iter().any(|node| !node.holes.is_empty())),
        "a lost Accept leaves a hole"
    );
}

// ---- the single-decree chapter (#1148 (single-decree chapter)) ----

#[test]
fn every_single_decree_run_is_a_story_that_leads_back_to_the_choices() {
    let trace = page("single-decree");
    let base = run(&trace, "base");
    let turn = base.beats.last().expect("the base run has beats");
    let opened: Vec<&str> = turn.choices.iter().map(|c| c.run.as_str()).collect();
    let others: Vec<&str> = trace.scenarios[1..].iter().map(|r| r.id.as_str()).collect();
    assert_eq!(opened, others, "the last screen offers every other run");
    for run in &trace.scenarios[1..] {
        assert!(run.beats.len() >= 3, "{}: a story", run.id);
        let last = run.beats.last().expect("beats");
        assert_eq!(
            last.to,
            run.steps.len(),
            "{}: the story tells every step",
            run.id
        );
        assert!(
            last.choices
                .iter()
                .any(|c| c.run == "base" && c.beat == base.beats.len() - 1),
            "{}: a way back to the choices",
            run.id
        );
    }
}

#[test]
fn an_accept_is_also_a_promise() {
    let trace = page("single-decree");
    let run = run(&trace, "accept-is-a-promise");
    let a3 = trace
        .lanes
        .iter()
        .position(|lane| lane.id == "n3")
        .expect("acceptor 3");
    assert!(
        !run.steps
            .iter()
            .any(|step| step.label.starts_with("Prepare") && step.to.as_deref() == Some("n3")),
        "acceptor 3 never gets a Prepare"
    );
    let accept = run
        .steps
        .iter()
        .find(|step| step.label == "Accept 1.5" && step.to.as_deref() == Some("n3"))
        .expect("acceptor 3 gets the Accept");
    assert_eq!(
        accept.nodes[a3].writes,
        ["promise 1.5", "vote alpha at 1.5"]
    );
}

#[test]
fn a_copy_and_an_old_answer_count_for_nothing() {
    let trace = page("single-decree");
    let run = run(&trace, "repeated-answers");
    let copy = run
        .steps
        .iter()
        .position(|step| step.kind == "copy")
        .expect("a copy");
    let promises: Vec<_> = run.steps[copy + 1..]
        .iter()
        .filter(|step| step.label == "Promise 1.5" && step.from.as_deref() == Some("n1"))
        .collect();
    assert_eq!(promises.len(), 2, "both copies arrive");
    assert!(
        promises[1].says.contains("ignores this copy"),
        "{}",
        promises[1].says
    );
    assert_eq!(promises[1].tally.as_ref().map(|t| t.members.len()), Some(1));
    assert!(
        run.steps
            .iter()
            .any(|step| step.says.contains("It ignores the old answer.")),
        "an answer for an old ballot is ignored"
    );
    assert_eq!(run.steps.last().unwrap().chosen, ["alpha"]);
}

#[test]
fn a_second_proposer_that_misses_the_only_vote_chooses_its_own_value() {
    let trace = page("single-decree");
    let run = run(&trace, "fenced-out");
    assert_eq!(run.steps.last().unwrap().chosen, ["bravo"]);
    let late: Vec<_> = run
        .steps
        .iter()
        .filter(|step| step.label == "Accept 1.5" && step.to.as_deref() != Some("n1"))
        .collect();
    assert_eq!(late.len(), 2);
    assert!(late.iter().all(|step| step.says.contains("does not vote")));
    assert!(
        run.steps.last().unwrap().broken.is_none(),
        "one value is chosen"
    );
}
