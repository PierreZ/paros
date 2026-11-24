use super::*;

fn run(id: &str) -> Scenario {
    trace()
        .scenarios
        .into_iter()
        .find(|run| run.id == id)
        .unwrap_or_else(|| panic!("the page has the run {id}"))
}

#[test]
fn every_run_tells_its_story_and_has_one_state_per_lane() {
    let trace = trace();
    let lanes = lanes();
    assert_eq!(trace.page, PAGE);
    assert_eq!(
        trace.scenarios.len(),
        3,
        "the seal, the slow acceptor, the rule broken"
    );
    for run in &trace.scenarios {
        assert!(!run.beats.is_empty(), "{} is told as a story", run.id);
        assert_eq!(
            run.start.len(),
            lanes.len(),
            "{}: one start state per lane",
            run.id
        );
        for step in &run.steps {
            assert_eq!(step.state.len(), lanes.len(), "one state per lane");
            assert_eq!(step.nodes.len(), lanes.len(), "one node per lane");
            assert!(step.stage.is_some(), "{}: every step has a stage", run.id);
            assert!(
                step.says.ends_with('.'),
                "{}: {:?} is a sentence",
                run.id,
                step.says
            );
            for lane in [&step.from, &step.to].into_iter().flatten() {
                assert!(
                    lanes.iter().any(|known| &known.id == lane),
                    "{lane} is a lane"
                );
            }
        }
    }
}

#[test]
fn the_seal_ends_one_above_the_vote_and_the_resend_is_below() {
    let base = run("base");
    let last = base.steps.last().expect("steps");
    let register = &last.state[2];
    assert!(
        register
            .iter()
            .any(|line| line.starts_with("segment 1: sealed") && line.contains("end 4")),
        "segment 1 ends at slot 4: {register:?}"
    );
    assert!(
        last.chosen.contains(&"slot 3 = w2".to_string()),
        "w2 is chosen at slot 3: {:?}",
        last.chosen
    );
    assert!(last.broken.is_none(), "the seal keeps one value per slot");
    let client = &last.state[0];
    assert!(
        client.contains(&"w2: Written at seq 2".to_string()),
        "Resolve finds w2 at seq 2: {client:?}"
    );
    assert!(
        base.steps
            .iter()
            .any(|step| step.label.starts_with("Reply · Below")),
        "the resend across the seal is answered Below"
    );
}

#[test]
fn a_phase_1_that_sees_no_vote_ends_the_segment_at_its_start() {
    let slow = run("slow-acceptor");
    let last = slow.steps.last().expect("steps");
    assert!(
        last.state[2].iter().any(|line| line.contains("end 3")),
        "segment 1 ends at slot 3: {:?}",
        last.state[2]
    );
    assert!(last.broken.is_none(), "no two values at one slot");
    assert!(
        last.state[0].contains(&"w2: Written at seq 2".to_string()),
        "the resend is written in segment 2: {:?}",
        last.state[0]
    );
}

#[test]
fn a_stale_bind_chooses_two_values() {
    let broken = run("break-the-rule");
    assert!(
        broken
            .steps
            .iter()
            .any(|step| step.label == "proposer 5 binds from a stale value"),
        "the fault is a bind from a stale register value"
    );
    let last = broken.steps.last().expect("steps");
    let said = last.broken.as_deref().expect("two values at one slot");
    assert!(said.contains("slot 3"), "{said}");
    assert!(said.contains("w2") && said.contains("w3"), "{said}");
    let first = broken
        .steps
        .iter()
        .position(|step| step.broken.is_some())
        .expect("a broken step");
    assert!(
        broken.steps[..first]
            .iter()
            .all(|step| step.broken.is_none()),
        "nothing is broken before the second value"
    );
}

#[test]
fn the_runs_share_their_first_steps() {
    let trace = trace();
    let base = &trace.scenarios[0];
    for run in &trace.scenarios[1..] {
        assert!(run.fork > 0, "{} shares the write of w1", run.id);
        assert_eq!(run.steps[..run.fork], base.steps[..run.fork], "{}", run.id);
    }
}

#[test]
fn a_page_is_recorded_alone_as_in_the_whole_trace() {
    assert_eq!(trace_of(PAGE), Some(trace()));
    assert_eq!(trace_of("single-decree"), None);
    assert_eq!(trace(), trace(), "a run draws no randomness");
}
