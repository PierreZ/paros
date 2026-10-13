//! Milestone 1 of #518 (Paxos visualization sandbox): three founding
//! machines form a cell and choose a value, survive their leader's crash,
//! and admit a fourth machine.

use paros_sandbox::{Action, Config, Op, run};

fn at(at_ms: u64, op: Op) -> Action {
    Action { at_ms, op }
}

fn write(at_ms: u64, value: &str) -> Action {
    at(
        at_ms,
        Op::Write {
            value: value.into(),
        },
    )
}

#[test]
fn a_cell_forms_survives_a_crash_and_admits_a_machine() {
    let config = Config::default();
    let base = vec![at(0, Op::Init), write(1_000, "a")];
    let first = run(&config, &base);
    assert_eq!(first.error, None, "{first:#?}");
    assert!(first.steps.iter().all(|s| s.ok), "{:#?}", first.steps);
    // The machine that leads the journal: the source of its last chosen value.
    let leader = first
        .events
        .iter()
        .rev()
        .find(|e| e.name == "leader_elected")
        .and_then(|e| e.machine)
        .expect("a leader was elected");

    let mut actions = base.clone();
    let crash_at = first.steps.last().map_or(5_000, |s| s.end_ms + 100);
    actions.push(at(crash_at, Op::Crash { machine: leader }));
    actions.push(write(crash_at + 1_000, "b"));
    actions.push(at(crash_at + 2_000, Op::Admit { machine: 3 }));
    let second = run(&config, &actions);
    assert_eq!(second.error, None, "{second:#?}");
    for step in &second.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    assert_eq!(second.steps.len(), 4);
    // Adding steps to the log never changes what came before them.
    assert_eq!(second.steps[..2], first.steps[..]);
    // Another machine took the lead after the crash.
    assert!(
        second.events.iter().any(|e| e.name == "leader_elected"
            && e.time_ms > crash_at
            && e.machine != Some(leader)),
        "no machine took the lead after the crash"
    );
    // The fourth machine serves the cell.
    assert!(
        second
            .events
            .iter()
            .any(|e| e.name == "machine_admitted" && e.machine == Some(3)),
        "machine 3 was not admitted"
    );

    // Replay: the same config and log give the same run.
    assert_eq!(run(&config, &actions), second);
}
