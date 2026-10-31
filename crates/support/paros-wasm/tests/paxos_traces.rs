//! Write `website/site/src/traces/` from the engine (#811 (Paxos flavor
//! pages)).
//!
//! Each page of `/paxos/` draws a step-through diagram from one JSON trace:
//! a recorded run of the real `paros-core` through the game's engine. The
//! files are committed and CI diffs them, so this test is the only way they
//! are produced: change a run or the engine, run
//! `cargo test -p paros-wasm --test paxos_traces`, commit what changed.

use std::path::PathBuf;

use paros_wasm::paxos::trace::traces;

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
