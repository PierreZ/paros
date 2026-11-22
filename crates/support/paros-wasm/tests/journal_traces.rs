//! Write the journal's step-through trace to `website/site/src/traces/`
//! (#1365 (reshape and seal step-through)).
//!
//! The Segments page draws a reshape and a seal from one JSON trace: a
//! recorded run of the journal roles of `paros-journal`
//! (`paros_wasm::journal::seal`). The file is committed and CI diffs it, so
//! this test is the only way it is produced: change a run or a role, run
//! `cargo test -p paros-wasm --test journal_traces`, commit what changed.

use std::path::PathBuf;

use paros_wasm::journal::seal::{PAGE, trace};

#[test]
fn the_seal_trace_is_written() {
    let trace = trace();
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../website/site/src/traces")
        .canonicalize()
        .expect("website/site/src/traces exists and is committed");
    let mut json = serde_json::to_string_pretty(&trace).expect("a trace serializes");
    json.push('\n');
    std::fs::write(out.join(format!("{PAGE}.json")), json).expect("the trace is written");
}
