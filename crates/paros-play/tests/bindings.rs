//! Write `web/play/src/generated/` from the `ts_rs::TS` derives.
//!
//! The generated files are committed and CI diffs them, so this test is the
//! only way they are produced: change a view type, run
//! `cargo test -p paros-play`, commit what changed.

use std::path::PathBuf;

use paros_play::action::{Action, ActionError};
use paros_play::view::{ErrorView, GameView, LevelSummary};
use ts_rs::TS;

fn out_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../web/play/src/generated")
        .canonicalize()
        .expect("web/play/src/generated exists and is committed")
}

#[test]
fn typescript_bindings_are_written() {
    // `u64` becomes `number`, not `bigint`: the engine's ids, slots, ticks and
    // ballot rounds are all small, and `JSON.parse` — which is how the browser
    // reads every one of these — produces `number`. A `bigint` binding would be
    // a type the runtime never actually hands the frontend.
    let config = ts_rs::Config::new()
        .with_out_dir(out_dir())
        .with_large_int("number");
    // Each root drags its whole dependency tree with it, so these five cover
    // every type on the JS boundary: the frame, the input, the two refusals,
    // and the level map.
    GameView::export_all(&config).expect("the view exports");
    Action::export_all(&config).expect("the action exports");
    ActionError::export_all(&config).expect("the error exports");
    ErrorView::export_all(&config).expect("the wasm error exports");
    LevelSummary::export_all(&config).expect("the level map exports");

    for name in [
        "GameView.ts",
        "WorldView.ts",
        "NodeView.ts",
        "MessageView.ts",
        "PromptView.ts",
        "GoalView.ts",
        "Action.ts",
        "ActionKind.ts",
        "AutomationFlag.ts",
        "PromptKind.ts",
        "Seam.ts",
        "LevelSummary.ts",
        "ErrorView.ts",
    ] {
        let path = out_dir().join(name);
        assert!(path.is_file(), "{name} was not generated at {path:?}");
    }
}
