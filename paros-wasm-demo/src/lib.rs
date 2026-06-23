//! `paros-wasm-demo` — a browser/wasm demo animating a deterministic paros
//! simulation.
//!
//! All the simulation logic — the node driver, the client workload, the oracles
//! — lives in `paros-sim` and is reused unchanged here, in its wasm-able
//! `default-features = false` configuration. This crate is only the thin
//! wasm-bindgen glue: it forwards a seed to [`paros_sim::run_seed_json`] and the
//! page (`web/index.html`) animates the returned timeline on a canvas.
//!
//! Stage 1 renders an empty cluster acknowledging client proposals and advancing
//! logical time; later milestones extend the same demo (election → log →
//! snapshot → reconfig → compartments).

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

// Re-exported so the native rlib path (and tests) can call the same entry point
// the browser does.
pub use paros_sim::{run_seed, run_seed_json};

/// wasm entry point exported to JavaScript as `runSeed(seed)`. Installs the panic
/// hook first so any runtime panic surfaces as a real message in the browser
/// console rather than an opaque trap.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = runSeed)]
#[must_use]
pub fn run_seed_wasm(seed: u64) -> String {
    console_error_panic_hook::set_once();
    paros_sim::run_seed_json(seed)
}
