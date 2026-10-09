//! `paros-sim-web`: one seed of the paros main campaign, run in a browser
//! (#307).
//!
//! The crate adds no simulation code. [`run_seed_json`] calls
//! [`paros_sim::run_chain_seed_view`], the same campaign the native hunt
//! runs, and encodes the [`SeedRun`] as JSON. The page reads the JSON string
//! the entry returns; nothing goes to stdout.
//!
//! On `wasm32` the entry is exported to JavaScript as `runSeed(seed)`. The
//! native binary `paros-sim-seed` prints the same JSON, so CI can prove that
//! a seed gives the same bytes in the browser bundle and natively.

use paros_sim::{SeedEvent, SeedRun};
use serde_json::{Value, json};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

/// Run one seed of the main campaign and encode it as JSON.
#[must_use]
pub fn run_seed_json(seed: u64) -> String {
    encode(&paros_sim::run_chain_seed_view(seed)).to_string()
}

/// The JSON contract the page reads. A `u64` that can pass 2^53 (the seed,
/// the digest) is a decimal string, so JavaScript reads it without loss.
fn encode(run: &SeedRun) -> Value {
    json!({
        "seed": run.seed.to_string(),
        "green": run.is_green(),
        "violations": run.violations,
        "failure": run.failure,
        "digest": run.digest.map(|digest| format!("{digest:016x}")),
        "simulated_ms": run.simulated_ms,
        "steps": run.steps,
        "events": run.events.iter().map(encode_event).collect::<Vec<_>>(),
    })
}

fn encode_event(event: &SeedEvent) -> Value {
    json!({
        "t": event.time_ms,
        "source": event.source,
        "name": event.name,
        "detail": event.detail,
    })
}

/// The JavaScript entry point, exported as `runSeed(seed)`. The seed is a
/// decimal string: a JavaScript number cannot hold every `u64`. A seed that
/// does not parse returns a JSON error object.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = runSeed)]
#[must_use]
pub fn run_seed_wasm(seed: &str) -> String {
    console_error_panic_hook::set_once();
    match seed.trim().parse::<u64>() {
        Ok(seed) => run_seed_json(seed),
        Err(error) => json!({ "error": format!("not a seed: {error}") }).to_string(),
    }
}
