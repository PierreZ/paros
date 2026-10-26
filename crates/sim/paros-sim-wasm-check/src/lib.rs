//! `paros-sim-wasm-check`: a seed of the paros main campaign is the same run
//! on `wasm32-unknown-unknown` as natively (#307 (paros-sim in wasm)).
//!
//! The crate adds no simulation code. [`fingerprints`] runs
//! [`paros_sim::chain_seed_fingerprint`] for each seed and returns one line
//! per seed. The native binary `sim-seed-fingerprint` prints these lines; on
//! `wasm32` the same function is exported to JavaScript as
//! `fingerprints(seeds)`. `scripts/check-sim-wasm.sh` runs both and compares
//! the bytes.

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

/// Run each seed of the main campaign and return one fingerprint line per
/// seed, each ended by a newline.
#[must_use]
pub fn fingerprints(seeds: &[u64]) -> String {
    let mut out = String::new();
    for &seed in seeds {
        out.push_str(&paros_sim::chain_seed_fingerprint(seed).to_string());
        out.push('\n');
    }
    out
}

/// Parse a list of seeds: decimal `u64`s split by white space. A JavaScript
/// number cannot hold every `u64`, so the seeds cross as text.
///
/// # Errors
///
/// A word that is not a `u64`.
pub fn parse_seeds(text: &str) -> Result<Vec<u64>, String> {
    text.split_whitespace()
        .map(|word| {
            word.parse::<u64>()
                .map_err(|error| format!("not a seed: {word}: {error}"))
        })
        .collect()
}

/// The JavaScript entry point, exported as `fingerprints(seeds)`: the seeds
/// as decimal text split by white space, the lines of [`fingerprints`] back.
///
/// # Panics
///
/// Panics on a word that is not a seed (the panic hook reports it to node).
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = fingerprints)]
#[must_use]
pub fn fingerprints_wasm(seeds: &str) -> String {
    console_error_panic_hook::set_once();
    let seeds = parse_seeds(seeds).expect("the seeds are decimal u64s");
    fingerprints(&seeds)
}
