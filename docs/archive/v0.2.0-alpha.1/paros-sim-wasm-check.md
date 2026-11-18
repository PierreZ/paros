# paros-sim-wasm-check

The CI proof that a paros-sim seed gives the same run on `wasm32-unknown-unknown` as natively. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/sim/paros-sim-wasm-check/` (71 lines of Rust).

## What it did

It added no simulation code. It depended only on `paros-sim` (and on `wasm-bindgen` = 0.2.129 and `console_error_panic_hook` for wasm32). Nothing depended on it. It was a CI check, not a site tool: the website's wasm tools are in `paros-wasm` (#540), which never used `paros-sim`.

The flow (#307 (paros-sim in wasm)):

1. `fingerprints(seeds)` runs `paros_sim::chain_seed_fingerprint(seed)` for each seed and returns one `SeedFingerprint` line per seed: seed, audit digest, simulated ms, scheduler events, failed flag, violations.
2. The native binary `sim-seed-fingerprint` prints these lines.
3. On wasm32 the same function is exported to JavaScript as `fingerprints(seeds)`. Seeds cross as decimal text, because a JavaScript number cannot hold every `u64`.
4. `scripts/check-sim-wasm.sh` builds both (wasm-bindgen `--target nodejs`), runs the seeds natively and under node, and fails on any byte that differs. CI's `sim-wasm` job ran it.

The crate type was `cdylib` (for the wasm bundle) and `rlib` (for the native binary).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/sim/paros-sim-wasm-check/src/lib.rs` | `fingerprints`, `parse_seeds`, `fingerprints_wasm` (`#[wasm_bindgen(js_name = fingerprints)]`) |
| `crates/sim/paros-sim-wasm-check/src/bin/sim-seed-fingerprint.rs` | Native binary: `sim-seed-fingerprint <seed>...` |
| `crates/sim/paros-sim-wasm-check/Cargo.toml` | `crate-type = ["cdylib", "rlib"]`; wasm-bindgen pinned to the flake's CLI |
| `scripts/check-sim-wasm.sh` | Builds both targets, runs, diffs `native.txt` and `wasm.txt` |
| `crates/sim/paros-sim/src/lib.rs` | `SeedFingerprint` and its `Display`; `chain_seed_fingerprint`; `draw_index` |

## Ideas worth reusing

- **Cross-target determinism check.** One line per seed, built only from values that the seed fixes (no host clock), compared byte for byte between two builds. It is cheap and catches target-dependent draws. A consensus-sim on moonpool-walk can use the same check, and also compare two runs of one build.
- **The `draw_index` rule** (`crates/sim/paros-sim/src/lib.rs`). Reduce a `u64` draw in `u64`, then narrow to `usize`. `usize::try_from(draw).unwrap_or(0) % len` gives `0` for most draws on a 32-bit target, so the runs split.
- **Seeds as text over the JS boundary** (`parse_seeds`).

Mistakes not to copy:

- The wasm build pulled `getrandom` with `wasm_js`, `ring` with its wasm32 feature, and a clang for wasm32 (see `crates/sim/paros-sim/Cargo.toml`), only because TLS and Biscuit were linked into the sim. Keep a consensus sim free of those dependencies, and the wasm build is simple.

## Bugs and scenarios to cover again

- A draw narrowed to `usize` before its modulo gave other runs on wasm32 (the reason for `draw_index`, #307 (paros-sim in wasm)).
- Any host-clock read or `getrandom` draw in a run breaks the fingerprint. The paros-sim `Cargo.toml` notes that the run draws nothing from `getrandom`: every key comes from the seed.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/sim/paros-sim-wasm-check/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/sim/paros-sim-wasm-check

Where to start reading: `src/lib.rs`, `scripts/check-sim-wasm.sh`, then `SeedFingerprint` and `chain_seed_fingerprint` in `crates/sim/paros-sim/src/lib.rs`. Its own map at the tag: `crates/sim/paros-sim-wasm-check/AGENTS.md`.
