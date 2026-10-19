# paros-sim-wasm-check

The proof that `paros-sim` runs the same on `wasm32-unknown-unknown` as natively (#307
(paros-sim in wasm)). `publish = false`; `cdylib` for the wasm-bindgen bundle node loads,
`rlib` for the native binary. It is a CI check, not a site tool: the site's wasm tools are
`paros-wasm` (#540), which never depends on `paros-sim`.

## Map

- `src/lib.rs` → `fingerprints` (one `paros_sim::SeedFingerprint` line per seed),
  `parse_seeds`, and `fingerprints(seeds)` exported to JavaScript on `wasm32`.
- `src/bin/sim-seed-fingerprint.rs` → the native binary that prints the same lines.
- `scripts/check-sim-wasm.sh` → builds both, runs the wasm bundle under node, and fails on
  any byte that differs. CI's `sim-wasm` job runs it.

## The rule

A seed names the same run on every target. A draw that narrows a `u64` to `usize` before
its modulo splits the runs on a 32-bit target: reduce in `u64` first
(`paros_sim::draw_index`).
