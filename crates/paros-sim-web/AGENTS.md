# paros-sim-web

One seed of the paros main campaign in a browser (#307 (paros-sim in wasm)): the wasm-bindgen
entry the site's seed widget loads. `publish = false`; `cdylib` for the browser, `rlib` for the
native binary. It adds no simulation code: the run is `paros_sim::run_chain_seed_view`, the
same `chain_builder` the hunt and the sweep run, with two observers (the audit digest and the
recorder of `paros_sim::VIEW_EVENTS`).

## Map

- `src/lib.rs` → `run_seed_json(seed)`, and `runSeed(seed: string)` on wasm32 → the JSON
  contract the widget reads: `seed` and `digest` as strings (a `u64` can pass 2^53), `green`,
  `violations`, `failure`, `simulated_ms`, `steps`, `events` (`t`, `source`, `role`, `name`,
  `fields`).
- `src/bin/paros-sim-seed.rs` → the same JSON natively, one line per seed.
- The widget is `website/sim-seed/` (`seed.js`, `worker.js`, `seed.css`), mounted by the Zola
  shortcode `sim_seed` (`website/zola/templates/shortcodes/sim_seed.html`), built and staged at
  `/sim-seed/` by `scripts/build-sim-seed.sh`.

## The rule

**A seed is the same run in the browser and natively.** `scripts/build-sim-seed.sh --check
SEED...` runs seeds through `paros-sim-seed` and through the wasm under node and fails unless
the JSON is identical (CI job `sim-seed`). On wasm32 a `usize` is 32 bits: never narrow a `u64`
draw to `usize` before reducing it (`paros_sim::draw_index`). Float transcendental functions
(`ln`, `powf`) may differ in the last bit between the native libm and wasm's; a seed that
splits on one is a moonpool fix (portable `libm`), not a paros one.

## Gates

`cargo check --target wasm32-unknown-unknown -p paros-sim` (CI `portability`);
`scripts/build-sim-seed.sh --check 1 … 10` (CI `sim-seed`); `scripts/build-sim-seed.sh` in the
Pages workflow. Building for wasm32 needs `CC_wasm32_unknown_unknown` (ring's C code), which the
flake sets.
