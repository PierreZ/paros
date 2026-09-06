# paros-sim-runner

`publish = false`. Two binaries over `paros_sim`'s entry points; no environment
variables, no flags beyond the positional arguments below.

- `sim-paros-chain` (`src/main.rs`) — the CI gate and the only binary
  registered with `cargo xtask sim`. Optional positional: the exploration
  iteration cap (default `COVERAGE_ITERATIONS`). Runs `explore`, prints
  saturated-vs-cap, exploration stats and recipes, the guidance watermarks
  and the gates that never fired; exits 1 on any assertion violation, failed
  run, coverage violation or convergence timeout. Then `run_corpus_axes`:
  `corpus_hunt(CORPUS_CI_ITERATIONS)` and
  `chunk_corpus_hunt(CHUNK_CORPUS_CI_ITERATIONS)`, each through `gate_corpus`
  with the same exit rule.
- `sim-paros-hunt` (`src/hunt.rs`) — raw volume, coverage-blind.
  `sim-paros-hunt [axis] [iterations]` with axes `main` (default), `canary`,
  `corpus`, `corpus-chunks`; iterations default to 2000 (the normal evidence
  budget; 10,000 only for a substantial change). Replays take a seed or mask
  as the second argument: `replay-main`, `replay-canary`, `explore-main`,
  `replay-corpus`, `replay-corpus-mask`, `replay-bare-quorum`,
  `replay-lifecycle`, `replay-departed`, `replay-chunk-mask`,
  `replay-chunk-seed`; they print GREEN/RED and exit 1 on red.

Adding a corpus family means adding its `replay-*` arm here and, if CI must
sweep it, a `gate_corpus` call in `main.rs`. Verbosity is
`init_sim_tracing` in the binary, not `RUST_LOG` (the flake exports it, but
moonpool's sim subscriber is floored at INFO).
