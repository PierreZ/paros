# paros-sim-runner

Two native binaries over `paros_sim`'s entry points (`publish = false`, `autobins = false`).
Top of the stack: `paros-core` ← `paros` ← `paros-sim` ← **`paros-sim-runner`**. No
environment variables, no flags beyond the positional arguments below.

## Map

- `src/main.rs` → `sim-paros-chain` → the CI gate; the only binary `cargo xtask sim` registers.
- `src/hunt.rs` → `sim-paros-hunt` → raw seed volume and single-seed replays, coverage-blind.
- `src/common.rs` → `arg`, `is_clean`, `print_seed_counts`, `print_failed_runs` (a panicked process is a failed run with no violation), `print_never_fired` → shared parsing and printing.

## Entry points

- `sim-paros-chain [iterations]` (default `COVERAGE_ITERATIONS`): runs `explore`, prints
  saturated-vs-cap, saturation signal, exploration stats and bug recipes, guidance watermarks
  and the gates that never fired. Exits 1 on any assertion violation, failed run, coverage violation or
  convergence timeout.
- `sim-paros-hunt [main|canary] [iterations]` (default 2000, the normal evidence budget;
  root *Simulation rules*): prints seed counts, assertion slots used and dropped, and gates
  that never fired; exits 1 on a violation, 2 on an unknown axis. Coverage never decides it.
- `sim-paros-hunt <replay> <seed>` with `replay-main`, `replay-canary`, `explore-main`
  (`EXPLORATION_TIMELINES_PER_SEED` timelines): prints GREEN or RED with the violations, exits
  1 on red (`replay_for`, `hunt.rs:25`). There is one campaign: the CTRL corpus is folded into
  it (#263).

## Local rules

- Verbosity: neither binary installs a tracing subscriber or reads `RUST_LOG`; the only capture
  is moonpool's sim layer at `SimulationBuilder::trace_level` (default `INFO`), which `paros-sim`
  never raises. Read violations and their detail maps from the printed report.
- Seeds printed here are evidence for a commit message, never constants to keep (root
  *Simulation rules*).

## Tests & gates

- `cargo xtask sim run paros-chain` (= `run-all`; CI `sim` job) builds `sim-paros-chain` under
  sancov and runs it.
- Hunts: `cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- main 2000`.

## Deps

- `paros-sim` only (`Cargo.toml:21`); binaries declared at `Cargo.toml:12-18`.
