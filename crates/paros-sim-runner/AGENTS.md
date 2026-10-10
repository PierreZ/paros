# paros-sim-runner

Two native binaries over `paros_sim`'s entry points (`publish = false`, `autobins = false`),
and the mutation hunt's test target.
Top of the stack: `paros-core` ← `paros` ← `paros-sim` ← **`paros-sim-runner`**. The
binaries read no environment variables and take no flags beyond the positional arguments below;
the `mutants` test reads `PAROS_MUTANT_SEEDS`.

## Map

- `src/main.rs` → `sim-paros-chain` → the CI gate; the only binary `cargo xtask sim` registers.
- `src/hunt.rs` → `sim-paros-hunt` → raw seed volume and single-seed replays, coverage-blind.
- `tests/mutants.rs` → the mutation hunt (#269), cargo-mutants' test command: `chain_mutants`
  over seeds `1..=PAROS_MUTANT_SEEDS` (default `MUTANT_SEEDS`) in batches of `MUTANT_BATCH`,
  exit 1 at the first batch with a violation or failed run. Built only with the `mutants` feature, so nextest never runs it.
- `src/common.rs` → `arg`, `is_clean`, `print_seed_counts`, `print_failed_runs` (a panicked process is a failed run with no violation), `print_never_fired` → shared parsing and printing.

## Entry points

- `sim-paros-chain [iterations]` (default `COVERAGE_ITERATIONS`): runs `explore`, prints
  saturated-vs-cap, saturation signal, exploration stats and bug recipes, guidance watermarks
  and the gates that never fired. Exits 1 on any assertion violation, failed run, coverage violation or
  convergence timeout.
- `sim-paros-hunt [main|canary] [iterations] [gate-filter]` (default 2000, the normal evidence
  budget; root *Simulation rules*): prints seed counts, assertion slots used and dropped, gates
  that never fired, and with a filter every gate whose name contains it, with its successes and
  checks (a gate's rate before and after a change); exits 1 on a violation, 2 on an unknown
  axis. Coverage never decides it.
- `sim-paros-hunt <replay> <seed>` with `replay-main`, `replay-canary`, `explore-main`
  (`EXPLORATION_TIMELINES_PER_SEED` timelines): prints GREEN or RED with the violations, exits
  1 on red (`replay_for`, `hunt.rs:25`). `sim-paros-hunt replay-recipe <seed> <count>:<reseed>,..` replays one explored timeline from a bug recipe the sweep printed (`paros_sim::replay_chain_timeline`). There is one campaign: the CTRL corpus is folded into
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
- Mutation hunt: `cargo xtask mutants [--seeds N] [cargo-mutants args]` (weekly in CI,
  `.github/workflows/mutants.yml`); one mutant by name: `--re '<name from --list>'`.

## Deps

- `paros-sim` only (`Cargo.toml:21`); binaries declared at `Cargo.toml:12-18`.
