# paros-sim-runner

Two native binaries over `paros_sim`'s entry points (`publish = false`, `autobins = false`),
and the mutation hunt's test target.
Top of the stack: `paros-core` ← `paros` ← `paros-sim` ← **`paros-sim-runner`**. The
binaries take no flags beyond the positional arguments below; `sim-paros-chain` reads
`PAROS_SIM_SWEEP_MINUTES`, `PAROS_SIM_MISSING_GATES` and `PAROS_SIM_RED` (below); the `mutants` test reads `PAROS_MUTANT_SEEDS`.

## Map

- `src/main.rs` → `sim-paros-chain` → the CI gate; the only binary `cargo xtask sim` registers.
- `src/hunt.rs` → `sim-paros-hunt` → raw seed volume and single-seed replays, coverage-blind.
- `tests/mutants.rs` → the mutation hunt (#269), cargo-mutants' test command: `chain_mutants`
  over seeds `1..=PAROS_MUTANT_SEEDS` (default `MUTANT_SEEDS`) in batches of `MUTANT_BATCH`,
  exit 1 at the first batch with a violation or failed run. Built only with the `mutants` feature, so nextest never runs it.
- `build.rs` + `src/scan.rs` → the declared-gate scan (over `build.rs`'s `SCANNED`, which holds every crate `cargo xtask sim` instruments: a unit test) (#446 (a gate that never fires cannot fail
  the sweep)): every `assert_reachable!`, `reachable!`, `reach_once!`, `assert_sometimes!`,
  `sometimes!`, `probe!` and `assert_always!` (#909 (oracles that do not judge)) with a
  literal message in the library crates and `paros-sim` (`#[cfg(test)]` items and comments
  skipped), compiled in as `gates::DECLARED`. `assert_always_or_unreachable!` (a failure
  branch) is not declared.
- `src/gates.rs` → `DECLARED`, `RARE` (declared gates the sweep is known to miss, each with its reason; it only shrinks, `RARE_CEILING` and a unit test fail the build when it grows, #911 (rare gates into buggify)), `judge`, `print_rarest` → the declared-gate check: moonpool judges only gates that
  have a slot, and a gate gets its slot the first time it runs, so this check names every
  declared gate that never ran. Slots are named by their first 63 bytes (`slot_name`); no two
  gates may share them (a unit test).
- `src/common.rs` → `arg`, `is_clean`, `print_seed_counts`, `print_failed_runs` (a panicked process is a failed run with no violation), `print_excuses` (#738 (count excuses, the baseline): per excuse and silent skip, the runs where it fired, their share, and the journals it excused) → shared parsing and printing.

## Entry points

- `sim-paros-chain [minutes]` (default 45, or `PAROS_SIM_SWEEP_MINUTES`; #920 (sweep budget)):
  runs `explore(SWEEP_BATCH)` batch after batch until the budget is spent, and stops at the
  first red batch. It prints each batch's base seed, the seeds, batches and timelines, the
  sancov edges, guidance watermarks, the gates no batch fired, the declared gates that never
  ran, the ten rarest declared gates that fired, the excuse table, and on red the bug recipes
  and failing seeds. Exits 1 on any assertion violation or failed run, and, outside a CI shard,
  on a gate that never fired or a declared gate that never ran (unless listed in
  `gates::RARE`). With `PAROS_SIM_MISSING_GATES=<path>` the run is a CI shard: it writes its
  gate defects to `<path>` and does not fail on them; CI's `sim-gates` job fails on a gate
  every shard missed. With `PAROS_SIM_RED=<path>` a red run writes one line per failed
  assertion or failed run, `<message>\t<seeds>`, for the `main-reds` job
  (`scripts/main-red-record.sh`).
- `sim-paros-hunt [main|canary] [iterations] [gate-filter]` (default 2000, the normal evidence
  budget; root *Simulation rules*): prints seed counts, assertion slots used and dropped, gates
  that never fired, the declared gates that never ran, the 25 rarest declared gates, the excuse table, and with a filter every gate whose name contains it, with its successes and
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

- `cargo xtask sim run paros-chain` (= `run-all`; CI `sim` job, four parallel shards with their own seeds) builds `sim-paros-chain` under
  sancov and runs it.
- Hunts: `cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- main 2000`.
- Mutation hunt: `cargo xtask mutants [--seeds N] [cargo-mutants args]` (weekly in CI,
  `.github/workflows/mutants.yml`); one mutant by name: `--re '<name from --list>'`.
- Nightly hunt: `.github/workflows/nightly-sim.yml` runs `sim-paros-hunt main 50` again and again,
  one process per core, on 8 runners for 180 minutes (`scripts/nightly-sim-shard.sh`); failing
  seeds go to the rolling `nightly-sim` issue (`scripts/nightly-sim-report.sh`).

## Deps

- `paros-sim` only (`Cargo.toml:21`); binaries declared at `Cargo.toml:12-18`.
