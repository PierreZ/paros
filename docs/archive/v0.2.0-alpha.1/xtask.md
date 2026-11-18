# xtask

The `cargo xtask` build tool of paros: the sancov-guided simulation sweep, the cargo-mutants hunt and the negative hunt. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/support/xtask/` (930 lines of Rust).

## What it did

It had no crate dependencies (`Cargo.toml` lists none). It ran `cargo` as a child process and drove the binaries and the test target of `paros-sim-runner`. The alias `xtask = "run --package xtask --"` is in `.cargo/config.toml`. Its design mirrors moonpool's own `xtask`.

Three commands (`src/main.rs`, `main`):

- `cargo xtask sim {list|run <filter> [-- args]|run-all}`. It builds and runs each entry of `SIM_BINARIES` with `cargo run --release --target-dir target/sancov`, `SANCOV_CRATES` set, and `scripts/sancov-rustc.sh` as `RUSTC_WRAPPER` (set by the flake). So moonpool's coverage-guided exploration reads real LLVM edge coverage of the shipped crates, not the assertion fallback. The registry held one entry, `sim-paros-chain`, so `run paros-chain` and `run-all` were the same sweep.
- `cargo xtask mutants [--seeds N] [cargo-mutants args]` (#269). It runs cargo-mutants over the paros-core files in `.cargo/mutants.toml` (`examine_globs`), with the fixed-seed hunt (`paros-sim-runner`, `--features mutants --test mutants`) as the test. First it runs the unmutated hunt and records its reds at `target/mutants-baseline.txt`; the per-mutant timeout is 3 times the baseline time, at least 60 s.
- `cargo xtask negative [--list] [--seeds N] [--shard K/N] [--baseline FILE] [--baseline-only] [--timeout S] [filter]` (`src/negative.rs`, #742 (mutant per excuse), #1094 (injected liveness bugs)). It records the baseline, applies each `.mutant` file in `crates/sim/paros-sim-runner/negative/` to the tree, builds and runs the hunt, and puts the tree back. Verdicts go to `negative.out/verdicts.txt`. It exits non-zero when a verdict differs from the mutant's `expect`.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/support/xtask/src/main.rs` | `SimBinary`, `SIM_BINARIES`, `run_binaries`, `ensure_instrumentation_fresh`, `SANCOV_STAMP`; `mutants`, `mutant_baseline`, `MUTANT_HUNT`, `MUTANT_BASELINE`, `MUTANT_INCREMENTAL`, `MUTANT_TIMEOUT_MULTIPLIER` |
| `crates/support/xtask/src/negative.rs` | `negative`, `Mutant`, `Edit`, `Expect` (`Caught`, `Survives(issue)`), `Verdict` (`Caught`, `Survived`, `Unviable`, `Timeout`, `Stale`), `parse`, `apply` (with a drop guard that restores the file), `record_baseline`, `run`, `wait`; `MUTANT_DIR`, `OUT_DIR`, `DEFAULT_SEEDS` = 600, `DEFAULT_TIMEOUT` = 90 min |
| `scripts/sancov-rustc.sh` | The `RUSTC_WRAPPER`: instruments only crates in `SANCOV_CRATES`, never build scripts or proc-macros |
| `.cargo/mutants.toml` | cargo-mutants scope (paros-core acceptor, proposer, journal state, matchmaking, membership, catch-up) and `exclude_re` with one reason per entry |

## Ideas worth reusing

- **Instrument the system under test, not the harness** (`SIM_BINARIES` comment in `src/main.rs`). Harness edges inflate the coverage denominator and send exploration into harness code. For consensus-sim, instrument only `paros-consensus`.
- **A stamp for the sancov crate list** (`ensure_instrumentation_fresh`). `SANCOV_CRATES` is not in cargo's fingerprint. The stamp at `target/sancov/.sancov-crates` and a `cargo clean -p` of the symmetric difference stop a stale build from guiding the sweep.
- **Mutant file format and verdicts** (`src/negative.rs` module doc). A find text that is missing or occurs twice makes the mutant **stale**, so a mutant follows the code. `expect: survives #N` keeps known survivors honest.
- **Baseline first, then mutants** (`mutant_baseline`, `record_baseline`). The baseline time sizes the per-mutant timeout. A livelocked mutant is a timeout, not a hang.
- **Incremental release builds** for mutants (`MUTANT_INCREMENTAL`): about 5 s per rebuild instead of 58 s on 4 cores.

Mistakes not to copy:

- `crates/support/xtask/AGENTS.md` says `sancov_crates: "paros_core,paros"`, but `src/main.rs` lists twelve library crates. The doc drifted from the code.
- The negative hunt edits the working tree in place. The `Applied` drop guard puts files back after a panic, but a killed process (no drop) leaves a mutated file. A copy of the tree, as cargo-mutants does, is safer.

## Bugs and scenarios to cover again

This crate judged no protocol itself. The bug list it ran is the 23 mutants of `crates/sim/paros-sim-runner/negative/` (see `paros-sim-runner.md`) and the cargo-mutants scope of `.cargo/mutants.toml`. At the step 1.5 commit, 21 of 23 negative mutants were caught in 300 seeds; two survive because the seeds do not reach their state, #1177 (mutants whose state the seeds never reach). A new sim must catch the same mutants again, ported to `paros-consensus` where the code moved. The triage notes are in `docs/analysis/simulation/mutation-triage.md`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/support/xtask/src/main.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/support/xtask

Where to start reading: `src/negative.rs` (module doc), `src/main.rs` (`mutants`, then `run_binaries` and `ensure_instrumentation_fresh`), `scripts/sancov-rustc.sh`, then `.cargo/mutants.toml`. Its own map at the tag: `crates/support/xtask/AGENTS.md`.
