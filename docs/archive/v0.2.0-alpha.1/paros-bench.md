# paros-bench

The load generator: writers, tailers and readers over `paros::client` against a cell or a local cell, with HDR latency histograms, pacing, presets, `--expect` limits and mergeable JSON reports. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/bin/paros-bench/` (5,396 lines of Rust).

## What it did

A binary at the top of the stack (`publish = false`). It depended on `paros` (with `tls`), on `parosctl`'s library (`parosctl::support::connect::{Ending, Runtime}`, `output::note`) and on compio providers. It was `parosctl bench` until #722 (three products). No sim path: it called only `paros::client`, on real time.

Phases are subcommands (#725 (bench phases)): `init` fills the journals, `run` measures, `clean` deletes what the bench made (the tenant is tagged `paros-bench/created:true`, journals are `bench-RxC-K`), `report` merges `--json` reports. `run --local N` starts N shipped `run_machine`s on loopback in their own thread, forms them with `paros::client::initialize`, and deletes the stores after.

The load is roles, not a call mix (#724 (bench roles)). A writer appends at the tail; the writers of one journal share one writer through `paros::client::Pipeline`, so several writes are in flight at consecutive positions (#597 (several writes in flight per journal)). A tailer long-polls from the tail and measures `e2e` (seen time minus the send time in the record). A reader reads from the start, a random position or the tail of a journal a `Picker` draws (uniform or Zipfian, θ = 0.99). Set-leader and truncate run on a schedule. Every read checks every byte. `--backlog` stops the tailers, writes a backlog, then measures catch-up. `--topology 3x1,3x3` runs each journal shape in turn under the same seed.

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/bin/paros-bench/src/main.rs` | Subcommands `init`, `run`, `clean`, `report`. |
| `crates/bin/paros-bench/src/bench.rs` | `Settings`, `Plan` (`run_one`, `finish`), `prepare`, `drive` (1 s tick, ramp, warmup, Ctrl-C, `--max-ops`), roles `writer`, `tailer`, `reader`, `schedule`; `Backlog` (waiting, filling, draining, done); `Failure::kind`. |
| `crates/bin/paros-bench/src/bench/load.rs` | `WriteFlags`, `LoadFlags`, `Preset` (`append-only`, `pub-sub`, `catch-up`, `leader-churn`), `Load`, `ReadFrom`, `Distribution`, `Picker`. |
| `crates/bin/paros-bench/src/bench/pace.rs` | `Pace`: calls/s or bytes/s, fixed or `sin`, `square`, `pulse` (after `mako`). |
| `crates/bin/paros-bench/src/bench/records.rs` | `Rng` (SplitMix64 from `(seed, client)`), record `i` as a pure function of `(seed, journal, i)` with a 24-byte header, `check`. |
| `crates/bin/paros-bench/src/bench/histograms.rs` | `Call`, per-client HDR histograms swapped and merged each tick, `--histograms` lines. |
| `crates/bin/paros-bench/src/bench/summary.rs`, `bench/report.rs` | The report document (ops, bytes, errors by kind, percentiles, raw V2 histogram in base64, commit, command line, host) and lossless merge. |
| `crates/bin/paros-bench/src/bench/expect.rs` | `Limits`: `e2e.p50<5ms`, `throughput>100MiB/s`, `errors<1`, `catch-up<10s`. |
| `crates/bin/paros-bench/src/bench/setup.rs`, `bench/local.rs`, `bench/topology.rs`, `bench/units.rs` | Tenant and journals, local cell, `Topology`, unit parsing. |
| `crates/bin/paros-bench/build.rs` | `PAROS_COMMIT` from the environment or `git rev-parse HEAD`. |

## Ideas worth reusing

- **Open loop with no coordinated omission** (`bench/pace.rs`, `bench.rs`). With a rate, a call's latency counts from its due time, so a slow call is charged to the calls it delays.
- **Self-checking records** (`bench/records.rs`). Record `i` is a pure function of `(seed, journal, i)`, so every read checks every byte with no stored copy. `a_record_checks_at_its_own_position_only` catches a record at the wrong position.
- **Roles, not a call mix**, and presets that only set defaults (an explicit flag wins).
- **Raw HDR histograms in the report** (`summary.rs`, `report.rs`): reports of several processes merge with no loss; reports of other runs are refused.
- **`--expect` as a pass/fail gate** (`expect.rs`), and the CI smoke run checks that it works, never a number (shared runners are noise).
- **The report names the commit, command line, CPU count, kernel and file system** of the stores (`the_longest_mount_that_holds_the_directory_names_its_filesystem`).
- **Every random choice from `(seed, client)`**, seed logged.
- **Catch-up as a state machine** (`Backlog`): a clear way to measure recovery throughput.

Leaks and mistakes not to copy:

- Depends on `parosctl` as a library for two types (`Cargo.toml`, `bench.rs`). Share a small crate instead.
- `bench.rs` is 1,677 lines: driver, roles, schedule and backlog in one file.
- It uses `Instant::now()` and `SystemTime::now()` directly (`bench.rs`), so it cannot run on simulated time; fine for a bench, but no seed replays a run.

## Bugs and scenarios to cover again

- **Every read checks every byte, at its own position**: `a_record_checks_at_its_own_position_only`. A new journal bench must keep a byte check.
- **Init, run, clean on a local cell** (about 15 s): `init_run_and_clean_on_a_local_cell` in `src/bench/tests.rs`.
- **Clean deletes only what the bench made**: `only_the_bench_journals_are_its_own`.
- **Catch-up reports its time and rate**: `the_catch_up_reports_its_time_and_rate`.
- **Reports merge without loss, other runs refused**: `reports_merge_without_loss`, `reports_of_other_runs_are_refused`.
- **A run the arguments cannot hold is refused before it starts**: `a_run_the_arguments_cannot_hold_is_refused`, `a_load_that_cannot_run_is_refused`.
- **CI smoke**: `cargo run -p paros-bench -- run --local 3 --duration 5s --preset pub-sub --readers-per-journal 1 --expect 'errors<1'`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/bin/paros-bench/src/bench.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/bin/paros-bench

Where to start reading: `src/bench.rs` (module doc, then `drive`), `src/bench/load.rs`, `src/bench/records.rs`, `src/bench/pace.rs`, then `src/bench/summary.rs`. Its own map at the tag: `crates/bin/paros-bench/AGENTS.md`.
