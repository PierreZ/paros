# parosd

The daemon binary: one uniform `parosd` per machine, a thin shell around `paros::machine::run_machine` on one compio thread and a real disk. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/bin/parosd/` (2,040 lines of Rust).

## What it did

The top of the stack (`publish = false`). It depended on `paros` (with `tls`) and `paros-authz-biscuit`, and it was the only crate that linked moonpool's `CompioProviders`. Nothing depended on it. The lifecycle (format, amnesia check, wait for admission, serve) was the library's (#246 (machine lifecycle in the library)); the simulation ran the same code. `parosd` held only: configuration, name resolution, the runtime, signals, HTTP probes, OTLP export and exit codes.

Flow of `main` (`src/main.rs`): parse `Settings` (every `PAROS_*` variable is also a `--flag`; an unknown `PAROS_*` variable is an error), build `DriverTunables` from `production()` plus `PAROS_<FIELD>[_MS]` overrides refused below their floor, resolve the listen address once (with patience for Compose peers that still start), keep dial addresses as names, build the transport (mutual TLS by default when the PEMs are set) and the admin pin (`BiscuitAuthz` over `PAROS_ROOT_PUBLIC_KEY`), start the compio runtime, run the machine on `ProviderDisk` over `CompioStorageProvider` rooted at the data directory, and map the result to an exit code.

A machine needs no peer list: no id, no seeds (#277 (no seeds)). The data directory holds `<data-dir>/machine` (the record) and `<data-dir>/journals/<tenant>/<journal>/` (the stores, #235).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/bin/parosd/src/main.rs` | `main`, `prepare`, `run`, `ended`, `exit`, `remedy`, `shutdown_on_signal`, `patiently`; constants `EXIT_RESTART` (75), `EXIT_REFUSED` (78), `EXIT_FORCED` (130), `STOP_BACKSTOP`, `RESOLVE_PATIENCE`. |
| `crates/bin/parosd/src/settings.rs` | `Settings` (clap with `env`), `check_unknown`, `Settings::transport`, `Settings::pin`, `Settings::frontend`. |
| `crates/bin/parosd/src/tunables.rs` | `from_env`, `variables`: one override per row of `DriverTunables::FIELDS`. |
| `crates/bin/parosd/src/http.rs` | `serve`, `route`: a hand-written HTTP/1.1 server for `/metrics`, `/healthz`, `/readyz` (`503` with the phase until the machine serves its cell). |
| `crates/bin/parosd/src/otel.rs` | `Telemetry`, `exported`: OTLP spans and one observable instrument per metrics family; a one-worker Tokio runtime only when an exporter is set. |
| `crates/bin/parosd/tests/real_fs.rs` | Both storage contract suites on a real disk; restart loop; machine record. |
| `crates/bin/parosd/README.md` | Operator guide: Compose toy, configuration, tunables, exit codes, data directory. |

## Ideas worth reusing

- **A thin binary over a library lifecycle** (`src/main.rs`). The simulation runs `run_machine` too, so the binary adds no behavior to test.
- **Exit codes that tell the supervisor what to do** (`main.rs` module doc table): `75` restart (fail-stop storage fault), `78` do not restart (amnesia, edited `Config`, class change), `1` environment, `2` bad configuration, `130` forced stop. `remedy` gives one sentence per `BootRefusal`.
- **Clean stop with a bound and a backstop** (`shutdown_on_signal`, #560 (clean Ctrl-C shutdown)): the first signal starts a stop that hands leaderships on; a second signal or a hang past the bound exits as a crash.
- **Unknown `PAROS_*` variable is an error** (`settings.rs`, `check_unknown`). A typo never runs silently on defaults.
- **Overrides from the tunables table** (`src/tunables.rs`): no line per knob, floors checked.
- **Wall clock taken once at start**, then provider time (`Settings::frontend`, `pin`), so token checks stay close to the simulated path.
- **Real-disk contract test** (`tests/real_fs.rs`): `a_store_dropped_mid_batch_reopens_with_every_acknowledged_write` kills a store between a write and its sync, 40 times.

Leaks and mistakes not to copy:

- **Stale docs.** The `main.rs` module doc still says "Tokio wiring" and "Tokio's filesystem", but the binary runs on compio.
- **Interim control-plane rules** (AGENTS.md "Interim (M8 → M9)"): the election journal and admin commands that claim the cell control journal. `docs/architecture` rule 3 (one writer per control journal) deletes them.
- **Dead settings** kept for compatibility: `PAROS_CAPACITY` (ignored since #837 (remove role slots)).
- **Feature unification hazard** (`Cargo.toml`): built with `paros-sim` in one workspace build, the binary got moonpool-sim's deterministic `select!`. Only `cargo build -p parosd` was the shipped build.
- **A hand-written HTTP server** (`src/http.rs`): small, but it needed its own tests for head parsing and bounds.

## Bugs and scenarios to cover again

- **Exit code per ending**: `each_ending_has_its_exit_code`, `a_driver_error_maps_to_restart_refused_or_failure`, `each_refusal_has_its_own_remedy` (`src/main.rs`).
- **Kill between write and fsync on a real disk keeps every acked write and nothing else**: `a_store_dropped_mid_batch_reopens_with_every_acknowledged_write`.
- **Machine record written whole; a journals directory without a record is a lost identity**: `the_machine_record_is_absent_then_read_back_whole`.
- **Advertised address must not default to a wildcard listen address**: `the_advertised_address_defaults_to_a_listen_address_that_is_not_a_wildcard`.
- **Name resolution waits for Compose peers, fails only at its deadline**: `patience_returns_the_first_answer_and_an_error_only_at_its_deadline`.
- **HTTP head parsing is bounded**: `a_head_is_read_to_its_blank_line_the_end_or_the_bound` (`src/http.rs`).
- **OTLP exporter does not export its own spans** (a feedback loop): `the_exports_own_spans_stay_home` (`src/otel.rs`).
- **End-to-end check**: `scripts/verify-cell.sh` spawned three `parosd` on loopback and drove `parosctl`; it was never a CI job.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/bin/parosd/src/main.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/bin/parosd

Where to start reading: `src/main.rs` (module doc and `run`), `src/settings.rs`, `src/tunables.rs`, `tests/real_fs.rs`, then `README.md`. Its own map at the tag: `crates/bin/parosd/AGENTS.md`.
