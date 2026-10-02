# parosd

The daemon and its CLI (`publish = false`): every role of a deployment over moonpool's
`TokioProviders` and `paros::journal`'s stores on a real filesystem, plus `parosctl`. Stack:
`paros-core` ← `paros` ← **`parosd`**. The only crate that links `TokioProviders`; the drivers
and stores are the library's, the same code the simulation runs. User-facing doc: `README.md`.

## Map

- `src/main.rs` → `parosd node|matchmaker|replica|proxy`, `parosd provision node|matchmaker|replica` → args, tracing subscriber, runtime, `SIGTERM`/`SIGINT` → shutdown token, exit codes.
- `src/deployment.rs` → `Deployment`, `Entry` → the address books (`--node`, `--matchmaker`, `--proxy`, `--replica`, `--journal`) and the derived core `Config`.
- `src/stores.rs` → `DirStores` (`JournalStores`) → `<data-dir>/journals/<id>/`, `matchmaker/`, `replica/`; a created journal is a first boot until `opened`, resolved from the disk at `load`.
- `src/record.rs` → `Record` → `<data-dir>/provisioned`: role, id, provisioned journals; rewritten atomically (#208).
- `src/bin/parosctl/main.rs` → `parosctl` → global options, `Client::connect`, exit codes.
- `src/bin/parosctl/commands.rs` → one fn per command: `write`, `read`, `tail`, `truncate`, `set-leader`, `inspect`, `reconfigure`, `retire`.
- `src/bin/parosctl/output.rs` → `Printer` → text or one JSON document per answer (`--json`); diagnostics to stderr.
- `tests/real_fs.rs` → both storage contract suites on a real disk; a store dropped mid-batch reopens with every acked write.
- `tests/deploy.rs` → one node, one matchmaker, one replica on a laptop, provisioned, driven by `parosctl --json`: write, read back, restart, refusals (amnesia, a second provision, an edited config, another identity's dir), `SIGTERM`.

## Entry points

- `parosd provision <role> --id N --data-dir DIR [--layout default|small] <deployment>`, once
  per identity; then `parosd <role>` with the same arguments. `parosd proxy` takes no data dir
  and is never provisioned. `PAROS_DATA_DIR` sets `--data-dir`.
- `parosctl [--servers ID=HOST:PORT,…] [--json] [--timeout-ms N] <command>`; servers also from
  `PAROSCTL_SERVERS`, the writer's owner id from `PAROSCTL_OWNER` (default 1).
- `RUST_LOG` filters both (`parosd` default `warn,parosd=info`, `parosctl` default `error`).

## Exit codes

- `parosd` (`src/main.rs:13-18`, `serve`): `0` clean shutdown · `75` `RunError::Storage`
  (restart) · `78` `RunError::Refused` (do not restart; resolve the claim) · `1`
  `RunError::Infra` / `SeamCrash` · `2` an invalid deployment or bad arguments.
- `parosctl` (`src/bin/parosctl/main.rs:12-18`, `Ending`): `0` success · `3` refused/not served
  · `4` ambiguous · `5` no server answered · `2` bad arguments · `1` no servers configured.

## Local rules

- `parosctl` holds **no client policy**: redirects, retries, claims, ambiguity and reader resume
  are `paros::client`'s; a command only parses, calls the library, prints and maps an exit code.
- The boot claim is data (#208): every start is `BootKind::ExistingMember`; only `parosd
  provision` formats (through `paros::provision_store`), and only a created journal's first
  open is `FirstBoot`. The record, never a directory's existence, says what was provisioned;
  an interrupted provision or a created journal's lost record is resolved by reading the disk.
  The library refuses amnesia, a re-format and an edited `Config`.
- The deployment is derived identically by every process, never typed twice: the derived
  `Config` is recorded at `format` and an edit is refused (#207).

## Tests & gates

- `cargo nextest run -p parosd` (`tests/deploy.rs` uses `CARGO_BIN_EXE_parosd` / `_parosctl`).
- `cargo build -p parosd` is the shipped build (see the feature note below).

## Deps & pins (`Cargo.toml`)

- `paros` (`:12`); `moonpool-core` with `tokio-providers` + `select` (`:22`); `moonpool-rpc`
  `prost` (`:24`) — same rev as `paros` and `paros-sim`, advanced together.
- **Feature unification** (`Cargo.toml:18-21`): built with `paros-sim` in one workspace build,
  cargo unifies moonpool-core's features and this binary gets moonpool-sim's deterministic
  `select!`; `cargo build -p parosd` alone keeps tokio's macro verbatim.
- `clap` (`derive`, `env`), `tokio` (`rt-multi-thread`, `macros`, `signal`), `tokio-util`,
  `tracing`, `tracing-subscriber` (`env-filter`, `fmt`), `serde_json`; dev `futures`, `tempfile`.
