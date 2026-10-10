# parosd

The daemon and its CLI (`publish = false`): **one uniform `parosd`** every machine runs, over
moonpool's `TokioProviders` and `paros::journal`'s stores on a real filesystem, plus `parosctl`.
Stack: `paros-core` ← `paros` ← **`parosd`**. The only crate that links `TokioProviders`; the
drivers, the stores and the machine phase are the library's. User-facing doc: `README.md`; the
image and the Compose toy are `Dockerfile` and `docker-compose.yml` at the repository root.

## Map

- `src/main.rs` → `paros::machine::run_machine` (the lifecycle is the library's, #246: format,
  amnesia, wait, serve) → tracing subscriber, runtime, name resolution, `SIGTERM`/`SIGINT`
  → shutdown token, `MachineError` → exit codes.
- `src/settings.rs` → `Settings`, `Layout`, `check_unknown` → `PAROS_*` variables (each also a
  `--flag`); an unknown `PAROS_*` variable is an error.
- The disk is the library's `paros::machine::ProviderDisk` over `TokioStorageProvider`, rooted
  at the data directory: the record `<data-dir>/machine` and the stores
  `<data-dir>/journals/<tenant>/<journal>/` (#235). No disk or store type lives here (#294).
- `src/resolve.rs` → `check_shape`, `resolve`, `resolve_all` → `HOST:PORT` (port required),
  names resolved once at startup (`parosd` its listen address; `parosctl` its servers and
  `init`'s members, where a name such as a Compose alias yields every address); shared with
  `parosctl` by `#[path]` (#209).
- `src/tunables.rs` → `from_env`, `variables` → `DriverTunables::production()` plus a
  `PAROS_<FIELD>[_MS]` override per field, refused below its floor (#209); the cell election's
  `PAROS_ELECTION_LEASE_MS`, `PAROS_ELECTION_RENEW_MS`, `PAROS_ELECTION_COMPACT_AFTER` (#240).
- `src/bin/parosctl/main.rs` → `parosctl` → global options, server ids discovered from
  `Inspect`, `init` vs the cell commands, exit codes.
- `src/bin/parosctl/init.rs` → `parosctl init [--members a,b,c] [--patience-ms N]`: resolves the
  founding members (default: the servers), runs `paros::client::initialize` over them and prints
  what it came to (`cell init`, the elected coordinator, the fleet steps; #229, #240, #246, #277).
- `src/bin/parosctl/fleet.rs` → `parosctl tenant create|delete|list` over
  `paros::client::fleet`; the session, refusal labels and endings `init` shares.
- `src/bin/parosctl/journal.rs` → `parosctl journal create|delete|list` over `paros::client::journals` (#210): the tenant named, found in the fleet directory; one request id per run, re-sent by the library until decided.
- `src/bin/parosctl/cell.rs` → `parosctl cell add-machine <addr>` over `paros::client::cell` (#216): registers an idle machine in the cell control journal, then admits it.
- `src/bin/parosctl/commands.rs` → one fn per cell command: `write`, `read`, `tail`, `truncate`,
  `set-leader`, `inspect`, `reconfigure`, `retire`.
- `src/bin/parosctl/output.rs` → `Printer` → text or one JSON document per answer (`--json`);
  diagnostics to stderr.
- `tests/real_fs.rs` → both storage contract suites on a real disk; a store dropped mid-batch
  reopens with every acked write; the machine record on a real disk.

## Entry points

- `parosd` with `PAROS_LISTEN`, `PAROS_DATA_DIR`, `PAROS_CLASS`, `PAROS_CAPACITY`,
  `PAROS_FAILURE_DOMAIN`, `PAROS_STORE_LAYOUT` and the tunable overrides (`settings.rs`). No
  role, no id, no peer: a machine is configured with no other machine (`PAROS_ID`,
  `PAROS_SEEDS` and `PAROS_RENDEZVOUS` are gone, #277).
- `parosctl [--servers HOST:PORT|ID=HOST:PORT,…] [--json] [--timeout-ms N] <command>`; servers
  also from `PAROSCTL_SERVERS`, the writer's leader uuid from `PAROSCTL_LEADER` / `--leader` (hex;
  drawn at random when absent, so a run that names none claims a term of its own, #241).
  `parosctl init` sends `CellInit` to the first founding member still idle.
- `RUST_LOG` filters both (`parosd` default `warn,parosd=info`, `parosctl` default `error`).

## Exit codes

- `parosd` (`src/main.rs`): `0` clean shutdown · `75` `RunError::Storage` or `MachineError::Storage` (restart) · `78` a
  refusal (do not restart: amnesia, a lost identity, a class change, an edited `Config`) · `1`
  `RunError::Infra` · `2` an invalid configuration.
- `parosctl` (`src/bin/parosctl/main.rs`, `Ending`): `0` success · `3` refused/not served · `4`
  ambiguous · `5` no server answered · `2` bad arguments · `1` no servers configured.

## Local rules

- **Interim (M8 → M9)**: the cell's journals are the cell control journal, the fleet tenant's,
  the election journal, every hosted tenant's control journal and the journals tenants create
  (#210), plain Multi-Paxos or a grid over the founding members; any other
  machine idles until `parosctl cell add-machine` admits it, and then, like every `stateless` one, waits for placement (#211, #212). The founding members
  elect the cell coordinator over the cell's election journal (#240); admin sessions still claim the cell control journal until
  admin calls become requests to it (#212, #225). Do not build on these as final; the machine record, the boot rule and
  `init`'s resumability stay.
- `parosctl` holds **no client policy**: redirects, retries, claims, ambiguity, reader resume
  and `init`'s patience are `paros::client`'s; a command only parses, calls the library,
  prints and maps an exit code.
- The boot claim is data (#208): every start after formation is `BootKind::ExistingMember`;
  only a cell's formation formats a store (through `paros::provision_store`), and only a
  created journal's first open is `FirstBoot`. The library refuses amnesia, a re-format and an
  edited `Config`.
- Names are resolved in this crate's configuration layer, never in a driver: the library sees
  socket addresses only.

## Tests & gates

- `cargo nextest run -p parosd`: unit tests and `tests/real_fs.rs`. No test spawns `parosd`
  processes: behaviour is proved in simulation.
- `cargo build -p parosd` is the shipped build (see the feature note below).
- CI's `image` job: `scripts/check-dockerfile-toolchain.sh`, `docker compose build`. The
  Compose toy is the user's demo, run by hand, never a test.

## Deps & pins (`Cargo.toml`)

- `paros` (`:12`); `moonpool-core` with `tokio-providers` + `select` (`:22`); `moonpool-rpc`
  `prost` (`:24`) — same rev as `paros` and `paros-sim`, advanced together.
- **Feature unification** (`Cargo.toml:18-21`): built with `paros-sim` in one workspace build,
  cargo unifies moonpool-core's features and this binary gets moonpool-sim's deterministic
  `select!`; `cargo build -p parosd` alone keeps tokio's macro verbatim.
- `clap` (`derive`, `env`), `tokio` (`rt-multi-thread`, `macros`, `signal`), `tokio-util`,
  `tracing`, `tracing-subscriber` (`env-filter`, `fmt`), `serde_json`; dev `futures`, `tempfile`.
