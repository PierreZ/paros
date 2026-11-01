# parosd

The daemon (`publish = false`): **one uniform `parosd`** every machine runs, on one
compio thread over moonpool's `CompioProviders` and `paros::storage`'s journal stores on a real
filesystem. It depends on `paros` and `paros-authz-biscuit` (#722 (three products)). The CLI is `crates/bin/parosctl`, the
load generator `crates/bin/paros-bench`. The only crate that
links `CompioProviders`; the
drivers, the stores and the machine phase are the library's. User-facing doc: `README.md`; the
image and the Compose toy are `Dockerfile` and `docker-compose.yml` at the repository root.

## Map

- `src/main.rs` → `paros::machine::run_machine` (the lifecycle is the library's, #246: format,
  amnesia, wait, serve) → tracing subscriber, runtime, name resolution, `SIGTERM`/`SIGINT`
  → shutdown token, `MachineError` → exit codes. Every binary runs on one thread: a compio
  runtime (io_uring on Linux, polling where a seccomp profile refuses it), decided on 2026-10-22.
- `src/http.rs` → `serve`, `route` → `PAROS_HTTP_LISTEN` (#366 (metrics)): `/metrics` (the machine's `paros::observe::metrics::Metrics` as Prometheus text), `/healthz`, `/readyz` (`503` with the phase until the machine serves its cell); plain HTTP/1.1, `GET` and `HEAD`, no state, no sim path. `main.rs` always records metrics (`Metrics::on`).
- `src/otel.rs` → `Telemetry`, `exported` → `PAROS_OTLP_ENDPOINT`, `PAROS_OTLP_TRACES_ENDPOINT`, `PAROS_OTLP_METRICS_ENDPOINT`, `PAROS_OTLP_INTERVAL_MS` (#366 (metrics), decided on 2026-10-15: OTLP beside Prometheus): the `tracing` spans through `tracing-opentelemetry` (the exporter's own crates filtered out), and one observable instrument per `paros::observe::metrics::Family` that reads the same registry at each push (a counter drops `_total`); started inside the runtime, flushed after `run` returns. tonic needs Tokio, so when an exporter is configured `Telemetry::start` starts a one-worker Tokio runtime (`parosd-otlp`) beside the compio thread; without one, no Tokio runtime starts.
- `src/settings.rs` → `Settings`, `check_unknown` → `PAROS_*` variables (each also a
  `--flag`); an unknown `PAROS_*` variable is an error. `PAROS_TLS_CERT`, `PAROS_TLS_KEY`, `PAROS_TLS_CA` (all or none; each a file, or its `_PEM` twin with the PEM text itself, `paros::net::tls::Pem::pick`), `PAROS_TLS_CLIENT_AUTH` (`required` by default) → `Settings::transport` (PR #509 (mutual TLS)). `PAROS_ROOT_PUBLIC_KEY` (`.public` files, comma-separated) and `PAROS_ROOT_PUBLIC_KEY_JSON` (one key, the file's text itself) → `Settings::pin`, the machine's `AdminPin` over `BiscuitAuthz` at the start's wall clock (#245 (Biscuit tokens)); none: open.
- The disk is the library's `paros::machine::ProviderDisk` over `CompioStorageProvider`, rooted
  at the data directory: the record `<data-dir>/machine` and the stores
  `<data-dir>/journals/<tenant>/<journal>/` (#235). No disk or store type lives here (#294).
- Name resolution is the library's (`paros::net::resolve`: `check_shape`, `resolve`, `resolve_all`,
  `expand`, #209, #257): `HOST:PORT` (port required), the listen address resolved once at
  startup; an advertised address and `init`'s members kept as names and resolved at dial time,
  except a name for several machines (a Compose alias), which `expand` turns into their IPs.
- `src/tunables.rs` → `from_env`, `variables` → `DriverTunables::production()` plus a
  `PAROS_<FIELD>[_MS]` override per field, refused below its floor (#209); the cell election's
  `PAROS_ELECTION_LEASE_MS`, `PAROS_ELECTION_RENEW_MS`, `PAROS_ELECTION_COMPACT_AFTER` (#240).
- The frontend role (#723 (frontend as a parosd role)): `Settings::frontend` builds the `FrontendRole` every machine serves on its own edge, `paros_authz_biscuit::BiscuitAuthz` over the keys of `PAROS_ROOT_PUBLIC_KEY` and `PAROS_ROOT_PUBLIC_KEY_JSON` (the pin until it read the universe entry's keys), 5 s per forwarded attempt; its wall clock is `SystemTime::now()` at start plus the provider's time, shared with the bootstrap pin.
- `tests/real_fs.rs` → both storage contract suites on a real disk; a store dropped mid-batch
  reopens with every acked write; the machine record on a real disk.

## Entry points

- `parosd` with `PAROS_LISTEN`, `PAROS_ADVERTISE` (#257), `PAROS_DATA_DIR`, `PAROS_ROLES`, `PAROS_CAPACITY` (ignored since #837 (remove role slots)),
  `PAROS_ZONE`, `PAROS_RACK` (its `paros/zone` and `paros/rack` tags, #575 (key:value tags)), `PAROS_NAME` (the machine's name, default the advertised host, #399), and the tunable overrides (every store takes the default geometry, #600 (drop the small layout)) (`settings.rs`). No
  role, no id, no peer: a machine is configured with no other machine (`PAROS_ID`,
  `PAROS_SEEDS` and `PAROS_RENDEZVOUS` are gone, #277).
- `RUST_LOG` filters it (default `warn,parosd=info`). `parosctl`'s entry points are in
  `crates/bin/parosctl/AGENTS.md`.

## Exit codes

- `parosd` (`src/main.rs`): `0` clean shutdown · `75` `RunError::Storage` or `MachineError::Storage` (restart) · `78` a
  refusal (do not restart: amnesia, a lost identity, a class change, an edited `Config`) · `1`
  `RunError::Infra` · `2` an invalid configuration.

## Local rules

- **Interim (M8 → M9)**: the cell's journals are the cell control journal, the fleet tenant's,
  the election journal, every hosted tenant's control journal and the journals tenants create
  (#210), plain Multi-Paxos or a grid over the founding members; any other
  machine idles until `parosctl cell add-machine` admits it, and then, like every `stateless` one, waits for placement (#211, #474). The founding members
  elect the cell coordinator over the cell's election journal (#240); `cell add-machine` and `cell drain-machine` still claim the cell control journal until
  admin calls become requests to it (#212, #225); the fleet operations only read it (#513, #225). Do not build on these as final; the machine record, the boot rule and
  `init`'s resumability stay.
- The boot claim is data (#208): every start after formation is `BootKind::ExistingMember`;
  only a cell's formation formats a store (through `paros::storage::provision_store`), and only a
  created journal's first open is `FirstBoot`. The library refuses amnesia, a re-format and an
  edited `Config`.
- The listen address is resolved in this crate's configuration layer. Peer and server names are
  resolved at dial time through `paros::Names` over `CompioResolver` (#257).

## Tests & gates

- `cargo nextest run -p parosd`: unit tests and `tests/real_fs.rs`. No test spawns `parosd`
  processes: behaviour is proved in simulation.
- `cargo build -p parosd` is the shipped build (see the feature note below).
- CI's `image` job: `scripts/check-dockerfile-toolchain.sh`, `docker compose build`. The
  Compose toy is the user's demo, run by hand, never a test. It runs with mutual TLS and a
  pinned root key out of the box: the pre-generated, public demo set in `demo/keys/`, passed
  as text in `*_PEM`, `*_JSON` and `PAROS_TOKEN` (no mounted file).

## Deps & pins (`Cargo.toml`)

- `paros` with `tls`; `paros-authz-biscuit` (the admin pin and the frontend's authorizer);
  `moonpool-core` with `compio-providers` + `select`, from the workspace pin.
- **Feature unification** (`Cargo.toml`): built with `paros-sim` in one workspace build,
  cargo unifies moonpool-core's features and this binary gets moonpool-sim's deterministic
  `select!`; `cargo build -p parosd` alone keeps tokio's macro verbatim.
- `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp` (`grpc-tonic`), `tracing-opentelemetry`, `prometheus` (the registry's types): the OTLP push, `parosd` only.
- `compio` (`runtime`, `time`, `signal`, `io-uring`, `polling`): the runtime of every binary;
  `futures` (the HTTP server's stream I/O); `libc` (`SIGTERM`); `tokio` (`rt-multi-thread`)
  only for the OTLP exporters' runtime.
- `clap` (`derive`, `env`), `tokio-util` (`CancellationToken`),
  `tracing`, `tracing-subscriber` (`env-filter`, `fmt`); dev `tempfile`.
