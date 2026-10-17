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
- `src/http.rs` → `serve`, `route` → `PAROS_HTTP_LISTEN` (#366 (metrics)): `/metrics` (the machine's `paros::metrics::Metrics` as Prometheus text), `/healthz`, `/readyz` (`503` with the phase until the machine serves its cell); plain HTTP/1.1, `GET` and `HEAD`, no state, no sim path. `main.rs` always records metrics (`Metrics::on`).
- `src/otel.rs` → `Telemetry`, `exported` → `PAROS_OTLP_ENDPOINT`, `PAROS_OTLP_TRACES_ENDPOINT`, `PAROS_OTLP_METRICS_ENDPOINT`, `PAROS_OTLP_INTERVAL_MS` (#366 (metrics), decided on 2026-10-15: OTLP beside Prometheus): the `tracing` spans through `tracing-opentelemetry` (the exporter's own crates filtered out), and one observable instrument per `paros::metrics::Family` that reads the same registry at each push (a counter drops `_total`); started inside the runtime, flushed after `run` returns.
- `build.rs` → `PAROS_COMMIT` (from the environment, else `git rev-parse HEAD`), the commit `parosctl bench --json` reports (#194 (throughput benchmark)).
- `src/settings.rs` → `Settings`, `check_unknown` → `PAROS_*` variables (each also a
  `--flag`); an unknown `PAROS_*` variable is an error. `PAROS_TLS_CERT`, `PAROS_TLS_KEY`, `PAROS_TLS_CA` (all or none), `PAROS_TLS_CLIENT_AUTH` (`required` by default) → `Settings::transport` (PR #509 (mutual TLS)). `PAROS_ROOT_PUBLIC_KEY` (`.public` files, comma-separated) → `Settings::pin`, the machine's `AdminPin` over `BiscuitAuthz` at the start's wall clock (#245 (Biscuit tokens)); none: open.
- The disk is the library's `paros::machine::ProviderDisk` over `TokioStorageProvider`, rooted
  at the data directory: the record `<data-dir>/machine` and the stores
  `<data-dir>/journals/<tenant>/<journal>/` (#235). No disk or store type lives here (#294).
- `src/resolve.rs` → `check_shape`, `resolve`, `resolve_all` → `HOST:PORT` (port required),
  the listen address resolved once at startup; an advertised address and `init`'s members kept as names and resolved at dial time (#257), except a name for several machines (a Compose alias), which `expand` turns into their IPs; shared with
  `parosctl` by `#[path]` (#209).
- `src/tunables.rs` → `from_env`, `variables` → `DriverTunables::production()` plus a
  `PAROS_<FIELD>[_MS]` override per field, refused below its floor (#209); the cell election's
  `PAROS_ELECTION_LEASE_MS`, `PAROS_ELECTION_RENEW_MS`, `PAROS_ELECTION_COMPACT_AFTER` (#240).
- `src/bin/paros-frontend.rs` → `paros-frontend` (#192 (the frontend)): `paros::frontend::run_frontend` over Tokio with `paros_authz_biscuit::BiscuitAuthz`; `PAROS_FRONTEND_LISTEN`, `PAROS_FRONTEND_CELL` (the founding members), `PAROS_FRONTEND_ROOT_PUBLIC_KEY` (`.public` files: the pin until it read the universe entry's keys; optional since #245 (Biscuit tokens)), `PAROS_FRONTEND_TIMEOUT_MS`; its wall clock is `SystemTime::now()` at start plus the provider's time.
- `src/bin/parosctl/main.rs` → `parosctl` → global options (`--frontends`/`PAROSCTL_FRONTENDS`: `write`, `read`, `tail`, `truncate` and `set-leader` go through the frontends with the token in `PAROS_TOKEN`, #192 (the frontend)), the offline `key`, `token`, `tls` and `store`
  commands (no server, no client runtime), the admin `--token`/`--token-file` (`PAROS_TOKEN`, `PAROS_TOKEN_FILE`) that `init` and `cell` present to a pinned machine (#245 (Biscuit tokens)), server ids discovered from `Inspect`, `init` vs the cell
  commands, exit codes.
- `src/bin/parosctl/universe.rs` → `parosctl universe key list|add|remove` (#245 (Biscuit tokens)): the universe entry's root public keys, by label, each change one idempotent write through `paros::client::fleet`; `read_key` reads a `.public` or `.private` file, shared with `init`.
- `src/bin/parosctl/init.rs` → `parosctl init [--members a,b,c] [--patience-ms N] [--root-public-key FILE]` (the universe's first root key, #245): resolves the
  founding members (default: the servers), runs `paros::client::initialize` over them and prints
  what it came to (`cell init`, the elected coordinator, the fleet steps; #229, #240, #246, #277).
- `src/bin/parosctl/fleet.rs` → `parosctl tenant create|delete|tag` over
  `paros::client::fleet` (`create --tag k:v`, `tag NAME [k:v…]`, #575 (key:value tags)); the session, refusal labels and endings `init` shares.
- `src/bin/parosctl/journal.rs` → `parosctl journal create|delete|tag|list` over `paros::client::journals` (#210): the tenant named, found in the fleet directory; one request id per run, re-sent by the library until decided. `create --store-durability ordered|batched` (#599 (durability mode per journal)), `create --tag`, `tag TENANT NAME [k:v…]` (through the frontends with the tenant's token when `--frontends` is given, `journals::tag_through`), `list -l SELECTOR` (`tenant.<key>` reads the tenant's tags) (#575).
- `src/bin/parosctl/cell.rs` → `parosctl cell add-machine <addr>` over `paros::client::cell` (#216): registers an idle machine in the cell control journal, then admits it. `parosctl cell drain-machine <machine>` (#474 (journals on admitted machines)): marks a registered machine draining (by name or full id), and the cell coordinator moves every journal off it; a founding member is never drained. `parosctl cell tag-machine <machine> [k:v…]` and `cell tag <cell> [k:v…]` set user tags (#575 (key:value tags)). `parosctl cell remove-machine <machine>` (#359): retires a drained machine that holds no slot, for good. `parosctl cell maintenance <machine> --for <duration>` (or `--zone`; `--for 0` ends it) (#359): opens a maintenance window, and the coordinator re-places no role of a down machine it covers.
- `src/bin/parosctl/entry.rs` → `parosctl resolve <tenant>` over `paros::client::resolve` (#216): the servers are the entry endpoint; prints the tenant's universe and cell by name (short hex only when the answer names none) and the cell's machines.
- `src/bin/parosctl/tls.rs` → `parosctl tls init|ca|issue|show|verify`, offline (PR #509 (mutual TLS)): `init` writes the authority and a leaf per machine, frontend, admin and client in one directory and prints each process's variables; never overwrites, keys 0600; `show` reads with `x509-parser`; `verify` runs rustls's `WebPkiServerVerifier` (name `paros`) and `WebPkiClientVerifier`. The global `--tls-ca`, `--tls-cert`, `--tls-key` (`PAROSCTL_TLS_*`) build the client runtime's transport (`paros::tls::Files::dialing`). `paros-frontend` reads `PAROS_FRONTEND_TLS_*`.
- `src/bin/parosctl/store.rs` → `parosctl store inspect <data-dir> [--records] [--scratch]` (#365 (offline store inspection)): offline, its own current-thread runtime; `paros::inspect::inspect` over `TokioStorageProvider` with a scratch directory under the system's temporary directory (removed after; refused inside the data directory), names from `paros::inspect::names` through `Labels::of`; exit 3 when the record or a store does not read.
- `src/bin/parosctl/key.rs` → `parosctl key generate|show` (#400): root key pairs, offline, entropy from the provider's random source; `<label>.private` (mode 0600) and `<label>.public`, written once; outputs name a key by its label, never its id.
- `src/bin/parosctl/token.rs` → `parosctl token mint|derive|inspect` (#400): Biscuit tokens over `paros-authz-biscuit`, offline; a token from the argument, `--token-file` or `PAROS_TOKEN`.
- `src/bin/parosctl/views.rs` → `Asker`, `parosctl machine list|show`, `cell list|show`, `tenant list|show`, `roles [--cell|--tenant|--machine]` (#399): `-l SELECTOR` on the lists and the tags in every table and `--json` (`CLASS` and `ZONE` read from `paros/class` and `paros/zone`, #575 (key:value tags)); one `View` to the servers' cell, printed as a table (`output::table`) or JSON; the global `--as-tenant NAME` asks in one tenant's scope. `machine list|show` also ask every machine for `Load` (#424): the `CPU`, `MACHINE` and `DISK` columns, a `load` block, `no metrics` for a machine that does not answer.
- `src/bin/parosctl/status.rs` → `parosctl status [--tenant t] [--cell c]` (#191 (status)): the universe line when the servers' cell hosts its directory, the cell (coordinator, machines available, slots per class) or one tenant (its machines and footprint), and every journal's desired, current and available acceptors with the word the cell shows (`healthy (now degraded)` while hysteresis holds a word back); `--json` adds a `status` block. A tenant's status adds a usage table per journal, summed from every machine's `Load` (`N of M machines metered`; `usage` in `--json`, #366 (metrics)).
- `src/bin/parosctl/bench.rs` → `parosctl bench` (#517 (`parosctl bench`), #194 (throughput benchmark)), the load generator: clients over `paros::client`, closed loop or open loop (`--rate`, each latency counted from the moment the call was due), `write` (client `i` to journal `i % N`, one shared `paros::client::Pipeline` per journal, so its clients' writes are in flight at once, #597 (several writes in flight per journal); the report names the writes in flight per journal and the writes sent again), `read-tail` and `read-start` (every byte checked) and `set-leader` drawn by `--mix` weights (`bench/mix.rs`), `truncate` on `--truncate-every`; the 1 s tick, the ramp, Ctrl-C, `--tolerate-errors`; `--json` prints one report (`bench/summary.rs`: per call ops, bytes, errors, mean, p50, p90, p99, p99.9 and max, with the commit, the command line, the CPU count, the kernel and the stores' filesystem). `bench/local.rs` → `--local N`: N shipped `run_machine`s on loopback in a thread and multi-thread runtime of their own, stores in a new directory under `--local-dir` (deleted after), formed by `paros::client::initialize`; CI's `test` job runs `bench --local 3 --duration 5s`; `LocalCell::servers` waits until every machine answers its `Inspect`. `bench/topology.rs` → `Topology`, `Topologies` → `--topology 3x1,3x3` (`ROWSxCOLUMNS`, a `Durability` of `F = ROWS/2` over `COLUMNS` partitions): each runs in turn under one seed (`Plan`, `run_one`, `finish`), a new local cell per topology, then one total row per topology and call. `bench/setup.rs` creates the tenant and the journals `bench-RxC-K` when missing (`--store-durability`, #599) and claims them; `bench/histograms.rs` → per-client HDR histograms swapped and merged each tick, the `--histograms` JSON lines (V2, base64; temp name, fsync, rename) and `read`, their reading half; `bench/records.rs` → `Rng` (SplitMix64 from `(seed, client)`), record `i` a pure function of `(seed, journal, i)` and `check`. No sim path: it calls only `paros::client`.
- `src/bin/parosctl/labels.rs` → `Labels` → the names every command prints (#399): one admin cell view per command; short hex only when no view names an entity.
- `src/bin/parosctl/commands.rs` → one fn per cell command: `write`, `read`, `tail`, `truncate`,
  `set-leader`, `inspect`, `reconfigure`, `retire`.
- `src/bin/parosctl/names.rs` → `JournalRef`, `NodeRef` (a machine name first, then a hex prefix, #399) → a journal argument as a name (`TENANT/JOURNAL`, `paros://…`, resolved through `paros::client::names` with operator rights, or sent as it is to a frontend, #192 (the frontend)) or as ids (`id:TENANT/JOURNAL`, hex prefixes matched among the journals `parosctl` can list); a node id as a hex prefix of a server's id (#239).
- `src/bin/parosctl/output.rs` → `Printer`, `short` → text or one JSON document per answer (`--json`);
  diagnostics to stderr; `table` for the views; text names every entity (#399), JSON carries names and whole ids; short hex only when no name is known.
- `tests/real_fs.rs` → both storage contract suites on a real disk; a store dropped mid-batch
  reopens with every acked write; the machine record on a real disk.

## Entry points

- `parosd` with `PAROS_LISTEN`, `PAROS_ADVERTISE` (#257), `PAROS_DATA_DIR`, `PAROS_CLASS`, `PAROS_CAPACITY`,
  `PAROS_ZONE`, `PAROS_RACK` (its `paros/zone` and `paros/rack` tags, #575 (key:value tags)), `PAROS_NAME` (the machine's name, default the advertised host, #399), and the tunable overrides (every store takes the default geometry, #600 (drop the small layout)) (`settings.rs`). No
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
  machine idles until `parosctl cell add-machine` admits it, and then, like every `stateless` one, waits for placement (#211, #474). The founding members
  elect the cell coordinator over the cell's election journal (#240); `cell add-machine` and `cell drain-machine` still claim the cell control journal until
  admin calls become requests to it (#212, #225); the fleet operations only read it (#513, #225). Do not build on these as final; the machine record, the boot rule and
  `init`'s resumability stay.
- `parosctl` holds **no client policy**: redirects, retries, claims, ambiguity, reader resume
  and `init`'s patience are `paros::client`'s; a command only parses, calls the library,
  prints and maps an exit code.
- The boot claim is data (#208): every start after formation is `BootKind::ExistingMember`;
  only a cell's formation formats a store (through `paros::provision_store`), and only a
  created journal's first open is `FirstBoot`. The library refuses amnesia, a re-format and an
  edited `Config`.
- The listen address is resolved in this crate's configuration layer. Peer and server names are
  resolved at dial time through `paros::Names` over `TokioResolver` (#257).

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
- `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp` (`grpc-tonic`), `tracing-opentelemetry`, `prometheus` (the registry's types): the OTLP push, `parosd` only.
- `clap` (`derive`, `env`), `tokio` (`rt-multi-thread`, `macros`, `signal`), `tokio-util`,
  `tracing`, `tracing-subscriber` (`env-filter`, `fmt`), `serde_json`, `hdrhistogram` and
  `base64` (`parosctl bench`); dev `futures`, `tempfile`.
