# parosd

`publish = false`. The server: the **only** crate in the workspace that links
moonpool's `TokioProviders` (#206). The `paros` library stays free of
production providers and wasm-checkable; every driver runs here unchanged, so
the code the simulation tests is the code that ships. Nothing protocol-shaped
lives here — a decision that needs I/O policy belongs in `paros`'s
provider-generic driver, never in this crate.

## Map

- `src/main.rs` the `parosd` binary: one role per process (`node` →
  `run_journals`, `matchmaker` → `run_matchmaker`, `proxy` → `run_proxy`,
  `replica` → `run_replica`), `NoHooks` / `NoAudit`, `DriverTunables` and
  `JournalStoreConfig` defaults, `SIGTERM` / `SIGINT` → the shutdown token.
- `src/bin/paros.rs` a minimal client (`set-leader`, `write`, `read`,
  `inspect`): one at-most-once attempt per server, in order, until a verdict.
  The full CLI and the shipped client policy are #196.
- `src/topology.rs` `Topology`: the address books (`ID=IP:PORT`) and every
  role's configuration derived from them — the same rules as the sim's
  role map (the first journal is the deployment's; every other one is plain
  Multi-Paxos over the pool; `ProxyId(i)` / `ReplicaId(i)` by position).
- `src/stores.rs` `DirStores`, the production `JournalStores`: one
  `JournalStorage` directory per journal under the data dir; the operator's
  `BootKind` claim handed to each journal's first open only (re-opens are
  `ExistingMember`). No `create`: a created journal's claim needs #208's
  provisioning record, never the disk (a wiped node would rejoin as new).
- `src/exit.rs` the exit code per `RunError` (`sysexits.h`: 75 restart,
  78 operator, 70 fatal, 64 usage).
- `tests/laptop.rs` the smoke on the real binaries: node + matchmaker +
  replica, write, read, `SIGTERM`, restart as existing members, amnesia
  refused.

## Rules local to this crate

- Configuration is flags with a `PAROS_*` variable each; list values are
  comma-separated.
- The moonpool pin here is the same rev as in `crates/paros` and
  `crates/paros-sim`; advance every line together.
- Not yet here, each its own issue: provisioning and the interrupted-format
  rule (#208, the `--first-boot` flag stands in), `Config` durable at format
  (#207), hostnames and tunable overrides (#209), the uniform binary, Compose
  and the full CLI (#196), system journals (`SystemPlan` is `None`).
