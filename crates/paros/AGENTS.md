# paros

The library: `pub use paros_core::*` plus everything that does I/O — the provider-generic
drivers for the four roles, the storage seam and its stores (in-memory and the durable
`journal`), the RPC contract, the client (`paros::client`) and the system journals' folds
(`paros::system`). Written once over moonpool's `P: Providers`; the same code runs in
production (`parosd`) and in simulation (`paros-sim`). Stack: `paros-core` ← **`paros`** ←
`paros-sim` ← runner; `paros` ← `parosd`. Stays wasm-checkable and provider-free.

## Map

- `driver/mod.rs` → `run_node`, `run_journals`, `RunError`, `BootKind` → the node loop; one journal or a static list (#188).
- `driver/journals.rs` → `JournalStores`, `SingleStore` → per-journal runtime and quarantine (`quarantine_ticks`).
- `driver/system.rs` → `SystemPlan` → system-journal follower; applies directory/registry folds (#189).
- `driver/{boot,ready,report}.rs` → format-marker check, the `Ready` I/O side in persist-before-send order, boot report.
- `driver/transport.rs` → `PeerMailbox`, `LaneOpener`, `peer_address` → keep-newest lanes per journal, round-robin.
- `driver/{matchmaking,handover,operator,events}.rs` → matchmaker wire, set handover, Reconfigure/Retire/Inspect, events.
- `driver/edge.rs` → `RpcEdge`, `NodeInbox`, `ReplicaInbox`, `MatchmakerInbox` → the inbound edge, polled as a `select!` arm.
- `driver/reply.rs` → `answer`, `match_answer`, `maybe_duplicate` → the one client-reply seam.
- `driver/calls.rs` → held `Write`/`SetLeader`/`Truncate`, answered with the verdict their slot folded to (#204).
- `driver/log_reads.rs` → `JournalReads` → the public `Read`: quorum-confirmed, served from the fold, long-polled.
- `driver/config.rs` → `DriverTunables` → every driver cadence/budget, with production defaults.
- `hooks.rs` → `DriverHooks`, `NoHooks`, `Seam` (four), `HandoffContext`, `Reply` → BUGGIFY prong-1 surface.
- `audit.rs` → `Audit`, `NoAudit` → the observation port.
- `storage/mod.rs` → `LogStorage`, `StorageError`, `StorageRecord`, `WriteOutcome` → the async seam.
- `storage/mem.rs` → `MemStorage` · `storage/contract.rs` → `storage_contract_suite`.
- `matchmaker/{mod,storage}.rs` → `run_matchmaker`, `MatchmakerStorage`, `MemMatchmakerStorage`, `matchmaker_storage_contract_suite`.
- `proxy/mod.rs` → `run_proxy`, `ProxyConfig` → Phase-2 subset, nothing durable (#142).
- `replica_tier/mod.rs` → `run_replica` → learner subset over a `LogStorage`; serves `Read` (#144).
- `rpc/methods.rs` → one `RpcMethod` per call, `WellKnownMethod` ids (public `0x5041_00xx`, internal `0x5041_01xx`, matchmaker `0x5041_02xx`; retired ids never reused).
- `rpc/inbound.rs` → `Inbound`, `ReplySender`, `serve_deliveries`, `rpc_config`, `MAX_FRAME_BYTES`.
- `rpc/client.rs` → `NodeClient` (one at-most-once attempt per call), `MatchmakerClient`.
- `rpc/codec.rs` → shared scalar codecs (ballot, party, quorum system, config, command).
- `rpc/consensus.rs` → `Message` ↔ protobuf · `rpc/matchmaker_codec.rs` → matchmaker wire ↔ protobuf.
- `rpc/tests.rs` → round-trip tests; malformed input refused.
- `client/mod.rs` → `Client`, `ClientTunables`, `Retarget`, `LeaderHint` → policy loops: `write`, `resolve`, `read_any`, `read_until`, `claim`, `set_leader`, `truncate`, `reconfigure*`, `inspect`, `retire`; `*_attempt` one-shot calls.
- `client/outcome.rs` → `WriteOutcome`, `ReadOutcome`, … → every reply judged once.
- `client/writer.rs` → `Writer` (`stale_entry` is the explicit misbehaviour) · `client/reader.rs` → `Reader`, `ReaderOutcome::Gap`.
- `client/observer.rs` → `CallObserver`, `NoObserver` · `client/tests.rs` → the pure parts pinned.
- `system.rs` → `SystemCommand`, `Directory`, `Registry` → pure folds of journals 1 and 2.
- `corruption.rs` → `classify_log` → CTRL record classification.
- `journal/mod.rs` → `JournalStoreConfig`, `JournalBootFacts` · `journal/node.rs` → `JournalStorage` · `journal/matchmaker.rs` → `JournalMatchmakerStorage`.
- `journal/{frame,plan,node_image}.rs` → record ↔ entry codec, boot fold start, node records + per-kind corruption table.
- `journal/tests.rs` → both contract suites, targeted damage, crash loop under two fault models on `SimStorageProvider`.
- `proto/{common,paros,internal,matchmaker,system}.proto` → compiled by `build.rs` with `prost-build`.

## Public surface

`run_node`, `run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`; `paros::client`;
`paros::system`; `paros::journal`; `paros::wire::{methods, common, public, internal,
matchmaker, system}`; the RPC request/ack types (`lib.rs`).

## Local rules

- A new driver decision is a `DriverHooks` method, consulted only on the node loop where its
  answer is observable; `H: DriverHooks` is deliberately not `Send + 'static` (`hooks.rs`).
- A new tunable is a `DriverTunables` field with a default (`driver/config.rs`) and a
  `buggify_knob!` in `paros-sim`'s `NodeShape`; a new durability boundary is a `Seam` variant.
- Every call is one at-most-once attempt (`try_get_reply`), never `get_reply`.
- `paros::client` draws no randomness: every choice is the caller's.
- Storage implementations pass both contract suites.
- Spans are non-optional (`#[tracing::instrument(skip_all, fields(..))]`); see root *Tracing spans*,
  *Audit doctrine*.

## Tests & gates

- `cargo nextest run -p paros` — `rpc/tests.rs`, `client/tests.rs`, `journal/tests.rs`, inline tests.
- `PAROS_JOURNAL_CRASH_SEED=<n>` (one seed) / `PAROS_JOURNAL_CRASH_SEEDS=<n>` (`1..=n`):
  the journal crash loop (`journal/tests.rs:473`).
- `cargo check --target wasm32-unknown-unknown -p paros` (CI `portability`).
- Building needs `protoc` (`build.rs` runs `prost-build`): the flake ships `protobuf` (`flake.nix:46`);
  on the web see root *Environment & Nix* (`PROTOC`).

## Deps & pins (`Cargo.toml`)

- `paros-core` with `tracing` + `serde` (`:17`); `prost` (`:34`), `postcard` (`:37`), `serde`
  (`:38`), `crc32c` (`:39`), `tracing` (`:42`), `tokio` `sync` (`:45`), `tokio-util` (`:48`).
- moonpool, rev `7a066e9`: `moonpool-core` (`select`, `:26`), `moonpool-rpc` (`prost`, `:28`),
  `moonpool-journal` (`:32`), dev `moonpool-sim` (`:56`).
- Dev: `futures` executor (`:53`), `tokio` `rt`+`macros` (`:57`). Build: `prost-build` (`:60`).
- The pin is **eight lines**: four here, `crates/paros-sim/Cargo.toml:20,29`,
  `crates/parosd/Cargo.toml:22,24` — advance every line together.
