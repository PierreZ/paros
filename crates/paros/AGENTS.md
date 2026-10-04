# paros

The library: `pub use paros_core::*` plus everything that does I/O — the provider-generic
drivers for the four roles, the storage seam and its stores (in-memory and the durable
`journal`), the RPC contract, the client (`paros::client`) and the system journals' folds
(`paros::system`). Written once over moonpool's `P: Providers`; the same code runs in
production (`parosd`) and in simulation (`paros-sim`). Stack: `paros-core` ← **`paros`** ←
`paros-sim` ← runner; `paros` ← `parosd`. Stays wasm-checkable and provider-free.

## Map

- `driver/mod.rs` → `run_node`, `run_journals`, `RunError`, `BootKind` → the node loop; one journal or a static list (#188).
- `driver/journals.rs` → `JournalStores` (`opened`: a store passed its boot, #208), `SingleStore` → per-journal runtime and quarantine (`quarantine_ticks`).
- `driver/system.rs` → `SystemPlan` (`cell_id`, reported by `Inspect`) → system-journal follower; applies directory/registry folds, folds meta (#189, #229).
- `provision.rs` → `provision_store`, `provision_matchmaker_store`, `Provisioned` → format a store ahead of its first start; an interrupted run resumes from the disk (#208).
- `driver/{boot,ready,report}.rs` → format-marker check, the `Ready` I/O side in persist-before-send order, boot report.
- `driver/transport.rs` → `PeerMailbox`, `LaneOpener`, `peer_address` → keep-newest lanes per journal, round-robin.
- `driver/{matchmaking,handover,operator,events}.rs` → matchmaker wire, set handover, Reconfigure/Retire/Inspect, events.
- `driver/edge.rs` → `RpcEdge`, `NodeInbox`, `ReplicaInbox`, `MatchmakerInbox` → the inbound edge, polled as a `select!` arm.
- `driver/reply.rs` → `answer`, `match_answer`, `maybe_duplicate` → the one client-reply seam.
- `driver/calls.rs` → held `Write`/`SetLeader`/`Truncate`, answered with the verdict their slot folded to (#204).
- `driver/log_reads.rs` → `JournalReads` → the public `Read`: quorum-confirmed, served from the fold, long-polled.
- `driver/config.rs` → `DriverTunables` → every driver cadence/budget; `default()` is the sim's baseline · `driver/tunables.rs` → `DriverTunables::production`, `check_floors`, `BelowFloor` → the shipped profile and the floors (#209).
- `hooks.rs` → `DriverHooks`, `NoHooks`, `Seam` (four), `HandoffContext`, `Reply` → BUGGIFY prong-1 surface.
- `audit.rs` → `Audit`, `NoAudit` → the observation port.
- `storage/mod.rs` → `LogStorage`, `StorageError`, `StorageRecord`, `WriteOutcome` → the async seam.
- `storage/mem.rs` → `MemStorage` · `storage/contract.rs` → `storage_contract_suite`.
- `matchmaker/{mod,storage}.rs` → `run_matchmaker`, `MatchmakerStorage`, `MemMatchmakerStorage`, `matchmaker_storage_contract_suite`.
- `proxy/mod.rs` → `run_proxy`, `ProxyConfig` → Phase-2 subset, nothing durable (#142).
- `replica_tier/mod.rs` → `run_replica` → learner subset over a `LogStorage`; serves `Read` (#144).
- `rpc/methods.rs` → one `RpcMethod` per call, `WellKnownMethod` ids (public `0x5041_00xx`, internal `0x5041_01xx`, matchmaker `0x5041_02xx`, machine `0x5041_03xx`; retired ids never reused).
- `machine.rs` → `wait_for_cell`, `MachineFacts`, `CellPlan`, `CellLedger`, `Class`, `CELL_CONTROL`, `META_CONTROL` → the machine before its cell: `Identify`, `Init` (a seed forms the cell over the seeds, resumable), `FormCell` (#196, #216); not in the simulation yet.
- `rpc/inbound.rs` → `Inbound`, `ReplySender`, `serve_deliveries`, `rpc_config`, `MAX_FRAME_BYTES`.
- `rpc/client.rs` → `NodeClient` (one at-most-once attempt per call), `MatchmakerClient`.
- `rpc/codec.rs` → shared scalar codecs (ballot, party, quorum system, config, command).
- `rpc/consensus.rs` → `Message` ↔ protobuf · `rpc/matchmaker_codec.rs` → matchmaker wire ↔ protobuf.
- `rpc/tests.rs` → round-trip tests; malformed input refused.
- `client/mod.rs` → `Client`, `ClientTunables`, `Retarget`, `LeaderHint` → policy loops: `write`, `resolve`, `read_any`, `read_until`, `claim`, `set_leader`, `truncate`, `reconfigure*`, `inspect`, `retire`; `*_attempt` one-shot calls.
- `client/outcome.rs` → `WriteOutcome`, `ReadOutcome`, … → every reply judged once.
- `client/writer.rs` → `Writer` (`truncate` carries the owner's fence, #228; `stale_entry` and `stale_truncate_request` are the explicit misbehaviours) · `client/reader.rs` → `Reader`, `ReaderOutcome::Gap`.
- `client/observer.rs` → `CallObserver`, `NoObserver` · `client/tests.rs` → the pure parts pinned.
- `client/checkpoint.rs` → `Checkpointable`, `Folder`, `Checkpointer`, `CheckpointRecord` (`MAGIC`, `Inline` / `Ref`), `load` → checkpoint and truncate for any journal owner (#230); `Folder` is also the registry follower's fold.
- `client/bootstrap.rs` → `init`, `discover`, `claim_cell`, `register_fleet`, `TOY_JOURNAL` → `parosctl init`'s calls (the cell, then the fleet steps, #229) and server ids learned from `Inspect.node` (#196).
- `client/fleet.rs` → `CellRegistration`, `TenantCreation`, `TenantRemoval`, `FleetStep`, `load_meta` → the fleet's resumable operations over meta and the cell control journal, one entry per step (#229).
- `system/mod.rs` → `DIRECTORY`, `REGISTRY`, `META`, `SystemEvent` · `system/command.rs` → `SystemCommand`, `FleetContext` → one record per position.
- `system/directory.rs` → `Directory` → a tenant's journal names; a create carries its drawn id (#235).
- `system/registry/mod.rs` → `Registry` → the cell control journal: nodes by `node_id` with class, capacity and bookings (#211); `Checkpointable` (#230) · `system/registry/fleet.rs` → `FleetRegistration`, `HostedTenant` → its fleet half: the cell's registration and hosted tenants (#229).
- `system/meta.rs` → `Meta`, `TenantState`, `CellState`, `METADATA_VERSION` → the fleet's directory (`1/1`): cells and tenants with their states (#229); `Checkpointable`.
- `corruption.rs` → `classify_log` → CTRL record classification.
- `journal/mod.rs` → `JournalStoreConfig`, `JournalBootFacts` · `journal/node.rs` → `JournalStorage` · `journal/matchmaker.rs` → `JournalMatchmakerStorage`.
- `journal/{frame,plan,node_image}.rs` → record ↔ entry codec, boot fold start, node records + per-kind corruption table.
- `journal/tests.rs` → both contract suites, targeted damage, crash loop under two fault models on `SimStorageProvider`.
- `proto/{common,paros,internal,matchmaker,system,machine,checkpoint}.proto` → compiled by `build.rs` with `prost-build`.

## Public surface

`run_node`, `run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`; `provision_store`,
`provision_matchmaker_store`; `paros::client`;
`paros::system`; `paros::journal`; `paros::machine`; `paros::wire::{methods, checkpoint, common,
public, internal, matchmaker, system, machine}`; the RPC request/ack types (`lib.rs`).

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
