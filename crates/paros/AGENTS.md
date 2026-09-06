# paros

The library: `pub use paros_core::*`, the provider-generic driver, the
in-memory stores, the RPC contract, and the matchmaker driver. Everything here
is written once over moonpool's `P: Providers` and runs unchanged in
production and in simulation. Protocol logic that needs I/O policy lives in
the driver, never in a sim-only path.

## Map

- `driver/mod.rs` `run_node<P, S, H, A>` (the etcd-raft `Node` layer) ·
  `driver/{boot,ready,report,transport,snap_repair,matchmaking,handover,events}.rs`
  by stage: boot replay, the `Ready` handshake's I/O side in
  persist-before-send order, post-batch upkeep, the bounded keep-newest
  `PeerMailbox`, the chunk-repair plane (`SnapAck`/`SnapChunkRequest`/
  `SnapChunkResponse` never enter `ColocatedNode`), the matchmaker wire, the
  matchmaker-set handover · `driver/config.rs` `DriverTunables` and its
  production defaults.
- `hooks.rs` `DriverHooks` (the BUGGIFY prong-1 surface, every method
  defaulting to inert, `NoHooks` for production), `Seam` (eight durability
  seams), `HandoffContext`, `Reply`. The `H: DriverHooks` bound on `run_node`
  is deliberately **not** `Send + 'static`: consulting a hook from a spawned
  task must not compile, because a hook answer is a randomness draw and a
  detached task can shift the next run's stream.
- `audit.rs` `Audit` (the observation port, `NoAudit` for production): report
  once, typed, where the matching `tracing` event is; an implementation
  returns nothing, draws nothing, reads no clock.
- `storage.rs` `NodeStorage: Storage` (async seam: every method that may
  touch the device returns a `Send` future; the boot scan loads and verifies,
  the synchronous accessors answer from memory), `MemStorage`,
  `storage_contract_suite` · `matchmaker/{mod,storage}.rs` `run_matchmaker`,
  `MatchmakerConfig`, `MatchmakerStorage: RegistryStorage`,
  `MemMatchmakerStorage`, `matchmaker_storage_contract_suite`.
- `grpc.rs` + `proto/{common,internal,matchmaker,paros}.proto` (built by
  `build.rs` with `tonic-prost-build`; runtime-free tonic so `paros` stays
  wasm-checkable): `Paros` (Propose/Read/Compact/Reconfigure/
  ReconfigureMatchmakers), `ParosInternal` (Deliver/Inspect/Retire),
  `ParosMatchmaker` (Matchmake/GarbageCollect/Reconfigure).
- `corruption.rs` the CTRL record classification (`classify_log`).

## Rules local to this crate

- A new driver decision is a `DriverHooks` method with an honest contract;
  consult it only where the answer has an observable effect and only from
  the node loop; report what happened through `Audit`.
- A new tunable is born as a `DriverTunables` field with a default here and
  a `buggify_knob!` draw in `paros-sim`'s `NodeShape`.
- A new durability boundary is a new `Seam` variant.
- Spans are non-optional here (`#[tracing::instrument(skip_all, fields(..))]`
  on the loop stages, handlers and storage impls).
- Storage implementations pass the two contract suites; the faulty fake is
  `paros-sim`'s world-backed store, not a crate here.
- Deps: `paros-core`, `moonpool-core` (git pin, `default-features = false`,
  `select`), `moonpool-hyper`, tonic. The pin rev is repeated in
  `crates/paros-sim/Cargo.toml`; advance all four lines together.
