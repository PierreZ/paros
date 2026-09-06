# paros-core

The sans-IO Paxos roles and `ColocatedNode`, the node that wires them.
Dependency-free with `--no-default-features`, wasm-safe, never buggified: it is
perturbed only through its public API. The root `AGENTS.md` holds the
doctrine; this file is the map.

## Map

- `acceptor.rs` `Acceptor` · `proposer.rs` + `proposer/{election,probe,rounds,recovery,authority}.rs`
  `Proposer` · `replica.rs` `Replica` (owns `chosen_gap()`; reach it as
  `node.replica().chosen_gap()`) · `membership.rs` `AcceptorConfig`,
  `MatchmakerSet`, `QuorumSystem` (the one quorum boundary) · `matchmaking.rs`
  `Matchmaking` (the candidate's phase) · `retained.rs` `RetainedWindow`.
- `matchmaker.rs` `Matchmaker` + `matchmaker/{reconfigurer,decree,generation,
  handover_model,storage,message,state,write}.rs`: the registry, the handover
  and the single decree over the shared roles at slot zero; `MemRegistry` is
  the reference in-memory registry.
- `node.rs` `ColocatedNode` (`step`/`tick`/`ready`/`advance`, the client entry
  points, the driver-policy surface such as `resend_pending`, `step_down`,
  `relinquish_to`, `reconfigure`) + `node/*.rs` named by **concern**
  (`election`, `replication`, `handoff`, `gc`, `matchmaking`, `reconfigure`,
  `reads`, `decide_apply`, `catch_up_snapshot`, `boot`, `acceptor`,
  `helpers`, `invariants`). Wiring only; no protocol tally lives here.
- `message.rs` `Message` + `Audience` · `ready.rs` `Ready<'a>` (the borrow
  guard that makes a second `ready()` before `advance()` a compile error) ·
  `state.rs` `HardState` (two scalars, `#[non_exhaustive]`) and `Config` ·
  `storage.rs` the read-only `Storage` recovery port · `types.rs` · `write.rs`
  `WriteOp`.

## Rules local to this crate

- `ColocatedNode::assert_invariants` is `pub(super)` (`node/invariants.rs`):
  called at boot and at the exit of every public mutating entry point, never
  from outside the crate. A new entry point calls it.
- Hard `assert!` everywhere, no `debug_assert!`; every public function that
  can panic has a `# Panics` section (pedantic enforces it).
- `AcceptorConfig::new` / `MatchmakerSet::new` are the only constructors.
- Spans are `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`;
  `serde` adds derives; both features are observation-only.
- The handover model checker (`matchmaker/handover_model.rs`) runs under
  nextest; `HANDOVER_MODEL_SEEDS`, `HANDOVER_MODEL_STEPS`, `HANDOVER_MODEL_TRACE`
  are the only environment variables the workspace reads.

## Tests and gates

Unit tests are inline: `node/tests.rs` + `node/tests/*.rs` (one file per
concern), plus the role, matchmaker, reconfigurer, decree and model-checker
modules. `examples/{single_decree,multi_paxos,matchmaker}.rs` run in CI.
Gates: `cargo check --target wasm32-unknown-unknown -p paros-core` (with and
without default features), `cargo check -p paros-core --features serde`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps`. A protocol
change is proven by the sweep (`cargo xtask sim run paros-chain`), not here.
`CHANGELOG.md` is release-plz's (`version_group = "paros"`).
