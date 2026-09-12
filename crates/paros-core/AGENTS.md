# paros-core

The sans-IO Paxos roles and `ColocatedNode`, the node that wires them.
Dependency-free with `--no-default-features`, wasm-safe, never buggified: it is
perturbed only through its public API. The root `AGENTS.md` holds the
doctrine; this file is the map.

## Map

- `acceptor.rs` `Acceptor` + `acceptor/retention.rs` (the two floor-moving ops, `truncate` and
  `install`: a module, not a role, because the role that moves the floor emits the write) ·
  `proposer.rs` + `proposer/{election,probe,rounds,recovery,authority}.rs`
  `Proposer` (its Phase-2 tally is the standalone `proposer::Rounds` it embeds and delegates
  to — the one tally a proxy leader runs without the rest of the role, #142; a round's
  `Custody` is `Colocated` or `Delegated` to a `ProxyId`; its standing authority — the read
  fence, the read-index rounds, the `CheckQuorum` window — is the standalone
  `proposer::Authority` it embeds the same way) · `proxy_leader.rs` `ProxyLeader` +
  `ProxyReady` (the **second deployment**, #142: a `Rounds` plus routing on a process that is
  neither an acceptor nor a replica; it fans a delegated `Accept` out, folds the `Accepted`s,
  emits the `Commit`, relays a `Nack`, re-fans-out on its beat, and works for the highest
  ballot it was handed) · `replica.rs` `Replica` (owns `chosen_gap()`; reach it as
  `node.replica().chosen_gap()`) · `membership.rs` `AcceptorConfig`,
  `MatchmakerSet`, `QuorumSystem` (the one quorum boundary; `Majority`, `Flexible { q1, q2 }` and
  `Grid { rows, cols }`, whose column addressing — `column_of`, `phase2_addressees`,
  `is_phase2_addressee`, `has_phase2_quorum_in` — is the one place a column is chosen, and
  whose row addressing — `row_of`, `phase1_addressees`, `is_phase1_addressee`,
  `has_phase1_quorum_in` — the one place a read row is) · `quorum_read.rs` `QuorumRead` /
  `QuorumReads` (the leaderless read tally, #143: a row's vote watermarks, the maximum, the
  replica's `covers`; wired on any node by `node/quorum_reads.rs`) · `matchmaking.rs`
  `Matchmaking` (the candidate's phase) · `retained.rs` `RetainedWindow`.
- `matchmaker.rs` `Matchmaker` + `matchmaker/{reconfigurer,decree,generation,
  handover_model,storage,message,state,write}.rs`: the registry, the handover
  and the single decree over the shared roles at slot zero; `MemRegistry` is
  the reference in-memory registry.
- `node.rs` `ColocatedNode` (`step`/`tick`/`ready`/`advance`, the client entry
  points with their `Delegation`, the driver-policy surface such as `resend_pending`,
  `take_back_delegated`, `step_down`, `relinquish_to`, `reconfigure`) + `node/*.rs` named
  by **concern** (`election`, `replication`, `authority` — `CheckQuorum` — `phase2` — open, fan out or delegate, fold,
  decide, take back — `learn` — a chosen value reaching the record and the prefix —
  `handoff`, `gc`, `matchmaking`, `reconfigure`, `reads`, `quorum_reads`,
  `catch_up_snapshot`, `boot`, `acceptor`, `helpers`, `invariants`). Wiring only; no
  protocol tally lives here.
- `message.rs` `Message` + `Audience` + `Party` (a node or a proxy: the reply party of an
  `Accept`, the sender of a `Commit`) · `ready.rs` `Ready<'a>` (the borrow
  guard that makes a second `ready()` before `advance()` a compile error) ·
  `state.rs` `HardState` (two scalars, `#[non_exhaustive]`) and `Config` (`proxy_count`,
  zero on the plain deployment) · `storage.rs` the read-only `Storage` recovery port ·
  `types.rs` · `write.rs` `WriteOp`.
- `proxy_model.rs` the proxy model checker and `model_support.rs` the seeded RNG and lossy
  mailbox both model checkers share (test-only).

## Rules local to this crate

- `ColocatedNode::assert_invariants` is `pub(super)` (`node/invariants.rs`):
  called at boot and at the exit of every public mutating entry point, never
  from outside the crate. A new entry point calls it.
- Hard `assert!` everywhere, no `debug_assert!`; every public function that
  can panic has a `# Panics` section (pedantic enforces it).
- `AcceptorConfig::new` / `MatchmakerSet::new` are the only constructors.
- `ColocatedNode::adopt_configuration` is the one way the configuration in force moves:
  it binds the ballot and records the membership in one call.
- The observability counters are one struct (`Counters` in `node.rs`) behind the public
  accessors; a new one is a field there, never a loose `u64` on the node.
- Spans are `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`;
  `serde` adds derives; both features are observation-only.
- The two model checkers — the handover's (`matchmaker/handover_model.rs`) and the
  proxy leader's (`proxy_model.rs`) — run under nextest; `HANDOVER_MODEL_SEEDS`,
  `HANDOVER_MODEL_STEPS`, `HANDOVER_MODEL_TRACE`, `PROXY_MODEL_SEEDS`, `PROXY_MODEL_STEPS`,
  `PROXY_MODEL_FROM`, `PROXY_MODEL_TRACE` are the only environment variables the
  workspace reads.
- **The allocator frontier is durable by construction**: a leader records every round it
  opens in its own log whenever its promise allows, colocated or delegated, in its column or
  not (`node/phase2.rs`, `record_own_round`), so a reboot rederives the frontier and the
  handoff's replay guard holds. Never skip the record to save a write.

## Tests and gates

Unit tests are inline: `node/tests.rs` + `node/tests/*.rs` (one file per
concern), plus the role, matchmaker, reconfigurer, decree and model-checker
modules. `examples/{single_decree,multi_paxos,matchmaker,flexible_quorums,acceptor_grid,quorum_read,proxy_leader}.rs`
run in CI.
Gates: `cargo check --target wasm32-unknown-unknown -p paros-core` (with and
without default features), `cargo check -p paros-core --features serde`,
`RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps`. A protocol
change is proven by the sweep (`cargo xtask sim run paros-chain`), not here.
`CHANGELOG.md` is release-plz's (`version_group = "paros"`).
