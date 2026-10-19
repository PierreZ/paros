# paros-core

The sans-IO Paxos roles and `ColocatedNode`, the node that wires them: `step`/`tick` in, one
`Ready` out, `advance()`; no I/O, clock, RNG or deps. Bottom of the stack (`paros-core` ←
`paros` ← `paros-sim` ← runner; `paros` ← `parosd`; `paros-wasm`'s `paxos` feature drives it directly). Never
buggified: perturbed only through its public API. Doctrine lives in the root `AGENTS.md`.

## Map

- `lib.rs` → the re-export list (roles, `ColocatedNode`, messages, `PreReadFold`, `LogPage`/`LogRead`, `WriteOp`, `Batcher`/`Unbatcher`).
- `acceptor.rs` → `Acceptor` → the durable promise, accepted records, compaction floor, CTRL faulty set; emits its own writes.
- `acceptor/retention.rs` → `Acceptor::truncate` / `trim_to` → the two floor-moving ops (a concern, not a role).
- `proposer.rs` → `Proposer` → the leader-side tallies; embeds the next five.
- `proposer/election.rs` → Phase 1 → per-configuration completion, the P2c merge.
- `proposer/probe.rs` → the CTRL repair probe (Stage 8) for slots a won election could not decide.
- `proposer/recovery.rs` → `RecoveryPolicy::{Phase1Backed, Inherited}` → the bounded recovery a fresh leadership drains.
- `proposer/rounds.rs` → `proposer::Rounds`, `Custody` → the one Phase-2 tally (#142); a proxy runs it alone.
- `proposer/authority.rs` → `proposer::Authority` → the leadership fence and the `CheckQuorum` window.
- `collector.rs` → `Collector`, `GcStep` → the leader-side GC tally (#123): chosen-index reports, matchmaker acks, the effective floor.
- `replica.rs` → `Replica`, `LogRead`, `LogPage` → chosen prefix, apply walk, journal fold (`journal`, `outcome_at`, `accepted_at`, `fold_hole`, `chosen_gap`, `covers`).
- `journal_state.rs` → `JournalState` (leader uuid, hidden term, positions), `JournalView` (what a client sees: no term), `Outcome` → the journal-control state machine; pure `apply` judges `Write`/`SetLeader`/`Truncate` (#204, #241); `Write` and `Truncate` share the writer fence `is_current` (#228). `WriterMode` (`Single`, `Multi`, in `Config.writer_mode`) is judged at apply: a call of the other mode is `Outcome::WrongMode`; a multi-writer `Write` is unfenced and takes `next_seq`.
- `proxy_leader.rs` → `ProxyLeader`, `ProxyReady` → the second deployment (#142): fan-out, fold, `Commit`, `Nack` relay, `expire_stale`.
- `replica_node.rs` → `ReplicaNode`, `ReplicaReady`, `ReplicaCounters` → the third deployment (#144); module doc holds the coupling analysis.
- `replica_node/tests.rs` → three `ColocatedNode`s and two replicas over a hand-driven network.
- `quorum_read.rs` → `QuorumRead`, `QuorumReads`, `PreReadFold`, `ReadBasis` → the leaderless read tally (#143) and the won leadership's configuration and fence it is judged over (#260).
- `membership/` (`quorum_system.rs`, `acceptor_config.rs`, `ids.rs`, `matchmaker_set.rs`, `failure_domain.rs`, `zone_rule.rs`) → `AcceptorConfig` (labeled with `FailureDomain`s by `zoned`, owned by a cell with `with_cell`, #215 (zones)), `ZoneRule` (`AcceptorConfig::zone_rule`: survives any one domain, no domain commits alone, through the quorum predicates), `MatchmakerSet`, `QuorumSystem::{Majority, Flexible, Grid}` → the one quorum boundary, incl. column/row addressing; `Grid { rows, cols, acks }` is the acceptor partitions (#532): `acks` of one column decide, `rows - acks + 1` of every column elect, a row is a read quorum only when `acks = rows` (`QuorumSystem::grid`, `QuorumSystem::partitions`).
- `matchmaking.rs` → `Matchmaking`, `MembershipProbe` → the candidate's matchmaking phase and the membership probe (#173: at boot; #270: re-probed from outside a heard belief; #278: against a matched fact or a won leadership, late answers folded).
- `matchmaker.rs` → `Matchmaker` → the registry and its generations (re-exports the submodules below); a history page (`history_page`) leaves out a belief a later registration repeats (#570 (election duel outlasts the recovery tail)).
- `decree.rs` → `Decree<Id, V>`, `DecreePromise`, `AcceptFold`, `DECREE_SLOT` → single-decree Paxos over the shared `Proposer` and `Acceptor` at slot zero: the matchmaker handover's successor decree (`SuccessorDecree`) and `cell init`'s plan (#277).
- `matchmaker/{generation,reconfigurer}.rs` → the generation machine, `MatchmakerReconfigurer`; a `Stop` and a `Bootstrap` are paged (`BootstrapPage`, #439).
- `matchmaker/{message,state,storage,write}.rs` → wire contract, durable state, `RegistryStorage` + `MemRegistry` (the reference registry), `MatchmakerWriteOp` + `MatchmakerReady`.
- `matchmaker/handover_model.rs` → the handover model checker (test-only); half its seeds page by one or two journals (#439).
- `proxy_model.rs` → the proxy-leader model checker (test-only); `model_support.rs` → seeded RNG + lossy mailbox both share.
- `retained.rs` → `RetainedWindow` → a map with a floor under it.
- `batcher.rs` → `Batcher`, `BatchLimits`, `Batch`, `Unbatcher` → the batcher's pure lanes (#145 (batchers)): one lane per journal, flushed at `flush_records`, before it would pass the limits, or at a tick; an empty or over-limit write goes alone; `Unbatcher::scatter` cuts a merged write's range into each write's own, densely and in order.
- `node.rs` → `ColocatedNode`, `Delegation`, the private `Counters` → entry points and driver-policy methods (`resend_pending`, `take_back_delegated`, `step_down`, `campaign_above`, `relinquish_to`, `reconfigure`, `quorum_read`).
- `node/{election,phase2,learn,replication,catch_up}.rs` → campaign (a paging campaign re-sends its `Prepare` to the peers that answered on every tick, #428 (paging livelock), asked from one past the highest slot the tally merged (`Election::keep_alive_from`), so an answered peer re-affirms with an empty page, never its whole suffix again), Phase-2 open/delegate/decide (`record_own_round`), learner, catch-up + trim-point jump; a follower pulls a gap from the beat's sender, and from the whole pool once its pulls at one chosen index outlast an election timeout (`PULL_FALLBACK_BEATS` at least, #349: a dead lane to a renamed leader).
- `node/{authority,quorum_reads}.rs` → `CheckQuorum`; the read path (`PreRead` wiring, `serve_quorum_reads`/`tick_quorum_reads`, `READ_TTL_TICKS`).
- `node/{handoff,gc,matchmaking,reconfigure}.rs` → `DPaxos` handoff, GC wiring, matchmaking wiring, online reconfiguration.
- `node/{boot,acceptor,helpers,invariants}.rs` → boot path, acceptor wiring, `adopt_configuration`, `assert_invariants`.
- `node/tests.rs` + `node/tests/*.rs` → unit tests, one file per concern.
- `message.rs` → `Message`, `Audience`, `Party` · `ready.rs` → `Ready<'a>` (second `ready()` before `advance()` is a compile error).
- `state.rs` → `HardState` (two scalars, `#[non_exhaustive]`), `Config` (`proxy_count`, `replica_count`, `reply_owner`).
- `storage.rs` → `Storage` (read-only recovery port) · `write.rs` → `WriteOp`, `AcceptorWrite`, `MustSync` · `types.rs` → `Ballot`, `Slot`, `Command`, `TenantId`, `JournalId`, `JournalIdentifier` (the identifier, #235), ….

## Public surface

`ColocatedNode::{new, step, tick, ready, advance, propose, propose_in, propose_control, move_round,
read_log, quorum_read, reconfigure, may_retire, …}`, `ReplicaNode`, `ProxyLeader`,
`Matchmaker`, `MatchmakerReconfigurer`, and the bare roles (`acceptor`, `proposer`, `replica`,
`matchmaking`, `membership`, `quorum_read`, `retained`, `journal_state` are `pub mod`).
Examples: `examples/{single_decree,multi_paxos,matchmaker,flexible_quorums,acceptor_grid,
quorum_read,proxy_leader,replica_tier}.rs`, each asserting the property it teaches.

## Local rules

- `ColocatedNode::assert_invariants` (`node/invariants.rs`, `pub(super)`) runs at boot and at
  the exit of every public mutating entry point; a new entry point calls it.
- Hard `assert!` only, never `debug_assert!`; every public fn that can panic has `# Panics`
  (clippy pedantic enforces it).
- `AcceptorConfig::new` / `MatchmakerSet::new` are the only constructors (deserialisation included).
- No tally compares a count against a threshold: every quorum question goes through `membership/`.
- `ColocatedNode::adopt_configuration` (`node/helpers.rs`) is the one way the configuration in force moves.
- A new observability counter is a field of `Counters` (`node.rs`), never a loose `u64`.
- `record_own_round` (`node/phase2.rs`): a leader records every round it opens whenever its
  promise allows, so the allocator frontier is durable by construction; never skip it.
- No application and no snapshot (#186): a below-floor node jumps to a peer's trim point
  (`Message::TrimmedTo`, `WriteOp::TrimmedTo`) and never moves its promise.
- Spans are `#[cfg_attr(feature = "tracing", tracing::instrument(..))]`; `serde` and `tracing`
  are observation-only (root *Tracing spans*, *Where each kind of turbulence lives*).

## Tests & gates

- `cargo nextest run -p paros-core` — unit tests and both model checkers.
- Model-checker env vars (the only ones this crate reads): `HANDOVER_MODEL_SEEDS`,
  `HANDOVER_MODEL_STEPS`, `HANDOVER_MODEL_TRACE`, `PROXY_MODEL_SEEDS`, `PROXY_MODEL_STEPS`,
  `PROXY_MODEL_FROM`, `PROXY_MODEL_TRACE` (`model_support.rs:10`, `env_or`).
- `cargo run -p paros-core --example <name>` for each of the eight examples (CI `examples` job).
- `cargo check --target wasm32-unknown-unknown -p paros-core` with and without
  `--no-default-features`; `cargo check -p paros-core --features serde` (CI `portability`).
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps` (CI `clippy` job).
- A protocol change is proven by the sweep (`cargo xtask sim run paros-chain`), not here.

## Deps & constants

- Features (`Cargo.toml`): `default = ["tracing", "assertions"]`; deps `serde`, `tracing` and
  `moonpool-assertions` (git rev pinned with the other moonpool lines, plus a crates.io
  `version` for publishing), all optional. Zero deps with `--no-default-features`.
- `probe.rs` → `probe!(reachable, "..")` / `probe!(sometimes, cond, "..")` → coverage probes
  (#317) behind `assertions`; observe only, compile to nothing with the feature off. Never
  reword a probe's message (the slot is its hash).
- Exported constants (`lib.rs`): `HANDOFF_BATCH`, `HANDOFF_FENCE_ELECTIONS`, `HEARTBEAT_TICKS`,
  `LEADER_RECOVERY_BATCH`, `PROMISE_BATCH`, `REPAIR_TIMEOUT_ELECTIONS`, `REGISTRY_PAGE`.
- `CHANGELOG.md` is release-plz's (`version_group = "paros"`); never edit it by hand.
