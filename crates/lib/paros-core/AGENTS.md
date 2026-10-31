# paros-core

The sans-IO Paxos roles and `ColocatedNode`, the node that wires them: `step`/`tick` in, one
`Ready` out, `advance()`; no I/O, clock, RNG or deps. Bottom of the stack (`paros-core` ←
`paros` ← `paros-sim` ← runner; `paros` ← `parosd`; `paros-wasm`'s `paxos` feature drives it directly). Never
buggified: perturbed only through its public API. Doctrine lives in the root `AGENTS.md`.

## Layers

The modules make three layers that point down only (#730 (paros-core rework)):
**foundations** ← **roles** ← **deployments**.

- **Foundations**: `probe`, `types`, `membership`, `journal`, `contract`, `retained`. A
  foundation uses only foundations.
- **Roles**: `acceptor`, `proposer`, `replica`, `quorum_read`, `batcher`, `collector`,
  `decree`, `matchmaking`, `matchmaker`. A role uses foundations and roles, never a deployment.
- **Deployments**: `node`, `proxy_leader`, `replica_node`. A deployment wires roles and owns
  one `Ready`. It never uses another deployment.

`scripts/check-core-layers.sh` fails on a `crate::` path that reaches up a layer (CI `fmt`
job). Test code (`tests.rs`, `tests/`, `models/`, `test_support.rs`) is exempt. A new module
gets its layer in that script.

## Map

Foundations:

- `probe.rs` → `probe!(reachable, "..")` / `probe!(sometimes, cond, "..")` → coverage probes
  (#317) behind `assertions` (first in `lib.rs`: a textual-scope macro).
- `types.rs` → the Paxos scalars: `NodeId`, `Slot`, `Ballot`, `Value`, `Fingerprint`.
- `membership/` (`quorum_system.rs`, `acceptor_config.rs`, `ids.rs`, `matchmaker_set.rs`, `failure_domain.rs`, `zone_rule.rs`) → `AcceptorConfig` (labeled with `FailureDomain`s by `zoned`, owned by a cell with `with_cell`, #215 (zones)), `ZoneRule` (`AcceptorConfig::zone_rule`: survives any one domain, no domain commits alone, through the quorum predicates), `MatchmakerSet`, `QuorumSystem::{Majority, Flexible, Grid}` → the one quorum boundary, incl. column/row addressing; `Grid { rows, cols, acks }` is the acceptor partitions (#532): `acks` of one column decide, `rows - acks + 1` of every column elect, a row is a read quorum only when `acks = rows` (`QuorumSystem::grid`, `QuorumSystem::partitions`).
- `journal.rs` + `journal/` → what the log carries, judged at apply (not Paxos):
  - `ids.rs` → `TenantId`, `JournalId`, `JournalIdentifier` (the identifier, #235), `LeaderUuid`, `Seq`.
  - `command.rs` → `Command`, `Control`, `Entry`, `command_fingerprint`.
  - `state.rs` → `JournalState` (leader uuid, hidden term, positions), `JournalView` (what a client sees: no term), `Outcome` → the journal-control state machine; pure `apply` judges `Write`/`SetLeader`/`Truncate` (#204, #241); `Write` and `Truncate` share the writer fence `is_current` (#228). `WriterMode` (`Single`, `Multi`, in `Config.writer_mode`) is judged at apply: a call of the other mode is `Outcome::WrongMode`; a multi-writer `Write` is unfenced and takes `next_seq`.
- `contract.rs` + `contract/` → what a driver must honor: persist before send.
  - `message.rs` → `Message`, `Audience`, `Party`.
  - `write.rs` → `WriteOp`, `AcceptorWrite`, `MustSync`.
  - `storage.rs` → `Storage`, the read-only recovery port.
  - `mem_storage.rs` → `MemStorage`: the in-memory `Storage` a driver writes with `apply` (tests, examples).
  - `hard_state.rs` → `HardState` (two scalars, `#[non_exhaustive]`); `config.rs` → `Config` (`proxy_count`, `replica_count`, `reply_owner`).
  - `limits.rs` → `Limits` (the page sizes and the read window a deployment takes at construction), the ceilings (`RECOVERY_BATCH`, `PROMISE_BATCH`, `RESEND_BATCH`, `APPLY_BATCH`, `REGISTRY_PAGE`) and the read-window floor `READ_TTL_TICKS`.
- `retained.rs` → `RetainedWindow` → a map with a floor under it.

Roles (pure tallies, no deployment knowledge):

- `acceptor.rs` → `Acceptor` → the durable promise, accepted records, compaction floor, CTRL faulty set; emits its own writes.
- `acceptor/retention.rs` → `Acceptor::truncate` / `trim_to` → the two floor-moving ops (a concern, not a role).
- `proposer.rs` → `Proposer` → the leader-side tallies; embeds the next five.
- `proposer/election.rs` → Phase 1 → per-configuration completion, the P2c merge.
- `proposer/probe.rs` → the CTRL repair probe (Stage 8) for slots a won election could not decide.
- `proposer/recovery.rs` → `RecoveryPolicy::{Phase1Backed, Inherited}` → the bounded recovery a fresh leadership drains.
- `proposer/rounds.rs` → `proposer::Rounds`, `Custody` → the one Phase-2 tally (#142); a proxy runs it alone.
- `proposer/authority.rs` → `proposer::Authority` → the leadership fence and the `CheckQuorum` window.
- `replica.rs` → `Replica`, `LogRead`, `LogPage` → chosen prefix, apply walk, journal fold (`journal`, `outcome_at`, `accepted_at`, `fold_hole`, `chosen_gap`, `covers`).
- `quorum_read.rs` → `QuorumRead`, `QuorumReads`, `PreReadFold`, `ReadBasis`, `ReadState` → the leaderless read tally (#143) and the won leadership's configuration and fence it is judged over (#260).
- `batcher.rs` → `Batcher`, `BatchLimits`, `Batch`, `Unbatcher` → the batcher's pure lanes (#145 (batchers)): one lane per journal, flushed at `flush_records`, before it would pass the limits, or at a tick; an empty or over-limit write goes alone; `Unbatcher::scatter` cuts a merged write's range into each write's own, densely and in order.
- `collector.rs` → `Collector`, `GcStep` → the leader-side GC tally (#123): chosen-index reports, matchmaker acks, the effective floor.
- `decree.rs` → `Decree<Id, V>`, `DecreePromise`, `AcceptFold`, `DECREE_SLOT` → single-decree Paxos over the shared `Proposer` and `Acceptor` at slot zero: the matchmaker handover's successor decree (`SuccessorDecree`) and `cell init`'s plan (#277).
- `matchmaking.rs` → `Matchmaking` → the candidate's matchmaking phase; `matchmaking/probe.rs` → `MembershipProbe` (#173: at boot; #270: re-probed from outside a heard belief; #278: against a matched fact or a won leadership, late answers folded).
- `matchmaker.rs` → `Matchmaker` → the registry and its generations (re-exports the submodules below); a history page (`history_page`) leaves out a belief a later registration repeats (#570 (election duel outlasts the recovery tail)).
- `matchmaker/{generation,reconfigurer}.rs` → the generation machine, `MatchmakerReconfigurer`; a `Stop` and a `Bootstrap` are paged (`BootstrapPage`, #439).
- `matchmaker/{message,state,storage}.rs` → wire contract, durable state, `RegistryStorage` + `MemRegistry` (the reference registry); `matchmaker/ready.rs` → `MatchmakerWriteOp` + `MatchmakerReady`.

Deployments (each wires roles and owns one `Ready`):

- `node.rs` → `ColocatedNode`, `Delegation` → the protocol entry points (`new`, `step`, `tick`, `propose*`, `compact`, `repair_record`, `ready`).
- `node/ready.rs` → `Ready<'a>` (second `ready()` before `advance()` is a compile error).
- `node/policy.rs` → the driver-policy methods (`resend_pending`, `take_back_delegated`, `move_round`, `step_down`, `campaign_above`, `advance_recovery`, `set_election_timeout`, `extend_pool`).
- `node/view.rs` → the read-only accessors (`config`, `limits`, `pool`, `acceptors`, `read_basis`, `hard_state`, `role`, `leader`, `read_log`, …).
- `node/counters.rs` → the private `Counters`, `RepairCounters`, `MembershipCounters` and the `NodeCounters` snapshot (`ColocatedNode::counters`).
- `node/{election,phase2,learn,replication,catch_up}.rs` → campaign (a paging campaign re-sends its `Prepare` to the peers that answered on every tick, #428 (paging livelock), asked from one past the highest slot the tally merged (`Election::keep_alive_from`), so an answered peer re-affirms with an empty page, never its whole suffix again), Phase-2 open/delegate/decide (`record_own_round`), learner, catch-up + trim-point jump; a follower pulls a gap from the beat's sender, and from the whole pool once its pulls at one chosen index outlast an election timeout (`PULL_FALLBACK_BEATS` at least, #349: a dead lane to a renamed leader).
- `node/{authority,quorum_reads}.rs` → `CheckQuorum`; the read path (`PreRead` wiring, `serve_quorum_reads`/`tick_quorum_reads`).
- `node/{handoff,gc,matchmaking,reconfigure}.rs` → `DPaxos` handoff, GC wiring, matchmaking wiring, online reconfiguration (`relinquish_to`, `reconfigure`, `quorum_read`).
- `node/{boot,acceptor,helpers,invariants}.rs` → boot path, acceptor wiring, `adopt_configuration`, `assert_invariants`.
- `node/tests.rs` + `node/tests/*.rs` → unit tests, one file per concern.
- `proxy_leader.rs` → `ProxyLeader` → the second deployment (#142): fan-out, fold, `Commit`, `Nack` relay, `expire_stale`; `proxy_leader/ready.rs` → `ProxyReady`.
- `replica_node.rs` → `ReplicaNode`, `ReplicaCounters` → the third deployment (#144); module doc holds the coupling analysis; `replica_node/ready.rs` → `ReplicaReady`; `replica_node/tests.rs` → three `ColocatedNode`s and two replicas over a hand-driven network.

Test-only:

- `test_support.rs` → the shared builders (`MemStorage::for_node`, `from_node`, `rot`).
- `models.rs` + `models/` → the model checkers: `handover.rs` (the matchmaker handover; half its seeds page by one or two journals, #439), `proxy.rs` (the proxy leader), `support.rs` (the seeded RNG and lossy mailbox both share).

## Public surface

Every module is `pub`, so every item has one path under its module (`paros_core::journal::Command`,
`paros_core::node::NodeCounters`, `paros_core::contract::limits::PROMISE_BATCH`). The root
re-exports only what a driver loop needs: the deployments (`ColocatedNode`, `Ready`,
`ProxyLeader`, `ReplicaNode`, `Matchmaker`, `MatchmakerReconfigurer`), the contract (`Config`,
`HardState`, `Limits`, `Message`, `Storage`, `WriteOp`) and the scalars (`Ballot`, `NodeId`,
`Slot`, `Value`). A deployment takes its `Limits` at construction (`ColocatedNode::new(storage,
limits)`), and reads them back with `limits()`; `set_election_timeout` stays a method, because a
driver draws a new timeout per election. The crate has no `examples/` (removed with #313): the
roles driven by hand are the Paxos game's levels (`paros-wasm`, feature `paxos`), each asserting
the property it teaches.

## Local rules

- `ColocatedNode::assert_invariants` (`node/invariants.rs`, `pub(super)`) runs at boot and at
  the exit of every public mutating entry point; a new entry point calls it.
- Hard `assert!` only, never `debug_assert!`; every public fn that can panic has `# Panics`
  (clippy pedantic enforces it).
- `AcceptorConfig::new` / `MatchmakerSet::new` are the only constructors (deserialisation included).
- No tally compares a count against a threshold: every quorum question goes through `membership/`.
- `ColocatedNode::adopt_configuration` (`node/helpers.rs`) is the one way the configuration in force moves.
- A new observability counter is a field of `Counters` (`node/counters.rs`) and of the
  `NodeCounters` snapshot, never a loose `u64` getter.
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
  `PROXY_MODEL_FROM`, `PROXY_MODEL_TRACE` (`models/support.rs`, `env_or`).
- `cargo check --target wasm32-unknown-unknown -p paros-core` with and without
  `--no-default-features`; `cargo check -p paros-core --features serde` (CI `portability`).
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-core --no-deps` (CI `clippy` job).
- A protocol change is proven by the sweep (`cargo xtask sim run paros-chain`), not here.

## Deps & constants

- Features (`Cargo.toml`): `default = ["tracing", "assertions"]`; deps `serde`, `tracing` and
  `moonpool-assertions` (git rev pinned with the other moonpool lines, plus a crates.io
  `version` for publishing), all optional. Zero deps with `--no-default-features`.
- `probe!` observes only and compiles to nothing with the `assertions` feature off. Never
  reword a probe's message (the slot is its hash).
- Constants: the page ceilings and the read-window floor are in `contract::limits`;
  `HANDOFF_BATCH` (`Limits::DEFAULT.promise_page`), `HANDOFF_FENCE_ELECTIONS`,
  `HEARTBEAT_TICKS` and `REPAIR_TIMEOUT_ELECTIONS` are in `node`.
- `CHANGELOG.md` is release-plz's (`version_group = "paros"`); never edit it by hand.
