# paros-consensus

The sans-IO Paxos roles and `ColocatedNode`, the node that wires them: `step`/`tick` in, one
`Ready` out, `advance()`; no I/O, clock, RNG or deps. Bottom of the stack (`paros-consensus` ←
`paros`; `paros-wasm`'s `paxos` feature drives it directly). The driver and the simulation
that ran it were removed on 2026-11-19 (tag `v0.2.0-alpha.1`, `docs/archive/v0.2.0-alpha.1/`). Never
buggified: perturbed only through its public API. Doctrine lives in the root `AGENTS.md`.

## Layers

The modules make three layers that point down only (#730 (paros-consensus rework)):
**foundations** ← **roles** ← **deployments**.

- **Foundations**: `probe`, `types`, `membership`, `journal`, `contract`, `retained`. A
  foundation uses only foundations.
- **Roles**: `acceptor`, `proposer`, `replica`, `belief`, `quorum_read`, `batcher`, `collector`,
  `decree`, `matchmaking`, `matchmaker`, `publisher`. A role uses foundations and roles, never a deployment.
- **Deployments**: `node`, `proxy_leader`, `replica_node`. A deployment wires roles and owns
  one `Ready`. It never uses another deployment.

`scripts/check-core-layers.sh` fails on a `crate::<module>` path that reaches up a layer (CI
`checks` job). It maps each re-export of the crate root (`crate::ColocatedNode`) to its module,
and fails on the two forms that hide a module: a grouped `use crate::{..}` and
`super::super::`. So the modules write each upward-looking import as `crate::<module>::..`. Its
`--self-test` runs it on `scripts/fixtures/core-layers/` (a good tree, and one bad tree per
form); CI runs both. Test code (`tests.rs`, `tests/`, `models/`, `test_support.rs`) is exempt. A new module
gets its layer in that script.

## Map

Foundations:

- `probe.rs` → `probe!(reachable, "..")` / `probe!(sometimes, cond, "..")` → coverage probes
  (#317) behind `assertions` (first in `lib.rs`: a textual-scope macro).
- `types.rs` → the Paxos scalars: `NodeId`, `Slot`, `Ballot`, `Value` (its bytes are shared, `Arc<[u8]>`: a clone costs a count), `Fingerprint`, `Weigh` (the bytes a value holds: the unit of `Acceptor::retained_bytes`, which a driver reads through `ColocatedNode::retained_bytes` and `ReplicaNode::retained_bytes` to bound the memory of the retained log, #1153 (retained log memory)), and `fnv1a` with `FNV_OFFSET`, the one FNV-1a step every fingerprint and trace digest folds with.
- `membership/` (`quorum_system.rs`, `acceptor_config.rs`, `ids.rs`, `matchmaker_set.rs`, `failure_domain.rs`, `zone_rule.rs`, `shape_error.rs`) → `ShapeError` (why a membership from the wire or an operator is refused), `AcceptorConfig` (labeled with `FailureDomain`s by `zoned`, #215 (zones); it names no cell: cells are the upper stack's, #1264 (remove the cell)), `ZoneRule` (`AcceptorConfig::zone_rule`: survives any one domain, no domain commits alone, through the quorum predicates), `MatchmakerSet`, `QuorumSystem::{Majority, Grid}` → the one quorum boundary, incl. column/row addressing; `Grid { rows, cols, acks }` is the acceptor partitions (#532): `acks` of one column decide, `rows - acks + 1` of every column elect, a row is a read quorum only when `acks = rows` (`QuorumSystem::grid`, `QuorumSystem::partitions`); a grid with `acks < rows` is admitted only when its Phase-1 quorums meet (`phase1_quorums_meet`, #1483 (read fence on a grid whose Phase-1 quorums do not meet)).
- `journal.rs` + `journal/` → what the log carries, judged at apply (not Paxos):
  - `ids.rs` → `TenantId`, `JournalId`, `JournalIdentifier` (the identifier, #235), `LeaderUuid`, `Seq`.
  - `command.rs` → `Command`, `Control`, `Entry`, `command_fingerprint`.
  - `segment.rs` → `Roll` (the cut, `Command::Roll`), `SegmentId`, `Segment` (the tail segment: id and first slot), `ReplicaSet`, `ReplicaSetError` → the segments of the log (`docs/architecture/04-7-segments.md` §4.7).
  - `chain.rs` → `Chain`, `SegmentRecord`, `Opening`, `merge_chain` → the chain of segments at the matchmakers (R5): an add-only map, each field written once and merged field by field (the opening holds the replicas the `Roll` named and the boundary state a replica joins from); two values of one field are a detected safety violation. Its model is the chain of `specs/tla/handover-scalars/`.
  - `state.rs` → `JournalState` (leader uuid, hidden term, positions), `JournalView` (what a client sees: no term), `Outcome` → the journal-control state machine; pure `apply` judges `Write`/`SetLeader`/`Truncate`/`Roll` (#204, #241; a `Roll` to the tail plus one moves the tail `segment`, any other is `RollIgnored`); `Write` and `Truncate` share the writer fence `is_current` (#228). `WriterMode` (`Single`, `Multi`, in `Config.writer_mode`) is judged at apply: a call of the other mode is `Outcome::WrongMode`; a multi-writer `Write` is unfenced and takes `next_seq`.
- `contract.rs` + `contract/` → what a driver must honor: persist before send.
  - `message.rs` → `Message`, `Audience`, `Party`.
  - `write.rs` → `WriteOp`, `AcceptorWrite`, `MustSync`.
  - `storage.rs` → `Storage`, the read-only recovery port.
  - `mem_storage.rs` → `MemStorage`: the in-memory `Storage` a driver writes with `apply` (tests, examples); `rot` damages a record into a faulty entry, and a later write at its slot heals it.
  - `hard_state.rs` → `HardState` (two scalars, `#[non_exhaustive]`); `config.rs` → `Config` (`proxy_count`, `replica_count`, `reply_owner`).
  - `limits.rs` → `Limits` (the page sizes, the read window and the proxy leader's retention a deployment takes at construction), the ceilings (`RECOVERY_BATCH`, `PROMISE_BATCH`, `RESEND_BATCH`, `APPLY_BATCH`, `REGISTRY_PAGE`, `CATCHUP_BATCH`) and the read-window floor `READ_TTL_TICKS` (crate-private: `Limits::kept_read_window` raises the driver's window to it).
- `retained.rs` → `RetainedWindow` → a map with a floor under it.

Roles (pure tallies, no deployment knowledge):

- `acceptor.rs` → `Acceptor` → the durable promise, accepted records, compaction floor, CTRL faulty set; emits its own writes.
- `acceptor/retention.rs` → `Acceptor::truncate` / `trim_to` → the two floor-moving ops (a concern, not a role). Their TLA+ model, with the floor guard of `on_prepare` and the trim-point jump of `node/catch_up.rs`, is `specs/tla/truncation/` (#1017 (truncation model)); the floor as a fold of the journal (`Replica::compaction_target`) is in it too (#1216 (truncation model, part 2)).
- `proposer.rs` → `Proposer` → the leader-side tallies; embeds the next five. Its two Phase-1 rules are public functions, so a Phase-1 tally outside the core applies them: `merge_report` (the P2c merge) and `qualifying_answers` / `slot_decidable` (the CTRL tri-state tally). Its paging half, `PromiseTally` (the ballot, the first slot, a cursor for each sender mid-suffix and the senders that answered completely; `accepts`, `close_page`, `answered`, `cursor`), is public too, so that such a tally pages its promises by the same rule.
- `proposer/election.rs` → Phase 1 → per-configuration completion, the P2c merge.
- `proposer/probe.rs` → the CTRL repair probe (Stage 8) for slots a won election could not decide. The tri-state tally (`slot_decidable`) and the probe have their TLA+ model in `specs/tla/truncation/` (#1216 (truncation model, part 2)).
- `proposer/recovery.rs` → `RecoveryPolicy::{Phase1Backed, Inherited}` → the bounded recovery a fresh leadership drains.
- `proposer/rounds.rs` → `proposer::Rounds`, `Custody` → the one Phase-2 tally (#142); a proxy runs it alone.
- `proposer/authority.rs` → `proposer::Authority` → the leadership fence, the `CheckQuorum` window, and the windows in a row each member missed (`missed`, observe-only, #1564 (journal controller, version 1)).
- `replica.rs` → `Replica`, `LogRead`, `LogPage`, `ChosenLog` → chosen prefix, apply walk, journal fold (`journal`, `outcome_at`, `accepted_at`, `fold_hole`, `chosen_gap`, `covers`); a bound replica keeps one segment and seals at its `Roll` (`bind_segment`, `is_sealed`, `sealed_end`). The replica holds the set of chosen slots, never their commands: it reads each chosen command through the `ChosenLog` port from the one copy its deployment holds (the acceptor's records on a node, the learned records on a `ReplicaNode`; #1101 (one copy of the retained log)). The deployment writes the record before `Replica::learn`, which asserts it. `ColocatedNode::chosen_at` / `chosen_commands` and the same on `ReplicaNode` read it for a driver or an oracle.
- `belief.rs` → `Belief`, `Judgement` → the configuration a deployment believes in force, its ballot and the read basis (#1101 (one Belief role)): the follower rule (`judge`, `learn`: matchmakers, a strictly newer ballot, a configuration drawn from the pool), `bind` for a reason the deployment owns, and the read basis (`learn_read_basis`, `read_basis`). `ColocatedNode`, `ProxyLeader` and `ReplicaNode` each hold one and call `Belief::assert_invariants` in their own.
- `quorum_read.rs` → `QuorumRead`, `QuorumReads`, `PreReadFold`, `ReadBasis`, `ReadState` → the leaderless read tally (#143) and the won leadership's configuration and fence it is judged over (#260).
- `batcher.rs` → `Batcher`, `BatchLimits`, `Batch`, `Unbatcher` → the batcher's pure lanes (#145 (batchers)): one lane per journal, flushed at `flush_records`, before it would pass the limits, or at a tick; an empty or over-limit write goes alone; `Unbatcher::scatter` cuts a merged write's range into each write's own, densely and in order.
- `publisher.rs` → `Publisher` → the node-side tally of the chain's publishes (R5): each `SegmentPublish` until a majority of one matchmaker generation acked it; reset at a new generation.
- `collector/held.rs` → `HeldTally`, `SealedSegment` → the held tally of R7 (§4.7): the replicas that reported a sealed segment (`Message::SegmentHeld`), counted against `F` by `AcceptorConfig::is_held_by` once the end is published; wired in `node/held.rs` (`ColocatedNode::held_segments`).
- `collector.rs` → `Collector`, `GcStep` → the leader-side GC tally (#123): chosen-index reports, matchmaker acks, the effective floor. Its rule across a reconfiguration onto empty stores (`covered`: a Phase-2 quorum of `C_b` at or past the fence) has its TLA+ model in `specs/tla/truncation-reconfig/` (#1218 (truncation model, part 3)).
- `decree.rs` → `Decree<Id, V>`, `DecreePromise`, `AcceptFold`, `DECREE_SLOT` → single-decree Paxos over the shared `Proposer` and `Acceptor` at slot zero: the matchmaker handover's successor decree (`SuccessorDecree`), and any one-shot decision a caller above the core runs. Its TLA+ model is `specs/tla/decree/` (#1186 (rework 1.15)).
- `matchmaking.rs` → `Matchmaking` → the candidate's matchmaking phase; `matchmaking/probe.rs` → `MembershipProbe` (#173: at boot; #270: re-probed from outside a heard belief; #278: against a matched fact or a won leadership, late answers folded).
- `matchmaker.rs` → `Matchmaker` → the registry and its generations (re-exports the submodules below); a history page (`history_page`) leaves out a belief a later registration repeats (#570 (election duel outlasts the recovery tail)), but keeps each owner's lowest registration of each configuration (PR #1064 (gone-node gap)).
- `matchmaker/{generation,reconfigurer}.rs` → the generation machine, `MatchmakerReconfigurer`; a `Stop` and a `Bootstrap` are paged (`BootstrapPage`, #439).
- `matchmaker/chain.rs` → `Matchmaker::publish_segment`, `segment_ack`, `chain`, `PublishOutcome` → a publish merged into the journal's chain (`JournalScalars::segments`), refused off its generation; the handover merges the chains of the stop quorum (`reconfigurer.rs`) and the activation merges the result into the member's own (`generation.rs`).
- `matchmaker/{message,state,storage}.rs` → wire contract (`SegmentPublish`, `SegmentAck` too), durable state, `RegistryStorage` + `MemRegistry` (the reference registry); `matchmaker/ready.rs` → `MatchmakerWriteOp` + `MatchmakerReady`.

Deployments (each wires roles and owns one `Ready`):

- `node.rs` → `ColocatedNode`, `Delegation` → the protocol entry points (`new`, `step`, `tick`, `propose*`, `compact`, `repair_record`, `ready`). Its plain Multi-Paxos path (Phase 1 recovery, the allocator frontier, the `Noop` gap fill, commit and catch-up, restarts, the majority and the grid) has its TLA+ model in `specs/tla/multi-paxos/` (#1185 (rework 1.14)).
- `node/ready.rs` → `Ready<'a>` (second `ready()` before `advance()` is a compile error) and `Flight` (#1100): an async driver calls `Ready::release` to keep the batch's token across its fsync, and gives it back with `ColocatedNode::durable`; until then `step`, `tick` and `ready` panic.
- `node/policy.rs` → the driver-policy methods (`resend_pending`, `take_back_delegated`, `move_round`, `step_down`, `campaign`, `campaign_above`, `advance_recovery`, `set_election_timeout`, `extend_pool`).
- `node/view.rs` → the read-only accessors (`config`, `limits`, `pool`, `acceptors`, `read_basis`, `hard_state`, `role`, `leader`, `read_log`, …).
- `node/counters.rs` → the private `Counters`, `RepairCounters`, `MembershipCounters` and the `NodeCounters` snapshot (`ColocatedNode::counters`).
- `node/{election,phase2,learn,replication,catch_up}.rs` → campaign (a paging campaign re-sends its `Prepare` to the peers that answered on every tick, #428 (paging livelock), asked from one past the highest slot the tally merged (`Election::keep_alive_from`), so an answered peer re-affirms with an empty page, never its whole suffix again), Phase-2 open/delegate/decide (`record_own_round`), learner, catch-up + trim-point jump (taken only as the answer to a `CatchUpRequest` the node sent below the point, `catchup_asked`; an unasked one makes the node ask); a follower pulls a gap from the beat's sender, and from the whole pool once its pulls at one chosen index outlast an election timeout (`PULL_FALLBACK_BEATS` at least, #349: a dead lane to a renamed leader).
- `node/{authority,quorum_reads}.rs` → `CheckQuorum`; the read path (`PreRead` wiring, `serve_quorum_reads`/`tick_quorum_reads`).
- `node/{handoff,gc,matchmaking,reconfigure}.rs` → `DPaxos` handoff, GC wiring, matchmaking wiring, online reconfiguration (`relinquish_to`, `reconfigure`, `quorum_read`).
- `node/roll.rs` → `roll`, `RollResult`, `RollRefusal` → the `Roll` cut (§4.7): refused with no matchmakers; the tail segment from the chosen prefix and the open rounds (`tail_segment`). A `Roll` is its own `Command` variant, not a `Control`, so only `roll` proposes one; the tail also counts the `Roll`s a paged recovery has not proposed yet.
- `node/chain.rs` → `note_rolls`, `resend_segment_publishes`, `segment_publish_pending`, `on_segment_ack` → the publish of the chain: after the walk applied a `Roll`, every node of a matchmaker deployment publishes its end (`Ready::segment_publishes`) until a majority acks it; the plain arm has no publisher.
- `node/held.rs` → `on_segment_held`, `judge_held`, `held_segments` → the wiring of the held tally: the reports, the seal noted by the fold (`note_folded_rolls`), the completed publish.
- `node/route.rs` → `route_to_segment_replicas` → the route of R4d: a leader sends the `Commit` of each slot its fold applied to the replicas of that slot's segment (`Audience::ReplicasOf`), with the set the fold gives (`JournalState::replicas`, else `Config::segment_replicas`), at a ballot that chose the slot (the `choosing` map: the leader's own decision or a learned `Commit`, never its record's ballot); a slot with no choosing ballot, a follower and the plain arm route nothing.
- `node/{boot,acceptor,helpers,invariants}.rs` → boot path, acceptor wiring, `adopt_configuration`, `assert_invariants`.
- `node/tests.rs` + `node/tests/*.rs` → unit tests, one file per concern.
- `proxy_leader.rs` → `ProxyLeader` → the second deployment (#142): fan-out, fold, `Commit`, `Nack` relay, and the beat `tick` (re-send page and bounded retention, both from its `Limits`); `proxy_leader/ready.rs` → `ProxyReady`. Its TLA+ model is `specs/tla/proxy-leader/` (#1188 (rework 2.9)): a `Commit` only on a Phase-2 quorum of the round's own configuration. `ProxyLeader::new(id, &Config, limits)` reads the bootstrap configuration, the pool and the matchmakers from the deployment's `Config`; a delegation whose configuration it cannot run (outside its pool, or none after a restart) is ignored, and the leader moves the round.
- `replica_node.rs` → `ReplicaNode`, `ReplicaCounters` → the third deployment (#144); module doc holds the coupling analysis; `replica_node/ready.rs` → `ReplicaReady`; `replica_node/tests.rs` → three `ColocatedNode`s and two replicas over a hand-driven network; `replica_node/tests/segments.rs` → a replica of one segment: the stop at the `Roll`, the join from the boundary state (`join_segment`, `JoinRefusal`), the sealed answer (`LogRead::Sealed`, `ReplicaReady::sealed_reads`).

Test-only:

- `test_support.rs` → the shared builders (`MemStorage::for_node`, `from_node`).
- `models.rs` + `models/` → the model checkers: `handover.rs` (the matchmaker handover; half its seeds page by one or two journals, #439; claim 11: the chain of segments crosses a handover and only grows on a disk), `proxy.rs` (the proxy leader), `support.rs` (the seeded RNG and lossy mailbox both share).

## Public surface

Every module is `pub`, so every item has one path under its module (`paros_consensus::journal::Command`,
`paros_consensus::node::NodeCounters`, `paros_consensus::contract::limits::PROMISE_BATCH`). The root
re-exports only what a driver loop needs: the deployments (`ColocatedNode`, `Ready`,
`ProxyLeader`, `ReplicaNode`, `Matchmaker`, `MatchmakerReconfigurer`), the contract (`Config`,
`HardState`, `Limits`, `Message`, `Storage`, `WriteOp`) and the scalars (`Ballot`, `NodeId`,
`Slot`, `Value`). A deployment takes its `Limits` at construction (`ColocatedNode::new(storage,
limits)`), and reads them back with `limits()`; `set_election_timeout` stays a method, because a
driver draws a new timeout per election.

## Developer experience

A new user reads `README.md`, then the crate root's rustdoc (`src/lib.rs`: a quick start that
is a doc test), then `examples/grow.rs`. Keep the three in step with the API.

- Examples (Pierre, 2026-11-25: one example that grows the whole system): `examples/single_decree.rs`
  runs single-decree Paxos on `Decree` and `Acceptor`; a decree has no deployment, so it gives
  each `Acceptor` its disk itself. `examples/grow.rs` is the one tour of the deployments: one
  journal grows from three nodes to the whole deployment by reconfiguration, one stage per
  function, on the toy cluster. A new flavor or driver call gets a stage there, not a new
  example. Each claim of a trace is an assertion, and no example builds a node from the roles
  by hand: a reader copies what an example does. A change of the stages updates the step
  list in `README.md`, the root `README.md` and the table of `src/lib.rs`.
- `examples/common/cluster.rs` → `Cluster`, the toy cluster: the nodes with one `MemStorage`
  each, and from the `Config` the `Matchmaker`s (a `MemRegistry` each) and the `ProxyLeader`s;
  `add_replicas` starts `ReplicaNode`s at any time, and `restart_proxy` a `ProxyLeader`. The
  wire can lose a message (`settle_where`), and `take_delivered` counts what reached each process. `Cluster::pump` is the reference driver loop (persist, send,
  learn, advance). A new example that drives a deployment uses it and does not write a
  network of its own.
- The small helpers of a driver, each with a doc example: `Ready::is_empty` (and the same on
  `ReplicaReady` and `ProxyReady`) ends a drain loop; `ColocatedNode::campaign` opens an election
  with no clock; `Config::with_peers` sorts and deduplicates a membership; `Message::kind`
  names a variant for a trace or a metric; `NodeId`, `Slot` and `Ballot` (`round.node`) have
  `Display`.
- A doc example is a test: `cargo test --doc -p paros-consensus` (CI `checks` job). A new
  public entry point of a driver gets one.
- A boot assertion on `Config` names the field that is wrong (`Config::assert_valid`, which
  `ColocatedNode::new` and `ReplicaNode::new` run; a driver can run it on a configuration it
  built).

## Local rules

- `ColocatedNode::assert_invariants` (`node/invariants.rs`, `pub(super)`) runs at boot and at
  the exit of every public mutating entry point; a new entry point calls it.
- Hard `assert!` only, never `debug_assert!`; every public fn that can panic has `# Panics`
  (clippy pedantic enforces it).
- `AcceptorConfig::try_new` / `try_zoned` / `try_with_counts` and `MatchmakerSet::try_new` are the
  only constructors (deserialisation included, through `TryFrom`); they answer a malformed
  membership with a `ShapeError`. `new`, `zoned` and `with_counts` panic on it, for values the
  program builds itself. A configuration from the wire or an operator goes through `try_*`.
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

- `cargo nextest run -p paros-consensus` — unit tests and both model checkers.
- Model-checker env vars (the only ones this crate reads): `HANDOVER_MODEL_SEEDS`,
  `HANDOVER_MODEL_STEPS`, `HANDOVER_MODEL_TRACE`, `PROXY_MODEL_SEEDS`, `PROXY_MODEL_STEPS`,
  `PROXY_MODEL_FROM`, `PROXY_MODEL_TRACE` (`models/support.rs`, `env_or`).
- `cargo run -p paros-consensus --example <name>` for each of the two examples, and
  `cargo test --doc -p paros-consensus` for the doc examples (CI `checks` job).
- `cargo check --target wasm32-unknown-unknown -p paros-consensus` with and without
  `--no-default-features`; `cargo check -p paros-consensus --features serde` (CI `portability`).
- `RUSTDOCFLAGS="-D warnings" cargo doc -p paros-consensus --no-deps` (CI `checks` job).
- `scripts/mutants.sh` (`.cargo/mutants.toml`): a mutant of this crate must fail a test of
  this crate or of `consensus-sim` (add `-f <file>` to cut the set). CI runs the mutants of the lines main changed
  each night (`.github/workflows/mutants-nightly.yml`) and every mutant each week (`.github/workflows/mutants.yml`).
- Every change needs unit tests here, and a change to a protocol rule needs its TLA+ model in
  `specs/tla/` first (root `AGENTS.md`). The `ub-mode` skill measures coverage (crapper,
  mutator).

## Deps & constants

- Features (`Cargo.toml`): `default = ["tracing", "assertions"]`; deps `serde`, `tracing` and
  `moonpool-assertions` (git rev pinned), all optional. Zero deps with `--no-default-features`.
  `consensus-sim` pins `moonpool-walk` to the same rev: advance both together.
- `probe!` observes only and compiles to nothing with the `assertions` feature off. Never
  reword a probe's message (the slot is its hash).
- Constants: the page ceilings and the read-window floor are in `contract::limits`;
  `HANDOFF_BATCH` (`Limits::DEFAULT.promise_page`), `HANDOFF_FENCE_ELECTIONS`,
  `HEARTBEAT_TICKS` and `REPAIR_TIMEOUT_ELECTIONS` are in `node`.
- The crate is public, published with `paros` at one version (root `AGENTS.md`, "Releases").
  release-plz creates and writes its `CHANGELOG.md`; never edit a changelog by hand.
