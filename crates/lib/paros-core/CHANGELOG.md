# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

## [0.2.0] - 2026-11-02

### 🚀 Features

- **paros-sim**: Matchmaker sets declare shards, moonpool's gate replaces the matchmaker ledger (#760 slice 2) ([#834](https://github.com/PierreZ/paros/pull/834))
- **sim**: Moonpool owns fault placement, part 1 (#760 (moonpool owns fault placement)) ([#762](https://github.com/PierreZ/paros/pull/762))

### 🐛 Bug Fixes

- **fix-main**: CheckQuorum acks, round memo, fleet and sets windows, storage gate, parked replicas ([#845](https://github.com/PierreZ/paros/pull/845))
- **core**: A stale Accept of the predecessor does not depose a handoff successor ([#807](https://github.com/PierreZ/paros/pull/807))
- Five red seeds on main (replica boot report, wipe before provisioning, handoff prior configurations, gate-off injector, placeholder tag) ([#815](https://github.com/PierreZ/paros/pull/815))
- Shard replicas by host, stores publish their layouts, sets wait on the founders ([#772](https://github.com/PierreZ/paros/pull/772))
- Slow-machine strikes in the chaos window only; a writer never moves back; a campaign adopts a reconfiguration its leadership ran ([#737](https://github.com/PierreZ/paros/pull/737))

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- **core**: One path per item, a short root (#730 (paros-core rework))
- **core**: MemStorage replaces the test and example stores (#730 (paros-core rework))
- **core**: Node.rs splits into policy, view and counters (#730 (paros-core rework))
- **core**: NodeCounters replaces the eight counter getters (#730 (paros-core rework))
- **core**: Limits replaces the page setters (#730 (paros-core rework))
- **core**: MembershipProbe moves to matchmaking/probe.rs (#730 (paros-core rework))
- **core**: Each Ready lives in its deployment (#730 (paros-core rework))
- **core**: Contract/ holds what every deployment speaks (#730 (paros-core rework))
- **core**: Journal/ holds ids, command and state (#730 (paros-core rework))
- **core**: Membership/zone_rule's tests move to membership/zone_rule/tests.rs (#730 (paros-core rework))
- **core**: Membership/quorum_system's tests move to membership/quorum_system/tests.rs (#730 (paros-core rework))
- **core**: Batcher's tests move to batcher/tests.rs (#730 (paros-core rework))
- **core**: Acceptor's tests move to acceptor/tests.rs (#730 (paros-core rework))
- **core**: Proposer/rounds's tests move to proposer/rounds/tests.rs (#730 (paros-core rework))
- **core**: Decree's tests move to decree/tests.rs (#730 (paros-core rework))
- **core**: Membership/acceptor_config's tests move to membership/acceptor_config/tests.rs (#730 (paros-core rework))
- **core**: Quorum_read's tests move to quorum_read/tests.rs (#730 (paros-core rework))
- **core**: Journal_state's tests move to journal_state/tests.rs (#730 (paros-core rework))
- **core**: Replica's tests move to replica/tests.rs (#730 (paros-core rework))
- **core**: Proxy_leader's tests move to proxy_leader/tests.rs (#730 (paros-core rework))
- **core**: Proposer's tests move to proposer/tests.rs (#730 (paros-core rework))
- **core**: Matchmaker/reconfigurer's tests move to matchmaker/reconfigurer/tests.rs (#730 (paros-core rework))
- **core**: Matchmaker's tests move to matchmaker/tests.rs (#730 (paros-core rework))
- **core**: The model checkers move to models/ (#730 (paros-core rework))
- One crate per layer, grouped under crates/ (#722 (three products))

### 🎨 Styling

- **core**: Rustfmt the moved tests (#730 (paros-core rework))

### ⚙️ Miscellaneous Tasks

- **core**: The layer rule, its CI check and the docs (#730 (paros-core rework))

### 📦 Other

- Ub-mode pass — GC ack in the core, 83 new unit tests, unit tests and sim both ([#823](https://github.com/PierreZ/paros/pull/823))
- One live universe the operator drives ([#813](https://github.com/PierreZ/paros/pull/813)) ([#820](https://github.com/PierreZ/paros/pull/820))
- Harness rot and wipes go through moonpool's shard gate; the rot and cut budgets go (#760 (moonpool owns fault placement)) ([#774](https://github.com/PierreZ/paros/pull/774))
- One mutant per oracle excuse, the negative hunt; a leader's GC finishes a frozen matchmaker set ([#771](https://github.com/PierreZ/paros/pull/771))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))


### 🚀 Features

- **core**: the candidate's matchmaking phase is a public role,
  `paros_core::matchmaking::Matchmaking` — the registration tally, the union
  of histories above the maximum watermark (`H_b`), the effective
  configuration and the stale-belief signal — folded page by page
  (`RegisteredPage`, `MatchFold`) exactly as `ColocatedNode` drives it. The
  node keeps the wiring; the role can now be composed by hand.
- **core**: `MemRegistry`, the reference in-memory `RegistryStorage` with the
  library's semantics for every `MatchmakerWriteOp`, replaces the four ad-hoc
  test registries; the handover model checker now reboots matchmakers from it.
- **core**: `Decree::ballot`, `value`, `adopted_prior_vote` and `preempted`
  are public, so a driver can observe a running decree through
  `ReconfigurerPhase::Deciding`.

### 📚 Documentation

- **core**: three runnable, deterministic examples that drive the composable
  roles by hand — `single_decree` (Phase 1, P2c, Phase 2), `multi_paxos`
  (slots versus ballots, amortized Phase 1, per-slot recovery) and
  `matchmaker` (configuration discovery, reconfiguration, and the matchmaker
  set chosen by the same single-decree Paxos over `Vec<MatchmakerId>`). Run
  with `cargo run -p paros-core --example <name>`.
- **core**: every intra-doc link in the crate resolves; CI builds the docs
  with warnings denied.

### 🚜 Refactor

- **core**: `RawNode` is renamed `ColocatedNode`. The type is not a "raw"
  anything: it is the deployment that colocates the `Acceptor`, `Proposer`
  and `Replica` roles on one node and wires them together. The driver half of
  the etcd-raft split it mirrors keeps its name, `paros::run_node`, so only
  the type and its methods are affected — a caller renames the type and
  nothing else.
