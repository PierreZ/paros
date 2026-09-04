# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

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
