# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-05

### 🚀 Features

- **paros**: Make the NodeStorage and MatchmakerStorage seams async ([#136](https://github.com/PierreZ/paros/pull/136))
- **core**: Cooperative leader handoff (DPaxos "Leader Handoff") ([#118](https://github.com/PierreZ/paros/pull/118))
- Stage 8 — disk faults C: protocol-aware recovery (CTRL) ([#21](https://github.com/PierreZ/paros/pull/21)) ([#112](https://github.com/PierreZ/paros/pull/112))
- **storage**: Stage 7 disk faults B — corruption detection (CLStore) ([#111](https://github.com/PierreZ/paros/pull/111))
- **storage**: Stage 6 disk faults A — fail-stop ([#19](https://github.com/PierreZ/paros/pull/19)) ([#110](https://github.com/PierreZ/paros/pull/110))
- **core**: Plumb configuration identities ([#109](https://github.com/PierreZ/paros/pull/109))
- **core**: CheckQuorum — a leader without an ack quorum for an election timeout steps down
- **sim**: Widen the BUGGIFY surface; drift-immune gap-wedge detection
- **sim**: Make the #88 mid-election snapshot window reachable
- **sim**: Raise the odds of the #88/#80/#60 scenarios per seed
- **sim**: Saturate guided chain exploration
- **sim**: Add chain state-machine campaign

### 🐛 Bug Fixes

- **core**: Bound promise and recovery batches ([#106](https://github.com/PierreZ/paros/pull/106))
- **paros**: Ack-on-commit verifies the decided command is the waiter's own
- **paros**: A durable compaction floor never outruns the durable application state
- **core**: #94 at-most-once — session ledger travels with truncation and snapshots; duplicates suppress at the apply seam
- **core**: Land the adversarial review's findings; arm the at-most-once oracle
- **paros**: The tick deadline is absolute, not a fresh sleep per select pass
- **sim**: Drive the mid-election snapshot gate from the driver's role check
- Close grpc channels on shutdown

### 📚 Documentation

- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))
- Add project logo and README quick reference

### 🚜 Refactor

- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))
- **sim**: Move correctness checking from trace scanning to an audit port
- **rpc**: Encode consensus traffic with protobuf

### 🧪 Testing

- **sim**: Compare external replica digests
- **sim**: Pin the #94/#95-arc regression seeds; clippy/doc polish

### ⚙️ Miscellaneous Tasks

- Organize rust crates under crates

### 📦 Other

- Advance moonpool to the single-stream canary and put the campaign under it ([#138](https://github.com/PierreZ/paros/pull/138))
- Review of #133: protocol fixes, the composable core, the driver, and the simulation's reach ([#134](https://github.com/PierreZ/paros/pull/134))
- Matchmaker GC, reconfiguration under the full fault matrix, matchmaker-set generations (#123, #124, #125) ([#133](https://github.com/PierreZ/paros/pull/133))
- Leader matchmaking phase, cross-configuration Phase 1, online reconfiguration (#120, #121, #122) ([#132](https://github.com/PierreZ/paros/pull/132))
- Matchmaker state machine, durable configuration registry, and a per-seed deployment role map ([#119](https://github.com/PierreZ/paros/pull/119)) ([#131](https://github.com/PierreZ/paros/pull/131))
- Instrument every important method; core spans behind a default-on `tracing` feature ([#130](https://github.com/PierreZ/paros/pull/130))
- Sim harness: node shape survives restarts, frontier-tied convergence, hook coverage; core: local protocol assertions ([#129](https://github.com/PierreZ/paros/pull/129))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))
- Retire the red demos and every pinned seed; widen the BUGGIFY surface ([#127](https://github.com/PierreZ/paros/pull/127))
- Unify network chaos into the main campaign (moonpool 43304d8); driver: per-kind keep-newest peer mailbox ([#126](https://github.com/PierreZ/paros/pull/126))
- Audit follow-up, randomness half: repair-plane chaos, chunk-repair seams, born-buggified knobs ([#117](https://github.com/PierreZ/paros/pull/117))
- Audit follow-up: strengthen core asserts, audit oracles, and the BUGGIFY surface ([#116](https://github.com/PierreZ/paros/pull/116))
- M3 finishing batch: CTRL evaluation corpus ([#113](https://github.com/PierreZ/paros/pull/113)) + Control::Snap chunked snapshot repair ([#101](https://github.com/PierreZ/paros/pull/101)) ([#114](https://github.com/PierreZ/paros/pull/114))

