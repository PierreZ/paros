# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-08-22

### 🚀 Features

- **paros**: Per-message SendFilter hook at the driver's send point
- **core**: Nack carries the acceptor's promised ballot for faster convergence
- Linearizable reads served through a read-index protocol ([#42](https://github.com/PierreZ/paros/pull/42))
- **sim**: Read workload + linearizability oracle; sweep red on seed 286172402316494352 ([#42](https://github.com/PierreZ/paros/pull/42))
- Snapshot transfer recovers below-floor nodes
- **core**: Truncation as a Paxos-decided control command
- Persist truncation and expose the Compact RPC ([#18](https://github.com/PierreZ/paros/pull/18))
- **core**: Compact() truncates the chosen log prefix ([#18](https://github.com/PierreZ/paros/pull/18))
- **core**: Commit-replay catch-up so every live node converges
- **wasm**: Watch a node crash at the persist/send seam and recover
- **sim**: Simulation-driven development + crash/restart, proving a restart safety bug
- Multi-slot replicated log + stable leader ([#16](https://github.com/PierreZ/paros/pull/16))
- Single-decree safety visualization ([#28](https://github.com/PierreZ/paros/pull/28))
- Single-decree Paxos safety kernel under network chaos ([#15](https://github.com/PierreZ/paros/pull/15))
- Scaffold workspace + paros-core types ([#13](https://github.com/PierreZ/paros/pull/13))

### 🐛 Bug Fixes

- **core**: No-op gap fill at election, so the chosen prefix never wedges

### 📚 Documentation

- Add project logo and README quick reference

### 🚜 Refactor

- Drop snapshot seams, the application owns compaction ([#18](https://github.com/PierreZ/paros/pull/18))
- Promote provider-generic driver into paros; drop paros-storage ([#14](https://github.com/PierreZ/paros/pull/14))

### 🧪 Testing

- **sim**: Every committed ack names its slot, and an oracle checks it
- **sim**: Oracle for the election hole, and the slot starvation that reaches it

### ⚙️ Miscellaneous Tasks

- Bump moonpool pin past the deterministic executor ([#65](https://github.com/PierreZ/paros/pull/65))

### 📦 Other

- MustSync tests, pinned-seed corpus, and book updates
- Seam-targeted crash injection at the persist/send seam (first buggify use)
- Per-slot persist events + recovery oracle
- Semantic durable-storage substrate: split HardState, per-slot writes

