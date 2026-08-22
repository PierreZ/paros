# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-08-22

### 🚀 Features

- **paros**: Per-message SendFilter hook at the driver's send point
- **core**: Nack carries the acceptor's promised ballot for faster convergence
- Linearizable reads served through a read-index protocol ([#42](https://github.com/PierreZ/paros/pull/42))
- Snapshot transfer recovers below-floor nodes
- **core**: Truncation as a Paxos-decided control command
- **core**: Compact() truncates the chosen log prefix ([#18](https://github.com/PierreZ/paros/pull/18))
- **core**: Commit-replay catch-up so every live node converges
- **sim**: Simulation-driven development + crash/restart, proving a restart safety bug
- Multi-slot replicated log + stable leader ([#16](https://github.com/PierreZ/paros/pull/16))
- Single-decree Paxos safety kernel under network chaos ([#15](https://github.com/PierreZ/paros/pull/15))
- Moonpool integration — sim driver, oracle harness, wasm demo ([#14](https://github.com/PierreZ/paros/pull/14))
- Scaffold workspace + paros-core types ([#13](https://github.com/PierreZ/paros/pull/13))

### 🐛 Bug Fixes

- **core**: A command is applied when the prefix reaches it, not when it is chosen
- **core**: No-op gap fill at election, so the chosen prefix never wedges
- **core**: Acceptors refuse below-floor prepares and accepts ([#18](https://github.com/PierreZ/paros/pull/18))

### 🚜 Refactor

- Drop snapshot seams, the application owns compaction ([#18](https://github.com/PierreZ/paros/pull/18))

### 🧪 Testing

- **sim**: Every committed ack names its slot, and an oracle checks it
- **sim**: Oracle for the election hole, and the slot starvation that reaches it

### 📦 Other

- MustSync tests, pinned-seed corpus, and book updates
- Semantic durable-storage substrate: split HardState, per-slot writes

