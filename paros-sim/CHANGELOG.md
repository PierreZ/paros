# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-28

### 🚀 Features

- **wasm**: Self-describing Multi-Paxos demo + Watch-it-live category ([#29](https://github.com/PierreZ/paros/pull/29))
- **sim**: Simulation-driven development + crash/restart, proving a restart safety bug
- Multi-slot replicated log + stable leader ([#16](https://github.com/PierreZ/paros/pull/16))
- Single-decree safety visualization ([#28](https://github.com/PierreZ/paros/pull/28))
- Single-decree Paxos safety kernel under network chaos ([#15](https://github.com/PierreZ/paros/pull/15))
- Moonpool integration — sim driver, oracle harness, wasm demo ([#14](https://github.com/PierreZ/paros/pull/14))
- Scaffold workspace + paros-core types ([#13](https://github.com/PierreZ/paros/pull/13))

### ⚡ Performance

- **sim**: Bound the sancov sweep and scope it to the shipped library

### 🚜 Refactor

- Promote provider-generic driver into paros; drop paros-storage ([#14](https://github.com/PierreZ/paros/pull/14))

