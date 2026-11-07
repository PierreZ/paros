# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-07

### 🚀 Features

- **paros-sim**: Matchmaker sets declare shards, moonpool's gate replaces the matchmaker ledger (#760 slice 2) ([#834](https://github.com/PierreZ/paros/pull/834))

### 🐛 Bug Fixes

- Main after #859 (founders boot hint): controls, sets, matchmaker clears, injector race, watch ([#864](https://github.com/PierreZ/paros/pull/864))
- **paros-roles**: A torn matchmaker registry drops its set alone ([#848](https://github.com/PierreZ/paros/pull/848))
- Shard replicas by host, stores publish their layouts, sets wait on the founders ([#772](https://github.com/PierreZ/paros/pull/772))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- **core**: One path per item, a short root (#730 (paros-core rework))
- **core**: Limits replaces the page setters (#730 (paros-core rework))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Remove the founders: initial members only bootstrap the cell (#685 (no founders)) ([#883](https://github.com/PierreZ/paros/pull/883))
- Ub-mode on every crate: tests for sim-only paths, one rule in one place (#824-#833) ([#861](https://github.com/PierreZ/paros/pull/861))
- Split a node's sync on BUGGIFY so its entries-durable hint fires ([#495](https://github.com/PierreZ/paros/pull/495)) ([#870](https://github.com/PierreZ/paros/pull/870))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

