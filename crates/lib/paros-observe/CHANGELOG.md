# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-02

### 🐛 Bug Fixes

- **fix-main**: CheckQuorum acks, round memo, fleet and sets windows, storage gate, parked replicas ([#845](https://github.com/PierreZ/paros/pull/845))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- **core**: One path per item, a short root (#730 (paros-core rework))
- **core**: Node.rs splits into policy, view and counters (#730 (paros-core rework))
- One crate per layer, grouped under crates/ (#722 (three products))

