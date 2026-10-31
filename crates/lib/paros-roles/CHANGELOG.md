# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-31

### 🚀 Features

- **sim**: Moonpool owns fault placement, part 1 (#760 (moonpool owns fault placement)) ([#762](https://github.com/PierreZ/paros/pull/762))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- An adopted book adds the founders as seeds; the leads oracle takes the loss excuses ([#781](https://github.com/PierreZ/paros/pull/781))
- Learn a moved leader's address, and wait out a coordinator handover in the tail ([#761](https://github.com/PierreZ/paros/pull/761))
- **frontend**: Reach every storage machine of the cell ([#700](https://github.com/PierreZ/paros/pull/700)) ([#744](https://github.com/PierreZ/paros/pull/744))
- A machine's clean stop waits for its frontend (#723 (frontend as a parosd role))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- **core**: One path per item, a short root (#730 (paros-core rework))
- **core**: NodeCounters replaces the eight counter getters (#730 (paros-core rework))
- **core**: Limits replaces the page setters (#730 (paros-core rework))
- **roles**: Split node.rs by concern (#722 (three products))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Faults, acceptor state, Paxos roles and replay mode (#518 (paros playground)) ([#803](https://github.com/PierreZ/paros/pull/803))
- A machine serves its booked replicas (#765 (booked proxies and replicas)) ([#780](https://github.com/PierreZ/paros/pull/780))
- A machine serves its booked proxy leaders (#765 (booked proxies and replicas)) ([#775](https://github.com/PierreZ/paros/pull/775))
- One mutant per oracle excuse, the negative hunt; a leader's GC finishes a frozen matchmaker set ([#771](https://github.com/PierreZ/paros/pull/771))
- Client, sim: merge a pipeline's fenced writes, fire the batcher's rare paths ([#770](https://github.com/PierreZ/paros/pull/770))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

