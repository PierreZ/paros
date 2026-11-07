# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-07

### 🚀 Features

- Remove role slots; placement orders by busyness, heal is shown (#837 (remove role slots)) ([#842](https://github.com/PierreZ/paros/pull/842))
- The cell tenant's matchmaker set moves, with the cached registry fold as its boot path (#685 (special tenants are tenants) step 4a) ([#847](https://github.com/PierreZ/paros/pull/847))
- **machine**: PAROS_ROLES, default all; a restart may widen a machine's roles ([#839](https://github.com/PierreZ/paros/pull/839))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- **resolve, client**: Reach the universe cell through its pointer; a deleted moved journal ends the follow; a move found flipped reads its target (main reds) ([#891](https://github.com/PierreZ/paros/pull/891))
- **resolve**: An initial member reads the registry through every machine its cache names (main red) ([#889](https://github.com/PierreZ/paros/pull/889))
- **frontend**: An admitted machine's frontend finds a renamed founder through its book ([#875](https://github.com/PierreZ/paros/pull/875))
- Released tenant control journal stops; registration reads the election from itself last (fix-main) ([#838](https://github.com/PierreZ/paros/pull/838))
- **frontend**: Reach every storage machine of the cell ([#700](https://github.com/PierreZ/paros/pull/700)) ([#744](https://github.com/PierreZ/paros/pull/744))
- A machine's clean stop waits for its frontend (#723 (frontend as a parosd role))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- **core**: One path per item, a short root (#730 (paros-core rework))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Remove the founders: initial members only bootstrap the cell (#685 (no founders)) ([#883](https://github.com/PierreZ/paros/pull/883))
- Remove the founder rules: every member holds a slot (#685 second PR) ([#865](https://github.com/PierreZ/paros/pull/865))
- Ub-mode on every crate: tests for sim-only paths, one rule in one place (#824-#833) ([#861](https://github.com/PierreZ/paros/pull/861))
- Move a tenant between cells (#782-#785) ([#862](https://github.com/PierreZ/paros/pull/862))
- Founders are a boot hint; auto F grows; start with one machine (#685 steps 5 to 7) ([#859](https://github.com/PierreZ/paros/pull/859))
- Fix main: a peer dials a moved machine where its own batches say; a spare keeps its remote follow ([#863](https://github.com/PierreZ/paros/pull/863))
- Grow the cell plan journals past the founders (#685 step 4b) ([#851](https://github.com/PierreZ/paros/pull/851))
- Balance a cell: busyness, warm moves, leader balance, cell balance (#687 (balance a cell), parts 2 and 3) ([#853](https://github.com/PierreZ/paros/pull/853))
- A machine serves its booked replicas (#765 (booked proxies and replicas)) ([#780](https://github.com/PierreZ/paros/pull/780))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- A machine serves its booked proxy leaders (#765 (booked proxies and replicas)) ([#775](https://github.com/PierreZ/paros/pull/775))
- Each reference carries its zone; the client prefers its own ([#673](https://github.com/PierreZ/paros/pull/673)) ([#768](https://github.com/PierreZ/paros/pull/768))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

