# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-07

### 🚀 Features

- Remove role slots; placement orders by busyness, heal is shown (#837 (remove role slots)) ([#842](https://github.com/PierreZ/paros/pull/842))
- The cell tenant's matchmaker set moves, with the cached registry fold as its boot path (#685 (special tenants are tenants) step 4a) ([#847](https://github.com/PierreZ/paros/pull/847))
- **machine**: PAROS_ROLES, default all; a restart may widen a machine's roles ([#839](https://github.com/PierreZ/paros/pull/839))
- The universe tenant's matchmaker set moves like a tenant's (#685 (special tenants are tenants)) ([#835](https://github.com/PierreZ/paros/pull/835))
- Control plane mode, tenant control journals grow past the founders (#685 (special tenants are tenants) steps 1-2) ([#821](https://github.com/PierreZ/paros/pull/821))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- **resolve, client**: Reach the universe cell through its pointer; a deleted moved journal ends the follow; a move found flipped reads its target (main reds) ([#891](https://github.com/PierreZ/paros/pull/891))
- **balance, resolve, move, election, client**: Main reds seen on #883's hunt, and the moved-out gate ([#885](https://github.com/PierreZ/paros/pull/885))
- Main after #859 (founders boot hint): controls, sets, matchmaker clears, injector race, watch ([#864](https://github.com/PierreZ/paros/pull/864))
- **fix-main**: A set's handover waits for a down target member; the election check gets a new term's window ([#855](https://github.com/PierreZ/paros/pull/855))
- **universe**: A re-sent add-cell repoints a joined cell at the universe machines' current names ([#852](https://github.com/PierreZ/paros/pull/852))
- **fix-main**: CheckQuorum acks, round memo, fleet and sets windows, storage gate, parked replicas ([#845](https://github.com/PierreZ/paros/pull/845))
- Shard replicas by host, stores publish their layouts, sets wait on the founders ([#772](https://github.com/PierreZ/paros/pull/772))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- **core**: One path per item, a short root (#730 (paros-core rework))
- **coordinator**: Split cell.rs by concern (#722 (three products))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Remove the founders: initial members only bootstrap the cell (#685 (no founders)) ([#883](https://github.com/PierreZ/paros/pull/883))
- Remove the founder rules: every member holds a slot (#685 second PR) ([#865](https://github.com/PierreZ/paros/pull/865))
- Ub-mode on every crate: tests for sim-only paths, one rule in one place (#824-#833) ([#861](https://github.com/PierreZ/paros/pull/861))
- Move a tenant between cells (#782-#785) ([#862](https://github.com/PierreZ/paros/pull/862))
- Sim, coordinator: fix main after #864 (fix main after #859) ([#874](https://github.com/PierreZ/paros/pull/874))
- Founders are a boot hint; auto F grows; start with one machine (#685 steps 5 to 7) ([#859](https://github.com/PierreZ/paros/pull/859))
- Grow the cell plan journals past the founders (#685 step 4b) ([#851](https://github.com/PierreZ/paros/pull/851))
- Balance a cell: busyness, warm moves, leader balance, cell balance (#687 (balance a cell), parts 2 and 3) ([#853](https://github.com/PierreZ/paros/pull/853))
- Balance a cell by role counts (#687 (balance a cell), part 1) ([#850](https://github.com/PierreZ/paros/pull/850))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- Client, sim: merge a pipeline's fenced writes, fire the batcher's rare paths ([#770](https://github.com/PierreZ/paros/pull/770))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

