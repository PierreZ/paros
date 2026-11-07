# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-07

### 🚀 Features

- Remove role slots; placement orders by busyness, heal is shown (#837 (remove role slots)) ([#842](https://github.com/PierreZ/paros/pull/842))
- The cell tenant's matchmaker set moves, with the cached registry fold as its boot path (#685 (special tenants are tenants) step 4a) ([#847](https://github.com/PierreZ/paros/pull/847))
- **machine**: PAROS_ROLES, default all; a restart may widen a machine's roles ([#839](https://github.com/PierreZ/paros/pull/839))
- Control plane mode, tenant control journals grow past the founders (#685 (special tenants are tenants) steps 1-2) ([#821](https://github.com/PierreZ/paros/pull/821))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- **resolve, client**: Reach the universe cell through its pointer; a deleted moved journal ends the follow; a move found flipped reads its target (main reds) ([#891](https://github.com/PierreZ/paros/pull/891))
- **balance, resolve, move, election, client**: Main reds seen on #883's hunt, and the moved-out gate ([#885](https://github.com/PierreZ/paros/pull/885))
- **universe**: A re-sent add-cell repoints a joined cell at the universe machines' current names ([#852](https://github.com/PierreZ/paros/pull/852))
- Released tenant control journal stops; registration reads the election from itself last (fix-main) ([#838](https://github.com/PierreZ/paros/pull/838))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- **core**: One path per item, a short root (#730 (paros-core rework))
- **model**: Split registry.rs by concern (#722 (three products))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Remove the founders: initial members only bootstrap the cell (#685 (no founders)) ([#883](https://github.com/PierreZ/paros/pull/883))
- Remove the founder rules: every member holds a slot (#685 second PR) ([#865](https://github.com/PierreZ/paros/pull/865))
- A moved tenant: a moved verdict, and a client that follows it (#785 (moved record)) ([#876](https://github.com/PierreZ/paros/pull/876))
- Ub-mode on every crate: tests for sim-only paths, one rule in one place (#824-#833) ([#861](https://github.com/PierreZ/paros/pull/861))
- Move a tenant between cells (#782-#785) ([#862](https://github.com/PierreZ/paros/pull/862))
- Make "a mode change waits for a member that is down" fire ([#777](https://github.com/PierreZ/paros/pull/777)); balance: abandon a move whose light machine left ([#872](https://github.com/PierreZ/paros/pull/872))
- Founders are a boot hint; auto F grows; start with one machine (#685 steps 5 to 7) ([#859](https://github.com/PierreZ/paros/pull/859))
- Grow the cell plan journals past the founders (#685 step 4b) ([#851](https://github.com/PierreZ/paros/pull/851))
- Balance a cell: busyness, warm moves, leader balance, cell balance (#687 (balance a cell), parts 2 and 3) ([#853](https://github.com/PierreZ/paros/pull/853))
- Balance a cell by role counts (#687 (balance a cell), part 1) ([#850](https://github.com/PierreZ/paros/pull/850))
- Add --layout colocated|compartmentalized to journal create ([#840](https://github.com/PierreZ/paros/pull/840))
- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- Each reference carries its zone; the client prefers its own ([#673](https://github.com/PierreZ/paros/pull/673)) ([#768](https://github.com/PierreZ/paros/pull/768))
- Judge journal healing from ground truth, not from the registry ([#739](https://github.com/PierreZ/paros/pull/739)) ([#766](https://github.com/PierreZ/paros/pull/766))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

