# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-07

### 🚀 Features

- Remove role slots; placement orders by busyness, heal is shown (#837 (remove role slots)) ([#842](https://github.com/PierreZ/paros/pull/842))
- The cell tenant's matchmaker set moves, with the cached registry fold as its boot path (#685 (special tenants are tenants) step 4a) ([#847](https://github.com/PierreZ/paros/pull/847))
- **paros-sim**: Matchmaker sets declare shards, moonpool's gate replaces the matchmaker ledger (#760 slice 2) ([#834](https://github.com/PierreZ/paros/pull/834))
- **machine**: PAROS_ROLES, default all; a restart may widen a machine's roles ([#839](https://github.com/PierreZ/paros/pull/839))
- Control plane mode, tenant control journals grow past the founders (#685 (special tenants are tenants) steps 1-2) ([#821](https://github.com/PierreZ/paros/pull/821))
- **sim**: Moonpool owns fault placement, part 1 (#760 (moonpool owns fault placement)) ([#762](https://github.com/PierreZ/paros/pull/762))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- **resolve**: An initial member reads the registry through every machine its cache names (main red) ([#889](https://github.com/PierreZ/paros/pull/889))
- **balance, resolve, move, election, client**: Main reds seen on #883's hunt, and the moved-out gate ([#885](https://github.com/PierreZ/paros/pull/885))
- **sim, frontend**: Three tail reds on main (plan journal wipe, fenced term, moved universe founder) ([#879](https://github.com/PierreZ/paros/pull/879))
- **frontend**: An admitted machine's frontend finds a renamed founder through its book ([#875](https://github.com/PierreZ/paros/pull/875))
- **paros-roles**: Re-sends to a matchmaker back off while nobody answers ([#857](https://github.com/PierreZ/paros/pull/857))
- **paros-roles**: A torn matchmaker registry drops its set alone ([#848](https://github.com/PierreZ/paros/pull/848))
- **fix-main**: CheckQuorum acks, round memo, fleet and sets windows, storage gate, parked replicas ([#845](https://github.com/PierreZ/paros/pull/845))
- Released tenant control journal stops; registration reads the election from itself last (fix-main) ([#838](https://github.com/PierreZ/paros/pull/838))
- Five red seeds on main (replica boot report, wipe before provisioning, handoff prior configurations, gate-off injector, placeholder tag) ([#815](https://github.com/PierreZ/paros/pull/815))
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

- Remove the founders: initial members only bootstrap the cell (#685 (no founders)) ([#883](https://github.com/PierreZ/paros/pull/883))
- Remove the founder rules: every member holds a slot (#685 second PR) ([#865](https://github.com/PierreZ/paros/pull/865))
- A moved tenant: a moved verdict, and a client that follows it (#785 (moved record)) ([#876](https://github.com/PierreZ/paros/pull/876))
- Ub-mode on every crate: tests for sim-only paths, one rule in one place (#824-#833) ([#861](https://github.com/PierreZ/paros/pull/861))
- Move a tenant between cells (#782-#785) ([#862](https://github.com/PierreZ/paros/pull/862))
- Delist the rare gates the sweep now fires, make four more fire or go ([#486](https://github.com/PierreZ/paros/pull/486)) ([#869](https://github.com/PierreZ/paros/pull/869))
- Founders are a boot hint; auto F grows; start with one machine (#685 steps 5 to 7) ([#859](https://github.com/PierreZ/paros/pull/859))
- Fix main: a peer dials a moved machine where its own batches say; a spare keeps its remote follow ([#863](https://github.com/PierreZ/paros/pull/863))
- Grow the cell plan journals past the founders (#685 step 4b) ([#851](https://github.com/PierreZ/paros/pull/851))
- Balance a cell: busyness, warm moves, leader balance, cell balance (#687 (balance a cell), parts 2 and 3) ([#853](https://github.com/PierreZ/paros/pull/853))
- Ub-mode pass — GC ack in the core, 83 new unit tests, unit tests and sim both ([#823](https://github.com/PierreZ/paros/pull/823))
- Faults, acceptor state, Paxos roles and replay mode (#518 (paros playground)) ([#803](https://github.com/PierreZ/paros/pull/803))
- A machine serves its booked replicas (#765 (booked proxies and replicas)) ([#780](https://github.com/PierreZ/paros/pull/780))
- A machine serves its booked proxy leaders (#765 (booked proxies and replicas)) ([#775](https://github.com/PierreZ/paros/pull/775))
- One mutant per oracle excuse, the negative hunt; a leader's GC finishes a frozen matchmaker set ([#771](https://github.com/PierreZ/paros/pull/771))
- Client, sim: merge a pipeline's fenced writes, fire the batcher's rare paths ([#770](https://github.com/PierreZ/paros/pull/770))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

