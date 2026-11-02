# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-02

### 🚀 Features

- The frontend is a role of parosd (#723 (frontend as a parosd role))
- **sandbox**: Run a paros universe in the browser (#518 (Paxos visualization sandbox)) ([#519](https://github.com/PierreZ/paros/pull/519))
- TLS and mutual TLS on every session, parosctl tls, TLS on half the sim seeds ([#509](https://github.com/PierreZ/paros/pull/509))
- **core**: The replica tier, core half (#144 part A) ([#171](https://github.com/PierreZ/paros/pull/171))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))
- **core**: Three runnable teaching examples for the composable roles ([#135](https://github.com/PierreZ/paros/pull/135))
- Add project logo and README quick reference

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- One crate per layer, grouped under crates/ (#722 (three products))
- Delete the inert wire plumbing, settle the open judgment calls, advance the moonpool pin ([#137](https://github.com/PierreZ/paros/pull/137))

### ⚙️ Miscellaneous Tasks

- Organize rust crates under crates

### 📦 Other

- Add --layout colocated|compartmentalized to journal create ([#840](https://github.com/PierreZ/paros/pull/840))
- Transparent logo with a dark-mode variant ([#841](https://github.com/PierreZ/paros/pull/841))
- One Astro site in one style, a landing page that sells parosd ([#810](https://github.com/PierreZ/paros/pull/810)) ([#814](https://github.com/PierreZ/paros/pull/814))
- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))
- Batchers and tenant role counts (#145 (batchers), #604 (role counts)) ([#698](https://github.com/PierreZ/paros/pull/698))
- Disk full goes read-only, admission control answers busy (#360 (disk full)) ([#683](https://github.com/PierreZ/paros/pull/683))
- Background scrubbing of stored records (#362 (background scrubbing)) ([#631](https://github.com/PierreZ/paros/pull/631))
- Add and remove cells: idle cells, universe add-cell and remove-cell (#232 (multiple cells), part 1) ([#593](https://github.com/PierreZ/paros/pull/593))
- Tenant modes: configure a journal's durability (#214 (tenant modes)) ([#574](https://github.com/PierreZ/paros/pull/574))
- Metrics, probes, usage ([#366](https://github.com/PierreZ/paros/pull/366)) and the bench ([#194](https://github.com/PierreZ/paros/pull/194)) ([#567](https://github.com/PierreZ/paros/pull/567))
- Acceptor partitions and moving a stuck round (#532 (acceptor partitions), #533 (move a stuck round)) ([#562](https://github.com/PierreZ/paros/pull/562))
- Tenant control journal: journal create and delete as real RPCs ([#210](https://github.com/PierreZ/paros/pull/210)) ([#347](https://github.com/PierreZ/paros/pull/347))
- The Zola + Goyo skeleton at web/site/, the book ported, Pages on Zola ([#306](https://github.com/PierreZ/paros/pull/306)) ([#320](https://github.com/PierreZ/paros/pull/320))
- Leader uuid fences the journal; the term never reaches clients ([#241](https://github.com/PierreZ/paros/pull/241)) ([#281](https://github.com/PierreZ/paros/pull/281))
- Meta tenant, fleet operations and no fixed ids ([#229](https://github.com/PierreZ/paros/pull/229)) ([#238](https://github.com/PierreZ/paros/pull/238))
- Fenced Truncate ([#228](https://github.com/PierreZ/paros/pull/228)), the (TenantId, JournalId) frame ([#235](https://github.com/PierreZ/paros/pull/235)), uniform parosd + Compose ([#196](https://github.com/PierreZ/paros/pull/196)) ([#236](https://github.com/PierreZ/paros/pull/236))
- Journal API, no application, many journals per process, sim on JournalStorage (#185, #186, #188, #187) ([#199](https://github.com/PierreZ/paros/pull/199))
- Rounds extracted (#142 rung 0), quorum reads core half ([#143](https://github.com/PierreZ/paros/pull/143)), acceptor grid sim half ([#141](https://github.com/PierreZ/paros/pull/141)) ([#150](https://github.com/PierreZ/paros/pull/150))
- Acceptor grid (core half, #141) and flexible quorums drawn per seed (sim half, #140) ([#149](https://github.com/PierreZ/paros/pull/149))
- Flexible quorums (core half, #140), the chunk-restore seam visited ([#146](https://github.com/PierreZ/paros/pull/146)), the wiped-identity format marker ([#147](https://github.com/PierreZ/paros/pull/147)) ([#148](https://github.com/PierreZ/paros/pull/148))
- Advance moonpool to the single-stream canary and put the campaign under it ([#138](https://github.com/PierreZ/paros/pull/138))
- Instrument every important method; core spans behind a default-on `tracing` feature ([#130](https://github.com/PierreZ/paros/pull/130))
- One workload, one audit, everything knobbed — the paros-sim simplification ([#128](https://github.com/PierreZ/paros/pull/128))

