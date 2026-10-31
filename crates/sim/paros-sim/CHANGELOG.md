# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-31

### 🚀 Features

- **sim**: Count every oracle excuse and silent skip (#738 (count excuses, the baseline)) ([#763](https://github.com/PierreZ/paros/pull/763))
- **sim**: Moonpool owns fault placement, part 1 (#760 (moonpool owns fault placement)) ([#762](https://github.com/PierreZ/paros/pull/762))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- Three sim reds (faulty copies rewritten, re-answered registrations, slow read quorum floor) ([#789](https://github.com/PierreZ/paros/pull/789))
- An adopted book adds the founders as seeds; the leads oracle takes the loss excuses ([#781](https://github.com/PierreZ/paros/pull/781))
- Shard replicas by host, stores publish their layouts, sets wait on the founders ([#772](https://github.com/PierreZ/paros/pull/772))
- Learn a moved leader's address, and wait out a coordinator handover in the tail ([#761](https://github.com/PierreZ/paros/pull/761))
- **frontend**: Reach every storage machine of the cell ([#700](https://github.com/PierreZ/paros/pull/700)) ([#744](https://github.com/PierreZ/paros/pull/744))
- Slow-machine strikes in the chaos window only; a writer never moves back; a campaign adopts a reconfiguration its leadership ran ([#737](https://github.com/PierreZ/paros/pull/737))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- **core**: One path per item, a short root (#730 (paros-core rework))
- **core**: Journal/ holds ids, command and state (#730 (paros-core rework))
- **sim**: Split machine.rs by concern (#722 (three products))
- **sim**: Split the audit by reporting role (#722 (three products))
- **sim**: Split shape.rs into shape/ (#722 (three products))
- **sim**: Workload/, world/scenarios/, paper names in process/ (#722 (three products))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- Harness rot and wipes go through moonpool's shard gate; the rot and cut budgets go (#760 (moonpool owns fault placement)) ([#774](https://github.com/PierreZ/paros/pull/774))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- Client, sim: merge a pipeline's fenced writes, fire the batcher's rare paths ([#770](https://github.com/PierreZ/paros/pull/770))
- End every maintenance window before the heal check ([#741](https://github.com/PierreZ/paros/pull/741)) ([#769](https://github.com/PierreZ/paros/pull/769))
- Each reference carries its zone; the client prefers its own ([#673](https://github.com/PierreZ/paros/pull/673)) ([#768](https://github.com/PierreZ/paros/pull/768))
- Judge journal healing from ground truth, not from the registry ([#739](https://github.com/PierreZ/paros/pull/739)) ([#766](https://github.com/PierreZ/paros/pull/766))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

