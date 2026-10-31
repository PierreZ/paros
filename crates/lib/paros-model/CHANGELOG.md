# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-31

### 🚀 Features

- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- Say `survive machines` instead of `failures-per-partition` (#748 (survive)) ([#759](https://github.com/PierreZ/paros/pull/759))
- **core**: One path per item, a short root (#730 (paros-core rework))
- **model**: Split registry.rs by concern (#722 (three products))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- Sim, parosctl: a second cell mid-run, the operator lane, cross-cell commands (#490 (second cell mid-run), #489 (operator workload)) ([#786](https://github.com/PierreZ/paros/pull/786))
- Place tenants by override, then by bucket (#713 (bucket placement), #672 (cell tag selector)) ([#787](https://github.com/PierreZ/paros/pull/787))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- Each reference carries its zone; the client prefers its own ([#673](https://github.com/PierreZ/paros/pull/673)) ([#768](https://github.com/PierreZ/paros/pull/768))
- Judge journal healing from ground truth, not from the registry ([#739](https://github.com/PierreZ/paros/pull/739)) ([#766](https://github.com/PierreZ/paros/pull/766))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

