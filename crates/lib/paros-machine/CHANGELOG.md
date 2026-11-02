# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-02

### 🚀 Features

- Remove role slots; placement orders by busyness, heal is shown (#837 (remove role slots)) ([#842](https://github.com/PierreZ/paros/pull/842))
- The cell tenant's matchmaker set moves, with the cached registry fold as its boot path (#685 (special tenants are tenants) step 4a) ([#847](https://github.com/PierreZ/paros/pull/847))
- **machine**: PAROS_ROLES, default all; a restart may widen a machine's roles ([#839](https://github.com/PierreZ/paros/pull/839))
- The frontend is a role of parosd (#723 (frontend as a parosd role))

### 🐛 Bug Fixes

- Released tenant control journal stops; registration reads the election from itself last (fix-main) ([#838](https://github.com/PierreZ/paros/pull/838))
- **frontend**: Reach every storage machine of the cell ([#700](https://github.com/PierreZ/paros/pull/700)) ([#744](https://github.com/PierreZ/paros/pull/744))
- A machine's clean stop waits for its frontend (#723 (frontend as a parosd role))

### 📚 Documentation

- A map for every crate, and the docs name the new paths (#722 (three products))

### 🚜 Refactor

- **core**: One path per item, a short root (#730 (paros-core rework))
- One crate per layer, grouped under crates/ (#722 (three products))

### 📦 Other

- A machine serves its booked replicas (#765 (booked proxies and replicas)) ([#780](https://github.com/PierreZ/paros/pull/780))
- A universe-level role that redirects to the tenant's cell (#233 (resolver)) ([#776](https://github.com/PierreZ/paros/pull/776))
- A machine serves its booked proxy leaders (#765 (booked proxies and replicas)) ([#775](https://github.com/PierreZ/paros/pull/775))
- Each reference carries its zone; the client prefers its own ([#673](https://github.com/PierreZ/paros/pull/673)) ([#768](https://github.com/PierreZ/paros/pull/768))
- Per-journal proxy leaders, batchers and replicas; counts off the format marker ([#764](https://github.com/PierreZ/paros/pull/764))

