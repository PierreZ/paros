# paros: the journal service (index)

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; these files describe what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction. Decided on 2026-09-30; the universe, the control hierarchy, identifiers, checkpoints,
recovery and the fenced `Truncate` decided on 2026-10-02; the leader-uuid API with its two
journal modes, election over a journal, the request channel, liveness, names, trust, capacity as
role slots, journals born with their matchmaker set, tenant modes, the data-plane limits, storage
chaos in simulation and the deferral of recovery decided on 2026-10-04; the resolver and the
frontend, zones inside a cell and multi-region cells decided on 2026-10-07; bootstrap by admin
calls, `cell init` as a single-decree Paxos, the election journal on the founding members and the
tenant control journal's fields decided on 2026-10-09. The milestones at the end carry the issue
numbers.

This folder holds one file per chapter; section 3 has one file per subsection. Read the files in
the order below. The section numbers did not change when the single `docs/architecture.md` was
split (#675), so a reference such as "§3.7" anywhere in the repository, in an issue or in a PR
stays valid: find its file in the section map.

## Files, in order

1. [01-goal.md](01-goal.md): §1 Goal
2. [02-data-plane.md](02-data-plane.md): §2 The data plane
3. [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md): §3 The control plane; §3.1 The cell tenant and bootstrap
4. [03-2-machines.md](03-2-machines.md): §3.2 Machines
5. [03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md): §3.3 Coordinators and placement
6. [03-4-tenant-modes.md](03-4-tenant-modes.md): §3.4 Tenant modes
7. [03-5-frontend-and-resolver.md](03-5-frontend-and-resolver.md): §3.5 The frontend and the resolver
8. [03-6-status.md](03-6-status.md): §3.6 Status
9. [03-7-universe.md](03-7-universe.md): §3.7 The universe
10. [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md): §3.8 Identifiers; §3.9 Checkpoints; §3.10 Recovery (deferred); §3.11 Transport security
11. [04-roles.md](04-roles.md): §4 Roles
12. [05-failure-model-and-zones.md](05-failure-model-and-zones.md): §5 Failure model and zones
13. [06-verification.md](06-verification.md): §6 Verification
14. [07-changes-milestones-done.md](07-changes-milestones-done.md): §7 What changes against today; §8 Milestones; §9 The toy, done means
15. [10-sources.md](10-sources.md): §10 Sources
16. [11-alternatives.md](11-alternatives.md): §11 Alternatives considered

## Section map

| Section | Title | File |
|---|---|---|
| §1 | Goal | [01-goal.md](01-goal.md) |
| §2 | The data plane | [02-data-plane.md](02-data-plane.md) |
| §2.1 | Positions | [02-data-plane.md](02-data-plane.md) |
| §2.2 | Single-writer writes and fencing | [02-data-plane.md](02-data-plane.md) |
| §2.3 | Leadership | [02-data-plane.md](02-data-plane.md) |
| §2.4 | Multi-writer journals | [02-data-plane.md](02-data-plane.md) |
| §2.5 | Reads | [02-data-plane.md](02-data-plane.md) |
| §2.6 | Truncation | [02-data-plane.md](02-data-plane.md) |
| §2.7 | Limits | [02-data-plane.md](02-data-plane.md) |
| §2.8 | Underneath | [02-data-plane.md](02-data-plane.md) |
| §2.9 | Store durability | [02-data-plane.md](02-data-plane.md) |
| §3 | The control plane | [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) |
| §3.1 | The cell tenant and bootstrap | [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) |
| §3.2 | Machines | [03-2-machines.md](03-2-machines.md) |
| §3.3 | Coordinators and placement | [03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md) |
| §3.4 | Tenant modes | [03-4-tenant-modes.md](03-4-tenant-modes.md) |
| §3.5 | The frontend and the resolver | [03-5-frontend-and-resolver.md](03-5-frontend-and-resolver.md) |
| §3.6 | Status | [03-6-status.md](03-6-status.md) |
| §3.7 | The universe | [03-7-universe.md](03-7-universe.md) |
| §3.8 | Identifiers | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.9 | Checkpoints | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.10 | Recovery (deferred) | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.11 | Transport security | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §4 | Roles | [04-roles.md](04-roles.md) |
| §4.1 | How the pieces fit | [04-roles.md](04-roles.md) |
| §5 | Failure model and zones | [05-failure-model-and-zones.md](05-failure-model-and-zones.md) |
| §6 | Verification | [06-verification.md](06-verification.md) |
| §7 | What changes against today | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §8 | Milestones | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §9 | The toy, done means | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §10 | Sources | [10-sources.md](10-sources.md) |
| §11 | Alternatives considered | [11-alternatives.md](11-alternatives.md) |
