# paros: the journal service (index)

This is the end goal. AGENTS.md describes what paros is today and the doctrine every change
follows; these files describe what paros is becoming, so that every issue, plan and session
aims at the same target. Where the two disagree, AGENTS.md is the present and this is the
direction.

**The desired state of the rework** (decided by Pierre on 2026-11-16, decision 28; written on
2026-11-17 as step 0.0 of the rework). The rework plan of 2026-11-15
(`/mnt/project-files/plans/paros-rework-plan-2026-11-15.md` of the project, revision 321, with
Pierre's decisions 1 to 35) replaces stacked fixes with **eight root rules**. These files state
the rules and the decisions in the present tense, each in the chapter where it belongs. Section
1.1 lists the rules. Section 7 gives the phases and the steps that take paros from the code at
commit `25d81d5` to this state, and section 11 keeps the choices that lost. A decision number
in these files ("decision 15") is a row of the table of Pierre's decisions in the plan.

**Two marks.** A rule that a TLA+ model must still prove carries the mark **Not proved**, with
the model and the gate that proves it (the proof rule, section 1.1). The mark goes when that
gate is green. A rule that the plan leaves open carries the mark **Open**, with the step that
decides it: the docs do not guess a value that nobody decided.

Older decisions keep their dates and issues: the universe, the control hierarchy, identifiers,
checkpoints, recovery and the fenced `Truncate` (2026-10-02); the leader-uuid API with its two
journal modes, the request channel, liveness, names, trust, journals born with their matchmaker
set, the data-plane limits, storage chaos in simulation and the deferral of recovery
(2026-10-04); the resolver, zones inside a cell and multi-region cells (2026-10-07); bootstrap
by admin calls and `cell init` as a single-decree Paxos (2026-10-09); the journal's
configuration redesign (2026-10-28). Where an older decision and a decision of the rework
disagree, the rework wins, and these files say so where it matters.

**The journal is to be designed again** (Pierre, 2026-11-24, D6 of
[`docs/archive/journal-design-a/prompt.md`](../archive/journal-design-a/prompt.md)). The journal v1
of design A had its own map, `docs/journal/`, which held the journal rules of §2, §2.16, §4.2,
§5.1 and the journal part of §4.3 (#1332 (journal architecture docs), 2026-11-20). It was removed
with the journal crates; it is at the git tag `journal-design-a`. Each of those sections keeps its
heading here, says "to be designed again" and names where design A had it. The journal comes back
on the consensus core once the core has segments.
These files keep the service around the journal: cells, tenants, machines, coordinators,
placement, the frontend, the resolver and the universe.

This folder holds one file per chapter; section 3 has one file per subsection. Read the files in
the order below. The section numbers did not change when the single `docs/architecture.md` was
split (#675), so a reference such as "§3.7" anywhere in the repository, in an issue or in a PR
stays valid: find its file in the section map.

## Files, in order

1. [01-goal.md](01-goal.md): §1 Goal; §1.1 The goals and the root rules
2. [02-data-plane.md](02-data-plane.md): §2 The data plane (to be designed again)
3. [02-1-journal-register.md](02-1-journal-register.md): §2.16 The journal's matchmaker register (to be designed again)
4. [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md): §3 The control plane; §3.1 The cell tenant and bootstrap
5. [03-2-machines.md](03-2-machines.md): §3.2 Machines
6. [03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md): §3.3 Control planes, recovery and placement
7. [03-4-tenant-modes.md](03-4-tenant-modes.md): §3.4 Journal configuration
8. [03-5-frontend-and-resolver.md](03-5-frontend-and-resolver.md): §3.5 The frontend and the resolver
9. [03-6-status.md](03-6-status.md): §3.6 Status
10. [03-7-universe.md](03-7-universe.md): §3.7 The universe
11. [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md): §3.8 Identifiers; §3.9 Checkpoints; §3.10 Recovery (deferred); §3.11 Transport security
12. [04-roles.md](04-roles.md): §4 Roles; §4.2 The journal's sans-IO contract (a link); §4.3 The consensus core is a library for everyone; §4.4 The layers of the service
13. [04-5-old-duties.md](04-5-old-duties.md): §4.5 The duty table of the old coordinator and machine
14. [04-6-parity.md](04-6-parity.md): §4.6 The features of the tag and their home
15. [05-failure-model-and-zones.md](05-failure-model-and-zones.md): §5 Failure model and zones; §5.1 Journal health (a link)
16. [06-verification.md](06-verification.md): §6 Verification
17. [07-changes-milestones-done.md](07-changes-milestones-done.md): §7 The rework: phases and steps; §8 Milestones; §9 The toy, done means
18. [10-sources.md](10-sources.md): §10 Sources
19. [11-alternatives.md](11-alternatives.md): §11 Alternatives considered

## Section map

| Section | Title | File |
|---|---|---|
| §1 | Goal | [01-goal.md](01-goal.md) |
| §1.1 | The goals and the root rules | [01-goal.md](01-goal.md) |
| §2 | The data plane (moved to journal §J2) | [02-data-plane.md](02-data-plane.md) |
| §2.1 | Positions (moved to journal §J2.1) | [02-data-plane.md](02-data-plane.md) |
| §2.2 | Single-writer writes and fencing (moved to journal §J2.2) | [02-data-plane.md](02-data-plane.md) |
| §2.3 | Leadership (moved to journal §J2.3) | [02-data-plane.md](02-data-plane.md) |
| §2.4 | Multi-writer journals (moved to journal §J2.4) | [02-data-plane.md](02-data-plane.md) |
| §2.5 | Reads (moved to journal §J2.5) | [02-data-plane.md](02-data-plane.md) |
| §2.6 | Truncation (moved to journal §J2.6) | [02-data-plane.md](02-data-plane.md) |
| §2.7 | Limits (moved to journal §J2.7) | [02-data-plane.md](02-data-plane.md) |
| §2.8 | Underneath (moved to journal §J2.9) | [02-data-plane.md](02-data-plane.md) |
| §2.9 | Store durability (moved to journal §J2.10) | [02-data-plane.md](02-data-plane.md) |
| §2.10 | Segments (moved to journal §J3.1 to §J3.3) | [02-data-plane.md](02-data-plane.md) |
| §2.11 | Quorum everywhere (moved to journal §J5.1) | [02-data-plane.md](02-data-plane.md) |
| §2.12 | The grid controller's fenced reshape (moved to journal §J6.1) | [02-data-plane.md](02-data-plane.md) |
| §2.13 | A column below quorum (moved to journal §J5.5) | [02-data-plane.md](02-data-plane.md) |
| §2.14 | Record headers (moved to journal §J2.8) | [02-data-plane.md](02-data-plane.md) |
| §2.15 | The v1 scope of the journal (moved to journal §J1.1) | [02-data-plane.md](02-data-plane.md) |
| §2.16 | The journal's matchmaker register (moved to journal §J3.4) | [02-1-journal-register.md](02-1-journal-register.md) |
| §3 | The control plane | [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) |
| §3.1 | The cell tenant and bootstrap | [03-1-cell-tenant-and-bootstrap.md](03-1-cell-tenant-and-bootstrap.md) |
| §3.2 | Machines | [03-2-machines.md](03-2-machines.md) |
| §3.3 | Control planes, recovery and placement | [03-3-coordinators-and-placement.md](03-3-coordinators-and-placement.md) |
| §3.4 | Journal configuration | [03-4-tenant-modes.md](03-4-tenant-modes.md) |
| §3.5 | The frontend and the resolver | [03-5-frontend-and-resolver.md](03-5-frontend-and-resolver.md) |
| §3.6 | Status | [03-6-status.md](03-6-status.md) |
| §3.7 | The universe | [03-7-universe.md](03-7-universe.md) |
| §3.8 | Identifiers | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.9 | Checkpoints | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.10 | Recovery (deferred) | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §3.11 | Transport security | [03-8-identifiers-checkpoints-recovery-security.md](03-8-identifiers-checkpoints-recovery-security.md) |
| §4 | Roles | [04-roles.md](04-roles.md) |
| §4.1 | How the pieces fit | [04-roles.md](04-roles.md) |
| §4.2 | The journal's sans-IO contract (moved to journal §J4.2) | [04-roles.md](04-roles.md) |
| §4.3 | The consensus core is a library for everyone | [04-roles.md](04-roles.md) |
| §4.4 | The layers of the service | [04-roles.md](04-roles.md) |
| §4.5 | The duty table of the old coordinator and machine | [04-5-old-duties.md](04-5-old-duties.md) |
| §4.6 | The features of the tag and their home | [04-6-parity.md](04-6-parity.md) |
| §5 | Failure model and zones | [05-failure-model-and-zones.md](05-failure-model-and-zones.md) |
| §5.1 | Journal health (moved to journal §J6.2) | [05-failure-model-and-zones.md](05-failure-model-and-zones.md) |
| §6 | Verification | [06-verification.md](06-verification.md) |
| §7 | The rework: phases and steps | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §8 | Milestones | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §9 | The toy, done means | [07-changes-milestones-done.md](07-changes-milestones-done.md) |
| §10 | Sources | [10-sources.md](10-sources.md) |
| §11 | Alternatives considered | [11-alternatives.md](11-alternatives.md) |
