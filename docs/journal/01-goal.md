[← README.md](README.md) · [Index](README.md) · [02-data-plane.md →](02-data-plane.md)

## J1. Goal

**The journal is one ordered log for each journal id.** A writer appends batches of opaque
records, and readers read them in order. Paxos orders the data inside every segment, not only
the metadata (decision 1.A of the journal design, Pierre, 2026-11-19): the Delos and BookKeeper
shape, with quorum segments and consensus on the metadata only, lost. The journal uses the roles
of **Compartmentalized Paxos** and changes its acceptor grid with **Matchmaker Paxos**. Both are
non-negotiable, and so is reconfiguration (Pierre, 2026-11-19).

**Four calls make the data plane**: `Write`, `Read`, `Truncate` and `SetLeader(new, old)`
(section J2). A journal is in one of two modes: single-writer, where one writer holds a leader
uuid and gets exactly-once writes through `expected_seq`, and multi-writer, where any writer
appends at-least-once (§J2.4). The client is a pure state machine with no network (Pierre,
2026-11-19); the process that hosts it does the network.

**One consensus leader orders the whole journal.** Its throughput is the ceiling of v1 (Pierre,
2026-11-19). The grid controller is the scaling story of later releases; v1 uses it to heal
only (§J1.1). Key ranges (several journals under one topic) and Mencius (several leaders that
share slots) are later options, not v1.

**The journal knows ids only.** Each journal has its own matchmaker set (Pierre, 2026-11-19),
its own grid controller (section J6) and its own segment table (section J3). Names, tenants,
cells and bootstrap belong to the stack above (#1264 (remove the cell from paros-consensus)).
The journal takes its first configuration as an input: the first segment, its grid, its replica
set and the matchmaker set (Pierre, in the journal design). There is no `cell init` in the journal.

**The journal is a library first.** `parosd` will use it later, with no private API: one process
hosts many journals, by id (Pierre, 2026-11-19; §J4.2, rule 5). The default deployment is three
machines, with every role in one process on each (Pierre, in the journal design).

**Where the journal sits.** `docs/architecture/` describes the whole service around the journal:
cells, tenants, machines, coordinators, the frontend and the resolver. These files describe the
journal alone, and they are the home of every journal rule. Where an architecture section held a
journal rule, the rule moved here and the architecture section keeps its heading and a link, so
a reference such as "§2.10" in an issue still finds its rule (see the README of this folder).

**What paros builds on.** The journal composes the roles of `paros-consensus` and adds optional
behaviour where composition is not enough. The core stays a library for everyone (architecture
§4.3, §J4.3): no journal concept enters `paros-consensus`.

### J1.1 The v1 scope of the journal

*Moved here from `docs/architecture/02-data-plane.md` §2.15 (#1332 (journal architecture docs)).*

Pierre's answers to the first review of the journal plan (2026-11-20,
`/mnt/project-files/review/journal-plan-2026-11-20/fable-review-1.md`):

- **A column below quorum is not in the v1 promise**: the journal holds and alerts (§J5.5).
  The zero-stall bypass (#579) is out of v1.
- **The elected controller stays**: its own role, elected by a term in the matchmaker record
  (§J6.1).
- **Offload is prepared, not built** (§J3.1). This replaces "offload retired segments" in
  the controller's first scope: the controller heals (re-replication included) and scrubs.
- **The v1 controller has no SLOs and no scaling from latency or load.** It acts only on failures,
  from protocol facts: heal and scrub. The roles publish their metrics to the controller;
  which metric drives which action is decided after tests (#1260 (health measures)).
- **The boundary uuid lives in the segment record**, the result of #1303 (two seals in a row,
  §J3.1).
- **A write stall at each reshape is accepted in v1, and measured.**
- **The journal gets its own walk crate**: a `journal-sim` beside `consensus-sim`. `consensus-sim`
  stays on `paros-consensus` only.

Pierre's answers to the second review of the journal plan (2026-11-20, #1315 (bring docs and
models in step with the v1 scope),
`/mnt/project-files/review/journal-plan-2026-11-20/fable-review-2.md`):

1. **The journal's compare-and-set register at the matchmakers is an optional role.** The
   registration matchmaker of the paper stays in `paros-consensus` for the users of the library
   (§J3.4, architecture §4.3).
2. **The journal fold does not go into the core.** `Write`, `Truncate` and `SetLeader`, with
   `None` and `Below`, live only in the journal crates. The core's fold stays as it is today
   (§J2, architecture §4.3).
3. **Matchmakers below a majority during a seal stop acked writes: accepted for v1.** It is more
   than `F` matchmaker failures (§J3.4, §J6.2).
4. **The re-replication of a sealed segment is in v1**, with its model first: #1314
   (re-replication of a sealed segment's replica set) (§J3.1).
5. **The v1 grids are the majority and the partitioned grid only** (`rows = 2F + 1`,
   `acks = F + 1`). WritesBlocked and the separate phase-1 and phase-2 spares leave v1 (sections
   2.11 and 5.1).
6. **No size or age roll in v1.** A segment opens only on a reshape. Rolls come back with offload
   (§J3.1). So in v1 the stall above and the window of decision 3 come only with a heal.

### J1.2 Out of v1

These parts are designed, or named, and stay out of v1. Each one keeps its rules in these files
for the day it comes back. The list follows the consolidated v1 plan of 2026-11-20
(`/mnt/project-files/review/journal-plan-2026-11-20/journal-v1-plan.md`, section 9) and the
decisions of §J1.1.

| Out of v1 | Where its rules are | Decided |
|---|---|---|
| The Phase-1 bypass (#579 (zero-stall reconfiguration)), its hold before Phase 2 (#1258) and its read rule (#1270 (bypass read order)). A column below quorum is outside the promise | §J5.5 | Pierre, 2026-11-20 (first review) |
| The offloader role, the object store code and the scrub of the objects. The offloaded state, its read path and its record rules stay in v1 | §J3.1, `specs/tla/offload/` (#1256) | Pierre, 2026-11-20 |
| Size and age rolls: a segment opens only on a reshape | §J1.1, §J3.1 | Pierre, 2026-11-20 (#1315, decision 6) |
| Service level objectives, scaling from latency or load, and the compartment counts by load | §J6.2 | Pierre, 2026-11-20 |
| Grids other than the majority and the partitioned grid (`rows = 2F + 1`, `acks = F + 1`); the state WritesBlocked; the separate phase-1 and phase-2 spares | §J5.1, §J6.2 | Pierre, 2026-11-20 (#1315, decision 5) |
| Rolling upgrades and several releases in one binary. v1 keeps a release number in every header, a release field in the segment record and a reserved `Upgrade` command, and a role refuses a release it does not know | §J3.4 (the release field) | Pierre, 2026-11-19 (journal design) |
| Retention policies and cursors. `Truncate` is the only way to delete data | this row; `Truncate` is §J2.6 | Pierre, 2026-11-19 and 2026-11-20 |
| A dedup table: a single writer is exactly-once through `expected_seq`, multi-writer is at-least-once | §J2.4.1 | Pierre, 2026-11-20 (#1274) |
| Key ranges and a second leader (Mencius) | §J1 | Pierre, 2026-11-19 |
| Lazy acks (the plan names them and gives no definition). A v1 ack goes out after the write is chosen and applied in prefix order | §J2.2 | the consolidated plan, section 9 |
| An unsafe recover of a column lost for good | §J6.2 | Pierre, 2026-11-20 |
