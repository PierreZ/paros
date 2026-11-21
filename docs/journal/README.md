[Index](README.md) · [01-goal.md →](01-goal.md)

# The paros journal, v1 (index)

These files are the agents' map to the journal v1: what the journal is, what it promises, how it
is built and how it is checked. They are the equivalent of `docs/architecture/` for the journal
alone (Pierre, 2026-11-20: "create an equivalent of docs architecture of Parosd but for the
journal as first step"; #1332 (journal architecture docs), step 1 of #1323 (build the paros
journal v1)). Every later step of the build (#1189 and #1324 to #1331) builds against them.

**It is the target, not all of it is code.** The pure journal core exists (`paros-journal` and
`journal-sim`, #1324 (paros-journal)), and so does the matchmaker register (#1325), and so do the
first journal roles (segments, the seal and writes, #1326); the other roles and the driver do not
exist yet. `AGENTS.md` describes what paros
is today. Where these files and `docs/architecture/` disagree on a journal rule, these files win.

**One home for each rule.** A journal rule is written here once, with its decision, its issue
and its TLA+ model. The journal rules that `docs/architecture/` held moved here on 2026-11-20
(§2 to §2.16, §4.2, §5.1, and the journal part of §4.3). Each moved architecture section keeps
its heading and a link to its new home, so "§2.10" in an issue or a PR still finds its rule
through the table at the end of this page. The sections of these files carry a `J` (§J3.1), so
they never collide with the section numbers of `docs/architecture/`. "Architecture §3.4" means
section 3.4 of `docs/architecture/`.

**Marks** (the same as `docs/architecture/`). **Open** marks a point that nobody decided, with
the step that decides it: these files do not guess a value. **Not proved** marks a rule that a
TLA+ model must still check, with the model or the issue that checks it. A rule that is out of
v1 says so.

**Writing.** ASD-STE100 Simplified Technical English, most of the time (`AGENTS.md`). A decision
keeps its date and its issue. A step that changes a rule changes it here, in the same pull
request.

## The v1 scope in one page

- **One ordered log for each journal id**, with Paxos inside each segment, the roles of
  Compartmentalized Paxos and the reconfiguration of Matchmaker Paxos. One consensus leader
  orders the journal (§J1).
- **Four calls**: `Write`, `Read`, `Truncate`, `SetLeader(new, old)`. A single writer is
  exactly-once through `expected_seq`; `SetLeader(None, old)` makes the journal multi-writer and
  at-least-once. No dedup table (§J2).
- **Segments in the matchmakers** (design A): one record for each segment, written by
  compare-and-set at a majority. A segment opens only on a reshape; the first leader of the new
  grid seals the old one; the whole boundary state (`next_seq`, `first_seq`, the writer uuid) is
  in the segment record; retire at `F + 1` holders (§J3).
- **Each journal has its own matchmaker set and its own grid controller.** The matchmaker
  register is an optional role beside the paper's registration matchmaker (§J3.4, §J4.1).
- **Quorum everywhere**: no step waits for every member. v1 grids are the majority and the
  partitioned grid only (`rows = 2F + 1`, `acks = F + 1`) (§J5.1).
- **The v1 controller heals and scrubs**: a reshape or the re-replication of a sealed segment, and
  a repair through the protocol. Else it holds and alerts. No rolls, no offload, no SLOs, no
  scaling from load (§J6).
- **The journal fold stays out of `paros-consensus`**: journal crates only. The core stays a
  library for everyone (§J4.3).
- **Out of v1**: the Phase-1 bypass, the offloader, rolls, wider grids, rolling upgrades, an
  unsafe recover (§J1.2).
- **Checked first**: a TLA+ model for each rule, then #1189 composes them, then `journal-sim` and
  moonpool-sim judge the code (§J7).

## Files, in order

1. [01-goal.md](01-goal.md): §J1 Goal; §J1.1 The v1 scope; §J1.2 Out of v1
2. [02-data-plane.md](02-data-plane.md): §J2 The data plane (the calls, the modes, the answers, reads, truncation, limits, headers)
3. [03-segments-and-records.md](03-segments-and-records.md): §J3 Segments, the boundary state, the seal, retire, trim, and the matchmaker register
4. [04-roles.md](04-roles.md): §J4 Roles, the sans-IO contract (the flight rule), crates and layering
5. [05-quorums-and-failures.md](05-quorums-and-failures.md): §J5 Quorum everywhere, the counts of `F + 1`, CTRL, zones, a column below quorum
6. [06-controller.md](06-controller.md): §J6 The controller: the fenced reshape and journal health
7. [07-verification.md](07-verification.md): §J7 The model of each rule, #1189, `journal-sim`, moonpool-sim, the absolute rule
8. [08-build-steps.md](08-build-steps.md): §J8 The build steps of #1323
9. [10-sources.md](10-sources.md): §J10 Papers and prior art

## Section map

| Section | Title | File | Moved from |
|---|---|---|---|
| §J1 | Goal | [01-goal.md](01-goal.md) | |
| §J1.1 | The v1 scope of the journal | [01-goal.md](01-goal.md) | architecture §2.15 |
| §J1.2 | Out of v1 | [01-goal.md](01-goal.md) | |
| §J2 | The data plane | [02-data-plane.md](02-data-plane.md) | architecture §2 |
| §J2.1 | Positions | [02-data-plane.md](02-data-plane.md) | architecture §2.1 |
| §J2.2 | Single-writer writes and fencing | [02-data-plane.md](02-data-plane.md) | architecture §2.2 |
| §J2.3 | Leadership | [02-data-plane.md](02-data-plane.md) | architecture §2.3 |
| §J2.4 | Multi-writer journals | [02-data-plane.md](02-data-plane.md) | architecture §2.4 |
| §J2.4.1 | The switch between modes | [02-data-plane.md](02-data-plane.md) | architecture §2.4.1 |
| §J2.5 | Reads | [02-data-plane.md](02-data-plane.md) | architecture §2.5 |
| §J2.6 | Truncation | [02-data-plane.md](02-data-plane.md) | architecture §2.6 |
| §J2.7 | Limits | [02-data-plane.md](02-data-plane.md) | architecture §2.7 |
| §J2.8 | Record headers | [02-data-plane.md](02-data-plane.md) | architecture §2.14 |
| §J2.9 | Underneath | [02-data-plane.md](02-data-plane.md) | architecture §2.8 |
| §J2.10 | Store durability | [02-data-plane.md](02-data-plane.md) | architecture §2.9 |
| §J3 | Segments and records | [03-segments-and-records.md](03-segments-and-records.md) | |
| §J3.1 | Segments | [03-segments-and-records.md](03-segments-and-records.md) | architecture §2.10 |
| §J3.2 | A user truncate across segments | [03-segments-and-records.md](03-segments-and-records.md) | architecture §2.10 |
| §J3.3 | A leaderless read of the tail across a reshape | [03-segments-and-records.md](03-segments-and-records.md) | architecture §2.10 |
| §J3.4 | The journal's matchmaker register | [03-segments-and-records.md](03-segments-and-records.md) | architecture §2.16 |
| §J4 | Roles, contract and crates | [04-roles.md](04-roles.md) | |
| §J4.1 | The roles | [04-roles.md](04-roles.md) | |
| §J4.2 | The journal's sans-IO contract | [04-roles.md](04-roles.md) | architecture §4.2 |
| §J4.3 | Crates and layering | [04-roles.md](04-roles.md) | architecture §4.3 (the journal part) |
| §J5 | Quorums and failures | [05-quorums-and-failures.md](05-quorums-and-failures.md) | |
| §J5.1 | Quorum everywhere | [05-quorums-and-failures.md](05-quorums-and-failures.md) | architecture §2.11 |
| §J5.2 | The counts of `F + 1` | [05-quorums-and-failures.md](05-quorums-and-failures.md) | |
| §J5.3 | Damage: CTRL in the journal | [05-quorums-and-failures.md](05-quorums-and-failures.md) | |
| §J5.4 | Zones | [05-quorums-and-failures.md](05-quorums-and-failures.md) | |
| §J5.5 | A column below quorum | [05-quorums-and-failures.md](05-quorums-and-failures.md) | architecture §2.13 |
| §J6 | The controller | [06-controller.md](06-controller.md) | |
| §J6.1 | The grid controller's fenced reshape | [06-controller.md](06-controller.md) | architecture §2.12 |
| §J6.2 | Journal health | [06-controller.md](06-controller.md) | architecture §5.1 |
| §J7 | Verification | [07-verification.md](07-verification.md) | |
| §J8 | The build steps | [08-build-steps.md](08-build-steps.md) | |
| §J10 | Sources | [10-sources.md](10-sources.md) | |
