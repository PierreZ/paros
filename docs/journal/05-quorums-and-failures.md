[← 04-roles.md](04-roles.md) · [Index](README.md) · [06-controller.md →](06-controller.md)

## J5. Quorums and failures

**The promise of v1.** With `F` acceptors down in every column of the old and the new grid, `F`
matchmakers down in the journal's set and `F` replicas down in a segment's replica set, every
step of the journal completes (§J5.1). More failures than that are outside the promise: the
journal holds and alerts, and it never takes an unsafe step to go on (§J5.5, §J6.2). The
general failure model of the service (the faults, the fsync contract, zones inside a cell) is
architecture §5.

### J5.1 Quorum everywhere

*Moved here from `docs/architecture/02-data-plane.md` §2.11 (#1332 (journal architecture docs)).*

**Every step of the journal waits for a quorum only** (decided by Pierre on 2026-11-20, #1257
(quorum everywhere), checked by `specs/tla/quorum-everywhere/`). No step waits for every member
of a group, as a FoundationDB commit waits for every TLog. With `F` acceptors down in every column
of the old and the new grid, `F` matchmakers down in each matchmaker set and `F` replicas down in
a segment's replica set, each step completes:

| Step | It waits for |
|---|---|
| Write, and the decided `Truncate` | `F + 1` acceptors of one column; a stuck round moves to the next column (#533) |
| Election | a matchmaker majority, then `F + 1` acceptors of every column of each configuration of `H_b` |
| Leaderless read | `F + 1` acceptors of every column of the configuration in force; no matchmaker read: the fence of the code closes a reshape (#1254, decided in §J3.1) |
| Seal | a Phase-1 quorum of the old grid, then a matchmaker majority for the compare-and-set of the end slot |
| Boundary write | one replica of the old set that applied the segment, then a matchmaker majority. **Not proved**: no row yet (#1318) |
| Acceptor compaction | `F + 1` replicas of the segment's set report their compaction point. **Not proved**: no row yet (#1318) |
| Retire | `F + 1` replicas of the segment, then a matchmaker majority |
| Offload mark, record of a trim | a matchmaker majority. In v1 the offload mark is a rule of the record only: nothing writes it (§J3.1) |
| Matchmaker set handover | a majority of the old set (the stop quorum), then a majority of the new set (the successor decree) |
| Grid controller election, reshape call | a matchmaker majority for each compare-and-set on the matchmaker record (§J6.1, #1259) |
| Heal of a sealed segment's replica set | one member of the set that holds the segment whole (the copy), then a matchmaker majority for the compare-and-set of the new set (§J3.1, #1314); `heal_replica` passes even with `F + 1` members of the set down while one holder lives |
| Hold before Phase 2 of a bypass | not in v1 (§J5.5). `F + 1` acceptors of one column of the old grid; a stuck column moves (§J5.5, #1258) |

Each step blocks with one member more down than its quorum allows, and each step that waits for
every target blocks with `F` down: the model sees both. On a grid that is not full the leaderless
read sends its `PreRead` to every member, but it completes on a Phase-1 quorum of the answers
(`QuorumRead`, `has_phase1_quorum_in`): it sends to all and waits for a quorum, so it keeps its
targets.

A grid layout meets the rule for `F` exactly when `F + 1 <= acks <= rows - F`
(`grid/GridCheck.tla`, `PerColumnTolerance`). The partitioned grid of architecture §3.4
(`rows = 2F + 1`, `acks = F + 1`) and the majority meet it. The full grid (`acks = rows`) does not:
a write needs a whole column. The journal therefore offers only the majority and the partitioned
grid. **In v1 the register takes exactly these grids**: `rows = 2F + 1`, `acks = F + 1`, with
one or more columns, the majority being one column (decided by Pierre on 2026-11-20, #1315,
decision 5). The wider bound `F + 1 <= acks <= rows - F` is what the models check, and a v1 grid
is inside it. A grid of four rows, the state WritesBlocked and the separate phase-1 and phase-2
spares of §J6.2 leave v1.

### J5.2 The counts of `F + 1`

Three steps count `F + 1` members of a replica set, so that with `F` of them down one good copy
is still live. Each rule is in the section that the table names.

| Step | The count | Section | Model |
|---|---|---|---|
| Retire of a sealed segment | `F + 1` replicas of its set hold the whole segment, and the boundary state is written. A found damage that is not repaired also blocks it, as a margin (Pierre, 2026-11-20, #1263) | §J3.1, §J3.4 | `segment-replicas/` (#1271) |
| Compaction of a segment's acceptors | to a slot that `F + 1` replicas of its set compacted to, counted on the leader side through `membership/` | §J3.2 | `segment-truncate/` (#1276); the quorum row is **Not proved** (#1318) |
| Re-replication of a sealed segment | a member that holds the whole segment copies it to a new replica, which counts only on its own complete copy; with `D` members down, at least `F + 1 - D` live members hold the segment (v1, Pierre, 2026-11-20) | §J3.1, §J3.4 | `re-replication/` (#1314 (re-replication of a sealed segment's replica set)) |

### J5.3 Damage: CTRL in the journal

A disk can return a record that fails its checksum. The core handles this on plain Multi-Paxos
with the faulty entries of CTRL and the tri-state tally (`specs/tla/truncation/`, #1216
(truncation model, part 2)). #1263 (CTRL recovery in the journal) checks the same rules on a
grid and across segments, in two models that PR #1293 adds (`specs/tla/ctrl-grid/`,
`specs/tla/ctrl-segments/`). **That PR is open.** Its rules move into this section when it
merges, and it does not add a section to `docs/architecture/`. The PR reports these results:

- The tally needs a Phase-1 quorum of qualifying answers of every configuration of `H_b`, through
  `membership/`, and blocks on a slot that it cannot prove empty. A loss report is always true.
- A scrub and a boot give the same faulty record, with its ballot kept. A repair writes a chosen
  value under that ballot, in one write.
- A sealed segment retires at `F + 1` holders; the guard "no retire while a found damage is not
  repaired" is not needed for safety, and Pierre kept it as a margin on 2026-11-20 (§J5.2).
- v1 has no unsafe recover (§J6.2).

**The seal end counts a faulty vote** (#1313 (the seal end against a CTRL faulty record),
`specs/tla/seal-ctrl/`): the end is the frontier of the sealer's Phase 1 over the old grid, and the
seal does not wait for the repair probe (§J3.1).

### J5.4 Zones

The zone rule of the service (`AcceptorConfig::zone_rule`) is architecture §5. For a journal, a zone loss on three zones takes one acceptor from every
column at once, and no placement over two zones survives the loss of one of them. The v1
controller then holds and alerts once for the zone (§J6.2, "A zone loss"; `specs/tla/health/`).
With `F = 1` over three zones the layout is row = zone (§J6.2).

### J5.5 A column below quorum

*Moved here from `docs/architecture/02-data-plane.md` §2.13 (#1332 (journal architecture docs)).*

**Not in v1** (Pierre, 2026-11-20, from the first review of the journal plan): a column below
quorum is `F + 1` losses in one column, which is outside the promise of §J5.1. In v1 the
journal holds and alerts. The Phase-1 bypass (#579) is not built in v1, so the hold before Phase 2
below and the read rule of #1270 (bypass read order) do not apply to v1. They stay here as the
rules for the day #579 comes back.

**A column is below quorum** when fewer than `F + 1` of its acceptors are live (the grid of
§J5.1: `rows = 2F + 1`, `acks = F + 1`). Writes go on in the other columns (#533 (move a
stuck round)), but no Phase-1 quorum of the grid exists, so no new leader can be elected and no
reshape can run a full Phase 1 over the old grid. #1258 (reshape with a column below quorum)
checked with `specs/tla/degraded-reshape/` whether the live leader can reshape away with the
Phase-1 bypass of #579 (zero-stall reconfiguration, `specs/tla/zero-stall/`):

- **With the decision hold of #579, no.** The bypass moves the leader to the new grid with no
  Phase 1, and its writes are chosen there. But the hold acks no slot before a Phase-1 quorum of
  the old grid promised the new ballot, and GC waits for the same. That quorum does not exist, so
  no slot is acked and the old grid never retires.
- **The decision hold also does not order two reads**: #1270 (a read at the bypass ballot), and a
  read on the old basis that saw the old vote of one acceptor for a slot that the new grid chose,
  followed by a read on the old basis at a quorum without that acceptor. Gating the read basis
  closes the first, not the second.
- **The rule: a hold before Phase 2 on a blocking set** (decided by Pierre on 2026-11-20, #1258).
  The bypassing leader sends no `Accept` at its new ballot until the acceptors that promised it
  meet every read quorum of each prior configuration. On this grid that is `F + 1` acceptors of
  one old column, which a column below quorum leaves. Its acks and its GC wait for the same
  promises, not for a Phase-1 quorum of the old grid. The fence of the code (§J3.1, the
  promise in each `PreReadAck`) then orders every read, with no matchmaker read on the read path,
  which matches the choice of §J3.1. The bypass waits one round trip to one old column, not
  for a full Phase 1. Checked by `block_propose`, `block_propose_maj` and `live_block_propose`;
  with one promise less, TLC finds a stale read (`block_propose_short`). The segment-record check
  on each read, with no hold, also passes (`safe_record`, `live_record`) and is the rejected
  alternative.
- **The rule needs a live leader.** If the leader is lost while a column is below quorum, no
  election can run, and writes stop until a member of that column comes back: the grid
  controller holds and alerts (§J6.1).
- **The seal end of a bypass is the bypassing leader's allocator frontier**: one above the highest
  slot that it proposed at its old ballot. A bypass runs no Phase 1, so the end of §J3.1
  (one above the highest vote its Phase 1 saw) is replaced by the frontier, which is never lower,
  because a leader bypasses only after it proposed every slot that its own election recovered. An
  end at the chosen prefix chooses two values.
- **A heal that moves survivors to other columns is safe**: the layout is row-major over the
  sorted members (`grid/Grid.tla`), and a move between two partitioned grids where every survivor
  changes column keeps one value (`config-register/heal_partitioned`); with Phase 1 one acceptor
  short in every column, TLC finds two values (`heal_partitioned_short`).
