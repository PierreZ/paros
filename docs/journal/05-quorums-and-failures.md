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
| Boundary write | one replica of the old set that applied the segment, then a matchmaker majority (#1318); `boundary_replica` passes even with `F + 1` members of the set down while one lives |
| Acceptor compaction | `F + 1` replicas of the segment's set report their compaction point to the leader side; the floor that it sends the acceptors waits for no answer (§J3.2, #1318) |
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
| Compaction of a segment's acceptors | to a slot that `F + 1` replicas of its set compacted to, counted on the leader side through `membership/` | §J3.2 | `segment-truncate/` (#1276), the floor kept for each segment (`acc_grid`); `quorum-everywhere/`, `compact` (#1318) |
| Re-replication of a sealed segment | a member that holds the whole segment copies it to a new replica, which counts only on its own complete copy; with `D` members down, at least `F + 1 - D` live members hold the segment (v1, Pierre, 2026-11-20) | §J3.1, §J3.4 | `re-replication/` (#1314 (re-replication of a sealed segment's replica set)) |

### J5.3 Damage: CTRL in the journal

A disk can return a record that fails its checksum. The core handles this on plain Multi-Paxos
with the faulty entries of CTRL and the tri-state tally (`specs/tla/truncation/`, #1216
(truncation model, part 2)). **A damaged record is repaired by CTRL, whoever finds it** (#1263
(CTRL recovery in the journal), checked on 2026-11-20 by `specs/tla/ctrl-grid/` and
`specs/tla/ctrl-segments/`). The grid controller owns the scrub (§J6): it schedules a read of
every copy (the acceptor records, the replica logs, the offloaded objects), and the roles that
hold the data read them back with their checksums. Each rule has a control that TLC breaks:

- **The tri-state tally asks a Phase-1 quorum of every configuration of `H_b`, never a count.**
  At a slot with a faulty report, the threshold is the highest readable report. An answer
  qualifies unless it is a faulty report above it. The slot is decided only when the qualifying
  answers hold a Phase-1 quorum of each configuration of `H_b` (`slot_decidable` calls
  `has_phase1_quorum` for each `prior` configuration); otherwise it goes to the repair probe. On
  a grid this is `rows - acks + 1` qualifying answers of every column. A majority of the members
  fills a chosen slot with a `Noop` on a grid (`grid4_majority`). A tally that asks only the
  configuration of its ballot fills a slot of a sealed segment that a faulty record of the old
  grid holds (`two_current`). A threshold one above the highest readable report loses a chosen
  value (`plus_one`, and `truncation/ctrl_plus_one`).
- **A scrub and a boot give the same faulty record.** A damaged record keeps its ballot and
  loses its value, whoever reads it. A scrub that drops the damaged record, so that the slot
  reads as a slot with no vote, chooses two values (`damage_drop`).
- **A repair writes a chosen value.** The journal keeps no record in memory, so an acceptor
  cannot write the record again from its own copy. It writes the value that a node learned (a
  replica, or the leader) under the ballot that the faulty record keeps. A value copied from a
  peer's vote that is not known to be chosen loses a chosen value (`repair_peer`). With no repair
  at all the journal stays safe (`repair_none`): the repair adds a copy, it never decides.
- **A repair replaces the damaged record in one write.** A repair that deletes the record first
  and writes the new one after is safe while the acceptor promises nothing and does not
  campaign between the two (`drop_first_up`, `drop_first_two`, `drop_first_lead`; a vote between
  the two stays safe), but a restart between the two forgets the repair, and the slot reads as a
  slot with no vote (`drop_first`).
- **A sealed segment retires at `F + 1` replica holders, and a repair never needs the retired
  grid** (§J5.2). A found damaged replica log is repaired from a live peer of its set, from the
  grid until the retire, or from the object once offloaded. A damaged copy is one of the `F`
  failed copies of the survive promise until its repair, so `F + 1` holders always leave one
  good copy to repair from. A retire at `F` holders loses a slot (`retire_f`, `live_retire_f`).
- **The retire does not count a replica whose damage a scrub found and no repair fixed.** This
  is a margin, not a safety rule: the retire stays safe without it (`retire_any` passes with a
  damaged counted replica). **Decided** (Pierre, 2026-11-20): keep the guard, because a disk
  that rots once often fails again soon.
- **The offloader puts only a log that passes its checksum, and a replica drops its copy only
  after the record says offloaded** (§J3.1). A put of a log that it did not check makes a rotted
  copy the only one once the replicas drop (`upload_any`). A drop before the mark loses the
  segment with no fault (`drop_retired`).
- **A damaged object is replaced from a replica, or reported as data loss.** While a replica of
  the set holds a good log, a good copy exists to replace the object. Once no replica holds the
  segment, a damaged object is data loss: outside the survive promise, reported, never recovered
  in an unsafe way (§J6.2, Pierre, 2026-11-20). A loss report is always true: no good copy is
  left (`object`, `live_object`, `reach_loss`). The model replaces the object in one step. The
  offloader's conditional put never replaces an object, and no rule deletes the object of a
  segment that is not trimmed (`offload/`, #1256 (offload races and the offloader role)).
  **Open**: how a replace works (a delete that the mark fences) is for the offload design, with
  its own check (§J3.1).

**The seal end counts a faulty vote** (#1313 (the seal end against a CTRL faulty record),
`specs/tla/seal-ctrl/`): the end is the frontier of the sealer's Phase 1 over the old grid, and the
seal does not wait for the repair probe (§J3.1).

**CTRL in the journal roles** (#1326 (the journal roles over the core), `specs/tla/journal-ctrl/`,
on 2026-11-23). The composed model of the roles (`JournalCtrl.tla` over `JournalCompact.tla`) adds
a boot that finds a vote damaged (`Rot`), the leader's probe, its learn and the acceptor's repair:

- **The leader's Phase 1 takes the tri-state tally.** A slot whose top report is a faulty vote is
  decided when the qualifying answers hold a Phase-1 quorum of every grid that reads it; else it is
  blocked, and no proposal, a client's included, takes it. A late answer folds into the tally and
  can decide the slot (`ProbeDecide`). An answer that dropped the slot never enters that tally:
  the slot is then chosen and applied at `F + 1` replicas, and the other answers may predate the
  compaction, so the tally would read a stale vote. The leader skips the slot and proposes
  nothing there (`ProbeSkip`; the control `ctrl_probe_drop` tallies it and chooses a `Noop` over
  the chosen write).
- **A replica that applied a blocked slot gives its chosen command** (`Learn`, `Learned`;
  `LearnChosen`). The leader then proposes that command at its own ballot.
- **The leader repairs once its own round chose the command.** It sends `Repair` with its ballot to
  each acceptor that reported the slot faulty. The acceptor writes the command at the higher of
  that ballot and the faulty ballot, with the promise raised to it, in one write (the core's
  `Acceptor::learn_chosen`). A vote above the choosing ballot carries the chosen command, so the
  write gives back the lost vote; a vote below gives way to the vote at the choosing ballot. A
  repair from a peer's vote breaks one value at one ballot (`ctrl_peer`), and a damage that drops
  the vote breaks agreement (`ctrl_drop`).
- **A damage stays inside the contract.** A damaged copy is one of the `F` failed copies: the model
  damages a vote only when a replica applied the slot, the slot chose another value, a quorum of
  a live grid holds the chosen value on disk at or above the ballot that chose it, or no quorum can
  choose the vote's value (`Spare`). A rot of the last copy is data loss, outside the survive
  promise (`Readable`). In the roles walk, a rot of the two arms that no replica covers (a quorum
  holds the chosen value, or no quorum can choose the vote's value) keeps each column at no more
  than `F` damaged votes at the slot, so a tally can still decide it.
- **A vote that no quorum can choose may be damaged, and the tally then decides a `Noop`**
  (#1326, 2026-11-24, `Unchoosable` of `JournalCtrl.tla`). A promise quorum of the vote's grid
  without the damaged acceptor holds no vote at the slot, kept the slot and promised above the
  vote's ballot, so no quorum voted at that ballot. A blocked slot is then not always chosen: a
  late answer can decide it with a `Noop` (`reach_noop`), and the learn from a replica acts only
  on a chosen slot. The invariant `BlockedChosen` says this (Pierre, 2026-11-24): each blocked
  slot is chosen, or the highest ballot that voted there is a ballot that no quorum can complete (`DeadAt`). A probe decides a
  `Noop` at an unchosen slot only after such a rot (`NoNoopProbe`).

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
