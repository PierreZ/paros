# Experiment: leaderless reads across a reshape, on design A

#1254 (leaderless reads across a reshape in the segmented journal). This folder extends the
segments experiment of #1213 (`../a/`, design A, which Pierre chose on 2026-11-19). It changes no
model of the plan and no rule of `paros-consensus`. The report below goes to Pierre: he asked on
2026-11-19 for this rule to be checked with TLA+ before it is recorded.

## Result in short

The stale read of #1254 is real across a reshape when a read does not look at the promises of
the acceptors it asks: a reader that holds the record of the old segment asks the old grid, its
answers did not see the new grid's write, and the read misses a slot that the new grid chose
(`bare` on the full grid, `part_bare` on the partitioned grid).

**Rule 1 (the record check) adds nothing while each acceptor keeps its promise.** The read fence
that paros has today closes the case: each answer carries the acceptor's promise, and an answer
whose promise is a ballot of a later configuration ends the read (`quorum_read.rs`, `reads/`).

- On the partitioned grid, the default grid of the journal (#1257 (quorum everywhere) excludes
  the full grid), two Phase-1 quorums of one grid meet, so the reader always asks an acceptor
  that promised the new ballot: the fence alone passes (`part_fence`).
- On the full grid (which the journal does not offer, section 2.11 of `docs/architecture/02-data-plane.md`, but `paros-consensus` still builds for other users) two rows do not meet, and the fence also needs the seal of each row of
  decision 31 (#1209 (seal each row of a full grid)): `fence_rows` passes, and each half alone
  fails (`fence_only`, `seal_only`).
- Rule 2 (a row and a whole write column) passes on the full grid with no seal of each row
  (`column`).

**Rule 1 adds safety in one case: an acceptor of a retired segment that loses its state.** When
the GC of a retired configuration deletes an acceptor's state of the journal, or the node is
booked again onto an empty store, and the acceptor then answers a read as an empty store does
(promise 0, watermark 0), the promise fence has nothing to fence with. The promise fence then
fails on both grids (`part_wipe_fence`, `wipe_fence_rows`), and rule 2 fails once the write is
two reshapes away (`wipe_column_three`). Rule 1 passes in every check (`record`, `wipe_record`,
`part_wipe_record`).

**The fence of the code closes that case too, with no matchmaker read.** `wire_read_fence` is
the higher of the promise and the belief ballot, and an acceptor that did not hear what is in
force answers no read (`Refuse` of `reads/`, `on_pre_read`). An acceptor that lost its state
reads the register before it answers again (the membership probe at boot), so its fence names
the configuration in force. With that fence and the refusal, `part_wipe_belief` and
`wipe_belief` pass; on the full grid it still needs the seal of each row (`wipe_belief_noseal`
fails).

**Decided** (Pierre, 2026-11-20, `docs/architecture/02-data-plane.md` §2.10): do not add rule 1 to the read path. Keep the
fence of the code on both halves (the promise and the configuration that the acceptor heard of)
and the refusal of an acceptor that did not hear; on a full grid, keep the seal of each row of
#1209. Rule 1 costs a read of a matchmaker quorum for each client read, on matchmakers that the
paper keeps idle under a stable leader. Rule 1 is the fallback if the belief half ever leaves
the fence (for example if section 4.1's "a server learns its configuration from the register
only" removes the belief ballot from the acceptor's answer).

## The model

`SegmentReads.tla` takes design A from `../a/SegmentsA.tla`: the matchmakers hold the segment
table, the first leader of a configuration seals the one before it at one above the highest vote
that its Phase 1 saw, Phase 1 reads a sealed configuration only below its end, the leader writes
again each slot above the chosen prefix and below its start (the vote that Phase 1 saw or a
`Noop`), and a sealed configuration retires once each slot of its range is chosen. In place of
majorities, it takes its quorums from `Grid.tla` (linked from `specs/tla/grid/`), on two kinds
of grid. On a full grid (acks = rows) a whole column writes, one acceptor of each column elects,
and a reader asks one row; two rows do not meet (`grid/grid_full_rows`). On a partitioned grid
(acks < rows, #532) F+1 of one column write, F+1 of each column elect, and a reader asks every
member; two Phase-1 quorums meet.

The reader caches one record of the table: a configuration and its start slot. It asks the
acceptors of a row of that configuration, one at a time, for their vote watermark. It serves at
the highest watermark, or at the start minus one when that is higher, once each slot up to there
is chosen. One read runs in each behavior, and it can start in any state. A reader can fetch the
record in force before it starts. Acceptors keep their promise and votes on disk, and an acceptor
of a retired configuration still answers from its disk, unless `Wipe` lets it lose its state.

| Constant | Values |
|---|---|
| `Fence` | `TRUE`: an answer whose promise is a ballot of a later configuration than the record ends the read; the reader fetches the record in force and starts again. `FALSE`: no fence. |
| `SealRows` | `TRUE`: Phase 1 also takes one promise in each row of each configuration that it reads (decision 31). `FALSE`: a Phase-1 quorum of each only. |
| `ReadRule` | `"row"`: one row (the code). `"record"`: rule 1, the same row read, and a read of the table at a matchmaker quorum once the answers are complete; the reader serves in a later step, and a newer record in force starts the read again. `"column"`: rule 2, a row and one whole column, with the fence. |
| `Wipe` | `TRUE`: an acceptor that is in no configuration that still runs can lose its state, and then answers as an empty store. |
| `Belief` | `TRUE`: each acceptor also knows the newest configuration that it heard of; the fence is the higher of that and its promise; an acceptor that lost its state answers nothing until it read the register again. |

The invariant is `ReadFresh`: a served read covers the highest slot that was chosen when it
started. Every check also holds `Agreement` and `SealedFrozen` (nothing is chosen in a sealed
configuration at or above its end).

Phase 1 promises a smallest set that covers the configurations it must read (`MinCovers`). A
larger set only adds promises, and a promise only gives a fence more to see, so the smallest
sets are the hardest case for a read. This is a reading that keeps the state space small, not a
fact that TLC checked.

## Checks

Three layouts, in `MC.tla`. The full grid at its smallest, 2 x 1 (`QuorumSystem::grid(2, 1)`):
one column of two members writes, and each row is one member; configuration 1 is `{a1, a2}`,
configuration 2 is `{a2, a3}`, configuration 3 is `{a3, a4}`. The partitioned grid at its
smallest, 3 x 1 with acks 2 (`QuorumSystem::partitions(1, 1)`, the default grid of the journal: a
majority of three); configuration 1 is `{a1, a2, a3}`, configuration 2 is `{a2, a3, a4}`. The
2 x 2 full grid of `reads/`. Two slots unless the row says one, one value and the `Noop`, one
reshape and two ballots unless the row says more. TLC 2.19, 2 workers.

| Check | Grid | Fence | Seal rows | Rule | Wipe | Result |
|---|---|---|---|---|---|---|
| `bare` (rule 3 of #1254, the control) | 2 x 1 | no | no | row | no | violation (5,677 states) |
| `fence_only` | 2 x 1 | promise | no | row | no | violation (6,296 states) |
| `seal_only` | 2 x 1 | no | yes | row | no | violation (2,666 states) |
| `fence_rows` (the code with #1209) | 2 x 1 | promise | yes | row | no | pass (54,192 states, 9 s) |
| `record` (rule 1 alone) | 2 x 1 | no | no | record | no | pass (273,706 states, 36 s) |
| `column` (rule 2) | 2 x 1 | promise | no | column | no | pass (176,870 states, 23 s) |
| `wipe_fence_rows` | 2 x 1 | promise | yes | row | yes | violation (9,083 states) |
| `wipe_record` | 2 x 1 | no | no | record | yes | pass (295,104 states, 40 s) |
| `wipe_column` | 2 x 1 | promise | no | column | yes | pass (191,534 states, 27 s) |
| `wipe_belief_noseal` | 2 x 1 | promise and belief | no | row | yes | violation (6,927 states) |
| `wipe_belief` (the whole fence of the code) | 2 x 1 | promise and belief | yes | row | yes | pass (87,368 states) |
| `part_bare` | 3 x 1 | no | no | row | no | violation (27,728 states) |
| `part_wipe_fence` | 3 x 1 | promise | no | row | yes | violation (149,727 states, 52 s) |
| `part_fence_one` (the code), one slot | 3 x 1 | promise | no | row | no | pass (99,273 states, 14 s) |
| `part_wipe_fence_one`, one slot | 3 x 1 | promise | no | row | yes | violation (40,145 states) |
| `part_wipe_belief_one`, one slot | 3 x 1 | promise and belief | no | row | yes | pass (151,103 states, 20 s) |
| `part_wipe_record_one`, one slot | 3 x 1 | no | no | record | yes | pass (166,957 states, 22 s) |
| `reach_stale`: a read that started on a stale record, after configuration 2 chose a slot, is served | 2 x 1 | promise | yes | row | no | violation, as it must (5,145 states) |
| `reach_retire`: configuration 1 retires after configuration 2 chose a slot | 2 x 1 | promise | yes | row | no | violation, as it must (709 states) |
| `square_fence_only` | 2 x 2 | promise | no | row | no | violation (109,259 states) |

The long checks (`scripts/check-tla.sh --all`):

| Check | Grid | Fence | Seal rows | Rule | Wipe | Result |
|---|---|---|---|---|---|---|
| `wipe_column_three`: three configurations, three ballots | 2 x 1 | promise | no | column | yes | violation (1.5 million states, 4 min) |
| `part_fence`: two slots | 3 x 1 | promise | no | row | no | pass (6.3 million states, 39 min) |
| `fence_rows_long`: three ballots | 2 x 1 | promise | yes | row | no | pass (1.7 million states, 12 min) |
| `fence_rows_three`: three configurations, three ballots | 2 x 1 | promise | yes | row | no | pass (5.2 million states, 11 min on 4 workers) |

Seven passing runs did not finish in 45 minutes on 2 workers. They are bounded results, not
checks, and they are not in `checks.txt`: TLC found no violation in the states that it explored.

| Run | Grid | Rules | Explored with no violation |
|---|---|---|---|
| promise and belief fence, empty store, two slots | 3 x 1 | `part_wipe_belief` with `NS = 2` | 8.0 million states, depth 24 |
| rule 1, empty store, two slots | 3 x 1 | `part_wipe_record` with `NS = 2` | 7.8 million states, depth 23 |
| rule 1, three ballots | 2 x 1 | `record` with `NB = 3` | 8.0 million states, depth 23 |
| rule 2, three ballots | 2 x 1 | `column` with `NB = 3` | 7.2 million states, depth 24 |
| promise and belief fence and row seal, empty store, three configurations | 2 x 1 | `wipe_belief` with `NG = 3`, `NB = 3` | 6.9 million states, depth 26 |
| the code with #1209, two ballots | 2 x 2 | `fence_rows` on the 2 x 2 grids | 9.7 million states, depth 19 (27 min, then stopped) |
| rule 1, two ballots | 2 x 2 | `record` on the 2 x 2 grids | 7.1 million states, depth 17 (22 min, then stopped) |

`reach_stale` shows that the passing checks see the dangerous case: a read starts on the record
of configuration 1 after configuration 2 chose a slot, the fence ends it, and it is served on
the record of configuration 2.

## The traces

`bare`, the stale read of #1254:

1. Ballot 1 registers configuration 2 (a reshape). Its Phase 1 takes `a2` alone: one acceptor of
   the one column of configuration 1, and of configuration 2 too. `a1` hears nothing.
2. Ballot 1 seals configuration 1 at end 1, so configuration 2 starts at slot 1.
3. `a2` and `a3`, the column of configuration 2, accept slot 1. Slot 1 is chosen.
4. The reader holds the record of configuration 1. It asks the row `{a1}`: watermark 0. That is
   a Phase-1 quorum of configuration 1, and the read is served at index 0. It misses slot 1.

With the fence alone, `a1` still answers promise 0 (`fence_only`). With the seal of each row
alone, `a1` promised ballot 1, but the reader does not look at the promise (`seal_only`).

`wipe_fence_rows`, the empty store:

1. Ballot 1 registers configuration 1 and does nothing more. Ballot 2 registers configuration 2,
   and its Phase 1 takes each row of both grids: `a1`, `a2` and `a3` promise ballot 2. Ballot 2
   seals configuration 1 at end 1.
2. `a2` and `a3` choose slot 1 at ballot 2. Configuration 1 retires: its range is empty.
3. `a1` is in no configuration that still runs, and it loses its state: promise 0, no votes.
4. The reader holds the record of configuration 1 and asks the row `{a1}`: promise 0,
   watermark 0. The read is served at index 0 and misses slot 1.

With `Belief`, `a1` answers nothing until it reads the register; then its fence names
configuration 2, and the read starts again on the record of configuration 2.

`wipe_column_three`, rule 2 two reshapes away: ballot 1 takes configuration 2 with the promises
of `a1` and `a3` (no seal of each row, so `a2` hears nothing); ballot 2 takes configuration 3
and chooses slot 1 in the column `{a3, a4}`; configuration 1 retires and `a1` loses its state.
The reader asks configuration 1's row `{a1}` and column `{a1, a2}`: `a1` is empty and `a2` never
heard of ballots 1 or 2, so the read is served at index 0.

`part_wipe_fence_one`, the empty store on the partitioned grid:

1. Ballot 1 registers configuration 2. Its Phase 1 takes `{a1, a2}` of configuration 1 and
   `{a2, a4}` of configuration 2, and it seals configuration 1 at end 1. `a3` hears nothing.
2. `a2` and `a4` choose slot 1 at ballot 1. Configuration 1 retires, and `a1`, which is in no
   configuration that still runs, loses its state.
3. The reader holds the record of configuration 1 and asks every member. `a1` (empty) and `a3`
   (never heard) answer promise 0 and watermark 0: a Phase-1 quorum of configuration 1. The read
   is served at index 0 and misses slot 1.

## What the record check answers that the fence does not

The issue asked whether rule 1 adds anything over the read fence of the winning leadership. The
answer is in two parts:

- While every acceptor keeps its promise for as long as a reader can name its configuration, no:
  the fence on the promise gives the same result (with the seal of each row on a full grid), and
  a reader learns the new record from the first answer that is fenced.
- When an acceptor can answer with less than it once promised (a GC that deletes its state, a
  node booked again onto an empty store), the promise fence has a hole, and rule 1 closes it.
  The belief half of the code's fence, with the refusal of an acceptor that did not hear, closes
  it as well: an acceptor that lost its state hears the register again before it answers, and
  the register names the configuration in force. Rule 1 does that check on the reader's side, at
  the cost of a matchmaker read for each client read; the code does it once on the acceptor's
  side, at boot.

## Limits

- The matchmakers are one atomic register, as in `../a/`: a read of the table is linearizable by
  construction. `matchmaker/` and `handover/` check how a quorum keeps the register.
- No Phase-1 bypass (#579 (zero-stall reconfiguration)): a slot is chosen in a new configuration
  only after its first leader ran Phase 1 on the old one. #1270 (the bypass hold does not order
  two leaderless reads) shows that the bypass breaks leaderless reads on its own; a segmented
  journal with the bypass needs that fixed first.
- An acceptor that lost its state never joins a later configuration in these checks (`a1` is in
  configuration 1 only). Such a node could answer a read for
  an old configuration after it promised nothing in the new one; `Belief` covers that case by the
  same refusal, but no check runs it.
- One read for each behavior, and one value. The read that starts again keeps the highest slot
  that it must see.
- No replica tier: a read is served once the slots up to its index are chosen.

## How to run

    nix develop .#tla --command scripts/check-tla.sh experiments/segments/reads
    nix develop .#tla --command scripts/check-tla.sh --all experiments/segments/reads   # the long checks too
