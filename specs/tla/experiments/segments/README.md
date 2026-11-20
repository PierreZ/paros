# Experiment: segments in the protocol or in storage

#1213 (TLA+ experiment: segments in the protocol or in storage). This folder is an experiment,
not a model of the plan: it changes no model of `specs/tla/`, and no rule of `paros-consensus`
comes from it until Pierre decides. The report below goes to Pierre.

**Decision**: Pierre chose design A on 2026-11-19. The rules are recorded in
`docs/architecture/02-data-plane.md` section 2.10. The gap review of 2026-11-19 found a missing
rule of A and three rows of this report that were not fair to B (#1233 (segments gap fill));
Pierre kept A, and this report includes the corrections.

## Result in short

Design A (the matchmakers hold the segment table) passes all seven checks and the stranded-hole
check with six rules, each with a control that TLC breaks. Design B (plain Matchmaker Paxos,
chunks local to each replica) with fixed chunks and the read fence of paros passes checks 1, 2,
3, 4, 5 and 7. Plain B **fails check 6 by definition**: it has no decided record of an offload,
so a slot whose only copy is in the object store is declared nowhere. TLC only shows that such
a slot is reached. The variant of B that records each offload as an entry of the journal's own
log passes check 6, at the cost of a log entry for each offload, a leader to append it, and a
fold of the log for each client that wants the view. A lighter variant of B (fixed chunks and
one decided "offloaded up to" mark) is not modelled.

TLC found one rule of the design doc that is wrong as written. The doc says that the end slot of
a sealed segment is "the first uncommitted slot". With that end, a Phase 1 that reads the old
grid only below the end chooses a second value for a slot (trace below). The end must be one
above the highest slot where Phase 1 saw a vote in the old grid, or Phase 1 must keep reading
the whole old grid until it retires (then the end bounds nothing).

That end is complete only with a gap fill (rule 2b): the leader also writes a `Noop` at each
slot below the start of its segment where Phase 1 saw no vote. Without it, a slot below the end
with no vote is a hole that no ballot can write, and the journal stops there for ever
(`gap_off`, 10 states).

Pierre's lean towards A holds against the bar: A passes every check, and the upper stack reads
the segment table as one decided record. Its cost is the matchmaker writes of each segment
(cost section). B can reach a decided view with the log-claim variant, which moves the table
into the log and makes each client fold it.

## Reads across a reshape

`reads/` is the check of #1254 (leaderless reads across a reshape) on design A, on the
partitioned grid and the full grid. Its report is `reads/README.md`. In short: the stale read is
real with no fence; the read fence on the promise closes it (with the seal of each row of
decision 31, #1209, on a full grid), so the record check of the reader adds nothing there; an
acceptor of a retired segment that loses its state opens the case again, and the belief half of
the fence (with the refusal of an acceptor that did not hear) or the record check closes it.
Pierre chose the fence of the code, with no record check (2026-11-20,
`docs/architecture/02-data-plane.md` §2.10).

## The two designs

| | Design A: `a/SegmentsA.tla` | Design B: `b/SegmentsB.tla` |
|---|---|---|
| Matchmakers hold | the configurations and one record for each segment: start, end, state | the configurations only |
| A segment ends | at seal: the first leader of the next configuration writes the end once, by compare-and-set | nowhere in decided state; each replica cuts its own chunks |
| Object key | journal + configuration number | journal + first slot + last slot of a chunk (fixed chunks: a function of the slot) |
| Phase 1 reads | each sealed configuration below its end, the open one in full | every configuration that still runs, above the replicas' applied prefix |
| A tail read reads | a quorum of the cached configuration; the start of the cached segment bounds it below | a quorum of the cached configuration; the fence of a leadership that won bounds it below |
| A replica drops its copy | once the record says `offloaded` | plain B: once its object is in the store; variant: once it also applied the winning claim |
| Spec size | 521 lines | 469 lines |

Both models have the same parts: a reshape (a ballot of the successor configuration), a leader
change (a later ballot of the same configuration), a leader crash (a leader that stops taking
steps), retire (Matchmaker garbage collection), offload, the drop of a local copy, trim, a
consumer cursor, and a tail reader that caches its view and fetches it again on a "sealed"
answer. The matchmakers are one atomic register: `specs/tla/matchmaker/` and
`specs/tla/handover/` check how a quorum of matchmakers keeps it. Acceptors and replicas keep
their state on disk, so a restart changes nothing that the models see.

## Checks

| Check | A | B plain | B with log claims |
|---|---|---|---|
| 1. One value for each slot (`Agreement`) | holds | holds | holds |
| 2. Every slot at or above the floor is readable with the chosen bytes (`Readable`) | holds | holds | holds |
| 3. An object never changes (`StoreImmutable`); one object for each key | holds | holds | holds |
| 3+. One object for each slot (`OneHome`, not in the issue) | holds, but **vacuous**: with two configurations only one segment can be sealed, so only one object exists | holds with fixed chunks; fails with free cuts (`plain_onehome`) | holds: only the winning claim is uploaded |
| 4. Recovery reads a bounded number of slots, safely | holds with the end at the highest vote + 1 | holds: above the replicas' applied prefix, on every configuration that still runs | holds |
| 5. A tail read is linearizable across a reshape, with a stale cached view (`ReadFresh`) | holds | holds with the read fence of paros: one configuration is read (`reads_fence`) | not run (same read as plain B) |
| 6. One decided view of where each slot lives, offloaded slots included (`Visible`) | holds: the record | **fails by definition**: `Declared` is `FALSE` with no claims | holds: the winning claim in the log |
| 7. Offload, retire and trim never stop a leader that can write (`TailNeverBlocked`) | holds | holds | holds |
| No stranded hole (`NoStrandedHole`, not in the issue) | holds with the gap fill (rule 2b) | holds by construction | holds by construction |

`NoStrandedHole` says that each leader of the configuration in force that finished Phase 1 may
propose at each slot at or above the floor that is not chosen. It is a safety form of the
liveness property "the chosen prefix can always grow". A also passes `EndOnce` (the end is
written once), `SealedFrozen` (nothing is chosen in a sealed configuration at or above its
end), `TableSound` (the sealed records do not overlap the start of a later one) and
`CursorSafe`. `a/safety.cfg` checks all of A's invariants together, with Phase 1 bounded below
the end (the rule of check 4).

## What each design had to add to pass

Design A:

1. **The end slot is one above the highest vote that Phase 1 saw in the old grid**
   (`EndRule = "voted"`). Control `end_uncommitted_below`: with the doc's "first uncommitted
   slot" (here: the first slot the replicas did not apply), TLC finds two values for slot 1 in
   14 steps. Ballot 1 chooses v1 at slot 1 in grid 1. Ballot 2, the first leader of grid 2,
   seals grid 1 at end 1 (no replica applied slot 1 yet) and stops. Ballot 3, a leader change in
   grid 2, reads grid 1 below end 1 (nothing), and chooses v2 at slot 1. With the first
   uncommitted slot, a slot at or above the end can also be chosen in the sealed grid
   (`end_uncommitted_frozen`), and a tail read misses it (`reads_uncommitted`). The doc's rule
   is safe only when Phase 1 reads the old grid in full until it retires
   (`end_uncommitted_all` passes); then the end bounds nothing for recovery.
2. **The sealing leader proposes again, in its own grid, each slot below the end where it saw a
   vote and that the replicas did not apply.** With three acceptors for each grid, a vote that
   Phase 1 sees can be a vote that is not chosen; nobody else would ever propose that slot. So a
   few slots of the old segment's range are chosen in the new grid. TLC shows it with three
   acceptors (`reach_boundary_three`): a1 votes v1 at slot 1 in grid 1 and nothing is chosen;
   the leader of grid 2 sees that vote, seals grid 1 at end 2, and gets v1 chosen at slot 1 in
   grid 2. The rule is safe (`end_voted_below_three` passes). The table still says which
   range each segment covers; it does not say which grid holds each vote of the boundary slots.

   **2b. Each leader of the configuration in force writes a `Noop` at each slot above the
   applied prefix and below the start of its segment where its Phase 1 saw no vote**
   (`GapFill = TRUE`). It is the election gap fill that the core already has. Control
   `gap_off` (`NoStrandedHole` fails in 10 states): ballot 2 of grid 1 proposes slots 1 and 2,
   and a1 votes slot 2 only. Ballot 3 of grid 2 runs Phase 1, sees the vote at slot 2, and seals
   grid 1 at end 3, so grid 2 starts at slot 3. Slot 1 has no vote and is below the start. The
   old grid is sealed and no later ballot registers it, so nobody can propose slot 1. The
   replicas stop at slot 1 for ever, so retire, offload and trim never come, and a tail read
   answers 2 for a slot that is never chosen. With the gap fill, Phase 1 bounded below the end
   stays safe (`safety`, `end_voted_below_three`).
3. **The end is a compare-and-set from "none".** Control `seal_blind`: two sealing leaders both
   write an end (`EndOnce` fails).
4. **A leader's writes are fenced by its promise quorum, not by the table version.** Control
   `fence_table`: if a write needs the table version that the leader read, then the record change
   of an offload or a retire stops the tail until the leader reads the table again (check 7
   fails). This is the Delos rule: a change to a sealed loglet needs no seal.
5. **A replica drops its copy only after the record says offloaded.** Control `drop_uploaded`:
   a drop after the upload and before the record write leaves a slot only in the store with no
   record that names it (check 6 fails).

Design B:

1. **A tail read needs a bound below its cached configuration.** B has no segment start, so a
   reader that reads only its cached configuration with no bound misses a slot that the old grid
   chose and the replicas did not apply yet (control `reads_view`). Plain Matchmaker Paxos in
   paros has the bound already: the fence of a leadership that won (`quorum_read.rs`,
   `specs/tla/reads/`). With that rule (`ReadScope = "fence"`: a quorum of the cached
   configuration, on the basis of a ballot of it that finished Phase 1 and that no member of the
   quorum promised past, at least that ballot's fence), B reads one configuration, as A does
   (`reads_fence` passes; `reach_read_old` shows that the case of `reads_view` is reached).
   Reading every configuration that still runs, as Phase 1 does (`ReadScope = "live"`), also
   passes. So check 5 does not separate the designs: A takes the bound from the table, B from
   the leadership's fence.
2. **Fixed chunks for one object for each slot.** With one slot for each chunk, the key is a
   function of the slot, and plain B passes `OneHome` (`plain`). With free cuts, two replicas
   cut different chunks and two objects hold one slot (`plain_onehome`, the control).
3. **For check 6, the log-claim variant.** A replica asks the leader to append a claim for its
   next chunk; the first chosen claim that covers a slot wins; a replica uploads only a winning
   claim, and drops its copy only once it applied that claim and the object is in the store.
   Plain B has no decided record of an offload: a client must list the object store, and a
   listing is not decided state. `plain_visible` fails by the definition of `Declared`, not by a
   race that TLC finds.

Both designs share three rules, each with a control in each folder: retire a configuration only
when every slot with a vote in it is applied by f+1 replicas or chosen in a later configuration
(`retire_early` fails check 2), a tail read answers "sealed" from an acceptor that promised a
later configuration (`read_nofence` fails check 5), and trim stops at the consumer cursor
(`trim_nocheck`).

## What each design costs

Pierre's point: a segment that lives only in S3 must still be declared, so that the grid
controller, placement, the offload policy and the readers see it as decided state.

- In A, one read of a matchmaker quorum gives the whole table: each segment, its range, its
  state and its object key. Two clients that read the same table version see the same view.
- A pays for the table with **matchmaker writes**: four for each segment (seal, retire, mark
  offloaded, trim), each a compare-and-set at a matchmaker quorum. Only the seal is written by a
  leader; retire, mark and trim come from the offloader and the trim path, on matchmakers that
  the paper keeps idle under a stable leader. A client view is a matchmaker quorum read. The
  model hides this cost: its matchmakers are one atomic register.
- **A trims only at segment ends**: `Trim(p)` moves the floor to the end of a sealed segment, so
  how much a consumer must keep depends on how often a reshape or a roll seals a segment. B
  trims at any slot below the consumer cursor (a decided `Truncate`).
- In plain B, nothing decided declares an offloaded range. A client has to list the object
  store and ask the replicas, and two clients that list at different times see different sets
  of objects.
- In B with log claims, the view is decided, and two clients at the same log position agree.
  The costs: each offload takes one log entry and needs a live leader to append it; each client
  must fold the log from its start (or from a fold that a snapshot keeps) to know where a slot
  lives; and the claims sit in the log that trim cuts. **Not modelled**: once trim passes a
  claim entry, the declaration survives only if the journal state carries the fold of the
  claims across the trim, as the truncation model (#1017 (truncation model)) carries the floor.
  The model keeps every vote, so it cannot see this loss.

## State counts

Two configurations (one reshape), two slots, two values, two replicas with f = 1, one acceptor
for each configuration (a vote is then a choice) unless the row says otherwise. Symmetry over
the values and the replicas. TLC 2.19; the times are on 2 workers.

| Configuration | Ballots | A distinct states | B distinct states |
|---|---|---|---|
| Every rule on, storage on (A `safety`, B `plain` with fixed chunks) | 3 / 2 | 306,812 | 158,422 |
| Every rule on, storage on (B `plain_long`) | 3 | | 5,264,806 (22 min) |
| B with log claims, free cuts (`claims`, `claims_long`) | 2 / 3 | | 96,713 / 3,039,127 (31 min) |
| Tail reader, storage off (`reads`, `reads_long`) | 2 / 3 | 58,933 / 1,702,536 | 65,074 / 1,950,482 |
| Tail reader with the read fence of paros (B `reads_fence`, `reads_fence_long`) | 2 / 3 | | 63,049 / 1,901,777 |
| Bounded Phase 1 with the gap fill, three acceptors for each grid, no storage (`end_voted_below_three`) | 2 | 1,215,762 (8 min) | |

The earlier version of this report said that B is about 25 times larger than A because each
replica cuts its chunks where it likes. The cuts are not the main cause: with fixed chunks, B
still has 158,422 states at two ballots (227,778 with free cuts). The gap review reads the cause
as B offloading at any time while A offloads only after retire; that is a reading, not a
measurement. Three runs did not finish, and
they are not in `checks.txt`: A with three acceptors for each grid and three ballots (19.4
million distinct states in 56 minutes before the gap fill, no violation, the queue still
growing), and B plain and B with claims with three acceptors for each grid (45 minutes on one
worker each).

The CI set of each folder is its own CI job. The `long` checks run with
`scripts/check-tla.sh --all`.

## Limits of the experiment

- One acceptor for each configuration in the CI set. The three-acceptor checks of A are `long`,
  with storage off and one reshape; the full A and B with three acceptors do not finish.
- Two configurations only, so plan rule 1 (one in force plus at most one successor) is never
  stressed by a third reshape, and A's `OneHome` cannot fail (one sealed segment).
- No reader of old data runs while a replica drops its copy, the record is marked or trim runs:
  `Readable` checks that the bytes are somewhere, not that a reader finds them during the move.
- `NoStrandedHole` is a safety form of a liveness property; neither model checks liveness under
  fairness.
- The matchmakers are one atomic register; their quorum and handover are in other models.
- The object store is atomic: a put and a delete are single steps. A late upload after trim
  (review problem "offload safety" of the design doc) is not modelled.
- Grid quorums are not modelled here (`specs/tla/grid/` covers them); a configuration is a
  majority.

## How to run

    nix develop .#tla --command scripts/check-tla.sh experiments/segments/a experiments/segments/b
    nix develop .#tla --command scripts/check-tla.sh --all experiments/segments/a   # the long checks too

Each folder has its `checks.txt`, as every model of `specs/tla/` has.
