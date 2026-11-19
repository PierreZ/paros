# Experiment: segments in the protocol or in storage

#1213 (TLA+ experiment: segments in the protocol or in storage). This folder is an experiment,
not a model of the plan: it changes no model of `specs/tla/`, and no rule of `paros-consensus`
comes from it until Pierre decides. The report below goes to Pierre.

**Decision**: Pierre chose design A on 2026-11-19. The rules are recorded in
`docs/architecture/02-data-plane.md` section 2.10.

## Result in short

Design A (the matchmakers hold the segment table) passes all seven checks with five rules, each
with a control that TLC breaks. Design B (plain Matchmaker Paxos, chunks local to each replica)
passes checks 1, 2, 3, 4, 5 and 7, but **fails check 6**: a slot whose only copy is in the
object store is declared nowhere in decided state. The variant of B that records each offload as
an entry of the journal's own log passes check 6 too, at the cost of a log entry for each
offload, a leader to append it, and a fold of the log for each client that wants the view.

TLC found one rule of the design doc that is wrong as written. The doc says that the end slot of
a sealed segment is "the first uncommitted slot". With that end, a Phase 1 that reads the old
grid only below the end chooses a second value for a slot (trace below). The end must be one
above the highest slot where Phase 1 saw a vote in the old grid, or Phase 1 must keep reading
the whole old grid until it retires (then the end bounds nothing).

Pierre's lean towards A holds against the bar: A passes every check with less machinery, and
the upper stack reads the segment table as one decided record. B can reach the same view only
with the log-claim variant, which moves the table into the log and makes each client fold it.

## The two designs

| | Design A: `a/SegmentsA.tla` | Design B: `b/SegmentsB.tla` |
|---|---|---|
| Matchmakers hold | the configurations and one record for each segment: start, end, state | the configurations only |
| A segment ends | at seal: the first leader of the next configuration writes the end once, by compare-and-set | nowhere in decided state; each replica cuts its own chunks |
| Object key | journal + configuration number | journal + first slot + last slot of a chunk |
| Phase 1 reads | each sealed configuration below its end, the open one in full | every configuration that still runs, above the replicas' applied prefix |
| A replica drops its copy | once the record says `offloaded` | plain B: once its object is in the store; variant: once it also applied the winning claim |
| Spec size | 495 lines, 304 without comments, 27 operators | 427 lines, 277 without comments, 24 operators |

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
| 3+. One object for each slot (`OneHome`, not in the issue) | holds | **fails**: two replicas cut different chunks, so two objects hold one slot | holds: only the winning claim is uploaded |
| 4. Recovery reads a bounded number of slots, safely | holds with the end at the highest vote + 1 | holds: above the replicas' applied prefix, on every configuration that still runs | holds |
| 5. A tail read is linearizable across a reshape, with a stale cached view (`ReadFresh`) | holds | holds only if the read reads every configuration that still runs | not run (same read as plain B) |
| 6. One decided view of where each slot lives, offloaded slots included (`Visible`) | holds: the record | **fails** | holds: the winning claim in the log |
| 7. Offload, retire and trim never stop a leader that can write (`TailNeverBlocked`) | holds | holds | holds |

A also passes `EndOnce` (the end is written once), `SealedFrozen` (nothing is chosen in a sealed
configuration at or above its end), `TableSound` (the sealed records do not overlap the start of
a later one) and `CursorSafe`.

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

1. **A tail read reads every configuration that still runs**, as Phase 1 does, and asks f+1
   replicas for their applied prefix. Control `reads_view`: a reader that reads only its cached
   configuration (as in A) misses a slot that the old grid chose and the replicas did not apply
   yet. In A the start of the cached segment tells the reader that the slots below it are sealed;
   B has no such start.
2. **For check 6, the log-claim variant.** A replica asks the leader to append a claim for its
   next chunk; the first chosen claim that covers a slot wins; a replica uploads only a winning
   claim, and drops its copy only once it applied that claim and the object is in the store.
   Plain B has no decided record of an offload (`plain_visible`): a client must list the object
   store, and a listing is not decided state, so two clients that list at different times see
   different chunks (`plain_onehome`: two objects hold one slot).

Both designs share three rules, each with a control in each folder: retire a configuration only
when every slot with a vote in it is applied by f+1 replicas or chosen in a later configuration
(`retire_early` fails check 2), a tail read answers "sealed" from an acceptor that promised a
later configuration (`read_nofence` fails check 5), and trim stops at the consumer cursor
(`trim_nocheck`).

## What B costs on Pierre's point

Pierre's point: a segment that lives only in S3 must still be declared, so that the grid
controller, placement, the offload policy and the readers see it as decided state.

- In A, one read of a matchmaker quorum gives the whole table: each segment, its range, its
  state and its object key. Two clients that read the same table version see the same view.
- In plain B, nothing decided declares an offloaded range. A client has to list the object
  store and ask the replicas. Two clients can disagree, and two objects can hold one slot.
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
the values and the replicas. TLC 2.19.

| Configuration | Ballots | A distinct states | B distinct states |
|---|---|---|---|
| Every rule on, storage on (A `safety`, B `plain`) | 2 | 9,650 | 227,778 |
| Every rule on, storage on (A `safety`, B `plain_long`) | 3 | 298,648 (30 s) | 7,628,098 (27 min) |
| B with log claims (`claims`, `claims_long`) | 2 / 3 | | 96,713 / 3,039,127 (25 min) |
| Tail reader, storage off (`reads`, `reads_long`) | 2 / 3 | 58,229 / 1,675,784 | 65,074 / 1,950,482 |
| Bounded Phase 1 with the end at the highest vote + 1 (`end_voted_below`) | 3 | 298,648 | |
| The same, three acceptors for each grid, no storage (`end_voted_below_three`) | 2 | 1,195,666 (5 min) | |

B is about 25 times larger than A at the same size, because each replica cuts its chunks where
it likes and nothing orders the cuts; A has one object for each segment and the seal orders the
record. Three runs did not finish, and they are not in `checks.txt`: A with three acceptors for
each grid and three ballots (19.4 million distinct states in 56 minutes, no violation, the queue
still growing), and B plain and B with claims with three acceptors for each grid (45 minutes on
one worker each).

The CI set of both folders runs in about 5 minutes on 2 workers, and in CI each folder is its
own job of about 2 minutes. The `long` checks run with `scripts/check-tla.sh --all`.

## Limits of the experiment

- One acceptor for each configuration in the CI set. The three-acceptor checks of A are `long`,
  with storage off and one reshape; the full A and B with three acceptors do not finish.
- Two configurations only, so plan rule 1 (one in force plus at most one successor) is never
  stressed by a third reshape.
- The matchmakers are one atomic register; their quorum and handover are in other models.
- The object store is atomic: a put and a delete are single steps. A late upload after trim
  (review problem "offload safety" of the design doc) is not modelled.
- Grid quorums are not modelled here (`specs/tla/grid/` covers them); a configuration is a
  majority.

## How to run

    nix develop .#tla --command scripts/check-tla.sh experiments/segments/a experiments/segments/b
    nix develop .#tla --command scripts/check-tla.sh --all experiments/segments/a   # the long checks too

Each folder has its `checks.txt`, as every model of `specs/tla/` has.
