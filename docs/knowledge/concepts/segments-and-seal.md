# Segments and the seal

Group: On the log. Status: verified 2026-11-22.

The journal v1 runs **one** Multi-Paxos log across a chain of **segments**. A segment is the run
of slots that one configuration (one acceptor grid and one replica set) holds. The chain lives
in one compare-and-set register at the journal's matchmakers: one record per segment, with its
start, its end, its grid and its state. A reshape opens a segment; the first leader of the new
configuration **seals** the old one by writing its end slot once. This is the shape of Delos's
VirtualLog, a chain of Loglets in a MetaStore register ([Delos §3.1–§3.3]), with one difference:
in paros, Paxos orders the slots inside a segment, and a leader change opens no segment.

The rules are in `docs/journal/03-segments-and-records.md` (§J3, §J3.1, §J3.4); this card
explains them and maps them to the code. Do not copy a rule from here: link to §J3.

## 1. Problem

A journal must change its configuration (a failed member, a move to other machines) and keep
one ordered log. Matchmaker Paxos ([matchmaking](matchmaking.md)) does this inside one log: each
ballot has a configuration, and a new leader runs Phase 1 over every configuration it may need.
Two costs follow. Every replica executes every write of the journal, so the replicas bound the
write throughput. And the history of configurations grows until GC removes them, with no unit
that says "this range of slots lives on these machines".

Delos makes the range explicit: the VirtualLog maps ranges of the address space to Loglets, and
only the last Loglet takes appends ([Delos §3.1]). Each segment then has its own servers, a
sealed segment can be rebuilt, moved or trimmed with no stop of the tail ([Delos §3.2], "Only a
conflicting chain needs a seal"), and readers route a position to the segment that holds it.

## 2. Model

- The model of Multi-Paxos with grid quorums and matchmakers: crash-recovery processes with
  durable storage, an asynchronous network that loses, duplicates and reorders messages.
- `F` failures per column of the grid and per replica set; every step waits for a quorum only
  (`docs/journal/05-quorums-and-failures.md` §J5.1).
- The register is a compare-and-set register at a majority of the journal's matchmakers, as the
  Delos MetaStore is "one versioned register with a conditional write" ([Delos §3.3]).
- No clock for safety.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Matchmakers (the register) | one `SegmentRecord` per segment: start, end, grid, replica set, `SegmentState`, the boundary state, and the `OpeningCall` of the reshape that opened it | yes, at a majority |
| Grid controller | its term; it opens a segment by the fenced `reshape` call (`docs/journal/06-controller.md` §J6.1) | the term is in the register |
| First leader of a segment | the end it computed from its Phase 1 over the old grid | the end, once, in the old record |
| Replicas of a segment | the slots of their segment and its fold; the first that applied every slot writes the `BoundaryState` | yes |
| Acceptors of a sealed grid | their votes, until the segment retires | yes |

`SegmentState` (`paros_journal::segment`): `Pending` (the reshape announced it), `InForce`
(leaders write here), `Sealed` (no new slot), `Retired` (the old acceptors stop), `Offloaded`
(after v1) and `Trimmed`.

## 4. Normal flow

1. **Open.** The grid controller's `reshape` call writes a new record in state `Pending`, by
   compare-and-set on the register (§J3.4).
2. **Phase 1 over the old grid.** A candidate in the new configuration runs Phase 1 in the new
   grid and in the old one (`JournalLeader`, `roles/leader/phase1.rs`).
3. **Seal.** The first leader that wins writes the end of the old segment once: **one above the
   highest slot where its Phase 1 saw a vote in the old grid**, a CTRL faulty vote included
   (`SegmentRecord::seal`, §J3.1). The new segment is then in force.
4. **Fill.** The leader proposes again, in its own grid, each vote below the end that the
   replicas did not apply, and a `Noop` at each slot with no vote (the election gap fill of the
   core).
5. **Boundary.** The first replica of the old set that applied every slot writes the boundary
   state (`next_seq`, `first_seq`, the writer uuid) into the old record; the next segment folds
   from it.
6. **Retire.** Once `F + 1` replicas of the old set hold the segment and the boundary is
   written, the leader marks the record `Retired`, and only then do the old acceptors stop.

```mermaid
sequenceDiagram
    participant G as Grid controller
    participant R as Register (matchmakers)
    participant L as First leader (new grid)
    participant O as Old grid
    participant P as Old replicas
    G->>R: reshape: open segment 2 (Pending)
    L->>O: Phase 1 (read the old grid)
    O->>L: promises, highest vote at slot 41
    L->>R: seal segment 1, end = 42 (CAS, once)
    Note over L: segment 2 in force; re-propose votes below 42, Noop the holes
    P->>R: boundary state of segment 1 (next_seq, first_seq, uuid)
    L->>R: retire segment 1 (after F + 1 replicas hold it)
```

## 5. Recovery flow

- **Two first leaders race.** The end is a compare-and-set from `InForce`: one wins. The other
  sees the end that won, or the register refuses its write (`ChangeRefusal::Record`, which
  carries a `RecordRefusal`), and it stops (tests `a_first_leader_whose_end_lost_stops` and
  `a_refused_seal_stops_the_leader`). Delos has the same race on its MetaStore: "If many clients
  race, at most one wins" ([Delos §3.2]).
- **A leader stops after the seal.** The next leader reads the record, finds the end written, and
  reads the old grid only below it. Delos instead "rolls forward" a half-done reconfiguration
  with a default chain ([Delos §3.2]); in paros the end and the new segment are already in the
  register, so nothing is half done.
- **A vote lands in the old grid after the seal.** It is above the end, so it is not part of the
  journal. Delos calls these "zombie appends" and makes the same argument: "the chain fixes the
  segment boundary" ([Delos §3.4]).
- **A replica of a sealed segment is lost.** The controller re-replicates it from a holder to a
  new member, by compare-and-set on the version of the register (§J3.1, #1314
  (re-replication of a sealed segment's replica set)), the analogue of Delos's `reconfigModify`
  on a sealed Loglet ([Delos §3.1]).
- **The matchmaker set is handed over.** The records cross the handover as one register fenced
  by the matchmaker generation (§J3.1, `specs/tla/segment-records/`).

## 6. Why it is safe

The end is one above the highest vote that a Phase-1 quorum of the old grid reported, so every
slot that the old grid could have chosen lies below the end. Every later Phase 1 reads the old
grid **only below the end**, and the sealing leader has already proposed again, in its own grid,
each value below the end; so no ballot can choose a second value at a slot of the old grid. A
CTRL faulty vote counts for the end: an end of readable votes only can land at or below a
chosen slot whose top copy rotted, and a later leader that reads below that end chooses a
second value there (control `end_readable` of `specs/tla/seal-ctrl/`, #1313 (the seal end
against a CTRL faulty record)). The end is written once (`SegmentRecord::seal` refuses a
segment that is not `InForce`), so all readers of the register agree on it.

A leader's writes are fenced by its promise quorum, never by the version of the register, so a
change of a sealed segment (retire, re-replicate, trim) never stops the tail. Delos makes the
same choice: `reconfigModify` and `reconfigTruncate` "do not move the tail, so they need no seal"
([Delos §3.2]).

A replica of the next segment starts from the boundary state of the record **just before** its
segment, never from the newest record written: a deposed writer can write the newest one
(control `record_latest` of `specs/tla/seal-chain/`, #1303 (two seals in a row)).

## 7. Liveness

- The seal needs a Phase-1 quorum of the old grid and of the new grid, and a matchmaker
  majority; the fill below the end then needs Phase-2 quorums of the new grid. It does not wait for the repair probe of a faulty slot: a blocked slot below the
  end stays in the tally of every later Phase 1 (§J3.1).
- Without the `Noop` gap fill below the end, a slot with no vote is a hole that no ballot can
  write once the old grid is sealed: the replicas stop at it for ever, and retire and trim never
  come (#1233 (segments gap fill)).
- A reshape when a column of the old grid is below quorum needs the Phase-1 bypass
  (`specs/tla/degraded-reshape/`, #1258).

## 8. Costs

- One reshape: one compare-and-set to open, one Phase 1 over the old and the new grid, one
  compare-and-set to seal, one to write the boundary and one to retire. Delos measures its
  seal, `checkTail` and MetaStore write at "10s of ms" in production ([Delos §3.3]).
- A leader change in v1 costs no segment and no register write (Delos pays a seal and a MetaStore
  write for each leader change, [Delos §4.1], "Leader election is a VirtualLog reconfiguration").
- Over the whole journal, the replicas no longer bound write throughput: a write reaches only the
  replica set of its segment (#1271).

## 9. In paros

| What | Where |
|---|---|
| The segment record and its states | `paros-journal/src/segment.rs`: `SegmentRecord` (`seal` and the other compare-and-set steps), `SegmentState`, `SegmentId`, `Grid`, `OpeningCall`, `RecordRefusal` |
| The register that holds the records | `paros-journal/src/register/value.rs`: `RegisterValue`, `Change`, `ChangeRefusal` (re-exported by `register.rs`); its handover in `register/handover.rs` |
| The boundary state and the fold across a seal | `paros-journal/src/fold.rs`: `BoundaryState`, `JournalState`, `Answer::Below` for a resend across a seal |
| The leader's Phase 1 over the segments and its end | `paros-journal/src/roles/leader.rs`: `JournalLeader`, `ReadSegment`; `roles/leader/phase1.rs` (the end) |
| The replica of a segment and its boundary write | `paros-journal/src/roles/replica.rs`: `SegmentReplica` |
| The grid controller that opens segments | `paros-journal-control` (`decide`, the role's fenced `reshape` call) |
| The rules | `docs/journal/03-segments-and-records.md` §J3, §J3.1, §J3.4 |

**Judges today:**

- `journal-sim`'s roles walk (`src/roles.rs`), with oracles that share no code with
  `paros-journal`: "one command is chosen at each slot", "a grid chooses nothing at or above the
  end of its sealed segment", "a boundary state is the fold of the whole log at the segment end",
  and the fold oracles. It must reach "roles walk: a first leader sealed the segment before",
  "roles walk: a first leader lost the seal to another", "roles walk: a Noop filled a slot below
  the segment start", "roles walk: an old grid's vote was chosen again in a new one", "roles
  walk: a replica wrote the boundary state" and "roles walk: a sealed segment retired".
- `journal-sim`'s fold walk (traced against `specs/tla/journal-state/FoldTrace.tla`) and register
  walk (traced against `specs/tla/segment-records/RegisterTrace.tla`).
- The models: `specs/tla/segment-replicas/`, `specs/tla/seal-chain/`, `specs/tla/seal-ctrl/`,
  `specs/tla/segment-records/`, `specs/tla/segment-truncate/`, `specs/tla/re-replication/`,
  `specs/tla/degraded-reshape/` and the composed `specs/tla/journal-state/`; the design
  experiment that chose design A is `specs/tla/experiments/segments/` (#1213).
- Unit tests: `a_first_leader_seals_the_segment_before_above_its_highest_vote`,
  `a_first_leader_whose_end_lost_stops`, `a_refused_seal_stops_the_leader`,
  `the_end_is_at_least_the_first_slot_and_the_start`
  (`paros-journal/src/roles/leader/tests.rs`).

**Divergences from the sources:**

- A Delos Loglet needs no consensus, only a seal; every leader change is a new Loglet
  ([Delos §3.4], §4.1). In paros, Paxos orders the slots inside a segment, and a leader change
  opens no segment (§J3.1). paros does not get "a Loglet needs no consensus", and it does not
  pay a seal for each leader change.
- Delos fixes the end with `checkTail` on the sealed Loglet ([Delos §3.2]). paros fixes it from
  the new leader's Phase 1 over the old grid: one above the highest vote, a faulty vote
  included.
- The Delos MetaStore is one register on a chain of single-slot Paxos instances ([Delos §4]).
  The paros register lives at the journal's matchmakers and crosses their handover; it also
  holds the boundary state, which Delos does not need because its state machine is above the
  log.
- In v1 only a reshape opens a segment: no size or age roll (`docs/journal/01-goal.md` §J1.1),
  and offload is prepared, not built.

## 10. Pitfalls

- **The end is not the first uncommitted slot.** With that end, a Phase 1 that reads the old
  grid only below the end chooses two values for one slot (#1213 experiment, §J3.1).
- **A faulty vote is a vote** for the end (#1313); see §6.
- **Two seals in a row and an empty segment.** The first form put the writer uuid in the
  journal's matchmaker record and a `Fence` at the first slot of the next segment; it needed two
  more rules (`meta_any`, `meta_blank`), the `Fence` slot and a refusal before it (§J3.1). The whole boundary state in the segment record needs none (#1303,
  `specs/tla/seal-chain/`).
- **Act on a write seen done, never on a read.** A replica drops its copy only after a write of
  the mark is done at a majority; a read that shows the mark can be a minority's (control
  `drop_read`, #1275).

## 11. Relatives

- [Matchmaking](matchmaking.md) and [matchmaker generations](matchmaker-generations.md): the
  register lives at the matchmakers and crosses their handover.
- [Grid quorums](grid-quorums.md): the configuration of a segment.
- [Election](election.md): the Phase 1 and the `Noop` gap fill that the seal reuses.
- [Corrupted storage recovery](corrupted-storage-recovery.md): the faulty vote that counts for
  the end.
- [Writer fence](writer-fence.md): the uuid that the boundary state carries across a seal.
- [Truncation](truncation.md): the trim floor in the boundary state, and a trim across
  segments.
- [Replica tier](replica-tier.md): each segment has its own replica set.

## 12. Sources

- [Delos] Balakrishnan et al., *Virtual Consensus in Delos*, OSDI 2020:
  `docs/references/papers/delos-virtual-consensus/transcript.md` §3.1 (the VirtualLog and the
  chain), §3.2 (seal, `checkTail`, conditional write; only a conflicting chain needs a seal),
  §3.3 (the MetaStore), §3.4 (the Loglet; zombie appends), §4 and §4.1 (the MetaStore on
  single-slot Paxos; the NativeLoglet; leader election is a reconfiguration).
- The journal's own rules (the subject of this card, never its only source):
  `docs/journal/03-segments-and-records.md` §J3.1, §J3.4; `docs/journal/10-sources.md`.

[Delos §3.1]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §3.1–§3.3]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §3.2]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §3.3]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §3.4]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §4]: ../../references/papers/delos-virtual-consensus/transcript.md
[Delos §4.1]: ../../references/papers/delos-virtual-consensus/transcript.md
