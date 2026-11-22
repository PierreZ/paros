---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
lab: register
trace: journal-seal
title: Segments
description: A segment is the run of slots of one acceptor grid. A reshape opens one, the first leader of the new grid seals the old one, and the boundary state carries the journal across.
---

## The problem

A journal must change its acceptors while it takes writes: a disk fails, and a new machine
takes its place. [Matchmaker Paxos](../../paxos/matchmaker/) does this with a new ballot and a
new configuration. But the journal also needs to know, for each range of slots, which machines
hold it: to route a read, to free the old acceptors, and to trim. Paxos inside each segment
orders the data; the segment records order the segments.

## A chain of segments

A segment is the run of slots of one configuration: one acceptor grid and one replica set. The
journal's matchmakers keep one record for each segment, from the reshape that opens it to its
trim (design A of #1213, §J3.1). In v1, only a reshape opens a segment. A leader change opens
none, and there is no roll on size or age.

<div class="arch" role="img" aria-label="A chain of three segments. Segment 1 holds slots 1 to 40 and is retired. Segment 2 holds slots 41 to 97 and is sealed. Segment 3 starts at slot 98 and is in force. Each record holds its grid, its replica set, its seal end and its boundary state.">
  <div class="row three">
    <div class="box side">segment 1 <small>slots 1 – 40 · retired</small></div>
    <div class="box side">segment 2 <small>slots 41 – 97 · sealed</small></div>
    <div class="box lead">segment 3 <small>slots 98 – … · in force</small></div>
  </div>
  <div class="arrow">▲ <small>one record each, at the journal's matchmakers</small></div>
  <div class="box">record <small>grid · replica set · seal end · boundary state · state</small></div>
</div>

The slot numbers are an example.

## The life of a segment

<span class="chip-status">pending</span> → <span class="chip-status accent">in force</span> →
<span class="chip-status">sealed</span> → <span class="chip-status done">retired</span> →
<span class="chip-status">trimmed</span>

1. **Open.** The [controller](../controller/)'s fenced `reshape` call writes a new record, in
   state `pending`. The call names the new grid and its replica set in one write.
2. **Seal.** A leader of the new configuration runs Phase 1 over the old grid and the new grid.
   Then it writes the end of the old segment, once, by compare-and-set. The old segment is
   sealed, and the new one is in force.
3. **Boundary.** The first replica of the old set that applied every slot writes the boundary
   state into the old record: `next_seq`, `first_seq` and the writer uuid.
4. **Retire.** When `F + 1` replicas of the set hold the whole segment and the boundary is
   written, the leader marks the record retired. Only then do the old acceptors stop.
5. **Trim.** When a decided `Truncate` puts the floor over the whole segment, the record becomes
   trimmed. It never comes back.

A sealed segment whose replica set lost a member is copied to a new member (re-replication, in
v1). Offloaded is a state of the record, prepared for later: in v1 nothing sets it.

## Where the seal ends

The end is **one above the highest slot where the sealer's Phase 1 saw a vote in the old
grid**, not the first slot that is not committed. Then the sealer writes each slot below the end
in its own grid: the vote that Phase 1 saw, or a `Noop` where it saw none. A later Phase 1 reads
the old grid only below the end.

Why not the first slot that is not committed? A slot above it can be chosen with no leader
knowing it yet. A Phase 1 that reads the old grid only below that end misses the vote, and a
second value is chosen at that slot. TLC finds it in the segments experiment
(`specs/tla/experiments/segments/a/`). A CTRL faulty record, a vote that kept its ballot and lost
its value, counts as a vote for the end too
([`seal-ctrl/`](https://github.com/PierreZ/paros/tree/main/specs/tla/seal-ctrl)).

## The boundary state

Each segment has its own replica set, and a write reaches only that set. So the replicas of
segment 3 never see the slots of segment 2. They start from the **boundary state**, the journal
state at the seal: `next_seq`, the floor `first_seq`, and the leader uuid (or `None` for a
multi-writer journal). The whole boundary state is in the segment record, written once
([`seal-chain/`](https://github.com/PierreZ/paros/tree/main/specs/tla/seal-chain)).

A replica of the next segment starts from the record of the segment **just before it**, never
from the newest record. With the newest record, a writer that a `SetLeader` of the old segment
deposed can write again (control `record_latest`). An empty segment, sealed before its first
slot, passes its predecessor's boundary on unchanged, so two seals in a row need nothing more.

A write sent again across a seal gets `Below`, and the client asks the old segment with
`Resolve`: the [calls](../calls/) page shows the steps.

## Truncate across segments

`Truncate` stays a decided command of the log (§J3.2,
[`segment-truncate/`](https://github.com/PierreZ/paros/tree/main/specs/tla/segment-truncate)).
Segments add these rules:

- **The boundary holds `first_seq`.** A floor decided in the old segment is in no slot that the
  next segment applies.
- **A read takes its floor from the segment in force.** A replica of a sealed segment folds only
  up to the seal, so it misses every later `Truncate`.
- **A segment is trimmed only when it is sealed and the floor is decided over it**, never on the
  intent to truncate.
- **The acceptors of a segment compact only to a slot that `F + 1` of its replicas compacted
  to.** Else nobody can fold the segment, and the next segment never starts.
- **A laggard jumps** to a peer's trim point, and never fetches a slot of a trimmed segment.

## Reads across a reshape

A reader that holds the record of an old segment asks the old grid. Each acceptor's answer
carries its read fence: its promise and the ballot it believes in force. An answer above the
record ends the read, so a read never misses a slot that the new grid chose. The journal keeps
this fence of the code, and a read does not read the segment records at a matchmaker quorum
(§J3.3, `specs/tla/experiments/segments/reads/`).

## Step through

The runs below are recorded from the journal roles of `paros-journal`: a client with leader A,
the controller, the matchmaker register, two leaders (proposer 5 of the old grid, proposer 6 of the new one),
four acceptors and one replica for each segment. Acceptors 1, 2 and 3 form the grid of segment
1; a reshape opens segment 2 on acceptors 1, 2 and 4. Each step is one message that the driver
delivers to the real role; the seal end, the boundary state and each answer come from the role
that sent it.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## Your turn

Move the records of the segments by hand. Each button is one compare-and-set on the value of the
register, `RegisterValue::apply` of `paros-journal`, compiled to wasm: its guard runs, and a
refused move names its refusal. Here each move is a write already done at a majority; the
[Matchmakers](../matchmakers/#your-turn) page runs the reads and writes behind it. In this lab
each segment takes four slots before its seal, one record per slot, and the controller holds
term 1. A seal puts the next segment in force in the same write, so `put_in_force` is no button
of its own.

<div data-record-lab="records-retire records-trim records-third"></div>

## In paros

- [`paros-journal`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal):
  `SegmentRecord`, `SegmentState` and the guard of each step (`put_in_force`, `seal`,
  `write_boundary`, `retire`, `trim`) in [`segment.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/segment.rs);
  `BoundaryState` and `JournalState::from_boundary` in [`fold.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/fold.rs).
- [`journal-sim`](https://github.com/PierreZ/paros/tree/main/crates/sim/journal-sim) walks the
  fold over a chain of segments; the replica of the segment in force crashes and folds again
  from the boundary state.
- Rules: §J3.1 to §J3.3 of [`03-segments-and-records.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/03-segments-and-records.md).
  The composed model is [`journal-state/`](https://github.com/PierreZ/paros/tree/main/specs/tla/journal-state).
- [`paros-wasm`](https://github.com/PierreZ/paros/tree/main/crates/support/paros-wasm), feature
  `journal`: the exercises above, a driver of the register value with no rule of its own.
- [`paros-wasm`](https://github.com/PierreZ/paros/tree/main/crates/support/paros-wasm), feature
  `journal`: the step-through above, recorded from `JournalLeader`, `JournalAcceptor` and
  `SegmentReplica` of [`roles`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal/src/roles)
  and the register value, in [`journal/seal.rs`](https://github.com/PierreZ/paros/blob/main/crates/support/paros-wasm/src/journal/seal.rs).
- Not built yet: the retire and the trim of the journal roles (#1326).
