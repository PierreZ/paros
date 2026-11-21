---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
title: Quorums and failures
description: No step of the journal waits for every member of a group. With F members down in each group, every step completes; past that, the journal holds and alerts.
---

## The promise

With `F` acceptors down in every column of the old and the new grid, `F` matchmakers down in
the journal's set, and `F` replicas down in a segment's replica set, **every step of the journal
completes**. More failures than that are outside the promise: the journal holds and alerts, and
it never takes an unsafe step to go on (§J5 of
[`05-quorums-and-failures.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/05-quorums-and-failures.md)).

## Quorum everywhere

A FoundationDB commit waits for every log server. One slow server then slows every commit. The
journal does not do that: **each step waits for a quorum only** (decided on 2026-11-20).

| Step | It waits for |
|---|---|
| Write, `Truncate` | `F + 1` acceptors of one column; a stuck round moves to the next column |
| Election | a matchmaker majority, then `F + 1` acceptors of every column of each configuration it must read |
| Leaderless read | `F + 1` acceptors of every column of the configuration in force |
| Seal | a Phase-1 quorum of the old grid, then a matchmaker majority |
| Retire | `F + 1` replicas of the segment, then a matchmaker majority |
| Controller election, reshape | a matchmaker majority |
| Matchmaker handover | a majority of the old set, then a majority of the new set |
| Heal of a sealed segment | one replica that holds it whole, then a matchmaker majority |

[`quorum-everywhere/`](https://github.com/PierreZ/paros/tree/main/specs/tla/quorum-everywhere)
checks every row: each step completes with `F` down, and blocks with one member more. A control
that waits for every member, as the FoundationDB commit does, blocks with `F` down. Two steps have
no row yet, the boundary write and the acceptor compaction: they are **Not proved** (#1318).

## The v1 grids

A grid meets the rule for `F` exactly when `F + 1 <= acks <= rows - F`
([`grid/`](https://github.com/PierreZ/paros/tree/main/specs/tla/grid)). In v1 the journal takes
two grids only: the partitioned grid, `rows = 2F + 1` and `acks = F + 1` with one or more
columns, and the majority, which is that grid with one column. The default is one column of three
acceptors, `F = 1`. A full grid (`acks = rows`) needs a whole column for each write, so the
journal does not offer it.

<div class="arch" role="img" aria-label="A partitioned grid with F equal to 1: two columns of three acceptors. A write needs two acceptors of one column. An election needs two acceptors of every column.">
  <div class="row">
    <div class="box">column 1 <small>a · b · c</small></div>
    <div class="box">column 2 <small>d · e · f</small></div>
  </div>
  <div class="arrow"><small>write: 2 of one column · election: 2 of every column</small></div>
</div>

The [Grid quorums](../../engineering/grid-quorums/) page lets you stop acceptors and see what
still works, on `paros-consensus` in your browser.

## The counts of `F + 1`

Three steps count `F + 1` members of a replica set, so that with `F` of them down one good copy
is still live (§J5.2):

- **Retire** a sealed segment when `F + 1` replicas hold it whole
  ([`segment-replicas/`](https://github.com/PierreZ/paros/tree/main/specs/tla/segment-replicas)).
- **Compact** a segment's acceptors only to a slot that `F + 1` replicas compacted to
  ([`segment-truncate/`](https://github.com/PierreZ/paros/tree/main/specs/tla/segment-truncate)).
- **Re-replicate** a sealed segment: a new replica counts only on its own complete copy, taken
  from a member that holds the whole segment
  ([`re-replication/`](https://github.com/PierreZ/paros/tree/main/specs/tla/re-replication)).

## Damage: CTRL

A disk can return a record that fails its checksum. CTRL keeps the ballot of such a record and
marks its value lost: a **faulty record**. The journal repairs it through the protocol, whoever
finds it: a boot, or the controller's scrub (§J5.3, [`ctrl-grid/`](https://github.com/PierreZ/paros/tree/main/specs/tla/ctrl-grid),
[`ctrl-segments/`](https://github.com/PierreZ/paros/tree/main/specs/tla/ctrl-segments)):

- **A scrub and a boot give the same faulty record.** A scrub that drops the record, so that the
  slot looks empty, chooses two values.
- **The tally asks a Phase-1 quorum of every configuration, never a count.** A slot it cannot
  prove goes to the repair probe; it is never filled with a `Noop` on a guess.
- **A repair writes a value known to be chosen, in one write.** A value copied from a peer's vote
  can lose a chosen value. A delete, then a write, forgets the repair at a restart.
- **A damaged copy is one of the `F` failed copies** until its repair, so `F + 1` holders always
  leave one good copy. A segment with damage that is not repaired does not retire: a margin
  that the journal keeps (decided on 2026-11-20).
- **The seal end counts a faulty vote** ([Segments](../segments/)).

## Zones

With `F = 1` over three zones, each row of the grid is one zone. So a zone loss takes one
acceptor from every column at once. No placement over two zones survives the loss of one of them,
on any grid ([`health/`](https://github.com/PierreZ/paros/tree/main/specs/tla/health),
`NoPlacement`). After a zone loss the journal serves, holds its configuration, and the controller
alerts once for the zone (§J5.4).

## A column below quorum

`F + 1` losses in one column are outside the promise. Writes go on in the other columns, but no
new leader can be elected. In v1 the journal holds and alerts. The Phase-1 bypass that could heal
it is designed and checked, and it is out of v1 (§J5.5,
[`degraded-reshape/`](https://github.com/PierreZ/paros/tree/main/specs/tla/degraded-reshape)).

## In paros

- `paros-consensus`: `QuorumSystem` and the predicates `has_phase1_quorum` and
  `has_phase2_quorum_in` ([`membership/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/membership));
  the faulty records and the tri-state tally on plain Multi-Paxos.
- [`paros-journal`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal): `Grid`
  takes only `rows = 2F + 1`, `acks = F + 1`, and `retire` counts `F + 1` holders
  ([`segment.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/segment.rs)).
