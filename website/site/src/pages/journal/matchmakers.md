---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
title: Matchmakers and the register
description: Each journal has its own matchmaker set, and the set holds one compare-and-set register with the segment records and the controller term. A step acts only on a write it saw done.
---

## The problem

The segment records decide where each slot lives, when old acceptors may stop, and when a copy
may go. Those facts must survive the loss of machines, and two actors that race (two sealers, two
controllers) must never both win. The journal has no external store for this. It uses its own
matchmakers.

## One matchmaker set per journal

Each journal has its own set of `2F + 1` matchmakers, with majority quorums. The sets are
logical: one matchmaker process hosts the sets of many journals, by journal id. The matchmakers
do the job of [Matchmaker Paxos](../../paxos/matchmaker/): they keep the configuration of each
ballot, and a new leader registers its configuration there before Phase 1. The set itself moves
to new machines by a handover of generations (§J3.4 of
[`03-segments-and-records.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/03-segments-and-records.md)).

## The register

Beside that job, the set holds **one compare-and-set register** for the journal. An operation
reads at a majority of one matchmaker generation, checks its guard on the highest write it read,
and writes at that generation. **It is done when a majority took its write.** This is the
register of [`cas-register/`](https://github.com/PierreZ/paros/tree/main/specs/tla/cas-register),
after FoundationDB's `LocalGenerationReg`.

The register is an **optional role**. The registration matchmaker of the paper stays in
`paros-consensus`, unchanged, for every user of the library. The journal opts into the register
beside it (decided on 2026-11-20). Where its code lives is **Open**: #1325 decides.

## What it holds

| Field | Written by | Guard |
|---|---|---|
| Controller term | a controller candidate | Reads term `t`, writes `t + 1`. Of two candidates that read `t`, one loses. |
| New segment, `pending`, with its grid and replica set | the controller's `reshape` | The term, the expected configuration, every older segment retired, a v1 grid. A retry of a taken call gets "ok" again. |
| Seal end; the new segment in force | the first leader of the new configuration | Once, from "no end". |
| Boundary state | the first replica that applied the whole segment | Once, from "no boundary", all three fields in one write. |
| `retired` | the leader | From sealed, after `F + 1` holders and the boundary. |
| `trimmed` | a replica whose floor passed the segment | From sealed or retired. Never from the open segment. |

Each row has its model and its controls in the table of §J3.4. The segment states are on the
[Segments](../segments/) page.

## Act on a write seen done

A read can show a write that only a minority took. Another majority, or the next generation of
the set, can then return the older value. So **a step that acts on a record acts on a write that
it saw done at a majority, never on a read alone**
([`segment-records/`](https://github.com/PierreZ/paros/tree/main/specs/tla/segment-records),
control `drop_read`):

- The old acceptors stop only after the write of `retired` is done.
- A leader opens its segment, and a replica stops at an end, only on a seal end seen done.
  Two sealers that race write one end.
- A controller acts on its own election write, and on the answer to its own reshape call.

One write may be acted on while it is still partial: the boundary state. It is a function of the
chosen log up to an end that is done, so every write of it carries the same value
(`journal-state/`, `register_bound_read`). The end is the step that must be done.

## The register across a handover

When the matchmaker set hands over to new machines, the register goes with it. The successor
takes the highest write of the stop quorum, and a frozen member takes no write. So every write
that a majority took is in the successor, no record moves back, and a deposed controller's call
fails across the handover too. A successor that copies one member of the stop quorum loses a
write: TLC finds it (controls `close_one`, `close_one_term`, `frozen_serves`).

## Below a majority

With more than `F` matchmakers down, no seal, retire, reshape or controller election can run.
With no reshape in flight, the data plane serves under its leader, and reads go on: a read asks
no matchmaker. Between a seal and its boundary write, acked writes stop until a majority is back.
This window comes only with a heal in v1, and it is accepted for v1 (§J3.4).

## In paros

- [`paros-journal`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal): the
  segment record, its states and the guard of each compare-and-set
  ([`segment.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/segment.rs):
  `SegmentRecord`, `OpeningCall`, `RecordRefusal`).
- `paros-consensus`: the registration matchmaker and its generation handover,
  `Matchmaker` and `MatchmakerReconfigurer`
  ([`matchmaker/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/matchmaker)).
- Not built yet: the register role (#1325).
