---
layout: ../../layouts/Doc.astro
kicker: parosd
section: parosd/
status: parosd
title: One shape at every level
description: The universe, a cell and a tenant are each a control journal, a pure fold and one coordinator elected by SetLeader. The levels talk only through records.
---

## The problem

A service has state about itself: which machines it has, where each journal runs, which tenant
lives in which cell. Someone must write that state, and only one writer at a time. The usual
answer is an external coordination service, or a special election with its own rules. Each of
these is one more system to prove, and one more system that can be down.

## The rule

**Each level is one control journal, one pure fold and one coordinator** (goal 4 of
[§1.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/01-goal.md#11-the-goals-and-the-root-rules);
[§3.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-1-cell-tenant-and-bootstrap.md#31-the-cell-tenant-and-bootstrap),
[§3.3](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-3-coordinators-and-placement.md#33-control-planes-recovery-and-placement)).
The control journal is an ordinary journal of the service, written with `Write` like any other.
The fold reads it to the tail and gives the state. The coordinator is the one writer of that
journal. The levels talk only through records with idempotency ids, never through a call that
needs the other level alive.

<div class="arch" role="img" aria-label="Three levels of one shape. The universe, a cell and a tenant each have a control journal, a pure fold and one coordinator. The universe holds the directory, the cell holds the machines and the bookings, the tenant holds its catalog. The levels talk only through records.">
  <div class="box">universe <small>directory: tenants · cells · buckets · moves</small></div>
  <div class="arrow">▲▼ <small>records with idempotency ids</small></div>
  <div class="box">cell <small>machines · admission · bookings · placement · repair · balance</small></div>
  <div class="arrow">▲▼ <small>records with idempotency ids</small></div>
  <div class="box">tenant <small>catalog: namespaces · journals · their configuration</small></div>
  <div class="arrow"><small>each one:</small></div>
  <div class="row three">
    <div class="box side">control journal</div>
    <div class="box side">pure fold</div>
    <div class="box lead">one coordinator</div>
  </div>
</div>

## The term is `SetLeader`

A coordinator term is won by one compare-and-set on its control journal:
`SetLeader(new, old)`. The first candidate that replaces the old uuid wins. Nothing else is on
that path: no election journal, no registry entry, no address. Each write of the coordinator
renews the term, and the first refused write ends it. A coordinator hands on with
`SetLeader(next, me)` (§3.3, rule 3).

This is the same call that a tenant uses for its own journals. The control plane gets no call
that a tenant does not have (goal 3), so the election is a library of layer 3 that any program
can use ([Layers](../layers/)). The
multi-writer mode of a journal has no leader, so the election refuses a
journal in that mode (#1420 (paros-election)).

Two fences, two layers: the Paxos ballot says which machine runs the consensus of the journal,
and the leader uuid says which coordinator may write. A coordinator also sends orders outside its
journal, to a machine or to the matchmakers of a data journal. Each order carries its term and
names the epoch of its target, and the target refuses an old one. A deposed coordinator can only
repeat a step it already recorded.

## Each term rebuilds

Each term starts with one recovery pass: fence, rebuild the live view from fresh proof, then
drive one routine to the target (§3.3, rule 4). The registry and the bookings are hints, not
facts. A step that fails ends the pass, and the pass starts again. Nothing waits on a broken
step.

A child keeps serving while its parent is down. With every coordinator held down, each healthy
journal still serves writes, reads and a leader change. Only new tenants, new bookings and moves
wait (static stability, §3.3).

## The info table: a hint

The fold is the truth, but a reader that must find a role now cannot fold a journal first. So
the holder of the term of each level keeps a **transient table of references**, with a
generation, and pushes it (§4.4, the equivalent of FoundationDB's `ServerDBInfo`):

| Level | The table holds |
|---|---|
| Universe | the cells |
| Cell | the machines: each one says "I am up", with its address and its incarnation |
| Tenant | the roles of its journals: each machine says which roles of that tenant it hosts |

A reader long-polls with the generation it knows. It takes a table only from the term that it
read in the control journal, and only with a newer generation. Each term builds the table again
from registrations. No safety rests on it: a refused reference makes the caller ask the machine
"which reference serves this id now" ([Machines](../machines/)). The exact fields are **Open**
(#1425 (info tables)).

## In paros

The kit of one shape (the term, renewals, the recovery pass, a fold with a checkpoint, request
and outcome records) and one crate for each level are in #1409 (build the layered parosd). The
model `cell-control` proves rules 3 and 4; they are **not proved** until its gate is green
([§1.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/01-goal.md#11-the-goals-and-the-root-rules)).
Who sits on which level: [Machines](../machines/) for the cell, [The universe](../universe/) for
the directory.
