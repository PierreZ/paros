---
layout: ../../layouts/Doc.astro
kicker: Journal
section: journal/
status: journal
title: The controller
description: Each journal has one controller. It decides when and where to heal; the guards of the register decide whether it is safe. In every other case it holds and alerts.
---

## The problem

A machine fails, and a journal loses an acceptor or a replica. Something must notice and ask
for a new grid. But that something can be slow, can crash in the middle, can be deposed and keep
calling, and can compute the wrong plan. None of this may break the journal.

## The rule

**The controller decides when and where; the protocol decides whether it is safe** (§J6 of
[`06-controller.md`](https://github.com/PierreZ/paros/blob/main/docs/journal/06-controller.md)).
Each journal has one controller, its own role, on stateless machines. Its only effect on the
journal is one call to the [register](../matchmakers/):

<pre><code>reshape(expected configuration, new grid, controller term)</code></pre>

It never orders a write, never decides the safety of a retire, never calls `SetLeader` or
`Truncate`, and never moves data itself. A wrong measure can make it ask at the wrong time. It
can never make an unsafe step pass.

## The fenced reshape

A candidate reads the term `t` in the register and writes `t + 1` by compare-and-set. Then the
register checks each call (§J6.1,
[`grid-controller/`](https://github.com/PierreZ/paros/tree/main/specs/tla/grid-controller)). Each
guard has a control that TLC breaks:

| Guard | Without it |
|---|---|
| The call carries the register's term | A deposed controller that keeps calling opens a segment. |
| The expected configuration is the newest | One configuration opens two segments. |
| No reshape while a segment is not retired | More than one old grid stays live. |
| The new grid is a v1 grid | A grid that can never come into force blocks every later heal. |
| A retry of a taken call gets "ok" again | A lost "ok" tells the controller "refused" for a reshape that took place. |

The model runs two controllers, a race of candidates, a crash between plan and call, a takeover,
a lost reply and an acceptor that fails after a plan named it. After the faults stop, every call
gets an answer, and every configuration that a reshape opened comes into force.

## What v1 does

- **Heal.** A reshape that replaces a lost acceptor, or the re-replication of a sealed segment
  whose replica set lost a member.
- **Scrub.** A read of every copy, with a repair through the protocol when a checksum fails (the
  CTRL rules of [Quorums and failures](../quorums-and-failures/)).
- **Hold and alert** in every other case: a zone loss, a column below quorum, data loss.

There is no roll, no offload, no service level objective and no scaling from load in v1. The
roles publish their metrics to the controller. Which measure drives which action, past heal, is
decided after tests (decided on 2026-11-20).

## Journal health

Every part of a journal has a named measure, and every measure has a fixed action (§J6.2). A
**P** measure is a protocol fact: a pure function of the configuration and the live members,
through the quorum predicates of `membership/`. An **O** measure is a count or a time from the
roles. In v1 only the O measures of liveness and of the controller's own pace make it act.

The spare of a column is how many more members it can lose. On a v1 grid it is
`live − (F + 1)`. The grid part of the health is one of these states. Each set of live members
falls in exactly one, and the model checks that each action can run in its state
([`health/`](https://github.com/PierreZ/paros/tree/main/specs/tla/health)):

| State | When | Action |
|---|---|---|
| <span class="chip-status done">Healthy</span> | every column has a spare | none |
| <span class="chip-status accent">AtRisk</span> | a column has no spare; a placement meets the zone rule | one heal reshape |
| <span class="chip-status accent">AtRisk-Unplaceable</span> | as AtRisk, with no placement that meets the zone rule | hold and alert |
| <span class="chip-status gap">WritesDegraded</span> | no election possible; a column writes; the leader is live | hold and alert in v1 |
| <span class="chip-status gap">ElectionBlocked</span> | no election possible; a column writes; no leader | hold and page |
| <span class="chip-status gap">Stopped</span> | no election possible; no column writes | hold and page |

A seventh state, WritesBlocked, needs a grid that v1 does not take. Three flags sit beside the
state: Reshaping (no second reshape until the retire), ReshapeStalled (alert with the cause) and
ControlFrozen (the matchmakers are below a majority).

**A column lost for good is beyond the contract.** v1 has no unsafe recover. A slot that only
that column held is reported as data loss, and it is never filled with a `Noop`.

## Open

The cap of the rate limiter, the minimum interval between two reshapes and the suspicion
windows have no number yet. No number is set until a decision or a source gives one; #1328
decides them.

## In paros

- Not built yet: `paros-journal-control`, the pure `decide` of the controller and its role
  (#1328), and its walk in `journal-sim` with two controllers of different terms (#1261).
- [`paros-journal`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-journal):
  `OpeningCall`, the identity of the reshape call that the record keeps, so that a retry gets
  "ok" ([`segment.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-journal/src/segment.rs)).
- The tests that `decide` gets: property tests, a second `decide` with no shared code, walks
  trace-checked against `grid-controller/`, and a coverage gate on every refusal (§J6.2, §J7.5).
