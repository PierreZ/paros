---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
widget: true
title: Grid quorums
description: A journal can spread its acceptors over partitions. A write touches one partition; an election touches all of them. More acceptors, and each write still costs a majority of one small group.
---

## The problem

With a plain majority, each write goes to a majority of all acceptors. Add acceptors to share
the load, and each write gets more expensive, not cheaper.

## The technique

Lay the acceptors out in a grid. Each column is a **partition** of `2F+1` acceptors. Slot `s`
goes to column `s mod P`.

- **A write** needs a majority of its own partition: `F+1` acceptors.
- **An election** and a tail read need a majority of **every** partition.
- **Safe by construction:** inside one column, a write majority and an election majority share
  an acceptor. An election touches every column, so it meets every write.

So the write load spreads over `P` partitions, and each write still waits for `F+1` syncs. The
election pays: it must reach every partition. Elections are rare; writes are not.

## Stop some acceptors

Each column is a partition. Click an acceptor to stop it, and click it again to start it. The
answer comes from `paros-core` in your browser: the same `has_phase1_quorum` and
`has_phase2_quorum_in` that a leader asks before it counts a vote. One stopped acceptor per
partition stops nothing. A partition that loses its majority stops its own slots and every
election, and the other partitions still take writes.

<div data-widget="quorums"><p class="aside">This demo needs JavaScript and paros-core compiled to wasm.</p></div>

The [Quorums](../../paxos/quorums/) page steps through a grid run message by message.

## When a grid helps

A grid helps when each partition has its own disks and CPUs. On one machine, where all acceptors
share one disk, a plain majority is faster: the grid adds work and saves nothing. So the
default is one partition, and the partitions are a choice per journal.

## In paros

```sh
# Survive 1 failed machine in each of 3 partitions: 3 × 3 = 9 acceptors.
parosctl journal create acme orders --survive machines=1 --partitions 3
```

- `paros-core`: `QuorumSystem::Grid { rows, cols, acks }` in
  [`membership/quorum_system.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/membership/quorum_system.rs); `QuorumSystem::partitions` builds it, `column_of` picks the
  column of a slot.
- The status of a journal always shows its partition count.
- The [Quorums](../../paxos/quorums/) page compares majority, flexible and grid quorums.
