---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
trace: quorums
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

## Step through

Six acceptors, 2 rows and 3 columns. Each slot goes to one column. Step through a run of
`paros-core` in your browser, then try the "what if" runs: a dead acceptor stops only the slots
of its own column.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

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
  `membership/quorum_system.rs`; `QuorumSystem::partitions` builds it, `column_of` picks the
  column of a slot.
- The status of a journal always shows its partition count.
- The [Quorums](../../paxos/quorums/) page compares majority, flexible and grid quorums.
