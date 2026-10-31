---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: quorums
title: Quorums
description: Which acceptors must answer. Majority, flexible quorums, grids and partitions.
---

## The problem

A majority of `2F+1` acceptors survives `F` failures, but every write touches a majority.
Other quorum shapes move the cost between elections and writes.

## Step through

Six nodes in a grid of 2 rows and 3 columns. A full **row** answers Phase 1. A full
**column** answers Phase 2, and slot `s` goes to column `s mod 3`. So a write touches 2 of 6
acceptors.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**Every Phase 1 quorum must share an acceptor with every Phase 2 quorum.** Every row crosses
every column, so the grid is safe. A column with a dead node cannot choose its slots.

## In paros

| Shape | Phase 1 (election, read) | Phase 2 (write) |
|---|---|---|
| majority, 3 acceptors | 2 of 3 | 2 of 3 |
| flexible, 5 acceptors | 4 of 5 | 2 of 5 |
| grid, rows × columns | one full row | one full column |
| partitions, `P` × `2F+1` | a majority of every partition | a majority of one partition |

- `paros-core`: `QuorumSystem` (`membership/quorum_system.rs`). A configuration carries its
  quorum system, so a reconfiguration can change the shape.
- `parosctl journal create --survive machines=F --partitions P`: each partition has `2F+1`
  acceptors.
