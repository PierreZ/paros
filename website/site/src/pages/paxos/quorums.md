---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
title: Quorums
description: Which acceptors must answer. Majority, flexible quorums, grids and partitions.
---

## The rule

Paxos needs one thing from its quorums: **every Phase 1 quorum shares an acceptor with every
Phase 2 quorum.** A majority of `2F+1` acceptors does this and survives `F` failures. Other
shapes do it too, and they move the cost between elections and writes.

## Shapes

| Shape | Phase 1 (election, read) | Phase 2 (write) | Cost |
|---|---|---|---|
| majority, 3 acceptors | 2 of 3 | 2 of 3 | the default |
| flexible, 5 acceptors | 4 of 5 | 2 of 5 | cheap writes, expensive elections |
| grid, 3 × 3 | one full row | one full column | a write touches 3 of 9 |
| partitions, `P` × `2F+1` | a majority of every partition | a majority of one partition | writes spread over partitions |

<div class="seq"><table>
<thead><tr><th></th><th>col 0</th><th>col 1</th><th>col 2</th><th class="note"></th></tr></thead>
<tbody>
<tr><th>row 0</th><td>a00</td><td>a01</td><td>a02</td><td class="note">a full row is a Phase 1 quorum</td></tr>
<tr><th>row 1</th><td>a10</td><td>a11</td><td>a12</td><td class="note">slot s goes to column s mod 3</td></tr>
<tr><th>row 2</th><td>a20</td><td>a21</td><td>a22</td><td class="note">a full column is a Phase 2 quorum</td></tr>
</tbody></table></div>

Every row crosses every column, so the rule holds.

## What it costs

On one machine, a 3 × 3 grid was about 2 times slower than a plain majority: all acceptors
shared one disk and 4 CPUs, and the leader was in the column of a slot only 1 time in 3. A
grid helps when each column has its own disks and CPUs. Measure it with
`paros-bench --topology 3x1,3x2,3x3`.

## In paros

- `paros-core`: `QuorumSystem` (`membership/quorum_system.rs`). A configuration carries its
  quorum system, so a reconfiguration can change the shape.
- `parosctl journal create --survive machines=F --partitions P`: each partition has `2F+1`
  acceptors.
