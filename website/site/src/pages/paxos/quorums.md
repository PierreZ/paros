---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: quorums
title: Quorums
description: Which acceptors must answer. Majority, grids and partitions.
---

## The problem

A majority of `2F+1` acceptors survives `F` failures, but every write touches a majority.
Other [quorum](../#quorum) shapes move the cost between elections and writes.

## The story

Six nodes in a grid of 2 rows and 3 columns. Each **column** is a partition. Phase 1 needs
one acceptor of **every** column. Phase 2 needs a **full column**, and slot `s` goes to column
`s mod 3`. So a write touches 2 of 6 acceptors. The story ends with a choice: a majority, two
quorums that do not meet, an election with half the nodes down, a stuck column, a leader that
dies after a write, and partitions. Continue plays the next screen, and "Show each message"
opens the step-through.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## Try it

Pick a shape and stop some acceptors. `paros-consensus` says if an election and a write can still
finish.

<div data-quorum-picker><p class="aside">The quorum picker needs JavaScript.</p></div>

## The rule

**Every Phase 1 quorum must share an acceptor with every Phase 2 quorum.** Two Phase 2
quorums need not meet, and two Phase 1 quorums need not meet. A Phase 1 quorum of the grid
has an acceptor in every column, so it meets every full column. A column with a dead node
cannot choose its slots, and the other columns go on. The leader can move a stuck slot to
another column with the same ballot and value (`ColocatedNode::move_round`). The run "Break
the rule" counts two quorums that share no acceptor, and two values are chosen.

## In paros

| Shape | Phase 1 (election, read) | Phase 2 (write) |
|---|---|---|
| majority, 3 acceptors | 2 of 3 | 2 of 3 |
| grid, rows × columns | one acceptor of every column | one full column |
| partitions, `P` × `2F+1` | a majority of every partition | a majority of one partition |

- `paros-consensus`: `QuorumSystem` ([`membership/quorum_system.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/membership/quorum_system.rs)). A configuration carries its
  quorum system, so a reconfiguration can change the shape.
- `QuorumSystem::partitions(F, P)`: `P` partitions of `2F+1` acceptors each.
