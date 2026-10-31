---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: multi-paxos
title: Multi-Paxos
description: A journal is a log of slots. One Phase 1 elects a leader for every slot, and each write then costs one round trip.
---

## The problem

A journal needs many values in order, not one. Two phases per record cost two round trips,
and proposers that fight over each slot make no progress.

## Step through

Each **slot** of the log is one single-decree Paxos. A leader runs Phase 1 **once** for every
slot from the first one it does not know. After that, a write is only Phase 2 in the next
free slot. A `Commit` tells the followers which slots are chosen.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**A new leader proposes again every value that its Phase 1 reports, and it fills a slot with
no reported value with a no-op.** So a value that a dead leader got chosen stays in its slot,
and the log has no hole.

## In paros

- `paros-core`: the roles in `proposer/`, `acceptor/` and `replica/`, and `ColocatedNode`
  that runs them together.
- Each batch of work is **persist, then send, then learn** (`Ready` and `HardState` in
  `crates/lib/paros-core/src/lib.rs`).
- A heartbeat keeps the leader. A follower that misses it starts an election.
- Every node applies the chosen slots in order, so a `Write`, a `Truncate` or a
  `SetLeader` gets the same answer on every node.
