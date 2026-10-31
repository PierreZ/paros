---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
title: Multi-Paxos
description: A journal is a log of slots. One Phase 1 elects a leader for every slot, and each write then costs one round trip.
---

## The problem

A journal needs many values in order, not one. Running both phases for each record costs two
round trips and lets proposers fight over every slot.

## The protocol

The log is a sequence of **slots**, and each slot is one single-decree Paxos. A leader runs
Phase 1 **once** for all the slots above the last one it knows. After that, each write is only
Phase 2 in the next free slot.

<div class="seq"><table>
<thead><tr><th>leader</th><th>a1</th><th>a2</th><th>a3</th><th class="note">what happens</th></tr></thead>
<tbody>
<tr><td class="msg">Prepare(7, from slot 12) →</td><td>●</td><td>●</td><td>●</td><td class="note">one Phase 1 for every slot from 12</td></tr>
<tr><td>←</td><td class="msg">Promise + votes</td><td class="msg">Promise + votes</td><td>·</td><td class="note">each acceptor reports what it accepted in those slots</td></tr>
<tr><td class="msg">Accept(7, 12, w1) →</td><td>●</td><td>●</td><td>●</td><td class="note">a write: Phase 2 only</td></tr>
<tr><td>←</td><td class="msg">Accepted</td><td>·</td><td class="msg">Accepted</td><td class="note">slot 12 chosen</td></tr>
<tr><td class="msg">Accept(7, 13, w2) →</td><td>●</td><td>●</td><td>●</td><td class="note">the next write, the next slot</td></tr>
</tbody></table></div>

A new leader finishes what the old one started. For each slot that a promise reports, it
proposes the reported value again. It fills a slot with no reported value with a no-op, so
the log has no hole.

## Why it is safe

Each slot is a single-decree Paxos, so each slot is safe on its own. The ballot is shared by
all slots: an acceptor that promised ballot 7 refuses a lower ballot in every slot.

Every node applies the chosen slots in order. A `Write`, a `Truncate` or a `SetLeader` is
judged when its slot is applied, so all nodes give the same answer to the same call.

## In paros

- `paros-core`: the roles in `proposer/`, `acceptor/` and `replica/`, and `ColocatedNode`
  that runs them together.
- The durability contract: each batch of work is **persist, then send, then learn**
  (`Ready` and `HardState` in `crates/lib/paros-core/src/lib.rs`).
- A heartbeat keeps the leader. A follower that misses it starts an election.
- A read asks a read quorum, so a read never returns less than a write that finished.
