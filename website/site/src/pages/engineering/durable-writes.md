---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: Fast durable writes
description: Every Paxos vote is synced to disk before it counts. paros makes writes fast by working around the sync, never by skipping it.
---

## The rule

A Paxos acceptor makes its vote durable before it answers `Accepted`. If a majority forgets its
votes in a power loss, a chosen value can be lost or replaced. That is a safety failure, not
only lost data. So paros never makes a write fast by making it less durable.

A sync is slow. A write that waits for several syncs, one after the other, is slower. The work
is to wait for fewer syncs, and to let one sync carry more writes.

## What paros does

| Step | What it changes |
|---|---|
| Accepts before the leader's own sync | The leader sends its `Accept`s while it syncs its own vote. Its sync and the acceptors' syncs run at the same time (Paxos Made Live, section 4.2). |
| Sync off the node loop | One journal's sync does not stop the other journals. The node loop hands each batch to a flight and serves other journals while the store syncs. |
| Group commit | The writes that arrive during one sync share the next sync. |
| Several writes in flight | A writer does not wait for one write before it sends the next. |
| Durability mode per journal | `Ordered` syncs twice per commit (entries, then the record that points at them). `Batched` syncs once and repairs an ambiguous batch from its peers. |
| Balanced segments | A store segment holds many commits, and the next segment is prepared in the background, so a rollover does not stall the commit path. |
| Constant-time checks | The core's invariant checks do not walk the whole log on every call. |

Each step keeps the rule. The simulator checks it: the `hint!` "accepts sent, own vote not
durable" crashes the leader in exactly that window, and the audit checks that no chosen value is
lost.

## One write, step by step

<div class="seq">
<table>
<thead><tr><th>Step</th><th>client</th><th>leader</th><th>acceptors</th><th class="note">Why it is safe</th></tr></thead>
<tbody>
<tr><td>1</td><td class="msg">Write →</td><td>batch</td><td></td><td class="note">Writes that arrive during a sync wait for the next batch.</td></tr>
<tr><td>2</td><td></td><td class="msg">Accept →</td><td>write + sync</td><td class="note">An <code>Accept</code> carries a proposal, not a vote. It can leave first.</td></tr>
<tr><td>3</td><td></td><td>write + sync</td><td></td><td class="note">The leader's own vote, at the same time as step 2.</td></tr>
<tr><td>4</td><td></td><td>tally</td><td class="msg">← Accepted</td><td class="note">Each <code>Accepted</code> leaves only after its sync.</td></tr>
<tr><td>5</td><td class="msg">← Ok</td><td>chosen</td><td></td><td class="note">A majority of durable votes chose the slot.</td></tr>
</tbody>
</table>
</div>

## Other systems

- **Kafka** does not sync each write by default. Durability comes from replication
  (`acks=all`, `min.insync.replicas`). A power loss of all in-sync replicas at once can lose
  acknowledged writes. paros does not allow this.
- **Redpanda** runs Raft with a sync, one log per partition, and batches the writes that arrive
  during one sync. This is close to what paros does with group commit and the sync off the node
  loop.

## Not done yet

One shared write-ahead log per disk, for all journals, would turn many small syncs into one.
It costs fault isolation, garbage collection and fairness between tenants, so it waits.

## In paros

- `crates/lib/paros-roles/src/node/ready.rs`: `send_early` sends the `Accept`s before the flush.
- `crates/lib/paros-roles/src/node/flight.rs`: a journal's flush off the node loop, and group
  commit.
- `crates/lib/paros-model/src/tenant.rs`: `StoreDurability::{Ordered, Batched}`.
- `parosctl journal create … --store-durability batched` picks the mode at create time.
