---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
title: Compartmentalized Paxos
description: Split the leader into roles that scale on their own, and hand the leader over with no election.
---

## The problem

In Multi-Paxos the leader does everything: it takes every write, sends every Accept, counts
every answer and tells every learner. One machine sets the throughput of the journal.

## The roles

Compartmentalized Paxos moves each task of the leader to its own role. Each role scales on
its own. paros counts them per journal.

| Role | Task | Count in paros |
|---|---|---|
| leader | orders the writes: one slot each | one per journal |
| proxy leader | sends the Accepts of a slot and counts the answers | `--proxies N` |
| batcher | groups the writes of many writers in one slot | `--batchers N` (multi-writer) |
| acceptor | votes, keeps the records | `2F+1` per partition |
| replica | learns the chosen records, serves reads | `--replicas N` |
| frontend | checks the token, finds the journal | `--frontend-count N` per tenant |

<div class="arch" role="img" aria-label="Writers send writes to a batcher, the batcher to the leader, the leader gives each batch a slot and a proxy leader runs Phase 2 on the acceptors; replicas learn the result.">
  <div class="box">writers</div>
  <div class="arrow">▼</div>
  <div class="row"><div class="box">batcher</div></div>
  <div class="arrow">▼ <small>one write per batch</small></div>
  <div class="row"><div class="box lead">leader <small>slot numbers only</small></div></div>
  <div class="arrow">▼</div>
  <div class="row"><div class="box">proxy leader</div><div class="box">proxy leader</div></div>
  <div class="arrow">▼ <small>Accept / Accepted</small></div>
  <div class="row three"><div class="box">acceptor</div><div class="box">acceptor</div><div class="box">acceptor</div></div>
  <div class="arrow">▼ <small>chosen</small></div>
  <div class="row"><div class="box side">replica</div><div class="box side">replica</div></div>
</div>

## Leader handoff

A leader that is alive and wants to move gives its authority to another machine: its ballot,
its next free slot and the open slots below it. The new leader skips Phase 1. An election is
only for a leader that is gone (`crates/lib/paros-core/src/node/handoff.rs`).

## Why it is safe

The leader stays the only one that gives out slots at its ballot. A proxy leader only
carries Phase 2 for a slot it got from the leader. So the safety argument of Multi-Paxos
does not change: the roles move work, not decisions.

## In paros

- `paros-core`: `ProxyLeader` (`proxy_leader/`), `batcher/`, `replica/`.
- The tenant coordinator books the role counts that `parosctl configure` records, one slot
  per machine, zones in round robin.
