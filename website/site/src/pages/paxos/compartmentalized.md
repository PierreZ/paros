---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: compartmentalized
title: Compartmentalized Paxos
description: Split the leader into roles that scale on their own, and hand the leader over with no election.
---

## The problem

In Multi-Paxos one leader takes every write, sends every Accept and counts every answer. One
machine sets the throughput of the journal, and moving that machine costs an election.

## Step through

A leader that is alive and must move gives its authority to a peer (`Relinquish`): its
ballot, its next free slot and its open slots. The peer leads at the **same ballot** and
skips Phase 1. Compare with the run where the leader crashes: then an election is necessary.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**Only the holder of the ballot gives out slots at that ballot, and the authority moves one
hop only.** The other roles move work, not decisions, so the safety of Multi-Paxos does not
change.

## In paros

Each task of the leader is a role that scales on its own, counted per journal:

| Role | Task | Count |
|---|---|---|
| leader | gives each write a slot | one per journal |
| proxy leader | sends the Accepts of a slot, counts the answers | `--proxies N` |
| batcher | puts the writes of many writers in one slot | `--batchers N` |
| acceptor | votes, keeps the records | `2F+1` per partition |
| replica | learns the chosen records, serves reads | `--replicas N` |
| frontend | checks the token, finds the journal | `--frontend-count N` |

- `paros-core`: `ProxyLeader` (`proxy_leader/`), `batcher/`, `replica/`, and the handoff
  in `crates/lib/paros-core/src/node/handoff.rs`.
- The tenant coordinator books the role counts that `parosctl configure` records.
