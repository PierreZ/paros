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

Node 0 leads three acceptors (nodes 0, 1 and 2). Beside them run two proxy leaders, two
replicas and a batcher. Each card is one process.

- **Proxy leaders and replicas** (the base run): node 0 gives each write a [slot](../#slot) and hands it
  to a proxy leader (slot mod 2). The proxy leader sends the Accepts, counts the votes and
  tells everyone the slot is chosen. The replicas learn the log and vote on nothing.
- **Before**: the same two writes when node 0 does all the work. Compare the messages that
  node 0 sends and receives.
- **Two writers, one slot**: the batcher puts two writes in one write, so one round of
  Phase 2 chooses both.
- **A read with no leader**: a replica asks the acceptors for the highest slot that they voted
  in, and serves the read when it has learned that slot.
- **A proxy leader crashes**: the slot it held is not chosen. Node 0 sends it again on each
  beat, then takes it back and sends the Accepts itself.
- **Move the leader with no election**: a leader that is alive gives its authority to a peer
  (`Relinquish`): its [ballot](../#ballot), its next free slot and its open slots. The peer leads at the
  **same ballot** and skips Phase 1.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## The rule

**Only the leader gives out slots at its ballot. The other roles move work, not
decisions.** A proxy leader counts votes but casts none. A replica learns but votes on
nothing. A batcher joins writes before they get a slot. So the safety of Multi-Paxos does not
change, and a crashed proxy leader or replica loses nothing. The authority of the leader moves
one hop only, at the same ballot.

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

- `paros-core`: `ProxyLeader` ([`proxy_leader.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/proxy_leader.rs)), `Batcher` and
  `Unbatcher` ([`batcher.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/batcher.rs)), `ReplicaNode`
  ([`replica_node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/replica_node.rs)), the leaderless read
  ([`quorum_read.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/quorum_read.rs)) and the handoff
  ([`node/handoff.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-core/src/node/handoff.rs)). The step-through runs these roles in your
  browser.
- `parosctl journal create acme orders --layout compartmentalized` books 2 proxy leaders,
  2 replicas and, for a multi-writer journal, 2 batchers. A count beside it, for example
  `--proxies 3`, changes that count. `--layout colocated` (the default) runs every task in
  the leader and acceptor processes. `configure` takes the same flags on a live journal.
- The tenant coordinator books the role counts that `parosctl configure` records.
