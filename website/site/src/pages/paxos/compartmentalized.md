---
layout: ../../layouts/Doc.astro
kicker: Paxos
section: paxos/
trace: compartmentalized
lab: true
title: Compartmentalized Paxos
description: Split the leader into roles that scale on their own.
---

## The problem

In Multi-Paxos one leader takes every write, sends every Accept and counts every answer. One
machine sets the throughput of the journal.

## The story

Node 0 leads three acceptors (nodes 0, 1 and 2). Beside them run two proxy leaders, two
replicas and a batcher. Each card is one process. The story writes two values: node 0 gives
each write a [slot](../#slot) and hands it to a proxy leader (slot mod 2). The proxy leader
sends the Accepts, counts the votes and tells everyone the slot is chosen. The replicas learn
the log and vote on nothing. Then choose a case:

- **Before**: the same two writes when node 0 does all the work.
- **Two writers, one slot**: the batcher puts two writes in one write, so one round of
  Phase 2 chooses both.
- **A read with no leader**: a replica asks the acceptors for the highest slot that they voted
  in, and serves the read when it has learned that slot.
- **A proxy leader crashes**: the slot it held is not chosen. Node 0 sends it again on each
  beat, then takes it back and sends the Accepts itself.
- **A vote to a proxy is lost**: the proxy leader sends its Accepts again on its next beat.
- **A proxy meets a newer ballot**: the acceptors refuse with a `Nack`. The proxy leader
  closes the round and passes the `Nack` to node 0, which stops leading.
- **Break the rule**: a proxy leader sends a `Commit` after one vote of three. The replicas
  learn a value that the acceptors never chose. Then find the bug.

The leader handoff (`Relinquish`) is on the [Multi-Paxos](../multi-paxos/) page.

<div data-steps><p class="aside">The step-through diagram needs JavaScript.</p></div>

## Your turn

Now you are the network. Deliver or drop each message, and reach the goal of each exercise.
The leader, the proxy leaders and the replicas of `paros-consensus` run in this page and decide
everything else. "Deliver all" delivers every message on the wire, and the messages they send,
until the wire is empty.

<div data-lab="proxy-down lost-votes replica-waits"><p class="aside">The exercises need JavaScript.</p></div>

## The rule

**Only the leader gives out slots at its ballot. The other roles move work, not
decisions.** A proxy leader counts votes but casts none. A replica learns but votes on
nothing. A batcher joins writes before they get a slot. So the safety of Multi-Paxos does not
change, and a crashed proxy leader or replica loses nothing. The run "Break the rule" shows
why a proxy leader counts a majority before its `Commit`: a replica trusts each `Commit` and
counts no vote.

## In paros

Each task of the leader is a role of its own in `paros-consensus`: the leader gives each write a
slot, a proxy leader sends the Accepts of a slot and counts the answers, a batcher puts the
writes of many writers in one slot, the acceptors vote, and a replica learns the chosen records
and serves reads.

- `paros-consensus`: `ProxyLeader` ([`proxy_leader.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/proxy_leader.rs)), `Batcher` and
  `Unbatcher` ([`batcher.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/batcher.rs)), `ReplicaNode`
  ([`replica_node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/replica_node.rs)), the leaderless read
  ([`quorum_read.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/quorum_read.rs)). The story runs these roles in the
  browser.
- Consensus: [the deployments](../../consensus/deployments/#proxyleader) show `ProxyLeader` and `ReplicaNode`, each its own process.
