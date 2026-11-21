---
layout: ../../layouts/Doc.astro
kicker: Consensus
section: consensus/
title: The deployments
description: Four state machines a driver can run, one per process. Each has the same shape - step, then ready, then advance - and the configuration says which ones a journal uses.
---

## The problem

The papers put each Paxos role on its own machine. A small system puts them all on one. A
library must serve both: the same roles, wired in different ways, with no second copy of the
protocol.

## The technique

`paros-consensus` wires its roles into four **deployments**. Each deployment is one state
machine that a driver runs as one process, and each has the same shape as
[the driver loop](../driver/): `step`, then `ready`, then `advance`. Which deployments a journal
runs is data in its `Config` (the matchmakers, the proxy leaders, the replicas, the quorum
system), never a cargo feature. Plain Multi-Paxos is `ColocatedNode` alone.

| Deployment | Runs | On disk | For |
|---|---|---|---|
| `ColocatedNode` | proposer, acceptor, replica | promise, votes, chosen log | every journal |
| `ProxyLeader` | the leader's Phase-2 tally | nothing | a busy leader |
| `ReplicaNode` | a learner that serves reads | chosen log | more reads |
| `Matchmaker` | the configuration registry | ballot → configuration | a change of acceptors |

## ColocatedNode

One node is proposer, acceptor and replica. The node that wins Phase 1 is the leader: it gives
each write a slot and sends the `Accept`s. Every node votes, learns the chosen slots and applies
the journal in slot order.

<div class="arch" role="img" aria-label="Three ColocatedNodes. Each one is proposer, acceptor and replica, with its own disk. The leader sends Accept to the others and they answer Accepted.">
  <div class="cellframe">
    <span class="cellname">one journal</span>
    <div class="row three">
      <div class="box lead">node 1 <small>proposer · acceptor · replica</small><small>leader</small></div>
      <div class="box">node 2 <small>proposer · acceptor · replica</small></div>
      <div class="box">node 3 <small>proposer · acceptor · replica</small></div>
    </div>
    <div class="arrow">▼ <small>persist before send</small></div>
    <div class="row three">
      <div class="box disk">disk 1</div>
      <div class="box disk">disk 2</div>
      <div class="box disk">disk 3</div>
    </div>
  </div>
</div>

With no matchmakers in its `Config`, the node runs plain Multi-Paxos on a fixed membership: it
steps no matchmaker message, writes no matchmaker state and refuses `reconfigure`. A leader can
hand its ballot to a successor with no election (`relinquish_to`). Example:
[`quickstart`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/quickstart.rs),
[`journal_calls`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/journal_calls.rs),
[`leader_handoff`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/leader_handoff.rs).
Flavor: [Multi-Paxos](../../paxos/multi-paxos/).

## ProxyLeader

A Multi-Paxos leader does two jobs for each write: it picks the slot, and it sends the `Accept`s,
counts the `Accepted`s and sends the `Commit`. Only the first job must be on one machine. The
leader gives the second job to a proxy leader. The proxy sends the same `Accept` to the column of
the slot, counts the answers with the same tally the proposer uses, and sends the `Commit` to the
learners.

<div class="arch" role="img" aria-label="The leader gives each slot to a proxy leader. The proxy leader sends Accept to the acceptors of the slot's column, counts the Accepted answers and sends Commit to the learners.">
  <div class="box lead">leader <small>picks the slot</small></div>
  <div class="arrow">▼ <small>Accept, one per slot</small></div>
  <div class="row">
    <div class="box">proxy leader 1 <small>no disk</small></div>
    <div class="box">proxy leader 2 <small>no disk</small></div>
  </div>
  <div class="arrow">▼ <small>Accept → Accepted</small></div>
  <div class="box">acceptors of the column</div>
  <div class="arrow">▼ <small>Commit</small></div>
  <div class="box side">learners</div>
</div>

A proxy votes on nothing and orders nothing, so moving it cannot break agreement. It keeps
nothing durable: after a crash it boots empty, and the leader sends the rounds again. Its driver
calls `resend_pending` each beat. Example:
[`proxy_leader`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/proxy_leader.rs).
Flavor: [Compartmentalized Paxos](../../paxos/compartmentalized/).

## ReplicaNode

A replica learns the chosen log from the `Commit`s and serves reads from it. It never votes: it
answers no `Prepare` and no `Accept`, and it is in no quorum. A new acceptor makes each quorum
larger; a new replica only adds read capacity.

<div class="arch" role="img" aria-label="Acceptors vote and learn. Replicas learn the chosen log from Commit messages and serve reads. Replicas never vote.">
  <div class="row three">
    <div class="box">acceptor <small>votes · learns</small></div>
    <div class="box">acceptor <small>votes · learns</small></div>
    <div class="box">acceptor <small>votes · learns</small></div>
  </div>
  <div class="arrow">▼ <small>Commit</small></div>
  <div class="row">
    <div class="box">replica <small>learns · serves reads</small></div>
    <div class="box">replica <small>learns · serves reads</small></div>
  </div>
  <div class="arrow">▼ <small>read_log · quorum_read_in</small></div>
  <div class="box side">clients</div>
</div>

A replica serves a read from its own chosen prefix (`read_log`), or runs a leaderless read
against a row of acceptors (`quorum_read_in`). A replica that lags asks for the slots it lacks.
Example:
[`replica_tier`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/replica_tier.rs),
[`quorum_read`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/quorum_read.rs).
Flavor: [Compartmentalized Paxos](../../paxos/compartmentalized/).

## Matchmaker and MatchmakerReconfigurer

A matchmaker keeps a small durable map: each ballot and the configuration of acceptors that the
ballot uses. A candidate registers its ballot at a majority of matchmakers before Phase 1, and
learns each configuration used below it. Then a change of acceptors is only a new ballot with a
new configuration. The matchmakers see a message only on a round change, never on a write.

<div class="arch" role="img" aria-label="A candidate node registers its ballot and configuration at a majority of matchmakers, then runs Phase 1 over every configuration they report. The MatchmakerReconfigurer moves the matchmaker set to a new generation with a single decree.">
  <div class="box lead">candidate <small>a ColocatedNode with matchmakers in its Config</small></div>
  <div class="arrow">▼ <small>match_requests · gc_requests</small></div>
  <div class="cellframe">
    <span class="cellname">matchmaker set, generation g</span>
    <div class="row three">
      <div class="box">matchmaker <small>ballot → configuration</small></div>
      <div class="box">matchmaker <small>ballot → configuration</small></div>
      <div class="box">matchmaker <small>ballot → configuration</small></div>
    </div>
  </div>
  <div class="arrow">▼ <small>MatchmakerReconfigurer: stop, bootstrap, decide, publish</small></div>
  <div class="box side">matchmaker set, generation g + 1</div>
</div>

The node sends `Ready::match_requests` and `Ready::gc_requests` after the batch is durable, and
gives the answers back with `on_match_reply` and `on_gc_ack`. The matchmaker set itself moves to a
new generation with `MatchmakerReconfigurer`: it stops the old set, starts the new one, and
chooses it with a single decree (`Decree`) over the old set. Matchmaker quorums are majorities.
Example:
[`reconfiguration`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/reconfiguration.rs),
[`matchmaker`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/matchmaker.rs).
Flavor: [Matchmaker Paxos](../../paxos/matchmaker/).

## In paros

- [`node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node.rs),
  [`proxy_leader.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/proxy_leader.rs),
  [`replica_node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/replica_node.rs),
  [`matchmaker.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/matchmaker.rs) and
  [`matchmaker/reconfigurer.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/matchmaker/reconfigurer.rs):
  the four deployments. The module doc of each one states its contract.
- [`examples/common/cluster.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/common/cluster.rs):
  a driver for nodes and matchmakers together, with crashes and restarts.
- [`contract/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/contract): `Config`, which names what a deployment runs.
