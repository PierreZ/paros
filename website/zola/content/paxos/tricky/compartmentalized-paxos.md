+++
title = "Compartmentalized Paxos in one page"
weight = 16
+++

A Multi-Paxos leader does many jobs, and all of them go through one process. So the leader is
the bottleneck, and more machines make it slower. *Compartmentalized Paxos* (Whittaker et al.,
*Scaling Replicated State Machines with Compartmentalization*, 2021; the paper is in
`docs/references/papers/scaling-rsm-compartmentalization/`) takes the jobs apart. Each job goes
to its own process, and each process scales on its own. This page gives the whole idea. The
next pages give one rule each.

## The problem

Look at what one command costs a classic leader with `f` faults to tolerate:

1. It receives the client's command.
2. It selects a slot.
3. It sends `f + 1` `Accept`s and receives `f + 1` `Accepted`s.
4. It sends the `Commit` to every learner.

That is at least `3f + 4` messages through one process for each command (§3.1 of the paper).
An acceptor handles two. Add an acceptor and each quorum grows. Add a learner and the leader
sends one more `Commit`. The leader's work grows with each machine.

Only step 2 must be serial. Everything else can move to processes that do not need to agree
with each other.

## The six compartments

| Compartment | What moves off the leader, or off the acceptors | In paros |
|---|---|---|
| The leader only sequences | the broadcast of step 3 and 4 | the leader selects the slot and holds the ballot, nothing else |
| [Proxy leaders](@/paxos/tricky/proxy-leaders.md) | the `Accept` fan-out, the vote count, the `Commit` | `ProxyLeader`, one per `slot % proxies` |
| [Batchers and unbatchers](@/paxos/tricky/batchers.md) | one message per command at the leader | `Batcher` merges writes of a multi-writer journal into one slot |
| [Acceptor grids](@/paxos/tricky/quorum-systems.md) | the acceptors' share of each write | a slot goes to one column; partitions of `2F+1` |
| [Replicas](@/paxos/tricky/replica-tier.md) | learning and reads, off the voters | `ReplicaNode` learns and never votes |
| [Leaderless reads](@/paxos/tricky/quorum-reads.md) | linearizable reads, off the leader | a row of acceptors gives a watermark; a replica serves |

Each compartment keeps the safety argument of Paxos as it is. A proxy votes on nothing and
orders nothing. A batcher changes what is in a slot, not how a slot is chosen. A grid changes
the shape of a quorum, and a row still meets a column. A replica only learns values that a
Phase-2 quorum chose. That is why the paper can add the compartments one by one.

## Moving the leader

Splitting the leader is one half of this part. Moving it is the other half. A leader that is
alive and must move (a clean stop, a drain) can give its authority to a peer with one message,
and the peer continues under the same ballot with no Phase 1:
[Leader handoff without an election](@/paxos/tricky/leader-handoff.md). On a deployment with
proxies, the handoff and the proxies meet: a proxy can hold a round when the leadership moves.

The last page of this part is about what sits **on** the log: the small state machine that
decides, at apply, which client may write
([The state machine on the log](@/paxos/tricky/state-machine-on-the-log.md)).

## What it costs

- **One more message delay per command** for each compartment on the write path: client,
  batcher, leader, proxy, acceptors, proxy, learners.
- **More processes.** They are cheap: a proxy and a batcher keep nothing durable.
- **Plain Multi-Paxos does not change.** A proxy count of zero and a replica count of zero are
  exactly the classic deployment, and the wire carries the same messages.

## Where to start in the code

| What | Where |
|---|---|
| The proxy leader | `crates/lib/paros-core/src/proxy_leader.rs:180` `ProxyLeader` |
| The leader's choice: delegate or not | `crates/lib/paros-core/src/node.rs:118` `Delegation` |
| The batcher and the unbatcher | `crates/lib/paros-core/src/batcher.rs:134` `Batcher`, `:283` `Unbatcher` |
| The replica | `crates/lib/paros-core/src/replica_node.rs:158` `ReplicaNode` |
| The handoff | `crates/lib/paros-core/src/node/handoff.rs:398` `ColocatedNode::relinquish_to` |

The game plays this part as Act V: [`act5/proxy-leaders`](../../../play/#act5/proxy-leaders),
[`act5/the-dead-proxy`](../../../play/#act5/the-dead-proxy),
[`act5/handoff-through-a-proxy`](../../../play/#act5/handoff-through-a-proxy) and
[`act5/the-replica-tier`](../../../play/#act5/the-replica-tier). The field guide is
[Beyond Multi-Paxos](@/paxos/beyond-multi-paxos.md).
