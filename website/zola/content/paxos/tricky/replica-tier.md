+++
title = "The replica tier"
weight = 20
+++

A **replica** learns the chosen log and never votes. It is in no configuration, so adding a
replica adds read capacity and never a vote. The acceptors and the replicas then scale apart. In
paros a replica is a `ReplicaNode`, and because paros runs no application, it is a **read
replica**: it serves journal reads from its own chosen prefix.

{{ scenario(name="act5/the-replica-tier") }}

## The failure

**Learners on the voters.** When each acceptor is also the only learner, more read capacity
means more acceptors. More acceptors make each quorum bigger, and each write slower.

**A replica that serves a hole.** A replica misses the `Commit` of slot 2 and receives the one
of slot 3. If it serves slot 3, a read sees a journal with a gap that no writer made.

**A replica that heals from a replica.** A lagging replica asks another replica, which is
behind too. It learns a prefix that is not the chosen log.

## The rule

1. A replica answers no `Prepare` and no `Accept`, acks no beat, and counts toward no quorum.
   Its id is outside the acceptor pool.
2. The leader or a proxy sends each `Commit` to every learner, the replicas included.
3. A replica learns **in slot order**. It holds a slot above a hole and serves nothing past the
   hole.
4. A beat gives the replica the commit watermark. When the watermark is ahead of its prefix, it
   asks the beat's sender, an acceptor, for the missing range (`CatchUpRequest`).
5. Each acceptor stays a learner and keeps the chosen log. The acceptors are the durable tier,
   and a replica heals only from them.
6. A client read on a replica is a leaderless read: the replica asks a row of acceptors for
   their vote watermarks, and it serves once its prefix covers the maximum.

## Why the rule is enough

Each entry a replica records came in a `Commit` or a catch-up answer, and both rest on a durable
Phase-2 quorum. The replica votes on nothing, so it cannot change what is chosen. It learns in
slot order, so its prefix is a prefix of the chosen log. Its reads wait for the watermark of a
row, which meets every Phase-2 quorum, so a read sees every write acknowledged before it began.

## The code map

| What | Where |
|---|---|
| The replica | `crates/lib/paros-core/src/replica_node.rs:158` `ReplicaNode` |
| Its single entry point | `crates/lib/paros-core/src/replica_node.rs:342` `ReplicaNode::step` |
| The beat that shows the gap | `crates/lib/paros-core/src/replica_node.rs:517` `on_heartbeat` |
| A read from its fold | `crates/lib/paros-core/src/replica_node.rs:440` `quorum_read_in`, `:916` `read_log` |
| An acceptor serves the range | `crates/lib/paros-core/src/node/catch_up.rs:38` `serve_catchup` |
| The prefix, shared with nodes | `crates/lib/paros-core/src/replica.rs:112` `Replica` |
| The replica that answers a slot | `crates/lib/paros-core/src/membership/acceptor_config.rs:565` `AcceptorConfig::reply_owner` |

## The proof

The simulation runs replicas beside the acceptors and checks "replica: a slot is applied on a
replica that never voted", "replica: a replica's id is outside the node pool" and "replica: a
quorum read is served by a replica" (`crates/sim/paros-sim/src/audit/`). The lagging-replica
scenario holds one replica down until the cell decides past it, so the catch-up and the
trim-point jump run.
