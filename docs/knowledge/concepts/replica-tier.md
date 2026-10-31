# Replica tier

Group: Compartmentalization. Status: verified 2026-10-31.

A **replica** learns the chosen log and never votes. Because it is in no configuration, adding a
replica adds read capacity and never a vote, so the replica tier scales apart from the acceptors
([SRSM §3.3]). In paros a replica is a `ReplicaNode` (#144): a `Replica` over a durable chosen
log, with no `Acceptor` at all. Since paros runs no application (#186), a replica is a **read
replica**: it serves journal reads from its own chosen prefix, so read load leaves the acceptors.

## 1. Problem

In classic Multi-Paxos every replica executes every command and the leader sends every replica
every decision ([SRSM §3.3]). Adding replicas loads the leader and adds no throughput. Reads go
through the same path as writes, so they load the acceptors too.

## 2. Model

The Multi-Paxos model. A chosen slot is immutable, so any node that holds it can serve it. A
replica can crash, restart and lag; it is healed from the acceptors, never from another replica.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| `ReplicaNode` | the chosen log (learned records), the chosen index, the floor, the journal fold | the learned records and the floor yes (`WriteOp::Learned`, `Truncate`, `TrimmedTo`); the chosen index relaxed (`SetChosenIndex`); never a `WriteOp::Acceptor` op |
| `ReplicaNode` | a `QuorumReads` tally for the reads it serves | no |
| Acceptor (bare) | its votes, **and** a chosen index and chosen prefix | yes |

A replica answers no `Prepare` and no `Accept`, is never in `Config::peers` or the node pool,
acks no beat and counts toward no quorum (the `replica_node.rs` module doc).

**Why an acceptor keeps a chosen index anyway.** The module doc derives it from the code, coupling
by coupling: a chosen value becomes the authoritative record before the chosen index moves
(`mark_chosen`); the compaction floor moves only inside the chosen prefix; the repair probe and
the election recovery skip chosen slots; the GC fence counts chosen indices; catch-up is served
from the chosen prefix; the handoff names a decided tail; and the journal fold is judged at apply.
So every acceptor stays a learner, and the replica tier adds read capacity on top.

## 4. Normal flow

1. A leader or a proxy leader sends each `Commit` to every learner, the replicas included.
2. The replica writes a learned record, walks its contiguous prefix and folds the journal state
   (`ReplicaReady::committed`), in the node's own order: persist, send, report.
3. A beat gives it the commit watermark and the leader hint. When the watermark is ahead of its
   prefix, it sends `CatchUpRequest` to the beat's sender.
4. A client `Read` runs the leaderless read (§3.4): the replica asks a row of acceptors for their
   vote watermarks (`PreRead`), takes the maximum, and serves the page from its own fold once its
   prefix covers that index (`ReplicaNode::quorum_read_in`, `read_log`). See the reads card
   (planned).

```mermaid
sequenceDiagram
    participant L as Leader or proxy
    participant A as Acceptors (a row)
    participant R as Replica
    participant C as Client
    L->>R: Commit(b, 7, w)
    Note over R: persist learned record; walk to 7
    C->>R: Read(journal, from seq)
    R->>A: PreRead
    A->>R: PreReadAck(watermark 7)
    Note over R: prefix covers 7: serve from the fold
    R->>C: records
```

## 5. Recovery flow

- **A missed `Commit`.** The next beat reveals the gap and the replica pulls it (catch-up card).
- **A replica below the floor.** An acceptor answers its request with `TrimmedTo`; the replica
  jumps its floor and chosen index to the point and takes the journal state.
- **A restart.** A boot scan reads the learned records back through the same `Storage` port, and
  `Replica::from_boot` rebuilds the prefix and the sealed state, as on a node. On `run_replica` the format
  marker and the boot checks apply (#147); a machine-hosted replica that lost its store boots
  empty and learns again (commit `5833bb3`).
- **A configuration change.** A read opened over a superseded configuration is abandoned when a
  beat names a newer one.

## 6. Why it is safe

A replica only learns chosen values: every entry it records came in a `Commit` or a catch-up
response, both backed by a durable Phase-2 quorum. It votes on nothing, so it cannot change what
is chosen, and no quorum counts it. Its reads are the leaderless reads [SRSM §3.5] proves linearizable: the
replica waits until its prefix covers the maximum vote watermark of a row, and serves only then.

## 7. Liveness

- A replica depends on the acceptors for healing: a lagging replica is never a peer's source.
- A read that its prefix does not cover within `read_retry_ticks` answers `served: false`, and the
  client retries, here or elsewhere.
- Adding replicas never slows writes: they are learners outside every quorum.

## 8. Costs

- Each `Commit` goes to every replica too: more messages from the leader or its proxies.
- One durable learned record per slot per replica.
- [SRSM §8.5]: with 100% reads, throughput scales linearly in replicas, to about 650,000
  commands/s unbatched on 6 replicas.

## 9. In paros

| What | Where |
|---|---|
| The replica's state machine | `paros-core/src/replica_node.rs`: `ReplicaNode`, `step`, `tick`, `quorum_read_in`, `read_log`, `ready`, `reply_owner` |
| The prefix and the fold (shared with nodes) | `paros-core/src/replica.rs`: `Replica`, `Replica::from_boot` |
| The reply owner | `paros-core/src/membership/ids.rs`: `ReplicaId::of` (`slot % replica_count`); `paros-core/src/membership/acceptor_config.rs`: `AcceptorConfig::reply_owner` |
| The driver | `paros-roles/src/replica.rs`: `run_replica`; a machine runs the replica of each `Role::Replica` booking the registry holds for it (PR #780 (machine replicas), #765) |

**Sim gates and oracles** (`paros-sim/src/audit/state.rs` unless named):

- `"replica: a slot is applied on a replica that never voted"`.
- `"replica: a replica's id is outside the node pool"` (`audit/replica.rs`).
- `"replica: a quorum read is served by a replica"`, `"replica: a quorum read is served by a
  replica on a matchmaker deployment"`.
- `"replica: a replica below the floor jumps to the trim point"`.
- The lagging-replica scenario holds one replica down until the cell decides past it, at most
  `LATE_WINDOW` into the tail (`paros_sim::world::lagging_replica`, #550).

**Divergences from the sources:**

- [SRSM §3.3] replicas execute every command, and only one replica replies per command.
  paros runs no application: a replica folds the journal state and serves reads.
  `reply_owner` names the owner (`slot % replica_count`), but nothing routes on it yet (the `replica_node.rs` module doc).
- [SRSM §3.4] sends the read to "any one replica" after the client collects the watermarks. In
  paros the replica runs the pre-read itself, on the client's behalf.
- frankenpaxos's `ProxyReplica` forwards replies; paros has no proxy replica.

## 10. Pitfalls

- **Serving a page before it is reported.** A replica released its next apply page before it
  served the reads of the last one, so a read could see slots not yet persisted or reported
  (witnesses 147148122662988833 and 12015710127796105877). Reads are now served from the fold the
  batch reported, then the next page is released (#550, commit `f2d71f5`).
- **A replica as a source.** A replica serves no catch-up: the acceptors are the durable tier,
  and a replica tier is healed from them (coupling 5 of the `replica_node.rs` module doc).
- **A replica in a quorum.** A replica id inside the node pool would let a learner be counted as a
  voter. The sim asserts it never is.

## 11. Relatives

- [Catch-up](catch-up.md): how a replica heals.
- [Proxy leaders](proxy-leaders.md): a proxy sends its `Commit` to the replicas.
- Leaderless quorum reads (planned): the read a replica serves.
- [Batchers and unbatchers](batchers.md): [SRSM]'s unbatchers sit after the replicas.

## 12. Sources

- [SRSM] *Scaling Replicated State Machines with Compartmentalization*:
  `docs/references/papers/scaling-rsm-compartmentalization/transcript.md` §3.3 (more replicas),
  §3.4 (leaderless reads), §3.5 (correctness), §8.5 (read scalability).
- frankenpaxos: `docs/references/frankenpaxos/04-compartmentalization.md` §4 (`ProxyReplica`).

[SRSM §3.3]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §3.4]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §3.5]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §8.5]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
