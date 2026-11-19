# Introducing Northguard and Xinfra: scalable log storage at LinkedIn

**Authors:** Onur Karaman, Xiongqi Wu (LinkedIn)
**Date:** 2025-06-25, LinkedIn engineering blog.
**Source:** <https://www.linkedin.com/blog/engineering/infrastructure/introducing-northguard-and-xinfra>.
Read from the page on 2026-11-19. There is no paper and no public code.

---

## Summary

Northguard is LinkedIn's log store that replaces Kafka. LinkedIn ran Kafka at 32T records and 17
PB a day, on 400K topics, 10K+ machines and 150 clusters. The pain points were metadata scale
(one controller), balance (a partition is a coarse unit of replication, so an external service
moved data), availability and consistency (a partition replica set takes long to heal, so topics
traded consistency for produce availability), and weak durability. Northguard shards both data
and metadata, keeps minimal global state, and stripes each log over many replica sets. Xinfra is
a virtual Pub/Sub layer over Kafka and Northguard used for the migration.

## Data model

- **Record**: key, value, headers (bytes).
- **Segment**: a sequence of records, **the unit of replication**. Active (takes appends) or
  sealed (immutable). Offsets are relative to the segment start. A segment seals when **a replica
  fails, it reaches 1 GB, or it has been active for one hour**.
- **Range**: the log abstraction. A sequence of segments for one contiguous slice of the
  keyspace. Active or sealed. An active range has at most one active segment, the last one.
- **Topic**: a named set of ranges that covers the whole keyspace. A **split** seals a range and
  creates two; a **merge** seals two buddy ranges (as in the buddy allocator) and creates one
  child. Splits and merges keep a happens-before order: every record of a parent comes before
  every record of its children. Only producers of the split range are interrupted.
- **Storage policy** per topic: retention (when segments are deleted) and placement constraints
  on broker attributes (rack, data center, deploy group), set by administrators.

## Log striping

Each new segment gets a new replica set. Segments are created often, so a new broker gets load
from new segments and no existing data moves. An unlucky mix of segments on one broker fixes
itself as segments roll. The cluster balances by design.

## Metadata

- A **vnode** is a Raft replicated state machine that holds one shard of the metadata. The vnode
  leader runs the **coordinator**, the business logic. A new vnode leader takes over the
  coordinator from the state in the vnode.
- The coordinator tracks, per topic: seal, delete, splits and merges; per range:
  active/sealed/deleting, creation time, retention, topic; **per segment: the replica set, the
  state (active, sealed, reassigning), the start offset and length, the create time and the seal
  time**. It uses this state to re-replicate under-replicated sealed segments (self-healing).
- The **DS-RSM** is the set of vnodes on a consistent-hash ring: topics hash by name, ranges and
  segments by range id. LinkedIn runs 128+ vnodes per cluster.
- **SWIM** gossip carries membership and minimal global state (broker addresses and attributes,
  the ring, each vnode's leader, term and replicas). Any broker proxies a metadata request to the
  right vnode leader.

## Protocols

- Metadata calls are unary (request, response).
- Produce, consume and replication are **sessionized streams** with pipelining and windows. A
  producer opens a stream to the **active segment leader**; each `Append` carries a stream id, a
  sequence number and records. The broker may ack M of N appends and **acks only committed
  records**. Followers receive `ReplicaAppend`s that carry the committed state.
- Sealed-segment re-replication is the consume protocol between two brokers.
- The comparison table says produce acks wait for **fsync on all replicas** (batched at 10 ms,
  20k records or 10 MB). When a replica fails, producers move to a new segment.

## Storage and testing

The "fps store": a WAL, one file per segment, Direct I/O, a sparse index in RocksDB, and group
commit. Direct I/O keeps state consistent across fsync failures and lets Northguard cache by
known consume streams. Northguard runs under **deterministic simulation** (one thread, years of
simulated activity a day, faults: shutdowns, rolling restarts, partitions, packet loss and
corruption, disk corruption and I/O errors, config deploys).

## Xinfra

A Xinfra topic has **epochs**: each epoch can live in a different physical cluster (Kafka or
Northguard), so a topic moves between clusters at run time without client changes. Metadata is in
MySQL, coordination in ZooKeeper, consumer checkpoints in Vitess with a Couchbase cache.

---

## Segments, sealing, offload and ordering

| Question | Northguard |
|---|---|
| What a segment is | A run of records of one range, with its own replica set |
| Where segments are declared | In the vnode that owns the range: replica set, state, start offset, length, times |
| Why a segment seals | Replica failure, 1 GB, or 1 hour |
| Who seals | Not stated. The coordinator owns segment state, so it is the likely actor; this is my reading |
| How the end is fixed | Not stated beyond "start offset and length" in the segment record |
| Ordering inside a segment | One active segment leader; acks after commit, with fsync on all replicas |
| Ordering across segments | The range's segment order; across ranges, split and merge give happens-before |
| Offload | Not described. Retention deletes whole segments |
| Readers | Find segments through the vnode metadata |

## What this means for the paros journal

- **The segment record fields.** Northguard's segment record is a good checklist for design A of
  #1213 (segments experiment): replica set, state (active, sealed, reassigning), start and length,
  create and seal time. The journal design's review problem 1 asks the same: add the replica set
  to the segment record and a re-replication action for a dead replica of a sealed segment.
  Northguard's "reassigning" state is that action.
- **Roll by size and age.** Northguard rolls at 1 GB or one hour. The journal design adopted a roll
  by size or age as a same-grid reconfiguration (Fable review question 5, Pierre's rule 1). The
  numbers are LinkedIn's, not a paros decision.
- **Seal on replica failure is the "dumb segment" model.** Like a Delos Loglet, a Northguard
  segment does not survive a replica failure: it seals and producers move on. paros decided the
  opposite (decision 1.A): Paxos runs inside the segment, so one dead acceptor does not seal it;
  a reshape replaces the acceptor through the matchmakers.
- **Striping is the grid controller's model.** New segments on new machines, no data moves, the
  cluster balances by itself. This matches the journal design's automated grid: a reshape opens a
  segment on a new grid, and sealed data stays where it is.
- **Metadata sharded by range id.** Northguard shards segment metadata over 128+ Raft groups.
  paros goes further, one matchmaker set per journal (Pierre, 2026-11-19), multiplexed on shared
  machines.
- **Ranges are the later scaling option.** Split and merge with happens-before is how Northguard
  scales one topic past one leader. The journal design keeps one leader per journal for v1 and
  lists key ranges as a later option. Northguard's buddy-range rules are a reference for that.
- **Deterministic simulation.** Northguard uses the same test method as paros (moonpool).

Correction to earlier notes: the design doc and #1213 said "the vnode leader (coordinator) seals
and creates the next segment". The post does not say who seals. It says that the coordinator
keeps segment state and drives splits, merges and re-replication.
