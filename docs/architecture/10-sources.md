[← 07-changes-milestones-done.md](07-changes-milestones-done.md) · [Index](README.md) · [11-alternatives.md →](11-alternatives.md)

## 10. Sources

The AWS Journal, as publicly described:

- Brooker, "MemoryDB: Speed, Durability, and Composition",
  <https://brooker.co.za/blog/2024/04/25/memorydb.html>: the API evolution `write` and `read`,
  then lease-fenced writes, then `set_leader` as a compare-and-swap.
- Bowes, "The Adjudicator", <https://marc-bowes.com/dsql-adjudicator.html>: a superseded writer's
  commit is rejected; pipelining by expected sequence number, a refusal cascading to the writes
  behind it; generations as a later precondition.
- Bowes, "Meet Coupler", <https://marc-bowes.com/dsql-coupler.html>: journals added and removed
  transparently; every consumer a tailer.
- Brooker, "DSQL Vignette: Transactions and Durability",
  <https://brooker.co.za/blog/2024/12/05/inside-dsql-writes.html>: the journal stores decided
  outcomes, never proposals; durability is the journal's commit.
- Brooker, "Why Strong Consistency?", <https://brooker.co.za/blog/2025/11/18/consistency.html>:
  strictly monotone per-journal streams plus a reader that waits until caught up are the whole
  consistency mechanism.
- Brooker, "Wait! Isn't That Impossible?",
  <https://brooker.co.za/blog/2024/12/06/inside-dsql-cap.html>: the adjudicator holds no durable
  state and is rebuilt from the committed transactions, so it moves cheaply to the majority side;
  the witness region holds only a copy of the Journal; after a region loss every commit waits on
  two of two.
- Brooker, "Control Planes vs Data Planes", <https://brooker.co.za/blog/2019/03/17/control.html>:
  what belongs on the request path and what scales with the universe.
- Amazon MemoryDB, SIGMOD 2024,
  <https://cdn.amazon.science/e0/1b/ba6c28034babbc1b18f54aa8102e/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database.pdf>:
  §4.1 conditional append and leadership as one more conditional append, the lease and
  self-demotion; §7.2.1 snapshot verification by a checksum carried in the log.
- Aurora DSQL, arXiv 2607.13276, <https://arxiv.org/html/2607.13276v2>: §5 the journal's
  precondition on timestamp monotonicity, §6 TLA+ and P then deterministic simulation; §5.4 the
  Journal replicates inside a region with a variant of chain replication across AZs and across
  regions with a variant of Paxos (two of three). paros keeps quorum writes everywhere, inside a
  region too, by choice: a slow or dead acceptor costs nothing until reconfiguration, where a chain
  stalls until its membership is changed (section 11). Transcript:
  `docs/references/papers/aurora-dsql/transcript.md`.
- Demirbas's MemoryDB summary,
  <http://muratbuffalo.blogspot.com/2024/05/amazon-memorydb-fast-and-durable-memory.html>, and
  the 2025 Journal reconstruction,
  <https://ajalab.github.io/posts/2025-08-14-journal-distributed-log-replication-behind-aws/>:
  secondary readings; the latter states plainly that retention and truncation are undocumented.
  It also reports, from re:Invent 2024, that the Journal replicates to three AZs or regions,
  commits on two of three regions, performs reads from at least two regions, and has the third
  region relay a write between a partitioned pair; it notes that what a region member is made of
  is undocumented.

Cells and static stability:

- AWS Well-Architected, "Reducing the Scope of Impact with Cell-Based Architecture",
  <https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/what-is-a-cell-based-architecture.html>:
  cells contain overload and bad deployments and are not a failover domain; multi-AZ cells; the
  thinnest possible router routing on its cached map; range-based mapping; migration as copy,
  flip, redirect, forget; multiple cells and migration from day one; shuffle sharding inside a cell
  (its FAQ). Transcript: `docs/references/papers/aws-cell-based-architecture/transcript.md`.
- Amazon Builders' Library, "Static stability using Availability Zones",
  <https://aws.amazon.com/builders-library/static-stability-using-availability-zones>: a data plane
  that keeps serving while its control plane is down.
- FoundationDB's metacluster, deleted with multitenancy in apple/foundationdb commit `bab7637d8`
  on 2025-12-09 as an unowned, unfinished community contribution, not for a design failure; read
  at the parent commit, <https://github.com/apple/foundationdb/tree/bab7637d8%5E/metacluster>:
  `metacluster/include/metacluster/MetaclusterTypes.h` (tenant and cluster states, tenant groups,
  tenant locks), `MetaclusterMetadata.h` (the id prefix, the capacity index, tombstones),
  `CreateTenant.actor.h` (the resumable creation state machine), `RestoreCluster.actor.h` (restore
  in both directions, dry run, the restore id), `MetaclusterConsistency.actor.h` (the consistency
  checker) and `fdbclient/include/fdbclient/MetaclusterRegistration.h` (registration on both
  sides, verified on every step).

Bootstrap, identity and membership:

- CockroachDB, `cockroach init` and `cockroach start`,
  <https://www.cockroachlabs.com/docs/stable/cockroach-init>,
  <https://docs.cockroachlabs.com/docs/stable/cockroach-start>: start-and-wait plus a one-time
  init, `--join` stored and still passed on every start.
- Redpanda, `empty_seed_starts_cluster`,
  <https://docs.redpanda.com/docs/reference/node-configuration-sample>: implicit formation disabled
  in production.
- Kafka KIP-899 and KIP-1102, client rebootstrap,
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-899:+Allow+producer+and+consumer+clients+to+rebootstrap>,
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1102:+Enable+clients+to+rebootstrap+based+on+timeout+or+error+code>.
- Akka cluster membership, <https://doc.akka.io/docs/akka/current/typed/cluster-membership.html>:
  `host:port:uid` identity, a new incarnation at the same address removes the old member.
- Orleans cluster management,
  <https://learn.microsoft.com/dotnet/orleans/implementation/cluster-management>.
- Balakrishnan et al., "Virtual Consensus in Delos", OSDI 2020,
  <https://www.usenix.org/system/files/osdi20-balakrishnan.pdf>: clients refresh their view only
  when an append fails on a sealed loglet.
- Suresh et al., "Rapid", USENIX ATC 2018,
  <https://www.usenix.org/conference/atc18/presentation/suresh>.
- ScyllaDB, node-failure recovery,
  <https://docs.scylladb.com/manual/stable/troubleshooting/handling-node-failures.html>: host-id
  identity, the cluster id in gossip, the recovery leader picked by host id.

Checkpoints:

- Apache Pulsar PIP-14, "Topic compaction",
  <https://github.com/apache/pulsar/wiki/PIP-14:-Topic-compaction>, and the topic compaction docs,
  <https://pulsar.apache.org/docs/concepts-topic-compaction>: the compacted ledger and the
  compaction horizon.
- Kafka KIP-630, "Kafka Raft Snapshot",
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-630:+Kafka+Raft+Snapshot>, and KIP-876,
  "Time based cluster metadata snapshots",
  <https://cwiki.apache.org/confluence/display/KAFKA/KIP-876:+Time+based+cluster+metadata+snapshots>.
- Redpanda architecture, the controller partition and its snapshots,
  <https://docs.redpanda.com/current/get-started/architecture/>.
- Balakrishnan et al., "CORFU: A Shared Log Design for Flash Clusters",
  <https://www.cs.fsu.edu/~awang/courses/cop5611_s2024/corfu.pdf>, and the NSX / CorfuDB
  troubleshooting note on checkpoint failures for large tables,
  <https://knowledge.broadcom.com/external/article/378470/troubleshooting-vmware-nsx-datastore-cor.html>.

Zones:

- Ailijiang, Charapko, Demirbas, Kosar, "WPaxos: Wide Area Network Flexible Consensus", IEEE
  TPDS 2019, <https://arxiv.org/abs/1703.08905>: §3.1 the per-zone quorums `fz`, `fn`; §3.2 to
  §4 object stealing; §5.1 the majority-zone leader policy; §5.3 degraded operation and
  reconfiguration. The printed TLA+ quorum definition does not intersect; only the floor form the
  proof uses is sound (the counterexample is in section 5). Transcript:
  `docs/references/papers/wpaxos/transcript.md`.
- Nawab, Agrawal, El Abbadi, "DPaxos: Managing Data Closer to Users for Low-Latency and Mobile
  Applications", SIGMOD 2018, <https://www.nawab.me/Uploads/Nawab_DPaxos_SIGMOD2018.pdf>: leader
  handoff, adopted in a stricter form (`docs/analysis/consensus/dpaxos-leader-handoff.md`);
  expanding quorums, which Matchmaker Paxos already provides with the intent made durable at the
  matchmakers before Phase 1 (the matchmakers' watermark is the intent's GC); delegate and
  leader-zone quorums, rejected (section 11). Transcript:
  `docs/references/papers/dpaxos/transcript.md`.

Tokens:

- Biscuit, <https://www.biscuitsec.org/>, and `biscuit-auth` 6.0.0
  (<https://github.com/eclipse-biscuit/biscuit-rust>, read at `01778d2`): offline attenuation,
  Datalog authorization, the seeded-RNG constructors, the wall-clock `RunLimits::max_time`
  (`datalog/mod.rs`), `AuthorizerBuilder::time()` reading `SystemTime::now()`, and the
  `HashMap`-backed fact and rule sets.

Compartmentalized Paxos and Matchmaker Paxos are in `docs/references/papers/`.
