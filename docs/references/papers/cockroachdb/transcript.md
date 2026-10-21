# CockroachDB: The Resilient Geo-Distributed SQL Database

**Authors:** Rebecca Taft, Irfan Sharif, Andrei Matei, Nathan VanBenschoten, Jordan Lewis,
Tobias Grieger, Kai Niemi, Andy Woods, Anne Birzin, Raphael Poss, Paul Bardea, Amruta Ranade,
Ben Darnell, Bram Gruneir, Justin Jaffray, Lucy Zhang, Peter Mattis (Cockroach Labs, Inc.)

**Date:** SIGMOD 2020, June 14–19, 2020, Portland, OR, USA. 17 pages (pp. 1493–1508).
**DOI:** <https://doi.org/10.1145/3318464.3386134>
**Source:** <https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf>
(CC BY-SA 4.0). Added on 2026-10-21 at Pierre's request, for the tenant and journal move design
of #232 (multiple cells).

---

## Abstract (in full)

"We live in an increasingly interconnected world, with many organizations operating across
countries or even continents. To serve their global user base, organizations are replacing their
legacy DBMSs with cloud-based systems capable of scaling OLTP workloads to millions of users.
CockroachDB is a scalable SQL DBMS that was built from the ground up to support these global OLTP
workloads while maintaining high availability and strong consistency. Just like its namesake,
CockroachDB is resilient to disasters through replication and automatic recovery mechanisms.

This paper presents the design of CockroachDB and its novel transaction model that supports
consistent geo-distributed transactions on commodity hardware. We describe how CockroachDB
replicates and distributes data to achieve fault tolerance and high performance, as well as how
its distributed SQL layer automatically scales with the size of the database cluster while
providing the standard SQL interface that users expect. Finally, we present a comprehensive
performance evaluation and share a couple of case studies of CockroachDB users. We conclude by
describing lessons learned while building CockroachDB over the last five years."

---

## 1. Introduction

The motivating customer is a company with users in Europe, Australia and the US. Its
requirements: personal data of European users must be **domiciled within the EU (GDPR)**; data
should sit close to the users that access it most and "follow them (within regulatory limits)
if they travel"; the DBMS must survive "a full regional failure"; and it must offer SQL with
serializable transactions. CRDB (CockroachDB) answers with three features:

1. **Fault tolerance and high availability:** at least three replicas of every partition across
   diverse geographic zones, with automatic recovery when a node fails.
2. **Geo-distributed partitioning and replica placement:** horizontal scaling that "automatically
   increas[es] capacity and migrat[es] data as nodes are added", default placement heuristics,
   and fine-grained user control "for performance optimization or as part of a data domiciling
   strategy".
3. **High-performance transactions:** serializable isolation over multi-partition transactions,
   with NTP-grade clocks only.

## 2. System overview

### 2.1 Architecture

Shared-nothing: every node stores and computes, and a client can connect to any node. Each node
is layered:

- **SQL** (§5): parser, optimizer, execution; it sees one monolithic KV store.
- **Transactional KV** (§3, §4): atomicity and isolation across keys.
- **Distribution:** one ordered logical key space, range-partitioned into **Ranges of about
  64 MiB**, indexed by a two-level structure inside system Ranges that are cached aggressively.
  "Ranges are ~64 MiB because it is a size small enough to allow Ranges to quickly move between
  nodes but large enough to store a contiguous set of data likely to be accessed together."
  Ranges split when too large, merge when too small, and **split on load** to reduce hot spots.
- **Replication:** each Range has three replicas by default, each on a different node.
- **Storage:** RocksDB, treated as a black box.

### 2.2 Fault tolerance and high availability

**2.2.1 Replication using Raft.** The replicas of a Range form one Raft group with a long-lived
leader. The unit of replication is a command: "a sequence of low-level edits to be made to the
storage engine". Each replica applies commands as Raft declares them committed.

**Leases.** One replica (usually the Raft leader) is the **leaseholder**: "the only replica
allowed to serve authoritative up-to-date reads or propose writes to the Raft group leader".
Because every write goes through it, reads skip the Raft round trip. User Range leases are tied
to **node liveness**: each node heartbeats a record in a system Range every 4.5 s. System Ranges
use expiration-based leases renewed every 9 s. A replica that sees the leaseholder is not live
tries to take the lease. "To ensure that only one replica holds a lease at a time, lease
acquisitions piggyback on Raft": the acquirer commits a lease-acquisition log entry, and "lease
acquisition requests include a copy of the lease believed to be valid at the time of request"
(a compare-and-set through the log).

**2.2.2 Membership changes and automatic load (re)balancing.** Added nodes, removed nodes, and
temporary or permanent failures are treated alike: "they all cause load to be redistributed
across the new and/or remaining live nodes".

- **Short failures:** Raft keeps working with a majority. A replica that comes back is caught up
  by its peers "by either (1) sending a snapshot of the full Range data, or (2) sending a set of
  missing Raft log entries to be applied. The method used is determined based on the number of
  writes that occurred while the replica was unavailable."
- **Long failures:** CRDB "automatically creates new replicas of under-replicated Ranges (using
  the unaffected replicas as sources)", placed as in §2.2.3. Node liveness and cluster metrics
  travel by peer-to-peer gossip.

The paper does **not** describe the steps of a single replica move (it does not name learner
replicas or a promote step). §7.1.2 gives the one rule it states about moves: use joint
consensus.

**2.2.3 Replica placement.** Two mechanisms:

- **Manual:** nodes carry attributes for capability (hardware, RAM, disk type) and locality
  (country, region, availability zone). Tables carry placement **constraints and preferences** in
  their schema; for example a `region` column defines the partitioning and maps partitions to
  regions.
- **Automatic:** replicas spread across failure domains "(while adhering to the specified
  constraints and preferences), to tolerate varying severities of failure modes (disk, rack, data
  center, or region failures)", plus heuristics that balance load and disk use.

### 2.3 Data placement policies

- **Geo-partitioned replicas:** each partition is pinned to one region. Fast reads and writes in
  that region, survives an AZ loss, unavailable on a region loss. "This policy can also be used
  for enforcing data domiciling requirements."
- **Geo-partitioned leaseholders:** only the leaseholder is pinned to the access region, the other
  replicas sit in other regions. Fast local reads, survives a region loss, slower cross-region
  writes.
- **Duplicated indexes:** one copy of an index per region, each with its leaseholder pinned
  there. Fast local reads and region survival, at the cost of write amplification.

## 3. Transactions

MVCC with serializable isolation. A transaction starts at the **gateway** node of the SQL
connection, which acts as its coordinator.

**3.1 Coordinator and leaseholder (Algorithms 1 and 2).** Two optimizations let many
multi-statement transactions finish "with the latency of just one round of replication":

- **Write pipelining:** an operation that overlaps no in-flight one runs at once; the coordinator
  tracks in-flight operations and their dependencies. A dependency is a "pipeline stall".
- **Parallel commits:** a *staging* transaction status makes the commit conditional on all writes
  being replicated, so the staging record and the outstanding writes replicate in parallel. The
  coordinator acknowledges, then records the explicit commit asynchronously. Verified in TLA+
  (atomicity: every staging transaction ends committed or aborted, whatever the coordinator does;
  durability: committed stays committed). Figure 2: up to 72% more throughput, up to 47% lower p50.

The leaseholder verifies its lease, takes **latches** on the keys of the operation and of its
dependencies, checks the dependencies replicated, pushes a write's timestamp past the key's
highest read timestamp, **evaluates** the operation into a low-level command, answers early when
the operation does not commit, then replicates and applies.

**3.2 Atomicity.** Provisional values are **write intents**: MVCC values with metadata that
points to a **transaction record** (pending, staging, committed, aborted), stored in the Range of
the transaction's first write. Long transactions heartbeat their record. A reader that meets an
intent reads the record: committed is a value, aborted is ignored and cleaned, pending blocks, an
expired record is aborted. A staging record is resolved by trying to prevent one of its writes
from replicating; if all replicated, it is committed.

**3.3 Concurrency control.** Write-read: a read below an uncommitted intent waits (in-memory
queues); above it, it ignores it. Read-write: a write below a later read moves its commit
timestamp past it. Write-write: wait for a lower intent, move past a higher committed value;
distributed deadlock detection aborts one transaction of a cycle.

**3.4 Read refreshes.** Moving a read timestamp from `ta` to `tb` is valid if no key of the read
set changed in `(ta, tb]`; a refresh request re-scans the read set. Like PostgreSQL SSI, false
positives are allowed. A transaction that delivered no results is retried internally.

**3.5 Follower reads.** `AS OF SYSTEM TIME` reads can be served by non-leaseholders. The follower
must know that no later write can land at or below `T`, and that it holds the log prefix for the
snapshot at `T`. Each leaseholder emits a **closed timestamp** ("the timestamp below which no
further writes will be accepted"), exchanged with Raft log indexes, generated per node for
efficiency. Closed timestamps trail now by about 2 s. A node forwards such a read to the closest
node with a replica, by measured latency.

## 4. Clock synchronization

**4.1 Hybrid-logical clocks.** Physical time plus a Lamport component; maximum offset 500 ms by
default. Three properties: causality tracking on every message; strict monotonicity across
restarts (wait out the maximum offset at start); self-stabilization against transient skew. The
**lease disjointness invariant**: "for each Range, each lease interval is disjoint from every
other lease interval. This is enforced on cooperative lease handoff with causality transfer
through the HLC and is enforced on non-cooperative lease acquisition through a delay equal to the
maximum clock offset between lease intervals."

**4.2 Uncertainty intervals.** CRDB gives **single-key linearizability**, not strict
serializability. A transaction gets `[commit_ts, commit_ts + max_offset]`; a value inside that
window triggers an uncertainty restart above it, with the upper bound fixed.

**4.3 Behaviour under clock skew.** Raft has no clock dependency, but leases do: under heavy skew
two nodes can believe they hold the lease. Two safeguards keep isolation whatever the skew:

1. A lease has a start and end timestamp; a leaseholder serves no read above and no write outside
   its interval.
2. "Each write to a Range's Raft log includes the sequence number of the Range lease that it was
   proposed under. Upon successful replication, the sequence number is checked against the
   currently active lease. If they do not match, the write is rejected. Because lease changes for
   a Range are themselves written to the Range's Raft log, only a single leaseholder is ever able
   to make changes to a Range at a time."

Skew beyond the bound can still break single-key linearizability (stale reads through skewed
gateways). A node whose offset passes 80% of the maximum against a majority of nodes
self-terminates.

## 5. SQL

**5.1 Data model:** every table and index is stored in Ranges; a primary index keyed on the
primary key; secondary indexes store the primary key; hash indexes against hot spots.
**5.2 Optimizer:** Cascades-style, over 200 rules written in the Optgen DSL compiled to Go;
distribution-aware (infers filters from partitioning, costs index replicas by distance to the
gateway). **5.3 Execution:** gateway-only or distributed (read-only queries only at the time);
physical planning puts TableReaders on the nodes holding the Ranges and pushes filters, joins and
aggregations to them; a Volcano row-at-a-time engine and a vectorized engine (up to 4x on TPC-H).
**5.4 Schema changes:** online, F1's protocol: each change is a sequence of incremental versions,
at most two successive versions live at a time.

## 6. Evaluation

- **6.1 Scalability:** Sysbench throughput per vCPU stays nearly constant from 2 to 36 vCPUs and
  from 3 to 48 nodes. TPC-C with remote warehouses scales linearly; replication costs up to 48%
  (3 replicas) or 57% (5 replicas). TPC-C 100,000 warehouses: 1,245,462 tpmC at 98.8% efficiency
  on 81 nodes; single-master Aurora reaches 7.3% efficiency at 10,000 warehouses.
- **6.2 Multi-region availability:** 9 nodes over 3 US regions, an AZ failure then a region
  failure. Every policy survives the AZ loss; only geo-partitioned leaseholders survives the
  region loss, at the cost of higher p90 in steady state. "The slower recovery period is due to
  the primary region catching up on missed writes."
- **6.3 Spanner:** higher YCSB throughput on most workloads and lower latency, partly attributed
  to Spanner's commit-wait; workload A (zipfian updates) does not scale.
- **6.4 Case studies:** a telecom virtual agent on a hybrid on-prem and AWS cluster with
  geo-partitioned leaseholders; an online gaming platform pinning user data per locality for
  compliance.

## 7. Lessons learned

**7.1 Raft made live.**

- **7.1.1 Reducing the chatter:** one heartbeat message per node pair instead of per Range, and
  **paused (quiesced) Raft groups** with no recent writes.
- **7.1.2 Joint consensus (verbatim core):** "Raft's default membership change protocol is simple
  to implement but allows for only a single addition or removal of a member at a time. It turns
  out that this is problematic for availability guarantees during rebalancing operations (i.e.
  moving a replica from one node to another). For example, in a three-region deployment
  constrained to one replica per region, rebalancing requires either (1) temporarily dropping
  down to two replicas, or (2) temporarily increasing to four replicas, with two in one region.
  Both intermediate configurations lose availability during a single region outage. To solve
  this problem, we implemented atomic replication changes (called Joint Consensus) [...]. In
  Joint Consensus, an intermediate configuration exists, but requires instead the quorum of both
  the old and new majority for writes; this means unavailability will result only if either the
  old or new majority fails." They recommend joint consensus for every production Raft system.

**7.2 Removal of snapshot isolation:** safe SNAPSHOT needs pessimistic locking on every update,
SERIALIZABLE included, so `SNAPSHOT` became an alias of `SERIALIZABLE`.

**7.3 Postgres compatibility:** the dialect and wire protocol helped adoption, but clients must
still retry and page, so CRDB-specific drivers are considered.

**7.4 Pitfalls of version upgrades:** an upgrade is a rolling restart; a mixed-version cluster is
a source of serious bugs. Early CRDB replicated requests and evaluated them on each replica, so
code changes made replicas diverge across versions. The fix: **evaluate first, then propose the
effect** of the request, not the request.

**7.5 Follow the workload:** a mechanism that moves leaseholders closer to their users. "We've
found it to be rarely used in practice. CRDB's manual controls over replica placement prove
sufficient for most operators [...]. Adaptive techniques in databases are difficult to get right
for a general purpose system, and are either too aggressive or too slow to respond. Operators
favor consistency in performance; the unpredictability in this dynamic scheme hindered adoption."

## 8. Related work (condensed)

Spanner (strict serializability through read locks and commit-wait on specialized clocks);
Calvin, FaunaDB, SLOG (deterministic, no conversational SQL); H-Store, VoltDB, L-Store, G-Store;
data placement work (latency, availability, cost); Slicer (load-based splits and merges of hashed
ranges); Aurora, F1, TiDB, NuoDB, FoundationDB.

## 9. Conclusion

Serializable isolation at scale without special hardware; consensus replication with leaseholder
and follower reads; geo-partitioning and follow-the-workload to keep data near users; a full SQL
layer.

---

## Why this matters for paros

- **A move is a configuration change that never drops below its survival goal** (§7.1.2). Raft's
  one-at-a-time change forces a 2-replica or a 4-replica step, and both lose a region. paros does
  not have this problem: a Matchmaker reconfiguration goes from `C_old` to `C_new` in one round,
  and the next leader needs a Phase-1 quorum of every configuration in `H_b` until GC. That is
  the same guarantee as joint consensus (a quorum of both), without a joint entry in the log.
  The tenant move of #232 (multiple cells) and the rebalance of #687 (rebalance a cell) can move
  all replicas of a journal in one `Reconfigure`.
- **New replicas are built from the healthy ones, by snapshot or by log** (§2.2.2). The paper
  does not describe a learner step. A warm copy before the reconfiguration (non-voting replicas
  in the target, WPaxos §5.2) is a paros design choice for #232, not something this paper states.
- **Lease handoff is cooperative when it can be, and fenced by the log in all cases** (§4.1,
  §4.3): the lease sequence number in every log entry is the same idea as paros's leader uuid
  fence on `Write` and `Truncate` (#241), and the cooperative handoff matches `relinquish_to`
  (DPaxos §4.4).
- **Follow the workload was rarely used** (§7.5). This is a warning for #581 (the leader follows
  its writer): keep the policy predictable, with hysteresis, and keep manual placement first.
- **Placement constraints and data domiciling** (§2.2.3, §2.3): node attributes plus per-table
  constraints is the model of #714 (tenant residency) and the zone and region rules of #253
  (multi-region cells). Geo-partitioned leaseholders matches a multi-region cell with the leader
  pinned to the writer's region.
- **Fewer heartbeats, paused groups** (§7.1.1): the coalesced heartbeats and quiescence of #195
  (scale work for many journals).
- **Follower reads with a closed timestamp** (§3.5): compare with #594 (opt-in sequential reads
  for tailers); paros has no clock in its safety argument, so its form is a position, not a time.
- **Propose the effect, not the request** (§7.4): relevant to #363 (rolling upgrades); paros
  journals already carry opaque bytes that the journal does not evaluate.
