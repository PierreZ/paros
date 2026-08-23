# Aurora DSQL: Scalable, Multi-Region OLTP

**Authors:** Marc Brooker, Marc Bowes, Mike Hershey, Zak van der Merwe, James Morle, Matthys Strydom (all Amazon Web Services)

**Date:** arXiv:2607.13276v2 [cs.DB], submitted 14 Jul 2026, revised 16 Jul 2026.
**Source:** https://arxiv.org/pdf/2607.13276

> Terminology note from the paper's own footnote: "Aurora DSQL" / "DSQL" means the system described here; "Aurora" means Aurora MySQL and Aurora PostgreSQL, *a significantly different system*.

---

## Abstract

Aurora DSQL is a serverless SQL database designed for cloud-scale transaction processing with multi-region active-active capabilities. Built on a **disaggregated architecture**, DSQL separates compute, storage, and transaction coordination into independent, horizontally scalable services. Query processors run in Firecracker MicroVMs executing PostgreSQL-compatible SQL without local state. The system uses **multiversion concurrency control with precision timestamps** for coordination-free reads and **optimistic concurrency control** for writes, deferring coordination to commit time through distributed **adjudicators** and the **Journal** replication system. This minimizes cross-region latency by requiring coordination only during commits, not individual statements. DSQL enables elastic scaling from zero to millions of transactions per second while providing strong consistency, ACID transactions, and continuous availability during availability zone or region failures.

---

## 1. Introduction

Amazon has a long history of scalable database systems — Dynamo (2000s); DynamoDB, Aurora, and Redshift (early 2010s); Aurora Serverless and MemoryDB (2020s). Despite that, AWS lacked a **horizontally scalable, scale-out, OLTP-optimized SQL database**.

Product goals, drawn from a decade of AWS database customer feedback:

- **Serverless** — fully managed, fully serverless (like DynamoDB or S3): no infrastructure to manage, monitor, or patch.
- **Familiar** — an interface developers already know: the same clients, tools, and SQL dialect. Familiarity extends past the API to isolation and transaction semantics, data types, and other SQL concepts.
- **Scalable** — up to millions of transactions per second, and *down to zero*.
- **Multi-region active-active** — active-active architectures spanning AWS regions across continents, with strong consistency, fast failover, and no data loss.
- **Strongly consistent** — applications shouldn't have to reason about eventual consistency; they should read and write at any scale with strong consistency.

The overarching aim is a relational database that simplifies application building and operations — a "database of first resort," simple at low scale and growing with the application without adding complexity.

**Figure 1 (single-region application architecture, described):** regional DNS in front of three load balancers, each fronting an application stack in a separate availability zone (AZ), all backed by one **regional DSQL endpoint**. Stateless auto-scaling compute across three datacenters tolerates the loss of an entire AZ with no downtime, and has been proven to scale to millions of requests per second. Concurrency and durability are the database's responsibility; the application fleet is just stateless, independent service hosts. Host failures are handled by load-balancer health checks, datacenter-scale failures by DNS-level health checks.

**Figure 2 (multi-region application architecture, described):** the same picture duplicated in Region A (active) and Region B (active), each with its own regional DNS, three LBs, three application stacks, and a regional DSQL endpoint — with **global DNS** above both. The application tolerates the loss of an entire region without downtime, operator intervention, or data loss, while the programmer writes the same business logic against the regional endpoint.

The paper focuses on **SQL execution, replication, and transactions**, leaving on-disk storage, encryption, and query optimization to later publications.

---

## 2. Architecture Overview

DSQL's architecture is **disaggregated**: multiple independent services, each with a small number of well-defined concerns, communicating through carefully specified APIs with clear contracts. This yields three properties: independent change/improvement per component, independent scaling per component, and different security isolation decisions per component. Crucially, **no component is a singleton** — everything scales horizontally.

**Figure 3 (DSQL architecture overview, described):** Query Processors at the top; below them Adjudicators sharded by key range (`[n..z]`, `[a..m]`) with a Clock; each adjudicator writing to a Journal; Journals feeding a Crossbar; the Crossbar feeding Storage nodes sharded by range (`[a..f]`, `[g..s]`, `[t..z]`). Two paths are drawn: the **read path** (query processor → storage) and the **commit path** (query processor → adjudicator → Journal → crossbar → storage).

Component roles:

- **Query Processors (QPs)** handle the customer-facing side: execute SQL, return data for reads, **buffer writes locally**, and coordinate the transaction protocol. Each runs in a Firecracker MicroVM. QPs scale vertically (techniques from Aurora Serverless) and horizontally by adding a new QP per active connection.
- **Storage nodes** are the foundation for data access, holding both table data and indexes. Each stores a **range** of data by shard key. QPs ask for data *as of a specific timestamp*, enabling a coordination-free read path. Storage scales in two dimensions: enough shards to absorb all writes to a table, and within each shard, as many **read replicas** as read traffic demands.
- **Writes** (UPDATE, INSERT, …) are handled locally inside the QP, expanding to the rest of the write path only at commit.
- **Adjudicators** decide whether transactions may commit while maintaining isolation guarantees, and control transaction order and commit time. They scale by sharding: **each key belongs to at most one adjudicator at any given time**, distributing conflict detection. Adjudicator sharding and storage sharding are **unrelated**, so read heat and write heat shard independently (a low-write/high-read database can have one adjudicator shard and many storage shards).
- **Journal** is an internal AWS replication component used in many systems including **S3, DynamoDB, and MemoryDB**. Each Journal is a **durable, ordered, atomic data stream**. Single-region: a Journal commit means data is durable in two or more AZs. Cross-region: durable in two or more AWS regions. When a transaction commits it is written to a Journal, making it permanent and propagating it everywhere it must be visible. **Journals scale per transaction**: each transaction's writes go atomically to a *single* journal, but there can be as many journals as needed.
- **Crossbars** take data from Journals — totally ordered on each Journal — and **merge-sort** them into a total order per subscriber shard, then divide that ordered stream into shards aligned with the storage partitioning. Crossbars scale with the number of storage nodes.

Disaggregation was chosen not only for scalability but for durability, fault tolerance, and availability: there is **no single point of failure** — no single machine, network link, or datacenter failure can bring down a DSQL cluster.

Each component exposes a narrow interface:

- QPs accept SQL and produce reads against storage and **write sets** for adjudicators;
- adjudicators accept write sets and return **commit decisions**;
- Journals accept committed transactions and produce **ordered streams**;
- crossbars consume Journal streams and produce **per-shard change feeds** for storage.

### 2.1 Multi-tenancy and Security

DSQL is multi-tenant; each physical machine typically serves thousands of customer workloads. "Server" in the paper (e.g. "storage server") means a **logical** construct, many of which live on one physical machine.

Multi-tenancy (many uncorrelated workloads on the same hardware) and **soft allocation** (allocating CPU and memory on demand) are key to DSQL's economics, as they are to AWS Lambda and Aurora Serverless. Packing workloads reduces the peak-to-average load ratio on each machine approximately at the rate of √(number of loads). Since cost scales with peak and revenue with average, the economic benefit is obvious. There is also a customer benefit: sharding and replication handle long-term heat changes (section 7), while soft allocation handles **short-term** shape changes locally.

Security isolation: **process isolation** where AWS controls the code and it is written in a memory-safe language; **VM isolation using Firecracker** where the code is open source or written in an unsafe language (as with the PostgreSQL engine).

### 2.2 Optimizing for Coordination

A guiding principle: **minimize coordination between components**, and strongly avoid coordination that is not constant time with respect to scale. Coordination minimization helps scalability and availability, but the primary concern is **latency**.

- **Reads** happen between client, a QP, and one or more storage replicas — typically in the same AZ, always in the same region. High-quality clocks and physical time avoid the need to coordinate with other readers or writers.
- **Writes**, and the read-modify-writes ubiquitous in OLTP SQL, also stay in the same AZ and region.
- **Coordination is required only at commit** — for concurrency control, consistency, and durability. In the multi-region setting the two tasks needing cross-region communication (coordination for consistency/isolation, and replication for durability) are performed in **a single round of communication**. The same is true of cross-AZ communication in the single-region setting.

To achieve this minimal communication, DSQL uses **Optimistic Concurrency Control (OCC)**, a variant of Kung and Robinson's classic scheme. The usual OCC downsides are avoided two ways:

1. **MVCC** ensures all reads come from a consistent snapshot, so transactions are never aborted for having read old data.
2. **Snapshot isolation rather than serializability**, removing the need to abort on read-write conflicts.

By contrast, a pessimistic approach like Spanner's or CockroachDB's requires **continuous coordination during transaction execution** (typically per statement) to maintain distributed lock state.

Another practical advantage of OCC: **clients can never block other clients**. At scale, performance cross-talk between clients is a significant contributor to tail latency in apps backed by pessimistic relational databases (e.g. a client pausing for garbage collection while holding a write lock). Operational experience at Amazon shows lock contention and lock-acquisition retries are frequent contributors to outages and to **metastable failures** with long recovery times. The paper notes post-mortems for cases where "operators went out to lunch while holding locks."

### 2.3 Multi-Region Architecture

**Figure 4 (multi-region architecture, described):** Frontends over Query Processors in each of Region A and Region B; Adjudicators sharded `[a..m]` and `[n..z]`; two Journals drawn spanning the regions, one with its **head** in Region A and **tail** in Region B and the other reversed; Crossbars and Storage in each region; Region C shown as a third storage/region participant.

- **Read path is nearly unchanged.** Read-only transactions and the read portions of read-write transactions are served from storage in the QP's own region. The only difference: storage nodes must wait to see that **journals from all regions have advanced past the transaction's read timestamp** before serving data, ensuring consistent reads even after region failures.
- **Write path changes most.** Writes still buffer locally in QPs and commit through adjudicators, but the Journal now replicates transaction data **across multiple regions**. Journal commits ensure durability in two or more AWS regions rather than two or more AZs, and this cross-region replication happens as part of the commit protocol — **a single round of cross-region communication for both consistency and durability**.
- **Adjudicator placement becomes a control plane optimization problem.** Each adjudicator shard is placed in a specific region to minimize cross-region traffic. Straightforward for failover architectures with one active region; for active-active workloads with keys spread across regions, optimal placement may not always be possible.

DSQL chooses **consistency over availability** when necessary — **PC/EC** in Abadi's PACELC taxonomy. It remains both strongly consistent *and* available to clients on the majority side of a partition: if Region C in Figure 4 failed, DSQL stays consistent and available to Region A clients and keeps data durable in Region B.

### 2.4 Snapshot Isolation and Strong Consistency

DSQL's default (and currently only) isolation level is **strong snapshot isolation** — snapshot isolation *plus linearizability*. Advantages:

1. **Familiar**: equivalent to PostgreSQL's `REPEATABLE READ`.
2. **Transactions abort only on write-write conflicts** (other concurrent transactions wrote the same keys), not on read-write conflicts as serializability would require. In OLTP SQL, reads are far more common than writes — most writes (all UPDATEs, INSERTs with unique indexes, etc.) are *also* reads — so this lower isolation level means significantly lower abort rates for common patterns.

The downside is **write skew anomalies**. Practically, the most important effect is limiting which business constraints transactions can enforce. Working with customers, AWS found that giving them tools — schema designs that force write-write conflicts for business-logic violations, and `FOR UPDATE` — enabled correct applications at snapshot isolation. Aurora customer data supports the choice: few choose `SERIALIZABLE` in production, most opting for `REPEATABLE READ` or even `READ COMMITTED`.

DSQL does **not** currently support `READ COMMITTED`. This is a simplification snapshot isolation enables: because all reads in a transaction see a fixed snapshot at `τ_start`, there is no need for a mechanism like PostgreSQL's `EvalPlanQual`, which re-evaluates row visibility mid-statement when concurrent transactions commit. Avoiding that complexity reduces the surface area for subtle concurrency bugs in both the database and applications.

### 2.5 Impact of Architectural Properties on Quantitative Performance

Reducing round trips matters at continent scale. Over one week in 2022, the network RTT between **us-east-1** (Virginia) and **us-west-2** (Oregon) had mean **62.17 ms**, p50 **62.4 ms**, p99 **63.30 ms**, p99.99 **64.99 ms**. In a DSQL cluster spanning those regions this latency is incurred **once per transaction**. Better still: the Journal only needs to commit to **two regions of three**, so in a us-east-1 / us-west-2 / us-east-2 setup, commit time is the time to the *closest second region* — the us-east-1 ↔ us-east-2 RTT has p50 **11.5 ms** and p99 **14.4 ms**.

**Figure 5 (described):** normalized average transaction latency vs. number of statements per transaction, comparing DSQL in two client regions (dsql-r1, dsql-r2) against "Competitor A" in the same two regions (A-r1, A-r2), for transactions with increasing numbers of UPDATE statements. Competitor A's pessimistic locking design shows a **linear** rise from the extra round trips needed to maintain lock state; DSQL's latency is flat and **identical in both regions**.

**Figure 6 (described):** DSQL p99 latency for COMMIT, SELECT, and UPDATE across three configurations — us-east-1, us-east-2 (two-region cluster with a witness in us-west-2), and us-east-2 (SR) single-region:

| Operation | us-east-1 | us-east-2 | us-east-2 (SR) |
|---|---|---|---|
| COMMIT | 29.2 ms | 30.1 ms | 7.4 ms |
| SELECT | 2.1 ms | 1.8 ms | 1.4 ms |
| UPDATE | 3.4 ms | 2.9 ms | 2.3 ms |

Clients in each region get **in-AZ read latency (≈2 ms p99)** for primary-key SELECTs and **≈3 ms p99** for UPDATEs — both *significantly lower than one network round trip* (14.4 ms p99) between the primary region pair, despite strong consistency. COMMIT is ≈30 ms multi-region and 7.4 ms single-region; note ≈30 ms is **faster than the 63 ms p99 round trip to us-west-2**, showing the effect of quorum commit. Once COMMIT completes in the two-region setup, data is durable and available to strongly consistent readers in two regions.

**Figure 7 (described):** p99 COMMIT and SELECT latency for a **read-only** transaction on the same setup — COMMIT 1.4 / 1.1 / 1.1 ms and SELECT 1.8 / 1.3 / 1.4 ms across us-east-1, us-east-2, and us-east-2 (SR). Reads stay fast and local (in-region, even in-AZ) even multi-region. For read-only transactions COMMIT is a **no-op** handled locally by the QP; with an explicit COMMIT, latency is **< 1.5 ms p99**.

Summary: applications need not care whether they are single- or multi-region for read-only workloads, and see identical performance. For read-write workloads the only change is that **COMMIT takes approximately 2× the round-trip time between the nearest pair of regions**.

### 2.6 Comparisons To Other Systems, Briefly

- **FoundationDB** — architecturally the most similar, though with a significantly different approach to logging, atomic commitment, and replication. DSQL adds a scalable SQL engine, predicate pushdown to storage, multi-region, and interactive transactions.
- **Aurora, Neon, AlloyDB** — distributed storage but a **single writable leader**.
- **Vitess, Citus** — scale out single-leader systems by partitioning the key space over leaders, typically sacrificing transaction atomicity and isolation.
- **CockroachDB, Spanner** — similar distributed SQL approach, but **pessimistic** concurrency control, a single leader per write shard with an associated lock table, and replication via **Paxos groups** rather than a disaggregated Journal. DSQL and Spanner are alike in using physical clocks; CockroachDB uses Hybrid Logical Clocks.
- **Calvin and SLOG** — once COMMIT starts, DSQL's approach resembles their deterministic order-based approach.
- **DynamoDB** — transactional NoSQL with similar serverless properties and multi-region support; architecturally closer to Spanner (sharding over Paxos groups) but with timestamp-ordering transactions, not unlike FoundationDB's.
- **MemoryDB** — like DSQL, uses a **disaggregated Journal service** for replication, but has a single writable leader and eventually consistent scale-out reads.

---

## 3. Hosting the SQL Engine

To meet the familiarity goal and reuse decades of implementation work, DSQL uses the **PostgreSQL engine** for SQL parsing, execution, and optimization, and for the core PostgreSQL wire protocol implementation.

Each transaction runs within a customized PostgreSQL engine hosted inside a **dedicated Firecracker MicroVM** — similar to PostgreSQL's process-per-session model, with the extra protection of a hardware-secured VM around each process. MicroVMs are provisioned dynamically, scaling up by adding MicroVMs in the AZs and regions where client connections originate, keeping QPs geographically close to clients — important given the inherently chatty client-database interaction of SQL workloads.

**MicroVM creation is a snapshot restore, not a boot.** The OS and database engine start once; a snapshot of memory, register, and device state is taken; that snapshot is then **cloned and restored** on demand. This substantially reduces restore time and lets unmodified memory pages (kernel and engine code) be shared across QPs for the same cluster via **copy-on-write** — much as they would be shared between processes in PostgreSQL's default `fork()`-based model.

DSQL uses **only specific components** of PostgreSQL: the SQL engine, an adapted query planner/optimizer, and the client protocol. It does **not** use PostgreSQL's storage or transaction processing. DSQL **bypasses the buffer pool** by plugging in at PostgreSQL's **Access Method (AM) layer**: the storage plugin implements the AM interface directly, fetching from DSQL's distributed storage without going through shared buffers or the lock manager. This eliminates buffer pool contention and lock manager overhead while staying compatible with the query executor above the AM interface.

Each QP is an **independent unit that never communicates directly with other QPs** — a shared-nothing architecture that eliminates the usual distributed-SQL coordination overhead while ACID guarantees come from the storage and transaction management layers below. Each QP executes **one transaction at a time**, with statements executing sequentially. Within a statement, reads **fan out to multiple storage shards in parallel**, and at commit the prepare phase is **broadcast to all involved adjudicators concurrently**.

Because QPs hold **no durable state** (writes are buffered locally only until commit), a **QP failure simply aborts the in-flight transaction**, which the client retries on a freshly provisioned MicroVM.

In front of the MicroVMs runs a per-database **proxy** (conceptually like pgbouncer) that terminates the PostgreSQL wire protocol and performs basic authentication and authorization. As sessions start a transaction (e.g. with `BEGIN`) they are matched to an existing MicroVM or a new one is created. When transactions end, MicroVMs return to the per-database pool for reuse, or are destroyed if the system detects excess capacity.

---

## 4. Handling Reads

The read path provides strongly consistent, snapshot-isolated access **without coordination between query processors or storage replicas**, through precision timing plus MVCC.

Every transaction begins by selecting a start time **`τ_start`** using **EC2's precision time infrastructure**, which provides **microsecond-accurate clocks with strong error bounds across all AWS regions**. All reads within the transaction request data **as of `τ_start`**, giving a point-in-time consistent view regardless of concurrent writes or which shards and replicas are accessed.

Storage implements these temporal reads with **MVCC**, keeping multiple versions of each row so historical states are accessible without blocking concurrent operations. Asked for data as of `τ_start`, storage returns the **most recent version of each row committed before that timestamp**. So a transaction sees:

- all data committed **before** `τ_start`,
- **no** data committed after `τ_start`,
- **no** in-flight transactions.

If a storage node is not current up to `τ_start`, **the reader waits** until replication catches up.

Reads **never** require communication with a primary replica or lock server for sequencing — they maintain no lock state. Reads are served from the nearest replica in the same region and AZ, minimizing latency and cross-AZ data transfer cost. The system supports **unlimited read replicas without coordination overhead**, and **readers never block writers or other readers**. This applies to both read-only and read-write transactions, and clients do **not** have to declare a transaction read-only to get the full benefit.

Storage nodes hold a **logical** copy of the data with knowledge of the schema: **rows and index entries, not opaque physical pages** — in contrast with Aurora, which stores physical pages in its distributed storage layer. Each node stores only its shard. The logical interface enables **query pushdown**: rather than requesting pages, QPs request rows and delegate filtering, aggregation, and projection to storage replicas, which can perform index-only scans and complex filtering directly — moving computation to data rather than data to computation.

**The absence of large coherent caches** in the compute layer is another deliberate choice: avoiding cache coherence protocols and their coordination lets compute scale independently. Slowly-changing but frequently-read data such as the **catalog** is cached inside every QP to avoid round trips; coherence of that smaller cache is provided by the **transaction commit protocol**. By comparison, Aurora PostgreSQL relies heavily on a large local cache on its single writer, a trait shared by most single-writer designs; the lack of cache coherence there is exposed to clients as **eventual consistency** in exchange for read scale. Coherent cache designs like ScaleStore and its precursors (including Rdb/VMS in the early 1990s) require tightly coupled clusters, limiting scalability and fault tolerance beyond a single datacenter.

**For read-only transactions COMMIT is a no-op** — the QP simply forgets the transaction. No lock release, no further storage communication. **Read-only transactions never abort.** If a storage node fails, in-flight reads are retried against another replica of the same shard; a replacement node is recreated from **S3 and the Journal** and can begin serving reads once caught up to the current Journal position.

### 4.1 Shard Scheme

Two ways to spread data over shards: **ranges** or **hashing**.

- **Hash partitioning** (DynamoDB, Dynamo, many others) intentionally destroys spatial locality, spreading heat over the key space but losing locality optimizations — notably making in-order scans much chattier.
- **Range-based partitioning** preserves spatial locality and gives superior performance on access patterns that exploit it, at a higher chance of hot-spotting.

**DSQL is range based**, on the observation that many OLTP SQL workloads have significant spatial locality.

---

## 5. Handling Writes and Commits

Beyond using a consistent snapshot, read-write transactions must check for conflicts to ensure snapshot isolation (which requires coordination), replicate changes for durability, and ensure correct transaction ordering.

The paper's worked example:

```sql
START TRANSACTION;
SELECT name, id FROM dogs;
UPDATE dogs SET latest_treat = now(),
  treats = treats + 1 WHERE id = 5;
COMMIT;
```

A start time `τ_start` is picked and all reads are performed at that timestamp against MVCC storage. When the UPDATE executes it **writes nothing to storage** — it records the planned change **locally inside the Query Processor**, keeping the UPDATE fast, in-region, and in the same AZ as the application.

On COMMIT, three things must happen:

1. Check whether isolation rules allow the commit (or whether it conflicts with concurrent transactions and must abort).
2. Make the results **durable and atomic**.
3. **Replicate** the transaction to all AZs and regions where it must be visible.

The protocol starts by picking the set of adjudicators involved (in the example, one, since only one row is written). A protocol across those adjudicators picks a commit time **`τ_commit`**. To achieve snapshot isolation, **write-write conflicts** must be detected — whether any other transaction wrote the same keys **between `τ_start` and `τ_commit`**. Transactions that committed before `τ_start` don't matter (their effects were already seen); transactions committing after `τ_commit` don't matter (their effects aren't seen), though they may fail to commit because of *our* changes.

If no write-write conflicts are found, one involved adjudicator **writes the transaction to its Journal as a logical post-image**. The Journal ensures this write is **atomic**, and that **no prior transaction has been committed to this Journal with a timestamp later than `τ_commit`** — giving a **per-Journal total transaction order**. The crossbar then reads the post-image from the Journal and distributes it to storage nodes owning the relevant key space.

**The Journal is a commit log** whose payload is ordered, committed transactions **applicable deterministically** — even without a local read-modify-write on the storage node. So **storage replicas need no coordination when consuming the journal**. In this sense DSQL's post-commit portion resembles deterministic databases like Calvin or SLOG; unlike them, DSQL executes **arbitrary interactive SQL transactions**.

DSQL's replication approach differs significantly from DynamoDB, Spanner, and CockroachDB, all of which use **Paxos variants within a fixed group of replicas per shard**. DSQL adds another layer but allows **any number of read replicas of a shard, overlapping shard key spaces, and other flexibility**.

The Journal is also **continuously consumed to create a snapshot of database state in S3**. The combination of recent changes durable in the Journal and older data complete in S3 means **DSQL storage is not involved in durability at all**. This permits a significant optimization: **storage nodes don't need to sync to disk.** They may use local SSDs to spill data too large for memory but need not make it durable. A failed storage node — or a new one for a new read replica or shard — is **recreated from S3 and the Journal**.

One more crucial piece for consistency: storage must know it has seen **all transactions with `τ_commit ≤ τ_start`** before serving reads at `τ_start`. The adjudicator handles this by **promising never to commit transactions at earlier timestamps once it has committed at `τ_commit`**, plus a **heartbeat protocol** where adjudicators move their commit points forward **in lockstep with the physical clock**, so storage knows when its data is complete.

### 5.1 Detecting Conflicts, More Formally

For a commit going to a single adjudicator, committing transaction `A` with start time `τ_start^A` and commit time `τ_commit^A`:

1. Calculate `W_A`, the set of keys written by `A`.
2. Calculate `W_C`, the set of potentially conflicting keys: the union of `W_t` for all committed transactions `t` with `τ_start^A < τ_commit^t < τ_commit^A`.
3. **If `W_A ∩ W_C = ∅` then `A` can commit.**
4. **Promise** to commit no additional transaction `B` with `W_A ∩ W_B ≠ ∅` and `τ_commit^B ≤ τ_commit^A`.

If `W_A`'s keys span multiple adjudicators, step 3 becomes a **yes vote** in the multi-adjudicator protocol. If all of `W_A`'s keys belong to a single adjudicator, that adjudicator can commit the transaction to its Journal directly (writing post-images of all modified rows).

#### 5.1.1 Committing Across Multiple Adjudicators

At scale, transactions write across multiple adjudicators. The cross-adjudicator commit protocol is a **two-phase commit (2PC) variant with inspiration from Warp**. Given the set of adjudicators `A` involved:

1. One adjudicator **`a_l`** is chosen from `A`.
2. The **prepare phase** is broadcast by the query processor to all `a ∈ A`; each forwards its vote and current time `τ_a` to `a_l`, **along with a promise not to commit any conflicting transactions with `τ_commit < τ_max`**, where `τ_max = τ_commit` plus a system-chosen timeout.
3. Once `a_l` has received all positive votes, it calculates **`τ_commit` as the max of all `τ_a`**.
4. **If `τ_commit` exceeds `min(τ_max)` the transaction is abandoned.**
5. Otherwise it is written to **`a_l`'s Journal atomically at time `τ_commit`**, a **go-ahead** message is broadcast to `A`, and the query processor and client are told the good news.

If voting takes too long, an adjudicator fails, or the go-ahead broadcast is not delivered, the other involved adjudicators resume processing conflicting transactions after the timeout `T`.

**Key insight:** "this protocol is not an atomic commitment at all, but rather just an atomic voting on candidacy for commitment." `a_l` may abandon the commit **at any time before writing to the Journal** without affecting correctness. **The actual atomicity of the commitment is handled by the Journal**, which accepts the entire transaction as a single atomic write. Because **only `a_l`'s Journal is written to**, there is **no cross-Journal coordination and no fault-tolerant coordinator**, avoiding the well-known pitfalls of fault-tolerant atomic commitment (Gray & Lamport). The other adjudicators in `A` never write their own Journals for this transaction; their role is limited to **voting on conflicts and temporarily holding promises**. The crossbar then distributes the committed transaction from `a_l`'s Journal to all relevant storage shards, regardless of which adjudicators voted.

**Adjudicator failure.** Failed adjudicators are replaced by standby adjudicators via a **leader election protocol**, which is fast because adjudicators hold very little critical state. In-flight transactions involving a failed adjudicator are aborted:

- if the failed adjudicator is `a_l`, it has **either already written to the Journal** (so the transaction is committed and durable) **or it has not** (so the transaction is safely abandoned);
- if a **non-leader** adjudicator fails, `a_l` will not receive its vote and will abandon the commit.

In both cases the promises held by surviving adjudicators **expire after `τ_max`**, and the query processor can transparently retry the aborted transaction.

**Deadlock.** The protocol is correct for **any arbitrary choice of `a_l`**, but a judicious choice reduces deadlock probability. Deadlock can still occur; the later transaction is then aborted and its commit transparently retried. Because the commit algorithm runs only during commit (not during the transaction itself) and executes very quickly, deadlock is **extremely rare** in evaluated real-world workloads. There is **no hard limit** on how many adjudicator shards a transaction may span, but commit latency grows with the count due to prepare-phase fan-out and the increased probability that `τ_commit` will exceed `min(τ_max)`. In practice OLTP transactions rarely span more than a handful.

### 5.2 Index Maintenance

The change written to the Journal is not only a copy of the modified rows — it is also a **post-image of any changes needed to maintain indexes** on the modified data. In the example, if there were an index on `latest_treat`, the Journal change would additionally remove `id = 5` from the previous value and add it to the new value. **Index changes are committed atomically alongside the modified data at the same `τ_commit`**, ensuring correct transactional and consistent index behavior.

Index changes are computed **by the Query Processor using the current catalog**, before the candidate transaction is submitted to the adjudicator. The adjudicator treats index changes **exactly like regular updates, including for conflict detection** — two transactions inserting the same key into the same unique index conflict, and the later is rejected.

**Blind write optimization.** While index updates are writes, the adjudicator conflict check on those written rows can often be optimized away. The optimization rests on the common structure of secondary indexes: they essentially **insert or remove a row's primary key from a set**. Since write-write conflicts on the primary key are already checked, conflicting set operations are prevented and the remaining operations are **logically monotonic** and can safely be issued in parallel (the CALM principle). Depending on schema, some INSERTs can be optimized the same way.

The optimization applies to **all non-unique secondary indexes**, including partial indexes and indexes with included columns, because their entries are keyed in part by the primary key and therefore cannot conflict independently of the base row. **Unique indexes are excluded** — their conflict detection goes through the normal adjudicator path, since distinct primary keys can produce duplicate unique-index keys.

### 5.3 Garbage Collection

A common MVCC challenge. Remove old row versions too late and you pay in storage and diluted caches (lower performance); collect too early and transactions depending on the removed data must abort. The problem is **particularly acute in DSQL because no part of the system knows the full list of running transactions** — in contrast to, say, PostgreSQL, which tracks in-flight transactions.

The workaround: **cap transaction run time**. This allows simple, independent, **time-based garbage collection at each storage server**: a server can forget rows (other than the most recent) simply when they are older than a fixed time **`τ_expiry`**. If a read arrives with `τ_start < τ_expiry`, the storage node rejects it and the QP fails the transaction.

This would not be acceptable in analytics or OLAP systems, but few OLTP workloads have unbounded-duration transactions. **In the current deployment `τ_expiry` is five minutes before the current wall-clock time.** For analytics use cases, DSQL offers **change data capture streams** for ingestion into analytics and warehousing systems (including Redshift).

### 5.4 The Journal and Erasure Coding

Journal has been used internally at AWS **for over a decade** in various forms, including processing **many millions of transactions per second for S3, DynamoDB, and MemoryDB**. Implementation details are out of scope, but:

- **single-region replication uses a variant of chain replication** (where chain replication's simplicity and efficiency provide significant benefits);
- **cross-region replication uses a variant of Paxos** (where a quorum protocol's ability to pick **2-of-3 latency** matters most).

**Erasure coding across Journals.** To further reduce latency variance, DSQL does not use a Journal directly but **erasure codes data across multiple Journals**. This is primarily a **latency optimization**, although it also strictly improves durability and significantly improves availability. The motivation: a DSQL storage replica cannot make progress on replication (and therefore cannot serve reads) unless **all** the Journals it consumes from are available for reads. Erasure coding across multiple Journals raises availability by multiple orders of magnitude at modest cost.

**DSQL uses a 2-of-3 code** — erasure coded across three Journals such that data can be retrieved from any two.

**Figure 8 (described):** availability in "nines" (y-axis, 5.0 to 10.0) plotted against `k` (x-axis, 1 to 4) with one curve per `M` (1, 2, 3, 4), for a base availability of 99.99%. For `k = 2, M = 3`, availability rises **beyond seven and a half nines**. Multiple `k`/`M` values were considered, but this simple case met the availability goals and has the additional benefit that the erasure code for **`M = k + 1` is a trivial XOR**.

The paper's editorial: erasure coding for availability is widely used, but **erasure coding for latency is an approach that should be more widely used by database and systems builders**. If an individual Journal becomes unavailable, the erasure coding lets storage nodes keep consuming committed transactions from the remaining Journals **without interruption**, and the commit path can keep writing to the available Journals while the failed one recovers.

### 5.5 Exceptions to Snapshot Isolation

Academic definitions of snapshot isolation don't map cleanly to what developers expect from SQL.

**The catalog.** DSQL, like PostgreSQL, stores the catalog (table and schema definitions) in the database itself. The vast majority of OLTP transactions don't modify the catalog and, under snapshot isolation's definitions, would not be required to conflict with transactions (such as `ALTER TABLE`) that do. **This is insufficient.** Consider an INSERT concurrent with an `ALTER TABLE`: the ALTER may modify the table between the UPDATE's `τ_start` and `τ_commit` in a way that makes the UPDATE invalid or illegal. The fix: DSQL **additionally detects read-write conflicts on the catalog, making catalog table updates always serializable**.

**Explicit locking clauses** such as `FOR UPDATE` are treated specially depending on requested lock strength. For `FOR UPDATE`, DSQL **detects read-write conflicts on those rows**, aborting the transaction if they have been modified since `τ_start`.

These are not the only exceptions — "SQL is full of such edge cases." In general, academic snapshot-isolation definitions **do not account for DDL or explicit locking**, creating the need for selectively stronger isolation on specific operations. DSQL handles these within the existing OCC framework by **extending the conflict check to include read-write conflicts on the affected rows**, without changing the default isolation level for ordinary DML.

### 5.6 Effects of Clock Skew

`τ_start` is taken directly from the QP's local clock; `τ_commit` is picked by combining the local clocks and highest observed sequence numbers of all involved adjudicators. These clocks are very high quality — **typical skew significantly below one network round-trip time** — but skew must still be reasoned about. Writing true physical time as `τ^p`:

| Condition | Consequence |
|---|---|
| `τ_start ≫ τ_start^p` | Reads delayed until the stream of Journal heartbeats catches up to `τ_start` — **latency, not correctness**. |
| `τ_start ≪ τ_start^p` | The client may observe a snapshot from before reads it knew to be committed — **violates linearizability, not isolation**. |
| `τ_commit ≫ τ_commit^p` | Future transactions with `τ_start = τ_start^p` may not observe this committed transaction — **violates linearizability**. Future transactions on the same adjudicator can't move time backwards and so **block until `τ^p` catches up** with the highest committed transaction time — latency. |
| `τ_commit ≪ τ_commit^p` | Affects **liveness**: storage must catch up to `τ^p` before processing any reads. |

The clock hardware provides **error bounds** on true clock time, and **the correct edge is chosen in each case to preserve correctness**.

**In short: clock skew beyond expected bounds costs linearizability, but not isolation, durability, or atomicity.** DSQL degrades to *merely* snapshot isolated rather than *strong* snapshot isolated. To avoid this, AWS closely monitors clock skew across the fleet using multiple approaches, and the clock synchronization hardware has a high level of internal redundancy. The team believes strong consistency is very important to programmers' ability to write correct business logic, and **treats any clock skew as a failure**.

---

## 6. Testing and Correctness

Correctness is, along with durability, the most important property customers expect. Building the query processor on PostgreSQL gave a head start, but replacing the storage layer, concurrency control, and other key components required extensive testing.

- **Formal verification of key protocols.** Core protocols were specified in **TLA+ and P** with extensive model checking. During this phase the team also **explored protocol optimizations**, with model checking giving confidence the proposed optimizations were correct.
- **Deterministic simulation testing at build time.** Pioneered by FoundationDB, this requires making the code fully deterministic (notably running inside a **deterministic thread scheduler**) and building a simulation framework that can simulate loss, latency, and re-ordering in the network. AWS developed **turmoil, a framework in Rust**, for this purpose. Deterministic simulation testing gives much better coverage than testing on real infrastructure by making tests cheaper and faster. While the happy path is tested too, **the focus is the system's ability to remain correct while handling errors and failures** — the team's experience and data from Yuan et al. show that the majority of bugs in complex distributed systems are **in error handling logic**.
- **Fault injection testing under load** once deployed, validating that the failure-handling results from simulation are correct. A subset of these in-production fault injection tests is available to DSQL customers via the **AWS Fault Injection Service**.
- **Fuzz testing against PostgreSQL.** Building on the **SQLancer** approach, millions of example SQL statements are generated and run on **both DSQL and Aurora PostgreSQL**, alongside a static set of hundreds of fixed-function tests. Where feature sets fully overlap, DSQL is expected to produce **identical results** to PostgreSQL. Some behavior differences are tolerated where the SQL specification allows — e.g. a `SELECT` without an explicit `ORDER BY` may return rows in a different order, though the result *set* must be identical. This approach has found bugs in edge cases around **floating point, collations, and NULL handling**.
- **Event-based numerical simulation** as pre-design validation, exploring expected performance under various workloads and design variations.

**Figure 9 (described):** normalized goodput (throughput of committed transactions) for a TPC-C-like benchmark across six configurations — `usw2_w1`, `use1_use2_w1`, `use1_usw2_w1` and the same three at `w100` — i.e. single region, multi-region Virginia↔Ohio, and multi-region Virginia↔Oregon, at two client-concurrency levels. The limited-concurrency nature of TPC-C-like benchmarks shows that the **additional latency of cross-region commits can reduce workloads when client concurrency is limited**, but that the overall system **scales well**.

Simulation results validated well against in-production testing, and numerical simulation allowed exploring optimizations and design options quickly and confidently **without needing to build the whole system first**.

---

## 7. Control Plane and Heat Management

How are the sharded storage servers and adjudicators found? When a QP starts up it is bootstrapped with a small per-database data set maintained by the control plane, letting it discover the storage servers and adjudicators for that database. The QP then loads the **catalog**, which contains (much like PostgreSQL's) table and schema definitions, statistics for query optimization, and the **authoritative partition map** mapping the key space to adjudicators and storage servers.

**The partition map must be up-to-date for liveness and availability, but an incorrect partition map does not affect safety or correctness.** An adjudicator **knows for sure** whether it is the leader of a given partition at a given `τ_commit`, and a storage server **knows for sure** whether it is complete for a key or key range at a given `τ_start`. Requests sent to the wrong place are simply **rejected**.

Maintaining the partition map is the control plane's most important job. A new database starts with a **single storage partition** (likely three replicas, one per AZ) and a **single adjudicator partition**. As the database is used, storage servers and adjudicators report **heat** (read and write rate and throughput) and storage size across the key range to the control plane. The control plane monitors this and uses a **predictive approach** to decide when and where to split storage servers or adjudicators — **significantly ahead of resource exhaustion**, to ensure availability.

**Figure 10 (heat handling in the control plane, described):** a Control Plane box connected to a Catalog, monitoring two Adjudicators (`[a..m]`, `[n..z]`) and three Storage servers (`[a..f]`, `[g..p]`, `[q..z]`), with heat/size information flowing upward.

**Splits.** For storage servers, the control plane provisions a new storage server, **restores data for its key range from S3**, subscribes it to changes from the crossbar, and adds it to the partition map. Shards are kept **small enough that new servers can serve traffic in seconds**. The control plane then instructs the previous server to clean up keys it no longer tracks (and narrow its crossbar subscription). Because storage shards may **temporarily have overlapping key ranges** during this process, **splits do not need to be atomic**, significantly simplifying the scaling path. Splitting adjudicators is slightly more complex because of tighter correctness requirements — **each key must be owned by at most one adjudicator, while each key must be owned by at least one storage server** — but is simplified by their much smaller state.

The control plane can also **merge** shards and **add replicas** to any storage shard.

**Storage trade-offs:**

- *More shards* → more read and write throughput across the key space, plus more storage space.
- *Fewer shards* → fewer queries spanning multiple shards, better spatial locality — benefits for both performance and availability.
- *More replicas* → more read throughput for a given key (or small key range), and better replica locality to query processors.
- *Fewer replicas* → lower storage and write processing cost. Writes for a key range must be applied to all replicas, so write cost scales with **O(write rate × replicas)**.

The ability to control **sharding and replication for storage independently** was a key lesson taken from DynamoDB, which lacks the flexibility to handle hot *read* ranges by increasing the replication factor.

**Adjudicator trade-offs:**

- *More shards* → more write throughput across the key space.
- *Fewer shards* → a lower percentage of commits needing the multi-adjudicator protocol, with benefits for commit performance and scalability.

The paper stresses that the importance of the control plane and heat management to overall performance (in DSQL and cloud services generally) **should not be underestimated**: monitoring and predicting heat distributions and quickly, correctly orchestrating splits, merges, and replications is critical.

---

## 8. Lessons from Production

### 8.1 Commit Size Limits

DSQL currently allows a single transaction to modify at most **3,000 rows and 10 MiB of data**. The need for a transaction size limit came from operational experience at AWS about the importance of **bounding tail latencies** for stable applications.

The necessity is partly driven by **Little's Law**: concurrency in a system is linearly proportional to mean response latency, and concurrency in turn drives resource allocation and transaction contention. Large writes in relational databases have a large impact on mean latency — **much larger than limited-concurrency benchmarks like TPC-C measure, due to coordinated omission**. Transactions must consistently observe the world **after or before, but never during**, a transaction, causing head-of-line blocking of readers and potentially other writers. Capping transaction size to something appliable in milliseconds limits this effect.

However, size limits are inconvenient for some applications, most notably during **large data loads**. AWS heard more customer feedback than expected about working through transaction size limitations, and is working on a change to give customers who aren't interested in consistent-latency benefits **more control over transaction sizes**.

### 8.2 Foreign Key Constraints

The original production release does **not** enforce foreign key constraints (FKCs), although JOIN and schemas with foreign key relationships are fully supported. This was a **time-to-market optimization**, based on conversations with at-scale customers who avoid FKCs for performance reasons and small customers who see little value in them. Demand turned out **higher than expected**, and implementation is underway.

As with the section 5.5 cases, **it is not possible to implement FKCs correctly under pure snapshot isolation.** The implementation will introduce **read-write conflict checking for FK relationships**, similar but not identical to `FOR UPDATE`. The performance impact is that transactions will more often need to **span multiple adjudicators** and run the more complex multi-adjudicator commit protocol — but it is **not a significant architectural change**.

### 8.3 Sequences and Indexes

The team still believes range partitioning was the right choice, but it makes three cases significantly harder:

1. **Serial keys** (sequences / `AUTO_INCREMENT`);
2. **Secondary indexes with write locality** (e.g. indexing a timestamp column and always inserting `now()`);
3. **Indexes with low cardinality** (e.g. an index on a boolean).

The paper suspects that "supporting these high locality cases at scale while preserving read locality may not be possible in general."

The plan is to extend the range-based scheme to a **hybrid scheme**, allowing sub-ranges to be distributed **hash-style over multiple storage nodes**. This scales per-range write throughput while still avoiding the hash-key downside of ordered scans jumping around the whole storage fleet.

### 8.4 Strong Consistency

The 2007 Dynamo paper's embrace of eventual consistency, and Werner Vogels' 2009 *Eventually Consistent*, reflected the thinking that cloud-scale systems needed eventual consistency to meet availability and latency goals. Since then, advances in **time distribution, datacenter networks, power and cooling infrastructure, and distributed protocols** have changed the trade-offs. Two decades of working with application programmers and building thousands of cloud-scale services at Amazon have shown the difficulty programmers face writing correct business logic under eventual consistency.

DSQL embraces **strong consistency (namely linearizability)** while still achieving **nearly 10× lower read latency and 4× lower write latency than reported for 2007's Dynamo paper**.

---

## 9. Conclusion

The team believes it achieved its most important goal: a relational database that simplifies customer architectures through strong semantics, scalability up and down, high availability and durability, and no operations. They report considerable success running production applications on DSQL, and believe they have **validated the hypothesis that a disaggregated SQL database built around a journal service can offer excellent features, performance, and operational properties**.

Remaining work: foreign key constraints, stored procedures, and other popular SQL features; plus active work on latency, throughput, and query planning and execution.

**Acknowledgments** note a significant debt to the earlier work of Al Vermeulen and the teams at AWS that built **JournalDB, QLDB, Aurora, and DynamoDB**.

---

## Selected references (from the paper's bibliography)

- Kung & Robinson, *On Optimistic Methods for Concurrency Control* (1981) — the OCC scheme DSQL varies.
- Van Renesse & Schneider, *Chain Replication* (OSDI 2004) — Journal's single-region replication basis.
- Gray & Lamport, *Consensus on Transaction Commit* (2006) — the fault-tolerant atomic commitment pitfalls DSQL's design avoids.
- Escriva, Wong & Sirer, *Warp: Lightweight Multi-Key Transactions for Key-Value Stores* (2015) — inspiration for the multi-adjudicator 2PC variant.
- Zhou et al., *FoundationDB: A Distributed Unbundled Transactional Key Value Store* (SIGMOD 2021) — closest architecture; source of deterministic simulation testing.
- Thomson et al., *Calvin* (2012); Ren, Li & Abadi, *SLOG* (VLDB 2019) — deterministic post-commit ordering.
- Corbett et al., *Spanner* (OSDI 2012); Taft et al., *CockroachDB* (SIGMOD 2020) — pessimistic, Paxos-group comparisons.
- Taleb et al., *Amazon MemoryDB: A Fast and Durable Memory-First Cloud Database* (SIGMOD 2024) — the other disaggregated-Journal system.
- Berenson et al., *A Critique of ANSI SQL Isolation Levels* (SIGMOD 1995) — write skew.
- Abadi, *Consistency Tradeoffs in Modern Distributed Database System Design* (2012) — PACELC.
- Hellerstein & Alvaro, *Keeping CALM* (2020) — logical monotonicity behind the blind write optimization.
- Yuan et al., *Simple Testing Can Prevent Most Critical Failures* (OSDI 2014) — bugs concentrate in error handling.
- Brooker & Desai, *Systems Correctness Practices at Amazon Web Services* (CACM 2025); Newcombe et al., *How Amazon Web Services Uses Formal Methods* (CACM 2015).
