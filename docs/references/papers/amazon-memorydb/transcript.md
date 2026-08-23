# Amazon MemoryDB: A Fast and Durable Memory-First Cloud Database

**Authors:** Yacine Taleb (AWS, Canada), Kevin McGehee (AWS, USA), Nan Yan (AWS, Canada), Shawn Wang (AWS, USA), Stefan C. Müller (AWS, Canada), Allen Samuels (AWS, USA)

**Date:** SIGMOD-Companion '24, June 9–15, 2024, Santiago, AA, Chile. Received 30 November 2023; accepted 4 February 2024. 12 pages.
**Source:** https://doi.org/10.1145/3626246.3653380 — PDF: https://assets.amazon.science/e0/1b/ba6c28034babbc1b18f54aa8102e/amazon-memorydb-a-fast-and-durable-memory-first-cloud-database.pdf

**CCS Concepts:** Information systems → Main memory engines.

---

## Abstract

Amazon MemoryDB for Redis is a database service designed for **11 nines of durability with in-memory performance**. The paper describes MemoryDB's architecture and how it leverages open-source Redis, a popular data structure store, to build an enterprise-grade cloud database. **MemoryDB offloads durability concerns to a separate low-latency, durable transaction log service**, allowing performance, availability, and durability to scale independently from the in-memory execution engine. Using this architecture MemoryDB remains **fully compatible with Redis** while providing **single-digit millisecond write and microsecond-scale read latencies, strong consistency, and high availability**. MemoryDB launched in 2021.

---

## 1. Introduction

For real-time applications — Finance, Advertising, IoT — fast response time is critical, especially when they require multiple consecutive data accesses. Modern key-value stores deliver millions of operations per second per machine and microsecond-scale latencies with simple key-value semantics, but those simple semantics push complexity onto clients and overhead onto the storage system. Example: a real-time bidding application accessing multiple keys of user profiles to perform real-time aggregations such as sorting profiles by criteria. With billions of users this does not scale, and leads to business logic being customized around storage limitations.

**Open Source Software (OSS) Redis** emerged as the most popular in-memory key-value store per db-engines.com. It provides microsecond latencies (**p99 under 400 µs**) while letting applications manipulate remote data structures, perform complex operations, and push compute to storage. Redis's support for complex shared data structures substantially simplifies distributed applications and is chiefly responsible for its popularity.

But: Redis uses **asynchronous replication** for high availability and read scaling, and an **on-disk transaction log** for local durability. **Redis does not offer a replication solution that can tolerate node loss without data loss, nor scalable strongly-consistent reads.** This limits its use beyond caching.

AWS supports Redis for caching with **Amazon ElastiCache**. Many ElastiCache for Redis customers use Redis as their *main* data store for low-latency microservices. To work around the lack of durability they build complex pipelines: ingest data, store it durably, then hydrate Redis. When data loss is detected, a separate job must re-hydrate the cache. This adds significant complexity and cost and impacts availability.

The paper's running example: a catalog microservice in an e-commerce shopping application wants to fetch item details from Redis to serve millions of page views per second. Optimally it would store all data in Redis; instead it must use a data pipeline to ingest catalog data into a separate database like **DynamoDB**, then trigger writes to Redis through a DynamoDB stream. When an item is missing in Redis — a sign of data loss — a separate job must reconcile Redis against DynamoDB.

MemoryDB was built to meet these needs: a fully managed in-memory cloud database with **in-memory performance plus durability, strong consistency, and high availability**. It ensures **strong consistency with 99.99% availability** while providing **microsecond read and single-digit millisecond write latencies**, by leveraging Redis as an in-memory execution engine and **offloading data persistence to an internal scale-out transaction log service**.

Contributions:

- How MemoryDB leverages **separation of concerns** to provide durability, consistency, performance, and availability while remaining fully compatible with Redis.
- Mechanisms developed to provide **predictable performance at scale** while ensuring correctness.

---

## 2. Background and Motivation

### 2.1 Background on Redis

The paper refers to **OSS Redis version 7.0.7** throughout — not Redis Enterprise or Redis Inc's Cloud offerings.

Redis supports **over 200 commands on 10 data structures**, including hash tables, sorted sets, streams, and hyperloglogs. Commands can be combined into **atomic groups with all-or-nothing guarantees**. Redis also supports server-side execution of **Lua scripts**, which execute atomically. Lua is Turing-complete, so customers can implement complex logic wholly within the server, eliminating network round trips and complicated locking/synchronization.

**Horizontal scaling.** Redis splits its flat key space into **16384 slots** using CRC16. Slots are distributed across one or more shards as part of server-side setup. **Each shard has a single writer (the "primary") and zero or more read-only replica nodes.** Clients discover the slot-to-shard mapping from any node in the cluster and send commands **directly** to the node owning a requested key — for maximum performance, routing among cluster members is the clients' responsibility. If the mapping changes (e.g. after scaling), clients receive **redirection instructions** when executing a command on a key not owned by the receiving node. In this configuration, **multi-key transactions are supported only when all keys belong to the same slot**, ensuring the transaction executes fully within one shard.

MemoryDB supports an **atomic slot-level migration process (resharding)** allowing nodes to keep servicing requests normally during migration (detailed in section 5.2).

**Replication is passive logical replication:** mutating commands are first executed on the primary, updating its data structures, and then **asynchronously replicated by sending the command to the replicas**. Reads from a replica provide a consistent view of the data, **but at a past point in time**. The lag between primary and replica is **not controlled by Redis** and can become significant — e.g. if a replica is overloaded and slow to consume updates. Clients must **explicitly opt in** to read from replicas, so they do not accidentally consume stale data.

**Non-deterministic commands.** Not all Redis commands can be naively forwarded to replicas for execution. `SPOP` removes a *random* element from a set: when executed on a primary, an element is randomly selected, and then **an explicit delete command for the selected element** is sent to replicas over the replication channel. Following the same pattern, when a **Lua script** (similar to a stored procedure in a traditional RDBMS) executes, **the script itself is not replicated** — the *effects* it had on the data set are recorded and replicated **atomically**. This mechanism allows non-deterministic operations to be replicated deterministically.

### 2.2 Challenges of Maintaining Durability and Consistency in Redis

#### 2.2.1 Data Loss During Failover

Beyond serving read-only requests, replicas increase availability: they can be **promoted to primary** within a shard if the primary fails. Redis uses a **quorum-based approach for both failure detection and election** of new primaries. Since replicas hold the shard's data in memory, write availability can be restored within seconds.

However, **the Redis quorum-based protocol does not guarantee consistent replica promotion**, because replication between primary and replica is **asynchronous**. As a result, **a failover will cause permanent loss of writes not replicated to the promoted node** at the time of failure.

Redis implements lightweight persistence mechanisms:

- **Point-in-time snapshots** — serializing all items to disk.
- **Append-Only File (AOF)** — appending all mutating commands to a file. In the most conservative mode AOF issues an `fsync()` for every update, synchronously flushing to disk, **effectively linearizing Redis**.

In a **single primary node** configuration, AOF could provide durability (as long as the AOF file is available) **at the expense of availability**. In realistic multi-node configurations, primary failure triggers a leader election that tries to promote the replica most up-to-date with the failed primary. **There is no mechanism to ensure the elected replica received all acknowledged updates**, which can cause data loss. **In the worst case, Redis could elect a replica with no data, causing all nodes within a shard to synchronize with it, leading to complete data loss.**

#### 2.2.2 Asynchronous Replication

Redis provides the **`WAIT`** command to enforce synchronous replication for a given client: the client blocks until all prior executed updates are acknowledged by a configurable number of replicas. But **`WAIT` does not synchronize replication globally on a shard**, so other clients accessing the same shard can still **observe unacknowledged updates**. Furthermore, **in the event of a failover there is no mechanism to enforce promotion of a replica that observed all acknowledged writes**.

"At Amazon, we design for failures, and although Redis does a good job at maintaining high availability, it can lose data." Customers asked for a solution letting Redis be used as a **primary database with multi-Availability Zone (AZ) durability**. The team's challenge: maintain consistency across **all** failure modes in Redis while **minimizing both performance impact and divergence from the Redis code base**, so that full parity with the Redis API can be supported now and into the future.

> Footnote definition: an **Availability Zone (AZ)** is a subset of a Region connected to other AZs in the region through low latency links but isolated for faults, including power, networking, software deployments, flooding, etc.

---

## 3. Durability and Consistency

A durable database must ensure that once data is committed and acknowledged it can be read back. Common logging and replication strategies provide durability levels that are usually a factor of the number of available database cluster nodes and their (storage) lifetime. But nodes can fail, get terminated, or be scaled — therefore **decoupling durability from the database nodes** helps provide consistent durability guarantees. MemoryDB offloads durability to a **distributed transaction log service** providing **low-latency and strongly consistent commits across multiple AZs**.

**Figure 1 (MemoryDB high-level overview, described):** a primary node **synchronously executes and replicates updates to a multi-AZ transaction log**; secondary (replica) nodes **asynchronously fetch updates from the transaction log**.

### 3.1 Decoupling Durability

To minimize divergence from Redis, MemoryDB followed an approach similar to **Amazon Aurora**: decompose the stack into layers, decoupling the execution engine from the durability layer. Redis is used as an in-memory execution and storage engine, but **its existing replication stream is redirected into the transaction log**, which is responsible for **propagation of writes to replicas *and* leader election**. This allows offering the full Redis API without invasive engine modifications, since the same replication strategy is leveraged.

The internal AWS transaction log service provides strong consistency, durability across multiple AZs, and low latency. **Writes to the log are only acknowledged once durably committed to multiple AZs, providing 11 nines of durability.**

Relying on a **loosely coupled** transaction log service lets it scale independently from the in-memory engine. Therefore **the amount (and cost) of availability can be varied independently of the cost of durability**. The cost of a MemoryDB node is dominated by DRAM, sized relative to the working set; a transaction log is sized relative to **write bandwidth** and typically costs a small fraction of a MemoryDB node. Many MemoryDB customers operate shards with **either a primary only, or a primary with a single replica**, yet still receive **durability across three AZs** — impossible if compute and storage were coupled.

Each shard uses **passive replication**: the primary replicates the mutative commands it executes to its transaction log. Specifically, **MemoryDB intercepts the Redis replication stream, chunks it into records, and sends each record to the transaction log**. Replicas **read the replication stream sequentially from the transaction log** and stream it into Redis. As a result, every replica holds an **eventually consistent** copy of the data set.

### 3.2 Maintaining Consistency

Redis is single-threaded and sequentially executes all commands it receives; however, it may **lose committed writes across failovers** due to asynchronous propagation. **MemoryDB provides linearizability by making propagation to the multi-AZ transaction log synchronous.**

**Write-ahead vs. write-behind logging.** The conversion to synchronous replication faced this classic choice. MemoryDB selected **write-behind logging**, because it aligns with the Redis replication model, which generates replication information **at the end of an operation**. Write-behind logging also allows MemoryDB to support **non-deterministic commands** such as `SPOP` by replicating the **effects** of the command instead of the original command.

**Client blocking / the tracker.** Because of passive replication, mutations are **executed on a primary node before being committed into the transaction log**. If a commit fails — for example due to network isolation — the change **must not be acknowledged and must not become visible**. Other engines use isolation mechanisms like MVCC to achieve this, but **Redis data structures do not support MVCC and it cannot be readily decoupled from the engine itself**. Instead, MemoryDB adds **a layer of client blocking**:

- After a client sends a mutation, **the reply from the mutation is stored in a tracker** until the transaction log acknowledges persistence, and **only then sent to the client**. Meanwhile the Redis workloop can process other operations.
- **Non-mutating operations execute immediately but must consult the tracker** to determine whether their results must also be delayed until a particular log write completes.
- **Hazards are detected at the key level.** If the value or data structure in a key has been modified by an operation not yet persisted, responses to read operations on that key are delayed until all data in that response is persisted.
- **Replica nodes do not require blocking**, as mutations are only visible once committed to three AZs.

**Consistency levels offered:**

| Access | Guarantee |
|---|---|
| MemoryDB **primary** nodes | **Strongly consistent** (linearizable) |
| A **single replica** (client opts in via Redis `READONLY`) | **Sequential consistency** — a consistent point-in-time view |
| **Multiple replicas** (e.g. load balancing reads across them) | **Eventually consistent** view |

---

## 4. Availability, Recovery and Resilience

### 4.1 Leader Election

Redis's cluster architecture is leader-follower with a **majority-based quorum**, using a gossip protocol called the **cluster bus**. Primaries from each shard constantly heartbeat each other via the cluster bus. When a majority has not received heartbeats from a given primary, that primary is declared failed and the majority votes to elect one of its replicas. Replicas are chosen by a **ranking algorithm** that tries to promote the most-up-to-date replica **based on the local perspective of each voting node**. There is **no guarantee the elected replica observed all committed updates**, since Redis does not use consensus when accessing or updating data. For instance, if a primary is isolated from the rest of the cluster it **continues servicing data until a certain timeout**, while on the healthy partition a replica may be promoted.

In essence, Redis fails to satisfy some safety properties of a quorum-based replicated system:

1. **Leader singularity** — at most a single leader operating at any given point in time.
2. **Consistent failover** — only a consistent replica can campaign and win leadership.

MemoryDB implements a leader election mechanism that **always maintains these safety properties**, built on the transaction log. It ensures **only fully caught-up replicas become eligible for promotion**, maintaining strong consistency across failures; it **always ensures leader singularity** via a **lease system that demotes failed primaries**; and it **does not require any cluster quorum for liveness**, improving availability over the Redis cluster bus mechanism.

#### 4.1.1 Building atop the Transaction Log

The transaction log service provides a **conditional append API**. Each log entry is assigned a **unique identifier**, and **each append request must specify the identifier of the entry it intends to follow as a pre-condition**. **Acquiring leadership is done by appending a specific log entry to the transaction log.** Leadership is granted for a **pre-determined lease** (Gray & Cheriton).

#### 4.1.2 Consistent Failover

Ensuring consistent failover becomes simpler with the append API: **only replicas that have observed the latest write's unique identifier will succeed at appending the leadership entry to the log.** When a replica is fully caught up with the transaction log it is **notified via a control message**. When multiple replicas contend for leadership, **only one will succeed, and it invalidates the pre-condition of any other concurrent append requests.**

This also yields an interesting property: **old replicas rejoining the cluster after failures are naturally fenced and cannot contend for leadership.**

MemoryDB leader election **bypasses the Redis cluster bus**. It requires **no majority and no minimum number of nodes**. Each replica interacts only with the transaction log service, **not with each other**. Only when a new primary is elected is the role change propagated **asynchronously via the cluster bus** to inform the rest of the nodes, which can then inform clients about role changes for minimal downtime.

#### 4.1.3 Leader Singularity

The **lease** approach was chosen to provide in-memory performance while maintaining consistency. Many consensus-based systems use leases so a node can satisfy read requests locally without committing an operation through the relatively more expensive consensus protocol. This improves read throughput and latency, and also improves **write** performance by reducing the total number of operations handled through consensus (Moraru et al., Paxos Quorum Leases).

**The lease granularity is at the Shard level.** Leader and replica nodes cooperate to ensure **leases remain always disjoint**:

- Leaders **periodically renew their lease by appending a lease renewal entry to the transaction log**.
- Replicas observe transaction log entries and **start a pre-determined timer after observing a lease renewal**. This duration, called **backoff**, is ensured to be **strictly greater than the lease duration**. Replicas refrain from campaigning for leadership during the backoff.
- **A primary that cannot renew its lease voluntarily stops servicing reads and writes at the end of its lease.**
- When replicas do not observe any lease renewal entry after the backoff duration, they **resume attempting to campaign** for leadership.

**Summary of benefits** of building leader election on the transaction log service:

1. **Improved liveness** over Redis cluster bus leader election: it depends only on the availability of the transaction log service — an existing availability dependency regardless — instead of the *additional* availability of a majority in the cluster.
2. **Strengthened consistency** by strictly ensuring a single primary throughout failures **including split-brain scenarios**. If a primary cannot keep its lease it **self-demotes** to prevent servicing stale data. A replica cannot campaign unless it observed all the updates in the transaction log.
3. **Reuse** of the append API and its battle-tested consistency property, already used by other Amazon production transactional systems — simplifying MemoryDB's overall design and maintenance.

### 4.2 Recovery

In steady state a primary **periodically commits heartbeat messages in the transaction log** to indicate liveness and extend its lease. When replicas have not observed a heartbeat after a timeout, they suspect primary failure, triggering leader election to recover availability.

A **MemoryDB monitoring service** constantly polls all replicas for health. This service is **external** to the data nodes and its polling results form an **external view** of cluster connectivity and health. Additionally, nodes within the same cluster **gossip** to form an **internal view**. When deciding a failure, **both views are consulted** to improve failure detection accuracy. Once a node is determined failed, the monitoring service acts: depending on failure mode, the database process may be restarted in place or the underlying hardware replaced. **New nodes always start up as replicas.**

#### 4.2.1 Data Restoration

MemoryDB is strongly consistent, so after a failure, **restoring previously committed data is on the critical path to recovering availability**. Data restoration efficiency is critical to **mean-time-to-recovery (MTTR)** of a cold restart.

MemoryDB leverages Redis's existing, battle-tested data synchronization APIs: a recovering replica **loads a recent point-in-time snapshot and then replays subsequent transactions**. While **Redis requires the presence of a primary node to restore previously stored data**, MemoryDB **periodically creates snapshots and stores them durably in S3**. This lets MemoryDB recover committed data to a node **without the presence of a primary**. Recovering replicas fetch and load the latest snapshot from S3, then replay from the transaction log.

Consequences:

- **Data restoration is local to the restoring replica** — it does not interact with available peers in any way.
- **Multiple replicas can recover in parallel** without any centralized scaling bottleneck. S3 and the transaction log are separately scaled to potentially allow **all** replicas to restore data at the same time.
- It **avoids compounding node failures** by not incurring extra workload on healthy peers.

#### 4.2.2 Off-box Snapshotting

Redis creates snapshots by **forking** the database process. Leveraging **copy-on-write (COW)** virtual memory management, the child process captures a point-in-time of the data set and serializes it into a snapshot file while the main process continues accepting mutations. This **amplifies overall memory usage** and is **CPU intensive**. Some Redis users are accustomed to **reserving extra memory space** to offset the impact.

MemoryDB instead built **off-box snapshot creation**. Off-box clusters are **ephemeral clusters, invisible to customers**, that do not directly interact with customer clusters in any way. They **share the same durable data sources (S3 and the transaction log)** with customer clusters so they can create snapshots on the customer's behalf. S3 and the transaction log are scaled to accommodate the extra read workload.

Off-box clusters essentially consist of **shadow replicas** of the customer clusters, bootstrapped using the **same data restoration procedure** any recovering customer replica uses.

**Figure 2 (off-box snapshot creation, described):** off-box clusters are scheduled periodically and created using the latest generated snapshot. Then **(1)** the off-box cluster reads the transaction log up to the latest known update (a **tail position recorded at the off-box cluster's creation time**), and stops. This re-creates a **static data view** reflecting a recent state on the customer cluster, **guaranteed to be fresher than any previous snapshot**. Then **(2)** each off-box replica dumps its data view into a new snapshot and uploads it to S3, effectively making it the latest generated snapshot for that cluster.

Because off-box replicas are not part of the customer cluster, they are **not subject to customer traffic** and create snapshots by **fully utilizing their available CPU and memory** without interference.

Snapshotting **could** be done on customer replicas to optimize cost, but was rejected for two reasons:

a) Customers can still read from replicas, and snapshotting is a compute- and IO-heavy operation that could impact client latencies for replica readers.
b) Replicas play a crucial role in **write availability** if a primary fails; a replica busy snapshotting could **delay recovery**, impacting customer availability.

#### 4.2.3 Snapshot Creation Scheduling

A snapshot is usually a **more efficient format than the transaction log** for storing and restoring data: it captures each piece of data **exactly once with its latest content in compact form**, whereas the transaction log contains all historical changes, many irrelevant to the latest content. So to optimize restoration efficiency and cold restart time, MemoryDB aims to **bound the amount of transaction log to replay** and make data restoration **always snapshot-dominant**.

Snapshot creation is computationally expensive, so scheduling strikes a balance between **freshness and cost**. MemoryDB constantly monitors the freshness of each cluster's latest snapshot. **Freshness can be visualized as the snapshot's distance from the current tail of the transaction log.** The fresher the snapshot, the less transaction log a recovering replica must replay, and the more snapshot-dominant and efficient the restoration.

How fast freshness deteriorates is a function of **both customer write throughput and customer data set size**:

- Higher write throughput grows the snapshot's "distance" from the tail faster.
- Larger data set size makes creating a new snapshot take longer, indirectly allowing the transaction log to grow more.

The monitoring service continuously samples this on live clusters, calculates freshness from these factors, and **schedules new snapshot creations whenever freshness is too stale**.

---

## 5. Management Operations

### 5.1 Cluster Management

MemoryDB's success at scale results from the management capabilities of the **control plane**, responsible for processing customer provisioning requests, performing cluster updates and upgrades including coordinating scaling activities, and maintaining high availability by quickly diagnosing and remediating cluster-level failures. **Recovery is coordinated by the control plane.**

The control plane is a **regional multi-tenant service** managing a fleet of **single-tenant clusters** on behalf of customers. Each create-cluster request provisions the specified number of **EC2 instances** and the required number of **multi-AZ transaction logs**, then configures the nodes into the requested topology. It uses appropriate **AWS KMS** keys (customer-owned or service) in an **envelope encryption** strategy — plaintext data encrypted with a data key, then the data key encrypted under another key — to encrypt data at rest on the MemoryDB nodes **and on the multi-AZ transaction log**.

At cluster creation the customer provides a **VPC**. When a customer creates shards with at least one replica, **nodes are placed in different AZs** to ensure no downtime on a single AZ failure. The control plane attaches nodes into the customer VPC, provisions **stable DNS endpoints**, vends **TLS certificates** if needed, and pushes configuration such as **Access Control Lists (ACLs)** to each node — coordinated cluster-wide and parallelized across shards and/or nodes.

**Figure 3 (MemoryDB monitoring, described):** customer clusters are accessed via a customer-provided VPC; the MemoryDB service uses **another VPC** to interact with customer clusters; a **multi-tenant fleet** monitors and manages MemoryDB clusters.

**Rolling N+1 upgrades.** Instead of a traditional **blue/green** deployment strategy, MemoryDB uses a rolling **N+1** upgrade process: rather than upgrading nodes in place, **new nodes running the new software are provisioned**. This mitigates the impact on cluster availability by allowing all nodes to serve traffic while an upgrade occurs. Similarly, scaling out a cluster adds a new shard of new nodes and **gradually moves Redis slots** from existing shards to new shards, orchestrated centrally.

The monitoring service **fetches data from all nodes in a cluster every 5 seconds** to understand cluster health. It serves as a **watchdog for cluster configurations**, fixing those that are valid (such as detected dead replicas) and alarming on those that are invalid (such as only replicas detected in a shard).

### 5.2 Scaling

Cluster size is measured in **three dimensions**: number of shards, number of replicas per shard, and EC2 instance type (CPU & memory per instance). Control plane APIs dynamically adjust any of the three on a running cluster without significant interruption, invoked manually or programmatically.

**Scaling the number of replicas** is simplest. To decrease, one replica from each shard is selected and terminated, releasing the EC2 instance. To increase, a new EC2 instance per shard is created and provisioned; once operational it **reloads the most recent snapshot for the shard from S3 and then replays its transaction log**. Once the tip of the transaction log is reached, the replica joins the cluster, **advertising its availability via the Redis cluster bus**.

**Scaling the instance type** is an **N+1 rolling update**. Replicas of the new instance type are created as above. Once a new replica has joined, the control plane selects a node of the previous instance type to decommission — **replicas first, primary last** — causing a leader election in the case of the shard primary. A **collaborative leadership transfer**, where the old instance actively hands over leadership, minimizes downtime. If the new instance type is **smaller** than the old, it is possible to run out of memory, in which case **the scaling operation is reverted**, restoring the original instance type.

**Scaling the number of shards** requires transferring one or more slots between shards, plus creating shards at the start (scale out) or destroying shards at the end (scale in). Shard creation/destruction involves provisioning/terminating nodes as above **plus a per-shard transaction log creation/destruction**.

The **slot transfer** is divided into two phases: **data movement** and **slot ownership transfer**.

**Data movement** is conceptually similar to a Redis replica synchronization but limited to a specific slot: keys for the slot being transferred must be **serialized and transmitted from the source primary to the target primary while continuing to allow operations that may mutate those same keys**. As a result the transferred data includes **both serialized keys and replication stream mutations of keys already transmitted**. The target primary **commits all messages to the transaction log**, allowing its replicas to reach the same state for the slot.

Before ownership transfer can begin, the source primary ensures **all data has been transferred** by **blocking all new incoming write operations for the slot** and waiting for any in-progress writes to complete execution and propagation to **the source and target transaction logs**. Then a **data integrity handshake** with the target validates correct transfer. **Any error up to this point (out of memory, network error, validation failure, etc.) is easily recovered from** by abandoning the transfer: resuming write operations and directing the target to delete all transferred data.

**Slot ownership.** In Redis, slot ownership is controlled and communicated through the **eventually consistent cluster bus**, a mechanism known to have several failure modes resulting in **corruption or loss of stored data**. Consistent with the principle of minimizing divergence from the open-source code base, communication of slot ownership **remains a cluster bus responsibility**. However, **slot ownership is stored in the transaction log**, and changes to slot ownership are performed using a **2 Phase Commit (2PC) protocol of durably committed messages between the old and new owner of a slot**.

Once ownership has transferred, the new owner begins accepting writes while the old owner **responds with a redirect** for operations on the moved slot and starts **rate-limited background deletion** of all transferred data. Typically, **write unavailability for the slot during the ownership transfer phase is limited to a few network round trips and the transaction log update latencies**. Failures at source or target — for example due to **lease expiration** (§4.1) — can be recovered because **the progress of the 2PC is recorded in the transaction log**: after a primary node failure (source or target) recovers, the ownership transfer protocol **can continue**.

---

## 6. Evaluation

The goal is to evaluate **the cost of durability** in MemoryDB. There are two components: **(1) steady state writes committed to the transaction log**, and **(2) periodic snapshots uploaded to S3**. Steady state is measured with a benchmark against **OSS Redis as a baseline**; snapshotting is measured by comparing Redis's OSS snapshotting facility against MemoryDB's purpose-built off-box approach.

### 6.1 Performance Benchmark

#### 6.1.1 Setup

- All supported **graviton3** instance types, **r7g.large** up to **r7g.16xlarge**.
- Both MemoryDB and OSS Redis use **engine version 7.0.7**.
- Three workloads: **Read Only** (each client sends GET back-to-back, no pipelining), **Write Only** (SET), and **Read Write Mixed** (**80% GET / 20% SET**).
- **10 EC2 instances** each running a `redis-benchmark` process drive traffic, launched **in the same AZ** as MemoryDB and Redis to minimize network latency.
- Nodes pre-filled with **1 million keys** so GETs have a **100% hit rate**.
- Each `redis-benchmark` process configured with **100 client connections and 100-byte values**.
- Simple GET/SET is used for a consistent performance baseline rather than compute-heavy operations manipulating (potentially large) data structures.
- Redis supports **threaded IO**, offloading IO to background threads; MemoryDB supports **Enhanced IO**, a similar internal feature with more advanced capabilities — notably **multiplexing clients into a single connection**, reducing IO fan-in/fan-out overhead. Redis is configured with the **same number of IO threads** as MemoryDB per instance type.
- **Since Redis does not support SSL with IO threads, TLS encryption and authentication are disabled** for the comparison.

#### 6.1.2 Benchmark Results

**Throughput — Figure 4 (described):** maximum throughput per instance type, (a) read-only and (b) write-only, MemoryDB vs. OSS Redis.

- **Read-only (a):** comparable throughput for instance types below 2xlarge, **up to 200K Op/s**. Starting at **2xlarge**, MemoryDB outperforms Redis, achieving **500K Op/s** across all instance types, while Redis peaks at **330K Op/s**. MemoryDB's advantage comes from **Enhanced IO Multiplexing** aggregating multiple client connections into a single connection to the engine, improving processing efficiency.
- **Write-only (b):** **Redis outperforms MemoryDB on all instance types**, achieving a maximum near **300K Op/s**, whereas MemoryDB achieves up to **185K Op/s**. MemoryDB commits **every single write to the multi-AZ transaction log**, resulting in higher request latency; with the same number of clients making sequential blocking requests, MemoryDB therefore delivers lower write throughput. With **more clients, pipelining, or larger payload sizes**, experiments show **a single shard can achieve up to 100 MB/s write throughput** in MemoryDB.

**Latency — Figure 5 (described):** p50 and p99 latency vs. offered throughput (20K, 50K, 100K, 150K, 200K Op/s) on an **r7g.16xlarge**, for (a) read-only, (b) write-only, (c) mixed read-write:

| Workload | Redis p50 | Redis p99 | MemoryDB p50 | MemoryDB p99 |
|---|---|---|---|---|
| Read-only | sub-millisecond | < 2 ms | sub-millisecond | < 2 ms |
| Write-only | sub-millisecond | up to 3 ms | **3 ms** | up to **6 ms** |
| Mixed read-write | sub-millisecond | up to 2 ms | sub-millisecond | up to **4 ms** |

Conclusion: MemoryDB offers **sub-millisecond median latencies for read and mixed read-write workloads** and **single-digit millisecond latencies for write and tail mixed read-write workloads**, *while ensuring multi-AZ durability for every single write*.

### 6.2 Snapshotting Evaluation

Redis uses **Background Save (BGSave)**, which **forks** the Redis process to create a child that iterates the entire keyspace serializing data to disk. During serialization, a **Copy on Write (COW)** occurs if a memory page is modified by the parent, so the page is copied to keep the child's corresponding page intact. COW can accumulate excessive memory under a heavy write workload — **in the worst case doubling memory consumption**, leading to high swap usage, significant latency increase, and throughput degradation.

**Experiment setup:** an instance with **2 vCPU and 16 GB RAM**, max memory configured as **12 GB**, pre-filled with **20 million keys of 500 bytes each** (larger payloads than §6.1.2 to increase memory pressure faster). **100 clients issue GET** commands to measure throughput and latency while **another 20 clients issue SET**. During the snapshot process, average throughput plus **average and p100** latency are recorded. p100 is chosen explicitly for tail latency because the number of latency samples in a single second is lower than over the entire run.

#### 6.2.1 What is the overhead of BGSave?

**Figure 6 (described):** client-perceived (a) latency (log-scaled, with average and p100 curves plus a swap-usage curve) and (b) throughput (with swap usage), over ~40 seconds with a **"bgsave start"** marker.

- When BGSave starts there is **no impact on throughput**, but a **spike on P100 latency reaching up to 67 milliseconds**. This is due to the **`fork` system call cloning the entire memory page table** — internal measurement puts this at about **12 ms per GB of memory**.
- Once the instance exhausts DRAM and starts using **swap** to page out memory, latency increases and throughput drops significantly: **the CPU stalls waiting for memory pages to spill to disk before it can continue performing COW**.
- **Tail latency increases over a second and throughput drops close to 0 as swap goes beyond 8% of total memory — effectively an availability outage from a client's perspective.**

To prevent this in practice, Redis users would need to **reduce database available memory to at most half of host DRAM** to keep write workloads from driving the system to swap, **or run snapshotting during off-peak hours** when little to no write traffic is expected.

#### 6.2.2 What is the overhead of snapshotting in MemoryDB using off-box?

MemoryDB customers can use instance types with memory capacity **as low as 1.37 GB and 2 vCPUs**. To provide durability with snapshots while ensuring performance, MemoryDB performs the snapshot **on off-box clusters**.

**Figure 7 (described):** client-perceived (a) latency (average and p100) and (b) throughput over ~180 seconds, with **"start"** and **"end"** markers for the off-box snapshot process running in parallel.

- **Average latencies hover around 1 millisecond**, while **maximum latency varies between 10 and 20 milliseconds**. The p100 is higher than §6.1.2 because this is a **mixed read/write workload while snapshotting**, values are **5× larger**, and tail read latency is impacted by commit latency (i.e. if a read tries to access an uncommitted key).
- **Throughput and latencies are stable before, throughout, and after the process.** Since the off-box process spins up a cluster isolated from the customer cluster, **there is no impact on the customer workload during snapshotting**.

As a result, **MemoryDB customers need not reserve any memory capacity for snapshotting, nor coordinate snapshotting during off-peak hours.**

---

## 7. Validating and Maintaining Consistency at Scale

### 7.1 Consistency During Upgrades

MemoryDB maintains **version currency with open-source Redis**. Because of the decoupling of in-memory storage and durability, Redis can be used as an in-memory execution engine and open-source changes can be merged **without much difficulty, provided they do not fundamentally alter the replication strategy**. Customers may elect to upgrade engine versions to gain functionality such as new data structures or commands.

The **N+1 rolling upgrade** strategy maintains availability: **replicas are upgraded first and the leader node is upgraded at the very end** to preserve read throughput capacity. To maintain availability, **all nodes cannot be forced to upgrade at the same time in a transactional fashion** — so clusters undergoing an upgrade **can have mixed versions during a transient period**, which can cause inconsistencies.

Example hazard: a **leader running a newer engine can send a newly introduced command to the transaction log, while replicas running older engines observe those commands and in the worst case misinterpret them.** If the new-engine leader then fails and an older-engine replica becomes leader, this could lead to inconsistency.

**Upgrade protection mechanism:** the replication stream is protected by **indicating which engine version produced it**. If a replica with an older engine version observes a replication stream originating from a **newer** version than it is running, it **stops consuming the transaction log**. To keep the cluster available even during failures while upgrading, the control plane coordinates off-box processes such that **a snapshot is taken with the oldest engine version running in the cluster** — allowing nodes still running older versions to be replaced in case of failures during the upgrade.

### 7.2 Verifying Correctness

"At AWS we strive to build services that are simple for customers to use. That external simplicity is built on a hidden substrate of complex distributed systems, and MemoryDB is no exception." High complexity increases the probability of human error in design, code, and operations, and errors in the core could cause **loss or corruption of data**, or violate interface contracts customers depend on.

#### 7.2.1 Snapshot Correctness Verification

MemoryDB **verifies every newly created snapshot in production** to make sure its consistency invariant holds: **snapshots are equivalent to their corresponding prefix of the transaction log.**

Mechanism:

- MemoryDB maintains a **running checksum of the entire transaction log** and **periodically injects the current checksum value into the transaction log itself**.
- A snapshot stores **the checksum value as of the transaction log prefix it captures**, **a positional identifier for the last log entry in the prefix**, and **a checksum covering the data it contains**.
- Verification proceeds by **rehearsing restoring the snapshot on an off-box cluster**: first validate the contents of the snapshot itself against the data checksum; then use the stored positional identifier to locate the subsequent transaction log to replay; while replaying, **use the snapshot checksum as the basis to recalculate a running checksum** and compare against the checksum persisted in the transaction log.
- **Verification fails if the snapshot's checksum does not match the transaction log prefix it captures. Only successfully verified snapshots are made available to customers.**

#### 7.2.2 Consistency

Validating the functional correctness of the Redis API while maintaining strong consistency is not trivial. Similarly to the S3 lightweight formal methods work (Bornholt et al.), MemoryDB **decomposes system correctness validation**, allowing a diverse suite of formal methods tools to best check each property.

**7.2.2.1 Formal Verification.** MemoryDB's durability dependencies use various tools including formal verification. **S3 uses TLA+ and lightweight formal methods** to model and test various components. **The internal transaction log replication protocol is modelled and verified using TLA+.** MemoryDB also uses **P** for new feature development, which proved helpful in reasoning about the overall system and **catching bugs early**.

**7.2.2.2 Consistency Testing Framework.** Applying formal approaches to the Redis API proved challenging, as its implementation is fast moving and frequently changing. The team needed validation results to remain relevant **regardless of the Redis implementation**, while validating API properties and consistency **under failure modes**. MemoryDB uses **porcupine, a linearizability checker**, taking as input a **concurrent history of client commands** and outputting whether that history is **linearizable**. To ensure full coverage over the Redis API, the framework **parses the API specification provided by the engine and generates commands from the output**. Similarly to the S3 work, **argument biasing** improves testing coverage, especially around edge cases. Overall this improved confidence in testing MemoryDB's durability components and its dependencies.

---

## 8. Related Work

### 8.1 Disaggregated databases

A number of distributed database systems have been built for disaggregated storage. **Amazon Aurora** offloads redo processing to a multi-tenant, scale-out storage service. **Sinfonia** and **Hyder** abstract transactional access methods over a scale-out service, allowing database systems to be implemented using those abstractions. **PolarDB** uses **RDMA** to connect disaggregated storage with computation nodes. **MemoryDB leverages database nodes for storage and command processing while offloading replication and durability to a scale-out transactional log service.**

### 8.2 Log-based Replication

Log-based replication has been extensively used by consensus algorithms and by distributed storage systems as a way to provide durability. **Multi-Paxos and Raft are well-known protocols used to build consistent replicated logs. MemoryDB implements a similar protocol to perform consistent log-based replication, with similar safety properties:**

1. **A single node can become leader at a given point in time.**
2. **A committed log entry will always appear in any future leader state.**
3. **A node can only become leader if it observed all committed log entries.**

**MemoryDB improves liveness by leveraging a scale-out transaction log service to build consensus and durability.**

### 8.3 In-memory databases

Large-scale web applications found DRAM indispensable for performance, leading to in-memory NoSQL storage systems and architectures such as **anti-caching**. A number of relational databases (SAP HANA, H-Store) use memory as their main storage medium. Redis emerged as one of the most popular in-memory storage systems given its rich data model, but it is hard to rely on as a primary database due to weak durability guarantees. **MemoryDB is a cloud native memory-based database providing strong consistency, 11 nines durability, and 4 nines availability.**

---

## 9. Conclusion

A core design behind MemoryDB is to **decouple durability from the in-memory execution engine by leveraging an internal AWS transaction log service**. Doing so separates consistency and durability concerns away from the engine, allowing performance and availability to be scaled independently.

The key challenge was **ensuring strong consistency across all failure modes while maintaining performance and full compatibility with Redis**. MemoryDB solves this by **intercepting the Redis replication stream, redirecting it to the transaction log, and converting it into synchronous replication**, and by building **a leadership mechanism atop the transaction log which enforces strong consistency**. MemoryDB unlocks new capabilities for customers who do not want to trade consistency or performance while using the Redis API.

---

## Selected references (from the paper's bibliography)

- Verbitski et al., *Amazon Aurora: Design considerations for high throughput cloud-native relational databases* (SIGMOD 2017) — the decoupling model MemoryDB follows.
- Lamport, *The Part-Time Parliament* (1998); Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (Raft, ATC 2014) — the consistent-replicated-log protocols MemoryDB's safety properties mirror.
- Gray & Cheriton, *Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache Consistency* (1989) — the lease mechanism behind leader singularity.
- Moraru, Andersen & Kaminsky, *Paxos Quorum Leases: Fast Reads Without Sacrificing Writes* (SoCC 2014) — leases improving read *and* write performance.
- Arulraj, Perron & Pavlo, *Write-behind Logging* (VLDB 2016) — the logging choice.
- Bornholt et al., *Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3* (SOSP 2021) — the decomposed-validation and argument-biasing approach.
- Athalye, *Porcupine: A fast linearizability checker in Go* — the consistency checker.
- Desai et al., *P: Safe Asynchronous Event-Driven Programming* (PLDI 2013); Lamport, *The Temporal Logic of Actions* (TLA+, 1994); Newcombe et al., *How Amazon Web Services Uses Formal Methods* (CACM 2015).
- Elhemali et al., *Amazon DynamoDB* (ATC 2022); Wang et al., *Building a Replicated Logging System with Apache Kafka* (VLDB 2015).
- Aguilera et al., *Sinfonia* (SOSP 2007); Bernstein, Reid & Das, *Hyder* (CIDR 2011); Cao et al., *PolarDB* (FAST 2020).
