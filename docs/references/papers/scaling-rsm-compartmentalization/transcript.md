# Scaling Replicated State Machines with Compartmentalization

**Authors:** Michael Whittaker (UC Berkeley), Ailidani Ailijiang (Microsoft), Aleksey Charapko (University of New Hampshire), Murat Demirbas (University at Buffalo), Neil Giridharan (UC Berkeley), Joseph M. Hellerstein (UC Berkeley), Heidi Howard (University of Cambridge), Ion Stoica (UC Berkeley), Adriana Szekeres (VMWare)

**Venue:** PVLDB, Vol. 14, No. 1 (2021). Technical Report, May 18, 2021.
**arXiv:** 2012.15762v3 [cs.DC], 16 May 2021
**Source:** https://arxiv.org/pdf/2012.15762
**Implementation:** https://github.com/mwhittaker/frankenpaxos

---

## Abstract

State machine replication (SMR) protocols like MultiPaxos and Raft are critical components of distributed systems and databases, but offer relatively low throughput due to several bottlenecked components. Existing protocols fix individual bottlenecks in isolation but fall short of a complete solution — when you fix one bottleneck, another arises.

This paper introduces **compartmentalization**: the first comprehensive technique to eliminate SMR bottlenecks. Compartmentalization = **decoupling** individual bottlenecks into distinct components + **scaling** those components independently.

Two key strengths:
1. **Strong performance** — compartmentalizing MultiPaxos increases throughput **6×** on a write-only workload and **16×** on a mixed read-write workload, *without* specialized hardware.
2. **A technique, not a protocol** — practitioners can apply it incrementally to existing protocols without adopting something new.

---

## 1. Introduction

In many SMR protocols, a single node has multiple responsibilities. A Raft leader is simultaneously a batcher, sequencer, broadcaster, and state machine replica. These overloaded nodes become throughput bottlenecks.

Databases rely on SMR to replicate large data partitions (tens of GB). Exceeding a partition's throughput budget can force splits (e.g., Cosmos DB splits a partition under high throughput even when under the storage limit), which costs resources and can harm consistency (Cosmos DB provides strongly consistent transactions only within a partition).

It is widely believed that SMR protocols don't scale. The paper debunks this. The key example is the **MultiPaxos leader**, which has two distinct responsibilities:
- **Sequencing** state machine commands into a log (entry 0, then 1, then 2, ...).
- **Broadcasting** commands to acceptors, collecting responses, and broadcasting to replicas.

To compartmentalize the leader: **decouple** sequencing from broadcasting (introduce **proxy leaders** to broadcast), then **scale** the number of proxy leaders (broadcasting is embarrassingly parallel). This scaling was impossible when the two responsibilities were coupled, since sequencing is not scalable.

### Three key strengths
1. **Strong Performance Without Strong Assumptions** — 6× on write-only (using 6.66× the machines), 16× on mixed read-write (using 4.33× the machines). No perfect failure detector, no specialized hardware, no uniform data access patterns, no clock synchrony, no key-partitioned state machines.
2. **General and Incrementally Adoptable** — it's a technique applied to existing protocols. Demonstrated on MultiPaxos plus three other protocols.
3. **Easy to Understand** — based on simple principles of decoupling and scaling.

---

## 2. Background

### 2.1 System Model
- Asynchronous network: messages can be arbitrarily dropped, delayed, reordered.
- Crash failures only (no Byzantine).
- Machines operate at arbitrary speeds; no clock synchronization.
- At most `f` machines fail, for configurable `f`.

### 2.2 Paxos
Consensus = choosing a single value among proposed values. A Paxos deployment tolerating `f` faults: arbitrary clients, ≥ `f+1` proposers, `2f+1` acceptors.
- **Phase 1:** proposer sends Phase1a to a majority of acceptors; acceptors reply Phase1b (learns of possibly-chosen values).
- **Phase 2:** proposer sends Phase2a⟨x⟩; acceptors vote and reply Phase2b⟨x⟩. A value with votes from a majority is **chosen**.

### 2.3 MultiPaxos
SMR = choosing a *sequence* (log) of values. One Paxos instance per log entry. Deployment: clients, ≥ `f+1` proposers, `2f+1` acceptors, ≥ `f+1` replicas. One proposer is elected leader and runs Phase 1 for every entry. Flow:
1. Client sends command `x` to leader.
2. Leader assigns log entry `i`, sends Phase2a⟨i,x⟩ to acceptors.
3. Acceptors vote, reply Phase2b⟨i,x⟩.
4. On a majority, leader informs replicas.
5. Replicas insert into logs, execute in prefix order; only **one** replica replies to the client (e.g., round-robin partitioned).

### 2.4 MultiPaxos Doesn't Scale? (the conventional wisdom)
- **More proposers** doesn't help: every client sends to the leader regardless.
- **More acceptors** *hurts*: leader must contact a majority, so more acceptors = more leader messages. Every acceptor processes ≥ half of all commands.
- **More replicas** *hurts*: leader broadcasts to all replicas, so more replicas = more leader load. Every replica executes every command.

The bottleneck is always the leader, and naive scaling moves load *onto* the leader.

---

## 3. Compartmentalizing MultiPaxos

Six compartmentalizations are introduced (summary — **Table 1**):

| # | Section | Bottleneck | Decouple | Scale |
|---|---------|-----------|----------|-------|
| 1 | 3.1 | leader | command sequencing & command broadcasting | number of proxy leaders |
| 2 | 3.2 | acceptors | read quorums & write quorums | number of write quorums |
| 3 | 3.3 | replicas | command sequencing & command broadcasting | number of replicas |
| 4 | 3.4 | leader & replicas | read path & write path | number of read quorums |
| 5 | 4.1 | leader | batch formation & batch sequencing | number of batchers |
| 6 | 4.2 | replicas | batch processing & batch replying | number of unbatchers |

### 3.1 Compartmentalization 1: Proxy Leaders
**Bottleneck:** leader. To process one command, the leader sends/receives ≥ `3f+4` messages (client msg, `f+1` Phase2a, `f+1` Phase2b, `f+1` to replicas). Acceptors process only 2; replicas 1–2.

**Decouple:** the leader's two jobs — sequencing and broadcasting. Introduce ≥ `f+1` **proxy leaders**. Leader sequences (assigns entry `i`, forms Phase2a) then sends the Phase2a to a *randomly selected* proxy leader (load balanced). The proxy leader broadcasts to acceptors, gathers `f+1` Phase2b, notifies replicas. The leader now processes only **2** messages per command instead of `3f+4`.

**Scale:** proxy leaders are embarrassingly parallel and independent; increase their count until they're not a bottleneck.

**Discussion:** decoupling *enables* scaling — you can't add leaders (multiple sequencers not allowed), but you can add broadcasters. Protocol remains `f`-fault tolerant, though more machines shortens expected time to `f` failures (mitigated by fast reconfiguration).

### 3.2 Compartmentalization 2: Acceptor Grids
**Bottleneck:** acceptors. With proxy leaders, the leader no longer talks to acceptors, so adding acceptors no longer hurts the leader. But every command must still be processed by a majority — so each acceptor handles ≥ half of all commands.

**Decouple:** use **flexible quorums**. Correctness only needs every **read quorum** (Phase 1) to intersect every **write quorum** (Phase 2). Read quorums needn't intersect read quorums; write needn't intersect write. Arrange acceptors in an `r × w` grid (`r, w ≥ f+1`). Every **row** = a read quorum; every **column** = a write quorum. Each row intersects each column → valid.

Example: 2×3 grid → 2 read quorums (rows), 3 write quorums (columns). Each acceptor processes only 1/3 of commands (impossible with majority quorums).

**Scale:** each acceptor processes `1/w` of commands; increase `w` (columns) to reduce load.

**Discussion:** increasing write quorums increases read-quorum size → more acceptors contacted in Phase 1. Acceptable trade-off, since Phase 1 only runs on leader failure.

### 3.3 Compartmentalization 3: More Replicas
**Bottleneck:** replicas. Previously adding replicas hurt because (a) every replica executes every command and (b) more replicas loaded the leader. (b) is fixed by proxy leaders.

**Scale:** with `n` replicas, each replica only sends results for `1/n` of commands (only one replica replies per command). More replicas → fewer reply messages per replica.

### 3.4 Compartmentalization 4: Leaderless Reads
**Bottleneck:** leader and replicas (the remaining bottlenecks after 1–3).

**Decouple:** **reads** don't modify state, so the leader needn't linearize them (reads commute) and only a *single* replica need execute a read. **Writes** still go through the leader and all replicas. Decouple the read path from the write path using **Paxos Quorum Reads (PQR)**:
- Client sends `PreRead⟨⟩` to a **read quorum** (row) of acceptors.
- Each acceptor `a_i` replies `PreReadAck⟨w_i⟩` where `w_i` is the largest log entry it has voted in (a **vote watermark**).
- Client computes `i = max` of received watermarks, sends `Read⟨x, i⟩` to any one replica.
- The replica waits until it has executed log entry `i`, then executes `x` and replies.

**Scale:** reads go to a row of acceptors (increase rows = read quorums to reduce per-acceptor read load) and to a single replica (increase replicas). Rows and columns are independent — increasing read throughput doesn't reduce write throughput. Many workloads are read-heavy (Chubby <1% writes, Spanner <0.3% writes).

### 3.5 Correctness
Defines **linearizability** (Herlihy & Wing): an execution is linearizable if it has a *linearization* — a consistent sequential history respecting real-time order (`<_H`). Formalized via histories, `complete(H)`, client subhistories, equivalence, and the happens-before partial order. **Proof** that the protocol implements linearizable reads via case analysis on pairs of related operations (write/write, read/read, read/write, write/read), showing log indices respect `<_H`.

### 3.6 Non-Linearizable Reads
Writes are always linearizable; reads can be weakened for performance:
- **Sequentially consistent reads:** each client tracks a watermark `w_i` of the largest entry it has written/read; reads/writes must use an entry ≥ `w_i`. One round-trip in the best case (vs. two for linearizable), and **does not involve acceptors** — scale read throughput by scaling replicas alone. Also causally consistent.
- **Eventually consistent reads:** client sends read directly to any replica, executed immediately against a consistent prefix. No watermark bookkeeping, no acceptors, never waits for writes.

---

## 4. Batching

Standard batching: clients send commands to the leader, which groups them into batches. Replicas execute a batch at a time and must reply to every client in the batch.

### 4.1 Compartmentalization 5: Batchers
**Bottleneck:** leader. With batching, the leader's cost is linear in the number of *commands* (it receives `n` messages per batch of `n`), unlike acceptors/proxy leaders whose cost is per-batch.

**Decouple:** leader's two jobs — forming batches and sequencing batches. Introduce ≥ `f+1` **batchers**. Clients send commands to a random batcher; the batcher forms a batch (after enough commands or a timeout) and forwards to the leader, which only receives **one** message per batch.

**Scale:** batchers are embarrassingly parallel. Read batching is analogous.

### 4.2 Compartmentalization 6: Unbatchers
**Bottleneck:** replicas. After executing a batch of `n`, a replica must send `n` replies (linear in commands).

**Decouple:** replicas' two jobs — executing batches and replying. Introduce ≥ `f+1` **unbatchers**. After execution, the replica sends a result batch to a random unbatcher, which sends individual results to clients.

**Scale:** unbatchers are embarrassingly parallel.

---

## 5. Further Compartmentalization

The six compartmentalizations aren't exhaustive, and MultiPaxos isn't the only protocol that can be compartmentalized. The paper demonstrates generality on **Mencius** and **S-Paxos** (and references EPaxos in [50]; Raft and EPaxos noted as ongoing work).

---

## 6. Mencius

### 6.1 Background
Mencius is a MultiPaxos variant using **multiple leaders** to avoid a single-leader bottleneck — the log is **round-robin partitioned** among leaders (l1 → slots 0,3,6...; l2 → 1,4,7...; l3 → 2,5,8...). Works well if all leaders process at the same rate; otherwise holes appear in a slow leader's slots. A lagging leader fills its vacant entries with **noops** (using Coordinated Paxos) so replicas can execute in order. Deployed as `2f+1` servers, each playing leader + acceptor + replica → ~`3f+5` messages per command plus acking overhead.

### 6.2 Compartmentalization
Decouple servers into proposers (each a leader, log round-robin partitioned), acceptors, replicas. Proposers periodically broadcast their next available slots to detect lag. Then apply the same compartmentalizations as MultiPaxos: **proxy leaders, acceptor grids, more replicas** — plus increase the number of leaders. Batchers/unbatchers and linearizable leaderless reads also apply.

---

## 7. S-Paxos

### 7.1 Background
S-Paxos decouples **command dissemination** from **command sequencing** (separating control flow from data flow) and distributes dissemination across all nodes — useful when commands are large. `2f+1` servers, each a proposer/acceptor/replica plus **disseminator** and **stabilizer**:
- A disseminator assigns a globally unique `id_x`, broadcasts `x` + `id_x` to persist on a majority (stabilizers store and ack with just `id_x`).
- The MultiPaxos leader, on a majority of acks, runs MultiPaxos on the **id** (not the command). MultiPaxos agrees on a log of commands; S-Paxos agrees on a log of command ids.

### 7.2 Compartmentalization
Decouple into ≥ `f+1` disseminators, `2f+1` stabilizers, proposers, acceptors, replicas. The leader is freed from the data path but still a bottleneck on the control path (broadcasting ids). Apply proxy leaders, acceptor grids, more replicas; scale disseminators; use disseminator/stabilizer **grids**; linearizable leaderless reads; batchers/unbatchers.

---

## 8. Evaluation

Implemented in Scala with Netty (frankenpaxos). Deployed on AWS `m5.xlarge` (4 vCPU, 16 GiB), single AZ, in-memory, `f=1`. Compared against vanilla protocols and an (un-fault-tolerant) unreplicated state machine as an upper bound. Key-value store, 16-byte values. Thriftiness enabled.

### 8.1 Latency–Throughput (MultiPaxos)
- **Without batching:** MultiPaxos ~25,000 cmds/s; **Compartmentalized MultiPaxos ~150,000** (6×); unreplicated ~250,000. Deployment: 2 proposers, 10 proxy leaders, 2×2 acceptor grid, 4 replicas.
- **With batching:** ~200,000 vs **~800,000** vs ~1,000,000 cmds/s.
- Uses 6.66× more machines for 6× throughput → **90% of perfect linear scalability** (superlinear on mixed read-write). MultiPaxos and Compartmentalized MultiPaxos are two extremes in a throughput-vs-resources trade-off; partial compartmentalization is possible.
- Larger values (100 B, 1000 B) reduce peak throughput as expected.

### 8.2 Mencius
Without batching: Mencius ~30,000 vs **Compartmentalized Mencius ~250,000** (8.3×). With batching: ~200,000 vs ~850,000 (4.25×).

### 8.3 S-Paxos
Without batching: ~22,000 vs **~180,000** (8.2×). With batching: ~180,000 vs ~750,000 (4.16×). (S-Paxos implementation less optimized.)

### 8.4 Ablation Study
Bottlenecks must be removed in the order they appear:
- Unbatched: MultiPaxos 25k → decouple+proxy leaders 70k → scale proxy leaders (2→7) ~135k → add replica (no gain, reloads proxy leaders) → scale proxy leaders to 10 ~150k → leader becomes bottleneck. Acceptors never the bottleneck on this **write-only** workload.
- Batched: decouple + 2 batchers/2 unbatchers (batch 10) → 300k → batch size 50/100 → 500k → 3 unbatchers → ~800k.
- Open problem: automatically deducing the optimal amount of decoupling/scaling (currently manual).

### 8.5 Read Scalability
Vary replicas (2–6) and read fraction (0/60/90/100%).
- 100% reads: scales **linearly** to ~650,000 cmds/s (unbatched, 6 replicas); ~17.5 million cmds/s batched.
- **Counterintuitive anomalies** (fundamental to any protocol where writes hit every replica and reads hit one):
  - Peak throughput `T = nα / (n·f_w + f_r)` (n replicas, α cmds/replica/s, write fraction `f_w`, read fraction `f_r`). As `n→∞`, `T → α/f_w`.
  - A small increase in write fraction causes a large throughput drop (1→2% writes ≈ halves throughput).
  - Throughput does **not** scale linearly with replicas (bounded by `α/f_w`), except at 100% reads.
- Sequentially/eventually consistent reads have similar throughput but need far fewer acceptors.

### 8.6 Skew Tolerance (vs CRAQ)
CRAQ (chain replication variant) forwards reads with pending writes to the tail → **sensitive to skew**. Compartmentalized MultiPaxos throughput is **constant** in skew `p` (agnostic to keyed data). As `p: 0→1`, CRAQ drops ~300k → ~100k cmds/s. CRAQ can do single-round-trip low-latency reads at low skew with fewer nodes, but each chain node processes 4 messages/write (vs 2 for Compartmentalized MultiPaxos replicas). Neither is strictly better: CRAQ wins for very read-heavy low-skew; Compartmentalized MultiPaxos wins for more writes or more skew.

### 8.7 Comparison to Scalog
Scalog is a replicated shared-log protocol (batching idea like batchers/unbatchers) but lacks state machine replicas. Extended Scalog with replicas for fair comparison. Scalog peaks ~400,000 vs Compartmentalized MultiPaxos ~800,000 cmds/s; Scalog uses 17 machines vs 15. Scalog bottlenecked on **replicas** (not batching) — adding proxy replicas (2 replicas + 4 proxy replicas) raised it to ~650,000 (1.625× throughput for 1 extra machine). Demonstrates: eliminate **every** bottleneck, not just one.

---

## 9. Related Work

- **MultiPaxos** — already logically decouples proposer/acceptor/replica roles, which *enables* compartmentalization.
- **PigPaxos** — relay network between leader and acceptors; relays ~ proxy leaders but simpler (only alter communication flow, can't take over other leader roles). Goal is larger clusters; compartmentalization is more general.
- **Chain Replication** — even load but 4 messages/write; tail is a read bottleneck; not partition-tolerant.
- **Ring Paxos** — decouples control/data flow + chain arrangement; doesn't optimize reads.
- **NoPaxos** — VR variant with an on-switch sequencer (hardware alternative to avoid leader bottleneck).
- **Scalog** — a protocol (vs compartmentalization, a technique); focuses on one bottleneck (batching/replication before ordering).
- **Scalable Agreement (Kapritsos & Junqueira)** — similar to Compartmentalized Mencius (round-robin log among replica clusters).
- **SEDA architecture** — pipeline of multithreaded modules; same decouple-and-scale idea applied within a single server.
- **Multithreaded Replication** — decoupling within a machine; complementary (protocol-level vs process-level).
- **Family of Leaderless Generalized Protocols (Losa et al.)** — modular template; an instance of compartmentalization.
- **Read Leases / Paxos Quorum Leases** — local reads but assume clock synchrony and leader stays a read bottleneck; Compartmentalized MultiPaxos assumes neither.
- **Harmonia / FLAIR** — specialized hardware (network switch); skew-sensitive, assume clock synchrony.
- **Sharding** — orthogonal further scaling if state can be partitioned.
- **Low-latency protocols (CURP, SpecPaxos)** — compartmentalization *increases* network delays (MultiPaxos 4 → Compartmentalized 6); choose latency-optimized protocols if latency is the goal.

---

## 10. Conclusion

Compartmentalization — **decoupling + scaling** — systematically eliminates SMR throughput bottlenecks. Establishes a new baseline for MultiPaxos: **6× throughput** on write-only, **16×** on a 90% read workload, without complex or specialized protocols.

---

## Key Takeaways

- **Decoupling enables scaling.** Many SMR components can't be scaled because one node holds two responsibilities (e.g., sequencing + broadcasting). Splitting them lets you scale the parallelizable part.
- **Eliminate *every* bottleneck.** Fixing one bottleneck just reveals the next; the contribution is a framework covering all of them.
- **It's a technique, not a protocol** — incrementally adoptable on top of battle-tested implementations (MultiPaxos, Mencius, S-Paxos, EPaxos, even partially Scalog).
- The fundamental limit: writes hit every replica, reads hit one — giving `T = nα / (n·f_w + f_r)`, bounded by `α/f_w`. Sharding is the escape hatch.
- Trade-off: higher throughput at the cost of more machines and more network delays (worse for WANs).
