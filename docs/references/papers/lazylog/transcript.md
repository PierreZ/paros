# LazyLog: A New Shared Log Abstraction for Low-Latency Applications

**Authors:** Xuhao Luo, Shreesha G. Bhat, Jiyu Hu, Ramnatthan Alagappan, Aishwarya Ganesan
(University of Illinois Urbana-Champaign; Alagappan and Ganesan also VMware Research)

**Venue:** SOSP 2024 (30th ACM Symposium on Operating Systems Principles), Austin, November
2024. Best paper. An extended version appeared in ACM TOCS (2025); it is not read here.
**Source:** <https://doi.org/10.1145/3694715.3695983>. The ACM site refuses scripted downloads;
the PDF here is the authors' lab copy, <https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf>.
Read from that PDF on 2026-11-19.
**Code:** <https://github.com/DistributedSystemResearch/24SOSP-LazyLog-Artifact> (named in the
journal design doc; not opened here).

Follow-up from the same group: [`../speclog/`](../speclog/) (OSDI 2025).

---

## Abstract (summary)

Shared logs with total order across shards (Corfu, Scalog) bind each record to its global
position **before** they acknowledge the append. That costs coordination on the critical path:
Scalog reports 1-2 ms appends even at low load. In many applications the order is needed only
when records are **read**, and readers lag behind writers. LazyLog acknowledges an append once
the record is durable, binds it to a linearizable position in the background, and makes a read of
a position wait until that position is bound. Two systems, Erwin-bb and Erwin-st, append in one
round trip.

---

## 2-3. Eager order is the problem; the LazyLog abstraction

The authors list applications where order matters only at read time: databases with readers
separate from writers, event sourcing, message queues, **high-availability journals** (state
changes read back only at fail-over), activity logging, log aggregation.

The interface:

- `append(record) -> bool`: durable, **no position returned**;
- `read(from, len)`, `checkTail()`, `trim(index)`: as in a normal shared log.

The log has an ordered part and a durable-but-unordered part. A read of the ordered part is
fast. A read past it takes a slow path: it waits until the order reaches the position. A
system orders in large background batches, so only the first slow read pays. In the worst case
(a reader at the tail, as in SMR), LazyLog moves the ordering cost from the append to the read.
The authors suggest an `appendSync` for writers that need their position.

## 4. Erwin-bb (black-box shards)

- A **sequencing layer** of f+1 replicas, with **no coordination between them**. A client sends
  the record to **all** sequencing replicas in parallel and the append completes when all ack
  (1 RTT). The records stay in memory; the layer is short-term durability only.
- **Why order can be recovered.** If append b starts after append a completed, a is already in
  every replica's log, so b comes after a everywhere. Only concurrent appends can differ between
  replicas. So any one replica's log is a linearizable order.
- **Background order.** One replica is the leader; its log order is used. It takes a batch, gives
  positions from `last-ordered-gp + 1`, and pushes the records to the shards (position `p` goes
  to shard `p mod n`, as in Corfu). When all shards ack, every sequencing replica drops the batch
  and advances `last-ordered-gp`. **Only then** the leader advances `stable-gp` and tells the
  shards.
- **Reads** go to the shards and are served only at or below `stable-gp`. A read above it waits.
  The leader's order must not be read before `stable-gp`, because a new leader may order the
  concurrent records differently.
- **Failure: views and reconfiguration.** A controller (ZooKeeper plus a stateless controller)
  detects the failure, **seals** the view (a sealed replica refuses new appends, so nothing new
  commits, since a commit needs all replicas), picks **any** live replica as the recovery replica,
  and flushes its log to the shards from its `last-ordered-gp + 1`. Records it lacks were never
  acknowledged; extra records it has were not acknowledged either, and are harmless. The new
  configuration is written to ZooKeeper **before** `stable-gp` advances, so a partitioned replica
  cannot overwrite what readers may have seen. The authors say the seal protocol resembles
  Delos and Boki. A reconfiguration paused the workload for about 15 ms, mostly failure detection
  and the ZooKeeper write.
- Shards must support "overwrite the tail" for the flush after a view change (Kafka: delete tail
  records and append).

## 5. Erwin-st (scalable throughput)

Erwin-bb sends all bytes through the sequencing layer, which saturates on 4 KB records. Erwin-st
splits a record into **data** (written by the client directly to the replicas of a shard of its
choice, in parallel, with no ordering in the shard) and **metadata** `<record-id, shard-id>`
(written to the sequencing layer in the same round trip). The leader orders metadata ids and
sends them to the shard primaries, which place the data at the global positions. Readers find the
shard of a position through the metadata log stored on the shards, cached by clients.

Client failure: data with no metadata is an orphan, removed by scrubbing. Metadata with no data
becomes a **no-op** after a timeout at the shard primary; a late data write is then refused.

## 5.5 Limits named by the authors

Writes must reach every sequencing replica, so any replica failure needs a reconfiguration (a
supermajority scheme is future work). Erwin-st scales like Corfu, not like Scalog. Writers that
need their position at once need an `appendSync`.

## 6. Evaluation (selected)

Up to 3.8X lower append latency than Corfu and two orders of magnitude lower than the Scalog
artifact at equal load. Reads that lag a few milliseconds behind appends see no extra cost.
Reads right at the tail pay the ordering cost, less with large batches. Erwin-bb gives a total
order over unmodified Kafka shards.

---

## Segments, sealing, offload and ordering

| Question | LazyLog (Erwin) |
|---|---|
| Segments | None in the paper. Shards hold positions by a fixed map (Erwin-bb) or by client choice (Erwin-st) |
| Where configuration lives | ZooKeeper: the view of the sequencing layer |
| Seal | The controller seals a view of the sequencing layer before it flushes and starts the next view |
| Ordering | After the ack, in the background, by the sequencing leader's log order; final at `stable-gp` |
| What a seal must carry | Every acknowledged but unordered record of the sealed view (the flush) |
| Offload | Not covered |

## What this means for the paros journal

The journal design doc already has a "LazyLog (SOSP 2024) and SpecLog (OSDI 2025)" subsection.
Its verdict: keep "ack after chosen and applied" for v1; a lazy mode can come later as a
separate append call; LazyLog is not part of #1213 (segments experiment). The primary text
supports that verdict. Additional points from the paper:

- **The lost-ack case is real, and the paper shows why.** The journal design noted that a lazily
  acked paros write could be ordered after a `SetLeader` and refused at apply. LazyLog has no
  writer fence, so the case does not occur there. A lazy paros mode must either drop the fence for
  those writes (as the doc proposes) or check the fence at ack time against a fence value that
  cannot change before the write is ordered.
- **"Stable" is paros's "chosen and applied".** `stable-gp` advances only after every sequencing
  replica dropped the batch, so no recovery can reorder a read position. In paros the chosen
  prefix has this property by Paxos; a lazy mode would need a second watermark (durable but
  unordered) below which nothing is readable.
- **The seal must carry the unordered tail.** In Erwin the flush of the sealed view is the most
  critical step of recovery. The journal design's note "a seal would have to order, or carry into
  the successor, every record that is acked but not yet ordered" matches the paper. It is a third
  thing a paros segment end would need, after the end slot and the gap fill.
- **"Order pointers, not payloads" is Erwin-st.** Data to storage replicas, ids to the sequencer.
  In paros this would put payloads on replicas and only ids through the leader, which is a large
  change to the Compartmentalized Paxos roles. Not for v1.
- **Writes to all replicas, not a quorum.** Erwin's 1-RTT append needs every sequencing replica,
  so any replica failure stops appends until a reconfiguration. This is the Delos "seal and
  move on" model again, and the opposite of paros's quorum-based Paxos inside a segment.
