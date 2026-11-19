# Low End-to-End Latency atop a Speculative Shared Log with Fix-Ante Ordering (SpecLog)

**Authors:** Shreesha G. Bhat, Tony Hong, Xuhao Luo, Jiyu Hu, Aishwarya Ganesan, Ramnatthan
Alagappan (University of Illinois Urbana-Champaign)

**Venue:** OSDI 2025 (19th USENIX Symposium on Operating Systems Design and Implementation),
Boston, July 2025, pages 465-481.
**Source:** <https://www.usenix.org/conference/osdi25/presentation/bhat>
(PDF: <https://www.usenix.org/system/files/osdi25-bhat.pdf>). Read from the PDF on 2026-11-19.

Same group as [`../lazylog/`](../lazylog/) (SOSP 2024). SpecLog does not build on LazyLog: its
system, Belfast, is a modified Scalog. The two papers attack different latencies: LazyLog the
append latency, SpecLog the delivery latency to readers.

---

## Abstract (summary)

Durability-first shared logs (Scalog and its family) let clients write to any shard, and the
shards report their durable counts to a Paxos sequencing layer in batches. This gives elastic
shards and free placement, but readers get a record only after the global cut, so the
**end-to-end** latency (produce to processed) is high. SpecLog delivers records to readers at a
**predicted** position before the cut, and later confirms or fails the prediction. **Fix-ante
ordering** makes the prediction almost always right: the global cuts are fixed in advance, and
each shard fills exactly its quota of slots in each cut. Belfast delivers records about 3X
earlier than Scalog and cuts e2e latency by 1.4X-1.6X.

---

## 2. Delivery latency in Scalog

A record is durable on a shard's primary and backup. Every ordering interval, shards report their
log lengths; the sequencing layer computes a global cut, makes it durable with Paxos, and sends
it back; shards then assign positions (lexicographic by shard) and deliver. Batching and global
coordination sit between the write and the first read. A shard cannot guess its positions,
because other shards report any count they like ("free will").

## 3. The SpecLog abstraction and fix-ante ordering

- **Interface.** `append(record, shard) -> position`, `subscribe(from, predicate)`,
  `deliver(record, pos, speculative)`, `failSpec(k)`, `trim`. A downstream task may compute on a
  speculative record, but must wait for the confirmation before it acts outside, and must roll
  back on a failure.
- **Predetermined cuts.** A sequence of cuts `P1, P2, ...`, each giving the count of records per
  shard. The quota of shard j in cut i is `d(i,j) - d(i-1,j)`. A shard's positions in cut i start
  after all positions of cut i-1 plus the quotas of lower-numbered shards in cut i. So each shard
  knows its positions in advance.
- **Meeting quotas.** A shard with fewer records **fills no-ops**; a shard with more **delays**
  the extra records to the next cut. Replica failures inside a shard do not matter (the shard is
  replicated). Only a whole-shard failure or a shard cut off from the sequencer breaks a cut.
- **The sequencer sends actual cut i only after every shard with a non-zero quota met its quota
  for cut i.** If a shard is silent too long, the sequencer cuts from what it has, and the
  speculation fails.
- **Appends are still acknowledged only after the actual cut.** The predicted position is for
  readers, not for the writer: an ack before the cut can break linearizability (a later append on
  a lower shard can get a lower position).

## 4. Belfast

- Shards use primary-backup; the sequencing layer is a Paxos group (Scalog).
- **Rate-based quotas**, so each shard reports about once per ordering interval (1 ms).
- **Lag fix** for bursts: a bursty shard reports as soon as it meets each quota; the sequencer
  tells lagging shards to report at once with no-ops, so the extra cuts can close.
- **Speculation lease windows**: quotas and cuts change only at window boundaries (100 cuts), so
  every shard predicts with the same cuts. Shards join and leave at a window boundary. A straggler
  gets quota 0 in the next window, then a small quota to test it.
- **Staggered cuts** with many shards: cut i waits only for shard group `i mod g`.
- **Whole-shard failure**: a view change. Speculations after `confirmed-gp` (the last position of
  the last actual cut of the old view) fail, and readers roll back to it. A live shard fills the
  failed shard's slots with no-ops for the rest of the window, so the positions of other shards
  stay valid.
- No-ops are coalesced into one disk entry and one message.

## 6. Evaluation (selected)

About 3X earlier delivery and 1.6X lower e2e latency than Scalog, with 5.8% more append latency at
10 shards. Belfast handles bursts, rate changes, stragglers, shard failures and shard
add/remove with low e2e latency. Applications: intrusion detection, fraud monitoring, trading.

---

## Segments, sealing, offload and ordering

| Question | SpecLog (Belfast) |
|---|---|
| Segments | None as such; the **speculation lease window** (a run of predetermined cuts) is the unit at which the slot layout can change |
| Where the layout lives | The Paxos sequencing layer, which also holds quotas (no extra Paxos round) |
| Seal | A view change on whole-shard failure; speculation past `confirmed-gp` fails |
| Ordering | Shards report durable counts; the sequencer's actual cut is the order; predicted cuts make it known early |
| Offload | Not covered |

## What this means for the paros journal

The journal design doc's LazyLog/SpecLog subsection says: "each shard gets a fixed quota of slots
per round, so a writer can predict its position and readers act on it before the order is final,
with rollback". From the paper, one correction: it is the **shard**, not the writer, that predicts
the position, and the writer's ack still waits for the actual cut. The doc's verdict (no lazy or
speculative mode in v1, not part of #1213 (segments experiment)) is unchanged. Further points:

- **Fix-ante is a predetermined slot partition.** Slots are assigned to shards in advance and
  filled with no-ops when a shard has nothing. That is the idea of Mencius (several leaders that
  share slots by a fixed rule and skip their own slots with no-ops), which the journal design
  lists as a later option past one leader per journal. SpecLog is a recent production-scale
  example of that idea; read it with the Mencius paper when that option comes up.
- **Change the layout only at a boundary that every party knows.** The speculation lease window
  is the same rule as plan rule 1 in the journal design: the slot layout (the grid in paros, the
  quotas in Belfast) changes only at a known boundary, and never under a slot someone already
  predicted. For paros the boundary is the segment end slot.
- **A failed shard's slots are filled, not removed, until the boundary.** Belfast keeps the
  positions of other shards valid by having a live shard write no-ops for the dead one. paros
  does the same at an election (gap fill with `Noop` below the frontier).
- **Speculative delivery to readers** would need paros to expose slots that are not yet chosen.
  The paros leaderless read and the replica serve only the chosen prefix. A speculative read is
  out of scope for v1.
