# Batchers and unbatchers

Group: Compartmentalization. Status: verified 2026-10-31.

With batching, a leader puts many commands in one slot. But the leader still receives one
message per command, so its cost stays linear in commands. A **batcher** takes the job of
forming batches off the leader: clients send to a batcher, and the batcher sends the leader one
message per batch. An **unbatcher** takes the job of answering each command off the replica
([SRSM §4.1–§4.2]). In paros a batcher merges many writers' writes of one **multi-writer**
journal into one slot, and the unbatcher hands each write its own `seq` range back (#145
(batchers)).

## 1. Problem

In [SRSM §4.1], "the leader's cost is linear in the number of *commands* (it receives `n`
messages per batch of `n`)", while the acceptors' and proxies' costs are per batch. After
proxy leaders and batching, the leader is the bottleneck again. [SRSM §4.2]: a replica that
executed a batch of `n` sends `n` replies.

## 2. Model

- The Multi-Paxos model. A slot holds one write; a write holds many records.
- A **multi-writer** journal has no leader uuid and no expected sequence: an accepted write gets
  `[next_seq, next_seq + n)` at apply (`docs/architecture/02-data-plane.md` §2.4). Writers own
  duplicates (at-least-once).
- A batcher is stateless. It may crash and lose what it queued; its writers see no answer.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| `Batcher` | one lane per journal: the queued writes and their records, the limits | no |
| `Unbatcher` | nothing: a pure scatter | no |
| Leader | unchanged: it sees one write | the slot is durable as any other |

## 4. Normal flow

1. A writer sends `BatchWrite` (`0x5041_0501`, the plain `Write`) to a batcher it was given by
   `Resolve`.
2. `Batcher::push` queues the write in its journal's lane. A lane never mixes journals.
3. The lane **flushes** when it holds `BatchLimits::flush_records` records, when the next write
   would pass the leader's limits, or at the driver's `Batcher::tick` (the core has no timer).
4. The batcher sends the lane to the journal's leader as one unfenced `Write` of the
   concatenated records, and follows the leader hints itself.
5. The leader answers once the slot applies, with the range `[seq, seq + N)`.
6. `Unbatcher::scatter` cuts the range in order: write `i` gets the positions of its own
   records, dense and disjoint.

```mermaid
sequenceDiagram
    participant W1 as Writer 1 (2 records)
    participant W2 as Writer 2 (3 records)
    participant B as Batcher + unbatcher
    participant L as Leader
    W1->>B: BatchWrite(r1, r2)
    W2->>B: BatchWrite(r3, r4, r5)
    Note over B: lane of journal J flushes
    B->>L: Write(J, r1..r5), unfenced
    Note over L: one slot; applied at [40, 45)
    L->>B: Ok(seq 40, count 5)
    B->>W1: Ok [40, 42)
    B->>W2: Ok [42, 45)
```

The unbatcher runs in the batcher's process: in paros the leader answers a held write itself, so
the answer comes back to the batcher that sent it, and a separate unbatcher would be one more hop
(decided on 2026-10-19, #145).

## 5. Recovery flow

- **The merged write is too large.** The leader refuses it `TooLarge`. The batcher learns the
  leader's limits (`Batcher::lower_limits`: a limit only goes down) and sends each write of it
  again alone.
- **No answer.** A merged write that may have run is answered **unanswered** (ambiguous) to every
  writer. The batcher never sends it again: the writers own an ambiguous write.
- **A leader change.** The batcher follows the leader hints and removes them from the answer.
- **A write the batcher must not merge** goes alone: an empty write (the journal refuses it; a
  merged one would be accepted with its neighbours), a write over the limits by itself, and a
  fenced write (it carries a leader uuid).

## 6. Why it is safe

`k` writes of one multi-writer journal merged in order into one write change nothing a writer
can observe: the merged write lands at `[seq, seq + N)` as a whole or not at all, and each write
sits at its own dense range (the `batcher.rs` module doc). A merged write is accepted or refused
whole. Within the limits a multi-writer write is never refused for its size, so a merge never
turns an accepted write into a refused one.

A fenced write is never merged by a batcher, because the log deduplicates a re-send only when it
is the identical write. A batcher that re-merged a fenced write with other writes would make the
journal refuse a write it already holds. A single-writer journal merges in its client's
`Pipeline` instead, which re-sends the merged write unchanged (#709 (merge fenced writes)).

## 7. Liveness

- Batchers are embarrassingly parallel ([SRSM §4.1]). The tenant coordinator books each
  multi-writer journal's batchers, by its `batchers=` count, on the cell's `stateless` machines
  (#753 (per-journal proxies)).
- A lane flushes at the latest one `batch_tick` after its first write.
- A dead batcher loses only its queued writes, which its writers see as unanswered.

## 8. Costs

- One extra hop: writer → batcher → leader.
- The leader receives one message per batch ([SRSM §4.1]).
- A write waits up to `batch_tick` in a lane that does not fill.
- [SRSM §8.4]: batching with 2 batchers and 2 unbatchers took the batched deployment to about
  300,000 commands/s, and larger batches to about 500,000.

## 9. In paros

| What | Where |
|---|---|
| The pure roles | `paros-core/src/batcher.rs`: `Batcher`, `push`, `tick`, `lower_limits`, `BatchLimits`, `Batch`, `Unbatcher::scatter` |
| The driver | `paros-roles/src/batcher.rs`: `run_batcher`; every machine serves the endpoint over the cell's address book |
| The decision | `docs/architecture/02-data-plane.md` §2.4 |

**Sim gates and oracles:**

- `"batcher: a write's range holds exactly its records"`, `"batcher: two writes never share a
  position"` (`paros-sim/src/workload/fleet/created.rs`).
- `"batcher: a merged write joins its journal's appended set"` (`workload/system.rs`).
- Scenarios: `"batchers: a run draws the batchers scenario"` and the batcher-limits scenario,
  where half the machines boot with a record limit of two or three so the too-large path fires
  (#702, commit `c88d808`).
- Driver gates: `"batcher: a merged write over the leader's limits is split"`, `"batcher: a
  merged write goes unanswered and is answered ambiguous"`, `"batcher: a fenced write passes
  through alone"`, `"batcher: a batch is re-sent across a leader change"`.

**Divergences from the sources:**

- [SRSM §4.1] batches any commands. paros merges only writes of one multi-writer journal, because
  only there does a merge change nothing a writer can observe.
- [SRSM §4.2] puts unbatchers after the replicas. paros runs the unbatcher in the batcher's
  process, because the leader answers the batcher directly.
- [SRSM §4.1] sends to a random batcher. paros's client sends to a batcher `Resolve` names.

## 10. Pitfalls

- **Merging a fenced write.** A merge of a single-writer journal's writes in a batcher would make
  a re-send non-identical, and the journal would refuse a write it holds. Fenced writes merge only
  in the client's `Pipeline` (#709).
- **Re-sending an ambiguous merged write.** It could duplicate every write in it. The batcher
  answers it unanswered and lets each writer decide.
- **A shared waiter that breaks replay.** A batcher that parked on the address book's watch
  channel added a second waiter there, and tokio spread the waiters in an order the seed did not
  decide; same-seed replay broke. The batcher reads the cached book instead (commit `544e289`).

## 11. Relatives

- [Proxy leaders](proxy-leaders.md): the other half of the leader's load.
- [Replica tier](replica-tier.md): the unbatcher's place in the paper.
- Writer fence (planned): why a fenced write is never merged by a batcher.

## 12. Sources

- [SRSM] *Scaling Replicated State Machines with Compartmentalization*:
  `docs/references/papers/scaling-rsm-compartmentalization/transcript.md` §4.1 (batchers), §4.2
  (unbatchers), §8.4 (the ablation).
- frankenpaxos: `docs/references/frankenpaxos/04-compartmentalization.md` §2 (`Batcher`).

[SRSM §4.1]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §4.2]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §4.1–§4.2]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
[SRSM §8.4]: ../../references/papers/scaling-rsm-compartmentalization/transcript.md
