+++
title = "Batchers and unbatchers"
weight = 19
+++

With proxy leaders, the leader handles two messages per command. That is still one message
**per command**. A **batcher** puts many writes into one message before the leader sees them,
so the leader's cost becomes one message per batch. An **unbatcher** does the reverse on the way
back: it gives each write its own answer. In paros, a batcher merges writes of one
**multi-writer** journal into one slot.

## The failure

**The leader is the bottleneck again.** After proxies, the acceptors and the proxies pay per
batch, but the leader still receives `n` messages for a batch of `n` commands (§4.1 of the
Compartmentalized Paxos paper).

**A merge a writer can see.** Merge writes carelessly and a writer can observe the merge:

1. Writer W1 sends an empty write; the journal refuses an empty write.
2. A batcher merges it with W2's three records. The merged write has three records, so the
   journal accepts it, and W1's empty write is "accepted" too.
3. Or: a batcher merges a **fenced** write (one that carries a leader uuid). The answer is lost
   and the writer re-sends the same write. The journal deduplicates a re-send only when it is
   the identical write, and the merged write was not identical. The journal refuses a write it
   already holds.

## The rule

1. A batcher keeps one lane per journal. A lane never mixes journals.
2. It merges only **unfenced writes of a multi-writer journal**. A multi-writer write gets its
   positions `[next_seq, next_seq + n)` at apply, so a merge of `k` writes in order lands as one
   dense range.
3. It sends a write alone when a merge could change its verdict: an empty write, a write over the
   leader's limits by itself, and every fenced write.
4. A lane flushes when it is full, when the next write would pass the leader's limits, or at the
   driver's tick. The core has no timer.
5. The unbatcher cuts the answer's range in order: write `i` gets the positions of its own
   records (`Unbatcher::scatter`).
6. A merged write that may have run is answered **ambiguous** to every writer in it. The batcher
   never sends it again: each writer owns its ambiguous write.

## Why the rule is enough

A merged write is accepted or refused **whole**, and within the leader's limits a multi-writer
write is never refused for its size. So a merge never turns an accepted write into a refused
one, and each write sits at its own dense range. The merge changes what one slot holds, not how
a slot is chosen, so Paxos safety does not change. A single-writer journal merges in its own
client's pipeline instead, which re-sends the merged write unchanged.

## The code map

| What | Where |
|---|---|
| The batcher | `crates/lib/paros-core/src/batcher.rs:134` `Batcher` |
| Queue one write | `crates/lib/paros-core/src/batcher.rs:206` `Batcher::push` |
| Flush on the driver's tick | `crates/lib/paros-core/src/batcher.rs:265` `Batcher::tick` |
| Learn the leader's limits | `crates/lib/paros-core/src/batcher.rs:179` `Batcher::lower_limits` |
| The limits | `crates/lib/paros-core/src/batcher.rs:40` `BatchLimits` |
| The unbatcher | `crates/lib/paros-core/src/batcher.rs:294` `Unbatcher::scatter` |

## The proof

The simulation checks each write a batcher answered: "batcher: a write's range holds exactly its
records" and "batcher: two writes never share a position"
(`crates/sim/paros-sim/src/workload/fleet/created.rs`). A scenario boots half the machines with
a record limit of two or three, so the too-large path runs. The driver gates prove that a merged
write over the limits is split, that a merged write without an answer is answered ambiguous, and
that a fenced write passes through alone.
