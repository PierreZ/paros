# Why reads are not free

Writes go through consensus, so a read looks like the easy half: the leader holds
the whole log, and it can surely answer from local state. It cannot. "I am the
leader" is a **belief** about the past, not a fact about the present. A leader
that answers from local state alone may have lost the leadership a millisecond
ago. A successor it has not heard of may already have overwritten the value.
This chapter builds the correct read path, the **read-index** protocol, and the
client-side oracle that keeps it honest: a **linearizability** checker over the
client's own history.

> **Play it.** One Act II level and four Act III levels build the read path.
>
> - [`act2/the-read-that-lies`](play/#act2/the-read-that-lies) — decide whether a
>   leader that peers have already deposed may answer a read.
> - [`act3/read-index`](play/#act3/read-index) — capture the index, confirm with a
>   beat, and judge one stale ack that arrives from an older beat.
> - [`act3/the-fresh-leader-trap`](play/#act3/the-fresh-leader-trap) — refuse a
>   read at a new leader that holds a valid quorum and a lagging chosen index.
> - [`act3/linearizable-or-not`](play/#act3/linearizable-or-not) — produce a
>   history with a read across a leader change, and have the three conditions
>   judge it.
> - [`act3/chosen-is-not-applied`](play/#act3/chosen-is-not-applied) — answer a
>   client retry for a slot that is chosen above a hole, twice.

<!-- toc -->

## The strawman: write a no-op to read

There is a correct read that needs no new protocol. The frankenpaxos notes in
this repository put it plainly:

> paros v1 can serve a linearizable read the dumb way — propose a no-op and
> read at its slot — long before a dedicated, scalable read path is worth
> building.

The commit proves that the proposer led **now**, at a quorum. It also costs a log
slot, an fsync on every acceptor and a full round trip, for each read.

## Read-index: confirm, do not write

Read-index keeps the proof and drops the log write. This is the seam that
etcd-raft exposes as `ReadIndex`/`ReadState`. The no-op's slot never mattered:
only the evidence of current leadership did, and a heartbeat round carries that
evidence for free. The leader answers a read in three moves:

1. **Capture.** Record the applied watermark, `chosen_index`, as the read index.
2. **Confirm.** Broadcast a heartbeat and collect acks from a quorum. Quorum
   intersection does the rest: if a higher ballot had committed a newer write
   first, a quorum promised that ballot, and one member of any quorum that answers
   us must refuse our older one.
3. **Serve.** Once the applied prefix covers the read index, answer from local
   state. No log write, no fsync, one message round.

Two details carry the safety, and both live in `paros-core`.

**Acks are matched to beats.** A `Heartbeat` carries a monotone per-ballot `seq`,
and a follower echoes the ballot and the `seq` in its `HeartbeatAck`. An ack
credits a read round only when it echoes the leader's **current** ballot and a
`seq` at or after the beat that opened the round. An ack to an earlier beat proves
nothing, because the follower may have promised a higher ballot elsewhere after it
sent that ack. Monotone seqs also batch: a later ack confirms every older pending
round at once.

**A follower acks only what its promise allows.** The ack leaves the same guard
that adopts a heartbeat's ballot, `ballot >= max_promised_ballot`. A deposed
leader keeps beating into its partition, and every follower that promised the new
ballot refuses to ack the old one. Its read rounds starve, its parked replies time
out, and the client retries elsewhere.

In the sans-IO core, `ColocatedNode::read_index(ctx)` starts a round. The
confirmation surfaces through the `Ready` handshake as a consume-once
`ReadState{ctx, index}`, after the batch's committed entries are applied. The
driver parks the client's reply under `ctx` and answers when the `ReadState`
arrives. paros holds no application state machine, so the state a read serves is
the **applied log prefix itself**: `ReadAck.read_index` is the watermark, and
`None` is the empty prefix.

## The fresh-leader trap

The confirmation round alone is not enough. A leader that has just won an election
holds a valid quorum, and its `chosen_index` can still lag writes that the
previous leader acknowledged. Election recovery re-proposes those slots, and they
have not decided again yet. Capture and confirm would serve that stale watermark
with a fresh quorum. Raft commits a no-op in the new term before it
serves a read; paros waits instead.

At the moment it wins, a leader records a **read floor**: the highest slot its
promise quorum reported, `next_slot - 1`. Quorum intersection, plus the truncation
floor guard of the [previous chapter](truncation-and-snapshots.md), puts every
write that an earlier leader acknowledged at or below that slot. A read captures
`max(chosen_index, read_floor)` and confirms only when **both** conditions hold:
the ack quorum is in, and `chosen_index >= index`. The second condition resolves
inside `advance_chosen_index`, the moment the recovered suffix decides again.

## What "linearizable" means here

The word has a precise definition from Herlihy and Wing, quoted at length in the
compartmentalized-Paxos transcript under `docs/references/`: every operation
appears to take effect atomically at one instant between its invocation and its
response. In paros the register under observation is the applied log prefix. A
write appends to it at its committed slot, and a read observes its watermark.
Because the log totally orders the writes, a recorded history needs no search.
Three conditions over the client's program order are the whole test:

1. A committed read observes every write acknowledged before it began: its
   watermark is at or past each such write's slot.
2. Watermarks do not move backwards across reads that do not overlap.
3. A write issued after a committed read lands **above** that read's watermark.

Failed and timed-out operations constrain nothing. A timed-out write may still
commit later, and that is not a violation. Condition 1 is what the deposed leader
and the fresh leader both break.

## Chosen is not applied

Condition 1 leans on the word *acknowledged*, so the ack must mean what it says. A
slot is **chosen** when a quorum has voted for it, and pipelining makes that true
at slot 6 while slot 5 is still open. A slot is **applied** when this node hands it
to its state machine, strictly in order. A node that acks a chosen-but-unapplied
command promises the client something that no node can read back yet. paros had
that bug: `mark_chosen` recorded a command as applied the moment the slot was
learned chosen. The fix is a definition rather than a special case, because
`applied_seq` is written **only** by the contiguous walk.

Both dedup tables must move together, which matters more than it looks. Move the
applied table alone and a retry in that window misses both tables. It then takes a
fresh slot for a command that is already chosen, and executes it twice. That is
worse than the early ack. So `mark_chosen` re-points the in-flight table at the
slot instead. A retry there gets `Duplicate(k)`, the reply parks on slot `k`, and
the apply loop fires it when the write enters the prefix.

## Where this lives in paros

| Protocol name | Symbol |
|---|---|
| Open a read round | `ColocatedNode::read_index(ctx)`, `Proposer::open_read` |
| The beat evidence | `Heartbeat{seq}`, `HeartbeatAck{ballot, seq}` |
| Credit and confirm | `Proposer::credit_read_ack`, `Proposer::confirm_reads` |
| The fresh-leader guard | `Proposer::read_floor`, `Replica::covers` |
| The driver seam | `Ready::read_states`, `ReadState{ctx, index}` |
| The two dedup tables | `Replica::applied_at`, `Replica::inflight_at` |
| A retry's answer | `ProposeResult::Duplicate`, `ProposeResult::Chosen` |

## Proven, not asserted

The client is the only party that knows its own program order, so this check lives
in the workload. `ClientHistory` (`crates/paros-sim/src/audit/client.rs`) records
every operation the client issues and asserts the three conditions over that
history: **"a committed read observes every write completed before it began"**,
**"committed-read watermarks never move backwards"**, and **"a write issued after
a committed read lands above its watermark"**. Nothing reads a trace back. Every
later stage — storage faults, reconfiguration — inherits this client's-eye
definition of "nothing was lost".

The red run came first, as the house rule demands. The read RPC landed naively,
serving `chosen_index` whenever `role == Leader`, and the sweep hunted until one
seed served a read from a stale leader's belief. That seed is cited as evidence
and is not kept as a replay, because a seed names a draw schedule rather than a
scenario. What stands guard is the sweep itself, beside the core test
`fresh_leader_read_waits_for_the_read_floor`.

The write ack got the same treatment. The fast path acked with no slot at all, and
both the workload and the audit skipped slotless acks, so the exemption was
exactly the size of the bug. `ProposeResult::Chosen` now carries its slot, so the
ack is falsifiable. The audit joins every committed ack (`Audit::client_acked`)
against the acking node's applied prefix (`Audit::applied`), and asserts **"a
committed write ack names a slot the acking node had already applied"**. It went
red on twelve seeds in the first two thousand.

Read-index deliberately does not buy locality. Every read still goes to the leader
and still costs a round trip. Act IV takes both away:
[quorum reads](beyond-multi-paxos.md#quorum-reads) serve a linearizable read at
any replica with no leader and no clock, and the
[optimizations table](stable-leader.md#optimizations-at-a-glance) keeps the score.
