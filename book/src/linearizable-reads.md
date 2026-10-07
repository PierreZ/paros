# Why reads are not free

Writes go through consensus, so a read looks like the easy half: a node holds the
log, and it can answer from local state. It cannot. A node does not know on its
own if its log is current. A new leader can decide slots without this node, and
no message tells this node about them. A quiet cluster and a cluster that moved
on look the same to it. This chapter builds the correct read path, the **quorum
read**, and the client-side oracle that keeps it honest: a **linearizability**
checker over the client's own history.

> **Play it.** One Act II level and three Act III levels build the read path.
>
> - [`act2/the-read-that-lies`](play/#act2/the-read-that-lies) — ask a follower
>   that missed a leader change for a read, and decide if it can answer now.
> - [`act3/the-fresh-leader-trap`](play/#act3/the-fresh-leader-trap) — hold a
>   read at a new leader while the slot that it inherited is not decided again.
> - [`act3/linearizable-or-not`](play/#act3/linearizable-or-not) — make a
>   history with a read across a leader change, and let the three conditions
>   judge it.
> - [`act3/chosen-is-not-applied`](play/#act3/chosen-is-not-applied) — answer a
>   client retry for a slot that is chosen above a hole, two times.

<!-- toc -->

## The strawman: write a no-op to read

There is a correct read that needs no new protocol. The frankenpaxos notes in
this repository put it plainly:

> paros v1 can serve a linearizable read the dumb way — propose a no-op and
> read at its slot — long before a dedicated, scalable read path is worth
> building.

The commit proves that the read saw every earlier write, at a quorum. It also
costs a log slot, an fsync on every acceptor and a full round trip, for each read.

## The quorum read: ask a Phase-1 quorum

The no-op's slot never mattered: only the evidence did. A **quorum read**
(Compartmentalized Paxos §3.4) gets that evidence with no log write and no
leader. Any node answers a read in three moves:

1. **Ask.** Send a `PreRead` to a Phase-1 quorum (a row of a grid, the whole
   membership under a majority): what is the highest slot you have voted in?
   Each acceptor answers its **vote watermark** in a `PreReadAck`.
2. **Settle.** Once a whole Phase-1 quorum answered, take the maximum. A write
   acknowledged before this read began was chosen by a Phase-2 quorum, and the
   Phase-1 quorum that answered crosses it, so the maximum is at or above that
   write.
3. **Serve.** Once this node's applied prefix covers the maximum, answer from
   local state. No log write, no fsync, no leader and no clock.

An acceptor raises its watermark when it **votes**, not when a slot is chosen.
A read can therefore settle on a slot that is not decided yet. It then waits for
the slot. The wait costs the reader time and never gives the client an old
answer.

In the sans-IO core, `ColocatedNode::quorum_read(ctx)` opens a read on any node
(`QuorumReads`, `quorum_read.rs`). The served read surfaces through the `Ready`
handshake as a consume-once `ReadState{ctx, index}`, after the batch's committed
entries are applied. The driver parks the client's reply under `ctx` and answers
when the `ReadState` arrives. paros holds no application state machine, so the
state a read serves is the **applied log prefix itself**: the `ReadState`'s
`index` is the watermark, and `None` is the empty prefix. Every public `Read`
(#204) is a quorum read, served by any node or replica; the grid version is in
[Beyond Multi-Paxos](beyond-multi-paxos.md#quorum-reads).

paros once also had the leader-side path that etcd-raft exposes as
`ReadIndex`: the leader captured its applied watermark, proved it still led with
a quorum of heartbeat acks, and served. It put every read on the leader, and no
public call reached it after #204, so it retired (#243).

## The fresh-leader trap

A leader that has just won an election holds a valid quorum, and its
`chosen_index` can still lag a value that the previous leader started. Election
recovery re-proposes that slot, and it has not decided again yet. A read that
the new leader answers from its own state alone would miss the value. Raft
commits a no-op in the new term before it serves a read.

A quorum read needs neither. The acceptor that holds the inherited value voted
in its slot, so the Phase-1 quorum reports the slot, and the read waits until
`Replica::covers` says the applied prefix reaches it. The wait resolves inside
`advance_chosen_index`, the moment the recovered suffix decides again.

## The campaign trap

Reconfiguration adds a second trap. On a deployment with matchmakers, a
configuration is bound to a ballot, and an acceptor learns the next one from
the `Prepare` it promises. A campaign may never finish, though, and the older
configurations it would have to cover may still hold slots that its own
quorums never voted. In #260 a leader under `{0, 1, 3}` with a Phase-2 quorum
of one chose a slot alone. It then campaigned with `{0, 2, 3}` and died.
Acceptor 3 had promised the campaign, so it read over a majority of the new
configuration, `{2, 3}`. Neither had voted the slot, and for a second every
read it served came back without it.

So a read is judged over a **read basis** (`ReadBasis`), not over the node's
belief. A basis is the configuration of a leadership that won its ballot: its
own election or handoff, or the leader's heartbeat. It carries that
leadership's **fence**, the highest slot its winning Phase 1 could have found
chosen under an older configuration. The read is served at the larger of the
row's watermark and the fence. A node that promised a newer campaign, or has
heard no leader since it booted, opens no read until the next beat arrives.
Reads go unavailable during a campaign; they never go stale.

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
commit later, and that is not a violation. Condition 1 is what a read answered
from a stale node's local state breaks.

## Chosen is not applied

Condition 1 leans on the word *acknowledged*, so the ack must mean what it says. A
slot is **chosen** when a quorum has voted for it, and pipelining makes that true
at slot 6 while slot 5 is still open. A slot is **applied** when this node's
contiguous walk reaches it, strictly in order: from then on a client that reads the
log through this node can see it and fold it into its own state. A node that acks a chosen-but-unapplied
command promises the client something that no node can read back yet. paros had
that bug: `mark_chosen` recorded a command as applied the moment the slot was
learned chosen. The fix is a definition rather than a special case: the
replica's fold position (`folded`) moves **only** with the contiguous walk.

The same definition answers a retry. Since the journal API (#204) there is no
dedup table at all: the log is the at-most-once table. A retry is the identical
write — generation, owner, position and bytes — and the journal state machine
judges it at apply like any other slot (`JournalState::apply`): it is a
`Duplicate` exactly when the log already holds that write at that position.
The judgement at apply is the safety rule (a leader may refuse early from its
own fold as an optimisation, never instead of it), and the driver answers a call
only with the verdict its slot folded to (`paros::driver::calls`), so an acknowledged write
is always one a read through this node can already see.

## Where this lives in paros

| Protocol name | Symbol |
|---|---|
| Open a read | `ColocatedNode::quorum_read(ctx)`, `QuorumReads::open` |
| Which configuration | `ColocatedNode::read_basis`, `ReadBasis{config, since, fence}`, `Heartbeat{fence}` |
| The evidence | `PreRead{reply_to, ctx}`, `PreReadAck{watermark}`, `Acceptor::vote_watermark` |
| Settle and serve | `QuorumReads::fold`, `QuorumReads::serve`, `Replica::covers` |
| The driver seam | `Ready::read_states`, `ReadState{ctx, index}` |
| The verdict at apply | `JournalState::apply`, `Outcome` (`journal_state.rs`) |
| A retry's answer | `Outcome::Duplicate`, answered from the fold (`paros::driver::calls`) |

## Proven, not asserted

The client is the only party that knows its own program order, so this check lives
in the workload. `ClientHistory` (`crates/paros-sim/src/audit/client.rs`) records
every operation the client issues. This stage asserted three conditions over that
history: a committed read observes every write completed before it began,
committed-read watermarks never move backwards, and a write issued after a
committed read lands above its watermark. Since the journal API (#205) the history
is judged whole instead: every attempt at `Write`, `Read`, `SetLeader` and
`Truncate`, answered or not, is searched for a linearization against the
sequential model of a journal (`crates/paros-sim/src/audit/linearizability.rs`),
and the three conditions are special cases of what that search refuses. Nothing
reads a trace back. Every later stage — storage faults, reconfiguration —
inherits this client's-eye definition of "nothing was lost".

The red run came first, as the house rule demands. The read RPC landed naively,
serving `chosen_index` whenever `role == Leader`, and the sweep hunted until one
seed served a read from a stale leader's belief. That seed is cited as evidence
and is not kept as a replay, because a seed names a draw schedule rather than a
scenario. What stands guard is the sweep itself, beside the core tests of
`node/tests/quorum_reads.rs`.

The write ack got the same treatment. The fast path acked with no slot at all, and
both the workload and the audit skipped slotless acks, so the exemption was
exactly the size of the bug. Every answer now names its slot, so the ack is
falsifiable. The audit joins every answered verdict (`Audit::answered`) against
the answering node's applied prefix (`Audit::applied`), and asserts **"a
committed write ack names a slot the acking node had already applied"**. It went
red on twelve seeds in the first two thousand.

A quorum read also buys locality: any replica serves it, and no read waits on
the leader. The [optimizations table](stable-leader.md#optimizations-at-a-glance)
keeps the score.
