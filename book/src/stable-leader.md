# The stable leader

A log of independent Paxos instances is correct but naive: every slot would cost two
round trips, and competing proposers would collide on every one. Multi-Paxos becomes
efficient by electing **one stable leader** that runs Phase 1 a single time — for the
whole log suffix — and then streams nothing but Phase 2 for as long as it stays up.
That optimization brings three obligations: the new leader must recover what the old
one left half-decided, it must account for the slots its promise quorum said *nothing*
about, and something must stop two nodes from campaigning forever.

> **Play it.** Three Act II levels, in order.
>
> - [`act2/elect-a-leader`](play/#act2/elect-a-leader) — tick a follower to its
>   election timeout, drive its single `Prepare(from_slot)` by hand, read what the
>   Promises reported, and answer the recovery prompt: re-propose that slot, or go
>   straight to new work?
> - [`act2/steady-state`](play/#act2/steady-state) — one Accept round per command,
>   slot 7's Accept fired before slot 6 comes back, and the Heartbeat carrying the
>   commit index. Reward: heartbeat delivery stops being your job.
> - [`act2/the-permanent-gap`](play/#act2/the-permanent-gap) — drop exactly slot 1's
>   Accepts, let slot 2 be chosen, crash the leader so the volatile proposer map dies
>   with it, then elect a successor and decide what to do about slot 1. Goal:
>   `chosen_gap()` back to `None`, every client command applied.

<!-- toc -->

## Phase 1 once, Phase 2 forever

Phase 1 does not mention a value. A Prepare only claims a ballot and asks what has
been accepted, so a proposer can claim a ballot for the **entire rest of the log** in
one message. Lamport describes a new leader doing exactly this:

> It runs phase 1 for instances 135 to 137 and all instances `> 139` using one
> proposal number, a single short message.

[Paxos Made Live](https://15799.courses.cs.cmu.edu/fall2013/static/papers/paxos_made_live.pdf)
(Google's Chubby experience) names the steady-state win:

> if the coordinator doesn't change between instances, propose messages can be
> omitted. Pick a long-lived coordinator, the **master**.

paros implements this literally. Its `Prepare` carries a `from_slot`, and one Prepare
covers every slot at or after it:

```rust
Prepare {
    from: NodeId,
    ballot: Ballot,
    from_slot: Slot,   // covers every slot >= from_slot
}
```

The matching `Promise` reports **all** entries the acceptor accepted in that suffix,
so one exchange tells the new leader everything in flight across the whole log. A node
that times out waiting for a leader becomes a `Candidate` and sends that Prepare
(`on_check_leader`); a promise quorum makes it `Leader` (`try_become_leader`,
`crates/paros-core/src/node/election.rs`). Afterwards a client command costs exactly
one round trip — the leader assigns the next free slot and goes straight to Accept —
which Lamport notes is not merely fast but **optimal**: "Phase 2 of Paxos has been
shown to have the minimum possible cost of any fault-tolerant agreement algorithm."
This is PMMC's scout-then-commander pattern, with the scout (`Election`) and the
commander (`proposer::Rounds`) both inside the node's own `Proposer`.

## The new leader's two duties

**Recover what the Promises reported.** The previous leader may have left slots
half-decided, and P2c (from [Why one value is safe](safety.md)) says those must be
re-proposed at the new ballot, never overwritten. The Promises piggybacked exactly the
values needed; paros collects them in the election's `recovered` map and re-proposes
each before opening fresh slots.

**Fill the slots the Promises were silent about.** Pipelining is what makes this
possible: the old leader streams `Accept`s for several slots at once, so a slot can
reach it *alone* while a later slot reaches enough acceptors to be chosen. If the
promise quorum excludes the old leader — it crashed, which is usually why there is an
election — the earlier slot appears in no Promise, and `next_slot` (one past the
highest recovered slot) steps straight over it.

Nothing would ever propose that slot again: `propose` only hands out `next_slot`, and
a restart recomputes `next_slot` from the accepted log the same way, so the hole
outlives reboots. And a hole is not a local blemish — the contiguous chosen prefix
stops one below it **cluster-wide and permanently**. Higher slots keep being chosen and
never apply; the fresh-leader read fence sits above the hole; and commit-replay
catch-up is useless, because every node's prefix is frozen at the same place and no
peer has anything to replay.

So the leader fills every slot in `first_unchosen()..next_slot` that no Promise
described with a `Control::Noop`. That is safe for exactly the reason Phase 1 exists:
any value already chosen there was accepted by a Phase-2 quorum, that quorum
intersects this promise quorum, so at least one Promise would have carried it (an
acceptor that truncated the range answers `Nack`, never a Promise that under-reports).
The Promises carried nothing, so the slot is genuinely free. Whether an undescribed
slot means "free" is a type, not a flag: `RecoveryPolicy::Phase1Backed` licenses the
fill and the `Inherited` policy a cooperative handoff installs does not
(`proposer/recovery.rs`).

Note what the crash is doing in that story. A leader re-sends the `Accept`s for its
pending slots on every heartbeat, so a slot that merely lost messages is not lost — the
retry lands and it decides. The hole needs the leader to *forget*: the proposer's round
map is volatile, so a crash or step-down drops it and the slot stops being re-sent by
anyone. That is why the level makes you crash the leader rather than partition it.

`Replica::chosen_gap` makes the failure observable from outside the core — `Ready` only
ever hands the driver the *contiguous* prefix, so a stranded chosen slot is invisible
otherwise — and the driver reports it through `Audit::chosen_gap` every tick. Nothing
is asserted on a gap itself, since a gap is what pipelining looks like; one that never
heals fails the end-of-run claim that **"every node converges to the cluster's chosen
prefix at the end of the settle tail"**, with the recorded gap saying where the node
was stuck. The fill itself has to happen, so the sweep gates on **"a new leader
gap-fills a hole its promise quorum never reported"** and asserts **"a recovery batch
reports only gap fills it actually started"** (`crates/paros-sim/src/audit/`).

## Holding the lead, and surviving its loss

A leader keeps its position by heartbeating. `Heartbeat` carries the leader's ballot
and its commit index; receiving it resets a follower's election clock, and the leader
uses its own heartbeat tick to **resend un-acked Accepts** so a lagging follower
catches up:

```rust
Heartbeat {
    from: NodeId,
    ballot: Ballot,
    commit: Option<Slot>,   // highest contiguous chosen slot, `None` if nothing
}
```

That `Option` is not decoration. `Slot(0)` is a real log position, so it cannot also
stand for *no* log position — and a watermark that used a bare `Slot(0)` for both made
a leader that had just chosen its first slot look exactly like one that had chosen
nothing. A follower missing precisely that slot compared `Slot(0)` against its own
empty prefix, concluded it was not behind, and never asked; every other repair path is
shut in that state, so it stayed stale until a *second* slot was chosen and the beat
carried a number that meant something.

The commit index on each beat is itself a piggyback, riding a message the leader
already sends, so followers advance their prefix at no extra cost. The whole life of a
node is three roles and four transitions: a `Follower` whose election timeout fires
becomes a `Candidate`; a `Candidate` that wins a promise quorum becomes `Leader`;
either falls back to `Follower` on a higher ballot; and a `Follower` reset by a
`Heartbeat` or an `Accept` stays one. The role is volatile in every direction —
`ColocatedNode::new` always boots a `Follower` — which is why a crash *is* an
abdication and needs no durable fence.

## Liveness: curing the duel without touching safety

Two proposers can livelock, each preempting the other forever. Safety never bends
during a duel, but progress stalls. The cure is to stop two nodes campaigning at once,
and it lives in the driver, not the safety core.

First, a rejected leader does **not** immediately retry: on a nacked `Accept` paros
steps the node down to `Follower` and waits (`on_nack` / `become_follower`), and the
in-code comment says plainly that "we do not immediately re-prepare: that, with the
randomized timeout, is the dueling-proposer livelock fix." Second, the election timeout
is **randomized** — the driver draws a fresh jittered value (`draw_election_timeout`,
`crates/paros/src/driver/`) so two followers rarely time out together. (Classic
single-decree Paxos cures the same duel with exponential backoff; the randomized
timeout is the Multi-Paxos equivalent.)

This is the separation Lamport insists on: leader election is needed only for
**progress**, never for safety. By the FLP result no purely asynchronous algorithm can
guarantee a leader is elected, which is why the cure uses real time and randomness.
Because liveness is a claim about what a run *reaches*, the simulation gates it rather
than asserting it — a campaign must show **"a leader is elected"**, **"a stable leader
streams several slots"** and **"leadership turns over and the cluster recovers"**,
while the always-checks (**"at most one value is ever chosen for a slot"**, **"a node's
promised ballot never decreases"**) hold throughout. A leader that loses its quorum
must also notice: **"a leader deposed by a promise-majority stops beating within an
election timeout (CheckQuorum)"** (`crates/paros-sim/src/audit/`).

## Optimizations at a glance

Multi-Paxos in practice is the bare protocol plus a set of optimizations. Most are
about doing less work in the steady state, and several are careful uses of
piggybacking. Here is the list, and where paros stands:

| Optimization | What it buys | In paros |
|---|---|---|
| Stable leader (master) | run Phase 1 once, then one round trip per command | yes, the `Leader` role |
| Phase-1 batching | a single `Prepare` claims the whole log suffix | yes, via `from_slot` |
| Piggybacking | ride data on messages already in flight (accepted values on `Promise`, commit index on `Heartbeat`) | yes |
| Pipelining | propose slot `i+1` before slot `i` is chosen | yes, the leader streams `Accept`s |
| Randomized backoff | jittered election timeout plus step-down on `Nack`, to break the proposer duel | yes, `draw_election_timeout` |
| Catch-up | a lagging node relearns missed values by resend and piggyback | yes: heartbeat resend, election recovery, commit-replay catch-up, *and* snapshot transfer once it falls below the floor |
| No-op gap fill | fill a hole with a no-op so the log can advance past a dead leader | yes: recovered slots are re-proposed, and every slot the promise quorum reported nothing for is filled with a `Control::Noop` |
| Command batching | pack many client commands into one slot | not yet |
| Read-index reads | linearizable reads with no log write, one heartbeat-ack round | yes: see [Why reads are not free](linearizable-reads.md) |
| Leader leases | serve linearizable reads locally for a lease period, skipping even the ack round | not yet |
| Truncation and snapshots | discard the applied log prefix; snapshot the state | yes: a leader-decided `Truncate` control command (one cluster-wide floor), plus opaque snapshot transfer for below-floor recovery — see [Truncation and snapshot restore](truncation-and-snapshots.md) |
| Cooperative handoff | move Phase-2 authority to another node under the *same* ballot, no second Phase 1 | yes, `relinquish_to`, one hop only |
| Flexible and grid quorums | trade Phase-1 cost against Phase-2 cost (`q1 + q2 > n`) | yes, as deployment data in `QuorumSystem` |

The "not yet" rows are the roadmap past this part. With a stable leader streaming a
log, one question remains: what happens when a node crashes mid-stream and comes back?
That is [Crash and restart safety](restart-safety.md).
