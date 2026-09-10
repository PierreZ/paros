# Crash and restart safety

Paxos assumes nodes crash and come back: a majority keeps serving while a minority is
down, and a recovered node rejoins. But "come back" hides two separate requirements.
The durable writes must be flushed **before** the replies they justify leave the wire,
and the durable state must stay **consistent with what the cluster chose**, so that
rebuilding from disk cannot resurrect a value nobody decided. paros got the second one
wrong, and the simulation caught it.

> **Play it.** Two Act II levels — one requirement each.
>
> - [`act2/persist-before-send`](play/#act2/persist-before-send) — a `Ready` batch
>   holds writes and messages and you say which goes first, on every batch. Then you
>   crash the node *at* each seam — before the fsync, and after the fsync but before
>   the send — and read off what survived.
> - [`act2/what-survives-a-crash`](play/#act2/what-survives-a-crash) — crash and
>   restart at each step of a decision. The prompt that matters: a `Commit` says slot 5
>   is `Y`, your own durable record says `X` at a lower ballot. Keep `X` or take `Y`?
>   Goal: the restarted node's `HardState` never regressed and the chosen value is
>   intact.

<!-- toc -->

## What must be durable, and when

Three things must reach stable storage, and the *order* in which they do is part of
the protocol: the promised ballot, the per-slot accepted value, and the commit index.
paros splits them the way etcd-raft splits `HardState` from `entries` — two small
scalars persisted whole, the log persisted one record at a time:

```rust
// The acceptor role owns both of its own durable ops:
enum AcceptorWrite<V> {
    SetPromise(Ballot),                                      // raise the promise
    AppendAccepted { slot: Slot, ballot: Ballot, value: V },  // one accept (upsert)
}
// A `Ready` batch carries those, plus `SetChosenIndex`, `Truncate` and
// `InstallSnapshot`, as `WriteOp`s.
```

The rule is **persist before send**: a raised promise reaches disk before the
`Promise` reply, a new accepted value before the `Accepted` reply, both as
`MustSync::Sync`. A commit-index-only advance may use a relaxed write, since a chosen
value is already durable from the accept that preceded it. Note where the sync-requiring
ops live — **every one is emitted by the `Acceptor`**, so persist-before-send is an
acceptor property; the `Replica` emits only the relaxed `SetChosenIndex` and the
`Proposer` holds no durable state at all. The reason sits beside the classification:

> Sending either reply before the corresponding field is durable violates Paxos
> safety: a crash could "un-promise" or "un-accept", letting two different values
> be chosen for one slot.

[Paxos Made Live](https://15799.courses.cs.cmu.edu/fall2013/static/papers/paxos_made_live.pdf)
states the same hazard from the other side:

> A corrupted disk losing persistent state lets a replica renege on past promises,
> violating a Paxos assumption.

paros enforces the ordering with a type rather than a convention: `ready()` borrows the
node uniquely, so the driver must run the batch's four steps in sequence — persist and
fsync, send, apply the committed entries, `advance()` — and a second `ready()` before
that `advance()` is a **compile** error, not a bug to find in review.

## The bug: a restart that resurrects a dead value

Persist-before-send is necessary but not sufficient: the durable state must not just be
*written in order*, it must stay *consistent with what was chosen*. The interleaving
that broke paros (commit `608bb58`): a node persists `X` for slot 5 at a low ballot that
never reaches a quorum, then learns by `Commit` that the cluster chose a **different**
value `Y` there. Its volatile chosen map holds `Y`, but its **durable** accepted record
still holds `X` — and on boot the volatile state is rebuilt from those durable records
(`crates/paros-core/src/node/boot.rs`), so the node comes back believing slot 5 is `X`
while every other node holds `Y`. That is exactly what
[Why one value is safe](safety.md) promised could never happen, and nothing in the run
was out of order: every write was flushed before its reply, which is why
persist-before-send alone does not catch it.
[`act2/what-survives-a-crash`](play/#act2/what-survives-a-crash) is that interleaving,
with you holding the crash.

## The fix, and why one word matters

The repair is a single word. When a value is chosen, `mark_chosen` records it as the
**authoritative** accepted entry, overwriting any stale one:

```rust
// Record the *chosen* value as the authoritative accepted entry. Using
// `insert` (not `or_insert_with`) is load-bearing: a node may hold a stale
// lower-ballot accept it picked up from a failed earlier ballot, and
// `chosen` is rebuilt from `accepted` on restart. Keeping the stale entry
// would resurrect a value the cluster never chose for this slot. A chosen
// value is durable and safe to record at its choosing ballot.
self.record_accepted(slot, ballot, entry.clone()); // insert (overwrite), then
                                                    // queue an AppendAccepted
```

`record_accepted` overwrites the slot's entry and queues an
`AcceptorWrite::AppendAccepted` at the choosing ballot; `or_insert_with` would have left
the stale `X` in place. The accepted record is an **upsert by slot**, which is what the
acceptor's own doc calls it, and with the overwrite the durable log and the chosen value
can never disagree — so the restart rebuilds `Y`.

## Proven, not asserted

The reason this chapter exists is *how* the bug was found. paros is simulation-first: a
suspected safety problem is not patched on a hunch, it is turned into a **failing
simulation** first. The harness already asserted **"at most one value is ever chosen for
a slot"** on every transition; to reach the bug it needed crash and restart, so the
sweep injects `Chaos::Attrition` (a node crashes and recovers, with `prob_wipe = 0.0` so
durable state survives — a clean restart). Under that chaos the assertion went red on
real seeds; the one-word fix turned it green and the sweep ran clean across thousands.
A core test, `chosen_value_survives_restart_over_a_stale_accept`
(`crates/paros-core/src/node/tests/acceptor.rs`), pins the mechanism.

That is the loop the project lives by: make the violation reproducible, watch it fail,
fix the core, watch it pass — and a safety bug the simulation cannot reproduce is
treated as unproven. What the loop does *not* leave behind is the seed. A seed names a
draw schedule, not a scenario, so it stops reproducing the moment anything in the
randomness tree moves; the evidence is the red→green transition in the commit, and the
live guard is the sweep plus the mechanism test.

## Crashing *inside* a batch: the persist/send seam

`Chaos::Attrition` crashes a node at *process* granularity, but the driver drains each
`Ready` batch synchronously — persist, fsync, *then* send — so attrition can only crash
a node **between** batches, never at the seam *within* one. Yet the seam is where
durability is subtle: what happens if a node dies after it fsyncs an accept but before
the `Accepted` leaves the wire? Or before the fsync, with the batch half-written?

To reach those points the harness uses `buggify!()` — deterministic fault injection
activated per seed and then fired probabilistically, so only some seeds exercise a seam
crash, always reproducibly. Each seam is a `Seam` variant on the driver's `DriverHooks`
port with its own BUGGIFY location. Crashing *before* the fsync loses the whole
un-synced batch, and since no message was sent, recovery is a clean "it never happened";
crashing *after* the fsync but before the send leaves the writes durable and the
messages gone, so peers are re-driven by the next beat's resend. The simulation makes
this a *real* crash: it unwinds the node loop, drops the volatile state, and re-runs the
node from the durable storage world exactly as a fresh process would. The audit then
checks the two things a restart must never do: **"a node's promised ballot never
decreases"** and **"a chosen index never regresses within a boot"**.

The seam injection immediately went red — not on safety, which held, but on the applied
prefix. A crash after the commit index was durable but *before* the node emitted its
applied-slot reports lost those reports, and the boot did not replay them, so the prefix
looked like it skipped, failing **"a node's applied prefix advances one slot at a time
(a forward jump only at the compaction floor or a snapshot install)"**
(`crates/paros-sim/src/audit/`). The durable prefix was gap-free the whole time; the
*apply* had simply not been re-driven. The fix mirrors what a real state machine must
do — on boot, re-drive the apply of the durable committed prefix, idempotent because
the commit index *is* the applied index — and the sweep now saturates with seam crashes
on, a stronger guard than replaying the one seed that surfaced it.
