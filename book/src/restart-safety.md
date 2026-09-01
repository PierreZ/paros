# Crash and restart safety

Paxos assumes nodes crash and come back. That is the whole point: a majority keeps
serving while a minority is down, and a recovered node rejoins. But "come back"
hides a subtle trap. A node's promises and votes are only safe if they **survive**
the crash exactly as they were. Get the durable state slightly wrong and a
restart can quietly un-make a decision the cluster already reached. paros hit this
edge, and the simulation caught it.

<!-- toc -->

## What must be durable, and when

Three things must reach stable storage, and the *order* in which they do is part
of the protocol: the promised ballot, the per-slot accepted value, and the commit
index. paros splits them the way etcd-raft splits `HardState` from `entries` —
two small scalars persisted whole, and the log persisted one record at a time:

```rust
pub struct HardState {          // the two scalars, persisted whole
    pub max_promised_ballot: Ballot,
    pub chosen_index: Option<Slot>,
}
// The accepted log — Slot -> (Ballot, Entry) — is persisted per record, not as a
// blob. Each `Ready` batch surfaces the minimal deltas instead of a whole-state
// clone:
enum WriteOp {
    SetPromise(Ballot),                                   // raise the promise
    AppendAccepted { slot: Slot, ballot: Ballot, entry: Entry }, // one accept
    SetChosenIndex(Slot),                                 // advance the commit index
}
```

The rule is **persist before send**. An acceptor must write a raised promise to
disk before it replies `Promise`, and write a new accepted value to disk before it
replies `Accepted`. A promise-raise or accepted-append is `MustSync::Sync` (fsync
before the reply leaves); a commit-index-only advance may use a relaxed write,
because a chosen value is already durable from the accept that preceded it. The
state lives in the code with the reason attached:

> Sending either reply before the corresponding field is durable violates Paxos
> safety: a crash could "un-promise" or "un-accept", letting two different values
> be chosen for one slot.

[Paxos Made Live](https://15799.courses.cs.cmu.edu/fall2013/static/papers/paxos_made_live.pdf)
states the same hazard from the other side:

> A corrupted disk losing persistent state lets a replica renege on past promises,
> violating a Paxos assumption.

paros enforces the ordering with a type. The `Ready` handshake hands the driver
four steps that must run in sequence, and the borrow checker makes a second batch
a **compile** error until the first is acknowledged:

```mermaid
flowchart TD
    step["step(msg) or tick():<br/>run protocol logic, fill the pending buckets"]
    r1["Step 1 — persist the batch's writes<br/>(set-promise / append-accepted / set-chosen-index), then fsync"]
    r2["Step 2 — send messages to peers"]
    r3["Step 3 — apply committed entries"]
    r4["Step 4 — advance(): release the gate"]
    step --> r1 --> r2 --> r3 --> r4
    r1 -. "a Promise or Accepted sent before its HardState<br/>is durable is a safety violation" .-> r2
```

## The bug: a restart that resurrects a dead value

Persist-before-send is necessary but not sufficient. There is a second, subtler
requirement: the durable state must not just be *written in order*, it must stay
*consistent with what was chosen*. Here is the interleaving that broke paros
(commit `608bb58`):

```mermaid
sequenceDiagram
    autonumber
    participant N as Node N0
    participant D as Durable storage
    Note over N,D: X = Entry("C7",7,"SET q=0"), Y = v5 = Entry("C7",1,"SET x=1")
    Note over N,D: Step 1 — accept a value at a low ballot that never gets chosen
    N->>D: accepted[5] = ((1,0), X)
    Note over N,D: Step 2 — learn the cluster chose a DIFFERENT value
    N->>N: Commit: slot 5 is ((4,2), Y)
    Note over N: chosen[5] = Y, in memory
    rect rgba(200, 70, 70, 0.25)
    Note over N,D: BUG: durable accepted[5] still holds the stale ((1,0), X)
    end
    Note over N,D: Step 3 — crash, then rebuild chosen from accepted on restart
    D->>N: accepted[5] = ((1,0), X)
    rect rgba(200, 70, 70, 0.25)
    Note over N: chosen[5] rebuilds to X, contradicting the cluster's choice of Y
    end
```

The node learned that slot 5 was chosen as `Y`, but it still had an old, never
chosen `X` sitting in its durable `accepted` map from a failed earlier ballot. In
memory that did not matter, because the volatile `chosen` map held `Y`. But on
restart, `RawNode::new` rebuilds the volatile state from the durable `accepted`
map (`node.rs`), and there it found `X`. The node came back believing slot 5 was
`X`. Two nodes, two different values for one slot: the exact thing
[Why one value is safe](safety.md) promised could never happen.

## The fix, and why one word matters

The repair is a single word. When a value is chosen, `mark_chosen` records it as
the **authoritative** accepted entry, overwriting any stale one:

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

`record_accepted` overwrites the slot's entry and queues a
`WriteOp::AppendAccepted` at the choosing ballot; `or_insert_with` would have left
the stale `X` in place. With the overwrite, the durable accepted log and the
chosen value can never disagree, so the restart rebuilds the right answer:

```mermaid
sequenceDiagram
    autonumber
    participant N as Node N0
    participant D as Durable storage
    Note over N,D: X = Entry("C7",7,"SET q=0"), Y = v5 = Entry("C7",1,"SET x=1")
    N->>D: accepted[5] = ((1,0), X)
    N->>N: Commit: slot 5 is ((4,2), Y)
    rect rgba(70, 170, 110, 0.25)
    N->>D: mark_chosen OVERWRITES accepted[5] := ((4,2), Y)
    end
    Note over N,D: crash, then rebuild on restart
    D->>N: accepted[5] = ((4,2), Y)
    rect rgba(70, 170, 110, 0.25)
    Note over N: chosen[5] rebuilds to Y, the value the cluster chose
    end
```

## Proven, not asserted

The reason this chapter exists is *how* the bug was found. paros is built
simulation-first: a suspected safety problem is not patched on a hunch, it is
turned into a **failing simulation** first. The harness already had the
`SafetyOracle` watching that "at most one value is ever chosen for a slot." To
reach the bug it needed crash and restart, so the sweep injects
`Chaos::Attrition` (a node crashes and recovers, with `prob_wipe = 0.0` so durable
state survives, exactly modelling a clean restart). Under that chaos the oracle
went red on real seeds. The one-word fix turned it green, and the sweep ran clean
across thousands of seeds. A regression test,
`chosen_value_survives_restart_over_a_stale_accept`, pins it so it can never come
back.

This is the loop the project lives by: make the violation reproducible, watch it
fail, fix the core, watch it pass. A safety bug the simulation cannot reproduce is
treated as unproven. This one was very real, and the simulation is why we know the
fix works.

## Crashing *inside* a batch: the persist/send seam

`Chaos::Attrition` crashes a node at *process* granularity. But the driver drains
each `Ready` batch synchronously — persist every write, fsync, *then* send the
messages — with no `.await` in between, so attrition can only ever crash a node
**between** batches, never at the seam *within* one. Yet the seam is exactly where
durability is subtle: what happens if a node dies after it fsyncs an accept but
before the `Accepted` reply leaves the wire? Or before the fsync, with the batch
half-written?

To reach those points the harness uses `buggify!()` — deterministic fault
injection that is activated per seed and then fires probabilistically, so only
some seeds exercise a seam crash, and always reproducibly. Two seams matter:

```mermaid
flowchart TD
    stage["stage the batch's writes<br/>(promise / accepts / commit index)"]
    s1{{"seam: crash before fsync"}}
    sync["fsync"]
    s2{{"seam: crash after fsync,<br/>before send"}}
    send["send messages"]
    stage --> s1 --> sync --> s2 --> send
    s1 -. "whole un-synced batch lost;<br/>no message was sent → clean 'never happened'" .-> lost1["recover: batch gone"]:::gap
    s2 -. "writes durable;<br/>batch's messages dropped" .-> lost2["recover: peers re-driven"]:::done
    classDef gap fill:#7a2f2f,stroke:#4d1f1f,color:#fff
    classDef done fill:#3b6e47,stroke:#244730,color:#fff
```

The simulation makes this a *real* crash: a seam crash unwinds the node loop, the
volatile state is dropped, and the node re-runs — rebuilding from the durable
storage world, exactly as a fresh process would. A recovery oracle checks that a
restart never lowers a promised ballot and never changes a pre-crash accepted
`(slot -> value)`.

The seam injection immediately went red — not on safety, which held, but on the
**no-gaps** oracle. A crash *after* the commit index was durable but *before* the
node emitted its "applied slot N" events lost those events, and the boot did not
replay them, so the applied prefix looked like it skipped. The durable prefix was
gap-free the whole time; the *apply* had simply not been re-driven. The fix mirrors
what a real state machine must do: on boot, re-drive the apply of the durable
committed prefix (it is idempotent — the commit index *is* the applied index). With
that, the sweep runs clean and saturates with seam crashes on: the boot replay
is exercised on every run that crashes at a seam, which is a stronger guard than
replaying the one seed that first surfaced the gap.
