# Persist before send

Group: Kernel. Status: verified 2026-10-31.

An acceptor's answer is a promise about its future: "I will refuse every ballot below `b`" or
"I accepted `v` at `b`". The promise holds across a crash only if the state behind it is on
stable storage **before** the answer leaves. paros makes this order structural: the core hands
the driver one batch, `Ready`, whose writes the driver persists before it sends the batch's
messages. One exception exists, and it is safe: a leader's own `Accept`s may leave before its
own vote is durable (#578 (leader sends Accept before its own fsync)).

## 1. Problem

Paxos safety rests on two durable facts per acceptor ([PMS §2.2]: the promise and the
highest-ballot accepted proposal). [PMS §2.5] states the rule: an acceptor "records its
intended response **before sending it**" in stable storage. If a `Promise` or an
`Accepted` leaves first and the node crashes before the write lands, the node restarts without
the promise or the vote. It can then promise a lower ballot or accept a different value, and two
values can be chosen for one slot.

The rule is easy to state and easy to break in a driver: a message queue and a disk queue are
two queues, and nothing in a plain API orders them.

## 2. Model

- The single-decree model ([PMS §2.1]) with crash and restart.
- An fsync makes a write durable. A write that was not synced can be lost by a crash.
- paros adds storage faults beyond the papers: a failed writeback (fsyncgate, #720) and a full
  disk (#360). See the storage cards (planned).

## 3. Roles and state

| What | Durable? | Where |
|---|---|---|
| The promise (`HardState::max_promised_ballot`) | yes, fsync before any `Promise` | `contract/hard_state.rs` |
| An accepted record (`AcceptorWrite::AppendAccepted`) | yes, fsync before its `Accepted` | `contract/write.rs` |
| A learned record (`WriteOp::Learned`), a truncate, a trim-point jump | yes, fsync | `contract/write.rs` |
| The chosen index (`WriteOp::SetChosenIndex`) | relaxed: no fsync needed | `contract/write.rs` |

The chosen index can be relaxed because it is derived: "the chosen value is already durable from
the accept that preceded it" (the `MustSync` doc). A crash that loses it lets the node learn the
prefix again by catch-up.

## 4. Normal flow

The core never does I/O. Each step of the driver loop asks for one batch and processes its
buckets in a fixed order (the `Ready` doc, `paros-core/src/node/ready.rs`):

1. **Persist** `Ready::writes`, in order. Fsync the batch when `MustSync::for_batch` says
   `Sync`.
2. **Send** `Ready::messages`, only after step 1 is durable.
3. **Learn** `Ready::committed`: ack the clients waiting on those slots.
4. **Answer** `Ready::read_states`, after step 3.
5. Call `Ready::advance`.

`Ready` holds the unique mutable borrow of the node, so a second `ready()` before `advance()`
is a **compile error**. etcd-raft only panics at run time (the `Ready` doc;
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md` §2).

```mermaid
sequenceDiagram
    participant L as Leader
    participant A as Acceptor (core)
    participant D as Acceptor (driver)
    participant S as Disk
    L->>A: Accept(b, 7, w)
    Note over A: admit: b >= promise; record in memory
    A->>D: Ready { writes: [AppendAccepted], messages: [Accepted] }
    D->>S: write + fsync
    S-->>D: durable
    D->>L: Accepted(b, 7)
    D->>A: advance()
```

**The leader exception** (#578, commit `25d00a7`). A leader's `Accept` promises nothing about
the leader's own state: it asks the others to vote. So the driver sends a batch's `Accept`s
**before** its flush, and the rest of the batch after it (`split_early` and `send_early` in
`paros-roles/src/node/ready.rs`). The leader's own vote counts only once it is durable: the
loop steps nothing of the journal until the flush returns. [PML §4.2] describes the same
optimization: the master "writes after sending accept; others write before sending ack". etcd-raft
documents it too, after Raft thesis §10.2.1 ([etcd-raft replication §1]).

`split_early` sends nothing early when the batch raises the promise (`SetPromise`): every
message of such a batch waits for the flush ("only from a batch that does not raise the
promise", the `split_early` doc).

## 5. Recovery flow

- **A crash before the fsync.** No answer left, so no peer counts the lost state. The node
  restarts with its older promise and records. This is safe: an un-sent promise binds nothing.
- **A crash after an early `Accept`, before the leader's own fsync.** The driver names this
  moment with `hint!("accepts sent, own vote not durable")`. The leader's copy is lost, never a
  decision: a slot is chosen only by a quorum of durable votes, and the restarted node
  campaigns under a fresh ballot (the `send_early` comment).
- **A relaxed chosen index lost.** The node boots with a lower chosen index. A follower pulls
  the prefix again on the leader's next beat. A leader advertises the lower value in its beat,
  and a follower that is ahead pushes the prefix back (catch-up card).
- **The boot scan re-asserts the pairs.** What a batch wrote is read back at boot and checked
  against the same rules (`assert_invariants` at boot; the paired assert on `Ready::writes`).

## 6. Why it is safe

The single-decree proof (single-decree card, §6) treats a promise and a vote as facts that stay
true. Persist before send makes that hold across crashes: every answer a peer counts is backed by
a durable write on its sender. A crash can only lose state that no peer counted.

The leader exception does not weaken this. A leader's `Accept` is a request, not a vote. The
leader's own vote is counted in its own tally only after the flush, so a quorum is still a
quorum of durable votes.

## 7. Liveness

The rule costs latency, not liveness. A disk that never syncs blocks the node, the same as a
crash. paros quarantines a journal on a storage fault and re-opens it later
(`DriverTunables::quarantine_ticks`), and a full disk makes the journal read-only, not dead.

## 8. Costs

- One fsync per acceptor per batch on the write path, one per promise on Phase 1.
- Batching amortizes the fsync: one `Ready` can carry many records.
- Before #578, a write paid two flushes in series: the leader's, then the followers'. After it,
  the two run in parallel. On `parosctl bench --local 3` with 5 ms per flush, throughput went
  from 41.5 to 74.8 writes/s and p50 from 23.3 to 11.9 ms (commit `25d00a7`).
- A batch that only moves the chosen index pays no fsync (`MustSync::Relaxed`).

## 9. In paros

| What | Where |
|---|---|
| The batch and its order | `paros-core/src/node/ready.rs`: `Ready`, `writes`, `messages`, `committed`, `read_states`, `advance` |
| The durable scalars and their contract | `paros-core/src/contract/hard_state.rs`: `HardState` |
| The write ops and the fsync rule | `paros-core/src/contract/write.rs`: `WriteOp`, `AcceptorWrite`, `MustSync::for_batch`, `WriteOp::needs_sync` |
| The leader's early send | `paros-roles/src/node/ready.rs`: `split_early`, `send_early` |

The rule is stated in the `Ready` doc (the five steps) and the `HardState` doc (the durability
contract). `docs/analysis/go-raft/etcd-raft-sans-io-patterns.md` §2 compares the shape with
etcd-raft's.

**Asserts in the code:**

- `"a batch persists no promise or record above the promise in memory"` (`Ready::writes`).
- `"a promise or a vote is always fsync'd"` (`MustSync::for_batch`).
- `"no Accept waits behind a flush that keeps the promise"` and `"every Accept of the batch left
  before its flush"` (`paros-roles/src/node/ready.rs`).

**Sim gates and oracles:**

- `"a sent Promise carries a durably promised ballot"` (`paros-sim/src/audit/node.rs`).
- `"an outgoing Accepted names a durably accepted record"` (`paros-sim/src/audit.rs`).
- `"an applied slot was decided by a durable accept quorum before any node applied it"`
  (`audit/node.rs`).
- The crash gate `"the driver crashes with its Accepts out before its own vote is durable"`
  (an inline `reachable!` in `send_early`).

**Divergences from the sources:**

- [PMS §2.5] says nothing about the leader's own vote. paros follows [PML §4.2]: the leader's
  `Accept`s leave first.
- [PML §4.2] counts five naive disk writes per instance. paros writes the proposer's state not
  at all: the proposer is volatile, and its allocator frontier is the acceptor's own log
  (`record_own_round`).
- paros relaxes the chosen index. The papers do not separate the commit index from the votes.

## 10. Pitfalls

- **A storage fault that lies.** A failed writeback can leave the kernel's page clean with the
  bytes not on disk, so a retried fsync "succeeds" (fsyncgate). paros drops the cached pages of a
  store's files at every open, so the open reads what the device holds (#720, commit `f21c0a7`,
  `docs/architecture/05-failure-model-and-zones.md`). Persist before send is only as strong as
  the fsync.
- **Async drivers.** The `Ready` accessors borrow node buffers, so a guard must not be held
  across an `.await`. The driver copies the buckets, calls `advance()`, awaits its I/O, then
  calls `advance_recovery` (the `Ready` doc).
- **An early message that promises.** Only `Accept` may leave early. A `Promise` or an
  `Accepted` in the early half would be a safety bug. `split_early` keeps them late by
  construction: only `Message::Accept` goes into the early half.

## 11. Relatives

- [Single-decree Paxos](single-decree-paxos.md): the two durable facts.
- [Multi-Paxos log](multi-paxos-log.md): one durable write per acceptor per command.
- [Catch-up](catch-up.md): how a lost relaxed chosen index heals.
- Storage cards (planned): the store's own durability, torn tails and quarantine.

## 12. Sources

- [PMS] *Paxos Made Simple*: `docs/references/papers/paxos-made-simple/transcript.md` §2.2 (the
  two facts), §2.5 (stable storage before the response).
- [PML] *Paxos Made Live*: `docs/references/papers/paxos-made-live/transcript.md` §4.2 (five naive
  writes; the master writes after it sends `accept`).
- [etcd-raft replication §1]: `docs/references/etcd-raft/replication.md` §1 (`msgsAfterAppend`,
  `raft.go:546`; `doc.go:79`).
- `docs/analysis/go-raft/etcd-raft-sans-io-patterns.md` §2 (the `Ready`/`Advance` handshake).

[PMS §2.1]: ../../references/papers/paxos-made-simple/transcript.md
[PMS §2.2]: ../../references/papers/paxos-made-simple/transcript.md
[PMS §2.5]: ../../references/papers/paxos-made-simple/transcript.md
[PML §4.2]: ../../references/papers/paxos-made-live/transcript.md
[etcd-raft replication §1]: ../../references/etcd-raft/replication.md
