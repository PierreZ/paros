# Agent A: `paros-core` and driver surface

Read-only audit against the repository at `0545f84`.

## Core API and recovery

`RawNode::new` reconstructs consensus state from `Storage::{initial_state,
accepted,first_slot,last_slot}` (`crates/paros-core/src/node.rs:273`).  It restores
the retained accepted log, chosen prefix, in-flight/applied client-sequence maps,
and `next_slot`; elections, reads, and leadership are volatile.

The public state-machine surface is:

- `step(Message)` (`node.rs:355`), the single peer/self-message router.
- `propose(ClientId, ClientSeq, Value)` (`node.rs:411`) and
  `propose_control(Control)` (`node.rs:434`).
- `read_index(ctx)` (`node.rs:454`).
- `compact(up_to)` (`node.rs:487`), which clamps to the contiguous chosen prefix.
- `tick()` (`node.rs:523`), `resend_pending()` (`node.rs:560`),
  `has_pending_accepts()` (`node.rs:602`), and `step_down()` (`node.rs:610`).
- `set_election_timeout`/`needs_election_timeout` (`node.rs:637`, `1516`).
- `ready()` (`node.rs:645`) and `chosen_gap()` (`node.rs:1533`).

`Ready`, not `RawNode`, owns `advance(self)`.  Its unique borrow makes a second
`ready()` a compile error.  It exposes ordered `writes`, `must_sync`, addressed
`messages`, contiguous `committed` commands, `snapshot_offers`, and confirmed
`read_states` (`crates/paros-core/src/ready.rs:9-114`).

`HardState` is `{ max_promised_ballot, chosen_index }`
(`crates/paros-core/src/state.rs:5`).  Accepted records remain per-slot.
`WriteOp` is `SetPromise`, `AppendAccepted`, `SetChosenIndex`, `Truncate`, or
`InstallSnapshot` (`crates/paros-core/src/write.rs:14`).  Promise, accept,
truncate, and snapshot-install writes require sync; chosen-index-only batches
may be relaxed.

`Command` is `User(Entry { client, seq, value })` or
`Control::{Truncate { up_to }, Noop}` (`crates/paros-core/src/types.rs:32`).
Consensus treats it opaquely; the contiguous apply walk interprets control
commands.

`Message` is non-exhaustive and includes `Prepare`, `Promise`, `Accept`,
`Accepted`, `Nack`, `Commit`, `CatchUpRequest`, `CatchUpResponse`,
`InstallSnapshot`, `CheckLeader`, `Heartbeat`, and `HeartbeatAck`
(`crates/paros-core/src/message.rs:10`).  Below-floor catch-up produces a
snapshot offer; the driver attaches application bytes.  Snapshot receipt raises
the promise, jumps the chosen prefix/floor, queues `WriteOp::InstallSnapshot`,
and does not replay folded entries (`node.rs:1159`).

Core constants are a one-tick heartbeat, 64-entry catch-up batches, and a
20-tick read-round TTL (`node.rs:13-27`).

## Provider-generic driver

`run_node<P: Providers, S: NodeStorage, H: DriverHooks>` is the one production
and simulation driver (`crates/paros/src/driver.rs:1009`).  It binds the gRPC
listener, constructs `RawNode`, starts bounded per-peer regular and snapshot
delivery lanes, then selects over listener accepts, proposal/read/compact RPCs,
peer delivery, a tick sleep, and shutdown.

Every core-mutating arm calls `drain_ready` then `maintain`.  `drain_ready`
copies the guarded batch, advances the borrow gate, persists and syncs writes,
materializes snapshot offers, optionally crashes after sync, sends messages,
then traces/applies committed commands and answers proposal/read waiters
(`driver.rs:554-683`).  `persist_writes` owns the before-sync seam and emits
only post-sync persistence facts (`driver.rs:695-817`).

Policy/timing decisions are:

- election timeout `[5,10)` ticks, optionally the shortest value;
- retry pending `Accept`s on every tick or deliberately skip;
- voluntarily resign a leader;
- expire parked reads after more than ten ticks;
- drop proposal waiters without a reply when leadership is lost;
- lossy `try_send` into bounded peer mailboxes and discard old backlog at the
  64-message batch threshold.

The tick arm creates a new `sleep(50 ms)` on every loop.  Other winning arms
drop that sleep, so sustained traffic can defer ticks; this is an idle-period
timer rather than a retained interval.

`DriverHooks` has four decisions: `crash_at(Seam)`,
`skip_accept_resend`, `resign_leadership`, and
`shortest_election_timeout`; `NoHooks` is inert
(`crates/paros/src/hooks.rs:24-56`).  The independent seams are before fsync and
after fsync/before send.

Hard-coded driver values (`driver.rs:38-109`, `grpc.rs:47-68`) include: 50 ms
tick, 2 s/1 s h2 keepalive interval/timeout, 1 s delivery timeout, regular and
snapshot mailbox capacities 4096/4, 3 MiB and 64-message delivery caps, read
retry threshold 10 ticks, election base 5 ticks, and RPC inbox capacities
256/256/256/1024.

## Storage and the missing application seam

`NodeStorage` adds fallible semantic consensus writes, sync, truncation, and
opaque `snapshot`/`install_snapshot` to the core recovery port
(`crates/paros/src/storage.rs:33-107`).  `MemStorage` retains only `HardState`,
accepted records, configuration, and compaction floor.  Its snapshot is a
chosen-index marker and installed snapshot bytes are discarded
(`storage.rs:109-211`).

There is no application apply callback.  A committed command currently flows
only through:

```text
Ready::committed -> driver::drain_ready -> tracing -> proposal waiter
```

The Chain-of-Blocks state must therefore be added through an honest
provider-generic application seam (most naturally `NodeStorage::apply` plus
inspectable application state), not a simulation-only protocol path.  Snapshot
install must update that same application state.

## Client outcome contract

`Propose { client, seq, command }` returns `ProposeAck { seq, leader,
committed, slot }` (`crates/paros/proto/paros.proto:13-24`).  A committed ack is
definitive and names an applied slot.  An explicit non-leader ack means that
node did not admit the call.  Missing replies, transport errors, deadlines,
seam crashes, and leader loss are ambiguous: the request may still commit.
Reconciliation must retry the identical `(client, seq)` and/or inspect applied
state.

Dedup is not permanent exactly-once across compaction/snapshot restart because
opaque snapshots cannot rebuild compacted client-sequence history.  A chain
workload must not assert at-most-once per command unless it explicitly persists
the required idempotency model.

