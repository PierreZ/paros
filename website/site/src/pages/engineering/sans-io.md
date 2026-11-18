---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
widget: true
title: The sans-IO core
description: paros-consensus is Paxos with no I/O, no clock and no randomness. Inputs go in, one Ready comes out, and the driver does the I/O in a fixed order.
---

## The problem

Paxos is short on paper. In code it mixes with sockets, disks, timers and threads. Then a test
of the protocol needs a network and a disk, and a bug in the protocol looks like a bug in the
I/O.

## The technique

`paros-consensus` holds every Paxos role as a plain state machine. It has no I/O, no clock, no random
numbers and no dependencies. A deployment such as `ColocatedNode` takes three kinds of input:

- `step(message)`: a message from a peer.
- `tick()`: one unit of time. The core counts ticks; it never reads a clock.
- a call from a client: a write, a read, a reconfiguration.

Then `ready()` gives one `Ready`: the work for the driver. The driver must do it in this order:

| # | Bucket | What the driver does |
|---|---|---|
| 1 | `writes()` | persist each write, and sync if one `needs_sync` |
| 2 | `messages()` | send them, only after step 1 is durable |
| 3 | `committed()` | answer the clients whose slots are now chosen |
| 4 | `read_states()` | serve the reads that wait on those slots |
| 5 | `advance()` | release the gate for the next `Ready` |

`Ready` holds the only mutable borrow of the node. A second `ready()` before `advance()` does not
compile. **Persist before send** is the rule that keeps Paxos safe: an acceptor that answers
`Promise` or `Accepted` before its vote is on disk can forget the vote in a crash.

## Be the driver

Below are three `ColocatedNode`s of `paros-consensus`, compiled to wasm and run in your browser. You
are their driver. Give one node one input: a `tick()`, a message from the wire with `step()`, or
a client write with `propose()`. The page then shows each `Ready` that the input gave, bucket by
bucket, in the order the driver does them. Nothing on the page decides anything: it copies the
buckets out of the real `Ready`.

Try this: press **timeout** on node 0, and see its promise in bucket 1, marked SYNC, before the
`Prepare`s in bucket 2. Deliver the messages. Then press **write** on the leader, deliver the
`Accept`s and the `Accepted`s, and see the slot arrive in bucket 3.

<div data-widget="ready"><p class="aside">This demo needs JavaScript and paros-consensus compiled to wasm.</p></div>

## What it gives

- **Fast, exact tests.** The core's tests and model checkers drive it with messages and ticks.
  No sleep, no socket, no flaky timing.
- **One core, many drivers.** The examples, the model checkers and the step-through diagrams
  on this site drive the same core, and so will the journal.
- **Faults from outside.** The core has no `buggify!`. A test perturbs it only through its
  public API: lost, late and repeated messages, crashes between buckets.

## In paros

- [`crates/lib/paros-consensus/src/node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node.rs): `ColocatedNode`, with `step`, `tick` and `ready`.
- [`crates/lib/paros-consensus/src/node/ready.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node/ready.rs): `Ready` and the order of its buckets.
- [`crates/lib/paros-consensus/src/contract/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/contract): what a driver must honor (`WriteOp`, `MustSync`,
  `Storage`).
- [`crates/lib/paros-consensus/examples/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/examples): `single_decree.rs`, `multi_paxos.rs`, `acceptor_grid.rs`
  and more, each a full run with an in-memory driver.
- [`paros-roles/src/node/ready.rs`](https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/lib/paros-roles/src/node/ready.rs) at the tag
  `v0.2.0-alpha.1`: the driver of the old journal service, which did the I/O of each `Ready`.
