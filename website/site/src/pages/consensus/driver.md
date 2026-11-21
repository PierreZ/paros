---
layout: ../../layouts/Doc.astro
kicker: Consensus
section: consensus/
widget: true
title: The driver loop
description: paros-consensus does no I/O. You give it an input, it gives one Ready back, and your driver persists, then sends, then learns, in that order.
---

## The problem

Paxos is short on paper. In code it mixes with sockets, disks, timers and threads. Then a test
of the protocol needs a network and a disk, and a bug in the protocol looks like a bug in the
I/O.

## The technique: sans-IO

`paros-consensus` holds every Paxos role as a plain state machine. It has no I/O, no clock, no
random numbers and no dependencies. Your program, the **driver**, owns the sockets, the disk
and the timers. A deployment such as `ColocatedNode` takes three kinds of input:

- `step(message)`: a message from a peer.
- `tick()`: one unit of time. The core counts ticks; it never reads a clock.
- a call from a client: `propose` (a `Write`), `propose_control` (`SetLeader`, `Truncate`),
  `quorum_read`, `reconfigure`.

After each input, `ready()` gives one `Ready`: the work for the driver. The driver does it in
this order:

| # | Bucket | What the driver does |
|---|---|---|
| 1 | `writes()` | persist each `WriteOp` in order, and fsync when `MustSync` says so |
| 2 | `messages()` | send them, only after step 1 is durable |
| 3 | `committed()` | answer the clients whose slots are chosen, with the `Outcome` of each |
| 4 | `read_states()` | serve the reads that wait on those slots |
| 5 | `advance()` | release the batch, then ask for the next `Ready` |

As code, from the README of the crate:

```rust
loop {
    let ready = node.ready();
    if ready.is_empty() {
        ready.advance();
        break;
    }
    disk.apply(ready.writes());                    // 1. persist, and fsync
    for (audience, message) in ready.messages() {  // 2. only then send
        for to in audience.resolve(&pool, me) {
            send(to, message.clone());
        }
    }
    for (slot, _, _, outcome) in ready.committed() {
        answer_the_client(*slot, outcome);         // 3. learn what is chosen
    }
    ready.advance();                               // 4. release the batch
    node.advance_recovery();
}
```

`Ready` holds the only mutable borrow of the node. A second `ready()` before `advance()` does not
compile. Between `advance()` and the end of the flush, the driver steps nothing into the node:
one batch is in flight.

## The rule: persist before send

Step 1 before step 2 is the rule that keeps Paxos safe. A `Promise` or an `Accepted` is a vote.
If it leaves before its write is durable, a crash takes the vote back, and two values can be
chosen. One message may leave early: a batch that raises no promise may send its `Accept`s
before its own flush, because an `Accept` carries a proposal, not a vote (Paxos Made Live,
section 4.2).

Two more facts of the contract surprise a new driver:

- **Leadership is never durable.** A restart is `ColocatedNode::new` on the storage that the
  crash left. The node boots as a follower and campaigns again if it must.
- **Time is the driver's.** The core counts ticks. A driver draws a random election timeout
  (`set_election_timeout`) and calls `tick()`. A test calls `campaign()` and needs no clock.

## Be the driver

Below are three `ColocatedNode`s of `paros-consensus`, compiled to wasm and run in your browser.
You are their driver. Give one node one input: a `tick()`, a message from the wire with
`step()`, or a client write with `propose()`. The page then shows each `Ready` that the input
gave, bucket by bucket, in the order the driver does them. Nothing on the page decides anything:
it copies the buckets out of the real `Ready`.

Try this: press **timeout** on node 0, and see its promise in bucket 1, marked SYNC, before the
`Prepare`s in bucket 2. Deliver the messages. Then press **write** on the leader, deliver the
`Accept`s and the `Accepted`s, and see the slot arrive in bucket 3.

<div data-widget="ready"><p class="aside">This demo needs JavaScript and paros-consensus compiled to wasm.</p></div>

## What it gives

- **Fast, exact tests.** The core's tests and model checkers drive it with messages and ticks.
  No sleep, no socket, no flaky timing.
- **One core, many drivers.** The examples, the simulation, the model checkers and the
  step-through diagrams of the [Paxos pages](../../paxos/) drive the same core.
- **Faults from outside.** The core has no `buggify!`. A test perturbs it only through its
  public API: lost, late and repeated messages, crashes between buckets. See
  [How it is checked](../proof/).

The journal v1 drives its roles with a different contract, the flight rule: each persist batch
has an id, and each send names the batch it waits on, so a driver can pipeline and group commit
([§J4.2](https://github.com/PierreZ/paros/blob/main/docs/journal/04-roles.md#j42-the-journals-sans-io-contract)).

## In paros

- [`node.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node.rs): `ColocatedNode`, with `step`, `tick`, `propose` and `ready`.
- [`node/ready.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/src/node/ready.rs): `Ready`, its buckets and `advance`.
- [`contract/`](https://github.com/PierreZ/paros/tree/main/crates/lib/paros-consensus/src/contract): what a driver honors (`Message`, `WriteOp`, `Storage`, `HardState`, `MemStorage`).
- [`examples/quickstart.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/quickstart.rs): the loop with an election, writes, a crash and a restart. [`examples/common/cluster.rs`](https://github.com/PierreZ/paros/blob/main/crates/lib/paros-consensus/examples/common/cluster.rs): the same loop with a network, crashes and matchmakers.
- The knowledge card [Persist before send](https://github.com/PierreZ/paros/blob/main/docs/knowledge/concepts/persist-before-send.md), with its sources.
