# Truncation and snapshot restore

A log that only grows fills the disk, so a real system must **truncate**: it
deletes the prefix that the cluster has already applied. Deletion makes a second
problem. A node that was down comes back and needs slots that no disk still holds.
Catch-up replays a value that a peer still holds, and it cannot replay a value
that every node deleted. That node is **stranded**. This chapter states the two
halves of the answer: how the cluster decides to truncate, and how it repairs the
node that truncation strands.

> **Play it.** Two Act III levels, one half each.
>
> - [`act3/truncate-by-consensus`](play/#act3/truncate-by-consensus) — ask the
>   leader to compact, take its refusal, retry, and watch each node move its floor
>   as it applies the decided slot.
> - [`act3/the-stranded-node`](play/#act3/the-stranded-node) — strand a node below
>   the floor, pull the snapshot that repairs it, and state the promise it holds
>   after the install.

<!-- toc -->

## Bytes stay opaque

paros does not read a value: it orders and replicates bytes that it does not
understand, and the application turns a chosen slot into state. So paros cannot
compact the state, because it does not know what the state is. What paros owns is
its **log**, and both mechanisms below drop a prefix of it with the bytes sealed.

## Truncation is a decision, not a side-channel

Each node can prune its own log whenever it wants to. That method fails as soon as
two nodes disagree about how far they pruned. A `Prepare` lands on a peer that
deleted the exact slot in the question, and the peer reports "nothing accepted
here" when the truth is "I no longer know". Two values can then be chosen for one
slot. So paros makes the floor a **decided value**.

A decided slot holds a `Command` (`crates/paros-core/src/types.rs`), which is
either a `User(Entry)` with the client's opaque bytes or one of paros's own
`Control` commands: `Truncate{up_to}`, the `Snap{at_index}` marker below, and the
`Noop` gap filler. The acceptors and the replication path do not tell the two
variants apart, exactly as Compartmentalized Paxos treats a `Noop`. Only the
**apply** step reads a control command. One decision therefore gives one
cluster-wide floor, forwarded by ordinary replication. A node that is behind
truncates later, when it applies that slot, and the prefix it drops always sits
inside its own chosen prefix.

One coupling rule holds the two halves together. Past the floor the entries are
gone everywhere, so only a snapshot repairs a node that was away. A snapshot that
nobody holds repairs nobody. The leader therefore proposes a `Truncate` only after
a quorum reports custody of a decided snapshot point that covers it. It refuses an
early request with `accepted: false`, and seeds a `Control::Snap` marker instead.

## The node below the floor, and the snapshot that repairs it

A node crashes with its disk intact while the cluster keeps deciding and
truncating past its position. Its peers refuse a `CatchUpRequest` below their own
floor, and they are right to refuse it: a peer that answered about a truncated
range would tell the same lie the [floor guards](stable-leader.md) exist to
prevent. So the one piece of state transfer that paros performs answers instead. A
peer that sees such a request offers a **snapshot**: the opaque application state
at its own chosen prefix, which the application produced through
`NodeStorage::snapshot()` and paros ships without reading a byte. The core records
only who needs one and up to which slot, and the driver, which owns storage,
attaches the bytes.

One line in that path carries the safety: the receiver adopts
`max(promise, ballot)`, and its durable promise **must not** go down. A snapshot
restores the log and says nothing about promises, because the peer that sent it
does not know what this node has sworn. A node that lost the promise itself is a
harder case with a different answer; see
[the wiped node](beyond-multi-paxos.md#the-wiped-node).

## Where this lives in paros

| Protocol name | Symbol |
|---|---|
| The truncation command | `Control::Truncate` (`types.rs`) |
| The decided snapshot point | `Control::Snap` (`types.rs`) |
| The client's request | the `Compact` RPC, `ColocatedNode::propose_control` |
| The local prefix drop | `ColocatedNode::compact`, `WriteOp::Truncate` |
| The offer the core records | `serve_catchup`, `Ready::snapshot_offers` |
| The transfer | `Message::InstallSnapshot`, `NodeStorage::snapshot()` |
| The install | `ColocatedNode::on_install_snapshot`, `Acceptor::install` |

## Proven, not asserted

Truncation makes the below-floor node *reachable*, and reaching it is how these
rules were proven. The sweep widens the attrition recovery window, so a crashed
node stays down long enough for the cluster to truncate past it. The convergence
claim — **"every node converges to the cluster's chosen prefix at the end of the
settle tail"** — used to exempt a below-floor node as unrecoverable. It now
demands that this node converges like any other. **"A below-floor node recovers
via snapshot transfer"** proves that the recovery path fires, and **"a node's
promised ballot never decreases"** watches the promise
(`crates/paros-sim/src/audit/`).

That sweep found two bugs before a human did. The simulation's storage fake
overwrote the promised ballot on an install instead of taking the maximum, so a
snapshot that carried a lower ballot lowered the promise. And a node can install
**two** snapshots in one batch when two peers both serve it. The applied-prefix
check must therefore track the set of landings, not the last one.

In an ordinary run a peer that truncated less aggressively often heals the
below-floor node by catch-up. Snapshot transfer becomes load-bearing once no such
peer is left.
