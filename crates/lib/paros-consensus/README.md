# paros-consensus

The Paxos roles of paros as sans-IO state machines: no I/O, no clock, no randomness, and std
only. A caller, the **driver**, owns the sockets, the disk and the timers. The core computes
what to persist and what to send, and the driver does it. The same code runs in a test, in a
simulation, in a browser (wasm32) and behind a real network.

paros is a learning project. It is not for production. Releases go to crates.io as
`paros-consensus`, and through the `paros` facade as `paros::consensus`.

## What is in the crate

- **`ColocatedNode`**: one node that is proposer, acceptor and replica. It runs Multi-Paxos with
  a fixed membership, or Matchmaker Paxos when the configuration names matchmakers.
- **`ProxyLeader`** and **`ReplicaNode`**: the two other deployments of Compartmentalized
  Paxos. A proxy leader runs Phase 2 for the leader. A replica learns and serves reads, and
  never votes.
- **`Matchmaker`** and **`MatchmakerReconfigurer`**: the registry of Matchmaker Paxos and the
  handover of the matchmaker set.
- **The roles** that the deployments wire: `proposer::Proposer`, `acceptor::Acceptor`,
  `replica::Replica`, `quorum_read::QuorumReads`, `decree::Decree` and others. Each one is
  public, so a program can wire its own deployment. `decree::Decree` is single-decree Paxos
  for a one-shot decision (`examples/single_decree.rs`).
- **The contract** of a driver, in `contract`: `Message`, `WriteOp`, `Storage`, `Config`,
  `Limits`, and `MemStorage`, an in-memory disk for tests and examples.

## The driver loop

Give the node an input (`step` for a peer message, `tick` for time, `propose` or
`propose_control` for a client call). Then drain its batch:

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

Step 1 before step 2 is the rule that makes Paxos safe. A `Promise` or an `Accepted` that
leaves before its write is durable is a vote that a crash takes back. The rustdoc of the crate
root has this loop as a complete program with three nodes, and `examples/common/cluster.rs` has
it with a network, crashes, matchmakers, proxy leaders and replicas.

Three things surprise a new user:

- **The log is a journal.** A `Write` is accepted only from the client that a `SetLeader`
  named, and only at the next position. The core judges nothing at `propose`: each node judges
  a command when its slot is applied, and the verdict is the `Outcome` in `ready.committed()`.
  A write on a journal with no writer is chosen by Paxos and refused by the journal.
- **Leadership is never durable.** A restart is `ColocatedNode::new` on the storage that the
  crash left, and the node boots as a follower.
- **Time is the driver's.** The core counts ticks. A driver draws a random election timeout
  (`set_election_timeout`) and calls `tick`. A test or an example calls `campaign` and needs no
  clock.

## Examples

Each example prints a trace and asserts the property that it shows:

    nix develop --command cargo run -p paros-consensus --example quickstart

Read them in this order. The first is single-decree Paxos on `decree::Decree`. Each other
example drives the deployments as a driver does, on the toy cluster of
`examples/common/cluster.rs`: one disk for each process, persist before send, and a network
that can lose or hold a message.

| Example | Shows |
|---|---|
| `single_decree` | `Decree` and `Acceptor`: Phase 1, P2c, Phase 2 for one value, and a restart |
| `quickstart` | the driver loop, an election, writes, a crash and a restart |
| `multi_paxos` | one Phase 1 for many slots, and the recovery of a new leader: P2c and the `Noop` gap fill |
| `journal_calls` | `SetLeader`, `Write`, a retry, a read, `Truncate`, and each `Outcome` |
| `leader_handoff` | `relinquish_to`: a new leader at the same ballot, with no election |
| `acceptor_grid` | a grid of acceptors: one column decides, one acceptor of each column elects, `move_round` |
| `quorum_read` | a linearizable read on a grid with no leader and no clock |
| `proxy_leader` | proxy leaders that run Phase 2 for the leader |
| `replica_tier` | replicas that learn and serve reads beside the acceptors |
| `matchmaker` | the registry of the matchmakers, and the handover of the matchmaker set |
| `reconfiguration` | `reconfigure` with three matchmakers, garbage collection, a retirement |

## Features

No feature changes a decision of the core.

| Feature | Default | Adds |
|---|---|---|
| `tracing` | on | `#[tracing::instrument]` spans on the entry points and the message handlers |
| `assertions` | on | coverage probes for a simulation (`moonpool-assertions`) |
| `serde` | off | `Serialize` and `Deserialize` on the public protocol types |

With `default-features = false` the crate has no dependency.

## Where to read next

- The rustdoc: `nix develop --command cargo doc -p paros-consensus --no-deps --open`.
- `AGENTS.md` in this folder: the map of the modules, and the rules for a change.
- `specs/tla/`: the TLA+ model of each Paxos flavor that the crate builds.
- `docs/knowledge/`: one card for each Paxos concept that paros uses.
