# The sans-IO contract

Most consensus bugs are not in the algorithm — they are in the *plumbing* around
it: a reply sent before its state was on disk, a timer that fired at the wrong
moment, a test that passes because it never reproduces the real schedule. paros's
answer is **sans-IO**: the protocol lives in a pure state machine that does **no
I/O, has no clock, and no randomness**. It only transforms inputs into outputs.

```mermaid
flowchart LR
    subgraph core["paros-core::RawNode — the sans-IO state machine (node.rs:55)"]
        direction TB
        IN["INPUTS"] --> SM["pure logic:<br/>O(state, event) → new state + buffered output"]
        SM --> OUT["OUTPUTS"]
    end
    step["step(msg) — a peer message<br/>node.rs:102"] --> IN
    propose["propose(value) — a client value<br/>node.rs:133"] --> IN
    tick["tick() — one unit of logical time<br/>node.rs:170"] --> IN
    OUT --> ready["ready() → a Ready batch:<br/>persist · send · apply<br/>node.rs:178"]
    core -. "TERM sans-IO: the core NEVER touches a socket, disk, or clock.<br/>A driver does all of that. So the same core runs in prod and in sim." .-> note([" "])
    style core fill:#eef,stroke:#44a
```

## Three inputs, one output

Everything that can happen to a node enters through three methods, and everything
the node wants done leaves through one:

```mermaid
flowchart TD
    subgraph inputs["INPUTS — how the world reaches the core"]
        i1["step(Message)<br/>a peer's Prepare/Promise/Accept/...<br/>node.rs:102"]
        i2["propose(Value)<br/>a client wants a value chosen<br/>node.rs:133"]
        i3["tick()<br/>logical time advances one step<br/>node.rs:170"]
    end
    inputs --> node["RawNode mutates its in-memory state<br/>and fills three pending buckets:<br/>pending_hard_state · pending_messages · pending_committed"]
    node --> out["ready() hands back a Ready borrow<br/>node.rs:178"]
    subgraph output["OUTPUT — one Ready batch (ready.rs:36)"]
        o1["hard_state() — durable state to persist"]
        o2["messages() — (dest, msg) to send"]
        o3["committed() — (slot, value) to apply"]
    end
    out --> output
```

> **TERM — `Ready`.** A `Ready` is one batch of side effects the driver must
> perform. **TERM — `advance()`.** Calling `advance()` acknowledges the batch,
> clears the buckets, and unlocks the next `ready()`.

## The ready / advance handshake — a compile-time gate

The trick that makes this safe: `ready()` returns a value that **holds the node's
unique `&mut` borrow**. You cannot call `ready()` again until you consume the first
one with `advance()`. A second `ready()` before `advance()` is a **compile error**
— not a runtime panic. (etcd-raft, the inspiration, only panics at runtime;
`paros-core/src/ready.rs:9` documents the difference.)

```mermaid
sequenceDiagram
    autonumber
    participant D as Driver
    participant N as RawNode
    D->>N: step(msg) / propose(v) / tick()
    Note over N: mutate state, fill pending buckets
    D->>N: let ready = node.ready()
    Note over D,N: `ready` borrows the node uniquely → a 2nd ready() here<br/>is a COMPILE ERROR (borrow checker), not a runtime bug. ready.rs:9
    N-->>D: Ready { hard_state, messages, committed }
    Note over D: process the batch in durability order (next chapter)
    D->>N: ready.advance()
    Note over N: clear buckets, release the borrow
    D->>N: node.ready()  ← now allowed again
```

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Dirty: step / propose / tick<br/>(buckets filled)
    Dirty --> Borrowed: ready()
    Borrowed --> Borrowed: ready() again<br/>❌ COMPILE ERROR
    Borrowed --> Idle: advance()<br/>(buckets cleared, borrow released)
    note right of Borrowed
        Exactly one Ready in flight.
        The type system enforces it.
    end note
```

## Why this shape

```mermaid
flowchart LR
    pure["Pure core:<br/>no I/O, no clock, no RNG"] --> b1["Deterministic — same inputs,<br/>same outputs, every time"]
    pure --> b2["Testable — drive it with a<br/>simulator, replay any seed"]
    pure --> b3["Portable — compiles to wasm<br/>(runs in your browser tab)"]
    pure --> b4["Reusable — ONE driver runs it<br/>in production and in simulation"]
    b1 --> dst["This is what makes deterministic<br/>simulation testing possible"]
    b2 --> dst
    style pure fill:#eef,stroke:#44a
```

The model is etcd-raft's `RawNode`/`Node` split, studied in
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`. `RawNode` is the sans-IO
object here; the next chapters cover the durability rule the driver must honour,
and the single driver that wraps the core for both production and simulation.

Next: [persist-before-send](durability.md) — the one ordering rule that keeps
Paxos safe across crashes.
