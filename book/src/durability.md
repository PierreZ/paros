# Persist-before-send durability

Paxos safety rests on a promise being *kept*. If an acceptor tells a proposer
"I promise ballot `b`" or "I accepted `(b, v)`", that fact must survive a crash —
otherwise a reboot could un-promise or un-accept, and two different values could be
chosen for the same slot. The rule that prevents this is **persist-before-send**.

> **INVARIANT — persist-before-send.** Never send a message that depends on durable
> state until that state is actually on stable storage. A `Promise` or `Accepted`
> published before its `HardState` is durable is a safety violation.
> (`paros-core/src/state.rs:9`, `paros-core/src/ready.rs:18`.)

## What must be durable: `HardState`

The core separates the *must-be-durable* state from everything volatile. Only three
things have to hit disk; everything else (the in-flight proposer round, learned-set
caches) is rebuilt after a crash.

```mermaid
classDiagram
    class HardState {
        +Ballot max_promised_ballot
        +BTreeMap accepted
        +Slot chosen_index
    }
    note for HardState "MUST be durable - state.rs:23.<br/>max_promised_ballot: highest ballot promised, monotonic.<br/>accepted: maps each Slot to the Ballot+Value voted there.<br/>chosen_index: highest contiguous chosen slot.<br/>BTreeMap, never HashMap, for deterministic iteration."
    class VolatileState {
        +Proposing in_flight_round
        +Set chosen_cache
        +u64 tick_count
    }
    note for VolatileState "NEVER persisted - node.rs:21, :75.<br/>A crash simply abandons the in-flight round:<br/>safe, because nothing was promised on its behalf."
```

## The ordering the driver must follow

When the driver drains a `Ready`, it must process the buckets in a fixed order.
This is the heart of `drain_ready` in `paros/src/driver.rs:163`.

```mermaid
flowchart TD
    r["ready() → a Ready batch"] --> s1
    subgraph order["STRICT ORDER — drain_ready, driver.rs:163"]
        s1["1 · PERSIST hard_state()<br/>write to stable storage FIRST<br/>driver.rs:176"]
        s2["2 · SEND messages()<br/>only AFTER step 1 is durable<br/>driver.rs:203"]
        s3["3 · APPLY committed()<br/>chosen + durable → hand to the app<br/>driver.rs:225"]
        s4["4 · advance()<br/>release the gate<br/>driver.rs:235"]
        s1 --> s2 --> s3 --> s4
    end
    s1 -. "INVARIANT: a reply predicated on a promise/accept is on the wire<br/>ONLY after that promise/accept is durable. A crash here loses<br/>nothing that any peer was told to rely on." .-> inv([" "])
    style inv fill:#fee,stroke:#c00
    style s1 fill:#ffe,stroke:#cc0
```

### What a crash looks like at each point

```mermaid
sequenceDiagram
    autonumber
    participant App as Driver + Storage
    participant Net as Peers
    Note over App: acceptor raised promise to b, ready() surfaces new HardState
    App->>App: 1 · persist HardState(promised = b)
    Note over App,Net: 💥 crash BEFORE the reply is sent?<br/>Safe: promise is durable, the peer just never heard back<br/>(it will retry at a higher ballot). No safety loss.
    App->>Net: 2 · send Promise(b)
    Note over App,Net: 💥 crash AFTER persist, AFTER send?<br/>Safe: on restart, RawNode::new reads HardState back and the<br/>promise still holds (node.rs:86). The reply was always backed by disk.
    Note over App,Net: The ONLY unsafe order — send before persist — is impossible:<br/>Ready hands messages out only after hard_state, and the driver<br/>writes storage first. ready.rs:18
```

## Crash recovery is just "read it back"

There is no special recovery path. A node boots — fresh or after a crash — by
reading its durable state and resuming. Bootstrap and restart share one code path.

```mermaid
flowchart LR
    boot(["node starts / restarts"]) --> read["RawNode::new(storage)<br/>storage.initial_state() → (HardState, Config)<br/>node.rs:86"]
    read --> resume["resume: promised ballot and accepted<br/>values are exactly as last persisted"]
    resume -. "the in-flight proposer round is gone (volatile) —<br/>a stalled round, never a safety problem" .-> note([" "])
```

> **Observability tie-in.** Each time the driver persists `HardState`, it emits an
> `EV_NODE_STATE` tracing event (`driver.rs:176`). The `SafetyOracle` reads that
> stream to check *monotonic promise* and *never-accept-below-promise* on every
> step — durability and safety-checking share the same boundary. See
> [deterministic simulation](simulation.md).

Next: how one driver runs this same core in [production and simulation](one-driver.md).
