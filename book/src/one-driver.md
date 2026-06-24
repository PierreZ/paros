# One driver, production and simulation

The sans-IO core does no I/O — so something has to. That something is the
**driver**: `paros::run_node` (`paros/src/driver.rs:254`). The load-bearing design
decision is that there is **exactly one** driver, written generically over
moonpool's `P: Providers`, so the *same* code runs in production and in
deterministic simulation. You test the code you ship.

```mermaid
flowchart TD
    core["paros-core::RawNode<br/>(pure state machine — no I/O)"]
    driver["paros::run_node&lt;P: Providers, S: NodeStorage&gt;<br/>the ONE driver — owns the RawNode, does all I/O<br/>driver.rs:254"]
    core --> driver
    driver --> prod
    driver --> sim
    subgraph prod["PRODUCTION"]
        tp["TokioProviders<br/>real sockets, real clock"]
        pd["future parosd binary"]
    end
    subgraph sim["SIMULATION"]
        sp["SimProviders<br/>in-memory network, logical clock"]
        ph["paros-sim Process adapter<br/>+ chaos + oracles"]
    end
    driver -. "TERM provider-generic: the driver names no concrete runtime. P is the<br/>only thing that differs between prod and sim. Same protocol code path." .-> note([" "])
    style driver fill:#eef,stroke:#44a
```

## What the driver does

It is a single `select` loop over four event sources, each feeding the core and
then draining the resulting `Ready` in [persist→send→apply→advance](durability.md)
order.

```mermaid
flowchart TD
    subgraph loop["run_node select loop — driver.rs:283"]
        direction TB
        ev{"which event fired?"}
        ev -->|"client Propose RPC"| c1["node.propose(value)<br/>driver.rs:289"]
        ev -->|"peer deliver RPC"| c2["node.step(msg)<br/>driver.rs:312"]
        ev -->|"tick timer (every 50ms)"| c3["node.tick()<br/>driver.rs:316"]
        ev -->|"shutdown token"| stop["return Ok(())"]
        c1 --> drain["drain_ready(): persist → send → apply → advance<br/>driver.rs:163"]
        c2 --> drain
        c3 --> drain
        drain --> ev
    end
```

> **TERM — Providers.** moonpool's abstraction over time, networking, and task
> spawning. `TimeProvider::sleep` is a real timer in production and a logical-clock
> wait in simulation — the driver calls the same method either way
> (`driver.rs:316`).

## The node RPC contract

Nodes talk to clients and to each other over one small `#[service]` interface
(`paros/src/driver.rs:81`). The same `paros_core::Message` is sent and received —
no separate wire DTO.

```mermaid
sequenceDiagram
    autonumber
    participant Cl as Client
    participant N0 as Node 0 (driver + core)
    participant N1 as Node 1
    Cl->>N0: propose(Propose { seq, command })
    Note over N0: node.propose(Value(command)). drain_ready
    N0-->>Cl: ProposeAck { seq }
    Note over Cl,N0: TERM ProposeAck = "accepted for processing", NOT yet chosen —<br/>consensus runs in the background. driver.rs:71, :285
    N0->>N1: deliver(Prepare/Accept/Commit/...)
    Note over N1: node.step(msg). drain_ready → may reply via its own deliver()
    N1-->>N0: deliver(Promise/Accepted/Nack)
```

## Why "test the code you ship" matters

```mermaid
flowchart LR
    subgraph wrong["The trap most systems fall into"]
        w1["protocol logic in a<br/>sim-only test harness"] --> w2["production path is<br/>DIFFERENT code"] --> w3["bugs the sim can never find"]
    end
    subgraph right["paros"]
        r1["protocol logic in the<br/>provider-generic driver"] --> r2["sim and prod run the<br/>SAME run_node"] --> r3["a bug found in sim is a<br/>bug fixed in production"]
    end
    style wrong fill:#fdd,stroke:#c00
    style right fill:#dfd,stroke:#0a0
```

This rule is why later protocol stages (leader election, the replicated log) must
land in `run_node` and the core — never in a sim-only path. The simulation harness
only adds the *environment* (a chaotic network) and *observers* (oracles), covered
next.

Next: [deterministic simulation and the safety oracle](simulation.md).
