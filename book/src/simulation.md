# Deterministic simulation and the safety oracle

How do you trust a consensus implementation? You cannot prove it correct by running
it on a calm network a few times — the bugs live in the unlucky schedules: the
drop, the reorder, the crash at the worst instant. paros's answer is **deterministic
simulation testing** (DST): run the real code against a *simulated* network whose
every event is driven by a seed, and assert the safety invariants on **every step
of every seed**.

```mermaid
flowchart LR
    seed["a u64 seed"] --> run["run_seed(seed)<br/>paros-sim/src/lib.rs:58"]
    run --> sim
    subgraph sim["one deterministic run"]
        direction TB
        net["SimProviders: in-memory network<br/>+ swarm chaos (drop/delay/reorder)"]
        nodes["3 real nodes — the SAME run_node<br/>as production"]
        net <--> nodes
    end
    sim --> trace["a trace of events<br/>(node_state, msg_sent, value_chosen, ...)"]
    trace --> oracle["SafetyOracle reads the trace<br/>and asserts invariants"]
    oracle -->|"violation"| boom["💥 panic — the seed that broke it<br/>is printed and replayable"]
    oracle -->|"all hold"| ok["✔ this seed is safe"]
    run -. "TERM deterministic: the same seed always produces the same run,<br/>byte for byte — in CI and in your browser tab. lib.rs:51" .-> note([" "])
    style boom fill:#fdd,stroke:#c00
    style ok fill:#dfd,stroke:#0a0
```

## The harness, wired once

The cluster, the workload, the chaos, and the oracles are assembled once in
`run_seed` (`paros-sim/src/lib.rs:58`) and reused by the native runner *and* the
wasm demo.

```mermaid
flowchart TD
    builder["SimulationBuilder — lib.rs:61"] --> p["3 NodeProcess-es<br/>(adapt run_node to a moonpool Process)"]
    builder --> w["1 ProposeClient workload<br/>round-robins 12 proposals across nodes<br/>→ deliberate contention"]
    builder --> chaos["enable_chaos(Network::Swarm)<br/>drop, delay, reorder messages"]
    builder --> inv["invariants (oracles):"]
    inv --> o1["SafetyOracle — the safety invariants"]
    inv --> o2["ClientLivenessOracle — client progress"]
    inv --> o3["TimelineRecorder / ProtocolRecorder —<br/>reconstruct the animation"]
    style o1 fill:#eef,stroke:#44a
```

> **TERM — oracle.** An *oracle* is an observer that reads the run's event trace and
> asserts a property. **TERM — invariant.** A property that must hold on every step
> (`assert_always`), as opposed to one that must merely be *possible*
> (`assert_sometimes` / `assert_reachable`, used to prove the test actually exercises
> interesting states). See `paros-sim/src/oracle.rs:11`.

## What the safety oracle checks

The oracle consumes the same `EV_NODE_STATE` / `EV_CHOSEN` events the driver emits
at its durability boundary (`paros/src/driver.rs:176`, `:225`) and turns the
[four safety invariants](single-decree.md) into runtime assertions.

```mermaid
flowchart TD
    subgraph events["trace events from the real driver"]
        e1["node_state (promised ballot,<br/>accepted value) — driver.rs:176"]
        e2["value_chosen (slot, value hash)<br/>driver.rs:225"]
    end
    events --> SafetyOracle
    subgraph SafetyOracle["SafetyOracle — asserts on every step"]
        a1["promised ballot monotonic<br/>(never decreases)"]
        a2["never accepted below promised"]
        a3["at most ONE value chosen per slot<br/>(no two value_chosen disagree)"]
    end
    SafetyOracle -. "INVARIANT: holds across the whole sweep of seeds, under chaos.<br/>A single violation fails the build." .-> inv([" "])
    style inv fill:#fee,stroke:#c00
```

## From one seed to confidence: the sweep

One seed is one schedule. Confidence comes from sweeping many, and from *knowing
when to stop*: `explore()` runs until coverage plateaus and every `sometimes`/
`reachable` marker has fired (`paros-sim/src/lib.rs:89`).

```mermaid
flowchart LR
    s0["seed 0"] --> chk0{"safe?"}
    s1["seed 1"] --> chk1{"safe?"}
    sn["seed N"] --> chkn{"safe?"}
    chk0 --> cov["coverage stable for 64 seeds<br/>AND every reachable marker fired?<br/>lib.rs PLATEAU_SEEDS:45"]
    chk1 --> cov
    chkn --> cov
    cov -->|"yes"| done["✔ stop — capped at 2000 seeds<br/>MAX_ITERATIONS:47"]
    cov -->|"no"| more["keep exploring new schedules"]
    more --> cov
    style done fill:#dfd,stroke:#0a0
```

## The same run, in your browser

Because the core and harness are wasm-safe, `run_seed` compiles to WebAssembly and
runs entirely in a browser tab — the [browser demo](wasm.md) is the *exact* same
simulation CI runs. Append `?dump` to any demo URL to see the raw JSON
`run_seed(seed)` returns; it matches the native runner byte for byte.

```mermaid
flowchart LR
    code["run_seed in paros-sim"] --> native["native runner<br/>cargo run -p paros-sim-runner"]
    code --> wasm["wasm demo<br/>paros-wasm-demo (browser)"]
    native --> same["identical output for the same seed"]
    wasm --> same
    style same fill:#dfd,stroke:#0a0
```

Next: see it run — [the browser demo](wasm.md).
