# From one value to a log

> **Status: planned — not yet in code.** Stage 2 is single-decree: it chooses one
> value, in slot `0`. This chapter sketches **Multi-Paxos**, the Stage 3+ roadmap,
> drawn from *Paxos Made Moderately Complex* and
> `docs/references/frankenpaxos/03-multipaxos-core.md`. The hooks already exist in
> the code — `Slot`, the per-slot `accepted` map, and `chosen_index` in
> `HardState` — but only slot 0 is used today.

A single chosen value is not very useful. A real system wants a **replicated log**:
an ever-growing, agreed-upon sequence of commands that every node applies in the
same order, turning a cluster into a fault-tolerant state machine.

```mermaid
flowchart LR
    subgraph idea["Multi-Paxos = one independent Paxos instance per log slot"]
        direction LR
        s0["slot 0<br/>set x=1"] --> s1["slot 1<br/>set y=7"] --> s2["slot 2<br/>del x"] --> s3["slot 3<br/>..."]
    end
    idea --> sm["apply in slot order →<br/>every node's state machine stays identical"]
    idea -. "TERM slot: one index in the replicated log. Each slot runs the SAME<br/>single-decree algorithm you already know. types.rs Slot:13" .-> note([" "])
    style sm fill:#dfd,stroke:#0a0
```

## The log: three regions

A node's log is **sparse** — slots can be chosen out of order — and is best read as
three regions divided by two watermarks.

```mermaid
flowchart LR
    subgraph log["one node's replicated log"]
        direction LR
        c0["0 ✓"] --- c1["1 ✓"] --- c2["2 ✓"] --- m3["3 ?"] --- m4["4 ✓"] --- o5["5 ·"] --- o6["6 ·"]
    end
    c0 -.- A["REGION A — chosen & applied<br/>[0, executed_watermark)"]
    m3 -.- B["REGION B — maybe-chosen / in-flight<br/>note the GAP at slot 3"]
    o5 -.- C["REGION C — open, no value yet"]
    B -. "TERM watermark: a boundary index. executed_watermark = highest slot<br/>applied so far; commit_index = highest contiguous chosen slot." .-> note([" "])
    style A fill:#dfd,stroke:#0a0
    style B fill:#ffe,stroke:#cc0
    style C fill:#eee,stroke:#999
```

## In-order execution and gap-filling

The state machine must apply commands **in order**, with no holes. Slot 4 may be
chosen while slot 3 is still a gap — so execution stops at the first hole, and the
leader fills stubborn gaps with a **no-op** so the log can drain.

```mermaid
flowchart TD
    start(["try to execute the log"]) --> at{"is slot at<br/>executed_watermark chosen?"}
    at -->|"yes"| apply["apply its command to the state machine<br/>advance executed_watermark"]
    apply --> at
    at -->|"no — a gap"| gap{"has it been a gap<br/>for too long?"}
    gap -->|"no"| wait["wait — it may still be chosen"]
    gap -->|"yes"| noop["leader proposes a NO-OP for this slot<br/>(Phase 1 learns nothing was there)<br/>→ gap fills, execution continues"]
    noop --> at
    at -. "INVARIANT: commands apply in slot order on every node →<br/>identical state machines (R1–R4 in Paxos Made Moderately Complex)" .-> inv([" "])
    style apply fill:#dfd,stroke:#0a0
    style inv fill:#fee,stroke:#c00
```

## The key optimization: Phase 1 once, Phase 2 per slot

Running full two-phase Paxos for every command would be wasteful. The Multi-Paxos
insight: a **stable leader runs Phase 1 just once** (for *all* slots, present and
future), then only needs Phase 2 per command. This is what makes Paxos fast in the
steady state.

```mermaid
sequenceDiagram
    autonumber
    participant L as Leader
    participant A as Acceptors
    rect rgb(235, 245, 255)
    Note over L,A: Phase 1 — ONCE, covering all slots ≥ next
    L->>A: Prepare(b)  — for the whole log, not one slot
    A-->>L: Promise(b, any accepted entries)
    end
    rect rgb(235, 255, 240)
    Note over L,A: Phase 2 — per command, no more Phase 1 needed
    L->>A: Accept(b, slot=5, cmd1)
    A-->>L: Accepted(b, 5)
    L->>A: Accept(b, slot=6, cmd2)
    A-->>L: Accepted(b, 6)
    end
    Note over L,A: TERM in steady state a command costs ONE round trip.<br/>frankenpaxos calls the Phase-1 runner a "scout", the Phase-2<br/>runner a "commander". (references/frankenpaxos/02, 03)
```

## How it maps onto today's code

The single-decree kernel is the per-slot engine; Multi-Paxos is mostly bookkeeping
*around* it. The data model is already slot-shaped:

```mermaid
flowchart LR
    subgraph today["already in paros-core (used for slot 0 only)"]
        t1["Slot(u64) — types.rs:13"]
        t2["HardState.accepted: Map&lt;Slot, (Ballot, Value)&gt; — state.rs:29"]
        t3["HardState.chosen_index: Slot — state.rs:32"]
        t4["messages carry a slot field — message.rs"]
    end
    subgraph todo["Stage 3+ adds"]
        d1["propose to the lowest free slot<br/>(not always slot 0)"]
        d2["execute loop + watermarks"]
        d3["gap-fill no-ops"]
        d4["a stable leader (next chapter)"]
    end
    today --> todo
    style today fill:#dfd,stroke:#0a0
    style todo fill:#ffe,stroke:#cc0
```

Next: who gets to be that stable leader, and what happens when it dies —
[leaders, election, and liveness](multipaxos-leaders.md).
