# Single-decree Paxos: the Synod

Single-decree Paxos (Lamport's *Synod*) answers the smallest possible consensus
question: **how does a cluster agree on one value, once, and never disagree?**
Everything else — the replicated log, leaders, reconfiguration — is built on top of
this kernel. paros implements it in `paros-core/src/node.rs`.

The entire algorithm is two round trips. Read this diagram slowly; every term and
every safety rule is annotated where it happens.

```mermaid
sequenceDiagram
    autonumber
    participant P as Proposer (node holding a client value)
    participant A as Acceptors (a majority must reply)

    Note over P,A: TERM ballot b = (round, node-id) — a proposer's numbered "right to<br/>propose". Totally ordered: higher round wins, ties broken by node-id, so<br/>two proposers can never hold the same ballot. paros: types.rs Ballot:46

    rect rgb(235, 245, 255)
    Note over P,A: PHASE 1 — Prepare / Promise ("can I lead under ballot b?")
    P->>A: Prepare(b)  — node.rs propose():133
    Note over A: PROMISE RULE — accept iff b is strictly higher than my promise.<br/>Persist the new promise, then reply. INVARIANT: my promised ballot<br/>never decreases (monotonic). paros: on_prepare():187
    A-->>P: Promise(b, alreadyAccepted?=(ab, av))
    Note over A: A Promise carries back any (ballot, value) this acceptor had<br/>ALREADY accepted — this is how a maybe-chosen value is rescued.
    end

    Note over P: Wait for a QUORUM (majority) of Promises. node.rs try_accept_phase():306
    Note over P: VALUE-SELECTION RULE — if ANY Promise reported an accepted value,<br/>adopt the one with the highest ballot. otherwise propose your own.<br/>This is the rule that protects an already-chosen value. on_promise():246, :316

    rect rgb(235, 255, 240)
    Note over P,A: PHASE 2 — Accept / Accepted ("commit value v at ballot b")
    P->>A: Accept(b, v)  — node.rs try_accept_phase():339
    Note over A: VOTE RULE — accept iff b is still ≥ my promise (nobody outbid you).<br/>Persist (b, v), then reply. INVARIANT: never accept below your promise.<br/>paros: on_accept():216
    A-->>P: Accepted(b, v)
    end

    Note over P: Wait for a QUORUM of Accepted → the value is CHOSEN. try_decide():352
    P->>A: Commit(v)  — tell everyone. learners record it. on_commit():299
    Note over P,A: INVARIANT (the whole point): because any two majorities overlap,<br/>at most one value is ever chosen. SafetyOracle checks this every step.
```

## The acceptor: three fields, two rules

An acceptor is the guardian of safety. Its entire durable state is the
[`HardState`](glossary.md) (`paros-core/src/state.rs`): the highest ballot it has
**promised**, and the `(ballot, value)` it has **accepted**. Its behaviour is two
decisions:

```mermaid
flowchart TD
    start(["A message arrives at an acceptor"]) --> kind{"Which message?"}

    kind -->|"Prepare(b)"| p1{"b &gt; my promised ballot?"}
    p1 -->|"yes"| p2["Raise promised := b<br/>PERSIST it (durable before reply)<br/>node.rs:189"]
    p2 --> p3["Reply Promise(b, my accepted value if any)"]
    p1 -->|"no — someone higher exists"| p4["Reply Nack(my higher ballot)<br/>node.rs:202"]

    kind -->|"Accept(b, v)"| a1{"b ≥ my promised ballot?"}
    a1 -->|"yes"| a2["Record accepted := (b, v)<br/>PERSIST it (durable before reply)<br/>node.rs:218"]
    a2 --> a3["Reply Accepted(b)"]
    a1 -->|"no — I promised someone higher"| a4["Reply Nack(my higher ballot)<br/>node.rs:230"]

    p2 -. "INVARIANT: promised never decreases" .-> inv1([" "])
    a2 -. "INVARIANT: never accept below promised" .-> inv2([" "])
    style inv1 fill:#fee,stroke:#c00
    style inv2 fill:#fee,stroke:#c00
```

> **TERM — promise.** A Promise is a commitment: *"I will not accept anything with
> a ballot lower than `b`."* It does not choose a value; it just locks out stale
> proposers. **TERM — Nack.** A rejection that reports the higher ballot the
> acceptor has already promised, so the loser knows it was outbid. (Stage 2 simply
> abandons the round on a Nack — `on_nack():289`; automatic retry is Stage 3.)

## The proposer: why two phases, and the value-selection rule

Phase 1 is reconnaissance: *grab the ballot and discover any value that might
already be chosen.* Phase 2 is the commit. The subtle, safety-critical step is
**value selection** between them.

```mermaid
flowchart TD
    prop(["Client gives node a value w<br/>propose(w), node.rs:133"]) --> b["Pick ballot b = (max round seen + 1, me)<br/>raise own promise to b, broadcast Prepare(b)"]
    b --> q1{"Promise quorum<br/>reached?"}
    q1 -->|"not yet"| wait1["wait for more Promises"]
    q1 -->|"yes (majority)"| sel{"Did ANY Promise carry<br/>an already-accepted value?"}

    sel -->|"yes"| adopt["Propose the highest-ballot value seen,<br/>NOT your own w.<br/>node.rs try_accept_phase():316"]
    sel -->|"no"| own["Propose your own value w"]

    adopt --> acc["Broadcast Accept(b, v)"]
    own --> acc
    acc --> q2{"Accepted quorum<br/>reached?"}
    q2 -->|"not yet"| wait2["wait for more Accepted"]
    q2 -->|"yes (majority)"| chosen["v is CHOSEN → broadcast Commit(v)<br/>try_decide():352"]

    adopt -. "INVARIANT: a later proposer is FORCED to re-propose a<br/>maybe-chosen value, so the choice can never change" .-> inv([" "])
    style inv fill:#fee,stroke:#c00
    style chosen fill:#0d3,stroke:#0a0
```

### Why value-selection makes Paxos safe (the intuition)

Suppose value `v` was chosen at ballot `b`: a majority `Q` accepted `(b, v)`. Now a
new proposer comes along at a higher ballot `b' > b`. Its Phase-1 quorum `Q'` is
also a majority, so **`Q` and `Q'` overlap** in at least one acceptor `x`. Two
things are true of `x`:

```mermaid
flowchart LR
    x["overlapping acceptor x<br/>(in both quorums)"] --> f1["accepted (b, v)<br/>before promising b'"]
    x --> f2["so x's Promise to b'<br/>reports (b, v)"]
    f2 --> f3["new proposer sees (b, v),<br/>adopts v by the value-selection rule"]
    f3 --> f4["re-proposes v — the choice is preserved<br/>INVARIANT: at most one value ever chosen"]
    style f4 fill:#0d3,stroke:#0a0
```

This is exactly the scenario the test `value_selection_adopts_previously_accepted_value`
(`paros-core/src/node.rs:710`) pins down, and what demo **seed 19** animates in
[How Paxos chooses one value](choose-one-value.md).

## The four safety invariants paros guarantees

These are *unconditional* — they hold under any crashes, drops, delays, and
contention, and the `SafetyOracle` asserts them on every step of every simulated
seed.

```mermaid
flowchart TD
    subgraph inv["The safety invariants (paros-core/src/node.rs)"]
        i1["1. Promised ballot is monotonic<br/>never decreases — test :556"]
        i2["2. Never accept below the promised ballot<br/>test :590"]
        i3["3. A superseded proposer never lowers its own promise<br/>even to self-accept — :329, test :622"]
        i4["4. At most one value is ever chosen<br/>guaranteed by quorum overlap + value-selection — test :710"]
    end
    i1 --> SAFE["No two nodes ever choose different values"]
    i2 --> SAFE
    i3 --> SAFE
    i4 --> SAFE
    style SAFE fill:#0d3,stroke:#0a0
```

Next: watch all of this run live in [How Paxos chooses one value](choose-one-value.md),
then see [what happens when things fail](failures.md).
