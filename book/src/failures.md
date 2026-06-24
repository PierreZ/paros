# Failures and failover

Paxos is only interesting because things break. This chapter is a gallery: each
diagram walks **one failure** and shows **why safety still holds** (and, where
relevant, how progress resumes). Every scenario here is reproducible in the
[browser demo](choose-one-value.md) and is exercised under swarm network chaos by
the `SafetyOracle` across thousands of seeds (`paros-sim/src/oracle.rs`,
`run_seed`/`explore` in `paros-sim/src/lib.rs`).

The golden rule to keep in mind throughout:

> **Any two majorities overlap.** Whatever crashes or gets dropped, the next
> proposer's Phase-1 quorum shares an acceptor with the quorum that may have chosen
> a value — so the value-selection rule always rediscovers it.

## 1. A message is dropped, delayed, or reordered

The network is unreliable by assumption. A lost `Promise` or `Accept` simply
stalls the in-flight round — **nothing is mis-chosen**.

```mermaid
sequenceDiagram
    autonumber
    participant P as Proposer (node 0)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    P->>A1: Prepare(b)
    P-xA2: Prepare(b) — DROPPED by the network
    A1-->>P: Promise(b)
    Note over P: only 1 of the 2 needed Promises arrived → no quorum → round stalls
    Note over P,A2: INVARIANT: a stalled round chooses NOTHING. No value is lost,<br/>no value is wrongly chosen. Stage 2 does not retransmit (on_nack:289).<br/>progress resumes when a fresh proposal starts a higher ballot.
    Note over P: Later: a client re-proposes (here, or at another node).<br/>propose() picks round = max-seen + 1, superseding the stuck round. node.rs:133
    P->>A1: Prepare(b2)  — b2 > b
    P->>A2: Prepare(b2)
    A1-->>P: Promise(b2)
    A2-->>P: Promise(b2)
    Note over P: quorum on b2 → Phase 2 proceeds normally
```

> Reordering is handled the same way: each handler checks the ballot against
> `HardState`, so a late or out-of-order message that is no longer current is
> simply Nacked or ignored (`on_promise`/`on_accepted` guard on
> `p.ballot != ballot`).

## 2. An acceptor crashes (a minority)

A 3-node cluster tolerates **one** failure: the surviving two still form a
majority.

```mermaid
flowchart TB
    subgraph cluster["3 acceptors — quorum = 2"]
        n0((0 ✔))
        n1((1 ✔))
        n2(("2 ✘<br/>crashed"))
    end
    n0 --- Q["Surviving quorum {0, 1}<br/>still a majority → consensus continues"]
    n1 --- Q
    Q -. "INVARIANT: any quorum of {0,1} overlaps any quorum that ran before<br/>the crash — a chosen value is never lost. quorum() = peers/2 + 1, node.rs:377" .-> inv([" "])
    style n2 fill:#fdd,stroke:#c00
    style Q fill:#0d3,stroke:#0a0
```

If a **second** acceptor crashes, the cluster cannot form a majority and **halts**
— a *liveness* loss, never a *safety* loss. It resumes the moment one returns.

## 3. A proposer crashes mid–Phase 1

Before a value is ever sent in an `Accept`, a proposer crash costs nothing: no
value was in play.

```mermaid
sequenceDiagram
    autonumber
    participant P as Proposer (node 0)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    P->>A1: Prepare(b)
    P->>A2: Prepare(b)
    A1-->>P: Promise(b)
    Note over P: 💥 node 0 crashes before sending any Accept
    Note over P,A2: Its volatile "Proposing" round vanishes (never persisted, node.rs:21).<br/>The acceptors only raised their promise to b — no value accepted.
    participant P2 as New proposer (node 2)
    P2->>A1: Prepare(b2)  — b2 > b
    P2->>A2: Prepare(b2)
    A1-->>P2: Promise(b2, accepted = none)
    A2-->>P2: Promise(b2, accepted = none)
    Note over P2: no accepted value reported → free to propose its OWN value. node.rs:316
```

## 4. A proposer crashes mid–Phase 2 (the subtle one)

This is the case the whole value-selection machinery exists for. A proposer sends
some `Accept`s, a value becomes **partially accepted** (maybe even chosen), and
then the proposer dies before anyone learns the outcome.

```mermaid
sequenceDiagram
    autonumber
    participant P as Proposer (node 0)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    P->>A1: Accept(b, v)
    A1-->>A1: persist accepted = (b, v) — on_accept:218
    Note over P,A1: v is now possibly chosen (A1 + maybe others voted).<br/>💥 node 0 crashes. no Commit was ever sent. Nobody has LEARNED v.
    participant P2 as New proposer (node 2)
    P2->>A1: Prepare(b2)  — b2 > b
    P2->>A2: Prepare(b2)
    A1-->>P2: Promise(b2, accepted = (b, v))
    A2-->>P2: Promise(b2, accepted = none)
    Note over P2: VALUE-SELECTION RULE — a Promise reported (b, v), so P2 MUST<br/>re-propose v, not its own value. node.rs on_promise:246, try_accept_phase:316
    P2->>A1: Accept(b2, v)
    P2->>A2: Accept(b2, v)
    A1-->>P2: Accepted(b2, v)
    A2-->>P2: Accepted(b2, v)
    Note over P2,A2: v is chosen — the SAME v the dead proposer may have chosen.<br/>INVARIANT: a maybe-chosen value can never be overwritten.
```

This is precisely `value_selection_adopts_previously_accepted_value`
(`paros-core/src/node.rs:710`) and demo **seed 19**.

## 5. Dueling proposers — a livelock (but never unsafe)

Two proposers can keep leapfrogging each other's ballots, each invalidating the
other's Phase 1, so nothing is ever chosen. This is a **liveness** failure.

```mermaid
sequenceDiagram
    autonumber
    participant P0 as Proposer 0
    participant A as Acceptors
    participant P1 as Proposer 1
    P0->>A: Prepare(1,0)
    A-->>P0: Promise(1,0)
    P1->>A: Prepare(2,1)  — outbids P0
    A-->>P1: Promise(2,1)
    P0->>A: Accept(1,0, v0)
    A-->>P0: Nack — acceptors promised (2,1) now. on_accept:230
    Note over P0,P1: P0's round is abandoned (no retry in Stage 2, on_nack:289).
    P1->>A: Accept(2,1, v1)
    P0->>A: Prepare(3,0)  — outbids P1
    A-->>P1: Nack
    Note over P0,P1: ...and around it goes. INVARIANT: still nothing wrongly chosen.<br/>Progress stalls, safety does not bend.
```

> **The cure is timing, not safety.** Demo **seed 42** shows this livelock.
> Randomized election timeouts (Stage 3) make one proposer back off and let the
> other win — a *liveness* fix. The Stage 2 safety kernel has **no timing logic at
> all** and is still always safe. This is the FLP split made concrete (see
> [Why consensus?](why-consensus.md)).

## 6. A network partition

Split the cluster and only the side with a **majority** can make progress.

```mermaid
flowchart LR
    subgraph minority["Minority side — {0}"]
        m0((0))
    end
    subgraph majority["Majority side — {1, 2}"]
        m1((1))
        m2((2))
    end
    m0 -. "✂ partition" .- m1
    m0 -.->|"cannot reach a quorum →<br/>makes NO progress"| stuck["stalls (liveness only)"]
    m1 -->|"quorum {1,2} →<br/>chooses values normally"| go["progresses"]
    m2 --> go
    go -. "on heal: the minority's Prepare/Promise discovers the chosen value<br/>via value-selection and catches up. INVARIANT: one value cluster-wide." .-> inv([" "])
    style go fill:#0d3,stroke:#0a0
    style stuck fill:#fdd,stroke:#c00
```

When the partition heals, the minority node rejoins, and any new round it
participates in surfaces the already-accepted value — so it can only ever converge
on the one chosen value. The [multi-Paxos chapter](multipaxos-leaders.md) revisits
this as **leader failover** across a partition, where a new leader must re-run
Phase 1 across every open log slot.

## Summary: failure → what saves you

| Failure | Safety preserved by | Progress |
|---|---|---|
| Message dropped/delayed/reordered | ballot checks in every handler | resumes on a fresh higher ballot |
| Acceptor crash (minority) | quorum overlap | continues on the majority |
| Proposer crash, Phase 1 | nothing accepted yet | new proposer free to choose |
| Proposer crash, Phase 2 | **value-selection rule** | new proposer re-proposes the same value |
| Dueling proposers | monotonic promise + Nack | stalls; cured by Stage 3 timeouts |
| Network partition | only a majority can choose | majority proceeds; minority catches up on heal |
