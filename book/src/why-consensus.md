# Why consensus?

A distributed system is a set of machines that must **agree** — on who holds a
lock, on the next command to apply, on a single chosen value — even though
machines crash and the network drops, delays, and reorders messages. Paxos is the
algorithm that lets a cluster agree **safely** under exactly those conditions.

The whole problem, and the one rule that solves it, in one picture:

```mermaid
flowchart TD
    C["Client wants a value chosen"] --> P
    subgraph cluster["A cluster of nodes — each plays every role"]
        direction LR
        P["Proposer<br/>TERM: proposes a value, drives the rounds"]
        A["Acceptors (a majority must agree)<br/>TERM: vote on proposals and remember their votes"]
        L["Learner<br/>TERM: finds out which value was chosen"]
        P -->|"asks a majority to agree"| A
        A -->|"once a majority agrees, the value is chosen"| L
    end
    A -. "INVARIANT (safety): at most one value is ever chosen,<br/>no matter what crashes or the network does" .-> SAFE([" "])

    style SAFE fill:#0d3,stroke:#0a0
```

Every paros node plays **all three roles at once** (see
`paros-core/src/node.rs`): it is a proposer when a client hands it a value, an
acceptor when a peer asks it to vote, and a learner when it hears a value was
chosen.

## The model: what can go wrong

Paxos assumes a deliberately harsh world. Naming the failures up front makes the
algorithm's choices obvious later.

```mermaid
flowchart LR
    subgraph allowed["Allowed to happen — Paxos tolerates all of these"]
        direction TB
        m1["Messages dropped"]
        m2["Messages delayed<br/>arbitrarily long"]
        m3["Messages reordered"]
        m4["Nodes crash and<br/>restart (losing volatile state)"]
        m5["Several proposers<br/>compete at once"]
    end
    subgraph forbidden["Assumed NOT to happen"]
        direction TB
        b1["Messages corrupted/forged<br/>(no Byzantine faults)"]
        b2["Durable state lost or<br/>silently corrupted<br/>(a later-stage topic)"]
    end
    allowed --> SAFE["Safety still holds:<br/>never two different values chosen"]
    forbidden -.->|"outside the model"| SAFE
```

> **TERM — quorum.** A *quorum* is any **majority** of the nodes (2 of 3, 3 of 5).
> The single most important fact in all of Paxos: **any two majorities overlap in
> at least one node.** That one shared node is what makes disagreement impossible —
> it cannot vote for two conflicting things.

```mermaid
flowchart TB
    subgraph N["3 acceptors"]
        n0((0))
        n1((1))
        n2((2))
    end
    QA["Quorum A = {0, 1}"] --- n0
    QA --- n1
    QB["Quorum B = {1, 2}"] --- n1
    QB --- n2
    n1 -. "shared by BOTH quorums →<br/>the overlap that guarantees safety" .-> OV([" "])
    style OV fill:#0d3,stroke:#0a0
```

## The two properties

Consensus is judged on two properties, and Paxos treats them very differently.

```mermaid
flowchart TD
    subgraph safety["SAFETY — never violated, ever"]
        s1["At most one value is chosen"]
        s2["A node only learns a value<br/>that was actually chosen"]
    end
    subgraph liveness["LIVENESS — eventually makes progress"]
        l1["Some proposed value is<br/>eventually chosen"]
        l2["...once the network is calm<br/>and a single leader emerges"]
    end
    safety -->|"unconditional"| ALWAYS["Holds under ANY crashes,<br/>delays, drops, and contention"]
    liveness -->|"conditional"| FLP["Cannot be guaranteed in an<br/>asynchronous network (the FLP result):<br/>with no timing assumptions, a perfectly<br/>unlucky schedule can stall forever"]
```

> **The FLP impossibility, in one line.** In a purely asynchronous network you
> *cannot* guarantee both safety and liveness with even one possible crash. Paxos's
> answer: **never compromise safety**, and recover liveness in practice with
> timeouts and leader election. paros makes this split concrete — the safety kernel
> (Stage 2) has *zero* timing logic and is still always safe; the
> [dueling-proposer livelock](failures.md) is a *liveness* gap, cured later by
> randomized timeouts, never needed for safety.

## Where this lands in paros

| Concept | In the code |
|---|---|
| Node plays all three roles | `paros-core/src/node.rs` (`RawNode`) |
| Quorum = majority | `paros-core/src/node.rs` `quorum()` — `peers.len() / 2 + 1` |
| The one safety property | asserted every step by the `SafetyOracle`, `paros-sim/src/oracle.rs` |

Next: how a single value actually gets chosen — the
[single-decree Synod algorithm](single-decree.md).
