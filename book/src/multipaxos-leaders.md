# Leaders, election, and liveness

> **Status: planned — not yet in code.** Stage 2 has *no* leader and *no* timing
> logic — it is a pure safety kernel, and that is deliberate. The message variants
> for this already exist but are inert: `CheckLeader` and `Heartbeat` are
> tick-injected self-events that Stage 2 ignores (`paros-core/src/node.rs:125`), and
> `tick()` is "a bare counter, no timeouts until Stage 3" (`node.rs:170`). This
> chapter is the roadmap, drawn from *Paxos Made Moderately Complex*,
> `references/frankenpaxos/02`, and `references/papers/paxos-vs-raft`.

Recall the [FLP split](why-consensus.md): safety is unconditional, but **liveness**
needs a timing assumption. The whole job of leader election is to recover liveness —
to make sure that, eventually, **one** proposer is in charge so the
[dueling-proposer livelock](failures.md) ends and progress resumes. It adds **zero**
new safety requirements.

```mermaid
flowchart LR
    safety["SAFETY<br/>(Stage 2, done)"] --> nochange["leader election changes NOTHING here"]
    liveness["LIVENESS<br/>(Stage 3)"] --> fix["leader election ends the livelock:<br/>one stable proposer → rounds complete"]
    fix -. "INVARIANT preserved: even with a wrong/competing leader, the ballot<br/>rules still allow at most one value per slot. Election only affects WHEN." .-> inv([" "])
    style safety fill:#dfd,stroke:#0a0
    style liveness fill:#ffe,stroke:#cc0
```

## A node's three modes

With leadership, each node moves through three states, driven by a failure detector
(am I hearing heartbeats from a live leader?).

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: election timeout —<br/>no heartbeat heard (CheckLeader fires)
    Candidate --> Leader: won a promise quorum<br/>at my new ballot
    Candidate --> Follower: saw a higher ballot /<br/>heard a live leader
    Leader --> Follower: saw a higher ballot
    Leader --> Leader: send Heartbeats,<br/>serve commands (Phase 2 only)
    note right of Candidate
        TERM candidate = a node running Phase 1
        to try to become leader. It picks a fresh
        higher ballot and Prepares the whole log.
    end note
    note right of Leader
        TERM leader = the node that ran Phase 1 and
        now drives Phase 2 for every command.
        Heartbeats suppress others' election timers.
    end note
```

## The heartbeat / timeout loop

Liveness comes from two timers, both driven by the core's logical `tick()` (no wall
clock — the timing lives in tick counts, exactly as etcd-raft does it).

```mermaid
sequenceDiagram
    autonumber
    participant L as Leader
    participant F as Follower
    loop steady state
        L->>F: Heartbeat (on the leader's tick)
        Note over F: reset my election timer — leader is alive
    end
    Note over L: 💥 leader crashes — heartbeats stop
    Note over F: CheckLeader keeps firing on each tick. election timer expires<br/>(node.rs:125 — inert in Stage 2, active in Stage 3)
    F->>F: become Candidate, pick ballot (round+1, me)
    participant A as Other acceptors
    F->>A: Prepare(b) across all open slots
    A-->>F: Promise(b, accepted entries per slot)
    Note over F: VALUE-SELECTION per slot — re-propose any maybe-chosen value<br/>before proposing anything new. Same rule as single-decree, per slot.
    Note over F,A: F is the new leader. serves commands with Phase 2 only.
```

> **TERM — randomized timeout.** Election timeouts are randomized so two followers
> rarely become candidates at once — the same idea that breaks the dueling-proposer
> livelock. This is a *liveness* tuning knob; pick it wrong and you get slow
> elections, never an unsafe outcome.

## Leader failover across a slot log

When a leader dies, the new leader cannot just start appending — it must first
**recover every open slot**: run Phase 1, see what might already be chosen, and
re-propose those values (filling true gaps with no-ops). Only then is the log
consistent enough to extend.

```mermaid
flowchart TD
    crash(["old leader crashed mid-log"]) --> elect["new candidate wins Phase 1 at ballot b"]
    elect --> scan["for each slot from executed_watermark up:"]
    scan --> d{"did Phase 1 Promises report<br/>an accepted value for this slot?"}
    d -->|"yes"| re["re-propose that value at b (Phase 2)<br/>— it might be chosen, INVARIANT preserved"]
    d -->|"no, but later slots have values"| noop["propose a no-op to fill the gap"]
    d -->|"no, truly open"| free["free to propose new commands here"]
    re --> cont["log is recovered → resume normal service"]
    noop --> cont
    free --> cont
    style cont fill:#dfd,stroke:#0a0
    style re fill:#fee,stroke:#c00
```

### Two leaders during a partition

A partition can briefly produce two would-be leaders. Safety still holds, because
**ballots are totally ordered**: for any given slot, only the higher ballot can win
a quorum, and the value-selection rule ties the outcome back to anything already
chosen.

```mermaid
sequenceDiagram
    autonumber
    participant L1 as Old leader (ballot b1)
    participant A as Acceptors
    participant L2 as New leader (ballot b2 > b1)
    Note over L1,L2: partition heals. both think they lead
    L1->>A: Accept(b1, slot 7, v1)
    A-->>L1: Nack — acceptors already promised b2
    Note over L1: b1 is stale → L1 steps down (saw higher ballot)
    L2->>A: Accept(b2, slot 7, v2-or-recovered)
    A-->>L2: Accepted(b2, 7)
    Note over L1,L2: INVARIANT: at most one value per slot. The higher ballot wins.<br/>a maybe-chosen value is re-proposed, never overwritten. (paxos-vs-raft)
```

## Paxos vs Raft, in one line

This is where Paxos and Raft actually differ. Per
`references/papers/paxos-vs-raft`, the normal-operation log machinery is nearly
identical; **the only real difference is leader election** — how a new leader is
chosen and how it catches its log up. Everything in [Part I](single-decree.md) and
the log machinery above is common ground.

That closes the roadmap. For precise terminology, see the [glossary](glossary.md);
for the source papers behind each chapter, see [further reading](further-reading.md).
