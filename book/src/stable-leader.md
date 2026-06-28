# The stable leader

A log of independent Paxos instances is correct, but naive. If every slot ran its
own Phase 1 and Phase 2, every command would cost two round trips, and competing
proposers would collide on every slot. Multi-Paxos becomes efficient by
electing **one stable leader** that runs Phase 1 a single time and then streams
nothing but Phase 2 for as long as it stays up. This chapter is about that
optimization and the liveness machinery that keeps a single leader in charge.

<!-- toc -->

## Phase 1 once, Phase 2 forever

The key observation: Phase 1 does not mention a value. A Prepare only claims a
ballot and asks what has been accepted. So a proposer can claim a ballot for the
**entire rest of the log** in one message. Lamport describes a new leader doing
exactly this:

> It runs phase 1 for instances 135 to 137 and all instances `> 139` using one
> proposal number, a single short message.

[Paxos Made Live](https://15799.courses.cs.cmu.edu/fall2013/static/papers/paxos_made_live.pdf)
(Google's Chubby experience) names the steady-state win:

> if the coordinator doesn't change between instances, propose messages can be
> omitted. Pick a long-lived coordinator, the **master**.

paros implements this literally. Its `Prepare` carries a `from_slot`, and a single
Prepare covers every slot at or after it:

```rust
Prepare {
    from: NodeId,
    ballot: Ballot,
    from_slot: Slot,   // covers every slot >= from_slot
}
```

The matching `Promise` reports **all** entries the acceptor has accepted in that
suffix, so one exchange tells the new leader everything in flight across the whole
log.

## Becoming leader, and filling the gaps

A node that times out waiting for a leader becomes a `Candidate`, bumps its
ballot, and sends that one Prepare (`on_check_leader` in `node.rs`). When a
majority of Promises arrive it becomes `Leader` (`try_become_leader`). But before
it may stream new commands, it has a duty: the previous leader may have left slots
half-decided, and the value-selection rule (P2c, from
[Why one value is safe](safety.md)) says those must be re-proposed at the new
ballot, not overwritten. The Promises piggybacked exactly the values needed; paros
collects them in the `Election.recovered` map and re-proposes each one through
`start_accept_round` before opening fresh slots.

```mermaid
sequenceDiagram
    autonumber
    participant C as Candidate N2, ballot (4,2)
    participant A0 as Acceptor N0
    participant A1 as Acceptor N1
    Note over C,A1: v5 = Entry("C7",1,"SET x=1")
    Note over C,A1: Phase 1, once, for the whole log suffix
    C->>A0: Prepare(ballot=(4,2), from_slot=5)
    C->>A1: Prepare(ballot=(4,2), from_slot=5)
    A0-->>C: Promise(ballot=(4,2), accepted_suffix={5: ((2,1), v5)})
    A1-->>C: Promise(ballot=(4,2), accepted_suffix={})
    Note over A0,A1: mpb (2,1)->(4,2)
    Note over C: a majority promised, so N2 is now Leader.<br/>Slot 5 came back accepted at (2,1): recover it first<br/>(re-propose v5 at (4,2)) before streaming new slots.
    Note over C,A1: Phase 2, streamed per slot, no more Prepare
    C->>A0: Accept(ballot=(4,2), slot=5, entry=v5)
    C->>A1: Accept(ballot=(4,2), slot=5, entry=v5)
    A0-->>C: Accepted(ballot=(4,2), slot=5)
    A1-->>C: Accepted(ballot=(4,2), slot=5)
    Note over A0,A1: accepted[5] := ((4,2), v5)
    Note over C: quorum on slot 5, chosen.<br/>chosen_index 4->5, Commit broadcast
```

This is Paxos Made Moderately Complex's scout-then-commander pattern, with the scout
(Phase 1) and commander (Phase 2) folded into the node's own `Candidate` and
`Leader` roles.

## Steady state: two messages per command

Once a node is the stable leader, a client command is cheap. There is no Phase 1:
the leader assigns the next free slot and goes straight to Accept.

```mermaid
sequenceDiagram
    autonumber
    participant Cl as Client C7
    participant L as Leader N2, ballot (4,2)
    participant A as Acceptors N0, N1 (a majority)
    Note over Cl,A: v6 = Entry("C7",2,"SET y=2")
    Cl->>L: Propose(v6)
    Note over L: assign the next free slot, 6, no Phase 1 needed
    L->>A: Accept(ballot=(4,2), slot=6, entry=v6)
    A-->>L: Accepted(ballot=(4,2), slot=6)
    Note over L: a quorum accepted, so slot 6 is chosen.<br/>accepted[6] := ((4,2), v6)<br/>chosen_index 5->6
    L-->>Cl: ProposeAck(slot 6 committed)
```

One round trip to a majority per command. Lamport notes this is not just fast but
**optimal**: "Phase 2 of Paxos has been shown to have the minimum possible cost of
any fault-tolerant agreement algorithm."

## Holding the lead, and surviving its loss

A leader keeps its position by heartbeating. paros's `Heartbeat` carries the
leader's ballot and its commit index. Receiving it resets a follower's election
clock, and the leader uses its own heartbeat tick to **resend any un-acked
Accepts** so a lagging follower catches up:

```rust
Heartbeat {
    from: NodeId,
    ballot: Ballot,
    commit: Slot,   // the leader's highest contiguous chosen slot
}
```

The commit index on each beat is itself a piggyback: it is attached to a message
the leader already sends, so followers advance their chosen prefix at no extra
cost, and a
follower that fell behind relearns a value it missed when the leader resends the
`Accept`, or when the next election piggybacks it on a `Promise`. That is paros's
catch-up today; there is no dedicated state-transfer RPC yet.

When the leader dies, its heartbeats stop, a follower's election timeout fires,
and the cycle repeats with a higher ballot. The whole life of a node is three
roles:

```mermaid
stateDiagram-v2
    direction TB
    [*] --> Follower
    Follower --> Candidate: election timeout fires,<br/>bump ballot to (4,2), Prepare(from_slot=5)
    Candidate --> Leader: won a promise quorum at (4,2)
    Candidate --> Follower: saw a higher ballot (5,0), Nack
    Leader --> Follower: saw a higher ballot (5,0)
    Leader --> Leader: heartbeat tick,<br/>resend un-acked Accepts
    Follower --> Follower: heartbeat or Accept from leader,<br/>reset election clock
```

## Liveness: curing the duel without touching safety

The previous part showed that two proposers can livelock, each preempting the
other forever. Safety never bends during a duel, but progress stalls. The cure is
to stop two nodes from campaigning at the same time, and it lives in the driver,
not the safety core.

Two pieces do it. First, a rejected leader does **not** immediately retry. When an
`Accept` is nacked, paros steps the node down to `Follower` and waits
(`on_nack` / `become_follower`); the in-code comment says plainly that "we do not
immediately re-prepare: that, with the randomized timeout, is the dueling-proposer
livelock fix." Second, the election timeout is **randomized**: the driver draws a
fresh jittered timeout (`draw_election_timeout`) so two followers rarely time out
together. With high probability one node campaigns first, wins its promise quorum,
and the rest fall back to following it. (Classic single-decree Paxos cures the same
duel with exponential backoff on the proposer; paros's randomized election timeout
is the Multi-Paxos equivalent of that idea.)

This is exactly the separation Lamport insists on: leader election is needed only
for **progress**, never for safety. By the FLP result, no purely asynchronous
algorithm can guarantee a leader is elected, which is why the cure uses real time
(timeouts) and randomness. The `LeadershipOracle` and `ProgressOracle` in
`paros-sim` watch that the cluster does in fact make progress (a stable leader
streams several slots, and leadership turns over and recovers) without ever
violating the `SafetyOracle`.

## Optimizations at a glance

Multi-Paxos in practice is the bare protocol plus a set of optimizations. Most of
them are about doing less work in the steady state, and several are just careful
uses of piggybacking. Here is the list, and where paros stands:

| Optimization | What it buys | In paros |
|---|---|---|
| Stable leader (master) | run Phase 1 once, then one round trip per command | yes, the `Leader` role |
| Phase-1 batching | a single `Prepare` claims the whole log suffix | yes, via `from_slot` |
| Piggybacking | ride data on messages already in flight (accepted values on `Promise`, commit index on `Heartbeat`) | yes |
| Pipelining | propose slot `i+1` before slot `i` is chosen | yes, the leader streams `Accept`s |
| Randomized backoff | jittered election timeout plus step-down on `Nack`, to break the proposer duel | yes, `draw_election_timeout` |
| Catch-up | a lagging node relearns missed values by resend and piggyback | partial: heartbeat resend and election recovery, no snapshot transfer yet |
| No-op gap fill | fill a hole with a no-op so the log can advance past a dead leader | not yet: paros re-proposes recovered in-flight slots instead |
| Command batching | pack many client commands into one slot | not yet |
| Leader leases | serve linearizable reads locally for a lease period | not yet |
| Snapshots and truncation | snapshot the state, discard the applied log prefix | not yet |

The "not yet" rows are the roadmap past this part: they are what turns a correct
log into a system you can run for months without the disk filling up.

## The whole flow, end to end

Putting the pieces together, here is the whole protocol in two pictures. First the
**election**: Node 2 times out, claims ballot `(4,2)` for the whole log suffix
(`from_slot=5`), and on a promise quorum becomes Leader. Slot 5 came back accepted
at the old ballot `(2,1)`, so before opening fresh slots the new leader
**recovers** it, re-proposing `v5` at `(4,2)` and watching `chosen_index` step
from `4` to `5`:

```mermaid
sequenceDiagram
    autonumber
    participant Cl as Client C7
    participant N0 as Node 0
    participant N1 as Node 1
    participant N2 as Node 2

    Note over N0,N2: v5 = Entry("C7",1,"SET x=1")
    Note over N0,N2: Election, Phase 1, run once for the whole log suffix
    Note over N2: no leader heard from, election timeout fires:<br/>become Candidate, bump ballot to (4,2)
    N2->>N0: Prepare(ballot=(4,2), from_slot=5)
    N2->>N1: Prepare(ballot=(4,2), from_slot=5)
    N0-->>N2: Promise(ballot=(4,2), accepted_suffix={5: ((2,1), v5)})
    N1-->>N2: Promise(ballot=(4,2), accepted_suffix={})
    Note over N2: a majority promised, Node 2 is Leader.<br/>slot 5 was accepted at (2,1): recover it first.
    N2->>N0: Accept(ballot=(4,2), slot=5, entry=v5)
    N2->>N1: Accept(ballot=(4,2), slot=5, entry=v5)
    N0-->>N2: Accepted(ballot=(4,2), slot=5)
    N1-->>N2: Accepted(ballot=(4,2), slot=5)
    Note over N2: slot 5 chosen. chosen_index 4->5
    N2-->>Cl: ProposeAck(slot 5 committed)
```

Then the **steady state**: the leader streams new client values into the log, one
Accept round per slot, never paying for Phase 1 again. Notice the **pipelining**:
the leader fires the Accept for slot 7 before slot 6 has come back, so several
values are in flight at once, and the commit index walks forward (`5 -> 6 -> 7`) as
quorums land:

```mermaid
sequenceDiagram
    autonumber
    participant Cl as Client C7
    participant N0 as Node 0
    participant N1 as Node 1
    participant N2 as Node 2

    Note over N0,N2: v6 = Entry("C7",2,"SET y=2"), v7 = Entry("C7",3,"SET z=3")
    Note over Cl,N2: Storing values, Phase 2, streamed per slot, no Prepare again
    Cl->>N2: Propose(v6)
    N2->>N0: Accept(ballot=(4,2), slot=6, entry=v6)
    N2->>N1: Accept(ballot=(4,2), slot=6, entry=v6)
    Cl->>N2: Propose(v7)
    N2->>N0: Accept(ballot=(4,2), slot=7, entry=v7)
    N2->>N1: Accept(ballot=(4,2), slot=7, entry=v7)
    N0-->>N2: Accepted(ballot=(4,2), slot=6)
    N1-->>N2: Accepted(ballot=(4,2), slot=6)
    Note over N2: slot 6 chosen. chosen_index 5->6
    N2-->>Cl: ProposeAck(slot 6 committed)
    N0-->>N2: Accepted(ballot=(4,2), slot=7)
    N1-->>N2: Accepted(ballot=(4,2), slot=7)
    Note over N2: slot 7 chosen. chosen_index 6->7
    N2-->>Cl: ProposeAck(slot 7 committed)

    Note over N0,N2: Heartbeat, carry the commit index so followers advance
    N2->>N0: Heartbeat(ballot=(4,2), commit=7)
    N2->>N1: Heartbeat(ballot=(4,2), commit=7)
    Note over N0,N1: followers apply the prefix [..., SET x=1, SET y=2, SET z=3]
```

With a stable leader streaming a log, one question remains: what happens when a
node crashes mid-stream and comes back? That is [Crash and restart
safety](restart-safety.md).
