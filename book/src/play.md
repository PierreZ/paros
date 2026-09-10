# Play the game

[**paros play**](play/) is the interactive half of this book. It runs the real
`paros-core` state machine in your browser, compiled to WebAssembly, and hands you
the two things a distributed system never lets you hold: **the network and the
clock**.

<!-- toc -->

## What the game is

You choose which in-flight message lands next, which one is dropped or duplicated,
which node ticks, who crashes, when it restarts, and what a client proposes. There
is no latency model and no random scheduler — a partition is you declining to
deliver, and a slow node is you declining to tick it.

Then, when a node has to *answer*, you answer for it. Promise or Nack? Which value
goes in the Accept? Re-propose the slot the quorum reported, fill the hole with a
no-op, or open a fresh slot? Serve the read or wait? The game asks, and **the core
judges**:

- A **right** answer is what the real role would have done, so the real node steps
  and the world advances.
- A **wrong** answer never enters the core. The world does not take the bad branch;
  instead the game explains the violation it would have caused, with this prompt's
  concrete ballots and values, and the level records the mistake. There is no toy
  "wrong acceptor" anywhere in the game — `paros-core` is never forked and never
  buggified, exactly as the rest of the project treats it.

Playing a role until you have it right is how you **stop** playing it: passing the
level that teaches a role unlocks automating it in every later level. Acceptor
replies, the P2c value selection, the apply walk, heartbeat delivery — each becomes
a toggle you earn, so the levels get shorter as the protocol gets larger, and by
Act II you are driving a leader rather than clicking every Promise.

## How a level works

Every level is the same five things:

- **Briefing** — a short statement of the world you are dropped into and why it is
  interesting. This is the mechanism prose that used to sit in the chapters.
- **Goal** — a predicate over the world, evaluated after every action: a value is
  chosen, `chosen_gap()` is `None`, the client's read watermark never regressed. It
  is `Open`, `Reached`, or `Failed` with a reason.
- **Prompt** — the role question, shown with the node's own relevant state (its
  promised ballot, its accepted record for the slot, what the Promises reported)
  and the choices. Answer it and the core says whether you were right.
- **Undo** — the engine keeps your action log and rebuilds the world by replay. The
  core is deterministic and draws no randomness, so undo is exact: there is no
  hidden state to restore and no seed to re-roll.
- **Hints and automation** — a hint appears after a mistake or two; the automation
  toggles you have unlocked sit beside the stage, and a level may pin one off when
  it needs you to do that part by hand.

The state you see is derived from the world on every change, never animated
independently: node ballots and roles, the accepted log as a column of slot boxes
(green chosen, red hole, grey undecided), the election arc filling with ticks, and
the in-flight queue as clickable dots on the wire.

## The level map

Act I is single-decree Paxos: three acceptors, one or two proposers, one slot, no
clock and no storage. Act II is the replicated log: three `ColocatedNode`s, a
client, and disks that survive a crash.

| Level | What you do by hand | What it teaches | Field guide |
|---|---|---|---|
| [`act1/choose-a-value`](play/#act1/choose-a-value) | Deliver Prepare, Promise, Accept, Accepted one message at a time, with one acceptor silent | The two phases, and why two of three is enough | [How Paxos chooses one value](choose-one-value.md) |
| [`act1/be-the-acceptor`](play/#act1/be-the-acceptor) | Answer every Prepare and every Accept yourself | The promise rule (`>`) and the vote rule (`>=`) — the whole of the acceptor | [How Paxos chooses one value](choose-one-value.md) |
| [`act1/adopt-the-value`](play/#act1/adopt-the-value) | Choose which value a second proposer puts in its Accept | P2c: a Promise that reports a value takes the proposer's own choice away | [Why one value is safe](safety.md) |
| [`act1/the-duel`](play/#act1/the-duel) | Run two proposers against each other and still get a value chosen | Livelock is a liveness failure and never a safety one | [How Paxos chooses one value](choose-one-value.md) |
| [`act1/quorum-intersection`](play/#act1/quorum-intersection) | Pick the Phase-1 and Phase-2 reach sets yourself, and try to dodge the overlap | Every Phase-1 quorum meets every Phase-2 quorum; the pivot acceptor is unavoidable | [Why one value is safe](safety.md) |
| [`act1/recovery-is-not-catch-up`](play/#act1/recovery-is-not-catch-up) | Adopt a value that exactly one acceptor ever accepted and nothing chose | Recovery repairs an invariant, not data: it fires when nothing was chosen | [Why one value is safe](safety.md) |
| [`act2/persist-before-send`](play/#act2/persist-before-send) | Order a `Ready` batch's writes and messages, then crash at each seam | A Promise or Accepted sent before its write is durable can un-make a decision | [Crash and restart safety](restart-safety.md) |
| [`act2/a-log-of-decisions`](play/#act2/a-log-of-decisions) | Deliver slot 3's Accepted before slot 2's, then decide what may apply | Slots are chosen out of order and applied in order: `chosen_index` is a contiguous prefix | [From one value to a log](replicated-log.md) |
| [`act2/elect-a-leader`](play/#act2/elect-a-leader) | Tick a follower to its timeout and drive one Prepare over the whole log suffix | Phase 1 once per leadership, and the recovery duty before any new work | [The stable leader](stable-leader.md) |
| [`act2/steady-state`](play/#act2/steady-state) | Deliver the Accepts, Accepteds and Heartbeats for a stream of commands | One round trip per command, pipelining, and the commit index piggybacked on the beat | [The stable leader](stable-leader.md) |
| [`act2/the-permanent-gap`](play/#act2/the-permanent-gap) | Drop exactly slot 1's Accepts, let slot 2 be chosen, crash the leader, then fill the hole | A slot no Promise reports wedges the whole cluster forever unless the new leader fills it | [The stable leader](stable-leader.md) |
| [`act2/what-survives-a-crash`](play/#act2/what-survives-a-crash) | Crash and restart at each step of a decision, and say what the durable record keeps | A chosen value must overwrite a stale lower-ballot accept, or a restart resurrects it | [Crash and restart safety](restart-safety.md) |
| [`act2/the-read-that-lies`](play/#act2/the-read-that-lies) | Decide whether a leader that has already been deposed may answer a read | Leadership is a belief; a read has to confirm it against a quorum | [Why reads are not free](linearizable-reads.md) |

Level ids are stable strings, never indices, and a level is opened directly at
`play/#<id>`.

## What comes next

**Act III** takes the log apart: truncation as a decided control command, the node
that falls below the floor and is recovered by `InstallSnapshot`, the read-index
round with its capture / confirm / serve split, the fresh-leader trap, and
linearizability as three client-side conditions.

**Act IV** is everything this book never wrote a chapter for: flexible quorums
(`q1 + q2 > n`), the acceptor grid, leaderless quorum reads, cooperative leader
handoff under the same ballot, matchmaker reconfiguration, the GC watermark, and
the matchmaker-set generations.

## How this book relates to it

The levels teach the mechanism; the chapters are the **field guide** beside them —
the papers a rule comes from, the proof structure, the doctrine, and the map from
each protocol name onto the real symbol in `paros-core`. A chapter never re-walks
an interleaving a level plays. It states the mechanism in a paragraph and links
you to the level that makes you do it.

[Open the game](play/).
