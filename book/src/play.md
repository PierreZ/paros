# Play the game

[**paros play**](play/) is the interactive half of this book. It runs the real
`paros-core` state machine in your browser, compiled to WebAssembly. It hands you
the two things a distributed system never lets you hold: **the network and the
clock**.

<!-- toc -->

## What the game is

You choose which in-flight message lands next. You choose which message is dropped
or duplicated, which node ticks, who crashes, when it restarts, and what a client
proposes. There is no latency model and no random scheduler. A partition is a
message you decline to deliver, and a slow node is a node you decline to tick.

When a node must **answer**, you answer for it. Promise or Nack? Which value goes
in the Accept? Re-propose the slot the quorum reported, fill the hole with a
no-op, or open a fresh slot? Serve the read or wait? The game asks, and **the core
judges**:

- A **right** answer is what the real role does, so the real node steps and the
  world advances.
- A **wrong** answer does not enter the core. The world does not take the bad
  branch. The game explains the violation that the answer causes, with this
  prompt's own ballots and values, and the level records the mistake. No toy
  "wrong acceptor" exists anywhere in the game. `paros-core` is never forked and
  never buggified, exactly as the rest of the project treats it.

You play a role until you get it right, and that is how you **stop** playing it.
Pass the level that teaches a role and you unlock automation of that role in every
later level. Acceptor replies, the P2c value selection, the apply walk and
heartbeat delivery each become a toggle that you earn. The levels get shorter as
the protocol gets larger, so by Act II you drive a leader instead of clicking
every Promise.

## How a level works

Every level is the same five things:

- **Briefing** — a short statement of the world you enter and why it is
  interesting. This is the mechanism prose that used to sit in the chapters.
- **Goal** — a predicate over the world, evaluated after every action: a value is
  chosen, `chosen_gap()` is `None`, the client's read watermark never regressed.
  It is `Open`, `Reached`, or `Failed` with a reason.
- **Prompt** — the role question, with the node's own relevant state and the
  choices. The state includes its promised ballot, its accepted record for the
  slot, and what the Promises reported. Answer the prompt and the core says
  whether you are right.
- **Undo** — the engine keeps your action log and rebuilds the world by replay.
  The core is deterministic and draws no randomness, so undo is exact. There is no
  hidden state to restore and no seed to roll again.
- **Hints and automation** — a hint appears after a mistake or two. The automation
  toggles you have unlocked sit beside the stage. A level pins one off when it
  needs you to do that part by hand.

The stage derives its state from the world on every change, and animates nothing
on its own. You see the node ballots and roles, and the accepted log as a column
of slot boxes: green for chosen, red for a hole, grey for undecided. You also see
the election arc that fills with ticks, and the in-flight queue as clickable dots
on the wire.

## The level map

Act I is single-decree Paxos: three acceptors, one or two proposers, one slot, no
clock and no storage. Act II is the replicated log: three `ColocatedNode`s, a
client, and disks that survive a crash. Act III adds truncation, snapshots and a
second client. Act IV adds the quorum systems, the matchmaker plane, disk faults
and a wipe.

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
| [`act3/truncate-by-consensus`](play/#act3/truncate-by-consensus) | Ask the leader to compact, take its refusal, retry, and watch each node move its own floor | The floor is a decided control command, so one decision gives one cluster-wide floor | [Truncation and snapshot restore](truncation-and-snapshots.md) |
| [`act3/the-stranded-node`](play/#act3/the-stranded-node) | Strand a node below the floor, pull the snapshot, and state its promise after the install | A snapshot restores the log and never a promise: the node keeps `max(promise, ballot)` | [Truncation and snapshot restore](truncation-and-snapshots.md) |
| [`act3/read-index`](play/#act3/read-index) | Capture the index, confirm with a beat, and judge one ack that answers an older beat | Capture, confirm, serve — and an ack to an older beat proves nothing | [Why reads are not free](linearizable-reads.md) |
| [`act3/the-fresh-leader-trap`](play/#act3/the-fresh-leader-trap) | Refuse a read at a new leader that holds a valid quorum and a lagging chosen index | The read floor: a fresh quorum does not make a short applied prefix current | [Why reads are not free](linearizable-reads.md) |
| [`act3/linearizable-or-not`](play/#act3/linearizable-or-not) | Produce a history with a read across a leader change, then try to break it | Linearizability as three conditions over the client's own program order | [Why reads are not free](linearizable-reads.md) |
| [`act3/chosen-is-not-applied`](play/#act3/chosen-is-not-applied) | Answer a client retry for a slot that is chosen above a hole, twice | Chosen is a fact about the cluster; applied is a fact about this node | [Why reads are not free](linearizable-reads.md) |
| [`act4/flexible-quorums`](play/#act4/flexible-quorums) | Pick each phase's reach over four acceptors, with `q1 = 3` and `q2 = 2` | `q1 + q2 > n`: the intersection that matters is between the phases, not inside one | [Beyond Multi-Paxos](beyond-multi-paxos.md#flexible-quorums) |
| [`act4/the-grid`](play/#act4/the-grid) | Decide two slots on two columns, then send a stray Accept outside a slot's column | A row elects and a column decides; a vote outside the column counts for nothing | [Beyond Multi-Paxos](beyond-multi-paxos.md#the-acceptor-grid) |
| [`act4/quorum-reads`](play/#act4/quorum-reads) | Serve a read at a follower from a row's vote watermarks, and hold one that is not covered | A linearizable read with no leader and no clock, paid for by waiting | [Beyond Multi-Paxos](beyond-multi-paxos.md#quorum-reads) |
| [`act4/the-handoff`](play/#act4/the-handoff) | Hand the leadership on under the same ballot, then try to hand it on again | Same ballot, no Phase 1, gap filling off, and one hop only | [Beyond Multi-Paxos](beyond-multi-paxos.md#cooperative-leader-handoff) |
| [`act4/matchmaking`](play/#act4/matchmaking) | Register a campaign, read the histories back, and close Phase 1 over each configuration | Phase 1 needs a quorum of every configuration in `H_b`, not a quorum of the union | [Beyond Multi-Paxos](beyond-multi-paxos.md#matchmakers-and-reconfiguration) |
| [`act4/reconfigure`](play/#act4/reconfigure) | Grow the acceptor set onto a spare and get a command chosen under it | A reconfiguration is a round change; a removed node still answers Phase 1 | [Beyond Multi-Paxos](beyond-multi-paxos.md#matchmakers-and-reconfiguration) |
| [`act4/garbage-collection`](play/#act4/garbage-collection) | Raise the watermark, then retire a removed acceptor with the evidence in hand | Installed is not collected, and a retire request must carry the effective watermark | [Beyond Multi-Paxos](beyond-multi-paxos.md#the-gc-watermark-and-retirement) |
| [`act4/matchmaker-generations`](play/#act4/matchmaker-generations) | Replace the matchmaker set: stop, reconstruct, bootstrap, decide, publish | The successor set is decided by single-decree Paxos over the old set | [Beyond Multi-Paxos](beyond-multi-paxos.md#matchmaker-set-generations) |
| [`act4/faulty-records`](play/#act4/faulty-records) | Damage one accepted record, then put each Promise that arrives into its CTRL case | "I no longer know" is a third answer, and it must not become "I did not vote" | [Beyond Multi-Paxos](beyond-multi-paxos.md#faulty-records) |
| [`act4/the-wiped-node`](play/#act4/the-wiped-node) | Erase a disk, take the library's refusal, and keep the survivors deciding | A lost promise cannot be restored, so the library refuses the boot | [Beyond Multi-Paxos](beyond-multi-paxos.md#the-wiped-node) |

Level ids are stable strings, never indices, and a level opens directly at
`play/#<id>`.

## What comes next

The four acts cover what paros implements today. *Compartmentalized Paxos* keeps
two compartments in reserve: proxy leaders and a separate replica tier. They are
not in the crate yet, so they have no level and no chapter. The rule holds for
both halves of this book. A mechanism gets a level when the core can play it for
real.

Correctness itself is not proven in the browser. The game runs one world that you
schedule by hand, and it draws no randomness at all. The deterministic simulation
is where paros proves its claims, over thousands of seeded runs with network
faults, crashes and storage faults. Every chapter's "Proven, not asserted" section
reports what that simulation holds.

## How this book relates to it

The levels teach the mechanism. The chapters are the **field guide** beside them:
the papers a rule comes from, the proof structure, the doctrine, and the map from
each protocol name onto the real symbol in `paros-core`. A chapter does not re-walk
an interleaving that a level plays. It states the mechanism in a paragraph and
links you to the level that makes you do it.

[Open the game](play/).
