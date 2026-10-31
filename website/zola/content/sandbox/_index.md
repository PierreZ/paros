+++
title = "Playground"
description = "Run a paros universe in your browser and watch Paxos at work"
weight = 3
sort_by = "weight"
+++

The **[paros playground](app/)** is a tool to understand Paxos and paros. It runs a paros
universe in your browser. Use it to see where paros puts each tenant and each journal, how Paxos
chooses the records you write, and how the cell recovers when you crash a machine or cut it from
the network.

## What runs

Each machine runs the same code as `parosd`. It starts with an empty disk and waits for a cell.
The operator in the page makes the same calls as `parosctl`:

- `init` is always the first step, at 0 ms. You choose how many machines it lists. It is
  `cell init`, then `universe init`. `cell init` is a single-decree Paxos over the listed
  machines: they choose the plan of the cell `cell-1`. `universe init` creates the universe
  tenant in that cell. The machines that `init` does not list stay idle.
- "Create a tenant" is `parosctl tenant create`.
- "Create a journal" is `parosctl journal create`. You choose its durability: the failures
  that each acceptor partition survives (`F`, 0 to 3) and the number of acceptor partitions
  (`P`). Each acceptor partition has `2F+1` acceptors. The cell's coordinator places the journal
  on some of the machines, one zone at a time.
- "Write N records" appends N records to a journal you choose. Multi-Paxos chooses each record
  at a position of the journal.
- "Read N records" reads N records of a journal you choose, from its first position.

The playground has one cell over three zones: machine *i* is in zone *i* mod 3.

## Faults

Four more actions break the simulated world. They never change the paros code:

- "Crash a machine" kills the machine's process. The writes that its disk did not sync are lost.
  The machine stays down until you restart it.
- "Restart a machine" starts it again on the disk it had. It reads its state back from the disk
  and joins its journals again.
- "Cut a machine from the network" drops every message to or from the machine, from the other
  machines and from the client.
- "Join a machine to the network again" removes that cut.

The simulator, [moonpool](https://pierrez.github.io/moonpool/), does these faults with its own
fault injector. The fault sites that paros carries for its simulation tests stay off, as they are
in production. A fault starts when the operator gets to its step, as every other step does. Try
the example "Crash the leader, then restart it": the journal elects a new leader, and the next
writes go to it.

## What is simulated

The network, the disks and the clock are simulated by
[moonpool](https://pierrez.github.io/moonpool/). The playground does not change the paros code: it
gives the code a simulated network, a simulated disk and a simulated clock in place of the real
ones. Times on the page are simulated milliseconds. They are not production numbers.

## Replay

A run depends only on three things: the seed, the configuration and the action log. The same
three things always give the same run, event for event. The link to the page holds all three,
so a link replays a run exactly. When you add an action, the page runs the whole log again from
the start. An action you add never changes what happened before it.

Replay is also a mode of the page. "Save the run" gives you the run as a JSON file: its seed, its
configuration and its action log. "Open a run" (or paste one) loads such a file and replays it.
In replay mode you cannot edit the log. Use "◀ step" and "step ▶" (or the `,` and `.` keys) to
go from one step of the log to the next, and "◀ msg" and "msg ▶" to go one message at a time.
A link that starts with `#replay:` opens in replay mode.

## What the page shows

The page plays the run in a cluster view. The cell is a dashed frame, and its machines are
grouped by zone. Each machine shows the journals it serves: one chip per journal, coloured by
tenant, with `L` for the leader and `A` for an acceptor. The internal tenants are grey. Each
machine also shows the tenants whose matchmaker set it is in.

Every message between machines is a dot that moves from one chip to another. The shape of the
dot tells the phase:

- a diamond is Phase 1 (`Prepare`, `Promise`) or a catch-up;
- a disc is Phase 2 (`Accept`, `Accepted`);
- a square is a commit: the slot is chosen;
- a triangle is a read;
- a small faint dot is a heartbeat.

A client outside the cell sends the writes and reads. Each call is a large dot to the leader
of the journal, and its answer comes back.

The run plays in three acts, which come from the action log:

1. **The cell runs itself.** Only the internal journals run: the universe directory, the
   machine registry and the election journal.
2. **A tenant arrives.** Its control journal and its journals appear on the machines.
3. **Writes flow.** The client writes and reads, and the internal traffic continues.

A crashed machine has a red dashed frame and the word "crashed". A machine cut from the
network has a red dotted frame and the words "cut off".

Click a chip to follow its journal. The panel on the right draws the quorum system of that
journal. For a grid, a full row is a Phase-1 quorum and a full column is a Phase-2 quorum.
Slot *s* goes to column *s* mod *cols*, and the panel lights that column. For a majority, the
panel shows how many acceptors decide. The panel also shows the chosen slots of the log.

Under the quorum, a table shows the state of each acceptor after the last step, as the admin
call `Inspect` reports it from each machine:

- **promised**: the highest ballot the acceptor promised in Phase 1, as `round.node`;
- **accepted**: how many accepted records it holds, and the last slot with the round it was
  accepted at (Phase 2);
- **chosen**: the highest slot it knows chosen;
- **floor**: the first slot it still holds. Truncation moves it up.

These values come from read-only accessors. Reading them never changes what a machine does.

## Which Paxos

paros uses three kinds of Paxos, and the page names the kind of each message and event:

- **Single-decree Paxos** chooses one value once. `cell init` uses it: the listed machines
  choose the plan of the cell.
- **Multi-Paxos** chooses a log, one slot at a time. Each journal is one Multi-Paxos instance:
  every `Accept`, `Accepted` and `Commit` is Multi-Paxos.
- **Matchmaker Paxos** changes who votes. Before Phase 1, a candidate registers its ballot and
  its configuration with the matchmakers of the tenant. Then Phase 1 asks the acceptors of every
  configuration still in force. The events `matchmaking_started` and `matchmaking_completed` show
  that step.

Pause, then step one message at a time. The caption tells what each message is and why it is
sent. The timeline under the view counts the messages, stacked by tenant. Drag it to move in
time. Below the view are the action log, what each step came to and the events of the
machines.

## How it differs from the game

[paros play](../play/) runs the Paxos core by hand: you are the network and the clock, and you
answer for each role. The playground runs the whole system and lets it answer for itself. Use the
game to learn the rules. Use the playground to watch a real deployment follow them, and to break
it.
