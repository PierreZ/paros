+++
title = "Sandbox"
description = "Run a paros universe in your browser and watch Paxos at work"
weight = 3
sort_by = "weight"
+++

The **[paros sandbox](app/)** runs a paros universe in your browser. Use it to see how Paxos
and paros behave when machines join, crash and lose the network.

## What runs

Each machine runs the same code as `parosd`. It starts with an empty disk and waits for a cell.
The operator in the page makes the same calls as `parosctl`:

- `init` forms the cell. `cell init` is a single-decree Paxos over the founding members: they
  choose the plan of the cell.
- A write appends a value to a journal. Multi-Paxos chooses it in a slot of the journal's log.
  The first write creates a tenant and its journal through the cell's coordinator.
- "Add a machine" is `parosctl cell add-machine`: the cell control journal registers the idle
  machine, then the machine is admitted.

You can also crash a machine, restart it, cut the network between two machines and heal it.

## What is simulated

The network, the disks and the clock are simulated by
[moonpool](https://pierrez.github.io/moonpool/). The sandbox does not change the paros code: it
gives the code a simulated network, a simulated disk and a simulated clock in place of the real
ones. Times on the page are simulated milliseconds. They are not production numbers.

## Replay

A run depends only on three things: the seed, the configuration and the action log. The same
three things always give the same run, event for event. The link to the page holds all three,
so a link replays a run exactly. When you add an action, the page runs the whole log again from
the start. An action you add never changes what happened before its time.

## What the page shows

- The machines, with their state at the time you select: idle, founding member, admitted or
  down. A gold ring shows a machine that leads at least one journal.
- The messages each machine sends, as lines that move from the sender to the receiver.
- What each action of the log came to, and when it started and ended.
- The events the machines log, up to the time you select.

The page reads what the machines log. It does not read or change their memory. A cell holds
several journals (the cell control journal, the election journal, the universe directory, the
tenant control journals and your journal), so the counts on the page add up all of them.

## How it differs from the game

[paros play](../play/) runs the Paxos core by hand: you are the network and the clock, and you
answer for each role. The sandbox runs the whole system and lets it answer for itself. Use the
game to learn the rules. Use the sandbox to watch a real deployment follow them.
