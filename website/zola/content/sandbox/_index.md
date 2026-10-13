+++
title = "Sandbox"
description = "Run a paros universe in your browser and watch Paxos at work"
weight = 3
sort_by = "weight"
+++

The **[paros sandbox](app/)** runs a paros universe in your browser. Use it to see where paros
puts each tenant and each journal, and how Paxos chooses the records you write.

## What runs

Each machine runs the same code as `parosd`. It starts with an empty disk and waits for a cell.
The operator in the page makes the same calls as `parosctl`:

- `init` is always the first step, at 0 ms. You choose how many machines it lists. It is
  `cell init`, then `universe init`. `cell init` is a single-decree Paxos over the listed
  machines: they choose the plan of the cell `cell-1`. `universe init` creates the universe
  tenant in that cell. The machines that `init` does not list stay idle.
- "Create a tenant" is `parosctl tenant create`.
- "Create a journal" is `parosctl journal create`. The cell's coordinator places the journal
  on some of the machines.
- "Write N records" appends N records to a journal you choose. Multi-Paxos chooses each record
  at a position of the journal.
- "Read N records" reads N records of a journal you choose, from its first position.

The sandbox has one cell. A universe of several cells comes with milestone M12. The sandbox
injects no faults yet.

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

After each step, the operator reads where every tenant and journal lives. It uses the admin API
only: the universe directory, the control journal of each tenant and an `Inspect` of each journal
on each machine. The page shows one table for each step:

- A row for each tenant: the internal tenants (the cell tenant and the universe tenant) and the
  tenants you created, with their groups and their cell.
- Under each tenant, a row for each journal, its control journal first. A column for each
  machine shows the journal's leader (L), its acceptors (A) and the machines that are only in
  the tenant's matchmaker set (M).
- The last row counts the acceptors on each machine: how the cell spreads the journals.

The page also shows what each action of the log came to and the events the machines log up to
the selected step.

## How it differs from the game

[paros play](../play/) runs the Paxos core by hand: you are the network and the clock, and you
answer for each role. The sandbox runs the whole system and lets it answer for itself. Use the
game to learn the rules. Use the sandbox to watch a real deployment follow them.
