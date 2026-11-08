---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: The playground
description: What runs in the playground, what is simulated, and what each block does.
---

The [playground](../../playground/) runs one live paros universe in your browser. Nine
machines run the same code as `parosd`. Machines 3 and 5 run with `PAROS_ROLES=stateless`: they
have no durable store, so they hold proxy leaders, batchers and frontends, and never a journal
or a matchmaker. They cannot found a cell. The other machines take every role. Each block calls the same client and admin API as
`parosctl`, and the operator log prints that `parosctl` line.

## What is simulated

The network, the disks and the clock are simulated by
[moonpool](https://pierrez.github.io/moonpool/). The paros code does not change: it gets a
simulated network, disk and clock in place of the real ones. The universe runs in a web
worker. Pause stops its clock; speed sets how many simulated milliseconds pass per real one.
Times are simulated time. Nothing on the page measures paros.

A universe depends only on its seed and on what you click, and when. Reset starts a fresh
universe with the seed you give.

## The blocks

Each action is one block. A block reads as a command, and its values are inline: a dropdown, a
number or a name. Pick the values, then click the block (not a field) to run it. A dropdown lists
only the values that are valid now, from the last topology the operator read: *restart* lists
only crashed machines, *heal* only cut machines. A block with no valid value is grey, and its
title says why. The groups fold, so that the cells stay in view on a phone.

- **Bootstrap.** *init cell-1 over (n) machines* forms the first cell over the first machines and
  the universe on it. *add [machine] to [cell]* admits an idle machine into cell-1 or cell-2
  (`cell add-machine`). *add cell-2 over (n) machines* forms an idle cell over the last idle
  machines that store (`init --idle`), then joins it to the universe (`universe add-cell`).
- **Resources.** *create tenant [name] in [cell]* creates a tenant in that cell. *create (n)
  journals in [tenant]* creates journals with the durability, partitions, writer mode and layout
  of the block. *configure [journal]* records a durability, a layout and the role counts for a
  journal: replicas, proxy leaders and batchers (`--replicas`, `--proxies`, `--batchers`). The
  counts apply to a compartmentalized journal only, and a batcher serves a multi-writer journal
  only. It also records the tenant's frontend count (`--frontend-count`). The tenant
  coordinator then reconfigures the journal and places the roles.
- **Workloads.** *write (n) records to [journal]* writes to one journal, or to every journal at
  the same time. *start writer on [journal] at (n) per second* appends to a journal at that rate
  until *stop writer on [journal]*. *read last (n) records of [journal]* reads the tail. *set new
  leader of [journal]* gives a single-writer journal a new writer: the old writer's next write is
  refused. *resolve [tenant]* asks where the universe sends a tenant's clients.
- **Machines.** *maintenance [machine] for (n) s* stops its cell from re-placing the machine's
  roles for that time. *drain [machine]* moves every role off the machine. *remove [machine]*
  retires a drained machine.
- **Faults.** *kill [machine]* crashes a machine: writes its disk did not sync are lost.
  `busiest` is the machine that leads the most journals. *restart [machine]* starts a crashed
  machine again on its disk. *cut off [machine]* drops every message to or from a machine. *heal
  [machine]* removes the cut. A fault strikes at once, even while the operator runs a step.

## What the page shows

The cells are side by side, their machines grouped by zone; the stateless machines are in their
own dashed column, and the idle machines wait outside.
Each machine shows its journals as chips, coloured by tenant, with `L` for the leader and `A`
for an acceptor. Internal journals are grey. Each message is a dot that moves between chips: a
diamond for Phase 1, a disc for Phase 2, a square for a commit, a triangle for a read.

### Matchmakers

Each tenant has one matchmaker set (Matchmaker Paxos). A machine in a set shows a dashed
badge `⬡ tenant` under its chips. The badge fills while that matchmaker works for the tenant.
A matchmaker message is a hexagon: hollow from the leader to the matchmaker, full on the way
back. A GC message is a cross.

When you follow a journal, its panel shows the tenant's matchmakers and the floor of each one.
It also shows the configurations they hold, by ballot: the acceptors of each, whether an
election or a reconfiguration registered it, and the matchmakers that still hold it. The
process is as follows:

1. A new leader registers its configuration under its ballot on a majority of the set.
2. The matchmakers send back the older configurations they hold, so the leader runs Phase 1
   over them too.
3. The leader raises the floor of each matchmaker to its ballot (GC). A configuration under the
   floor is dropped and shown struck out.

Kill an acceptor of a followed journal to see it: the cell heals the journal onto another
machine, by a reconfiguration, and the old configuration is dropped. The page reads this from
the events the machines log; it changes nothing.

*drain [machine]* is the operator's way to see it without a fault: the cell coordinator moves
each journal off the machine by a reconfiguration. When the machine is in a matchmaker set, the
set moves too, by a handover to a new generation. The panel shows its steps: the old set stops,
the successor gets every registry (bootstrap), a single-decree Paxos chooses it (decree
prepare, decree accept, chosen), and its matchmakers activate.

### Step through an election

*Next election* waits for the next election of the followed journal. Kill its leader to start
one. The page then holds its clock on the election, and ◀ ▶ move it from one step to the next:
matchmaking, the matchmakers' answer, Phase 1 (Prepare, then a promise quorum), elected, then
Phase 2 (Accept) and chosen when a writer runs. The universe runs two more simulated seconds,
then waits. *Resume* starts the clock again.

### One machine

Click a machine's name to see everything it holds: its journals with their role and acceptor
state, its matchmaker sets with each journal's floor, every role the cell books on it, and the
events it logged about itself. *← journal* goes back to the followed journal.

Click a chip, or pick a journal in the head of the journal panel, to follow it. The journal panel shows its quorum system and,
for each acceptor, the state that the admin call `Inspect` reports: the promised ballot, the
accepted records, the highest chosen slot and the floor. The roles table shows what each
machine holds: its class, its acceptors, leaders and matchmakers, and the other roles the cell
books on it (replicas, proxy leaders, batchers, frontends), as the admin view reports them. The timeline at the bottom shows the last 30 simulated seconds: messages by
tenant, and a mark for each step.
