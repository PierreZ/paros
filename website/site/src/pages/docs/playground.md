---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: The playground
description: What runs in the playground, what is simulated, and what each button does.
---

The [playground](../../playground/) runs one live paros universe in your browser. Eight
machines run the same code as `parosd`. Each button calls the same client and admin API as
`parosctl`, and the operator log prints that `parosctl` line.

## What is simulated

The network, the disks and the clock are simulated by
[moonpool](https://pierrez.github.io/moonpool/). The paros code does not change: it gets a
simulated network, disk and clock in place of the real ones. The universe runs in a web
worker. Pause stops its clock; speed sets how many simulated milliseconds pass per real one.
Times are simulated time. Nothing on the page measures paros.

A universe depends only on its seed and on what you click, and when. Reset starts a fresh
universe with the seed you give.

## The buttons

- **Bootstrap.** *Init cell-1* forms the first cell over machines 0 to 2 and the universe on
  it. *Add a machine* admits an idle machine into cell-1 (`cell add-machine`). *Add cell-2* forms
  an idle cell over the last three idle machines (`init --idle`), then joins it to the universe
  (`universe add-cell`).
- **Resources.** *Create a tenant* creates one in the cell you pick. *Create a journal* and *Add
  3 journals* create journals in the newest tenant, with the durability and writer mode you
  pick. *Configure* records a new durability and replica count for a journal; the tenant
  coordinator then reconfigures it.
- **Workloads.** *Generate traffic* writes ten records to every journal at the same time.
  *Start a writer* appends to a journal five times a second until you stop it. *Write* and *Read
  the tail* write five records and read the last five. *Set a new leader* gives a journal a new
  writer: the old writer's next write is refused. *Resolve* asks where the universe sends a
  tenant's clients.
- **Machines.** *Maintenance* stops cell-1 from re-placing a machine's slots for 30 s. *Drain*
  moves every slot off an added machine. *Remove* retires a drained machine.
- **Faults.** *Kill a machine* crashes the machine that leads the most journals (or the one you
  pick): writes its disk did not sync are lost. *Restart* starts it again on its disk. *Cut off*
  drops every message to or from a machine. *Heal* removes the cut. A fault strikes at once,
  even while the operator runs a step.

## What the page shows

The cells are side by side, their machines grouped by zone; the idle machines wait outside.
Each machine shows its journals as chips, coloured by tenant, with `L` for the leader and `A`
for an acceptor. Internal journals are grey. Each message is a dot that moves between chips: a
diamond for Phase 1, a disc for Phase 2, a square for a commit, a triangle for a read.

Click a chip, or pick a journal, to follow it. The journal panel shows its quorum system and,
for each acceptor, the state that the admin call `Inspect` reports: the promised ballot, the
accepted records, the highest chosen slot and the floor. The roles table shows what each
machine holds. The timeline at the bottom shows the last 30 simulated seconds: messages by
tenant, and a mark for each step.
