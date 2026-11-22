---
layout: ../../layouts/Doc.astro
kicker: parosd
section: parosd/
status: parosd
title: Machines
description: A machine is an id. It is formatted, admitted by a request to the cell, booked for roles, and released. The cell heals its journals with nobody watching.
---

## The problem

Machines fail, lose their disks, change their addresses and come back. A service that keys a
machine by its address, or that trusts its memory of which machine is dead, gets each of these
wrong one day. And a cell that waits for an operator to replace a machine is down until someone
wakes up.

## The rule

**A machine is an id** (rule 5,
[§3.2](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-2-machines.md#32-machines)).
A machine is its `node_id` and its incarnation. Its address is a hint, and "gone" is a record in
the cell control journal, never the memory of one coordinator. A wiped disk gets a new
`node_id`, so a wiped identity never comes back as the old one.

## The life of a machine

<span class="chip-status">formatted</span> → <span class="chip-status">admitted</span> →
<span class="chip-status accent">booked</span> → <span class="chip-status">draining</span> →
<span class="chip-status done">gone</span>

1. **Format.** `parosd` mints a random `node_id` and writes its machine record. The machine is
   idle: it answers `Identify` and waits. It never forms a cell or joins one on its own.
2. **Bootstrap, by one single decree.** `parosctl cell init --members a,b,c` runs
   single-decree Paxos on the plan of the cell, with every listed machine as an acceptor
   ([§3.1](https://github.com/PierreZ/paros/blob/main/docs/architecture/03-1-cell-tenant-and-bootstrap.md#31-the-cell-tenant-and-bootstrap)).
   Two `cell init`s sent to different machines form at most one cell. The decree only writes the
   first configuration of the cell control journal and the admission of each listed machine.
   After it, no machine is special.
3. **Admission.** Every later machine joins by a request to the cell coordinator
   (`cell add-machine`). The coordinator writes the admission, then sends `Admit` with a
   snapshot of the registry, which the machine keeps on disk. A machine that lost its disk is a
   new machine, admitted again by a credential.
4. **Bookings.** Only the cell coordinator writes a booking: where one role of one journal runs.
   A machine has no slots; placement spreads roles by busyness.
5. **Release.** A release is not a stop. A released acceptor keeps answering until
   `may_retire` holds ([Matchmaker Paxos](../../paxos/matchmaker/)), and a released matchmaker
   set stays open. The machine keeps a durable ledger, so a deleted
   role never starts again.
6. **Gone.** `MachineGone` is written only after the routine of rule 4 replaced the machine in
   each set, one for one. With no machine to take its place, the id stays an unreachable member,
   each set keeps its size, and the cell reports "cannot place".

## The boot path

A machine must serve the control journal of its cell before it can read its bookings. So it
restores its roles from its own disk first, then registers. The host gets a call that lists the
roles on a disk (#1414 (Storage lists the roles on a disk)), and a part between the cell and the
host turns a booking into `Input::Assign` and a release into `Input::Unassign` (#1424 (from a
booking to an Assign)). The journal never reads a booking.

A reference of moonpool RPC names one incarnation, so it changes at each restart. A machine
answers one fixed call, "which reference serves this id now". A peer whose reference is refused
asks it once, so it finds a restarted role even when the coordinator of the tenant is down
([§4.4](https://github.com/PierreZ/paros/blob/main/docs/architecture/04-roles.md#44-the-layers-of-the-service)).

## The cell heals with nobody watching

Goal 2: after the faults stop, each journal is whole again within one bound, or reports a true
reason. Two parts do it, and each has one job (§4.4, "The cut of the old tenant coordinator"):

<div class="arch" role="img" aria-label="The grid controller of a journal asks the cell, through the placement port, which member must leave, where a new member goes and how many moves may run. Then it changes the journal with its fenced reshape call on the register of the journal.">
  <div class="box">cell <small>who must leave · where a new member goes · how many moves run</small></div>
  <div class="arrow">▲▼ <small>the placement port</small></div>
  <div class="box lead">grid controller of one journal <small>book · reconfigure · wait for GC · release</small></div>
  <div class="arrow">▼ <small>fenced reshape</small></div>
  <div class="box side">the register of the journal</div>
</div>

- **The grid controller** of each journal drives the change of its membership. It changes the
  journal only through its fenced reshape call
  ([The controller](../../journal/controller/),
  [§J6.1](https://github.com/PierreZ/paros/blob/main/docs/journal/06-controller.md#j61-the-grid-controllers-fenced-reshape)).
- **The cell** answers the questions of the whole cell through the **placement port**: which
  member must leave (its machine drains, is slow, is held or is gone), where a new member goes,
  and how many moves run at one time. A grant of the port is also the token of the rate limit of
  the cell. The calls of the port are **Open** (#1421 (the placement port)).

## In paros

The machine is a crate of layer 5 (#1426 (paros-machine)); the controller exists today as
`paros-journal-control`. The model `cell-control` with a machine wiped and admitted again proves
rule 5; it is **not proved** until its gate is green.
