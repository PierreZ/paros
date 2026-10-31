---
layout: ../../layouts/Doc.astro
kicker: Docs
section: docs/
title: The playground
description: What runs in the playground, what is simulated and how a run replays.
---

The [playground](../../playground/) runs a paros universe in your browser. Each machine runs
the same code as `parosd`. The page calls the same client and admin API as `parosctl`.

## What is simulated

The network, the disks and the clock are simulated by
[moonpool](https://pierrez.github.io/moonpool/). The paros code does not change: it gets a
simulated network, disk and clock in place of the real ones. Times are simulated
milliseconds, not production numbers.

## Faults

- **Crash** kills the process of a machine. Writes its disk did not sync are lost.
- **Restart** starts it again on the disk it had.
- **Cut** drops every message to or from the machine. **Join** removes the cut.

## Replay

A run depends only on its seed, its configuration and its action log. The same three always
give the same run, event for event. The link to the page holds all three, so a link replays a
run. "Save the run" gives a JSON file; "Open a run" replays one.

## What the page shows

Machines are grouped by zone. Each machine shows its journals as chips, coloured by tenant,
with `L` for the leader and `A` for an acceptor. Internal journals are grey. Each message is
a dot that moves between chips: a diamond for Phase 1, a disc for Phase 2, a square for a
commit, a triangle for a read.

Click a chip to follow a journal. The panel shows its quorum system and, for each acceptor,
the state that the admin call `Inspect` reports: the promised ballot, the accepted records,
the highest chosen slot and the floor. These values come from read-only accessors.
