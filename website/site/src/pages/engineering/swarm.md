---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
widget: true
title: Swarm testing
description: Each seed turns on its own subset of faults and draws its own rates. Over many seeds, every mix of faults comes up, and the shipped code makes its rare choices often.
---

## The problem

Turn on every fault at a low rate, and the families crowd each other out. A cut link hides a
corrupt disk, a crash hides a slow sync. The extreme worlds that find bugs, such as "only the
disk fails, and often", almost never come up.

## The technique

**Swarm testing.** Each seed draws a subset of each fault family and turns the rest off. Then
it draws the rates of the families it kept. One seed is a calm network on a bad disk, the next
is a busy network on a good disk. The same seed always draws the same world.

**BUGGIFY.** A rare but valid choice of the shipped code is a site in the code:
`buggify_with_prob!` at the line that makes the choice. Each seed activates some sites, and an
active site fires often. A short timeout, a late machine, a batch sent alone: the rare branch
runs on many seeds. A `hint!` is the same idea for a crash: the code names the moment, and the
seed decides whether a machine dies there.

**Assertions that must fire.** Each rare path has a `reachable!` or `sometimes!` assertion. The
sweep in CI fails when one never fires: a fault or a branch that no seed reaches tests nothing.

## Six seeds

The runs below are the playground engine with every fault family on: the network, the disk
and crashes at the hints. The operator steps are the same each time. Each seed draws its own
world: the strip shows its faults on the time axis, and the line under it names each kind.

<div data-widget="swarm"><p class="aside">This demo needs JavaScript and the playground engine.</p></div>

## In paros

- `crates/sim/paros-sim`: the one workload, the swarm draws of the shape (machines, zones,
  tenants, journals) and of the operations, and the audit.
- `crates/sim/paros-sim-runner`: the sweep, the gates that must fire, and `gates::RARE`, the
  list of excused gates, which may only shrink.
- moonpool's `enable_chaos` with `ChaosMode::Swarm` per fault family, and `swarm_operations`
  for the workload's operations.
- The [same code](../same-code/) page shows the `hint!` points, and the
  [simulation](../simulation/) page shows a seed replayed.
