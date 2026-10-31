---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
widget: determinism
title: Deterministic simulation
description: paros runs whole cells inside one thread, on a simulated network, disk and clock. A seed decides every draw, so a seed is a run you can replay.
---

## The problem

A distributed bug needs a rare order of events: a crash between a sync and a send, a message
that arrives after a new leader, a disk that lies once. On real machines you see it once and
never again. You cannot fix a bug that you cannot replay.

## The technique

[moonpool](https://github.com/PierreZ/moonpool) runs every machine of a cell in one thread. It
gives each machine a simulated network, disk and clock. One seed feeds every random draw: the
latency of each message, the timing of each disk write, each fault, and each choice that paros
makes. The same seed gives the same run, event for event.

- **One workload, swarm testing.** `paros-sim` has one workload. Each seed draws its own shape:
  the number of machines and zones, the tenants and journals, the writers and readers, the
  faults and their rates.
- **Faults stay on for the whole run.** Crashes, restarts, cut links, slow and corrupt disks and
  power losses inside a sync. Only the final check is quiet.
- **Faults inside the shipped code.** Each risky point in paros has a `hint!` or a `buggify!`:
  "batch durable, not sent", "accepts sent, own vote not durable". In simulation, moonpool can
  stop, delay or kill the machine there. In production they do nothing.
- **Two judges.** An audit checks each fact when it happens: one leader per ballot, a dense
  sequence per journal, a chosen value never changes. At the end, a linearizability checker
  replays the client history against a model of a journal.
- **Assertions that must fire.** A `reachable!` or `sometimes!` assertion names a rare path.
  If no seed reaches it, the run fails: a fault that never fires tests nothing.

## Run a seed

The playground engine below is paros compiled to wasm, on moonpool's simulator. It runs five
machines: it forms a cell, creates a tenant and a journal, writes, crashes a machine, writes
again, restarts it and reads. Run a seed, then run it again: the two runs give the same
fingerprint. Change the seed and the run changes.

<div data-widget="determinism"><p class="aside">This demo needs JavaScript and the playground engine.</p></div>

## When a seed fails

CI runs a sweep of seeds on every pull request, in four shards. A nightly hunt runs many more.
A failing seed is a bug report that replays: run the same seed again with more logs, find the
first wrong event and fix it. A seed is not kept as a test, because a new random draw anywhere
changes what every seed does. The fix gets a `buggify!` that makes the rare path frequent, so
many seeds reach it.

```sh
# Replay one seed of the main campaign, event for event.
cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- replay-main 9163977011002237483
# The determinism canary: run each seed twice and compare every random draw.
cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- canary 200
```

## In paros

- `crates/sim/paros-sim`: the workload, the audit, the linearizability checker and the fault
  injectors.
- `crates/sim/paros-sim-wasm-check`: the same workload compiled to wasm, to check that the
  simulation does not depend on the host.
- The playground (`crates/support/paros-wasm`, feature `playground`) runs the shipped
  `run_machine` on the same simulator. [Open it](../../playground/).
