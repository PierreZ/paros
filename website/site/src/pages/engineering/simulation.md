---
layout: ../../layouts/Doc.astro
kicker: Engineering
section: engineering/
title: Simulation
description: Seeded random walks run the real state machines with faults on. Oracles with no code of their own judge each step, TLC checks each walk against its model, and a coverage gate asks for every probe. No exception, no allow list.
---

## The problem

A model checks the model, not the code. A unit test checks the case that its author thought of.
The bugs of a Paxos library are in the space between: a correct rule that the code builds a
little wrong, on an order of events that nobody wrote down.

## The absolute rule

This rule has no exception, and it is more important than every other rule of the repository.
The goal of simulation is to bend the software enough to trigger its rough edge cases, without
breaking it.

- **No excuse.** A failure of a run is a finding. Fix the code, or fix the walk when it does a
  thing that the contract of the code does not permit.
- **No allow list** of failures, seeds, messages, probes, oracles or faults.
- **No skip.** An oracle judges every case. A test that misses a tool fails; it does not skip.
- **No weaker check.** No oracle less strict, no budget larger and no fault less frequent to make
  a red run green.
- **No retry.** A red seed is not run again until it is green.
- **Bend, do not break.** A fault stays inside the contract: the disk keeps what an fsync covered.

The full text is at the top of
[`AGENTS.md`](https://github.com/PierreZ/paros/blob/main/AGENTS.md#absolute-rule-no-exception-in-simulation).

## Walks on the code that ships

[`consensus-sim`](https://github.com/PierreZ/paros/tree/main/crates/sim/consensus-sim) runs a
seeded random walk (moonpool-walk) over the real roles of `paros-consensus`, through their public
API only. A seed is a run, so a red seed replays. One walk for each flavor (a `Flavor`): Multi-Paxos
on a majority, a grid and partitions, the leader handoff, proxy leaders, replicas, leaderless
reads, matchmakers and reconfiguration, retirement, the matchmaker set handover, compaction and
disk faults, plus the walks of the single decree and the batcher.

The walk drops, repeats and reorders messages. It crashes a party and restarts it from its disk,
and the disk loses the writes that no fsync covered. It rots records inside the contract,
retires and rejoins nodes, admits late nodes, hands the matchmaker set over and moves stuck
rounds. The core is never buggified: it has no random number, no clock and no knob, so a rare
path is reached only through what a driver may do.

After each step, oracles judge the run. An oracle shares no code with the core: it reads what the
driver saw (a write that reached a disk, a message that left a party) and folds it again from the
papers and the contract.

| Oracle | What it says |
|---|---|
| `ONE_VALUE` | two durable quorums never choose two values for one slot |
| `COMMIT_BACKED` | each `Commit` is backed by a durable Phase-2 quorum at its ballot |
| `PERSIST_BEFORE_SEND` | a promise or a vote leaves a node only once it is on the node's disk |
| `LEARNED_CHOSEN` | what a party learns or applies is the value that was chosen |
| `JOURNAL_FOLD` | each party gets the outcome that the journal contract gives the chosen log |
| `PREFIX_SHRINKS` | the durable chosen prefix never moves back |
| `READ_SEES_WRITES` | a leaderless read sees every write that completed before it started |

## Each walk is checked against its model

Each walk records its steps, and TLC checks that the trace is a run of the TLA+ model
(moonpool-tla). A trace spec extends a model that TLC checks and defines no step of its own, so a
step of the code that the model does not have fails the check. A test that needs TLC calls
`require_tlc`: with no TLC, the test fails, it does not skip.

The trace checks are tested too:

- Each trace spec names the steps that its checked walks must take (a `Noop` gap fill, a
  reconfiguration, a compaction that keeps a record, a node that retires and rejoins), so a check
  over walks that never reach a rule fails.
- Each trace spec must refuse about 20 named edits of its recorded walks, at the edited event or
  after it, never before (`trace/mutate.rs`, #1308 (mutate recorded traces)).

## Every probe reached

A rare branch of the core that a walk must reach is a `probe!` at its line. The walks branch from
the runs that reach a new probe or a new edge of the core (branch coverage with SanitizerCoverage),
and a gate judges every probe over all the walks: CI fails when one is not reached. There is no
list of excused probes. A probe that no walk reaches gets a walk move, a flavor or a steer, never
only a larger budget. Each CI job has 30 minutes, so two shards walk half each and one job judges
their union. A nightly run walks new seeds.

## What is not there yet

- A moonpool-sim run of the whole journal, with its driver, many journals in one process, machine
  and zone loss. The journal is designed again on the core with segments.
- Coverage of the model's states, not only of the code's branches (moonpool #553).
