---
layout: ../../layouts/Doc.astro
kicker: Consensus
section: consensus/
title: How it is checked
description: Each flavor has a TLA+ model before its code. Then three judges run on the code that ships - each walk is checked against its model, the walks inject faults, and they reach every probe of the core.
---

## The problem

A Paxos bug hides in a rare order of messages, a crash between two writes, or a disk that forgets
what it did not sync. A test that a person writes finds the orders that the person thought of. A
model finds every order, but it checks the model, not the code.

## The technique

`paros-consensus` uses both, and links them:

1. **A TLA+ model first.** No code is built on a flavor before TLC checks its model. A rule that
   the paper does not have gets a check that passes with the rule, and a control that fails
   without it.
2. **Deterministic simulation.** `consensus-sim` runs a seeded random walk over the real state
   machines, through their public API only. A seed is a run. The walk drops, repeats and reorders
   messages, crashes a party and restarts it from its disk (and loses the writes that no fsync
   covers), and makes the moves of the clients and the clocks. After each step, oracles that share
   no code with the core judge the run.
3. **Trace checking.** Each walk records its steps, and TLC checks that the trace is a run of the
   model. Each trace spec extends a model that TLC checks, and defines no step of its own.
4. **Coverage.** The walks branch from the runs that reach a new probe or a new branch of the
   core. CI fails if a walk set does not reach every `probe!` of the core.
5. **Mutants.** Each oracle and each trace spec has a planted bug that it must catch. Each trace
   spec must refuse about 20 edits of its recorded walks. cargo-mutants edits the core and a test
   must fail; a TLA+ script removes each guard of each model and a check must fail.

There is no exception list: a failure is a finding, never a seed to skip. The rule is at the top of
[`AGENTS.md`](https://github.com/PierreZ/paros/blob/main/AGENTS.md#absolute-rule-no-exception-in-simulation).

## Each flavor and its judges

A model folder is in `specs/tla/`. A trace spec is the TLA+ module that a walk is checked against.
A walk is a `Flavor` of `consensus-sim` (`cluster/flavor.rs`), or its own walk where named.

| Flavor | Model | Trace spec | Walks |
|---|---|---|---|
| Single decree | `decree/` | `decree/DecreeTrace.tla` | the decree walk (`src/decree.rs`) |
| Multi-Paxos on a majority, a grid, partitions | `multi-paxos/`, `node/` | `node/MultiPaxosTrace.tla` | `multi_paxos`, `multi_paxos_five`, `grid`, `partitions` |
| Leader handoff | `leader-handoff/` | `leader-handoff/LeaderHandoffTrace.tla` | `handoff` |
| Proxy leaders | `proxy-leader/` | `proxy-leader/ProxyLeaderTrace.tla` | `proxies`, `proxies_on_grid`, `compartments` |
| Batchers | `batcher/` | `batcher/BatcherTrace.tla` | the batcher walk (`src/batcher.rs`) |
| Replicas and the read replica | `compartments/`, `node/` | `node/CompartmentsTrace.tla` | `replicas`, `compartments_compacting` |
| Leaderless reads | `reads/`, `node/` | `node/ReadsTrace.tla` | `reads`, `lagging_reads` |
| Matchmaker Paxos | `matchmaker/`, `node/` | `node/MatchmakerTrace.tla`, `node/MatchmakerGridTrace.tla` | `matchmakers`, `reconfiguring` |
| Retirement and rejoin | `node/` | `node/RetireTrace.tla` | `retiring` |
| Matchmaker set handover | `handover/`, `handover-chain/`, `generations/` | `generations/HandoverTrace.tla` | `handover` |
| Truncation | `truncation/`, `truncation-reconfig/`, `node/` | `node/TruncationTrace.tla` | `compaction` |
| Disk faults (CTRL) | `node/` | `node/FaultsTrace.tla` | `disk_faults`, `disk_faults_three` |

`decree/` and `multi-paxos/` also have an inductive invariant `Inv`: TLC checks that one step
from a state where `Inv` holds goes to a state where it holds (every such state for the decree,
drawn states for Multi-Paxos). Proofs are the later steps of that work. `specs/tla/README.md` lists every
model with its checks and its controls.

## What it gives

- **The model and the code agree.** A step of the code that the model does not have fails the
  trace check. Then the step becomes a step of the model, with its own TLC check.
- **Rare paths run in CI.** A probe that no walk reaches gets a walk move or a flavor that bends
  the run to it, never a larger budget alone.
- **The checks are checked.** A check that a planted bug does not fail is a check that tests
  nothing.

## In paros

- [`specs/tla/`](https://github.com/PierreZ/paros/tree/main/specs/tla): the models, with
  `checks.txt` in each folder and `scripts/check-tla.sh` to run them.
- [`crates/sim/consensus-sim/`](https://github.com/PierreZ/paros/tree/main/crates/sim/consensus-sim):
  the walks (`tests/flavors.rs`), the trace checks (`tests/traces.rs`), the coverage gate
  (`tests/coverage.rs`) and the planted bugs (`tests/mutants.rs`). Its `AGENTS.md` has the rules.
- [`scripts/mutants.sh`](https://github.com/PierreZ/paros/blob/main/scripts/mutants.sh) and
  [`scripts/mutate-tla.py`](https://github.com/PierreZ/paros/blob/main/scripts/mutate-tla.py):
  the mutants of the core and of the models.
- The step-through diagrams of the [Paxos pages](../../paxos/) and the
  [course map](../../paxos/#course) show the TLC state count of each lesson's model.
