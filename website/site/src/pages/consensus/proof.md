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

`paros-consensus` uses both, and links them. A TLA+ model comes before the code of each flavor.
Then three judges run on the code that ships: seeded walks with faults on, the trace check of each
walk against its model, and a coverage gate over every `probe!` of the core. Mutants test the
judges. Each technique has its page in Engineering:
[TLA+ models](../../engineering/tla/), [simulation](../../engineering/simulation/) and
[mutants and assertions](../../engineering/mutants/). The [safety case](../../engineering/) lists
each claim of the core with the model, the trace spec, the walk and the mutants behind it, and the
claims that are still open.

## Each flavor and its judges

A model folder is in `specs/tla/`. A trace spec is the TLA+ module that a walk is checked against.
A walk is a `Flavor` of `consensus-sim` (`cluster/flavor.rs`), or its own walk where named.

| Flavor | Model | Trace spec | Walks |
|---|---|---|---|
| Single decree | `decree/` | `decree/DecreeTrace.tla` | the decree walk (`src/decree.rs`) |
| Multi-Paxos on a majority, a grid, partitions | `multi-paxos/`, `node/` | `node/MultiPaxosTrace.tla` | `multi_paxos`, `multi_paxos_five`, `grid`, `partitions` |
| Leader handoff | `leader-handoff/`, `node/` | `node/LeaderHandoffTrace.tla` | `handoff` |
| Proxy leaders | `proxy-leader/`, `node/` | `node/ProxyLeaderTrace.tla` | `proxies`, `proxies_on_grid`, `compartments` |
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
drawn states for Multi-Paxos). TLAPS proves both at every size (`decree/DecreeProof.tla`,
`multi-paxos/MultiPaxosProof.tla`). `specs/tla/README.md` lists every
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
