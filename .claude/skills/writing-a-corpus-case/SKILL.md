---
name: writing-a-corpus-case
description: Add a scripted case to paros's CTRL corpus - a three-node (or the one four-node) cluster from scripted_builder, every fault a targeted injection through ScriptedLifecycle (moonpool fault_factory) or a corruption mask, an analytically known outcome per mask, a non-vacuous floor in the nextest test, and its entry point in paros_sim plus the hunt binary. Use when a storage-fault or recovery scenario has a closed-form expected outcome, when adding an E1 or chunk mask family, or when extending corpus.rs.
---

# Writing a corpus case

The corpus is the second axis of the harness: where the main campaign is
chaos under swarm, a corpus case is a **scripted** cluster with every fault a
targeted injection and an outcome you can derive by hand per mask (a slot
recovers, a slot waits for its lost custodian, a below-floor node heals
through a snapshot). It exists to make CTRL's per-slot corruption terrain
exhaustive where the swarm can only sample it.

## Shape

- `scripted_builder(nodes, bootstrap, matchmakers)` in
  `crates/paros-sim/src/lib.rs`: `NodeProcess::scripted()`, no swarm chaos, a
  long chaos window, and `fault_factory(ScriptedLifecycle)`
  (`lifecycle.rs`) whose crash/restart commands the workload issues through
  the shared `StateHandle`.
- Existing workloads in `corpus.rs`: `E1MaskWorkload` (a per-slot × per-node
  corruption mask over `CORPUS_SLOTS = 3`, mask space 512),
  `BareQuorumWorkload`, `DepartedStragglerWorkload` (the one four-node,
  one-matchmaker case: a spare and a prior configuration across a
  reconfiguration), `SnapshotLifecycleWorkload`, `ChunkMaskWorkload`.
  A new family is a new workload here, not a new process type.
- The seed **is** the input when the mask is drawn from it
  (`MaskSource::Seeded`), which is the one place a hard-coded seed is not a
  witness. A fixed mask (`MaskSource::Fixed`) is the canonical table the
  nextest test walks.

## Steps

1. Write the outcome table first: for each mask, what the analytic result is
   and why (which quorum still holds a clean copy, which floor stops which
   `Prepare`). The storage world's copy budget is computed over
   `shape::config_floor`; a case that exceeds it is not a corpus case.
2. Implement the workload: prime the log (`PRIME_BUDGET`), inject the mask
   through the world, script the lifecycle, then wait for the outcome inside
   `OUTCOME_BUDGET` with `WAIT_SETTLE`/`FLOOR_GRACE` — those thresholds are
   oracle judgement and are never buggified.
3. Assert the outcome with `assert_always!` and a detail map, report
   **non-vacuity** (the case actually exercised its terrain) so a vacuous
   run cannot pass silently, and gate reached outcomes with `sometimes`.
4. Add entry points in `lib.rs` (`run_<case>(seed)` and, for a mask family,
   `<case>_canonical_masks()`, `run_<case>_mask`, `<case>_hunt(n)`), a nextest
   test in `crates/paros-sim/tests/corpus.rs` with a non-vacuous floor, a
   `replay-<case>` command in `crates/paros-sim-runner/src/hunt.rs`, and, if
   CI must sweep it, a `gate_corpus` call in `src/main.rs`.
5. Run the family exhaustively once (`corpus_canonical_masks` style) and cite
   the result in the commit.

## Rules

- Every fault is targeted: no `Chaos::Network`/`Storage` swarm on a corpus
  builder; moonpool's `crash`/`restart` through `FaultContext` are the
  lifecycle primitives.
- The corpus registers no matchmaker group and draws no bootstrap except in
  the departed-straggler case; keep it that way unless the outcome table
  needs a reconfiguration.
- The same `AuditWorld` judges corpus runs; do not add a check that only a
  corpus case can see unless it is an application or storage fact reported
  through the storage layer's audit callbacks.
