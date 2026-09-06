---
name: simulation-driven-fix
description: The paros procedure for any suspected safety or liveness bug - state the invariant, make the scenario reachable with chaos and BUGGIFY, put the check where the fact arrives (audit, workload check(), storage callbacks), watch the sweep go red on the unfixed code, fix paros-core, watch it go green and saturate, then record the red-to-green result in the commit and the doc comment. Use whenever reading code suggests "this could choose two values", "this could lose a promise", "this could deadlock", whenever tempted to write a unit test for a distributed scenario, or when asked to fix a protocol bug.
---

# Simulation-driven fix

paros is simulation-first: the deterministic simulation plus the `paros-sim`
oracles are the source of truth for correctness. A potential bug you cannot
make the simulation reproduce is treated as **unproven** (usually an invariant
you missed preserves safety), and speculative defensive code for an
unreproduced claim is not added. The loop below is how a claim becomes a fix.

## 1. State the invariant

One sentence, in protocol terms: "at most one value is chosen per slot", "a
durable promise never regresses across restart", "a removed acceptor still
answers Phase 1 for ballots it took part in". If you cannot state it, you do
not yet have a bug.

## 2. Make the scenario reachable

The scenario needs a fault and an interleaving. Environmental faults (drop,
delay, reorder, partition, crash/restart, disk corruption) are moonpool's and
already ride the combined campaign; do not re-implement one at the protocol
layer. What you add is *likelihood*: a `DriverHooks` BUGGIFY location for a
rare-but-valid driver decision, or a `buggify_knob!` extreme for a tunable
(`/adding-a-buggify-site`). If the harness lacks a capability the scenario
needs (a durability seam, a fault the world cannot inject yet), build the
capability; do not downgrade to a unit test.

## 3. Put the check where the fact arrives

- a driver-observable transition → `paros_sim::audit` (add the `Audit`
  callback in `crates/paros/src/audit.rs` if the driver does not report it);
- a client-observable one → the workload's own history and `check()`;
- an application or storage fact → the storage layer's audit callbacks.

Never a scan over the trace (`/adding-an-audit-check`). Preserve existing
message strings; a reworded message is a new slot.

## 4. Go red

Run the sweep on the **unfixed** code (`/sim-sweep`): `cargo xtask sim run
paros-chain` for the campaign, or the hunt for volume. It must fail. If it
stays green after a proper budget, go back to step 2 (reach) or step 1 (the
invariant may hold for a reason you have not found). Keep the failing seed at
hand and replay it while you work.

## 5. Fix `paros-core` (or the driver)

The fix goes where the decision lives: a role in `paros-core` for a protocol
rule, `ColocatedNode` for wiring, the provider-generic driver for policy.
Never a sim-only path: the code you ship is the code you test. Add the hard
`assert!` that pins the invariant at the boundary it crosses
(`/changing-paros-core`).

## 6. Go green and saturate

The same sweep must pass **and** saturate (every gate fired, coverage
plateaued). A hunt of a few thousand seeds is the volume evidence; the
canary after any change to randomness or lifecycle.

## 7. Write it down where it stays true

The commit message records the invariant, the witness seed, and the red→green
result. The doc comment on the rule or oracle it proved load-bearing says the
same in one or two sentences, so the next reader knows the rule is not
decorative. Then let the seed go: it reproduces only this build.

A deterministic unit test may pin the *mechanism* afterward (a core
state-machine trap, a storage contract), written against the mechanism and
never against a seed; it never replaces step 4.
