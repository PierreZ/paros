---
name: adding-a-buggify-site
description: Add fault injection to paros the right way - an inline buggify_with_prob!/buggify_pick! at the line of paros that makes the choice, or a hint!("moment") where a crash is interesting (never a new DriverHooks method, Seam or sim wrapper, #294), a buggify_knob! tunable with a documented floor in ChainConfig or NodeShape (prong 2), a durability hint!, and the fired/recovery gates every site must pair with. Use when a rare state needs to become likely, when a constant should vary per seed, when adding a driver policy choice, or when a sweep gate never fires.
---

# Adding a BUGGIFY site

Three layers, nothing crosses them: moonpool owns environmental faults,
`paros-core` is never buggified (perturbed only through its public API), and
the driver's own decisions plus the harness's tunables are where paros plants
its sites. Pick the prong first.

**Hard rule (#294, 2026-10-09):** the site goes inline in the shipped code
(`paros`, `parosd`) at the line that makes the choice, never in a sim wrapper.
A choice is `buggify_with_prob!` or `buggify_pick!`; a moment where a crash is
interesting is `hint!("label").await`. Add no new `DriverHooks` method (the
`Seam`s are gone): prong 1 below describes the code being migrated
(`docs/analysis/simulation/production-fault-hints.md`).

## Prong 1: a driver decision → `DriverHooks`

For a timing or policy choice the provider-generic driver owns (skip a
resend, resign, hand off, drop a reply, hold a mailbox, stretch a tick):

1. Add a method to `DriverHooks` in `crates/paros/src/hooks.rs` with an honest
   contract in its doc: what the driver does when it answers `true`, and why
   that is always safe (`NoHooks` keeps the default `false`). If the core must
   expose the decision, add a method to `ColocatedNode` with the same honesty
   ("skipping is always safe; re-send is pure optimization"); the core gains
   no RNG and no flag.
2. Consult it in the driver **only where the answer can have an observable
   effect** (ask "skip the resend?" only when accepts are pending), trace the
   action that actually happened, and report it through the `Audit` port.
3. Implement it in `BuggifyHooks` (`crates/paros-sim/src/hooks.rs`) with its
   **own** `buggify_with_prob!` call site, so per-seed activation composes
   with every other site. Disruptive sites check the chaos cutoff and go quiet
   for the recovery tail.
4. **Consult only from the node loop, never from a spawned task.** A hook
   answer is a draw; a `spawn_task(..).detach()`ed task can outlive its run
   and shift the next run's stream (this broke the same-seed replay on CI
   once). Take the decision on the loop and carry it (the mailbox's
   `hold_next`/`reverse_next` flags are the pattern). The `H: DriverHooks`
   bound on `run_node` is deliberately not `Send + 'static` so the compiler
   catches the obvious version of this mistake.

## Prong 2: a tunable → `buggify_knob!`

For anything that shapes a run (a count, a window, a capacity, a rate):

- **Workload tunables** live in `ChainConfig::for_timeline`
  (`crates/paros-sim/src/chain_workload.rs`); **per-node driver tunables** in
  `NodeShape::draw` (`crates/paros-sim/src/shape.rs`), which draws once per
  logical node per seed and reuses the shape across restarts; **disk fault
  rates** in the injector's families (`world/injector.rs`); a cut mid-commit is a
  journal store's `hint!` rate, its copy budget in `world/cut.rs`.
- One knob is one `buggify_knob!(default, lo..hi)` call site: never a
  multiplier over a family, so a seed can be extreme in one dimension and
  ordinary in the next.
- **Document the floor next to the site**, and only add the knob where the
  extreme is a valid configuration: a peer queue that cannot hold one tick of
  traffic, or a one-message delivery batch, is a permanent partition wearing a
  knob's clothes (the driver timings floor at `ROUND_TRIP_FLOOR_MS`).
- Never buggify an oracle threshold (`DEPOSED_TICK_SLACK` in
  `audit/state.rs`, `PLATEAU_SEEDS` and `CHAOS_DURATION_MS` in `lib.rs`,
  `SETTLE` in `chain_workload.rs`) or a schedule
  ceiling (`*_ITERATIONS`); constants a correctness argument depends on
  (`MAX_TORN_TAIL`) are not tunables and say so where defined.
- A new production tunable is **born** as a `DriverTunables` field with a
  default in `crates/paros/src/driver/config.rs`, then drawn in `NodeShape`.

## Seams

Process-level attrition cannot crash between a write and its sync. The
drivers name those moments inline with moonpool's `hint!("label").await`
(#294): the node driver's "batch staged, not synced" and "batch durable, not
sent" (`driver/ready.rs`), the replica's "replica batch durable, not sent",
the matchmaker's "registration staged, not synced" and "registration
durable, reply not sent", and "reconfiguring prepare sent" (#260: a
reconfiguring candidate dies after its `Prepare`s left, so its campaign never
finishes). The seed's attrition regime decides whether the process dies
there. A new durability boundary is a new `hint!` at its own line (one
location each; sharing one stops the sweep from selecting the failure modes
independently), with a rate literal when the 5% default does not fit, and a
`moonpool_assertions::reachable!` on `Strike::Killed` as its fired gate:

```rust
let hinted = moonpool_buggify::hint!("batch durable, not sent");
if hinted.strike() == Strike::Killed {
    moonpool_assertions::reachable!("the driver crashes after sync and before sending a batch");
}
hinted.await;
``` If
the swarm cannot build the seam's precondition, make the precondition a
per-seed BUGGIFY draw (a `shape.rs` function, like `withhold_gc`) so the
campaign builds it; there is no scripted corpus to fall back on (#263).

## The four questions every site must answer

`BuggifyHooks`'s module doc keeps a table of *enabled / consulted / fired /
recovered* per hook. The rule for the **fired** gate: it sits wherever the
fact is reported (the audit callback for a hook the driver reports, an inline
`assert_reachable!` for one it only traces); the **recovery** gate is the
protocol outcome the fault exists to exercise, a `sometimes` in the audit.
Add the row when you add the hook. A perturbation never gets a `sometimes`
of its own.

Finally, run the sweep and confirm the new site fires and the recovery gate
saturates (`/sim-sweep`).
