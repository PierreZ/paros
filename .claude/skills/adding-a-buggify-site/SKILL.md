---
name: adding-a-buggify-site
description: Add fault injection to paros the right way - a DriverHooks method with its BuggifyHooks call site (prong 1, the driver's rare-but-valid decisions, consulted only from the node loop), a buggify_knob! tunable with a documented floor in ChainConfig or NodeShape (prong 2), a durability Seam, and the fired/recovery gates every site must pair with. Use when a rare state needs to become likely, when a constant should vary per seed, when adding a driver policy choice, or when a sweep gate never fires.
---

# Adding a BUGGIFY site

Three layers, nothing crosses them: moonpool owns environmental faults,
`paros-core` is never buggified (perturbed only through its public API), and
the driver's own decisions plus the harness's tunables are where paros plants
its sites. Pick the prong first.

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
  rates** in `world/storage.rs` and `world/rot.rs`.
- One knob is one `buggify_knob!(default, lo..hi)` call site: never a
  multiplier over a family, so a seed can be extreme in one dimension and
  ordinary in the next.
- **Document the floor next to the site**, and only add the knob where the
  extreme is a valid configuration: a peer queue that cannot hold one tick of
  traffic, or a one-message delivery batch, is a permanent partition wearing a
  knob's clothes (the driver timings floor at `ROUND_TRIP_FLOOR_MS`).
- Never buggify an oracle threshold (`DEPOSED_TICK_SLACK`, `PLATEAU_SEEDS`,
  `CHAOS_DURATION_MS`, `SETTLE`, `WAIT_SETTLE`, `FLOOR_GRACE`) or a schedule
  ceiling (`*_ITERATIONS`); constants a correctness argument depends on
  (`MAX_TORN_TAIL`) are not tunables and say so where defined.
- A new production tunable is **born** as a `DriverTunables` field with a
  default in `crates/paros/src/driver/config.rs`, then drawn in `NodeShape`.

## Seams

Process-level attrition cannot crash between a write and its sync. The
`Seam` enum in `crates/paros/src/hooks.rs` names the eight points the driver
asks `crash_at(seam)` at (`BeforeSync`, `AfterSyncBeforeSend`,
`AfterApplyBeforeSync`, `AfterBootReplayBeforeSync`, `BeforeChunkSync`,
`AfterChunkRestoreBeforeSync`, `MatchBeforeSync`, `MatchAfterSyncBeforeReply`).
A new durability boundary gets a new variant and its own location in
`BuggifyHooks`; sharing one location stops the sweep from selecting the
failure modes independently. If the swarm cannot build the seam's
precondition (it took #146 to visit `AfterChunkRestoreBeforeSync`), the
corpus scripts it: `ScriptedCrash` answers `crash_at` for one named seam
once per run, and the corpus case asserts the crash fired.

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
