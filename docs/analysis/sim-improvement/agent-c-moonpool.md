# Agent C: Moonpool API audit

Authority: `/Users/pierrezemb/workspace/rust/moonpool` at
`d7cfbd5686d7800d0573f93ef0eaba9fe6733da4`, matching paros's Cargo pin.

## Builder and lifecycle

`SimulationBuilder` is defined in
`crates/moonpool-sim/src/runner/builder.rs:208`.  Relevant current APIs are:

- `processes(count: impl Into<ProcessCount>, factory)` and
  `cluster(LocalityConfig, factory)`.
- `workload(instance)`, `workload_factory(|| Box<dyn Workload>)`, and
  `workloads(WorkloadCount, |index| Box<dyn Workload>)`.  Only the factory forms
  reconstruct fresh driver state and are accepted by exploration.
- `invariant`, `enable_chaos`, `chaos_duration`, `set_iterations`,
  `until_coverage_stable`, `set_debug_seeds`, `replay_timeline`,
  `swarm_operations`, and feature-gated `enable_exploration`.

Exploration rejects instance workloads, `before_iteration`, and custom
`fault(...)` objects.  `ExplorationConfig` fields are `workers`,
`max_runs_per_seed`, `branching_factor`, `max_frontier`, and `max_recipe_len`;
defaults are `0, 512, 4, 1024, 64`
(`crates/moonpool-explorer/src/controller.rs:73`).  `workers=0` is deterministic,
in-process exploration.

`Workload` has `setup`, `run`, and `check`
(`runner/workload.rs:33-62`).  Setups barrier before concurrent runs.  The first
workload completion cancels shared shutdown, although Moonpool waits for all
handles.  Processes are aborted before `check`, so final live RPCs belong in
`run`.  `check` errors/panics are logged rather than reliably reflected in the
workload result; correctness there must also use Moonpool assertions.

`Invariant::{observe,reset}` is run after simulation steps.  `TraceQuery` has
`len`, full `snapshot`, and cursor-advancing `since(name, &Cell<usize>)`.
Every stateful invariant must reset both cursor and tracked state.

## Chaos semantics

`Chaos` is `Network(ChaosMode)`, `Storage(ChaosMode)`,
`Attrition { config, mode }`, or `BuggifyKnobs`; modes are `Random` and `Swarm`.
`BuggifyKnobs` perturbs only explicitly enabled provider surfaces.

The source contradicts a strong reading of the guide's “quiet tail”:
`chaos_duration` bounds attrition and custom injectors, cancels them, and heals
current partitions once, but provider-level network/storage fault configuration
remains active for the whole timeline (`runner/orchestrator.rs:723-759`,
`814-850`).  A post-cutoff recovery check can disable paros hooks and avoid
attrition, but it is not a fault-free network/storage interval.

Even no explicit network surface uses `NetworkConfiguration::default`, which
contains timing variation, connect failures, partial I/O, rare close/corruption,
clock drift, and buggified delay.  Default storage latency is active but storage
fault probabilities are zero.

## Seeds, saturation, replay, and report

The builder defaults to `until_coverage_stable(10, 1000)`.  Mandatory adaptive
completion includes observed boolean sometimes/reachable/sometimes-all slots
plus the selected plateau signal.  Sancov edges are used when available;
otherwise satisfied boolean slots are the signal.  Numeric sometimes and
`sometimes_each` buckets do not block saturation, so a strict campaign must
also reject nonempty `coverage_violations`.

`set_debug_seeds` supplies seeds but does not change iteration count.
`replay_timeline(seed, recipe)` forces one iteration and installs recipe
breakpoints after `SimWorld` construction.  Recipes are
`Vec<(rng_call_count, new_seed)>`.

`SimulationReport` (`runner/report.rs:161`) contains iterations and run counts,
metrics, `seeds_used`, `seeds_failing`, assertion results/details,
`assertion_violations`, `coverage_violations`, optional exploration report,
`bucket_summaries`, `convergence_timeout`, and optional `saturation`.
Coverage misses/timeouts do not increment `failed_runs`; binaries must gate
them explicitly.  Explorer failures appear in
`exploration.bug_recipes: Vec<BugRecipe { seed, recipe }>`.

## Assertions and table budgets

Exact macro families are:

- `assert_always!`, `assert_always_or_unreachable!`, `assert_sometimes!`,
  `assert_reachable!`, `assert_unreachable!`;
- four always numeric comparisons and four matching sometimes numeric
  comparisons (`greater_than`, `greater_than_or_equal_to`, `less_than`,
  `less_than_or_equal_to`);
- `assert_sometimes_all!` and both identity-only and identity-plus-quality forms
  of `assert_sometimes_each!`.

Messages are the stable property identity.  Tables allow 128 assertion slots
and 256 total `sometimes_each` buckets.  At most six identity values are
displayed (all still affect hashing) and four quality values are packed from
their low 16 bits.  Exhaustion silently drops guidance.  Slot keys are 32-bit
FNV message hashes, not call sites or kinds, so message reuse merges properties.

`assert_sometimes_all` guides on frontier growth and novel truth combinations.
Numeric guidance records improvements in comparison distance.
`assert_sometimes_each` supplies discoveries/quality but has no “all buckets
visited” final contract.

## BUGGIFY and operation swarm

`buggify!()` uses per-call probability 0.25;
`buggify_with_prob!(p)` uses `p`; `buggify_knob!(default, lo..hi)` returns the
default unless its location is activated and fires.  Builder activation is 0.5
per `file:line`, cached for the timeline.  Moving/adding locations changes
identity/RNG consumption and invalidates recipes.

`buggify_init(activation_prob, firing_prob)` has a source-level discrepancy:
the second parameter is ignored.  Per-call firing is always controlled by the
macro argument (`chaos/buggify.rs:15-118`).

`swarm_op_enabled(u8)` is a pure seed/ID hash.  Without `.swarm_operations()` it
is always true; with it, each ID is independently enabled at 50% without
consuming simulation/config RNG.  Workloads must handle the all-disabled mask
and should remap a fixed number of RNG draws into the enabled subset.

`SIM_FAULT_EVENT_NAME` is `"sim_fault"`.  Its stable `kind` values include
process shutdown/kill/restart, partition create/heal, directional partitions,
random close, bit flip, storage read/write/sync fault, storage crash, and
storage wipe (`chaos/fault_events.rs:36-199`).

## Tested examples and discrepancies

- `tests/exploration/tests.rs`: factory exploration and fresh-builder recipe
  replay.
- `examples/src/dungeon.rs`: fixed-draw operation swarming and bounded guidance.
- `examples/src/tonic_grpc.rs`: provider-generic transport and legitimate
  application-level Buggify failures.
- `examples/src/topology.rs`: locality and provider timeouts.
- `tests/chaos/swarm.rs`: Buggify knobs only affect enabled surfaces.

Other documentation mismatches: wholly unexecuted runtime assertion sites are
unknown (not reported as unreached), one workload-design example uses stale
`.workloads`/`.run().await` syntax, and `until_coverage_stable` completion is
narrower than some prose implies.

