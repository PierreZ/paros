# seed-debugger memory

## Tooling
- A replay prints no trace. To get a full timeline without touching the repo: `git worktree add`
  a scratch copy, copy moonpool's git checkout next to it, add an `eprintln!` in
  `SimulationLayer::on_event` (and `record_sim_fault`), `[patch."https://github.com/PierreZ/moonpool"]`
  the four moonpool crates to the copy, build that worktree's `sim-paros-hunt`. Same draws, same red.
- Driver events carry no journal id: system journals (directory, registry) emit the same event
  names from the same node ips; a claim by the same client/generation has the same vhash.
- The parent may share the scratchpad: write replays into a private subdirectory.

## Failure shapes
- Membership-probe wedge (residual of #270): a reconfiguration registered at a matchmaker
  minority, then its leader dies. Old members' probes adopt it (outside), new members' probes
  close on the first quorum, which in the tail (fixed per-pair latencies) never contains the
  holder, so they keep the bootstrap (outside too). Nobody campaigns; every node skips its
  campaign as a non-member forever. Look for `membership_probe_closed member=false` on
  every node and `match_probed effective_round` differing across matchmakers.
- `AuditWorld::has_departure` is true on one matchmaker's registration, not on a removal that
  took effect; the late outage can strike mid-reconfiguration.
- Second probe-wedge variant (after the #270 re-probe fix): the new members learn the *old*
  configuration off a later campaign's `Prepare` (`learn_config` binds it to the campaign's
  ballot, above the minority reconfiguration's), so "a probe adopts only a strictly newer
  configuration" can never move them back. Meanwhile the old members adopted the minority
  reconfiguration through a probe or `StaleConfiguration`. The signature is two groups of
  `membership_probe_closed member=false`, each with a different `effective_round`, and the
  last `matchmaking_started` long before the tail. The cause is that `acceptors_since` mixes a
  campaign ballot (a belief) with a reconfiguration ballot (a fact).
- Patching moonpool to add a `tracing_subscriber::fmt` layer (feature `fmt`) in
  `SimulationLayer::install`, timed with `SimTime::new(handle)`, prints every event with sim
  time. Patch all seven git crates (assertions, buggify, core, explorer, journal, rpc, sim).
- Audit fold split: every path that raises `st.floor` must also call `prune_below_floor()`, or
  `cluster_min_floor()` moves past slots still in the live `decided` tally while
  `decided_below_floor` lacks them. An oracle that switches on `slot >= cluster_min_floor()` and
  reads only one map then reports a valid fact as red. The signature is a below-floor `proxy:`
  violation whose slot sits exactly one below a trim-jump `point` (`trimmed_to` did not prune).
- A CI sweep failure under forked workers is a timeline (seed + recipe), so `replay-main` of the
  printed seed can be green without a determinism leak. Run `replay-canary` to rule out a leak,
  then hunt for the message with a release `sim-paros-hunt` (about 1 s per seed, 16 s in debug).
