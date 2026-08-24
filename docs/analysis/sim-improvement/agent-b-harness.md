# Agent B: current `paros-sim` harness

Read-only audit against the repository at `0545f84`.

## Process and durable world

`NodeProcess` derives a stable `NodeId` ordering from sorted topology IPs and
runs the shared `paros::run_node` with `SimProviders`, `DurableStorage`, and
`BuggifyHooks` (`crates/paros-sim/src/node.rs:30-106`).  A `StorageWorld` in the
iteration `StateHandle` survives process reboot.  Per-node disks retain only
`HardState`, accepted records, and compaction floor (`node.rs:122-141`).

Writes stage locally and only `MustSync::Sync` flushes them into the durable
world (`node.rs:143-266`).  Snapshot bytes are not application state:
`snapshot()` returns a boot-time `MemStorage` marker and `install_snapshot`
ignores its bytes (`node.rs:275-296`).  Reboot re-derives protocol/applied trace
state from retained consensus records; there is no application state machine.

## Existing workload

The only workload is `ProposeClient` (`crates/paros-sim/src/workload.rs:101`).
It chooses sequential, pipelined, or quiet mode from config RNG.  It uses fixed
eight-byte sequence payloads, cycles proposals and reads across endpoints until
a one-second deadline, asks for compaction after acknowledged writes, and ends
with an eight-second settle period.

Fixed harness values are three nodes, 12 requests, 1 s request timeout, 20 ms
operation gap, 4 s chaos window, and 8 s settle tail
(`crates/paros-sim/src/lib.rs:40-57`, `181-218`).

After an outer proposal timeout the workload emits `client_failed` and forgets
the request.  It keeps no `Acked | Rejected | Ambiguous` model and performs no
final reconciliation.  A late commit is intentionally unconstrained by the
current linearizability oracle.

## Oracles and assertion identities

Three recorder invariants (`TimelineRecorder`, `ProtocolRecorder`, and
`RecoveryRecorder`) and thirteen checking invariants are registered.  All use
full `snapshot` rescans; none uses `TraceQuery::since` or implements `reset`.

Existing always properties include:

- client issue-before-terminal ordering and echoed sequence IDs;
- read/write linearizability watermarks;
- committed-ack-after-apply;
- Paxos agreement, monotone promises, accept-below-promise exclusion, one
  command per `(slot, ballot)`;
- recovered accepted-value stability;
- contiguous apply except snapshot/floor jumps;
- monotone leadership and leader ballot/promise relation;
- no persistent chosen gap at quiescence;
- no persist/recovery below the compaction floor.

The exact core property IDs are in `crates/paros-sim/src/oracle.rs:681-1855`,
including:

- `"at most one value is ever chosen for a slot"`
- `"a node's promised ballot never decreases"`
- `"a restart never changes a pre-crash accepted value for a slot"`
- `"a node's applied prefix advances one slot at a time (a forward jump only at the compaction floor or a snapshot install)"`
- `"a committed write ack names a slot the acking node had already applied"`
- `"a quiesced cluster holds no chosen slot above its applied prefix (an election left an undecided hole)"`
- `"a node never persists an accept below its compaction floor"`
- `"a truncated record is never recovered on boot (the log stays bounded)"`.

There is no Chain Agreement/validity oracle, no application state, and no
watermark/frontier/bucket guidance.  `ConvergenceOracle` supplies reachability
only.  `run_seed` performs final convergence out of band with ordinary
`assert_eq!`; the adaptive sweep omits that final check because Moonpool has no
end-of-run invariant hook (`lib.rs:291-306`).

Driver-hook coverage includes after-sync crash, snapshot outbound path,
shortest election timeout, accept-resend skip, and resignation.  The
before-sync seam has no paired coverage signal.

## Trace schema

Workload facts are `client_workload_mode`, `client_issued`,
`client_acknowledged`, `client_failed`, `client_read_issued`,
`client_read_acknowledged`, and `client_read_failed`.

Driver facts (`crates/paros/src/driver.rs:111-258`) are `node_tick`,
`node_state`, `persist`, `recovered`, `booted`, `crashed`,
`accept_resend_skipped`, `leadership_resigned`, `election_timeout_extreme`,
`synced`, `value_chosen`, `msg_sent`, `msg_received`, `leader_elected`,
`log_applied`, `compacted`, `snapshot_installed`, `snapshot_offered`,
`election_gap_filled`, `chosen_gap`, `prepare_below_floor`, and
`propose_dedup_ack`.

## BUGGIFY, chaos, and campaign wiring

Five independent `buggify_with_prob!` sites live in `BuggifyHooks`
(`crates/paros-sim/src/node.rs:314-349`): before-sync crash (0.03),
after-sync/before-send crash (0.03), skip accept resend (0.95), leader resign
(0.004), and shortest election timeout (0.5).  Each is disabled after the 4 s
cutoff.  There are no `buggify!`/`buggify_knob!` call sites.

Every current run enables swarm network faults plus single-node crash/restart
attrition (`max_dead=1`, `prob_wipe=0`, recovery 200..900 ms).  Storage chaos,
`Chaos::BuggifyKnobs`, operation swarming, and exploration are absent.

Builders already use process/workload factories and have no
`.workload(instance)`, `before_iteration`, or custom fault injector, so the
documented exploration blockers are absent.  `TraceQuery::since`,
`replay_timeline`, and `enable_exploration` are unused.

`SMOKE_ITERATIONS=50`, `COVERAGE_ITERATIONS=64`, and `PLATEAU_SEEDS=64`.
The regression corpus is `0, 99, 42, 7, 12345, 5,
18153519926117387038, 11316277997507784505, 286172402316494352, 53, 11,
6156, 283` (`lib.rs:58-180`).

The xtask registry contains `paros-sim-runner` with `SANCOV_CRATES=paros_core,paros`
(`crates/xtask/src/main.rs:26`).  The runner prints saturation but exits nonzero
only for run or assertion failures; `convergence_timeout` and coverage
violations do not currently fail it (`crates/paros-sim-runner/src/main.rs:23-63`).

## Highest-impact gaps

1. No provider-generic application apply/snapshot state.
2. No ambiguity model or final timed-out-write reconciliation.
3. No cursor-based chain invariant or final live chain convergence check.
4. No operation swarm or numeric/frontier/bucket exploration guidance.
5. No storage/Buggify-knob axis and no explorer/recipe replay.
6. The campaign does not enforce saturation.
7. The before-sync seam lacks an independent sometimes signal.

