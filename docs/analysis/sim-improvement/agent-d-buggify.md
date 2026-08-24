# Agent D: BUGGIFY candidate hunt

Read-only audit calibrated against `/Users/pierrezemb/workspace/cpp/foundationdb`.

## FoundationDB placement patterns

The local source shows all five intended patterns:

- minimal work: buggified limits/concurrency/batches collapse to one
  (`fdbserver/core/ServerKnobs.cpp` and
  `fdbserver/consistencyscan/ConsistencyScan.cpp`);
- forced error handling: real error conditions are widened with
  `real_error || buggify(...)` in commit proxy/storage/status paths;
- emphasized concurrency: independent delays surround coordination and TLog
  commit/replacement boundaries (`CoordinatedState.cpp`, `TLogServer.cpp`);
- knob spikes: batch sizes, concurrency, queues, timeouts, and intervals are
  driven to valid extremes in `ServerKnobs.cpp`;
- damage control: disruptive delays/failures are suppressed under
  `speedUpSimulation`, and recovery failure injection is time-bounded.

The common rule is to force a valid branch or scheduling boundary, never to
corrupt internal protocol truth.

## Existing locations: keep and repair coverage

Paros already has five independent Moonpool locations (two source lines behind
`crash_at` plus three policy hooks).  Keep them all:

| Location | Situation | Main oracles | Paired sometimes identity |
|---|---|---|---|
| `persist_writes`, before sync | staged batch is lost before fsync | recovery, promise, ChainAgreement | `the driver crashes before syncing a staged batch` |
| `drain_ready`, after sync/before send | durable batch is never announced | recovery, gap fill, snapshot, ChainAgreement | `the driver crashes after sync and before sending a batch` |
| tick, pending-accept resend | optional repair work is skipped | GapFill, NoGaps, ChainAgreement | `the driver skips a pending accept re-send` |
| tick, leader resignation | failover with in-flight work | leadership, P2b, linearizability, ChainAgreement | `the driver voluntarily resigns leadership` |
| election-timeout draw | shortest valid timeout | election/read fencing/convergence | `the driver selects the shortest valid election timeout` |

The current harness lacks a before-sync coverage signal and combines skip and
resign under one vague property.  Split them.  All current locations correctly
turn off at the chaos cutoff and `NoHooks` stays inert.

## Ranked new candidates

### P1: client-side ambiguous cancellation and reconciliation

Submit a stable `(client, seq)`, stop waiting before learning the result, then
retry the identical request and/or inspect state.  This is the honest form of
“timeout after commit”; never make the server return a false
`committed = false`.  Pair with
`chain: an ambiguous proposal is reconciled as committed`; ChainAgreement,
applied-command validity, AppliedAck, and final ack reconciliation check the
consequence.  This belongs to workload policy.

### P1: delay application of a durable chosen batch

A provider-generic driver hook consulted only for nonempty committed work can
pause before application.  It stresses ack-after-apply, read fencing, replay,
chain contiguity, and convergence.  Production remains zero-delay and the sim
hook turns off before recovery.  Signal:
`chain: application of a chosen prefix is deliberately delayed`.

### P1: run-shaping configuration extremes

Make these plain fresh-timeline configuration data rather than core branches:

- cluster size 3/5;
- command sizes empty/small/large;
- request count and pipeline depth 1/high;
- sparse/aggressive compaction;
- short/normal read-confirmation expiry.

Pair with bounded signals such as `chain: five-node majority is exercised`,
`chain: command size extremes`, `chain: singleton pipeline is exercised`,
`chain: concurrent proposal pipeline is exercised`, `chain: compact takes
effect`, and `chain: a read-index request expires before confirmation`.

### P2: scheduler pause after sync/before send

An optional hook may delay a nonempty durable outbound batch without selecting
or dropping messages.  It is defensible only as scheduler preemption and must
preserve persist-before-send.  Signal:
`chain: a durable outbound batch is delayed before send`.

### Guidance, not injection

- Reach exactly one minimal Paxos quorum through Moonpool faults; do not filter
  driver messages.  Signal `chain: a value is chosen by exactly one minimal
  quorum`.
- Observe the existing tick-before-step-down ordering as a resigning leader's
  final outbound beat before inventing “one extra beat” behavior.
- Batch size one is a driver config candidate, not a protocol mutation.

## Rejections and layer ownership

- Sending Prepare/Accept only to an alleged “alive quorum” duplicates network
  loss; the driver has no authoritative alive set.
- Processing only part of a `Ready` after `advance` violates/redefines the
  obligation contract.
- Fabricating stale `Promise`/`Accepted` duplicates is a protocol nemesis;
  authentic duplicates must arise from resend/transport behavior.
- Pretending fsync failed belongs to the storage provider.  The existing
  after-sync seam already covers “durable but not sent.”
- Delaying between `ready()` and `advance()` creates no concurrency because the
  guard owns the unique node borrow.
- Snapshot-install delay is storage/provider latency, not a protocol hook.
- Heartbeat behavior stays unbuggified in `paros-core`; vary driver timing only.

## Damage control

Driver perturbations, deliberate client cancellation, pauses, and aggressive
compaction stop before final reconciliation.  Attrition is bounded to one dead
node and `prob_wipe` remains zero.  If traces show repeated disruptions crowding
out useful work, add a harness cooldown/budget rather than weakening safety
oracles.

## Trimmed implementation order

1. Repair coverage identities for the five existing locations.
2. Add ambiguous client cancellation/reconciliation.
3. Add delayed application only if the shared-driver implementation can keep
   ordering explicit.
4. Buggify plain cluster/workload/read-expiry configuration.
5. Add minimal-quorum and resigning-final-beat guidance without new protocol
   hooks.
6. Consider a post-sync/pre-send pause only if earlier guidance shows the seam
   remains shallow.

