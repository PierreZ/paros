# Simulation improvement plan

Status: Stage 0 complete; implementation and validation evidence are recorded here as the work
progresses. The source authority for Moonpool is
`$HOME/workspace/rust/moonpool` at `d7cfbd5686d7800d0573f93ef0eaba9fe6733da4`, the revision pinned by
this workspace. References written as `/workspace/...` in the session brief are interpreted as
`$HOME/workspace/...` on this machine.

## Stage-0 system map

| Question | Paros/Moonpool representation and decision |
|---|---|
| What is restarted when a server crashes? | Moonpool kills and recreates a `NodeProcess` from its process factory. The recreated adapter invokes the same provider-generic `paros::run_node` as production. The `RawNode`, driver loop, queues, connections, waiters, role, timers, and hook object are volatile. |
| What persists across a reboot? | Only state in the iteration's `StorageWorld`: durable `HardState`, accepted records, compaction floor, opaque application snapshot, and the Chain application state. Staged but unsynced consensus writes do not survive. Process fields are never recovery state. `prob_wipe` remains `0`; promise amnesia is out of scope. |
| What drives client operations? | A factory-created `ChainWorkload`, fresh for every timeline, using only `SimContext` providers. Its stable operation alphabet is `PROPOSE=0`, `PROPOSE_TO_NON_LEADER=1`, `COMPACT=2`, `READ_STATE=3`, and `PAUSE=4`. `.swarm_operations()` filters that alphabet without changing its IDs. |
| What is the independent expected result? | A small `BTreeMap<cmd_hash, Outcome>` with `Acked`, `Rejected`, or `Ambiguous`, plus the attempted `(client, seq)` and observed slot where known. It records submitted command hashes and terminal observations; it does not reimplement Paxos or predict leader/election state. Chain agreement is computed independently from application facts emitted by every replica. |
| What must never be transiently false? | One `(command hash, state hash)` per application index cluster-wide; contiguous and monotone application indices per node except an authenticated snapshot jump; every applied user command was submitted; existing Paxos agreement, promise, persistence, recovery, apply-gap, leadership, read, and compaction facts. These are cursor-based `Invariant`s where new work is added. |
| What local condition must never fail? | At the application seam: a normal apply advances exactly one position, never regresses, and its slot is no greater than the durable chosen prefix passed by the driver. Snapshot decoding is total and versioned, the snapshot boundary agrees with its application count, and installation never regresses application state. Stable `assert_always!` identities are used at the transition, not ordinary test assertions. |
| Which rare situations prove the test is effective? | Success after leader change; compaction taking effect; snapshot recovery; a gap-fill `Noop` application; ambiguous proposal reconciliation; each existing driver decision firing; a post-chaos live advance and convergence; increasing applied-count watermark; the bounded failover frontier; coarse role/fault/floor buckets. |
| Which dangerous but valid choices are too rare naturally? | The two independent fsync crash seams, skipping an optional pending-accept resend, voluntary leader resignation, the shortest valid election timeout, client-side abandonment of a result followed by identical retry, and workload knob extremes. All disruptive choices are disabled after the chaos cutoff. No choice is injected into `paros-core`. |
| Which operation families can suppress one another? | The five stable operation IDs above. If a swarm mask disables every operation, the workload falls back to the full alphabet. Each logical step consumes a fixed draw shape before mapping to enabled operations, targets, payload class, and pause/compaction parameters. |
| Which nondeterministic APIs does production code call? | Network, time, tasks, randomness, and shutdown enter through `P: Providers`; durable state enters through `NodeStorage`; driver rare-policy choices enter through `DriverHooks`. The sim adapter supplies `SimProviders`, `DurableStorage`, and `BuggifyHooks`. Simulated behavior must not use Tokio time, `std::time`, OS randomness, real sockets/files, unordered decision iteration, or background OS threads. |

## Application state and recovery boundary

`Ready::committed` currently flows from the shared driver only to tracing and proposal waiters. The
driver therefore needs an application port before a meaningful Chain workload can exist. The plan
is to extend the provider-generic `NodeStorage` contract with an idempotent application method that
accepts `(slot, command)` and to invoke it before acknowledging that command. The default
`MemStorage` remains a no-op application. `DurableStorage` implements the real Chain state and
persists it in `StorageWorld`; protocol behavior does not move into `paros-sim`.

The Chain state is:

```text
(applied_slot, applied_count, chain_hash)
chain_hash[n + 1] = FNV1a64(chain_hash[n].to_le_bytes || encode(command[n + 1]))
```

The encoding has a variant tag and includes all user bytes, or the `Truncate.up_to` slot, or the
`Noop` tag. `command_hash` is FNV-1a over that command encoding. Fixed-width little-endian fields
make the representation deterministic and dependency-free. The application snapshot has a version
byte followed by the three fixed-width fields. The extra `applied_slot` is recovery metadata; the
checked application value remains `(applied_count, chain_hash)`.

The sim storage makes apply idempotent by slot. A reboot starts with the stored application
snapshot/state, then replays retained chosen records from `applied_slot + 1` through the durable
chosen prefix before serving. This closes the after-sync/before-apply crash window without inventing
process state. A decided `Truncate` can only remove a prefix already below its own apply slot, so its
application state already covers the removed commands. `InstallSnapshot` atomically installs both
the consensus boundary and the opaque Chain bytes. Snapshot offers are materialized only when the
application snapshot covers the offered chosen boundary.

Every normal application emits the required constant tracing fact:

```rust,ignore
tracing::info!(target: "chain", index, cmd = %cmd_hash, state = %state_hash, "command_applied");
```

It also carries bounded `node`, `slot`, and `kind` fields for validity and local ordering. Snapshot
installation emits a separate application-boundary fact so the invariant can authenticate an index
jump rather than pretending the transferred prefix was locally replayed.

`READ_STATE` needs live node state, not an out-of-band peek at `StorageWorld`. A private method on
the existing internal node service will return the opaque `NodeStorage::snapshot()` bytes. This is
simulation/driver observability over the already-internal transport, not public client API or
`parosd` work. The workload decodes the Chain snapshot while ordinary paros applications remain
free to use any opaque bytes.

## Workload model and ambiguous outcomes

Each proposal owns a stable `(client, seq)` and random byte payload. Payload size is selected from
plain config with empty, small, default, and large extremes. An acknowledged commit is definitive.
An explicit non-leader response is `Rejected` for that attempt. Deadline, transport failure,
process crash, and abandoned response are `Ambiguous`: the workload never treats them as an abort.
It reconciles by retrying the identical `(client, seq, bytes)` and by observing the application
facts/live state. A conflicting retry result is a test failure. Final recovery verifies that every
`Acked` command appeared in `command_applied`; ambiguous commands may legally be absent or applied,
but any observed application must match the originally submitted bytes.

Paros does **not** currently promise permanent at-most-once application for a retried client command
across compaction plus snapshot recovery. The core rebuilds dedup information from the retained log,
while the opaque application snapshot contract does not encode generic client dedup history. The
workload therefore does not assert one application per command hash. It asserts per-slot/per-index
state-machine safety, applies an identical retry when resolving ambiguity, and records duplicates as
diagnostic facts. Adding permanent exactly-once semantics would require a separate product contract
and snapshot state, outside this session.

## Operation alphabet

| ID | Operation | Contract |
|---:|---|---|
| 0 | `PROPOSE` | Submit random bytes through the current leader hint/rotating endpoint; classify the result and retain its stable identity. |
| 1 | `PROPOSE_TO_NON_LEADER` | Prefer a node other than the current leader hint to exercise rejection/redirect and leader races. An accidental leader may legitimately commit. |
| 2 | `COMPACT` | Ask the current leader to decide `Truncate` up to a safely acknowledged slot. Record the control submission; aggressive frequency is a workload knob. |
| 3 | `READ_STATE` | Query one live node's opaque application snapshot through the internal service and compare it with prior observations at the same count. |
| 4 | `PAUSE` | Sleep through provider time for a knob-selected short/long interval, creating scheduling room without an environmental fault. |

The workload is one coordinated client initially. Multiple Moonpool workload clients are deferred:
the first finisher initiates shutdown, so a multi-client version needs an explicit shared completion
barrier and provides less value than first establishing the single-client oracle. Pipeline depth is
still tunable inside that workload. All decision-affecting maps are ordered.

## Oracle inventory

### New global safety oracle

`ChainAgreement` uses `TraceQuery::since` cursors and clears every cursor/map in `reset`.

- `assert_always!(..., "chain: one state per applied index")`: all nodes emitting an ordinary
  apply for index `N` agree on both command and resulting state.
- `assert_always!(..., "chain: applies are contiguous per node")`: a node advances one application
  index at a time; a validated snapshot-boundary fact may move its baseline forward.
- `assert_always!(..., "chain: applied command was proposed")`: every applied user command hash
  was previously submitted. `Noop` is protocol-generated; `Truncate` must match a compact request.

This oracle complements, and does not weaken or rename, the existing Paxos/persistence/recovery
oracles.

### Local safety assertions

- `"chain: apply does not outrun chosen prefix"`.
- `"chain: local application transition is contiguous"`.
- `"chain: snapshot state matches its boundary"`.
- `"chain: snapshot install does not regress state"`.

### Recovery and liveness

Final live RPC work occurs in `Workload::run`, before Moonpool aborts processes. Once the cutoff has
disabled driver perturbations and attrition, the workload records its pre-tail count, retries a
fresh proposal until the application advances, then reads every node until one `(count, hash)` is
observed everywhere or the recovery budget expires. The stable always identity is
`"chain: cluster converged after chaos"`. Every `Acked` command must have an observed application
index. `Workload::check` revalidates the retained model with Moonpool assertions; it does not attempt
live RPCs.

Moonpool source shows that `chaos_duration` stops attrition/custom injectors and heals current
partitions once, but does **not** disable provider network/storage chaos. Therefore “quiet tail” in
this campaign means no attrition and no paros/client BUGGIFY disruption; provider turbulence can
continue. Recovery uses retries and a bounded but generous simulated-time budget rather than
claiming a fault-free network.

### Exploration guidance

Stable, bounded signals planned for the Chain campaign are:

- `assert_sometimes!`: `"chain: proposal succeeds after leader change"`,
  `"chain: compact takes effect"`, `"chain: node recovers through snapshot install"`,
  `"chain: noop gap fill is applied"`, and
  `"chain: ambiguous proposal is reconciled as committed"`.
- `assert_sometimes_greater_than!`: `"chain: applied index watermark"`.
- `assert_sometimes_all!`: `"chain: failover completed"` with the bounded facts old leader gone,
  new leader elected, and client acknowledged.
- `assert_sometimes_each!`: `"chain: state frontier"`, identity limited to coarse role
  (`leader`/`follower`/`unknown`), fault regime (`calm`/`network`/`storage`/`attrition`), and floor
  relation (`at_or_above`/`below`/`unknown`); quality is applied count.

No slot, ballot, client, seed, request, or hash becomes an identity. The campaign stays within
Moonpool's 128 assertion slots, 256 total buckets, six displayed identity values, and four quality
values. Numeric and bucket guidance do not satisfy the adaptive gate by themselves, so boolean
sometimes contracts are deliberately evaluated repeatedly and the runner separately rejects all
coverage violations.

## BUGGIFY map

The existing five independent source locations remain stable. Coverage identities are repaired and
split so every decision has its own signal.

| Layer and location | Rare but valid situation | Paired sometimes identity | Safety/liveness oracle | Why it finds bugs |
|---|---|---|---|---|
| Driver, before-sync seam | A process dies with a staged durable batch before fsync | `the driver crashes before syncing a staged batch` | promise/recovery/Paxos/ChainAgreement | Proves no unsynced promise or accept is relied upon after reboot. |
| Driver, after-sync/before-send seam | Durable work survives but no peer observes its messages | `the driver crashes after sync and before sending a batch` | recovery/gap-fill/snapshot/ChainAgreement | Forces restart to re-derive durable progress rather than depend on volatile outbound work. |
| Driver tick, pending accepts | An optional retransmission beat is skipped | `the driver skips a pending accept re-send` | no-gap/convergence/ChainAgreement | Exercises liveness when repair work is sparse without dropping protocol messages. |
| Driver tick, leadership | A valid leader voluntarily resigns with work in flight | `the driver voluntarily resigns leadership` | leadership/P2b/read/ChainAgreement | Compresses failover races around client ambiguity and recovered accepts. |
| Driver timeout draw | The shortest valid election timeout is selected | `the driver selects the shortest valid election timeout` | leadership/read/convergence | Increases legitimate election overlap without changing core timing rules. |
| Workload policy | The client stops waiting, marks the outcome ambiguous, then retries the same identity | `chain: ambiguous proposal is reconciled as committed` | validity/ack reconciliation/ChainAgreement | Reaches the classic committed-but-reply-lost path without making a server lie. |
| Workload config | Empty/large payload, shallow/deep pipeline, sparse/aggressive compact, short/long pause and recovery budgets | bounded knob-specific sometimes signals where useful | ChainAgreement/snapshot/convergence | Biases seeds toward edge values while keeping configuration plain data. |

The proposed delay-before-apply and post-sync delay hooks are deferred. `drain_ready` is synchronous,
so adding provider time there would require an async signature and broad ordering changes before
evidence shows it is needed. Minimal-quorum message filtering, fabricated stale responses, partial
`Ready` processing, pretend fsync errors, snapshot delays, and protocol-layer message faults are
rejected because they either duplicate Moonpool environmental faults or violate the real contract.
Minimal-quorum progress is guidance only.

Every driver hook is consulted only when it can have an observable effect. `NoHooks` remains all
false. Driver hooks and deliberate client abandonment stop at the chaos cutoff. Adding/moving these
source lines invalidates recipes, so they are changed as one batch before exploration evidence is
captured.

## Chaos axes and validation order

The axes are added in this order, and each is required to pass alone before the next combination:

1. Baseline deterministic seeds with default provider timing.
2. `Chaos::Network(Random)`.
3. `Chaos::Storage(Random)` with storage failures propagated through the real storage port.
4. `Chaos::Attrition { max_dead: 1, prob_wipe: 0.0 }`.
5. Per-surface `Swarm` modes, first network, then storage, then attrition.
6. `.swarm_operations()` over the five stable workload IDs.
7. `Chaos::BuggifyKnobs` for provider and workload extremes.
8. The combined main Chain campaign.
9. Deterministic in-process exploration with `workers: 0` and an adaptive saturation gate.

If the current simulated storage adapter cannot safely expose provider storage faults without
breaking its atomic durable-world model, the storage axis will be recorded as blocked rather than
faked inside the protocol. All campaign reports gate `failed_runs`, `assertion_violations`,
`coverage_violations`, and `convergence_timeout`; saturation is mandatory for the full campaign but
not for one-seed smoke or replay.

## Exploration and determinism audit

Prerequisites already established: processes and workloads use factories; the builder has no
instance workload, `before_iteration`, or custom fault injector. Implementation must additionally
verify the same seed produces the same trace/report twice, all ordering decisions use ordered
collections, and no simulated path uses Tokio/std time or OS randomness.

The registered campaign will use `enable_exploration(ExplorationConfig { workers: 0, ... })` and
`until_coverage_stable(plateau, cap)`. `workers > 0` remains disabled because the standalone fork
boundary has not been audited. The oracle proof sequence is:

1. Temporarily plant a one-node Chain transition divergence.
2. Run a one-seed/small sweep and record a red seed for `ChainAgreement`.
3. Remove the plant, keep the oracle, and pass smoke, nextest, and the random campaign.
4. Replant the same divergence behind a stable exploration choice.
5. Run exploration and record `exploration.bug_recipes[0]`.
6. Construct a fresh builder with `.replay_timeline(seed, recipe)` and prove the same invariant
   fails.
7. Remove the plant permanently and rerun the full green/saturated campaign.

### Evidence log

| Claim | Seed/recipe/result |
|---|---|
| Deliberately planted divergence is caught | Pending Phase 3. |
| Fresh-builder recipe replay reproduces it | Pending Phase 5. |
| Same seed produces the same trace twice | Pending Phase 5. |
| Real bugs found and fixed | None yet. |
| Final Chain campaign is green and saturated | Pending final validation. |

## Source/documentation discrepancies and decisions

- Local Moonpool source is authoritative. `chaos_duration` does not disable provider network or
  storage chaos; recovery wording and budgets account for that.
- `buggify_init` currently ignores its `firing_prob` parameter; each call site uses the explicit
  macro probability instead.
- `Workload::check` errors are logged rather than reliably promoted to the run result, and processes
  are already aborted. Safety checks use Moonpool assertions and final live RPC checks occur in
  `run`.
- `set_debug_seeds` does not set iteration count. Replay forces one iteration.
- Adaptive saturation observes boolean contracts and selected code coverage; numeric/bucket
  guidance is valuable but does not close the gate. The runner must inspect coverage violations and
  convergence timeout explicitly.
- The initial Chain campaign remains a three-node cluster. Selecting exactly 3-or-5 members is a
  builder-level decision made before timeline BUGGIFY is initialized; pretending it is a workload
  knob would be dishonest. A separate five-node factory/campaign can be added after the three-node
  campaign saturates, without changing protocol code or operation identities.

## Validation ledger

The required order is format and clippy before each focused commit, then:

1. One seed/one iteration smoke.
2. Deliberate red oracle proof and green replay after removing the plant.
3. `cargo nextest run` using only `SMOKE_ITERATIONS` plus regression seeds.
4. Every chaos axis alone, then combined.
5. Exploration discovery and fresh-builder recipe replay.
6. `cargo check --target wasm32-unknown-unknown -p paros-core`.
7. `cargo check --no-default-features -p paros -p paros-sim` if features change.
8. `cargo xtask sim list` and the full Chain campaign, requiring green saturation and no
   convergence timeout.

Exact commands, seeds, recipes, saturation, frontiers, and any changed decisions will be appended
to the evidence log before the pull request is opened.
