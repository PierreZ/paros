# paros-sim

The DST harness on top of `paros`: the moonpool `Process` adapters, the
deployment/role map, the fault world, the client workload, the audit and the
scripted corpus. Correctness lives here (audit + workload `check()`), never in
a trace scan. Every constant that shapes a campaign is a `pub const` in
`lib.rs`, not an environment variable.

## Map

- `roles.rs` the per-seed **deployment/role map** read off moonpool process
  groups: `ACCEPTOR_GROUP = "paros-node"`, `MATCHMAKER_GROUP = "paros-matchmaker"`,
  `Deployment`, `Role`.
- `shape.rs` `NodeShape::draw`: the per-logical-node knobs
  (`DriverTunables`, seam crash bias, wipe/loss percentages, lane count,
  `bootstrap_ranks`, `matchmaker_bootstrap_ranks`, the run's `QuorumPolicy`
  through `quorum_policy` — majority, a flexible split (#140) or an acceptor
  grid drawn from `grid_layouts` (#141, floor `rows >= 2`, `cols >= 2`)), drawn once
  per node per seed and reused across restarts; `MIN_BOOTSTRAP`, `config_floor`,
  `ROUND_TRIP_FLOOR_MS`.
- `process.rs` `NodeProcess::{chaotic, scripted, scripted_with_bootstrap,
  scripted_with_seam_crash}`, `MatchmakerProcess`, `IdleProcess`,
  `ContractSuiteWorkload` · `lifecycle.rs` `ScriptedLifecycle` (the corpus's
  `FaultInjector`) · `hooks.rs` `BuggifyHooks<T>`: all `DriverHooks` methods,
  one `buggify_with_prob!` location each, the module-doc table of *enabled /
  consulted / fired / recovered* per hook, and `ScriptedCrash` (#146): the
  corpus's one targeted seam crash, fired once per run, no draw.
- `chain.rs` `ChainState` (the Chain-of-Blocks application) ·
  `chain_workload.rs` `ChainWorkload` + `ChainConfig` (every field a
  `buggify_knob!`; the operation-id table `PROPOSE=0 … RETIRE=13`,
  `OP_COUNT`, the weight table, the reconfiguration shape rings).
- `world/mod.rs` `StorageWorld` (the protocol-blind fake disk, budgets,
  parked identities) · `world/storage.rs` `DurableStorage` (`NodeStorage` +
  write-path fault sites) · `world/rot.rs` boot-rot BUGGIFY sites, one per
  fault family · `world/matchmaker.rs` `DurableMatchmakerStorage`.
- `audit/mod.rs` `AuditWorld`, `check_run`, `reach_once!` · `audit/state.rs`
  `AuditState` (per-transition protocol safety) · `audit/client.rs`
  `ClientHistory` (linearizability, sequential-client consistency) ·
  `audit/matchmaker.rs` `MatchmakerAudit`.
- `corpus.rs` the scripted workloads: `E1MaskWorkload`, `BareQuorumWorkload`,
  `DepartedStragglerWorkload`, `SnapshotLifecycleWorkload`, `ChunkMaskWorkload`.

## Campaign constants (`lib.rs`)

`PROCESS_POOL_RANGE = 3..=6`, `MATCHMAKER_POOL_RANGE = 0..=5` (zero means the
plain Multi-Paxos deployment), `CLIENT_COUNT_RANGE = 1..4`, `PLATEAU_SEEDS = 8`,
`CHAOS_DURATION_MS = 4_000`, `SMOKE_ITERATIONS = 50`, `COVERAGE_ITERATIONS = 1024`,
`CORPUS_CI_ITERATIONS = 64`, `CHUNK_CORPUS_CI_ITERATIONS = 32`,
`EXPLORATION_TIMELINES_PER_SEED = 8`. `chaos_surfaces()` is `Network(Swarm)`,
attrition scoped per group with `AttritionVictims::group`, and `BuggifyKnobs`;
`BitFlip` is masked off. Exploration runs in-process (`workers: 0`). Oracle
thresholds and `*_ITERATIONS` are never buggified.

Entry points: `explore`, `run_chain_seed`, `chain_seed_digest`,
`chain_seed_canary`, `chain_canary_hunt`, `chain_smoke`, `explore_chain_seed`,
`run_storage_contract_suite`, and the corpus family (`corpus_canonical_masks`,
`run_corpus_mask`, `corpus_hunt`, `run_bare_quorum_case`,
`run_departed_straggler_case`, `run_snapshot_lifecycle_case`,
`chunk_corpus_canonical_masks`, `run_chunk_mask`, `chunk_corpus_hunt`, ...).

## Rules local to this crate

- moonpool macros only (`assert_always!` with a detail map, `assert_sometimes!`
  for outcomes, `reach_once!` for causes); never plain `assert!`; never reword
  a message; 512 slots and 256 buckets per campaign process; no slot, ballot,
  id, seed or hash as an identity.
- No seed constants, seed lists or seed-replay tests. `tests/sim.rs` is the
  smoke (a single seed converging, the storage contract, a same-seed digest
  replay, the canary pair, `chain_smoke(SMOKE_ITERATIONS)`); `tests/corpus.rs`
  walks the canonical mask tables with non-vacuous floors. Neither replays a
  witness; a seed there is either an arbitrary display seed or the input to a
  scripted case.
- Hooks are consulted from the node loop only; decisions a spawned task needs
  are carried to it.
- A wiped identity (lost promise) stays down because the **library** refuses
  its unformatted store (#147): the world keeps it parked for the budget and
  the composer only, and its provisioning ledger (`StorageWorld::provisioned`)
  is the operator's claim the process hands `run_node` as `BootKind`. Never
  short-circuit a wiped boot in the process again.
- Spans are non-optional (process and workload lifecycles, the world's
  injections, the audit's gate checks).
