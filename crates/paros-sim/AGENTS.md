# paros-sim

The DST harness on top of `paros`: the moonpool `Process` adapters, the
deployment/role map, the fault world, the client workload, the audit and the
scripted corpus. Correctness lives here (audit + workload `check()`), never in
a trace scan. Every constant that shapes a campaign is a `pub const` in
`lib.rs`, not an environment variable.

## Map

- `roles.rs` the per-seed **deployment/role map** read off moonpool process
  groups: `ACCEPTOR_GROUP = "paros-node"`, `MATCHMAKER_GROUP = "paros-matchmaker"`,
  `PROXY_GROUP = "paros-proxy"` (#142), `REPLICA_GROUP = "paros-replica"` (#144; a
  replica speaks as `replica_node_id(rank)` = `NodeId(1000 + rank)`, outside every pool),
  `JOINER_GROUP = "paros-joiner"` (#189; a joiner joins as `joiner_node_id(rank)` =
  `NodeId(100 + rank)`, outside the genesis pool until the registry admits it),
  `Deployment`, `Role`.
- `shape.rs` `NodeShape::draw`: the per-logical-node knobs
  (`DriverTunables`, seam crash bias, wipe/loss percentages,
  `bootstrap_ranks`, `matchmaker_bootstrap_ranks`, the proxy take-back budget
  `proxy_take_back_resends` and the proxy's retention budget `proxy_round_resends`,
  the run's `QuorumPolicy`
  through `quorum_policy` — majority, a flexible split (#140) or an acceptor
  grid drawn from `grid_layouts` (#141, floor `rows >= 2`, `cols >= 2`)), drawn once
  per node per seed and reused across restarts; the run's `JournalPlan` (`journals`,
  #188: one to three journals, the held one) and the quarantine re-open knob;
  `system_journals` (#189: the seeded coin for the directory and the registry) and
  `SEED_COUNT` / `seed_ranks` (the ranks that host them); `MIN_BOOTSTRAP`, `config_floor`,
  `ROUND_TRIP_FLOOR_MS`.
- `process.rs` `NodeProcess::{chaotic, scripted_with}` (`ScriptedOptions`: a
  fixed bootstrap subset, the GC requests withheld; an acceptor runs
  `paros::run_journals` over `SimStores`, one `Seat` per journal — its config, its
  storage world, its audit world and port), `MatchmakerProcess`, `ProxyProcess` (runs
  `paros::run_proxy`; nothing durable, a kill reboots it empty), `ReplicaProcess` (runs
  `paros::run_replica` in a seam-crash recovery loop over its own **fault-free** disk,
  registered with `StorageWorld::note_replica` so the copy budget never counts it — a
  replica's record is never a copy an acceptor quorum needs), `IdleProcess`,
  `JoinerProcess` (#189: `run_journals` with no journal of its own and the `SystemPlan`,
  over `SimStores` whose created seats appear at runtime; quiet seats — system and created
  journals — sit on fault-free world stores outside the copy budget), `ContractSuiteWorkload` ·
  `lifecycle.rs` `ScriptedLifecycle` (the corpus's
  `FaultInjector`, registered on the main campaign too for the chain client's one lifecycle
  act — rebooting every member of a configuration it installed, #173; it drains for the whole
  run) · `hooks.rs` `BuggifyHooks<T>`: all `DriverHooks` methods,
  one `buggify_with_prob!` location each, the module-doc table of *enabled /
  consulted / fired / recovered* per hook.
- `client.rs` `ClientRuntime` (a workload's client-only moonpool-rpc
  runtime, driven on its own task and stopped when the handle drops; one
  `paros::NodeClient` per server; the corpus builds on it) · `state.rs` `published` (the
  get-or-publish of every per-iteration singleton on the `StateHandle`).
- `chain.rs` `ChainState` (the Chain-of-Blocks fold a journal client
  computes, #186) · `chain_workload.rs` `ChainWorkload` + `ChainConfig`
  (every field a `buggify_knob!`; the operation-id table `WRITE=0 …
  SET_LEADER=22`, `OP_COUNT`, the weight table, the reconfiguration shape
  rings) · `chain_workload/system.rs` the system-journal operations (#189,
  `CREATE_JOURNAL=17 … RETIRE_NODE=21`) and their read-back · `chain_workload/fold.rs` the client's `Fold` of the journal and
  the run's trim fence (every trim clamped below every folding client's
  cursor).
- `world/mod.rs` `StorageWorld` (the protocol-blind fake disk, budgets,
  parked identities, the replica disks kept outside the copy count) · `world/storage.rs` `DurableStorage` (`LogStorage` +
  write-path fault sites) · `world/node_store.rs` `NodeStore` (#187: an acceptor's
  store, the world's or the library's `JournalStorage` over `SimStorageProvider` on the
  seeds `shape::journal_store` draws; `LedgeredJournal` keeps the provisioning ledger in
  two steps and counts the disk's I/O faults for the one-crash-per-fault correlation) ·
  `world/rot.rs` boot-rot BUGGIFY sites, one per
  fault family · `world/matchmaker.rs` `DurableMatchmakerStorage`.
- `audit/mod.rs` `AuditWorld` (one per journal, `audit_world_for`; `world/`'s
  `storage_world_for` likewise, keyed by `state::journal_key`), `check_run`,
  `reach_once!` · `audit/journals.rs` the journal board and the non-interference
  oracles (#188) · `audit/system.rs` the system board (#189: fold agreement, id
  allocation, tombstones, the joiner gates) · `audit/state.rs`
  `AuditState` (per-transition protocol safety) · `audit/client.rs`
  `ClientHistory` (linearizability, sequential-client consistency) ·
  `audit/matchmaker.rs` `MatchmakerAudit`.
- `corpus.rs` the scripted workloads: `E1MaskWorkload`, `BareQuorumWorkload`,
  `DepartedStragglerWorkload`.

## Campaign constants (`lib.rs`)

`PROCESS_POOL_RANGE = 3..=6`, `MATCHMAKER_POOL_RANGE = 0..=5` (zero means the
plain Multi-Paxos deployment), `PROXY_POOL_RANGE = 0..=3` (zero means every
Phase 2 colocated), `REPLICA_POOL_RANGE = 0..=2`, `JOINER_POOL_RANGE = 0..=2` (#189), `CLIENT_COUNT_RANGE = 1..4`, `PLATEAU_SEEDS = 8`,
`CHAOS_DURATION_MS = 4_000`, `SMOKE_ITERATIONS = 50`, `COVERAGE_ITERATIONS = 1024`,
`CORPUS_CI_ITERATIONS = 64`,
`EXPLORATION_TIMELINES_PER_SEED = 8`. `chaos_surfaces()` is `Network(Swarm)`,
attrition scoped per group with `AttritionVictims::group`, and `BuggifyKnobs`;
`BitFlip` is masked off. Exploration runs in-process (`workers: 0`). Oracle
thresholds and `*_ITERATIONS` are never buggified.

Entry points: `explore`, `run_chain_seed`, `chain_seed_digest`,
`chain_seed_canary`, `chain_canary_hunt`, `chain_smoke`, `explore_chain_seed`,
`run_storage_contract_suite`, and the corpus family (`corpus_canonical_masks`,
`run_corpus_mask`, `corpus_hunt`, `run_bare_quorum_case`,
`run_departed_straggler_case`, ...).

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
  short-circuit a wiped boot in the process again. The same holds for a
  matchmaker (#183): its loss coin wipes the registry and the process boots it
  as an existing member for `run_matchmaker` to refuse.
- Spans are non-optional (process and workload lifecycles, the world's
  injections, the audit's gate checks).
