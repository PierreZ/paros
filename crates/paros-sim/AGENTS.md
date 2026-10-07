# paros-sim

The DST harness on top of `paros`: moonpool `Process` adapters, the deployment/role map, the
fault world, the one client workload, the audit and the scripted corpus. Stack: `paros-core` ←
`paros` ← **`paros-sim`** ← `paros-sim-runner`. Correctness lives here (audit + workload
`check()`), never in a trace scan. Doctrine: root *Simulation rules*, *Turbulence layers*,
*Audit, correctness, assertions, spans*.

## Map

- `lib.rs` → builders (`chain_builder`, `scripted_builder`), `chaos_surfaces()`, the campaign constants, the entry points.
- `roles.rs` → `Deployment`, `Role`, group names, `joiner_node_id`, `replica_node_id` → the per-seed role map.
- `shape.rs` → `NodeShape`, `QuorumPolicy`, `JournalPlan` → every per-seed and per-node draw (below).
- `process.rs` → `NodeProcess::{chaotic, scripted_with}`, `MatchmakerProcess`, `ProxyProcess`, `ReplicaProcess`, `JoinerProcess`, `IdleProcess`, `ContractSuiteWorkload` → one `Seat` per journal over `SimStores`.
- `lifecycle.rs` → `ScriptedLifecycle` → the `fault_factory` injector (corpus kills; the chain client's successor reboot, #173).
- `hooks.rs` → `BuggifyHooks<T>` → every `DriverHooks` method, one `buggify_with_prob!` each; module table of enabled/consulted/fired/recovered.
- `client.rs` → `ClientRuntime`, `ChainClient = paros::client::Client<SimProviders>` → a workload's client-only RPC runtime.
- `state.rs` → `published`, `journal_key` → get-or-publish of per-iteration singletons on the `StateHandle`.
- `chain.rs` → `ChainState` → the Chain-of-Blocks fold a journal client computes (#186).
- `chain_workload.rs` → `ChainWorkload`, `ChainConfig` → the op alphabet, weights, reconfiguration shape rings.
- `chain_workload/rpc.rs` → `CallLog` → the library's `CallObserver` (the history), per-answer oracles, one-attempt calls, the retry-identity oracle (`open_write` / `close_write`).
- `chain_workload/races.rs` → races 1 and 2 of #205 (`burst`, `ack_race`).
- `chain_workload/foreign.rs` → the cross-tenant attack (#247): a `Write`, `Truncate` or `SetLeader` under another tenant's journal or an identifier nobody serves, refused and never applied (`AuditWorld::note_foreign`).
- `chain_workload/fold.rs` → the client's fold and the trim fence · `chain_workload/system.rs` → ops 17–21, 23 and 24, and their read-back (the registry's through a checkpoint `Folder`); `Announce`, the audit observer for library writes to system journals and the logger of every attempt at them into the control journals' shared history (#247, `rpc::control_attempts`).
- `chain_workload/fleet.rs` → `FleetOps` → ops 25 and 26 (#229): `init`'s fleet half and tenant create/remove through `FleetSession`, the crash-at-a-step, target-kill (#247), changed-identity and fleet-tenant checkpoint-crash shapes, a reachable per `Stage`, and the check that the fleet directory equals the cell's tenant list — mid-run when both folds are one instant's, and on every run over the final folds, with the recovery tail's control-plane liveness (`settle`, `final_check`).
- `world/mod.rs` → `StorageWorld`, `storage_world_for` → fake disk, copy budget, parked ids, provisioning ledger, reconfiguration ledger.
- `world/storage.rs` → `DurableStorage` (write-path fault sites) · `world/matchmaker.rs` → `DurableMatchmakerStorage` · `world/rot.rs` → boot-rot sites.
- `world/node_store.rs` → `NodeStore`, `LedgeredJournal`, `PowerCut` → world store or `JournalStorage` on `SimStorageProvider` (#187); `PowerCut` is the BUGGIFY site that cuts a node's power mid-commit (moonpool `SelfCrash`), the cut drawn inside the node's last commit's duration.
- `audit/mod.rs` → `NodeAudit`, `reach_once!` · `audit/world.rs` → `AuditWorld`, `audit_world_for`, `check_run`, `check_final_convergence`.
- `audit/state.rs` → `AuditState` (per-transition protocol safety) · `audit/matchmaker.rs` → `MatchmakerAudit`.
- `audit/client.rs` → `ClientHistory`, `check_control_history` (the fleet tenant's control journal, the registry and the directory, #247) · `audit/linearizability.rs` → Wing & Gong search over every attempt (#205), its own journal model.
- `audit/journal_model.rs` → the §6 invariants over every node's `applied` reports (one verdict per slot, dense positions, generation chain, monotone `first_seq`).
- `audit/journals.rs`, `audit/system.rs` → the journal board (#188) and system board (#189), below.
- `corpus.rs` → `E1MaskWorkload`, `BareQuorumWorkload`, `DepartedStragglerWorkload`.

## Harness shape

- **Main campaign**: process groups `paros-node` (acceptors, 3–6), `paros-matchmaker` (0–5),
  `paros-proxy` (0–3, `ProxyId(rank)`), `paros-replica` (0–2, `NodeId(1000 + rank)`, fault-free
  disk outside the copy budget), `paros-joiner` (0–2, `NodeId(100 + rank)`, idle without system
  journals). Attrition per group (`AttritionVictims::group`); joiners are no victim. 1–3
  `ChainWorkload` clients. Zero matchmakers = the plain Multi-Paxos deployment.
- **Bootstrap**: `bootstrap_ranks` — the whole pool, or on a matchmaker seed a subset of at least
  `MIN_BOOTSTRAP` leaving *spares* a `Reconfigure` pulls in.
- **Corpus**: a scripted three-node cluster (`scripted_builder`, `NodeProcess::scripted_with`),
  every fault targeted through `ScriptedLifecycle`, an analytic outcome per mask; plus the one
  four-node, one-matchmaker `DepartedStragglerWorkload` (CTRL Case 3 across a reconfiguration).

## Per-seed draws (`shape.rs`)

- `quorum_policy` → `Majority`, `Flexible { q2 }` (`q2` knob clamped `1..=n/2`, `q1 = n - q2 + 1`),
  or `Grid { rows, cols }` from `grid_layouts` (floor `rows >= 2`, `cols >= 2`: `2×2`, `2×3`,
  `3×2`). `QuorumPolicy::system(n)` applies it per configuration size; a size no layout tiles
  runs a majority. The composer may switch a successor to majority, never the reverse.
- `config_floor` → `MIN_BOOTSTRAP` on a matchmaker seed, the whole pool otherwise.
  `QuorumPolicy::clean_copies(floor, pool)` → floor minus the smallest `tolerated_loss` over
  `floor..=pool`; a grid tolerates zero, so a grid seed injects no lost leg and parks nobody.
- `journals` → `JournalPlan`: 1–3 journals (on matchmaker seeds too, #201), one held for the chaos
  window (`hold_journal`). The first is the run's main identifier (`Identifiers::main`; `identifiers` draws it, the
  directory's, the registry's, the fleet tenant's and the cell id once per seed: no identifier is fixed); the
  others' identifiers are drawn
  (#235: a random journal id in the default tenant or a random one, sometimes the first's journal
  id under another tenant). `journal_store` → `JournalStorage` on half the plain seeds, no
  injected rot, its `Durability` a knob (two syncs by default, one at the extreme); on a journal
  seed a seam crash is a power loss (`SelfCrash`), and moonpool's storage chaos runs under it. `system_journals` → the directory, the registry and the fleet tenant's control journal (#229) on half the seeds (the fleet operations too: kept at 50% from the sweep's coverage, #247), on
  `SEED_COUNT` (1) seed ranks. `NodeShape::draw` → `DriverTunables` (one knob per field, or on its own location the whole `DriverTunables::production()` profile `parosd` ships, #209), seam bias, wipe/loss %, `config_edit_pct`.

## Chain workload op ids (`chain_workload.rs:47-127`; ids never shift)

`WRITE=0` (owner writes at its believed next position; a superseded writer's stale write must be
refused) · `WRITE_TO_NON_LEADER=1` · `TRUNCATE=2` (an owner's, under its own fence and clamped by the trim fence; a superseded owner's stale truncate, `stale_truncate_pct`, must be refused, #228) · `READ_STATE=3`
(fold to tail) · `PAUSE=4` · `DUP_WRITE=5` (must fold `Duplicate`) · `DUAL_SUBMIT=6` (one
position per verdict) · `TRUNCATE_STORM=7` · `READ_INDEX=8` retired · `MATCHMAKE=9`,
`MATCH_GC=10` retired · `RECONFIGURE=11` (compose from the live pool; refused on a plain seed)
· `RECONFIGURE_MATCHMAKERS=12` · `RETIRE=13` · `QUORUM_READ=14` retired · `READ=15` (judged as
it arrives) · `CHECK_TAIL=16` retired · `CREATE_JOURNAL=17`, `DELETE_JOURNAL=18`,
`REGISTER_NODE=19`, `DRAIN_NODE=20`, `RETIRE_NODE=21` (a `Write` to the directory or the registry; a create draws its id and redraws on `IdTaken`; refused
`unknown_journal` without system journals; a register carries the joiner's drawn class and
capacity, and a registered joiner registering again is a reboot, #211) · `SET_LEADER=22` (CAS on
the generation) · `CHECKPOINT=23` (the registry's owner, through `paros::client::checkpoint`:
claim, fold to the tail, checkpoint and truncate when the policy finds it due, #230) ·
`BOOK_CAPACITY=24` (book or release a joiner's slot; a booking of the other class must be refused,
#211) · `FLEET_INIT=25` (`init`'s fleet half through `paros::client::fleet`, #229) · `TENANT=26`
(create or remove a tenant through the fleet tenant and the cell; either may stop after one step, a BUGGIFY
crash, and is resumed by the client's next fleet step) · `OP_COUNT=27`. Retired ids are no-ops that keep their slot in the alphabet.

- Each client is an **owner** or a **reader** for the run (knob; each journal's first client
  owns). Owners claim before writing and re-claim when superseded.
- Every client folds its journal into `ChainState` and reports each step
  (`AuditWorld::fold_applied`); truncations are clamped below the lowest folding cursor.
- Races (#205, each a BUGGIFY location): a claim racing its own burst, a write with
  `ack_race_timeout_ms` below its ack retried across a re-claim, a `READ` from a lagging cursor
  racing the client's truncation.
- **Every call goes through `paros::client`**; misbehaviours (`Writer::stale_entry`, `Writer::stale_truncate_request`,
  `write_attempt`, `DUAL_SUBMIT`, `DUP_WRITE`) are explicit calls. Timeouts are `Ambiguous`; a
  retry is the identical write. `ChainConfig::tunables` maps knobs onto `ClientTunables`
  (`write_redirect_limit` → `redirect_limit`, `resolve_attempts` → `retry_budget`, …).

## Boards

- **Journal board** (`audit/journals.rs`): every slot of journal `j` applies only an identity
  appended to `j` (a write sent under another tenant's fence only as a refusal, #247); a
  quarantined journal sends nothing; a sibling keeps committing while one is held; a node keeps
  serving the rest while one is quarantined; a tenant journal commits while the control
  journals' seed is held down (static stability, #247).
- **System board** (`audit/system.rs`): every node folds each system journal alike per LSN; a
  created journal takes its creator's drawn user id, never reused (`IdTaken` only for an id
  created before); no append acked after its tombstone; a checkpoint a node (or a client) meets
  with the whole prefix folded is that prefix's state (#230); a `stateless` joiner never serves
  a journal, a booking takes a slot of its node's class and never past its capacity, and a live
  booking id is never booked again (#211, on the registry's events in LSN order; the model
  crosses a truncation at the checkpoint a restoring node meets and equals every checkpoint it
  reaches, #247); no genesis node's message waits on the registry fold; every live node's
  registry fold reaches the tail after chaos; gates for name races,
  joiners learning before admission, refused-then-accepted joiner messages, a re-registration,
  and a fold restarting from a checkpoint once one truncated.

## Entry points (`lib.rs:309-578`)

`explore`, `run_chain_seed`, `chain_seed_digest`, `chain_seed_canary`, `chain_canary_hunt`,
`chain_smoke`, `explore_chain_seed`, `run_storage_contract_suite`, `corpus_canonical_masks`,
`run_corpus_mask`, `corpus_mask_case`, `corpus_hunt`, `run_corpus_seed`, `run_bare_quorum_case`,
`run_departed_straggler_case`, `departed_straggler_case`.

## Local rules

- moonpool macros only; never plain `assert!`; never reword a message. Budget: 2048 slots
  (moonpool `MAX_ASSERTION_SLOTS`), 256 buckets; no slot, ballot, id, seed or hash as identity.
- No seed constants, seed lists or seed-replay tests (root *Simulation rules*).
- Hooks are consulted from the node loop only; a decision a spawned task needs is carried.
- A wiped identity stays down because the **library** refuses it (#147, #183): the world parks
  it for the budget and composer only, and `StorageWorld::provisioned` is the `BootKind` claim.
- Operators coordinate through `StorageWorld::retire` / `reserve_joiner_retirement`. Spans are non-optional.

## Tests & gates

- `cargo nextest run -p paros-sim`: `tests/sim.rs` (single seed converges, storage contract,
  same-seed digest replay, canary pair, `chain_smoke(SMOKE_ITERATIONS)`); `tests/corpus.rs`
  (canonical masks in quarters with non-vacuous floors, bare quorum, departed straggler).
  Saturation is `cargo xtask sim run paros-chain`; hunts are `sim-paros-hunt`.

## Constants (`lib.rs`, `shape.rs`)

`pub(crate)`: `PROCESS_POOL_RANGE = 3..=6` (`lib.rs:105`), `MATCHMAKER_POOL_RANGE = 0..=5`
(`:119`), `PROXY_POOL_RANGE = 0..=3` (`:131`), `REPLICA_POOL_RANGE = 0..=2` (`:141`),
`JOINER_POOL_RANGE = 0..=2` (`:148`), `CLIENT_COUNT_RANGE = 1..4` (`:154`), `PLATEAU_SEEDS = 8`
(`:161`), `CHAOS_DURATION_MS = 4_000` (`:197`); `pub`: `SMOKE_ITERATIONS = 50` (`:165`),
`COVERAGE_ITERATIONS = 1024` (`:168`), `CORPUS_CI_ITERATIONS = 64` (`:170`),
`EXPLORATION_TIMELINES_PER_SEED = 8` (`:172`). `shape.rs`: `ROUND_TRIP_FLOOR_MS = 250` (`:48`),
`SEED_COUNT = 1` (`:574`), `MIN_BOOTSTRAP = 3` (`:753`). `chaos_surfaces()` = `Network(Swarm)` +
four per-group attritions + `BuggifyKnobs` + `Storage(Swarm)`; `BitFlip` masked; storage masked to
crash damage, failed syncs, short transfers and lost directory entries (`storage_fault_mask()`:
rot, phantom writes, degradation and disk failure stay out, #176); `prob_wipe = 0`.
Deps: `paros`, `moonpool-sim` (`exploration`, `Cargo.toml:20`), `moonpool-rpc` (`:29`) — pin
shared with `paros` and `parosd`.
