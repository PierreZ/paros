# paros-sim

The deterministic simulation (DST) harness of paros on moonpool: workload, fault world, audit and oracles. Removed after the tag `v0.2.0-alpha.1` (commit `d8047ad`). At the tag it lived in `crates/sim/paros-sim/` (64,630 lines of Rust, 122 files).

## What it did

paros-sim ran the whole shipped stack (acceptors, matchmakers, proxy leaders, replicas, joiners, machines with their frontends) as moonpool `Process` groups on simulated network, disk and time. It sat on top of `paros` (and so `paros-core`, `paros-roles`, `paros-machine`, and the other library crates). `paros-sim-runner` and `paros-sim-wasm-check` depended on it. Correctness lived here: in the audit (`src/audit/`) and in the workload's `check()`. It never judged from a trace scan.

One campaign ran per seed. `chain_builder` (`src/lib.rs`) drew a shape for the seed (`src/shape.rs`: process counts, quorum policy, journals, knobs, one of more than 20 named scenarios). It added the chaos surfaces (`chaos_surfaces()`: network swarm, per-group attrition, outages, BUGGIFY knobs, storage faults, CPU model, gray failures). Then 1 to 3 `ChainWorkload` clients ran an op alphabet of 32 ids (`src/workload/config.rs`) against the cell through `paros::client`. The run had a chaos phase, then a quiet tail. In the tail, chaos and operators stopped and the cell had to heal itself (rule 8 of `docs/architecture/06-verification.md`).

The driver reported each transition once, through the `paros::observe::audit::Audit` trait, to one shared `AuditWorld` per iteration (`src/audit/world.rs`, published on moonpool's `StateHandle`). Each check was O(1) at the report. At the end, `check_run` ran the slot-order checks, the linearizability search per journal, convergence, and the tail checks. The crate had about 715 `assert_always!` sites and about 850 reachable/sometimes gates.

The crate was also built for `wasm32-unknown-unknown`. A seed had to give the same run on every target (`draw_index` in `src/lib.rs`).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `crates/sim/paros-sim/src/lib.rs` | `chain_builder`, `chaos_surfaces()`, pool ranges, `CHAOS_CAP`, `RUN_FLOOR`, `MUTANT_SEEDS`; entry points `explore`, `run_chain_seed`, `replay_chain_timeline`, `chain_seed_digest`, `chain_seed_fingerprint` (`SeedFingerprint`), `chain_seed_dump` (`SeedDump`), `chain_mutants`, `run_storage_contract_suite`; `draw_index` |
| `src/audit/linearizability.rs` | Wing and Gong search with Lowe memo (Porcupine) over every client attempt. `Call`, `Seen`, `Attempt`, `Verdict`, `check(attempts, mode, budget)`. Own sequential journal model (`Model::step`), not the core's |
| `src/audit/linearizability/reference.rs` | Test only. Plain Wing and Gong with no pruning, and a differential test on 100,000 small histories (half mutated) |
| `src/audit/journal_model.rs` | `JournalModel`: folds every node's `applied` verdict per slot and checks the section 6 invariants in slot order (`check`). `AckedWrite`, `record_hash` |
| `src/audit/client.rs` | `ClientHistory`, `check_control_history`: merges client histories and calls the search; an undecided search is a violation |
| `src/audit/state.rs` | `AuditState`: per-transition Paxos safety (promise monotone, one value per slot and ballot, durable quorum decides one value, configuration bound to ballot, reconfiguration reach) |
| `src/audit/claims.rs` | Durable-claim oracles (#458 (core oracles)): Promise content, plain Phase 1 quorum, Accept record durable |
| `src/audit/quorum.rs` | `chooses`: the Phase-2 quorum rule written again from the papers, shares no code with `membership/` |
| `src/audit/losses.rs` | `Losses`, `Beyond::{Loss, Wipes}`: CTRL recoverability of a slot after an outage, re-derived from the papers |
| `src/audit/node.rs`, `matchmaker.rs`, `matchmaking.rs`, `proxy_leader.rs`, `replica.rs`, `machine.rs`, `net.rs` | `Audit` callbacks split by reporting role; `NodeAudit` in `src/audit.rs` stamps time |
| `src/audit/journals.rs`, `system.rs`, `tenants.rs` | Journal board, system board (`SystemBoard`, registry booking model), tenant board |
| `src/audit/installs.rs`, `liveness.rs` | Election term to installed leader order; `JudgedRegistry` (liveness marks) |
| `src/tail.rs`, `src/tail/predicate.rs` | Tail `Clock`, `PASSES` = 3, `PASS_GAP` = 2 s; pure cell healthy predicate `judge`, `judge_cell`, `Group`, `Healthy`, `Cause`; `judge_lost`, `cut_short` |
| `src/mutant_hunt.rs` | `reds`, `baseline`, `line`, `of_a_cause`, `FAILED_RUN`: judge a mutant against the unmutated baseline per seed |
| `src/workload.rs`, `workload/config.rs`, `workload/rpc.rs`, `workload/chain.rs` | `ChainWorkload`, op ids, `CallLog` (the history, from `paros::client`'s `CallObserver`), `ChainState` (client fold digest) |
| `src/workload/write.rs`, `truncate.rs`, `races.rs`, `multi.rs`, `owner.rs`, `edge.rs`, `pipelined.rs`, `deposed.rs`, `foreign.rs` | Data-plane ops and the explicit misbehaviours (stale writes, wrong mode, edge sizes, bursts, cross-tenant calls, deposed actor `ActorFence`) |
| `src/workload/fleet.rs` and `workload/fleet/*.rs` | `FleetOps`: init, tenants, admit, drain, election, placement, sets, views, tags, frontends, `cell_heals` (`healthy.rs`), `final_check` |
| `src/world.rs`, `world/injector.rs`, `world/outage.rs`, `world/scenarios/*.rs` | `StorageWorld` (budgets, parks), `Custody` (boot-time byte damage), correlated outage plans, named scenarios |
| `src/world/node_store.rs`, `world/registry_store.rs` | `LedgeredJournal`, `LedgeredRegistry`: the old wrapper pattern (see mistakes) |
| `src/shape.rs`, `shape/*.rs` | `NodeShape`, `QuorumPolicy`, `JournalPlan`, per-seed scenario draws; `shape/larger.rs` (`LargerCell`, 5 to 9 machines) |
| `src/machine/board.rs`, `src/frontend.rs`, `src/tls.rs` | `MachineBoard`, `MachineProcess`, name table, frontend tokens, TLS per seed |
| `tests/sim.rs` | Storage contract suite, same-seed replay, canary replay, `chain_smoke` |

## Ideas worth reusing

Best reuse candidates for `consensus-sim` are the judges that hold no cell. They take plain data and need only a few `paros::core::journal` types.

- **Linearizability checker** (`src/audit/linearizability.rs`). Input: a list of `Attempt { client, inv, call, seen }`, with `seen: None` for an unknown result. Its only imports are `JournalView`, `LeaderUuid`, `WriterMode` and `splitmix`. Good points: an unknown attempt is tried only where it changes state; an unknown attempt past its deadline leaves the search (#392); an unknown write lands only where read pages allow (`Landings`, #512 (search budget)); a `Seen::NotWritten` marker for a client-side resolve (#444); multi-writer mode (#241); a step budget with an `exhausted` flag. The prose proofs of each pruning are in the module doc. Port it with `reference.rs` together.
- **Reference checker and differential test** (`src/audit/linearizability/reference.rs`, #973 (reference checker and oracle mutants)). A second, naive checker over the same `Model::step`, and 100,000 generated histories (half linearizable by construction, half with one mutation). Rule: every new pruning keeps it green and gets a hand-made mutant that makes it red. This is the right pattern for any moonpool-tla or moonpool-walk checker.
- **Journal model** (`src/audit/journal_model.rs`). It folds per-slot verdicts from every node and never re-implements the journal. It checks: one verdict per slot, dense positions, leaders born in the log (a write accepted only under the uuid in force), no re-accept with other bytes, monotone `first_seq`, fenced truncation, writer mode both ways, and verdicts no client received (#656). Acked writes are judged by their bytes (#651). Order-free folding, then slot-order checks at the end: this suits a walk over a sans-IO state machine. 822 lines of unit tests in `journal_model/tests.rs` name each bug shape.
- **Independent quorum rule** (`src/audit/quorum.rs`, #909 (oracles that do not judge)). The oracle does not ask the core whether a slot is chosen. It writes the majority and grid rules again and tests them against every subset of small configurations. Copy this rule: an oracle must not share code with the code it judges.
- **Independent CTRL recoverability** (`src/audit/losses.rs`). A slot is recoverable while a Phase-1 quorum could qualify; an unrecoverable slot is never accepted again. Typed causes `Beyond::Loss` and `Beyond::Wipes`.
- **Durable-claim oracles** (`src/audit/claims.rs`). What a node says on the wire is checked against what it durably holds. Each "second proof" is its own case, with unit tests.
- **Pure tail predicate** (`src/tail/predicate.rs`). No I/O, no clock. Facts in, `Result<Healthy, Cause>` out, one typed cause per unhealthy group, N passes in a row (FoundationDB quiet database). Good shape for a liveness judge in a random walk.
- **Mutant judging against a baseline** (`src/mutant_hunt.rs`). A red counts for a mutant only if the baseline does not have it on that seed, and only if it is one of the mutant's named causes.
- **Seed fingerprint** (`SeedFingerprint` in `src/lib.rs`): one line per seed (digest, simulated ms, events, violations). Cheap determinism check across builds.
- **`draw_index`** (`src/lib.rs`): reduce a `u64` draw in `u64`, then narrow. A narrow-first modulo gives other runs on 32-bit targets.

Mistakes not to copy:

- **Wrapper stores that decide faults** (`src/world/node_store.rs` `LedgeredJournal`, `src/world/registry_store.rs` `LedgeredRegistry`). AGENTS.md calls them the old pattern. Faults belong in the shipped code (`buggify!`, `hint!`) or in moonpool, not in a harness wrapper (#294, #760 (moonpool owns fault placement)).
- **Harness kill policy**. `world::can_kill` was deleted (#1179 (the kill policy as shards)); the shard gate judges from `shard!` declarations of the shipped code.
- **Excuses and skips**. `src/excuses.rs` and `Excuse::ALL` were deleted (#1091 (oracle with no excuse)). Every oracle judges every seed, each red has a typed cause.
- **One giant workload**. One campaign with 32 op ids, more than 20 scenarios and per-scenario weight overrides. The crate grew to 64k lines and an 89 KB AGENTS.md. The gates came near the 8192 assertion-slot budget (`gates::SLOT_LIMIT` in the runner). Keep a new sim small and layer it.
- **Tail windows that grow to fit a red**. Per-feature windows (20 s, 60 s, 100 s) and `FLEET_SETTLE` (20 s) and `HANDOVER_CEILING` (60 s) in `src/workload/fleet.rs`; rule 8 wants one fixed bound from the tunables.
- **Audit tied to one driver trait**. Every oracle reads `paros::observe::audit::Audit` callbacks. For consensus-sim, feed the judges from the state machine's outputs instead.

## Bugs and scenarios to cover again

Safety shapes (all `assert_always!`; grep the message at the tag):

- Two values chosen for one slot; an applied value differs from the decided value; an applied slot not decided by a durable accept quorum (`src/audit/node.rs`).
- A promised ballot decreases; an accepted ballot above the promise; a restart changes a pre-crash accepted value; one ballot proposes two commands for a slot (`src/audit/state.rs`).
- Reconfiguration: a Prepare or Accept reaches the wrong configuration; an Accept leaves before every prior configuration promised a quorum; a configuration edited after it is bound to a ballot (`src/audit/state.rs`).
- Matchmaking: a refused or unregistered ballot becomes a leader; prior set is not the union above the watermark; a registration under the GC watermark; one ballot registered with two configurations; a frozen generation thaws (`src/audit/matchmaker.rs`, `matchmaking.rs`).
- Compaction floor regresses, or passes the decided frontier (#448); an accept persisted below the floor (`src/audit/node.rs`).
- A Promise page omits an accepted record or reports one not durably held; an Accept sent before its own fsync and never recorded (#578 (leader sends Accept before its own fsync)) (`src/audit/claims.rs`).
- Journal: a superseded writer acked written (fencing removed from the core: red on the first seed); a stale truncate applied; a write ahead of `next_seq`; a rewrite with one byte changed (#665 (conflicting rewrite)); a `Duplicate` acked over other bytes; a write resolved `NotWritten` that lands later (#444); a stale read after a completed write. Unit tests in `src/audit/linearizability.rs` and `journal_model/tests.rs` name each one.
- Election: two leaders in one term; a later term installed before an earlier one (#653 (election term to installed leader), `src/audit/installs.rs`).
- Storage: rot of an entry, record, metainfo or header at boot must get a verdict at open (`src/world/injector.rs`); a fold hole under the chosen index (#343); torn last batch of a registry; disk full must turn the store read-only (#360 (disk full)); fsyncgate failed writeback (#720).

Liveness and recovery shapes (rule 8):

- Correlated outage of every acceptor with a planned loss that keeps a clean quorum (#263, `src/world/outage.rs`).
- Split floor: ahead acceptors down, behind ones must jump to a trim point to unblock a repair probe (#409 (repair probe blocked below a trim point), `world/scenarios/split_floor.rs`).
- Lagging acceptor jumps to a peer's trim point (#340); lagging replica serves reads while it pages catch-up (#550 (replica serves an unreported fold page)).
- Stalled proxy and a leader that resigns while proxies hold delegated rounds (#341); a stuck round moved to another grid column (#533 (move a stuck round)); handoff storm (#484 (silent driver fault sites)).
- Recovery spans several pages (#330); promise, re-send and apply pages of size 1 to 4 (#338).
- Wiped member (amnesia) must be refused, not boot fresh (#147, #183; mutant `wipe-amnesiac-store-boots-fresh`).
- A gone node lost for good while a stale belief still names it (PR #1064 (gone-node gap)).
- Larger cell, 5 to 9 machines, `F` up to 3, with and without spares; typed `CannotPlace` when no spare (#1092 (larger cell)).
- Seeds to re-check for the search budget: 2216307595147519586 (PR #516 (internal clients), election journal, 332 attempts) and 8108577621986896023 (PR #527 (search budget), 304 attempts). They are witnesses at the tag only; do not keep them as constants.

The 23 negative mutants in `crates/sim/paros-sim-runner/negative/` are the regression list for oracles; see `paros-sim-runner.md`.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/sim/paros-sim/src/lib.rs
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1   # the whole tree
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1/crates/sim/paros-sim

Where to start reading: `src/audit/linearizability.rs` (module doc, then `check`), `src/audit/linearizability/reference.rs`, `src/audit/journal_model.rs`, `src/audit/quorum.rs` and `src/audit/losses.rs`, then `src/tail/predicate.rs`. For the harness itself: `src/lib.rs` (`chain_builder`), then `src/audit/world.rs` (`check_run`). Its own map at the tag: `crates/sim/paros-sim/AGENTS.md` (long; the *Map*, *Boards* and *Chain workload op ids* sections). The rules it served: `docs/architecture/06-verification.md`.
