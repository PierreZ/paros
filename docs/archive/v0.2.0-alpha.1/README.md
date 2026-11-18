# The code removed after `v0.2.0-alpha.1`

On 2026-11-19 paros was cut down to its consensus core (Pierre's decision). The tag
`v0.2.0-alpha.1` (commit `d8047ad`) is the last commit that holds the whole journal service: the
library layers above the core, the `parosd`, `parosctl` and `paros-bench` binaries, the
`paros-sim` simulation and its tools. This folder has one recap per removed crate, so that a later
agent can find its way back into that code.

What stays in the tree: `paros-consensus` (it was `paros-core`), the `paros` facade, the TLA+
models of `specs/tla/`, the site's Paxos pages and their wasm engine (`paros-wasm`, feature
`paxos`), and `docs/`. `docs/architecture/` is still the target design.

## How to use a recap

Each recap has the same five parts: what the crate did, its key files and types, the ideas worth
reusing (and the mistakes not to copy), the bugs and scenarios that a new journal must cover
again, and how to read the crate at the tag. Read the recap first, then the code at the tag. Do
not copy code back without a reason in the PR: the journal is being designed again from the
ground up, one proved Paxos flavor at a time.

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:crates/lib/paros-storage/src/lib.rs        # one file
git worktree add ../paros-v0.2.0-alpha.1 v0.2.0-alpha.1            # the whole tree
git grep -n 'fn read_back' v0.2.0-alpha.1 -- crates/                # search it
```

On GitHub: <https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1>.

Paths in the recaps are paths at the tag. `paros-core` and `paros_core` there are
`paros-consensus` and `paros_consensus` today. An issue or PR number in a recap points at
`PierreZ/paros`; most of the open ones were closed on 2026-11-19 with a comment that names the tag.

## The recaps

| Recap | At the tag | Lines | What it was |
| --- | --- | --- | --- |
| [paros-wire](paros-wire.md) | `crates/lib/paros-wire` | 3.6k | the RPC contract: 11 protobuf packages, method ids, codecs |
| [paros-model](paros-model.md) | `crates/lib/paros-model` | 22.0k | the pure service model: registry, tenant and fleet folds, names, placement, checkpoints |
| [paros-tunables](paros-tunables.md) | `crates/lib/paros-tunables` | 1.8k | the knobs of every role and the client, with floors |
| [paros-storage](paros-storage.md) | `crates/lib/paros-storage` | 6.8k | the acceptor and matchmaker stores on moonpool-journal, the typed fault model, the contract suites |
| [paros-net](paros-net.md) | `crates/lib/paros-net` | 3.0k | addresses, the transport, the inbound edge with the cell fence, the clients |
| [paros-client](paros-client.md) | `crates/lib/paros-client` | 18.3k | the journal client (write, read-back, pipeline, reader) and the service client |
| [paros-observe](paros-observe.md) | `crates/lib/paros-observe` | 4.0k | the `Audit` port, metrics, the named BUGGIFY scenarios |
| [paros-roles](paros-roles.md) | `crates/lib/paros-roles` | 25.7k | the IO drivers of every role: the node loop, proxy leader, replica, batcher, matchmaker, frontend |
| [paros-coordinator](paros-coordinator.md) | `crates/lib/paros-coordinator` | 13.8k | the cell, tenant and universe coordinators: placement, moves, balance, membership changes |
| [paros-machine](paros-machine.md) | `crates/lib/paros-machine` | 11.1k | one machine's life: format, `cell init` by single-decree Paxos, admission, serve |
| [paros-authz-biscuit](paros-authz-biscuit.md) | `crates/support/paros-authz-biscuit` | 1.8k | Biscuit tokens and the frontend's verifier |
| [parosd](parosd.md) | `crates/bin/parosd` | 2.0k | the daemon, one per machine |
| [parosctl](parosctl.md) | `crates/bin/parosctl` | 13.4k | the operator and user CLI |
| [paros-bench](paros-bench.md) | `crates/bin/paros-bench` | 5.4k | the load generator |
| [paros-sim](paros-sim.md) | `crates/sim/paros-sim` | 64.6k | the deterministic simulation: workload, fault world, audit, oracles, linearizability checker |
| [paros-sim-runner](paros-sim-runner.md) | `crates/sim/paros-sim-runner` | 2.3k | the sweep and hunt binaries, the gate check, the negative mutants |
| [paros-sim-wasm-check](paros-sim-wasm-check.md) | `crates/sim/paros-sim-wasm-check` | 0.1k | the proof that a seed runs the same on wasm32 |
| [xtask](xtask.md) | `crates/support/xtask` | 0.9k | the sancov sweep runner, the mutants and negative hunts |
| [paros-wasm-playground](paros-wasm-playground.md) | `crates/support/paros-wasm` (feature `playground`), `website/sandbox` | 4.1k | the live paros universe in the browser |
| [deployment](deployment.md) | root files, `.github/`, `scripts/`, `.claude/` | | Docker, Compose, the demo, release-plz, the removed CI jobs, scripts and skills |

## Where to start, by question

- **What must the new journal prove again?** The "Bugs and scenarios" part of
  [paros-sim](paros-sim.md), [paros-roles](paros-roles.md), [paros-storage](paros-storage.md),
  [paros-client](paros-client.md) and [paros-coordinator](paros-coordinator.md).
- **What can `consensus-sim` reuse?** The judges with no cell in them, in [paros-sim](paros-sim.md)
  (the linearizability checker and the journal model), and the negative mutants of
  [paros-sim-runner](paros-sim-runner.md).
- **Which protocol decisions lived outside the core, and so need a model?** [paros-roles](paros-roles.md)
  (the node driver) and [paros-coordinator](paros-coordinator.md) (membership changes).
- **How was the store made safe?** [paros-storage](paros-storage.md).

## The leak table

The recaps cite "leak N". N is a row of this table, from the review of the layers at `6376854`
(2026-11-18, a draft that Pierre parked). Paths are at the tag, under `crates/lib/` unless they
say otherwise.

| # | Leak | Evidence | What it costs |
| --- | --- | --- | --- |
| 1 | The journal node folds the control plane. The data loop reads the registry and the tenant journals, and opens journals from them | `paros-roles/src/node/system.rs` (1,133 lines), `node/control.rs` (1,180), `ControlPlan` at `system.rs:89`, `run_journals<P, J, H: CellHost>` with 13 arguments at `node.rs:293` | A journal node cannot run without `paros_model::registry`. This is the largest knot |
| 2 | Protocol decisions live in the IO driver. I count 18 | `split_early` sends an Accept before the leader's fsync (`paros-roles/src/node/ready.rs:508`). A read-only journal drops Accept behind the core's back (`node/control.rs:1072`). `contest_overwrite` (`node/draws.rs:37`). The election backoff (`node/report.rs:294-450`) | No TLA+ model and no fast judge sees these rules. Only the simulation does |
| 3 | The coordinator drives Paxos internals through operator calls | It reads `gc_watermark`, `config_ballot` and `matchmaker_generation` from `InspectReply`, builds the `RetireRequest` (`paros-coordinator/src/tenant/moves.rs:357-372`), and decides that a handover landed (`tenant/sets.rs:445`) | The safety of a membership change is split between the core and the service |
| 4 | `ColocatedNode` knows every flavor. A flavor is a mode flag, not a layer | 22 of its 48 fields serve only the matchmaker, retire and rejoin code (`paros-core/src/node.rs:193-469`). About 80 branch sites on `has_matchmakers()` or `proxy_count` in 17 files. `node/invariants.rs:257-320` asserts that the plain path leaves them empty | Plain Multi-Paxos cannot be proved or reused alone |
| 5 | The Paxos value is the journal command, and service ids sit in the core | `Command` is concrete in `Message`, `WriteOp`, `Storage::accepted` and `Ready::committed` (`paros-core/src/node/ready.rs:170`). `TenantId` and `JournalIdentifier` are in `journal/ids.rs`. `Config` carries `cell_id` and `peer_domains` | The lowest crate already speaks of tenants and cells |
| 6 | One observation trait covers every layer | `Audit` has 112 methods (`paros-observe/src/audit.rs:200-1190`), from `promised` and `accepted` to `machine_booted`, `cell_formatting` and `system_folded` | The journal cannot be observed without the client, the net, the store and the service model |
| 7 | The observation crate changes behaviour | `paros-observe/src/scenario.rs` holds named fault switches. Shipped code reads them: `lose_verdict()` at `paros-roles/src/node/reply.rs:80`, `quit_mid_move()` at `paros-coordinator/src/cell/campaign.rs:200`. `StorageFaultDecision::of` (`audit.rs:62`) is a driver policy | A crate that "changes no behaviour" (`lib.rs:4`) decides crash against read-only |
| 8 | The journal client holds service concepts | `Client::elsewhere`, `with_own_machine`, `rotating_over` (`paros-client/src/lib.rs:430-476`). `WriteOutcome::Denied(paros_model::authz::Denial)` (`outcome.rs:81`). Reconfigure, retire, inspect and scrub sit on the same `Client` (`lib.rs:1484-1716`) | 6.3k lines of service and 5.4k lines of journal client share one crate |
| 9 | The client proves ambiguous writes with no model | `Client::read_back` (`paros-client/src/lib.rs:1138-1183`) and the merge rules of the pipeline (`pipeline.rs:1-56`) | Exactly-once per position rests on code that only the simulation judges |
| 10 | The wire crate and the tunables mix journal and service | One build compiles 11 proto packages (`paros-wire/build.rs:17-29`). `driver_tunables!` holds `election_timeout_base` beside `balance_*` and `machine_down_after` | No journal user can take the wire without the registry records |
| 11 | `paros-net` is not a transport, and it imports the service model for three types | Only `transport.rs` (125 lines) is a transport. The rest is `CellFence`, the frontend `Pass` and the batcher branch (`clients.rs:174-456`). It imports `Address`, `Names` and `JournalName` from `paros-model` | The network layer knows cells, frontends and machines |
| 12 | Moonpool is in every generic bound and in public errors | `ParosProviders` has 73 use sites (`paros-net/src/runtime.rs:17`). `RunError::Infra(SimulationError)` (`paros-storage/src/boot.rs:109-127`). `RpcError` is in every client judge (`paros-client/src/outcome.rs:89`) | The journal cannot be reused without moonpool |
| 13 | The store port carries the RPC runtime and service facts | `JournalStores::host_on_edge` takes an `RpcHandle` and a `ServiceGroup<MachineInterface>` (`paros-roles/src/node/journals.rs:70-247`). `matchmaker_shard(tenant)` is in the store (`paros-storage/src/journal/matchmaker.rs:212`) | One trait is a store opener, an audit factory, a matchmaker host and a registry cache |
| 14 | The state machine over a journal exists three times by hand, in two crates | `Checkpointable` and `Folder` are in `paros-model/src/checkpoint.rs:61,246`. `Checkpointer` is in `paros-client/src/checkpoint.rs:103`. `CellSession`, `TenantDesk` and `universe.rs` each wire them again | A generic layer hides inside the service |
| 15 | A second Paxos driver lives in the machine | `paros-machine/src/wait.rs` drives `Acceptor` and `cell_init.rs` drives `Decree`, with their own store (`CellLedger`) and wire messages. `MachineRecord` keeps the promise in a text file with no checksum (`paros-model/src/machine/record.rs`) | Bootstrap has consensus state outside the storage contract |
| 16 | The judges are one block, and some reuse the code they judge | `AuditWorld` keys matchmaker facts by tenant (`crates/sim/paros-sim/src/audit/world.rs:131-259`). `audit/node.rs:930-951` decodes registry events inside `applied`. `audit/state.rs:846` calls the core's own quorum predicate | The journal cannot be judged without a cell. A bug in a reused fold is judged green |
| 17 | Copied code inside the core, forced by the layer rule | `ReplicaNode` repeats `learn_read_basis` and `learn_config` (`paros-core/src/replica_node.rs:580,611` against `node/helpers.rs:171,135`). There are four `Ready` types of one shape | The rule forbids a deployment to import a deployment, so shared code has no home |

