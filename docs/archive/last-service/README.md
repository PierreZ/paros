# The paros service at tag `last-service`

This folder records the state of the paros service on the day its code left the tree. The tag
`last-service` names the last commit that holds it: `6376854` (rework 1.7: TLA+ model
cell-control, #1184). The commit that deletes it is the next commit on `main`, and its
message names this folder.

Pierre decided the cut on 2026-11-18: delete the service code in one commit, record the
commit, and focus on `paros-core` and the journal. The service may come back later, one piece
at a time, on top of new journal crates. This document is for that work: it says what each
deleted part did, where its design lives, what was decided, what was still wrong, and how to
bring a part back.

Nothing here is kept up to date. It describes the code at `last-service`, not the code on
`main`.

## What stays on `main`

| Path | What it is | Why it stays |
| --- | --- | --- |
| `crates/lib/paros-core` | The sans-IO Multi-Paxos roles and `ColocatedNode` (57k lines) | The core. The retire stack stays in it for now: the planning thread decides its future. |
| `crates/lib/paros-storage` | The `LogStorage` seam and the stores on `moonpool-journal` (6.8k lines) | The starting text of the new store (Pierre, 2026-11-18). It took `corruption.rs` from `paros-model`, its only import from a deleted crate. |
| `crates/testkit/paros-testkit` | The linearizability checker and the journal model (4.8k lines) | The two judges of `paros-sim` that hold no cell. They moved out of `paros-sim/src/audit/` with their tests. |
| `specs/tla/` | The TLA+ models and their checks | The proof rule stays. |
| `docs/` | Architecture, knowledge base, analysis, papers | The design record. `docs/architecture/` still describes the service as the goal; this folder says the code is gone. |
| `scripts/check-tla.sh`, `check-module-style.sh`, `check-core-layers.sh` | The checks of what stays | |

## What left the tree

Lines of Rust at `last-service`, counted with `wc -l` over each crate's `.rs` files.

| Crate | Lines | What it did | Map (copy of its `AGENTS.md`) |
| --- | ---: | --- | --- |
| `crates/lib/paros` | 57 | The facade: one module per layer, the `run_*` entry points | [paros.md](crates/paros.md) |
| `crates/lib/paros-wire` | 3,611 | The RPC contract: `proto/`, method ids, codecs between core types and protobuf | [paros-wire.md](crates/paros-wire.md) |
| `crates/lib/paros-model` | 21,997 | Pure folds and data: registry, tenants, fleet directory, tags, placement, status, views, checkpoints | [paros-model.md](crates/paros-model.md) |
| `crates/lib/paros-tunables` | 1,803 | `DriverTunables`, `ClientTunables` and their floors | [paros-tunables.md](crates/paros-tunables.md) |
| `crates/lib/paros-net` | 2,994 | Addresses, name resolution, the transport, the RPC edge, mutual TLS | [paros-net.md](crates/paros-net.md) |
| `crates/lib/paros-client` | 18,319 | `paros::client`: writer, reader, pipeline, control-plane sessions | [paros-client.md](crates/paros-client.md) |
| `crates/lib/paros-observe` | 3,957 | The `Audit` port, metrics, named BUGGIFY locations | [paros-observe.md](crates/paros-observe.md) |
| `crates/lib/paros-roles` | 25,662 | One `run_*` loop per paper role: node, proxy leader, replica, batcher, matchmaker, frontend | [paros-roles.md](crates/paros-roles.md) |
| `crates/lib/paros-coordinator` | 13,820 | The cell, tenant and universe coordinators | [paros-coordinator.md](crates/paros-coordinator.md) |
| `crates/lib/paros-machine` | 11,055 | One machine's lifecycle: `run_machine`, `cell init`, admission, `inspect` | [paros-machine.md](crates/paros-machine.md) |
| `crates/bin/parosd` | 2,040 | The daemon, one compio thread per machine | [parosd.md](crates/parosd.md) |
| `crates/bin/parosctl` | 13,397 | The CLI over `paros::client` | [parosctl.md](crates/parosctl.md) |
| `crates/bin/paros-bench` | 5,396 | The load generator | [paros-bench.md](crates/paros-bench.md) |
| `crates/sim/paros-sim` | 64,630 | The simulation harness: processes, role map, fault world, `ChainWorkload`, audit (less the two judges that moved) | [paros-sim.md](crates/paros-sim.md) |
| `crates/sim/paros-sim-runner` | 2,275 | `sim-paros-chain` (the CI sweep), `sim-paros-hunt`, the mutation hunt's test target | [paros-sim-runner.md](crates/paros-sim-runner.md) |
| `crates/sim/paros-sim-wasm-check` | 71 | The proof that a seed is the same run on wasm32 | [paros-sim-wasm-check.md](crates/paros-sim-wasm-check.md) |
| `crates/support/paros-authz-biscuit` | 1,844 | Biscuit tokens: keys, offline mint, derive, the frontend's `Authz` | [paros-authz-biscuit.md](crates/paros-authz-biscuit.md) |
| `crates/support/paros-wasm` | 25,630 | The site's wasm tools: the step-through engine (`paxos`) and the playground | [paros-wasm.md](crates/paros-wasm.md) |
| `crates/support/xtask` | 930 | `cargo xtask sim` (the sancov runner) and `cargo xtask mutants` | [xtask.md](crates/xtask.md) |

Other files that left: `website/` (the Astro site, the design system, the playground), `demo/`,
`DEMO.md` ([copy](DEMO.md)), `Dockerfile`, `docker-compose.yml`, `.dockerignore`, the
`mutants`, `nightly-sim` and `pages` workflows, the CI jobs for the bench, the image, the
simulation and the wasm bundles, `.cargo/config.toml` (the `xtask` alias) and
`.cargo/mutants.toml` (the mutation scope and the equivalent-mutant triage, judged by the
simulation), the scripts of the simulation, the site and the image (`sancov-rustc.sh`,
`sim-canary.sh`, `sim-red-record.sh`, `nightly-sim-*.sh`, `mutants-report.sh`,
`check-sim-wasm.sh`, `build-wasm.sh`, `check-dockerfile-toolchain.sh`, `verify-cell.sh`),
the image outputs of `flake.nix`, and the skills and the agent whose subject left
(`adding-a-buggify-site`, `adding-an-audit-check`, `debug-a-seed`,
`extending-the-chain-workload`, `sim-sweep`, `simulation-driven-fix`, `update-the-site`,
`verify`, `working-on-the-client`, the `seed-debugger` agent). The root `AGENTS.md` lost the
doctrine of the service and the simulation; its full text at the tag is
[AGENTS-root.md](AGENTS-root.md).

## How the service was built

Read this with [AGENTS-root.md](AGENTS-root.md) and `docs/architecture/`, which have the
detail.

**Layers point down only.** Each layer was its own crate: `paros-core` ← `paros-wire` ←
`paros-model` ← `paros-tunables` ← `paros-storage` ← `paros-net` ← `paros-client` ←
`paros-observe` ← `paros-roles` ← `paros-coordinator` ← `paros-machine` ← `paros` (the facade).
A program depended on `paros` alone. The three binaries (`parosd`, `parosctl`, `paros-bench`)
and the harness (`paros-sim`) sat on top.

**One driver, two runtimes.** Every role loop was written once over `P: ParosProviders`
(moonpool's local provider traits plus a local task slot). `parosd` ran it on
`CompioProviders`, one thread per machine. `paros-sim` ran the same code on `SimProviders`
inside a moonpool `Process`. There was no `Sim*` variant of shipped code: faults were inline
`buggify!` sites and `hint!("moment").await` calls at the line that made a choice.

**A cell of uniform machines.** Every machine ran the same `parosd`. It formatted its disk,
minted a `node_id` and waited. `parosctl init` ran a single-decree Paxos (the cell decree,
`paros-core`'s `Decree`) over the initial members to fix the cell plan: the identifiers of the
control journals, the first configurations. After `init`, no machine was special
(#685 (no founders)). A machine found the coordinator from its cached registry.

**Control journals, eaten as food.** The cell's state was paros journals. The registry was the
cell tenant's control journal (`paros-model`'s `registry`). Each hosted tenant had its own
control journal (`TenantControl`). The fleet tenant held the fleet directory (tenant to cell).
Each change was a request with an idempotency id, judged at apply by a pure fold in
`paros-model`. The cell coordinator won a term through an election journal (`paros-client`'s
`election`), then drove placement, drains, moves and set handovers from `paros-coordinator`.

**Matchmakers for every served journal.** Each journal that `parosd` served had a matchmaker
set (one set per tenant, one registry per journal in it) and reconfigured through it: a
candidate registered `(b, C_b)` at a matchmaker quorum, unioned the histories, then ran
Phase 1 on every configuration in `H_b`. GC and retirement made old configurations and
removed nodes safe to forget.

**The data plane.** Four calls: `Write`, `Read`, `Truncate`, `SetLeader`. The leader uuid
fenced writes and truncations; `SetLeader(new, old)` was a compare-and-set. A frontend role
in every `parosd` checked Biscuit tokens and forwarded calls.

**The harness.** One campaign, one workload (`ChainWorkload`), two judges (the client
history checked by the linearizability checker, and the shared `AuditWorld`). Every moonpool
fault on one combined axis, swarm-masked per seed. The machines were real `parosd`s from an
empty disk. CI ran a sancov-guided sweep on four shards; the simulation was off in CI from
2026-11-17 (decision 33).

## Decisions that shaped it

The decisions are recorded, dated, in `docs/architecture/` and in
[AGENTS-root.md](AGENTS-root.md). The ones a later import must read first:

- Same code in production and in simulation; no sim wrappers (#294).
- Eat your own food: control-plane problems use paros's own journals and elections
  (`docs/architecture/01-goal.md` §1).
- Plain Multi-Paxos is a shape of the core, not of the service: every served journal has
  matchmakers (decision 15, 2026-11-16).
- No id is fixed and none has a default; control journal ids are drawn at `init`
  (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8).
- A storage fault quarantines its journal, not the process; a full disk makes it read-only
  (#360).
- A wiped node never rejoins with its old id; the set heals by reconfiguration.
- The rework plan of 2026-11-15 (`docs/architecture/07-changes-milestones-done.md` and
  `/mnt/project-files/plans/paros-rework-plan-2026-11-15.md` in the project) replaced eight
  patterns of the service with eight root rules. Its phases 2 to 7 targeted the deleted code;
  its rules and models stay valid for a rebuild.

## What was still wrong

The rework plan's diagnosis, in short: in the 30 days before 2026-11-15, 64 of 249 merged PRs
were fix-main PRs. The liveness ledger traced most simulation reds to eight patterns where the
strategy was the cause, not one bug:

1. A coordinator term needed two healthy Paxos groups (the election journal and the control
   journal).
2. Recovery repaired in place from stale state, through five step machines that were the same
   routine written five times (`tenant/moves.rs`, `modes.rs`, `controls.rs`, `arrivals.rs`,
   `sets.rs`: 4,998 lines).
3. Old configurations stayed relevant for ever; the retire fence grew six mechanisms in eight
   days.
4. A matchmaker handover finish proposed "the members that answered the freeze", so sets only
   shrank (#846).
5. Each red got its own backoff rule; timeouts were fixed guesses.
6. The harness played the operator, and the oracles had 34 excuses, 9 of them "cell lost" or
   "set lost".
7. Admin commands fenced the coordinator instead of going through it.
8. No single definition of "healed": four tail windows, one per feature.

Three bug shapes that only the service drove. A rebuild must keep them as required scenarios:

- a node pool that grows while a campaign runs;
- a node that rejoins after a retirement;
- a node pool that lags behind a read.

Open issues about the deleted code at the deletion (titles at
`last-service`; most are parked or folded into rework steps):

- **Simulation harness**: #452 (durability hints, round 2), #453 (client choices with no
  buggify), #456 (scenarios stack on almost every seed), #457 (hard-coded tunables in
  machines), #461 (small gates and tail rules), #485 (a double-fault scenario), #493 (a
  NotHeld read never fires), #531 (more chaos on the admin journals), #554 (storage chaos
  tears a one-matchmaker registry), #609 (slow-machine moves and fast grid columns), #617 (a
  Phase-2 voters oracle), #623 (zone-down fleet gates), #655 (cold rot never checked as
  repaired), #664 (name alphabets), #726 (nightly sim seeds), #732 (one typed audit event per
  role), #788 (torn repair scenario), #790 (re-answered registration scenario), #915 (unsent
  operations), #917 (operator errors never reach `run_machine`), #919 (drivers with few
  sites), #920 (main red more often than green), #921 (seed diagnosis tools), #964 (one smoke
  test takes 51 minutes), #975 (shipped functions the campaign never runs), #976 (a calm
  seed), #1036 (storage oracle counts a neighbour slot as lost), #1052 (the red record of
  main), #1183 (liveness causes by phase).
- **Control plane and machines**: #611 (a sole-acceptor control journal backs off behind a
  frozen set), #846 (a handover finish can shrink a set below 2F+1), #877 (a frozen set's
  pending bootstraps grow), #878 (plan journals wait for sets that are only booked), #897 (a
  sole initial member never catches up), #993 (a tenant desk misses a create whose answer was
  lost), #1023 (an admitted machine gets no candidate plan), #1070 (the move between cells).
- **Features, parked**: #195, #231, #253, #363, #367, #371, #372, #571, #572, #573, #576,
  #581, #594, #598, #601, #603, #621, #667, #668, #669, #670, #686, #714, #751, #756, #758,
  #899.
- **Site and build**: #518 (the playground), #596 (tick docs), #898 (playground processes),
  #965 (no step loads the built site), #967 (dependencies), #969 (dev shell fixes), #971
  (parosctl commands in the docs parse), #972 (site fixes).
- **Rework steps that targeted the deleted code**: #1103 to #1145 (phases 2 to 7), #1152
  (side line A, matchmaker chapter).

The open issues about what stays (`paros-core`, `paros-storage`, `specs/tla`): #248, #356,
#579, #890, #902, #906, #907, #979, #982, #1017, #1018, #1020, #1097 to #1102, #1153, #1177,
#1185 to #1190.

## Lessons

- **Write the model before the code.** The control plane had no TLA+ model. Two of 82 TLC
  configurations had a liveness property. The register model in the tree (`cas-register`)
  fails `Agreement` with three ballots (review of 2026-11-16). A rule without a model grew
  fixes, not proofs.
- **A fix that needs an exception to a rule means the rule is wrong.** The retire fence and
  the excuses list both grew one case per red.
- **The harness must not play the operator or excuse a red.** FoundationDB's simulator refuses
  a fault the cell does not promise to survive, then judges every other fault with one
  quiescence oracle. Paros's harness steered faults away (13 uses of `plan_quorum_holders`)
  and excused the rest.
- **Five copies of one routine drift.** One routine that moves a membership from A to B, with
  the differences as data, replaces them.
- **A simulation in CI costs more than it shows while main is red.** The chaos smoke alone
  kept the test job open for 48 minutes; main was red more often than green (#920).
- **The sans-IO split paid off.** `paros-core` stayed small to test, portable to wasm32 and
  free of the service's churn: every finding of the 2026-11-16 core review is local to it.

## How to bring a part back

1. Read the part's map in `crates/` here and its section of [AGENTS-root.md](AGENTS-root.md).
2. Take the files from the tag, not from memory:

       git fetch origin tag last-service
       git checkout last-service -- crates/lib/paros-roles

3. Cut the imports to what exists on `main`. Facts checked at the tag:
   - `paros-storage` imported only `corruption` from `paros-model` (now
     `paros_storage::corruption`).
   - `paros-net` imported only `address::{Address, Names}` and `name::JournalName` from
     `paros-model`.
   - The data modules of `paros-client` imported only `Address`, `Names` and `authz::Denial`
     from `paros-model`.
   - `paros-wire` and `paros-tunables` imported nothing from `paros-model` in code.
   - The linearizability checker read the page size from `paros-tunables`'
     `MAX_READ_RECORDS` (256); `paros-testkit` now holds it as `PAGE_RECORDS`.
4. A part that comes back goes through the rules of `main` (`AGENTS.md`), not the rules of
   [AGENTS-root.md](AGENTS-root.md): a liveness or reconfiguration rule gets its TLA+ model
   first.
