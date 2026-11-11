# paros-wasm

The site's wasm tools in one crate (#540, decided by Pierre on 2026-10-15), one cargo feature
per tool. `publish = false`; `cdylib` for the wasm bundles, `rlib` for the native tests and the
playground binary. Both features are on by default, so the workspace's native build, clippy and
tests cover both tools; each wasm bundle is built alone with `--no-default-features --features
<tool>` (`scripts/build-wasm.sh`), so a Paxos page never downloads the simulator.

- `paxos` → `src/paxos.rs` + `src/paxos/` → the engine of the step-through diagrams of the
  site's `/paxos/` pages (#811 (Paxos flavor pages)). Depends on `paros-core` only
  (`default-features = false`, `serde`). Bundle `paros_paxos`, staged into
  `website/site/public/paxos/wasm/`. It was the engine of the Paxos game (#308). **The game is
  deleted** (decided by Pierre on 2026-11-11, #966 (delete the game)): the web app
  `website/play/`, the TypeScript bindings, the `WasmGame` export, the levels no trace runs on,
  the goals, hints and reference solutions, the undo stack, the prompts, their judges and
  the automation flags are gone. Every decision of a run is `paros-core`'s own; a script only
  delivers, drops, copies, ticks, crashes and proposes. Add no level. The design of the game is
  `docs/analysis/play/game-plan.md`, a record.
- `playground` → `src/playground.rs` + `src/playground/` → the paros playground (#518 (Paxos
  visualization sandbox), #813 (live playground)): one live paros universe on moonpool's simulated
  network, disk and clock (`moonpool_sim::LiveSimulation`), driven one step at a time. Depends on `paros` (no `tls`) and `moonpool-sim` with
  default features off (no exploration). Bundle `paros_playground`, page `website/sandbox/`, served at
  `/playground/`; the site rules are in `website/site/AGENTS.md`.

**Never** a dependency on `paros-sim` or `parosd`, and no wasm-only protocol path: the tools
run the code paros ships. The proof that `paros-sim` itself runs the same on wasm32 is the
separate check crate `paros-sim-wasm-check` (#307).

## `paxos`: the engine of the Paxos pages

### The rule

**The core is never modified or forked.** The engine calls the public API of `paros-core`
and nothing else, and it holds no protocol rule of its own. The engine validates before it
calls the core (a core `assert!` in wasm is an abort): every refusal a script can reach is an
`ActionError`, and a contradiction is surfaced, never swallowed (`DecreeWorld::violation`).

### Map

- `src/paxos.rs` → `Game` → one run: the world and the last action's narration.
- `src/paxos/action.rs` → `Action`, `ActionError` → every verb and the one error type.
- `src/paxos/world/decree.rs` → `DecreeWorld` → Act I: bare `Proposer` + `Acceptor` at slot 0; phase *reach* is the network.
- `src/paxos/world/decree/render.rs` → the Act I view (acceptor and proposer share one `NodeView`).
- `src/paxos/world.rs` → `World`, `Party`, `Envelope`, `InFlight` → Act II on: constructors, `observe`, `settle`.
- `src/paxos/world/verbs.rs` → wire, clock, client and handoff verbs (`propose(.., column)`, `relinquish`).
- `src/paxos/world/history.rs` → the client's record and linearizability judge.
- `src/paxos/world/lifecycle.rs` → crash, the two seams, restart, wipe (boot refused, narrated), corrupt.
- `src/paxos/world/reads.rs` → `World::quorum_read`, the only read, served via `ReadState`.
- `src/paxos/world/disk.rs` → `Disk` → the game's `Storage` impl, the trim-point jump, and the `applied` log.
- `src/paxos/world/drain.rs` → **the drain contract** (module doc) — the only place a `Ready` is held.
- `src/paxos/world/render.rs` → the log world's view; a crashed node renders from its disk.
- `src/paxos/world/matchmakers/{mod,process,verbs,delivery,render}.rs` → the matchmaker plane: `MatchmakerProcess`, operator verbs (`retire` needs `reports_gc_floor`), beat-driven freeze/abandon.
- `src/paxos/narration.rs` → narration derived from the transition.
- `src/paxos/level.rs` → `Level`, `levels()`, `level(id)` → a setup a trace runs on: an id and a
  world to build. `src/paxos/level/common.rs` → shared
  worlds and shorthands. `src/paxos/level/act{1,2,3,4}.rs` → 1 + 3 + 1 + 6 = 11 setups.
- `src/paxos/view.rs` → `WorldView` and friends → the world as the recorder reads it.
- `src/paxos/trace.rs` + `src/paxos/trace/pages.rs` → `Recorder`, `Trace`, `traces()` → the
  step-through diagrams of the site's `/paxos/` pages (#811 (Paxos flavor pages)): a short
  script per page on a level's setup (`Game::act`, a run may use any verb), each
  message one step with the engine's narration (sentences to the player removed) and every
  lane's state. A "what if" run's `fork` is its first step that differs from the base run.
  The wasm export `paxos_trace(page)` records one page in the browser; the page draws that
  live run (bundle `paros_paxos`, staged into `website/site/public/paxos/wasm/`).
- `src/paxos/trace/compart.rs` → the compartmentalized page (#943 (compartmentalized
  diagram)): the game colocates every role, so this page has its own small driver, as the
  `proxy_leader` and `replica_tier` examples of `paros-core` do. Three `ColocatedNode`s, two
  `ProxyLeader`s, two `ReplicaNode`s and a `Batcher`; the driver only moves messages, and
  every state line and quorum answer is read off those roles. The handoff run stays a
  `Recorder` run on its level, with its own lanes.
- `src/paxos/picker.rs` → `check(shape, down)`: which quorums `paros-core` finds in a shape
  with some acceptors stopped; the wasm export `paxos_quorums` serves the quorums page's
  picker (#948 (quorum shapes and picker)).
- A "Break the rule" run (#946) causes its fault from **outside** the core, with verbs the
  shipped code refuses: `Wipe` boots an acceptor of the single-decree world on an empty disk,
  and `Reconfigure` there gives one proposer its own acceptors with no matchmaker. The core
  is never changed; `DecreeWorld::violation` and `DecreeWorld::decisions` record the two
  values chosen.
- `src/paxos/ready_lab.rs` → `ReadyLab` (wasm `ReadyLab`) → the sans-IO page's `Ready` inspector (#951): three
  `ColocatedNode`s on the game's `Disk`, a wire driven by hand (`timeout`, `tick`, `deliver`,
  `drop`, `write`), and each `Ready` an input gave, bucket by bucket. A driver, no protocol logic.
- `src/paxos/quorum_reach.rs` → `reach` (wasm `quorum_reach`) → the grid-quorums page's picker (#951):
  `QuorumSystem::partitions` and `AcceptorConfig::has_phase1_quorum` / `has_phase2_quorum_in` over
  the acceptors that are up.
- `tests/paxos_traces.rs` → writes `website/site/src/traces/*.json` (committed, diffed in CI).
- `tests/paxos_world.rs` → the log world driven directly: election, apply, heal, restart, seams, reads, recovery, the matchmaker plane.

The `applied` log in `world/disk.rs` is the **game's own** application stand-in, kept to teach
"chosen is not applied": paros itself runs no application (#186).

### Changing a run

1. A trace script is in `src/paxos/trace/pages.rs`. It runs on a setup of `src/paxos/level/`.
   Add no setup for a new game level; a page that needs a new world adds its setup there.
2. A new view field: change `src/paxos/view.rs`, rerun `paxos_traces`, commit `website/site/src/traces/`.
3. `cargo nextest run -p paros-wasm`; then the web gates below.

### Local rules

- No randomness, no clock, no `HashMap`/`HashSet`: the same actions on the same setup give the
  same run.
- Narration is derived from accessor diffs and sent messages, never scripted.
- View conventions: a ballot is `round.node`; a value is plain text; enums cross as enums; node
  and matchmaker ids are different spaces (`MessageView::from_party` / `to_party`).
- `# Panics` on anything that asserts; `#[must_use]` on accessors (pedantic).

## `playground`: the paros playground

### The rule

**Same code as production.** Every machine is the shipped `paros::machine::run_machine` on a
bare `ProviderDisk`; the operator calls `paros::client` as `parosctl` does. No wrapper, no
sim-only path, no buggify or hint here (the shipped sites go live only when `Config::chaos.crashes` asks, and
moonpool decides each strike). The timeline is an `Invariant` that only copies named
`tracing` events. A fact the page needs from inside paros is a read-only accessor in `paros`.

### Map

- `src/playground.rs` → `Live` (`start`, `act`, `advance`, `finish`), `Frame`, `Universe` (wasm:
  `new`, `act`, `advance`, `nowMs`), `run` and `run_json` (a whole action log on the same engine,
  for the tests and the native binary), `check_config`/`check_op` → one seed of
  `SimulationBuilder::live()`: the machine group, the operator workload, the fault injector, the
  timeline. Unless `Config::chaos.crashes` is on, every machine and the operator call `moonpool_sim::buggify_reset` first, so the shipped
  BUGGIFY sites and crash hints are inert, as in production. A fault step goes to the injector
  and strikes at once; any other step queues for the operator.
- `src/playground/model.rs` → `Config` (its `stateless` machines, #822 (stateless playground
  roles): they run with `PAROS_ROLES=stateless` and cannot found a cell), `Action`, `Op` (`init`, `create_tenant` with its cell,
  `create_journal` with its `mode` (`machines=F,partitions=P`, #523) and `multi`, `configure`
  (layout, replicas, proxies, batchers), `frontends` (the tenant's frontend count),
  `set_leader`, `stream`, `write`, `read` (the tail), `traffic`, `add_machine`, `drain`, `remove`,
  `maintenance`, `add_cell`, `resolve`, and the faults `crash`, `restart`, `isolate`, `heal`),
  `StepResult`, `Topology` (each machine's short and full node id, its zone, its class and its
  `bookings` by role, the `cells`),
  `TenantPlace`, `JournalPlace` (its `key`, acceptors in membership order, `Quorum`), `Event`,
  `RunView` → the JSON contract.
- `src/playground/machine.rs` → `MachineProcess`, `MACHINE_GROUP`, `machine_ip`, `machine_addr`, `zone_of`,
  `class_for` → machine `i` is `10.0.1.{i+1}:4500` in `zone-{i mod 3}`, literal addresses, plaintext;
  class `stateless` when the config names it, else parosd's default (every role).
- `src/playground/operator.rs` → `Operator` → pops its steps from an inbox; `init` over the first
  machines and `add_cell` over idle ones (`paros::client::initialize`, `FleetSession::add_cell`);
  `FleetSession::create_tenant` (`with_cell`); `journals::request` (create, configure);
  `CellSession` (add, drain, remove, maintenance), fresh for each command as `parosctl` opens one;
  a single-writer write claims and writes as the journal's `Writer`, one entry per record, the
  identical entry again after an unsettled answer; a multi-writer write is `multi::append_request`;
  `set_leader` claims with a fresh `Writer` and tries one write with the old one. Idle, it appends
  for the started writers and reads the topology every second. `survey` reads it through the admin
  API only: `read_directory`, `journals::list`, `Client::inspect` on every machine, and each
  cell's admin view (`views::ask`, `Query::Cell`) for the bookings of each machine.
- `src/playground/chaos.rs` → `surfaces`, `storage_mask`, `HintBudget`, `veto` → `Config::chaos` (#951 (live
  Engineering pages)) mapped onto moonpool's own chaos surfaces, swarm mode (network, storage,
  writeback alone, crashes: an attrition regime over the machines, with the shipped BUGGIFY sites
  and hints live, and a `HintVeto` that allows `HINT_KILLS` hint crashes once `init` ended). Off
  by default: a config with no chaos is the run it always was.
- `src/playground/faults.rs` → `Injector` → plays the fault steps on moonpool's simulated world
  and records their results.
- `src/playground/calls.rs` → `CallTrace` → a read-only `CallObserver` on the operator's journal clients:
  each attempt a `client_call` event, each answer a `client_answer` event.
- `src/playground/timeline.rs` → `Timeline`, `NAMES` → the events the page draws and lists (`msg_sent`
  names its journal, so the page colours each message by tenant).
- `src/bin/paros-playground.rs` → the same JSON on stdin and stdout, natively; in the browser the same
  `run_json` is the wasm export `runJson` (the Engineering pages' widgets, #951).
- `tests/playground_probe.rs` → `init` over four of five machines; two tenants create, write and read; the topology names each journal's acceptors
  and leader, and the quorum shape of a journal over four acceptor partitions; replay is identical, and a longer log
  keeps the shorter one's steps; a crashed leader is replaced; the DEMO's cell steps (a second
  cell, a tenant in it, a multi-writer journal, set-leader, a started writer, configure, resolve,
  maintenance, drain) in one run.

## Tests & gates

Prefix each with `nix develop --command` (on the web, see root *Environment & Nix*). CI's
`wasm` job runs the traces diff, the wasm build and the playground page's unit tests; clippy and nextest cover
both features through the workspace.

- `cargo nextest run -p paros-wasm`
- `cargo test -p paros-wasm --test paxos_traces` then `git diff --exit-code -- website/site/src/traces`
- `cargo check --target wasm32-unknown-unknown -p paros-wasm --no-default-features --features paxos`
  (and `--features playground`)
- `scripts/build-wasm.sh --wasm-only` (wasm-bindgen output into `target/wasm-tools/paxos/` and
  `website/sandbox/wasm/`, neither is committed)
- `node --test website/sandbox/*.test.mjs`
- Full deploy build: `scripts/build-wasm.sh` (stages the playground into
  `website/site/public/playground/app/`), then `npm run build` in `website/site/` (the Pages workflow).

## Deps & pins (`Cargo.toml`)

`serde`, `serde_json` always. `paxos`: `paros-core` (`default-features = false`, `serde`),
`playground`: `paros`, `moonpool-sim` and `moonpool-rpc` (the shared
moonpool rev, default features off), `async-trait`, `tracing`, `tokio-util`. wasm-only
`wasm-bindgen = "=0.2.129"` (must equal the flake's `wasm-bindgen-cli`, bumped together) and
`console_error_panic_hook`.
