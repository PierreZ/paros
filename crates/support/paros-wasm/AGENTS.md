# paros-wasm

The site's wasm tools in one crate (#540, decided by Pierre on 2026-10-15), one cargo feature
per tool. `publish = false`; `cdylib` for the wasm bundles, `rlib` for the native tests and the
playground binary. Both features are on by default, so the workspace's native build, clippy and
tests cover both tools; each wasm bundle is built alone with `--no-default-features --features
<tool>` (`scripts/build-wasm.sh`), so a Paxos page never downloads the simulator.

- `paxos` → `src/paxos.rs` + `src/paxos/` → the interactive Paxos game (#308). Depends on
  `paros-core` only (`default-features = false`, `serde`) and `ts-rs`. Bundle `paros_paxos`,
  page `website/play/`. Spec: `docs/analysis/play/game-plan.md`; read it before adding a level
  or a verb.
- `playground` → `src/playground.rs` + `src/playground/` → the paros playground (#518 (Paxos
  visualization sandbox)): a paros universe on moonpool's simulated network, disk and clock, run
  from a `Config` and an action log. Depends on `paros` (no `tls`) and `moonpool-sim` with
  default features off (no exploration). Bundle `paros_playground`, page `website/sandbox/`;
  the page rules are in `website/zola/AGENTS.md`.

**Never** a dependency on `paros-sim` or `parosd`, and no wasm-only protocol path: the tools
run the code paros ships. The proof that `paros-sim` itself runs the same on wasm32 is the
separate check crate `paros-sim-wasm-check` (#307).

## `paxos`: the game

### The rule

**The core is never modified, forked, or given a wrong answer.** When a level makes a role
manual, the engine computes the core's own answer on a **clone** of the role (`Acceptor`,
`Proposer`, `Replica`, `Matchmaker`, `Matchmaking` are `Clone`) and advances the world only
when the player matches it. A wrong answer is a mistake and an explanation, never a state.
A judge that restates a rule is a bug; the constant-answer prompts (`PersistOrder`,
`CommitOverwrite`, `WipedRejoin`) say why in their doc comments. The engine validates before it
calls the core (a core `assert!` in wasm is an abort): every player-reachable refusal is an
`ActionError`, and a contradiction is surfaced, never swallowed (`DecreeWorld::violation`).

### Map

- `src/paxos.rs` → `Game`, `WasmGame` → level, world, automation flags, the action log that is the undo stack.
- `src/paxos/action.rs` → `Action`, `ActionKind`, `ActionError` → every player verb and the one error type.
- `src/paxos/world/decree/mod.rs` → `DecreeWorld` → Act I: bare `Proposer` + `Acceptor` at slot 0; phase *reach* is the network.
- `src/paxos/world/decree/render.rs` → the Act I view (acceptor and proposer share one `NodeView`).
- `src/paxos/world/mod.rs` → `World`, `Party`, `Envelope`, `InFlight`, `WorldPolicy` → Act II on: constructors, `observe`, `settle`.
- `src/paxos/world/verbs.rs` → wire, clock, client, handoff and prompt-answer verbs (`propose(.., column)`, `relinquish`).
- `src/paxos/world/history.rs` → the client's record and linearizability judge; `World::proposed_values`.
- `src/paxos/world/lifecycle.rs` → crash, the two seams, restart, wipe (boot refused, narrated), corrupt.
- `src/paxos/world/reads.rs` → `World::quorum_read`, the only read, served via `ReadState`.
- `src/paxos/world/disk.rs` → `Disk` → the game's `Storage` impl, the trim-point jump, and the `applied` log.
- `src/paxos/world/drain.rs` → **the drain contract** (module doc) — the only place a `Ready` is held; `plan_recovery`.
- `src/paxos/world/prompts.rs` → which delivery raises which prompt, and the clone it is judged on.
- `src/paxos/world/render.rs` → the log world's view; a crashed node renders from its disk.
- `src/paxos/world/matchmakers/{mod,process,verbs,delivery,prompts,render}.rs` → the matchmaker plane: `MatchmakerProcess`, operator verbs (`retire` needs `reports_gc_floor`), beat-driven freeze/abandon.
- `src/paxos/prompt/{mod,acceptor,proposer,replica,reads,storage,matchmaker}.rs` → `PromptKind`, `Choice`, `Prompt`, `Verdict`, `ALL_PROMPTS`; constructors by role.
- `src/paxos/auto.rs` → `AutomationFlag`, `ALL_FLAGS` → automation as reward, and the delivery pump.
- `src/paxos/narration.rs` → narration derived from the transition.
- `src/paxos/level/mod.rs` → `Level`, `levels()`, `level(id)` · `src/paxos/level/common.rs` → shared worlds and shorthands.
- `src/paxos/level/script.rs` → `Script` → records a reference solution by driving a real `Game`.
- `src/paxos/level/act{1,2,3,4}.rs` → 6 + 7 + 5 + 10 = 28 levels.
- `src/paxos/view.rs` → `GameView` and friends → the one contract the browser reads.
- `tests/paxos_bindings.rs` → writes `website/play/src/generated/` from the `ts_rs::TS` derives.
- `tests/paxos_levels.rs` → every reference reaches its goal; wrong answers refused and inert; undo/replay bit-exact; ids unique; act order; field-guide links bare; unlocks pinned manual.
- `tests/paxos_narration.rs` → a line appears exactly when its transition happened, with its numbers, reproducibly.
- `tests/paxos_world.rs` → the log world driven directly: election, apply, heal, restart, seams, reads, each prompt kind.

The `applied` log in `world/disk.rs` is the **game's own** application stand-in, kept to teach
"chosen is not applied": paros itself runs no application (#186).

### Adding a level

1. Write the `Level` in `src/paxos/level/actN.rs` (stable string id `actN/slug`, never an index;
   briefing is two or three paragraphs of Paxos, mechanism first; core symbols only in
   `symbols`; `field_guide` is a bare book filename; `unlocks` only flags it pins manual).
2. Record its `reference` with `level::script::Script` — choose messages by what they are and
   answer prompts with the core's answer; never hand-count message ids.
3. Append it to that act's `levels()` in play order (Act IV's order is pinned in
   `tests/paxos_levels.rs::act_four_registers_its_ten_levels_in_play_order`).
4. A new prompt: compute the answer on a clone at raise time, author an explanation per wrong
   choice, add it to `prompt::ALL_PROMPTS` and its flag to `auto::ALL_FLAGS`.
5. A new view field: change `src/paxos/view.rs`, rerun the bindings test, commit `website/play/src/generated/`.
6. `cargo nextest run -p paros-wasm`; then the web gates below.

### Local rules

- No randomness, no clock, no `HashMap`/`HashSet`: the action log plus the flag set is the whole
  state, and `undo` replays it bit-exactly.
- Narration is derived from accessor diffs and sent messages, never scripted.
- View conventions: a ballot is `round.node`; a value is plain text; enums cross as enums; node
  and matchmaker ids are different spaces (`MessageView::from_party` / `to_party`).
- `# Panics` on anything that asserts; `#[must_use]` on accessors (pedantic).

## `playground`: the paros playground

### The rule

**Same code as production.** Every machine is the shipped `paros::machine::run_machine` on a
bare `ProviderDisk`; the operator calls `paros::client` as `parosctl` does. No wrapper, no
sim-only path, no buggify or hint here. The timeline is an `Invariant` that only copies named
`tracing` events. A fact the page needs from inside paros is a read-only accessor in `paros`.

### Map

- `src/playground.rs` → `run`, `run_json`, `runSandbox` (wasm), `check` → one seed of
  `SimulationBuilder`: the machine group, the operator workload, the timeline; no fault
  injection: every machine and the operator call `moonpool_sim::buggify_reset` first, so the
  shipped BUGGIFY sites and crash hints are inert, as in production. A log starts with `init`
  at 0 ms (over the first `machines`) and holds no other `init`.
- `src/playground/model.rs` → `Config`, `Action`, `Op` (`init`, `create_tenant`, `create_journal` with
  its `mode`: `failures-per-partition=F,partitions=P` (#523), either part optional, `write`, `read`), `StepResult`,
  `Topology` (each machine's short and full node id and its zone), `TenantPlace`,
  `JournalPlace` (its `key`, acceptors in membership order, `Quorum`), `Event`, `RunView` →
  the JSON contract.
- `src/playground/machine.rs` → `MachineProcess`, `MACHINE_GROUP`, `machine_ip`, `machine_addr`, `zone_of`
  → machine `i` is `10.0.1.{i+1}:4500` in `zone-{i mod 3}`, literal addresses, plaintext.
- `src/playground/operator.rs` → `Operator` → `init` over the first machines (`paros::client::initialize`);
  `FleetSession::create_tenant`; `journals::request` (create); a write claims and writes as the
  journal's one `Writer`, one entry per record, the identical entry again after an unsettled
  answer; a read walks a `Reader` from position 0. After each step, `survey` reads the topology through the admin API
  only: `read_directory`, `journals::list` and `Client::inspect` on every machine.
- `src/playground/calls.rs` → `CallTrace` → a read-only `CallObserver` on the operator's journal clients:
  each attempt a `client_call` event, each answer a `client_answer` event.
- `src/playground/timeline.rs` → `Timeline`, `NAMES` → the events the page draws and lists (`msg_sent`
  names its journal, so the page colours each message by tenant).
- `src/bin/paros-playground.rs` → the same JSON on stdin and stdout, natively.
- `tests/playground_probe.rs` → `init` over four of five machines; two tenants create, write and read; the topology names each journal's acceptors
  and leader, and the quorum shape of a journal over four acceptor partitions; replay is identical, and a longer log
  keeps the shorter one's steps.

## Tests & gates

Prefix each with `nix develop --command` (on the web, see root *Environment & Nix*). CI's
`wasm` job runs the bindings diff, the wasm build and the web app; clippy and nextest cover
both features through the workspace.

- `cargo nextest run -p paros-wasm`
- `cargo test -p paros-wasm --test paxos_bindings` then `git diff --exit-code -- website/play/src/generated`
- `cargo check --target wasm32-unknown-unknown -p paros-wasm --no-default-features --features paxos`
  (and `--features playground`)
- `scripts/build-wasm.sh --wasm-only` (wasm-bindgen output into `website/play/src/wasm/` and
  `website/sandbox/wasm/`, both gitignored)
- `bash -c 'cd website/play && npm ci --no-audit --no-fund && npm run check && npm test && npm run build'`
- Full deploy build: `scripts/build-wasm.sh` after `website/zola/build.sh` (stages into
  `website/zola/public/play/` and `website/zola/public/sandbox/app/`; the Pages workflow).

## Deps & pins (`Cargo.toml`)

`serde`, `serde_json` always. `paxos`: `paros-core` (`default-features = false`, `serde`),
`ts-rs` (`serde-compat`). `playground`: `paros`, `moonpool-sim` and `moonpool-rpc` (the shared
moonpool rev, default features off), `async-trait`, `tracing`, `tokio-util`. wasm-only
`wasm-bindgen = "=0.2.129"` (must equal the flake's `wasm-bindgen-cli`, bumped together) and
`console_error_panic_hook`.
