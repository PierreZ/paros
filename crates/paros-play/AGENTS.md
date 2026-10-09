# paros-play

The interactive Paxos game's engine: `paros-core` driven **by hand** (a driver with a player
where the network and the clock would be), plus the levels, prompts and views the browser reads.
`publish = false`; `cdylib` for the wasm bundle, `rlib` for native tests. Depends on
`paros-core` only (beside the main stack, not on `paros`). The TypeScript app is `web/play/`.
Spec: `docs/analysis/play/game-plan.md` — read it before adding a level or a verb.

## The rule

**The core is never modified, forked, or given a wrong answer.** When a level makes a role
manual, the engine computes the core's own answer on a **clone** of the role (`Acceptor`,
`Proposer`, `Replica`, `Matchmaker`, `Matchmaking` are `Clone`) and advances the world only
when the player matches it. A wrong answer is a mistake and an explanation, never a state.
A judge that restates a rule is a bug; the constant-answer prompts (`PersistOrder`,
`CommitOverwrite`, `WipedRejoin`) say why in their doc comments. The engine validates before it
calls the core (a core `assert!` in wasm is an abort): every player-reachable refusal is an
`ActionError`, and a contradiction is surfaced, never swallowed (`DecreeWorld::violation`).

## Map

- `src/lib.rs` → `Game`, `WasmGame` → level, world, automation flags, the action log that is the undo stack.
- `src/action.rs` → `Action`, `ActionKind`, `ActionError` → every player verb and the one error type.
- `src/world/decree/mod.rs` → `DecreeWorld` → Act I: bare `Proposer` + `Acceptor` at slot 0; phase *reach* is the network.
- `src/world/decree/render.rs` → the Act I view (acceptor and proposer share one `NodeView`).
- `src/world/mod.rs` → `World`, `Party`, `Envelope`, `InFlight`, `WorldPolicy` → Act II on: constructors, `observe`, `settle`.
- `src/world/verbs.rs` → wire, clock, client, handoff and prompt-answer verbs (`propose(.., column)`, `relinquish`).
- `src/world/history.rs` → the client's record and linearizability judge; `World::proposed_values`.
- `src/world/lifecycle.rs` → crash, the two seams, restart, wipe (boot refused, narrated), corrupt.
- `src/world/reads.rs` → `World::quorum_read`, the only read, served via `ReadState`.
- `src/world/disk.rs` → `Disk` → the game's `Storage` impl, the trim-point jump, and the `applied` log.
- `src/world/drain.rs` → **the drain contract** (module doc) — the only place a `Ready` is held; `plan_recovery`.
- `src/world/prompts.rs` → which delivery raises which prompt, and the clone it is judged on.
- `src/world/render.rs` → the log world's view; a crashed node renders from its disk.
- `src/world/matchmakers/{mod,process,verbs,delivery,prompts,render}.rs` → the matchmaker plane: `MatchmakerProcess`, operator verbs (`retire` needs `reports_gc_floor`), beat-driven freeze/abandon.
- `src/prompt/{mod,acceptor,proposer,replica,reads,storage,matchmaker}.rs` → `PromptKind`, `Choice`, `Prompt`, `Verdict`, `ALL_PROMPTS`; constructors by role.
- `src/auto.rs` → `AutomationFlag`, `ALL_FLAGS` → automation as reward, and the delivery pump.
- `src/narration.rs` → narration derived from the transition.
- `src/level/mod.rs` → `Level`, `levels()`, `level(id)` · `src/level/common.rs` → shared worlds and shorthands.
- `src/level/script.rs` → `Script` → records a reference solution by driving a real `Game`.
- `src/level/act{1,2,3,4}.rs` → 6 + 7 + 5 + 10 = 28 levels.
- `src/view.rs` → `GameView` and friends → the one contract the browser reads.
- `tests/bindings.rs` → writes `web/play/src/generated/` from the `ts_rs::TS` derives.
- `tests/levels.rs` → every reference reaches its goal; wrong answers refused and inert; undo/replay bit-exact; ids unique; act order; field-guide links bare; unlocks pinned manual.
- `tests/narration.rs` → a line appears exactly when its transition happened, with its numbers, reproducibly.
- `tests/world.rs` → the log world driven directly: election, apply, heal, restart, seams, reads, each prompt kind.

The `applied` log in `world/disk.rs` is the **game's own** application stand-in, kept to teach
"chosen is not applied": paros itself runs no application (#186).

## Adding a level

1. Write the `Level` in `src/level/actN.rs` (stable string id `actN/slug`, never an index;
   briefing is two or three paragraphs of Paxos, mechanism first; core symbols only in
   `symbols`; `field_guide` is a bare book filename; `unlocks` only flags it pins manual).
2. Record its `reference` with `level::script::Script` — choose messages by what they are and
   answer prompts with the core's answer; never hand-count message ids.
3. Append it to that act's `levels()` in play order (Act IV's order is pinned in
   `tests/levels.rs::act_four_registers_its_ten_levels_in_play_order`).
4. A new prompt: compute the answer on a clone at raise time, author an explanation per wrong
   choice, add it to `prompt::ALL_PROMPTS` and its flag to `auto::ALL_FLAGS`.
5. A new view field: change `src/view.rs`, rerun the bindings test, commit `web/play/src/generated/`.
6. `cargo nextest run -p paros-play`; then the web gates below.

## Local rules

- No randomness, no clock, no `HashMap`/`HashSet`: the action log plus the flag set is the whole
  state, and `undo` replays it bit-exactly.
- Narration is derived from accessor diffs and sent messages, never scripted.
- View conventions: a ballot is `round.node`; a value is plain text; enums cross as enums; node
  and matchmaker ids are different spaces (`MessageView::from_party` / `to_party`).
- `# Panics` on anything that asserts; `#[must_use]` on accessors (pedantic).

## Tests & gates (CI `play` job, `.github/workflows/rust.yml`)

Prefix each with `nix develop --command` (on the web, see root *Environment & Nix*):

- `cargo nextest run -p paros-play`
- `cargo test -p paros-play --test bindings` then `git diff --exit-code -- web/play/src/generated`
- `cargo check --target wasm32-unknown-unknown -p paros-play`
- `scripts/build-play.sh --wasm-only` (wasm-bindgen output into `web/play/src/wasm/`, gitignored)
- `bash -c 'cd web/play && npm ci --no-audit --no-fund && npm run check && npm test && npm run build'`
- Full deploy build: `scripts/build-play.sh` after `web/site/build.sh` (stages into `web/site/public/play/`).

## Deps & pins (`Cargo.toml`)

`paros-core` with `default-features = false`, `serde` (`:17`); `serde`, `serde_json`, `ts-rs`
`serde-compat` (`:20-24`); wasm-only `wasm-bindgen = "=0.2.129"` (`:29`, must equal the flake's
`wasm-bindgen-cli`, bumped together) and `console_error_panic_hook` (`:30`).
