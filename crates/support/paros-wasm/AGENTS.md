# paros-wasm

The site's wasm tools (#540), one cargo feature per tool. `publish = false`; `cdylib` for the wasm
bundle, `rlib` for the native tests. One tool, `paxos`, on by default (so clippy and nextest
cover it); the wasm bundle is built with `--no-default-features --features paxos`
(`scripts/build-wasm.sh`).

- `paxos` → `src/paxos.rs` + `src/paxos/` → the engine of the step-through diagrams of the
  site's `/paxos/` pages (#811 (Paxos flavor pages)). Depends on `paros-consensus` only
  (`default-features = false`, `serde`). Bundle `paros_paxos`, staged into
  `website/site/public/paxos/wasm/`. It was the engine of the Paxos game (#308). **The game is
  deleted** (decided by Pierre on 2026-11-11, #966 (delete the game)): the web app
  `website/play/`, the TypeScript bindings, the `WasmGame` export, the levels no trace runs on,
  the goals, hints and reference solutions, the undo stack, the prompts, their judges and
  the automation flags are gone. Every decision of a run is `paros-consensus`'s own; a script only
  delivers, drops, copies, ticks, crashes and proposes. Add no level. The design of the game is
  `docs/analysis/play/game-plan.md`, a record.
- `playground` was removed on 2026-11-19 with the journal layers. It is at the tag
  `v0.2.0-alpha.1` (`docs/archive/v0.2.0-alpha.1/paros-wasm-playground.md`).
- `journal` (the labs, the seal step-through and the live journal of the Journal pages) was
  removed on 2026-11-24 with the journal of design A. It is at the tag `journal-design-a`
  (`docs/archive/journal-design-a/README.md`).

No wasm-only protocol path: the tool runs the code paros ships.

## `paxos`: the engine of the Paxos pages

### The rule

**The core is never modified or forked.** The engine calls the public API of `paros-consensus`
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
- `src/paxos/world/forge.rs` → `World::skip_phase1` (`Action::SkipPhase1`): the fault of the
  Multi-Paxos "Break the rule" run, Accepts that the engine writes with no Phase 1.
- `src/paxos/world/reads.rs` → `World::quorum_read`, the only read, served via `ReadState`.
- `src/paxos/world/disk.rs` → `Disk` → the game's `Storage` impl, the trim-point jump, and the `applied` log.
- `src/paxos/world/drain.rs` → **the drain contract** (module doc) — the only place a `Ready` is held.
- `src/paxos/world/render.rs` → the log world's view; a crashed node renders from its disk.
- `src/paxos/world/matchmakers/{mod,process,verbs,delivery,render}.rs` → the matchmaker plane: `MatchmakerProcess`, operator verbs (`retire` needs `reports_gc_floor`; `roll`, `Action::Roll`, asks the leader for `ColocatedNode::roll(None)`, refused with no matchmakers), beat-driven freeze/abandon. `NodeView::segment` is a node's tail segment, and a node's card says `segment k` once its log has rolled. Each node's `SegmentPublish` goes to the matchmakers as a wire message (phase `chain`), and each `SegmentAck` comes back to `ColocatedNode::on_segment_ack`; a publish that waits for a majority goes out again on the node's tick (`resend_segment_publishes`); `MatchmakerView::chain` is a matchmaker's chain of segments, and its card says `chain 0–2, 3–4` for the sealed ones.
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
- `src/paxos/trace/story.rs` → `Beat`, `Choice`, `Flight`, `NodeState` → what the story mode of
  a page reads (#1146 (recorder for the animated chapters)): a script opens beats with
  `Recorder::beat("one sentence.")` (written next to its actions; a beat with no step is an
  introduction screen), may ask a question before it plays with
  `Recorder::ask(question, answers, right, why)`, or ask what went wrong after it played with
`Recorder::find` (same arguments; the "find the bug" question of a "Break the rule" run), and pins the right answer to the world with
  `Recorder::holds(fact, says)` after the beat's steps (a `holds` that fails stops the trace, so
  a change of the core cannot leave a wrong answer on the site), and ends one with
  `Recorder::choice(label, run, beat)`, which opens
  another run of the page at one of its beats (checked when the page is assembled). A run of
  the tour of the site (`website/site/src/tour.ts`) is marked with `Recorder::tour(chapter,
  title)`; its beats may link a run that tells more (`Recorder::more(run)`) and name the lanes
  that enter at them (`Recorder::enter(&[lane])`). `tests/paxos_tour.rs` holds every tour run
  to the rules of `steps::story::tour_problems`: 10 to 30 beats, at most 140 characters per
  sentence, no question, and every `enter` names a lane. Every step
  carries the id of its message, the messages on the wire after it (a broadcast shares one
  `fan`), and each lane's typed state with what the step wrote on its disk. Both drivers fill
  them.
- `src/paxos/trace/compart.rs` → the compartmentalized page (#943 (compartmentalized
  diagram)): the game colocates every role, so this page has its own small driver, as the
  `proxy_leader` and `replica_tier` examples of `paros-consensus` do. Three `ColocatedNode`s, two
  `ProxyLeader`s, two `ReplicaNode`s and a `Batcher`; the driver only moves messages, and
  every state line and quorum answer is read off those roles. The handoff run stays a
  `Recorder` run on its level, with its own lanes. Its "Break the rule" run has a `Commit`
  that the driver writes (`Run::forge_commit`): a proxy leader that commits after one vote.
  Its tour run, `tour` (chapter "Split the leader"), is one world: node 0 first writes alone
  (`Delegation::Colocated`; the replicas do not run yet, so no message goes to them), then
  the replicas start and catch up on node 0's beat, the proxy leaders and the batcher enter, a proxy leader crashes and a
  replica reads with no leader.
  The matchmaker page's tour run, `tour` (chapter "Change the acceptors",
  `trace/pages/matchmaker/tour.rs`), is one world too: the matchmakers before Phase 1, a new
  set of acceptors as a new ballot, two `Roll`s that cut the log into segments and the chain
  of segments that the matchmakers keep, the leader's
  crash and a Phase 1 over the old set and the new one, then the retirement of node 2.
  `trace/compart/desk.rs` → `Desk`, `Verb` → the same deployment driven by hand, the world of
  the lesson 6 exercises (deliver or lose each message, a client write, a beat of node 0 or of
  a proxy leader, a proxy leader crash). The guided lines of those exercises read it through
  its accessors only (`chosen`, `learned`, `written`, `counts`, `knows`, `held`, `taken_back`,
  `lost`).
- `src/paxos/picker.rs` → `check(shape, down)`: which quorums `paros-consensus` finds in a shape
  with some acceptors stopped; the wasm export `paxos_quorums` serves the quorums page's
  picker (#948 (quorum shapes and picker)).
- A "Break the rule" run (#946) causes its fault from **outside** the core, with verbs the
  shipped code refuses or never takes: `Wipe` boots an acceptor of the single-decree world on
  an empty disk, `Reconfigure` there gives one proposer its own acceptors with no matchmaker,
  `SkipPhase1` sends Accepts that no Phase 1 opened (log world), and the compartmentalized
  driver writes a proxy leader's `Commit` after one vote. The core is never changed; the
  receivers are the shipped code. `DecreeWorld::violation` and `DecreeWorld::decisions`
  record the two values chosen in the single-decree world; in the log world and on the
  compartmentalized page the recorder marks a step `broken` when two nodes hold two values
  chosen for one slot.
- `src/paxos/lab.rs` → `Exercise`, `Lab`, `Move`, `LabView` (wasm `PaxosLab`) → the "your turn"
  exercises of the Paxos course: a level's world that the reader drives by hand (deliver or
  drop each message, plus the few extra moves an exercise lists), and a goal read off the
  world. The view carries each lane's log (`logs`) and its typed state (`nodes`: up, role,
  holes), the same cells and states the trace gives the story, so the page draws the same
  stage. An exercise's setup (an election, a first write) is played with the same helpers as
  a solution and is not the reader's: undo never takes it back. An exercise runs a level
  (`Game`) or the compartmentalized deployment (`Desk`); its extra moves are `Extra::Act` or
  `Extra::Compart`. A rule that an exercise turns off is a driver move the shipped code refuses
  (`Wipe`), never a change of the core, and the exercise says so (`rule_off`). Each exercise
  has a solution that a test plays, so an exercise the core no longer lets a reader solve
  fails CI. Undo is a replay: the engine draws no randomness. The exercises live in one file
  per page (`lab/single_decree.rs`, `lab/quorums.rs`, `lab/multi_paxos.rs`,
  `lab/compartmentalized.rs`, `lab/matchmaker.rs`), their tests in `lab/tests.rs`.
- A **guided exercise** (plan `paxos-guided-tour-2026-11-24`, step F4; `gap` is the pilot, and
  the three single-decree exercises and the two quorum exercises are guided too) adds three lists to its `Exercise`; the
  others keep them empty (`NONE`, `NO_PARTS`) until their flavor fills them:
  - `intro`: the setup as `Step`s, each a sentence and the moves it plays. `Lab::start`
    records each step's frames (one per move: the message and the `LabView` after it), and the
    page plays them (`PaxosLab::intro_start`, `intro_step(n)`).
  - `parts`: the goal as a checklist. A `Part` has a label, a predicate (`holds`), a hint in
    three levels (`idea`, `plan`, `next`: the move to press now) and a dead end (`dead`,
    `dead_says`: the line cannot hold again). The goal is that every part holds (a test checks
    it). `LabView` gives `parts`, `hint` (of the first line that does not hold) and `stuck`.
    `DEAD_ENDS` in `lab/tests.rs` names the parts of each guided exercise that have a dead end
    (none for `two-values`: with the disk rule off, a wipe and a new ballot always remain), and
    the random walk of the tests must meet exactly those. "No value is chosen" in `duel` and
    `vote-not-chosen` is `DecreeWorld::chosen_by_votes`: a Phase-2 quorum voted for one value at
    one ballot, before any proposer learns it. `two-values` judges what a proposer learns
    (`chosen` of the view, `DecreeWorld::violation`), and its labels say so.
  - `guide`: the solution as `Step`s. The tests play it, and "Show me how" plays it on the page
    (`PaxosLab::guide_step(n)`). A guide step makes the lab a demo: its goal never counts as
    solved, and the reader's moves wait until Start again.
- `src/paxos/ready_lab.rs` → `ReadyLab` (wasm `ReadyLab`) → the sans-IO page's `Ready` inspector (#951): three
  `ColocatedNode`s on the game's `Disk`, a wire driven by hand (`timeout`, `tick`, `deliver`,
  `drop`, `write`), and each `Ready` an input gave, bucket by bucket. A driver, no protocol logic.
- `src/paxos/quorum_reach.rs` → `reach` (wasm `quorum_reach`) → the grid-quorums page's picker (#951):
  `QuorumSystem::partitions` and `AcceptorConfig::has_phase1_quorum` / `has_phase2_quorum_in` over
  the acceptors that are up.
- `tests/paxos_traces.rs` → writes `website/site/src/traces/*.json` (committed, diffed in CI).
- `src/steps.rs` + `src/steps/story.rs` → the JSON shape of every step-through (`Trace`,
  `Scenario`, `Step`, `Beat`, …) and `assemble`.
- `tests/paxos_world.rs` → the log world driven directly: election, apply, heal, restart, seams, reads, recovery, the matchmaker plane.

The `applied` log in `world/disk.rs` is the **game's own** application stand-in, kept to teach
"chosen is not applied": paros itself runs no application (#186).

### Changing a run

1. A trace script is in `src/paxos/trace/pages/` (one file per page: `single_decree.rs`,
   `multi_paxos.rs`, `matchmaker.rs`, `quorums.rs`; the compartmentalized page is
   `trace/compart.rs`). It runs on a setup of `src/paxos/level/`.
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

## Tests & gates

Prefix each with `nix develop --command`. CI's `wasm` job runs the traces diff and the wasm
build; clippy and nextest cover the crate through the workspace.

- `cargo nextest run -p paros-wasm`
- `cargo test -p paros-wasm --test paxos_traces` then `git diff --exit-code -- website/site/src/traces`
- `cargo check --target wasm32-unknown-unknown -p paros-wasm --no-default-features --features paxos`
- `scripts/build-wasm.sh --wasm-only` (wasm-bindgen output into `target/wasm-tools/paxos/`, not
  committed)
- Full deploy build: `scripts/build-wasm.sh`, then `npm run build` in `website/site/` (the Pages
  workflow).

## Deps & pins (`Cargo.toml`)

`paxos`: `serde`, and `paros-consensus` (`default-features = false`, `serde`); on wasm32 also `serde_json`, the JSON of the exports. The native tests take `serde_json` as a dev-dependency.
wasm-only `wasm-bindgen = "=0.2.130"` (must equal the flake's `wasm-bindgen-cli`, bumped
together) and `console_error_panic_hook`.
