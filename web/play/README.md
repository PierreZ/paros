# web/play — the paros play frontend

The browser half of **paros play**, the interactive Paxos game. The engine is
`crates/paros-play`, compiled to WebAssembly; everything here renders what it
returns and collects what the player does. No protocol logic lives in this
directory, and none ever should: if the UI needs a fact, the fact belongs in
the view contract.

## The dev loop

Every tool comes from Nix. Build the wasm once, then run vite:

```sh
nix develop --command scripts/build-play.sh --wasm-only   # writes src/wasm/
nix develop --command bash -c 'cd web/play && npm ci && npm run dev'
```

Then open <http://localhost:5173/>. Rebuild the wasm (the first command) after
any change under `crates/paros-play`; vite reloads the rest on save.

The four gates, the same ones CI runs:

```sh
nix develop --command bash -c 'cd web/play && npm ci && npm run check && npm test && npm run build'
```

The full deploy build — wasm, bundle, and the copy into `book/output/play/` —
is `nix develop --command scripts/build-play.sh`, after `mdbook build`.

## Layout

| path | what it is |
|---|---|
| `src/main.ts` | boot, routing, the render loop, the stage's click handling |
| `src/game.ts` | the typed wrapper over `WasmGame`: JSON in, `GameView` or `ErrorView` out |
| `src/route.ts` | the URL hash ⇄ level id (`#act1/choose-a-value`; no hash is the level map) |
| `src/progress.ts` | `localStorage`: levels passed, mistakes, automation unlocked |
| `src/narration.ts` | narration: the last move's lines (the caption) and the log's whole stream |
| `src/types.ts` | re-exports of the generated contract |
| `src/ballot.ts` | reads the printed `round.node` ballot back, for the one ballot a player passes on |
| `src/render/` | the SVG stage: `layout.ts` is the geometry, `grid.ts` reads the acceptor grid, `matchmaker.ts` is the matchmaker band's geometry and labels, `disk.ts` says which disks are gone, `stage.ts` draws |
| `src/ui/` | the panel, the prompt card, the wire list, the controls, `matchmakers.ts` for the matchmaker plane's own controls, the quorum sentences, the refusal, the client history, the level map |
| `src/generated/` | **the contract — never hand-edited** (see below) |
| `src/wasm/` | wasm-bindgen output, gitignored, produced by the build script |
| `src/fixtures/` | one captured `GameView`, for the tests |

## The contract rule

`src/generated/` is written by `cargo test -p paros-play` from the `ts_rs::TS`
derives in the Rust crate, is committed, and CI fails on a diff. **Never edit a
file in it.** A field the UI wants is a change to `crates/paros-play/src/view.rs`
followed by re-running that test.

Four conventions the generated types do not spell out: every number is a
`number` (never a `bigint`), a ballot is always the string `"round.node"`, and
a command's text is plain — the engine strips Rust's quoting before it sends
it. A slot that holds one of paros's own control commands says so in
`SlotView.control` (`noop`, `truncate`, `snap`), and the stage prints that name
in the box in place of the text. Node ids and matchmaker ids are **two identity
spaces**: matchmaker 0 and node 0 are two processes, and a matchmaker is written
`m0` everywhere the player reads one.

## What the engine says, and the UI must not work out

These facts arrive as fields, and the frontend must read them there:

- **A request or a reply** is `MessageView.reply`. Do not read the variant's
  name.
- **A control command** is `SlotView.control` / `ChosenView.control`. A slot
  with `null` there holds opaque client bytes.
- **The reach sets** of the single-decree world are `WorldView.reach`, so the
  checkboxes stay correct through an undo and a reset. The frontend keeps no
  copy of them.
- **A node's role** is `NodeView.role`, and a single-decree proposer holds
  `NodeView.attempt` instead.
- **The quorum system** is `NodeView.quorum`, a structure: `kind` plus `q1`/`q2`
  for a flexible split and `rows`/`cols` for a grid. `NodeView.quorum_system` is
  a sentence for a human and is never parsed. A majority sends no numbers, so
  the panel prints no count for one.
- **Where an acceptor sits in a grid** is `NodeView.grid_cell`, and **which
  column an Accept was addressed to** is `MessageView.column`. The frontend
  works neither out from the slot, even though the rule is public.
- **Which tier a message's endpoint belongs to** is `MessageView.from_party` /
  `to_party`. The two id spaces are separate, so the stage resolves an endpoint
  through that field and never through the message's name. `groupByLink` keys a
  link by the tier and the id at each end, so a node and a matchmaker with the
  same number never share a link.
- **The acceptor set in force** is `NodeView.acceptors`, and **the ballot it is
  bound to** is `NodeView.acceptors_since`. A configuration is never edited: it
  belongs to one ballot. The badge is drawn only where a deployment names
  matchmakers, because a plain deployment keeps one set for life.
- **The matchmaker tier's own state** is `WorldView.matchmakers`: the generation
  and phase (`MatchmakerPhaseView`), the registry (`RegistrationView`, a ballot,
  its members and whether it is a belief or a change), the floor
  (`gc_watermark`) and the successor a frozen matchmaker points at. The band
  draws those and nothing else.
- **An open matchmaking phase** is `NodeView.matchmaking`, **the floor a
  matchmaker quorum made effective** is `NodeView.gc` (with the acceptors it
  released), **the step of a handover** is `NodeView.handover`, and **a node
  that retired** is `NodeView.retired`. A retired node is drawn hollow, labelled
  `retired`, and draws no log: it does not come back.

One fact has no field yet. A node whose disk the player **erased** is drawn
hollow, and `src/render/disk.ts` reads `NodeView.wiped` first — the field the
contract should grow. Until it does, the fallback is two things the engine
already reports together: the action log holds a `wipe` entry naming the node,
and that node's disk reads empty. A node that merely crashed is therefore never
drawn as a lost disk.

## What the UI must not decide

- **Which controls exist** comes from `LevelView.allowed_actions` and the world
  flavour, never from the level id.
- **Whether a move was legal** is the engine's answer: an `ErrorView` is shown
  as a refusal and the board is left alone.
- **Whether an answer was right** is the engine's too — a wrong answer keeps the
  prompt open and renders `PromptView.feedback`; the world does not move.
- **The teaching order** is the plan's education rule: the briefing, the
  narration and the prompt are the page; `paros-core` symbol names live in the
  "In the code" footnote at the bottom, beside the field-guide link.
- **The reward** is `LevelView.unlocks`, which the panel prints as the
  automation the player gets when the level is passed.

All the text in this directory follows ASD-STE100: short active sentences,
present tense, one instruction per sentence, no idioms, and `must` for an
obligation. The `field_guide` field is a bare book filename, and the app is
served beside the book, so a link to it is `../` plus that name.
