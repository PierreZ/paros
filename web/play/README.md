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
| `src/render/` | the SVG stage: `layout.ts` is the geometry, `stage.ts` draws |
| `src/ui/` | the panel, the prompt card, the wire list, the controls, the client history, the level map |
| `src/generated/` | **the contract — never hand-edited** (see below) |
| `src/wasm/` | wasm-bindgen output, gitignored, produced by the build script |
| `src/fixtures/` | one captured `GameView`, for the tests |

## The contract rule

`src/generated/` is written by `cargo test -p paros-play` from the `ts_rs::TS`
derives in the Rust crate, is committed, and CI fails on a diff. **Never edit a
file in it.** A field the UI wants is a change to `crates/paros-play/src/view.rs`
followed by re-running that test.

Three conventions the generated types do not spell out: every number is a
`number` (never a `bigint`), a ballot is always the string `"round.node"`, and
a command's text is plain — the engine strips Rust's quoting before it sends
it. A slot that holds one of paros's own control commands says so in
`SlotView.control` (`noop`, `truncate`, `snap`), and the stage prints that name
in the box in place of the text.

## What the engine says, and the UI must not work out

Four facts arrive as fields, and the frontend must read them there:

- **A request or a reply** is `MessageView.reply`. Do not read the variant's
  name.
- **A control command** is `SlotView.control` / `ChosenView.control`. A slot
  with `null` there holds opaque client bytes.
- **The reach sets** of the single-decree world are `WorldView.reach`, so the
  checkboxes stay correct through an undo and a reset. The frontend keeps no
  copy of them.
- **A node's role** is `NodeView.role`, and a single-decree proposer holds
  `NodeView.attempt` instead.

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
