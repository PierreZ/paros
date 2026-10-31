# website/play

The browser half of paros play; the engine is the `paxos` feature of `crates/support/paros-wasm`
(compiled to wasm, bundle `paros_paxos`). The dev
loop, the gates and the layout are in the README:

@README.md

Two rules:

- **No protocol logic here.** The app renders what the engine returns and sends what the player
  does; a fact the UI needs belongs in the view contract (`crates/support/paros-wasm/src/paxos/view.rs`).
- **Never hand-edit `src/generated/`.** It is written by `cargo test -p paros-wasm --test
  paxos_bindings` from the `ts_rs::TS` derives, committed, and diffed in CI. `src/wasm/` is
  `scripts/build-wasm.sh` output and gitignored.

## Look (#804 (flat engineering redesign))

`src/styles.css` imports `website/shared/tokens.css` and `website/shared/base.css` and defines
no colour: `--page`, `--ink` and `--ink-soft` are names for the shared `--bg`, `--fg` and
`--muted`, and the status, `--*-dark`, `--phase-*` and `--column-*` colours are tokens. The page
has the site's top bar (`index.html`); a scenario framed on a book page (`?embed`) hides it.
`src/main.ts` imports `website/shared/theme.js`, so the reader's theme choice
(`localStorage["theme"]`) is the same in the game, the book and the sandbox; a `?theme=` or the
holding page's message wins over it. Flat rules: no radius, no soft shadow, mono type.
