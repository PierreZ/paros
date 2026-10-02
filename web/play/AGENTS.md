# web/play

The browser half of paros play; the engine is `crates/paros-play` (compiled to wasm). The dev
loop, the gates and the layout are in the README:

@README.md

Two rules:

- **No protocol logic here.** The app renders what the engine returns and sends what the player
  does; a fact the UI needs belongs in the view contract (`crates/paros-play/src/view.rs`).
- **Never hand-edit `src/generated/`.** It is written by `cargo test -p paros-play --test
  bindings` from the `ts_rs::TS` derives, committed, and diffed in CI. `src/wasm/` is
  `scripts/build-play.sh` output and gitignored.
