# Generated TypeScript bindings

`cargo test -p paros-play` writes this directory from the `ts_rs::TS` derives in
`crates/paros-play/src/view.rs`, `action.rs`, `auto.rs`, `prompt.rs` and
`level/mod.rs` (see `crates/paros-play/tests/bindings.rs`). The files are
committed and CI fails on a diff, so never hand-edit them: change the Rust type
and re-run the test.

Two conventions worth knowing before you read them:

- `u64` and `usize` are generated as `number`, not `bigint`: every value on this
  boundary is small, and `JSON.parse` — how the browser reads all of it —
  produces `number`.
- A ballot is always the string `"round.node"`, and a value is always its UTF-8
  text. Neither is ever a structured object.
