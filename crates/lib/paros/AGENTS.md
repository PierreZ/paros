# paros

The facade (#722 (three products)). A program depends on `paros` alone. It holds no code of its
own: each layer of the library is a module (`paros::core`, `paros::wire`, `paros::model`,
`paros::tunables`, `paros::storage`, `paros::net`, `paros::client`, `paros::observe`,
`paros::roles`, `paros::coordinator`, `paros::machine`), and the `run_*` entry points sit at the
root. It stays wasm-checkable and provider-free.

## Layers

Each layer is its own crate under `crates/lib/`, with its own `AGENTS.md` map. A layer depends
only on the layers below it (root *Layout*, *Layers point down only*):

`paros-core` ← `paros-wire` ← `paros-model` ← `paros-tunables` ← `paros-storage` ← `paros-net` ←
`paros-client` ← `paros-observe` ← `paros-roles` ← `paros-coordinator` ← `paros-machine` ←
**`paros`**.

Put new code in the lowest layer that can hold it. A type two layers share moves down. A call up
goes through a port, a trait the upper layer implements (`CellHost`).

## Map

- `src/lib.rs` → the module per layer and the root re-exports.

## Public surface

`run_machine`, `run_node`, `run_journals`, `run_matchmaker`, `run_proxy`, `run_replica`,
`run_batcher`, `run_frontend`; `paros::inspect`; one module per layer.

## Local rules (the whole library)

- **Faults are inline** (root *Simulation rules*, #294). A new driver decision is an inline
  `buggify_with_prob!` / `buggify_pick!` at the line that makes it, consulted only where its
  answer is observable. A moment where a crash is interesting (staged not synced, durable not
  sent) is `hint!("label").await` at that line. Never add a hook trait (the deleted
  `DriverHooks`, `ClientHooks`, commit hooks), a `Seam` variant, or a store or disk trait whose
  only second implementation would be a sim wrapper. A path the code walks gets a `reachable!`
  probe, with the message the sim's gate already uses, never reworded.
- A new tunable is a `DriverTunables` field with a default (`paros-tunables`) and a
  `buggify_knob!` in `paros-sim`'s `NodeShape`; a new durability boundary is an inline
  `hint!("label").await` with a `reachable!` on `Strike::Killed`.
- Every call is one at-most-once attempt (`try_get_reply`), never `get_reply`.
- `paros::client` draws no randomness: every choice is the caller's.
- Storage implementations pass both contract suites.
- Spans are non-optional (`#[tracing::instrument(skip_all, fields(..))]`); see the root
  *Audit, correctness, assertions, spans*.

## Tests & gates

- `cargo nextest run` per layer crate (each map names its tests).
- `cargo check --target wasm32-unknown-unknown -p paros` (CI `portability`).
- Building needs `protoc` (`paros-wire`'s `build.rs` runs `prost-build`): the flake ships
  `protobuf`; on the web see the root *Environment & Nix* (`PROTOC`).

## Deps & pins

- Every layer takes its dependencies from the root `[workspace.dependencies]`.
- `prometheus` without default features: the machine's metrics (#366 (metrics)), wasm-safe.
- The moonpool pin is **seven lines**: the six `moonpool-*` lines of the root
  `[workspace.dependencies]` and `paros-core`'s own `moonpool-assertions` line. Advance them
  together.
