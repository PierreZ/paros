# paros

The facade. It bundles every library crate of paros into one crate, so a program (and the
binaries to come) depends on `paros` alone. Today there is one layer: `paros::consensus` (the
sans-IO consensus core, `crates/lib/paros-consensus`). It stays wasm-checkable and provider-free.

Three rules (Pierre, 2026-11-19):

1. A binary depends on `paros` alone, never on a layer crate directly. The facade is the one
   public API to keep stable.
2. `paros` holds no code. It only re-exports each layer as a module. Logic lives in a layer
   crate, so each layer is tested and proved alone.
3. No layer crate depends on `paros`. A test crate (`consensus-sim`) depends on the layer it
   judges, never on the facade.

A layer depends only on the layers below it. Put new code in the lowest layer that can hold it.

The facade may bundle moonpool's storage and network (Pierre, 2026-11-21): when an adapter
crate exists, `paros` re-exports it behind a feature. The default features stay provider-free
(`scripts/check-core-deps.sh`), and no layer crate gains the dependency.

**Examples.** `paros` is a composing crate, so it has examples (`AGENTS.md`, "Composing crates
have examples"). They import `paros::` paths only, never a layer crate, so they show what a
program that depends on `paros` alone writes. `examples/consensus.rs` drives three
`ColocatedNode`s of plain Multi-Paxos with a driver loop written in the example (persist before
send): an election, a writer, two records, and a crash and restart that catches up. Each new
example joins the `checks` job of `.github/workflows/rust.yml`.

The journal was removed twice: the layers of the old service on 2026-11-19 (git tag
`v0.2.0-alpha.1`, `docs/archive/v0.2.0-alpha.1/README.md`), and the journal v1 of design A on
2026-11-24 (git tag `journal-design-a`, `docs/archive/journal-design-a/README.md`). The journal
is built again from scratch on the consensus core with segments (the plan is
`docs/archive/journal-design-a/prompt.md`). `docs/architecture/` is the target design that new
layers grow toward.
