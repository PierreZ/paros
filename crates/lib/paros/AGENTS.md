# paros

The facade. It bundles every library crate of paros into one crate, so a program (and the
binaries to come, like `parosd` and `parosctl`) depends on `paros` alone. Today there is one
layer, `paros::consensus` (the sans-IO consensus core, `crates/lib/paros-consensus`). It stays
wasm-checkable and provider-free.

Three rules (Pierre, 2026-11-19):

1. A binary depends on `paros` alone, never on a layer crate directly. The facade is the one
   public API to keep stable.
2. `paros` holds no code. It only re-exports each layer as a module. Logic lives in a layer
   crate, so each layer is tested and proved alone.
3. No layer crate depends on `paros`. A test crate (`consensus-sim`) depends on the
   layer it judges, never on the facade.

A layer depends only on the layers below it. Put new code in the lowest layer that can hold it.

The journal layers that sat above the core were removed on 2026-11-19. They are at the git tag
`v0.2.0-alpha.1`; `docs/archive/v0.2.0-alpha.1/README.md` maps them. `docs/architecture/` is the
target design that new layers grow toward.
