# paros

The facade. It bundles every library crate of paros into one crate, so a program (and the
binaries to come, like `parosd` and `parosctl`) depends on `paros` alone. Today there are three
layers: `paros::consensus` (the sans-IO consensus core, `crates/lib/paros-consensus`),
`paros::journal` (the pure journal core, `crates/lib/paros-journal`, on the consensus core) and
`paros::journal_io` (the journal's traits, `Host` and generic driver,
`crates/lib/paros-journal-io`; #1329). It
stays wasm-checkable and provider-free.

Three rules (Pierre, 2026-11-19):

1. A binary depends on `paros` alone, never on a layer crate directly. The facade is the one
   public API to keep stable.
2. `paros` holds no code. It only re-exports each layer as a module. Logic lives in a layer
   crate, so each layer is tested and proved alone.
3. No layer crate depends on `paros`. A test crate (`consensus-sim`, `journal-sim`) depends on
   the layer it judges, never on the facade.

A layer depends only on the layers below it. Put new code in the lowest layer that can hold it.

The facade may bundle moonpool's storage and network (Pierre, 2026-11-21): when the adapter
crate exists (`paros-journal-moonpool`, #1330), `paros` re-exports it behind a feature. The
default features stay provider-free (`scripts/check-core-deps.sh`), and no layer crate gains the
dependency (`docs/journal/04-roles.md` §J4.3).

**Examples.** `paros` is a composing crate, so it has examples (`AGENTS.md`, "Composing crates
have examples"). They import `paros::` paths only, never a layer crate, so they show what a
program that depends on `paros` alone writes. `examples/journal.rs` runs one journal in one
process on the in-memory backends: three acceptors, three replicas, one leader, one writer.
`examples/tokio_tasks.rs` runs the same journal on three tokio tasks of a `LocalSet`, with a
transport over tokio channels and a clock on `tokio::time` written in the example: it shows what a
program writes to bring its own runtime. `examples/full_journal.rs` runs every journal role on six
hosts, each a task of a multi-thread tokio runtime that share only `Arc<Mutex<..>>` state (the
network, the disks, the matchmakers): the register on three matchmakers through `RegisterCall`, a
grid of two columns, one leader at a time, the replicas and the pure client; then writes, reads, a
crash and restart of a host, and a roll to a second segment. tokio is a dev-dependency of `paros` only; the library
depends on no runtime.
Each new example joins the `checks` job of `.github/workflows/rust.yml`.

The journal layers of the old design were removed on 2026-11-19. They are at the git tag
`v0.2.0-alpha.1`; `docs/archive/v0.2.0-alpha.1/README.md` maps them. `docs/architecture/` is the
target design that new layers grow toward.
