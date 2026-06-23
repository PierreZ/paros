# paros

Learning project: implementing the Paxos consensus algorithm in Rust. WIP, not for production.

## Build & test

Dev shell is a Nix flake — enter `nix develop` (or rely on direnv) before running commands.

- `cargo build`
- `cargo nextest run` (fall back to `cargo test`)
- `cargo fmt` + `cargo clippy -- -D warnings` before committing

Rust 2024 edition, toolchain pinned in `rust-toolchain.toml` (incl. the `wasm32-unknown-unknown`
target). Clippy pedantic is on (`[workspace.lints]` in `Cargo.toml`).

- `cargo check --target wasm32-unknown-unknown -p paros-core` — portability gate; `paros-core`
  must stay buildable for wasm (CI enforces it).
- `cargo xtask sim …` — sancov-instrumented simulation runner (`scripts/sancov-rustc.sh` is the
  `RUSTC_WRAPPER`, gated by `SANCOV_CRATES`; the flake `shellHook` exports it). Registry is empty
  until Stage 1.

## Architecture

Sans-IO core driven by moonpool (etcd-raft `RawNode`/`Node` model). `paros-core` is a pure
synchronous state machine — `step`/`tick` in, one `Ready` out, `advance()` handshake; no I/O, clock,
RNG, or deps. The `ready()`/`advance()` handshake is type-enforced: `ready(&mut self) -> Ready<'_>`
holds the node's unique borrow, so a second `ready()` before `advance()` is a *compile* error.
Persist-before-send durability ordering is documented on `Ready`/`HardState`. Contract reference:
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`.

The **driver** (`paros::run_node`, the etcd-raft `Node` layer) owns the `RawNode` and does all I/O.
It is written **once, generic over moonpool's `P: Providers`** (and `S: NodeStorage`), so the *same*
code runs in production (`TokioProviders` + a future `parosd` binary) and deterministic simulation
(`SimProviders`). The boundary is the only thing that differs: `paros-sim` adapts it to a moonpool
`Process`; production adapts a `tokio::main`. This "test the code you ship" rule is load-bearing —
protocol logic added in later stages lives in the provider-generic driver, never in a sim-only path.

## Layout

Cargo workspace (mirrors moonpool). Dependency stack: `paros-core` ← `paros` ← `paros-sim` ←
{runner, wasm-demo}. `paros-core` has no deps; everything ultimately points into it.

- `paros-core/` — sans-IO Multi-Paxos state machine: zero *default* deps, std-only, wasm-safe (an
  optional `serde` feature adds derives only). Sancov crate-under-test; exempt from the global
  `#[instrument]`-on-pub-fns rule (must stay zero-dep by default).
- `paros/` — **the library.** Re-exports `paros-core`, plus the provider-generic driver
  (`run_node` over `P: Providers`, `S: NodeStorage`), the default in-memory `MemStorage`, and the
  node RPC contract (`Propose`/`ProposeAck`). The client API + a `parosd` binary land here. Deps:
  `paros-core`, `moonpool-core` + `moonpool-transport` (`default-features = false` → wasm-safe). No
  dedicated storage crate — the Stage-4+ faulty fake lands here or in the harness.
- `paros-sim/` — the DST harness on top of `paros`: the moonpool `Process` adapter, workloads,
  oracles (wasm-safe, `default-features = false`). Depends on `paros` + `moonpool-sim`.
- `paros-sim-runner/` — native sim runner binary (`publish = false`).
- `paros-wasm-demo/` — browser/wasm demo, `cdylib` + `rlib` (`publish = false`).
- `xtask/` — build automation (the sancov sim runner).
- `docs/references/papers/` — Paxos/consensus papers with transcripts.
- `docs/analysis/` — design notes (e.g. sans-IO patterns for Multi-Paxos).

Publishing/changelogs mirror moonpool: library crates share a `version_group` with per-crate
`CHANGELOG.md` (release-plz); binaries/demos/xtask are `publish = false`. Note: `paros` and
`paros-sim` depend on moonpool via a **git** pin, so they are *not* `cargo publish`-able until a
moonpool release is pinned — `paros-core` is currently the only truly publishable crate.
