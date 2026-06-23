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

Sans-IO core driven by moonpool (etcd-raft `RawNode` model). `paros-core` is a pure synchronous
state machine — `step`/`tick` in, one `Ready` out, `advance()` handshake; no I/O, clock, RNG, or
deps. The `ready()`/`advance()` handshake is type-enforced: `ready(&mut self) -> Ready<'_>` holds
the node's unique borrow, so a second `ready()` before `advance()` is a *compile* error.
Persist-before-send durability ordering is documented on `Ready`/`HardState`. Contract reference:
`docs/analysis/go-raft/etcd-raft-sans-io-patterns.md`.

## Layout

Cargo workspace (mirrors moonpool); dependency arrows point only *into* `paros-core`.

- `paros-core/` — sans-IO Multi-Paxos state machine: zero deps, std-only, wasm-safe. Sancov
  crate-under-test; exempt from the global `#[instrument]`-on-pub-fns rule (must stay zero-dep).
- `paros-storage/` — `Storage` implementations + the seeded faulty in-memory fake (Stage 4+).
- `paros-sim/` — moonpool-backed sim driver, workloads, oracles (wasm-safe, `default-features = false`).
- `paros-sim-runner/` — native sim runner binary (`publish = false`).
- `paros-wasm-demo/` — browser/wasm demo, `cdylib` + `rlib` (`publish = false`).
- `paros/` — user-facing facade crate; the client API and binary land here.
- `xtask/` — build automation (the sancov sim runner).
- `docs/references/papers/` — Paxos/consensus papers with transcripts.
- `docs/analysis/` — design notes (e.g. sans-IO patterns for Multi-Paxos).

Publishing/changelogs mirror moonpool: library crates publishable with a shared `version_group`
and per-crate `CHANGELOG.md` (created by release-plz); binaries/demos/xtask are `publish = false`.
