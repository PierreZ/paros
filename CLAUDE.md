# paros

Learning project: implementing the Paxos consensus algorithm in Rust. WIP, not for production.

## Build & test

Dev shell is a Nix flake — enter `nix develop` (or rely on direnv) before running commands.

- `cargo build`
- `cargo nextest run` (fall back to `cargo test`)
- `cargo fmt` + `cargo clippy -- -D warnings` before committing

Rust 2024 edition, toolchain pinned in `rust-toolchain.toml`. Clippy pedantic is on (`Cargo.toml`).

## Layout

- `src/` — library crate (`paros`).
- `docs/references/papers/` — Paxos/consensus papers with transcripts.
- `docs/analysis/` — design notes (e.g. sans-IO patterns for Multi-Paxos).
