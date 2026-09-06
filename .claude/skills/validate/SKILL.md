---
name: validate
description: Run paros's full local gate the way CI does - cargo fmt, clippy --all-targets -D warnings, rustdoc -D warnings on paros-core, nextest, the three paros-core examples, the wasm32 and no-default-features portability checks, and cargo xtask sim run-all when the protocol or harness changed - through Nix (nix develop locally, nix shell nixpkgs#rustup on Claude Code on the web). Use before declaring any change done, before committing, when asked "will CI pass", and after any Cargo.toml or feature change.
---

# Validate

`.github/workflows/rust.yml` has six jobs and every one runs through
`nix develop --command`. The sandbox's `/root/.cargo` toolchain is off-limits
in this project: the pinned toolchain is `rust-toolchain.toml` (1.95.0 with
`wasm32-unknown-unknown`), and results from anything else do not predict CI.

## Pick the runner

- **Local session** (`CLAUDE_CODE_ENTRYPOINT` is `cli`/`vscode`): the flake dev
  shell. Prefix every command with `nix develop --command` (or rely on direnv).
- **Claude Code on the web** (`CLAUDE_CODE_ENTRYPOINT` starts with `remote`):
  the flake's inputs are egress-blocked, so `nix develop` cannot build. Use a
  Nix-provided rustup, which reads `rust-toolchain.toml`:
  `nix shell nixpkgs#rustup -c cargo …` (add `nixpkgs#cargo-nextest` for
  nextest). Install Nix first if `nix-store` is missing, exactly as the root
  `AGENTS.md` describes (`nix-bin`, `NIX_CONFIG`, `NIX_SSL_CERT_FILE`).

Below, `RUN` stands for whichever prefix applies.

## The gate, in CI order

```bash
RUN cargo fmt --all -- --check              # job: fmt   (run `cargo fmt` to fix)
RUN cargo clippy --all-targets -- --deny warnings                       # job: clippy
RUSTDOCFLAGS="-D warnings" RUN cargo doc -p paros-core --no-deps        # job: clippy (doc lints)
RUN cargo nextest run                        # job: test  (fallback: cargo test)
RUN cargo run -p paros-core --example single_decree                     # job: examples
RUN cargo run -p paros-core --example multi_paxos
RUN cargo run -p paros-core --example matchmaker
RUN cargo check --target wasm32-unknown-unknown -p paros-core           # job: portability
RUN cargo check --target wasm32-unknown-unknown -p paros-core --no-default-features
RUN cargo check -p paros-core --features serde
RUN cargo check --target wasm32-unknown-unknown -p paros
RUN cargo xtask sim run-all                  # job: sim — the sancov sweep; see /sim-sweep
```

Run the sim job whenever the change touches `paros-core`, the driver, the
harness, a hook, a knob, an audit message, or the moonpool pin. It is the real
gate: `sim-paros-chain` exits non-zero on any assertion violation, failed run,
coverage gate that never fired, or convergence timeout, and then runs the two
corpus axes with the same rule. A docs-only change can skip it.

## How to read a failure

- **Clippy pedantic is on** (`[workspace.lints]`) and `clippy.toml` **denies
  `HashMap`/`HashSet`** (randomized iteration breaks deterministic replay):
  use `BTreeMap`/`BTreeSet`. Fix the code; do not add `#[allow]`.
- **A public `paros-core` function that can panic needs a `# Panics` doc
  section** (pedantic `missing_panics_doc`); hard `assert!` is the house style
  there, so write the section rather than removing the assert.
- **A nextest sim smoke failure** (`crates/paros-sim/tests/sim.rs`,
  `tests/corpus.rs`) is a safety-oracle violation or a determinism break, never
  a flake: replay the seed (`/debug-a-seed`).
- **A wasm check failure in `paros-core`** usually means a new dependency or a
  `std::time`/`rand` use crept into the core; the core is dependency-free with
  `--no-default-features`.
- **A coverage gate that never fired** in the sweep is a finding about reach,
  not noise; do not delete the gate (`/adding-an-audit-check`).

The book is built by `pages.yml`, not by `rust.yml`; if you touched
`book/src/`, also run `RUN mdbook build` (`/update-the-book`).
