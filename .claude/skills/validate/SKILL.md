---
name: validate
description: Run paros's full local gate the way CI's jobs do - cargo fmt, the module-style and core-layers scripts, clippy --all-targets -D warnings, rustdoc -D warnings on paros-core, paros-storage and paros-testkit, nextest (ci profile), the seven paros-core examples, the wasm32 and no-default-features portability checks of paros-core, and TLC on specs/tla when a spec changed - through Nix (nix develop, locally and on Claude Code on the web). Use before declaring any change done, before committing, when asked "will CI pass", and after any Cargo.toml or feature change.
---

# Validate

`.github/workflows/rust.yml` has these jobs: `scope` (it decides which jobs a pull request
runs), `clippy`, `fmt`, `test`, `examples` and `portability`. `.github/workflows/tla.yml` runs
TLC when a spec, its script or the flake changes. Every Rust command runs through
`nix develop --command`. The sandbox's `/root/.cargo` toolchain is off-limits in this project:
the pinned toolchain is `rust-toolchain.toml` (its channel, with `wasm32-unknown-unknown`), and
results from anything else do not predict CI.

## Pick the runner

Prefix every command with `nix develop --command` (or rely on direnv), in a local session and
on Claude Code on the web alike. The flake's inputs are fetched over git, which the web
session's egress policy allows (#727 (flake inputs over git)). On the web, install Nix first if
`nix-store` is missing, exactly as the root `AGENTS.md` describes.

Below, `RUN` stands for `nix develop --command`.

## The gate, in CI order

```bash
RUN cargo fmt --all -- --check              # job: fmt   (run `cargo fmt` to fix)
scripts/check-module-style.sh                # job: fmt
scripts/check-core-layers.sh                 # job: fmt
RUN cargo clippy --all-targets -- --deny warnings                       # job: clippy
RUSTDOCFLAGS="-D warnings" RUN cargo doc -p paros-core -p paros-storage -p paros-testkit --no-deps   # job: clippy (doc lints)
RUN cargo nextest run --profile ci           # job: test  (fallback: cargo test)
for ex in single_decree multi_paxos matchmaker \
          acceptor_grid quorum_read proxy_leader replica_tier; do        # job: examples
  RUN cargo run -p paros-core --example "$ex"
done
RUN cargo check --target wasm32-unknown-unknown -p paros-core           # job: portability
RUN cargo check --target wasm32-unknown-unknown -p paros-core --no-default-features
RUN cargo check -p paros-core --features serde
nix develop .#tla --command scripts/check-tla.sh                        # workflow: TLA (when specs/tla changed)
```

## How to read a failure

- **Clippy pedantic is on** (`[workspace.lints]`) and `clippy.toml` **denies
  `HashMap`/`HashSet`** (randomized iteration breaks deterministic replay): use
  `BTreeMap`/`BTreeSet`. Fix the code; do not add `#[allow]`.
- **A public `paros-core` function that can panic needs a `# Panics` doc section** (pedantic
  `missing_panics_doc`); hard `assert!` is the house style there, so write the section rather
  than removing the assert.
- **A failing seeded model of `paros-core`** (`models/`) is a safety violation or a
  determinism break, never a flake: find the cause (`AGENTS.md`, "A flaky check is a finding").
- **A wasm check failure** in `paros-core` usually means a new dependency or a
  `std::time`/`rand` use crept into the core; the core is dependency-free with
  `--no-default-features`.
