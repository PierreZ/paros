---
name: validate
description: Run paros's full local gate the way CI's jobs do - cargo fmt, clippy --all-targets -D warnings, rustdoc -D warnings on paros-core, nextest, the eight paros-core examples, the wasm32 and no-default-features portability checks (paros-core, paros and paros-sim), the sim-wasm job (a seed of paros-sim is the same run on wasm32 as natively), the wasm job (ts-rs bindings diff, scripts/build-wasm.sh, npm check/test/build), and cargo xtask sim run-all when the protocol or harness changed - through Nix (nix develop locally, nix shell nixpkgs#rustup plus protobuf on Claude Code on the web). Use before declaring any change done, before committing, when asked "will CI pass", and after any Cargo.toml or feature change.
---

# Validate

`.github/workflows/rust.yml` has these jobs (`clippy`, `fmt`, `test`,
`examples`, `portability`, `sim-wasm`, `wasm`, `image`, `sim`, `sim-gates`) and every one runs through
`nix develop --command`. The sandbox's `/root/.cargo` toolchain is off-limits
in this project: the pinned toolchain is `rust-toolchain.toml` (1.95.0 with
`wasm32-unknown-unknown`), and results from anything else do not predict CI.

## Pick the runner

- **Local session** (`CLAUDE_CODE_ENTRYPOINT` is `cli`/`vscode`): the flake dev
  shell. Prefix every command with `nix develop --command` (or rely on direnv).
- **Claude Code on the web** (`CLAUDE_CODE_ENTRYPOINT` starts with `remote`):
  the flake's inputs are egress-blocked, so `nix develop` cannot build. Use a
  Nix-provided rustup, which reads `rust-toolchain.toml`. `crates/paros/build.rs`
  compiles the wire messages with `prost-build`, which needs `protoc` (the
  flake ships `protobuf`; a bare `nix shell` does not), so the recipe is:

  ```bash
  nix shell nixpkgs#rustup nixpkgs#cargo-nextest nixpkgs#protobuf \
    -c bash -c 'export PROTOC=$(command -v protoc); cargo …'
  ```

  Install Nix first if `nix-store` is missing, exactly as the root
  `AGENTS.md` describes (`nix-bin`, `NIX_CONFIG`, `NIX_SSL_CERT_FILE`). The
  `wasm` and `sim-wasm` jobs additionally need `wasm-bindgen-cli` (its version
  must equal the `wasm-bindgen` pin in `crates/paros-wasm/Cargo.toml`),
  `binaryen` and `nodejs_22`, and paros-sim's wasm32 build needs an unwrapped
  clang for ring (`llvmPackages.clang-unwrapped`, `bintools-unwrapped`, exported
  as `CC_wasm32_unknown_unknown` / `AR_wasm32_unknown_unknown`), all from the
  flake; add them to the `nix shell` line if you run it there.

Below, `RUN` stands for whichever prefix applies.

## The gate, in CI order

```bash
RUN cargo fmt --all -- --check              # job: fmt   (run `cargo fmt` to fix)
RUN cargo clippy --all-targets -- --deny warnings                       # job: clippy
RUSTDOCFLAGS="-D warnings" RUN cargo doc -p paros-core --no-deps        # job: clippy (doc lints)
RUN cargo nextest run                        # job: test  (fallback: cargo test)
for ex in single_decree multi_paxos matchmaker flexible_quorums \
          acceptor_grid quorum_read proxy_leader replica_tier; do        # job: examples
  RUN cargo run -p paros-core --example "$ex"
done
RUN cargo check --target wasm32-unknown-unknown -p paros-core           # job: portability
RUN cargo check --target wasm32-unknown-unknown -p paros-core --no-default-features
RUN cargo check -p paros-core --features serde
RUN cargo check --target wasm32-unknown-unknown -p paros
RUN cargo check --target wasm32-unknown-unknown -p paros-sim
RUN scripts/check-sim-wasm.sh 1 2 3 4 5 6 7 8 9 10                    # job: sim-wasm
RUN cargo test -p paros-wasm --test paxos_bindings                      # job: wasm
git diff --exit-code -- website/play/src/generated   # the ts-rs bindings are committed
RUN scripts/build-wasm.sh --wasm-only
RUN bash -c 'cd website/play && npm ci --no-audit --no-fund && npm run check && npm test && npm run build'
RUN cargo xtask sim run-all                  # job: sim — the sancov sweep; see /sim-sweep
```

Run the `wasm` job whenever the change touches `crates/paros-wasm`, a
`paros-core` type its views read, or `website/play/`: a stale binding in
`website/play/src/generated` fails the diff, so commit the regenerated files.
Run the `sim-wasm` job whenever the change touches how paros-sim draws (an
index from a `u64` draw goes through `paros_sim::draw_index`) or its wasm32
dependencies.

Run the sim job whenever the change touches `paros-core`, the driver, the
harness, a hook, a knob, an audit message, or the moonpool pin. It is the real
gate: `sim-paros-chain` exits non-zero on any assertion violation, failed run,
coverage gate that never fired, or convergence timeout. A docs-only change can skip it.

## How to read a failure

- **Clippy pedantic is on** (`[workspace.lints]`) and `clippy.toml` **denies
  `HashMap`/`HashSet`** (randomized iteration breaks deterministic replay):
  use `BTreeMap`/`BTreeSet`. Fix the code; do not add `#[allow]`.
- **A public `paros-core` function that can panic needs a `# Panics` doc
  section** (pedantic `missing_panics_doc`); hard `assert!` is the house style
  there, so write the section rather than removing the assert.
- **A nextest sim smoke failure** (`crates/paros-sim/tests/sim.rs`) is a safety-oracle violation or a determinism break, never
  a flake: replay the seed (`/debug-a-seed`).
- **A wasm check failure** in `paros-core` usually means a new dependency or a
  `std::time`/`rand` use crept into the core; the core is dependency-free with
  `--no-default-features`.
- **A coverage gate that never fired** in the sweep is a finding about reach,
  not noise; do not delete the gate (`/adding-an-audit-check`).

The site is built by `pages.yml`, not by `rust.yml`; if you touched
`website/zola/`, also run `RUN website/zola/build.sh` (`/update-the-site`).
