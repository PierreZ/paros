# paros

Learning project: implementing the Paxos consensus algorithm in Rust. WIP, not for production.

## Build & test

Dev shell is a Nix flake — enter `nix develop` (or rely on direnv) before running commands.
(On Claude Code on the web the flake can't be built — its inputs are egress-blocked; use
`nix shell nixpkgs#rustup -c cargo …` instead. See *Always use Nix-provided software* below.)

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

**Sim sweep vs. sim smoke — where each lives.** The heavy, coverage-guided sweep (the one that
must *saturate* `AssertionCoverage`/`CodeCoverage`) always runs via `cargo xtask sim` so the
sancov code-coverage instrumentation guides seed selection; that runner (`paros-sim-runner`) exits
non-zero on any safety violation, so it is the real CI gate. The `cargo nextest` sim tests are only
a fast **smoke** (`SMOKE_ITERATIONS`, a few dozen random seeds through the safety oracles) plus the
pinned `REGRESSION_SEEDS`; they do **not** assert coverage saturation. So: to prove a new red→green
oracle result saturates, run `cargo xtask sim`; the nextest suite just keeps the safety oracles
green quickly. Do not put a multi-thousand-iteration `explore()` back into a nextest test.

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

**Truncation & snapshot doctrine.** Entry bytes are opaque: paros never *interprets or compacts*
application state. The application owns compaction of its own state. What paros does own is its
*log*, and it drops the log prefix two ways, both keeping the bytes opaque:

- **Truncation is a Paxos-decided control command.** A log slot decides a `Command`, which is either
  a `User(Entry)` (opaque client bytes) or a `Control` metadata command — `Truncate{up_to}`, or the
  `Noop` a new leader fills recovery holes with (a slot must be *decided*, never skipped, or the
  contiguous prefix wedges below it forever). A client
  asks the **leader** to truncate (the `Compact` RPC → `RawNode::propose_control`); the leader
  decides `Truncate` into a slot by ordinary consensus, and every node truncates *lazily* when it
  applies that slot (`RawNode::compact`, `WriteOp::Truncate`), giving **one cluster-wide floor**
  forwarded by normal replication + catch-up. The consensus/acceptor paths treat `Command` fully
  opaquely (exactly as Compartmentalized Paxos treats a `Noop`); only the replica/apply path
  interprets a control command, which keeps the eventual M5 compartment split clean.
- **Snapshot transfer recovers a below-floor node.** Acceptors refuse `Prepare`/`Accept` below their
  truncation floor (safety). A node that was down while the cluster truncated past it comes back
  below the floor, where commit-replay catch-up cannot heal it (the entries are gone). paros **does**
  transfer a snapshot to recover it: a peer offers `Message::InstallSnapshot` carrying the
  **opaque, application-produced** snapshot (from `NodeStorage::snapshot()`, the same hook a backup
  would use) plus the boundary `chosen_index`/ballot; the node jumps its chosen prefix, adopts
  `max(promise, ballot)` (its durable promise never regresses), and installs via
  `NodeStorage::install_snapshot()`. paros transfers and tracks the boundary slot; it never
  interprets the bytes. "No compaction" was never "no snapshot transfer": the *application* produces
  the snapshot, paros ships it.

A **wiped** node that lost its durable *promise* (amnesia: a lost disk, not a clean crash) is still
out of scope: a snapshot restores the log, not the promise, so a naive rejoin can regress the
promise (a real safety violation). That belongs to the disk-fault stage (`prob_wipe` stays 0).

## Simulation-driven development

This project is simulation-first: the deterministic simulation (moonpool DST + the `paros-sim`
oracles) is the source of truth for correctness, not hand-written unit tests.

When reading code surfaces a *potential* safety or liveness bug, do NOT reach for a classic unit
test. Reproduce it as a **failing simulation**:

1. State the invariant it would violate (e.g. "at most one value is chosen per slot").
2. Make the scenario reachable. Add the chaos it needs (network loss/reorder, crash/restart via
   `Chaos::Attrition`, storage faults) and use `buggify!()` / `buggify_knob!()` to make the rare
   interleaving likely. If the harness lacks a capability (e.g. persistent storage across restart),
   **build that capability**, do not downgrade to a unit test.
3. Add or strengthen an oracle (an `Invariant` using `assert_always!`) so the violation surfaces as
   a `SimulationReport.assertion_violation`.
4. Run the sweep, confirm it goes **red** on the unfixed code, and record the failing seed.
5. Fix `paros-core`.
6. Run the sweep, confirm it goes **green** and saturates.

A regression unit test may *pin* the bug afterward, but it never replaces step 4. A critical claim
the simulation cannot reproduce is treated as **unproven** (it is probably not a real bug: safety is
often preserved by an invariant you missed). Do not add speculative defensive code for an
unreproducible claim.

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

## Environment detection & setup

At the start of a session, run:

    echo "entrypoint=$CLAUDE_CODE_ENTRYPOINT sandboxed=$CLAUDE_CODE_SANDBOXED"

If `CLAUDE_CODE_ENTRYPOINT` starts with `remote` (e.g. `remote`, `remote_mobile`),
this project is open in **Claude Code on the web** (an isolated, Anthropic-managed
cloud VM). Set up Nix before doing anything else:

    # The sandbox ships broken third-party APT sources (deadsnakes, ondrej), so a
    # plain `apt-get update` fails with 403s — and `update && install` then
    # short-circuits before nix-bin is installed. Install straight from the
    # already-cached package lists; only fall back to `update` if that fails.
    if ! command -v nix-store >/dev/null 2>&1; then
      sudo apt-get install -y nix-bin \
        || { sudo apt-get update; sudo apt-get install -y nix-bin; }
    fi

    # Enable flakes and point Nix at the agent proxy's CA so it can fetch through
    # the proxy. Export these in every shell that runs a `nix` command (or add
    # them to ~/.bashrc):
    export NIX_CONFIG="experimental-features = nix-command flakes"
    export NIX_SSL_CERT_FILE=/root/.ccr/ca-bundle.crt

Any other value (e.g. `cli`, `vscode`) means it's running locally — do NOT run
the install; assume Nix is already set up on the host.

## Always use Nix-provided software

Use Nix-provided software for ALL tooling. **Never** run the sandbox's
pre-installed binaries — in particular the `rustup`/`cargo`/`rustc` under
`/root/.cargo`. Whatever the sandbox image ships is off-limits; every tool comes
from Nix so versions are reproducible across sessions.

- **Locally** (`cli`/`vscode` sessions) just use the project dev shell:
  `nix develop` (or rely on direnv), then run `cargo …` inside it.
- **On Claude Code on the web** the project's `nix develop` shell can't be built:
  its flake inputs (`flake-utils`, `rust-overlay`, `nix-systems`, `nixpkgs`) are
  fetched as GitHub tarballs the sandbox egress policy blocks with a 403
  (`…not enabled for this session`). Do not fight it — that is an org-policy
  denial, not a bug to retry. Instead get a **Nix-provided `rustup`**, which reads
  `rust-toolchain.toml` and runs the pinned channel (1.95.0 +
  `wasm32-unknown-unknown` + clippy/rustfmt):

      nix shell nixpkgs#rustup -c cargo build
      nix shell nixpkgs#rustup -c cargo test           # nextest: add nixpkgs#cargo-nextest
      nix shell nixpkgs#rustup -c cargo clippy -- -D warnings
      nix shell nixpkgs#rustup -c cargo fmt
      nix shell nixpkgs#rustup -c cargo check --target wasm32-unknown-unknown -p paros-core

  `nix shell nixpkgs#…` resolves against `cache.nixos.org` (which the egress
  policy allows), so it works even where `nix develop` does not. The `rustc`/
  `cargo` it runs are the official upstream pinned toolchain — never the sandbox's.
- **Other one-off tools:** `nix shell nixpkgs#<tool> -c <command>` or
  `nix run nixpkgs#<tool> -- <args>`. Do NOT use `apt-get`, `pip install`,
  `npm -g`, `brew`, or similar (`nix-bin` in setup is the sole exception, and only
  to bootstrap Nix itself).
- If a required tool isn't available, add it to the project's flake (or fetch it
  via `nixpkgs#<tool>`) rather than installing it globally.
