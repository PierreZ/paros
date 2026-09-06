---
name: sim-sweep
description: Run and interpret paros's simulation campaigns - the coverage-guided sancov sweep (cargo xtask sim run paros-chain, the CI gate that must saturate), the raw hunt binary sim-paros-hunt with its axes (main, canary, corpus, corpus-chunks), its replay/explore commands, and the evidence budgets (2,000-3,000 seeds normal, 10,000 only for a substantial change). Use when asked to run the sim, hunt for a bug, prove a fix saturates, check determinism after touching randomness or the process lifecycle, or when deciding which of the two runners a task needs.
argument-hint: [axis or seed]
---

# Sim sweep

Two runners, two jobs. Confusing them wastes hours: the raw hunt cannot prove
saturation, and the sweep is the wrong tool for volume.

| Need | Command | What it proves |
|---|---|---|
| **saturation** (every `sometimes`/`reachable` fired, sancov coverage plateaued) | `cargo xtask sim run paros-chain` (CI runs `run-all`; today they are the same single binary) | the CI gate: exits 1 on any violation, failed run, unfired gate or convergence timeout, then runs both corpus axes |
| **volume** through the safety oracles, no saturation claim | `cargo run -p paros-sim-runner --bin sim-paros-hunt [axis] [iterations]` | a failing seed, or "N seeds green" as evidence |
| **replay one seed** | `sim-paros-hunt replay-main <seed>` | GREEN/RED for that draw schedule on this build |
| **fork-explore around one seed** | `sim-paros-hunt explore-main <seed>` | timelines near the seed (`EXPLORATION_TIMELINES_PER_SEED = 8`) |
| **determinism** | `sim-paros-hunt canary [n]`, `replay-canary <seed>` | every seed twice under moonpool's `check_determinism`; a trip names the first diverging draw |

Both are built with sancov only under xtask (`scripts/sancov-rustc.sh` as
`RUSTC_WRAPPER`, `SANCOV_CRATES=paros_core,paros`, target dir `target/sancov`);
the hunt under plain `cargo run` is faster and coverage-blind, which is fine
for volume.

## Hunt axes and replay commands (`crates/paros-sim-runner/src/hunt.rs`)

Axes: `main` (default; the combined swarm campaign), `canary`, `corpus` (CTRL
E1 masks, one per seed), `corpus-chunks` (snapshot-chunk masks). Iterations
default to **2000**. Replays: `replay-main`, `replay-canary`, `explore-main`,
`replay-corpus <seed>`, `replay-corpus-mask <mask>`, `replay-bare-quorum`,
`replay-lifecycle`, `replay-departed`, `replay-chunk-mask <mask>`,
`replay-chunk-seed`. Neither binary reads environment variables;
`SMOKE_ITERATIONS`, `COVERAGE_ITERATIONS`, `CORPUS_CI_ITERATIONS` are `pub
const`s in `crates/paros-sim/src/lib.rs`, and the flake exports `RUST_LOG=debug`
while moonpool's sim subscriber stays floored at INFO.

## Budgets

- Normal evidence for a hunt: 2,000 to 3,000 seeds. Raise to 10,000 only for
  a substantial protocol, harness or fault-model change. Larger only on an
  explicit request.
- After any change to the harness's randomness, the driver hooks or the
  process lifecycle, run a few hundred seeds of `canary`.
- Saturation belongs to xtask and is not replaced by raw volume, however
  large.

## Reading the sweep's output

`sim-paros-chain` prints whether the sweep **saturated** or **hit the
iteration cap** (`COVERAGE_ITERATIONS = 1024`), the exploration stats and bug
recipes, the numeric watermarks and `sometimes_all` frontiers
(`print_guidance`), and finally the coverage gates that never fired. Then:

- a listed **always violation** is a bug in the protocol, the driver or the
  oracle; replay the seed and diagnose (`/debug-a-seed`);
- a **gate that never fired** after saturation is the harness not reaching a
  state it claims; either the claim is wrong (a `sometimes` on a perturbation
  should be a `reach_once!`) or a path needs a BUGGIFY location to become
  likely (`/adding-a-buggify-site`);
- **hit the cap without saturating** means coverage was still moving; look at
  which gates are late rather than raising the cap.

## Seeds are evidence, not artifacts

A seed names a draw schedule, not a scenario; any new draw anywhere shifts
every seed's interleaving. Replay a witness while you fix, cite it in the
commit message, and let it go. Never add a seed constant, a seed list, or a
seed-replay test (a seed that *is* the input to a scripted corpus case, or a
determinism replay of the same seed twice, is not a witness and is allowed).
