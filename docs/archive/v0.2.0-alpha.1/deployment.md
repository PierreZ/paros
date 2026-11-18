# deployment

Everything outside `crates/` that served the removed service: the image, the Compose demo, the
release, the CI jobs of the simulation, the scripts and the agent skills. Removed after the tag
`v0.2.0-alpha.1` (commit `d8047ad`).

## What it did

- **The image and the demo.** `Dockerfile` (a plain multi-stage Rust build, so a fresh clone runs
  with Docker alone) and the flake's `image` output (`nix build .#image`, the same image with no
  Docker Hub pull) built `parosd`, `parosctl` and `paros-bench`. `docker-compose.yml` ran two cells:
  four machines and one `stateless` machine in cell 1, four machines in cell 2, with one-shot
  `init`, `init-cell2` and `add-cell2` services, all under mutual TLS with the pre-generated keys of
  `demo/keys/` (demo only, never secret). `DEMO.md` was the walk-through.
- **The end-to-end check.** `scripts/verify-cell.sh` (the `verify` skill) started three `parosd`
  on loopback and drove them with `parosctl`: `init`, a tenant, a journal, writes and reads, a
  superseded writer, a SIGKILL and a restart, a clean stop. One line for each step.
- **The release.** `release-plz.toml` and `.github/workflows/release-plz.yml` released 16 crates in
  one `version_group` with one `v{version}` tag, and attached musl archives of the three binaries
  for `cargo binstall`. PR #708 (release v0.2.0) was never merged, so nothing past 0.1 reached
  crates.io.
- **The simulation in CI.** `rust.yml` had the jobs `sim` (four shards of the sancov sweep,
  `cargo xtask sim run-all`), `sim-gates` (a gate that every shard missed fails, after a chase),
  `sim-canary` (200 fresh seeds under the determinism canary), `main-reds` (the red table of main
  in the rolling `main-red` issue), `sim-wasm`, `image` and `nix-image`, and a bench smoke step in
  `test`. They were off from step 0.13 of the rework (2026-11-17). `nightly-sim.yml` ran a nightly
  hunt into the rolling issue #726 (nightly sim: failing seeds). `mutants.yml` ran the weekly
  mutation and negative hunts.
- **Scripts.** `sancov-rustc.sh` (the `RUSTC_WRAPPER` for selective SanitizerCoverage, gated by
  `SANCOV_CRATES`), `sim-canary.sh`, `sim-red-record.sh`, `nightly-sim-shard.sh`,
  `nightly-sim-report.sh`, `mutants-report.sh`, `check-sim-wasm.sh`,
  `check-dockerfile-toolchain.sh`, `verify-cell.sh`.
- **Skills and agents.** `adding-a-buggify-site`, `adding-an-audit-check`, `debug-a-seed`,
  `extending-the-chain-workload`, `sim-sweep`, `simulation-driven-fix`, `validate`, `verify`,
  `working-on-the-client`, and the `seed-debugger` agent with its memory
  (`.claude/agent-memory/seed-debugger/MEMORY.md`).

## Key files and types

| Path at the tag | What is there |
| --- | --- |
| `Dockerfile`, `docker-compose.yml`, `.dockerignore` | the image and the two-cell demo |
| `DEMO.md`, `demo/keys/` | the demo walk-through and its key set |
| `flake.nix` (`packages.paros`, `packages.image`) | the Nix build of the binaries and the image |
| `release-plz.toml`, `.github/workflows/release-plz.yml` | the release of 16 crates |
| `.github/workflows/rust.yml` (jobs `sim`, `sim-gates`, `sim-canary`, `main-reds`, `sim-wasm`, `image`, `nix-image`) | the simulation and image gates |
| `.github/workflows/nightly-sim.yml`, `.github/workflows/mutants.yml` | the nightly hunt and the weekly mutation hunts |
| `.config/nextest.toml` | the `ci` profile that left out the three seed tests of `paros-sim` |
| `scripts/` (the files listed above) | the simulation, release and demo scripts |
| `.claude/skills/*/SKILL.md`, `.claude/agents/seed-debugger.md` | the procedures of the simulation and of the service |
| `AGENTS.md` (825 lines) | the doctrine of the whole service: simulation rules, turbulence layers, the audit, boot safety, the layout of every crate |

## Ideas worth reusing

- **Gates judged over all the shards** (`sim-gates`): a rare gate fails CI only when every shard
  missed it, and a missed gate gets a chase with fresh seeds before it fails. A fixed budget
  cannot wait for a rare gate.
- **A red table of main** (`scripts/sim-red-record.sh`): each oracle gets a red rate on main, so a
  fix starts from the top line, not from the last red.
- **The determinism canary on fresh seeds** (`scripts/sim-canary.sh`): every seed runs twice and a
  divergence names its first different draw.
- **Selective sancov** (`scripts/sancov-rustc.sh`): instrument only the crates under test, in a
  separate target directory, so a cached plain build is never served.
- **The old root `AGENTS.md`** holds the simulation doctrine (turbulence layers, `hint!` for
  durability moments, knobs with floors, one combined chaos axis, the cutoff of chaos and operator
  actions). Read it before writing `consensus-sim`.

Not to copy: the simulation in the PR gate with a list of known causes that let a red pass (the
reason it was turned off), and a nextest profile that hides seed tests from CI.

## Bugs and scenarios to cover again

- The steps of `scripts/verify-cell.sh` are the user's flow; a new service needs the same check.
- The Compose demo's steps (two cells, a tenant in each, a multi-writer journal, set-leader,
  configure, resolve, maintenance, drain) are the demo list that the service once passed.

## How to read it at the tag

```sh
git fetch origin tag v0.2.0-alpha.1
git show v0.2.0-alpha.1:AGENTS.md
git show v0.2.0-alpha.1:.github/workflows/rust.yml
git show v0.2.0-alpha.1:docker-compose.yml
```

On GitHub: https://github.com/PierreZ/paros/tree/v0.2.0-alpha.1

Where to start reading: the old `AGENTS.md` (*Simulation rules*, *Turbulence layers*, *Audit*),
then `rust.yml`, then the `sim-sweep` and `simulation-driven-fix` skills.
