# The journal of design A, removed on 2026-11-24

On 2026-11-24 Pierre decided to remove the journal v1 of design A (D6 of
[`prompt.md`](prompt.md)). Design A wrote consensus a second time beside the core: a second
matchmaker (the register), a second leader and a second replica. The work now is the plan of
[`prompt.md`](prompt.md): segments come down into `paros-consensus` as an optional behaviour, with
the `Roll` cut, a TLA+ model first. Then the journal is built again from scratch, on the core with
segments, and it writes no consensus of its own.

The git tag `journal-design-a` is the last commit of `main` before the removal. It holds all of
design A. This page lists what the removal took, so that a later agent can find its way back.

## How to read it

```sh
git fetch origin tag journal-design-a
git show journal-design-a:docs/journal/README.md                  # one file
git worktree add ../paros-journal-design-a journal-design-a       # the whole tree
git grep -n 'Seal' journal-design-a -- crates/                    # search it
```

On GitHub: <https://github.com/PierreZ/paros/tree/journal-design-a>.

Read the design (`docs/journal/`) before the code. Do not copy code back without a reason in the
pull request: the new journal holds core roles inside it, and it does not write them again.

## What was removed

**Crates.**

- `crates/lib/paros-journal`: the pure core of the journal v1. The journal fold, the `Resolve`
  call, the held answers, the segment record, the matchmaker register, the journal roles
  (acceptor, leader, segment replica), the pure client and the codec. Published on crates.io
  with the other public crates; a yank is Pierre's call.
- `crates/lib/paros-journal-io`: the journal's IO side. The `Storage`, `Transport`, `Clock` and
  `Register` traits, the on-disk layout, the journal `Host` and `Driver` on `paros-host`, the
  in-memory backends and the contract suites. Published on crates.io too.
- `crates/lib/paros-journal-control`: the grid controller. The P measures, the placement of a
  heal under the zone rule, the pure `decide`, the controller role and `ControlDriver`.
- `crates/lib/paros-election`: the coordination store over a journal (#1420 (paros-election)):
  `Tree`, `Member`, and the recipes `Latch`, `Group` and `Counter`.
- `crates/sim/journal-sim`: the walks of the fold, the register, the roles, the driver, the
  whole journal, the controller and the election, the sweep of `decide`, their trace checks and
  the live check of a long fold walk.
- From the facade `paros`: the modules `paros::journal` and `paros::journal_io`, and the examples
  `journal`, `tokio_tasks` and `full_journal`. A new example, `consensus`, drives the core alone.
- From `paros-wasm`: the feature `journal` (the fold lab, the register lab, the record lab and
  the live journal of the site) and its test `journal_traces`.

**TLA+ models** (`specs/tla/`): `coord/`, `ctrl-grid/`, `ctrl-segments/`, `degraded-reshape/`,
`election/`, `grid-controller/`, `heal-reuse/`, `health/`, `journal-compact/`, `journal-ctrl/`,
`journal-ctrl-re/`, `journal-ctrl-trunc/`, `journal-read/`, `journal-repromise/`,
`journal-state/`, `multi-writer/`, `offload/`, `placement-port/`, `quorum-everywhere/`,
`re-replication/`, `seal-chain/`, `seal-ctrl/`, `segment-records/`, `segment-replicas/`,
`segment-truncate/`, and the experiment `experiments/segments/` (#1213 (segments in the protocol
or in storage)). Their sections of `specs/tla/README.md` and their lines of
`scripts/tla-minutes.txt` and `scripts/mutate-tla-shards.txt` went with them. The models of the
core, of `paros-host` (`flight-rule/`, `recorded-inputs/`) and of the service boundaries
(`cell-control/`, `journal-ids/`, `takeover/`) stay.

**Docs and site.**

- `docs/journal/`: the design of the journal v1, every journal rule with its decision, its issue
  and its model, and the build steps. The sections of `docs/architecture/` that pointed to it keep
  their headings and say that they are to be designed again.
- `docs/knowledge/concepts/segments-and-seal.md` and its line of `docs/knowledge/concepts.md`.
- The Journal pages of the site (`website/site/src/pages/journal/`), the lesson "seal" of the
  Paxos course and its trace `journal-seal.json`, and the scripts of the labs. `/journal` and its
  pages redirect to the home page.

**CI.** The workflows `journal-live.yml` and `journal-nightly.yml`; the journal steps of
`rust.yml`, `tla.yml`, `mutants.yml`, `mutants-nightly.yml` and `pages.yml`; the files
`.cargo/mutants-journal.toml`, `.cargo/mutants-journal-tlc.toml`,
`.cargo/mutants-journal-io.toml`, `.cargo/mutants-control.toml` and
`.cargo/mutants-election.toml`; the journal flags of `scripts/mutants.sh`; and the journal crates
of `release-plz.toml`, `scripts/layers.sh` and the root `Cargo.toml`.

## What stays

`paros-consensus` (with `src/journal/` and `journal_calls.rs`: the journal state that the core
already folds), `paros-host`, `consensus-sim`, `paros-authz-biscuit`, `paros-wasm` with the feature
`paxos`, the facade with `paros::consensus` and `paros::host`, and `docs/references/`,
`docs/archive/` and `docs/analysis/`.
