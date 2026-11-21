---
name: update-the-site
description: Keep the paros website (Astro under website/site/, one style for every page, the design system in website/shared/) in step with the code - map a change of paros-consensus, a TLA+ model or the wasm engine to the page that shows it, keep every named symbol and command real, and verify with npm run build plus a 390 px and 1280 px check in both themes. Use after changing a protocol rule, a role, the step-through engine, or when asked to explain part of paros on the site.
---

# Update the site

The site (`website/site/`, #810) does three jobs in short pages: it teaches the Paxos flavors
paros builds, it shows how paros is built on the real `paros-consensus` in the browser, and it
teases the journal service being rewritten (the version before is at the tag `v0.2.0-alpha.1`). Read
`website/site/AGENTS.md` first: it has the page map and the style rules.

| Changed | Page (`website/site/src/pages/`) |
|---|---|
| what paros is now | `index.astro` |
| the journal v1 scope, roles, crates, models (`docs/journal/` §J1, §J4, §J7) | `journal/index.astro` |
| the calls, answers, `Resolve`, reads, headers (§J2) | `journal/calls.md` |
| segments, the seal, the boundary state, retire, trim (§J3.1 to §J3.3) | `journal/segments.md` |
| the matchmaker register, its records and guards (§J3.4) | `journal/matchmakers.md` |
| quorum everywhere, the counts of `F + 1`, CTRL, zones (§J5) | `journal/quorums-and-failures.md` |
| the controller, the fenced reshape, journal health (§J6) | `journal/controller.md` |
| parosd, the version at the tag `v0.2.0-alpha.1` | `parosd/index.astro` |
| single decree | `paxos/single-decree.md` |
| slots, the leader, persist-before-send, reads | `paxos/multi-paxos.md` |
| matchmakers, reconfiguration, GC | `paxos/matchmaker.md` |
| proxy leaders, batchers, replicas, handoff | `paxos/compartmentalized.md` |
| quorum systems, grids, partitions | `paxos/quorums.md` |
| the Paxos section's landing page, the order of the flavors | `paxos/index.astro` |
| a knowledge card in `docs/knowledge/concepts/` | `paxos/concepts.astro`: a new card needs its entry in `SHOWN`, or the build fails |
| the top bar | `src/nav.ts` |
| the sans-IO core, `Ready`, `advance()`, the driver contract | `consensus/driver.md` |
| a deployment (`ColocatedNode`, `ProxyLeader`, `ReplicaNode`, `Matchmaker`), a role, a flavor type or an example | `consensus/index.astro` (the flavor table), `consensus/deployments.md` |
| a model, a trace spec, a walk flavor of consensus-sim, the mutants | `consensus/proof.md` |
| a safety claim, its model or invariant, a trace spec, a walk or oracle of consensus-sim or journal-sim, a planted mutant, an open issue that a claim waits on | `engineering/index.astro` (the safety case: its row and its state) |
| how TLA+ models are checked, the inductive invariants, the guard mutants | `engineering/tla.md` |
| the absolute rule of simulation, the walks, the trace checks, the coverage gate | `engineering/simulation.md` |
| cargo-mutants, a new full mutants run, the assertions | `engineering/mutants.md` (the score of a run, with its date) |
| the grid quorum system, partitions | `engineering/grid-quorums.md` |
| `crates/support/paros-wasm` (the `paxos` feature) or a `paros-consensus` type its views read | the step-through traces: run `cargo test -p paros-wasm --test paxos_traces` and commit `website/site/src/traces` |

Every symbol, path and command a page names must exist: `grep -rn "<symbol>" crates/`
before you write it. When a page moves, add a redirect in `astro.config.mjs`.

## Verify

```bash
nix develop --command scripts/build-wasm.sh                        # the wasm of the live parts
nix develop --command bash -c 'cd website/site && npm ci && npm run build && npm run check-links'
nix develop .#site --command bash -c 'cd website/site && npm test'  # browser smoke test
cd website/site && npm run dev                                      # live preview
```

Look at each page you changed at 390 px and 1280 px, light and dark. The site is built,
link-checked and smoke-tested by `.github/workflows/pages.yml` on every pull request that
touches `website/`, `paros-wasm`, `paros-consensus` or `paros-journal`, and deployed on
push to `main` (`website/site/AGENTS.md`, "Tests").
