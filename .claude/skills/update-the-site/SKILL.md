---
name: update-the-site
description: Keep the paros website (Astro under website/site/, one style for every page, the design system in website/shared/) in step with the code - map a protocol, API or harness change to the page that shows it, keep every named symbol and command real, and verify with npm run build plus a 390 px and 1280 px check in both themes. Use after changing a protocol rule, the journal API, parosctl, the roles, the playground, or when asked to explain part of paros on the site.
---

# Update the site

The site (`website/site/`, #810) does three jobs in short pages: it sells `parosd`, it
explains the Paxos flavors paros uses, and it shows the engineering techniques. Read
`website/site/AGENTS.md` first: it has the page map and the style rules.

| Changed | Page (`website/site/src/pages/`) |
|---|---|
| what parosd offers | `index.astro` |
| single decree, `cell init` | `paxos/single-decree.md` |
| slots, the leader, persist-before-send, reads | `paxos/multi-paxos.md` |
| matchmakers, reconfiguration, GC | `paxos/matchmaker.md` |
| proxy leaders, batchers, replicas, frontends, handoff | `paxos/compartmentalized.md` |
| quorum systems, grids, partitions | `paxos/quorums.md` |
| `parosctl`, the four calls, durability, roles, tokens | `docs/reference.md` |
| `DEMO.md`, docker compose | `docs/quickstart.md` |
| the playground engine or page | `playground/index.astro`, `website/sandbox/`, `docs/playground.md` |
| the Paxos section's landing page, the order of the flavors | `paxos/index.astro` |
| a knowledge card in `docs/knowledge/concepts/` | `paxos/concepts.astro`: a new card needs its entry in `SHOWN`, or the build fails |
| the engineering section's landing page | `engineering/index.astro` |
| the sans-IO core, `Ready`, `advance()` | `engineering/sans-io.md` |
| the providers, the same code in `parosd` and the sim | `engineering/same-code.md` |
| the simulation, BUGGIFY, hints, oracles | `engineering/simulation.md` |
| swarm draws, per-seed scenarios | `engineering/swarm.md` |
| the store's commit, persist-before-send on disk | `engineering/durable-writes.md` |
| a storage fault, quarantine, a full disk | `engineering/fsync.md` |
| the grid quorum system, partitions | `engineering/grid-quorums.md` |
| a control-plane journal, election or flavor | `engineering/dogfooding.astro` |
| `crates/support/paros-wasm` (the `paxos` feature) or a `paros-consensus` type its views read | the step-through traces: run `cargo test -p paros-wasm --test paxos_traces` and commit `website/site/src/traces` |

Every symbol, path and command a page names must exist: `grep -rn "<symbol>" crates/`
before you write it. When a page moves, add a redirect in `astro.config.mjs`.

## Verify

```bash
cd website/site && npm ci && npm run build      # inside nix develop
npm run dev                                      # live preview
```

Look at each page you changed at 390 px and 1280 px, light and dark (Playwright is
installed). The site is built by `.github/workflows/pages.yml` on every pull request that
touches `website/`, and deployed on push to `main`.
