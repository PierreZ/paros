---
name: update-the-site
description: Keep the paros site (Zola + Goyo under web/site/, the Paxos chapters in web/site/content/paxos and the parosd pages in web/site/content/parosd, mermaid-only diagrams, the both-theme colour rules in web/site/AGENTS.md) in step with the code - map a protocol or harness change to the page that explains it, keep every named symbol real, and verify with scripts/build-site.sh. Use after changing a protocol rule, a message, a recovery or read path, the journal API, truncation or trim-point behaviour, a game level, or when asked to explain part of paros for readers.
---

# Update the site

The site's Paxos part explains the Paxos family with diagrams grounded in the papers
(`docs/references/`) and mapped onto the real `paros-core` code; every symbol
a page names must exist in `paros-core`/`paros-sim`. A protocol change
therefore usually moves a page. `web/site/AGENTS.md` (loads when you edit
under `web/site/`) holds the diagram rules; this skill is the map. The `parosd`
part (`web/site/content/parosd/`) is written for people from the code and
`docs/architecture.md`, and never links readers to that file. #254 and its
sub-issues (#306 to #316) list the pages still to come.

| Changed | Page (`web/site/content/`) |
|---|---|
| what paros is, the two parts | `_index.md`, `paxos/_index.md`, `parosd/_index.md` |
| the game's levels, level ids, the level-to-chapter map (`crates/paros-play/src/level/`) | `paxos/play.md` (the single source of the level/chapter mapping; every chapter's "Play it" callout must agree with it) |
| single-decree kernel, Prepare/Promise/Accept, the P2c rule, the invariant ladder | `paxos/choose-one-value.md`, `paxos/safety.md` |
| slots, the log, `next_slot`, catch-up, the hole picture | `paxos/replicated-log.md` |
| leader election, heartbeats, election gap fill, the optimizations table | `paxos/stable-leader.md` |
| `HardState`, persist-before-send, the boot report, seams, promise monotonicity | `paxos/restart-safety.md` |
| the `Truncate` call and `Control::Truncate`, `first_seq`, the sealed journal state, floors, the trim-point jump (`TrimmedTo`), a `Read` below `first_seq` answered `truncated` | `paxos/truncation-and-snapshots.md` (titled *Truncation and the trim point*; keep the filename and its alias) |
| the quorum read as the read path (`ColocatedNode::quorum_read`, the fresh-leader trap; read-index retired, #243), chosen vs applied, the verdict answered at apply, the client-history linearizability check | `paxos/linearizable-reads.md` |
| flexible quorums, the grid, quorum reads (every public `Read`), proxy leaders, the replica tier, handoff, matchmakers and reconfiguration, GC and retirement, matchmaker-set generations, faulty records, the wiped node, many journals per process | `paxos/beyond-multi-paxos.md` (one `##` section per mechanism; written in ASD-STE100) |
| a new area | a new section of `paxos/beyond-multi-paxos.md`, or a new page with a `weight` in its section |

The journal API is #204's four calls, `Write`, `Read`, `Truncate`,
`SetLeader`; the older names (`Append`, `CheckTail`, `Trim`, the public
read-index read) are gone and must not come back into a page.

`grep -rn "<symbol>" web/site/content` finds every mention of a renamed symbol.

## Diagram rules that are easy to get wrong

Mermaid only (`flowchart TD`, `sequenceDiagram` with `autonumber`,
`stateDiagram-v2` with `direction TB`), written with the `{% mermaid() %}`
shortcode; no ASCII art, no SVG. Colours must survive both Goyo themes: highlight bands are
translucent (`rect rgba(200, 70, 70, 0.25)` for a bug, `rgba(70, 170, 110,
0.25)` for a fix), coloured nodes use the four house `classDef` chips with
`color:#fff`. A diagram must reveal a mechanism the prose cannot (an
interleaving, a quorum intersection, a counterexample); if it restates a
list, cut it, and a diagram survives only if no game level plays it. No demo
iframes, `runSeed` or wasm steps: the live surface is the game (`paxos/play.md`,
`/play/`), linked by level id (`../../play/#act2/elect-a-leader`), never embedded. A
chapter never re-explains an interleaving a level plays. `paxos/play.md`, every
"Play it" callout, each chapter's opening mechanism paragraph and all of
`paxos/beyond-multi-paxos.md` are written in ASD-STE100 (rules in `web/site/AGENTS.md`).

## Verify

```bash
scripts/build-site.sh        # inside nix develop; output in web/site/public/; fails on a broken internal link or anchor
scripts/build-site.sh serve  # live preview
```

Run through the Nix prefix that applies (`/validate`). The site is built by
`.github/workflows/pages.yml` on every pull request that touches it, and
deployed on push to `main`. Zola does not parse mermaid, so a malformed
block shows only in a browser: preview a page you changed.
