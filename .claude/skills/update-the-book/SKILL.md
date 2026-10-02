---
name: update-the-book
description: Keep the paros mdbook (the chapters under book/src, mermaid-only diagrams, the light-theme colour rules in book/CLAUDE.md) in step with paros-core - map a protocol or harness change to the chapter that explains it, keep every named symbol real, and verify with mdbook build. Use after changing a protocol rule, a message, a recovery or read path, the journal API, truncation or trim-point behaviour, a game level, or when asked to explain part of paros for readers.
---

# Update the book

The book explains the Paxos family with diagrams grounded in the papers
(`docs/references/`) and mapped onto the real `paros-core` code; every symbol
a chapter names must exist in `paros-core`/`paros-sim`. A protocol change
therefore usually moves a chapter. `book/CLAUDE.md` (loads when you edit
under `book/`) holds the diagram rules; this skill is the map.

| Changed | Chapter (`book/src/`) |
|---|---|
| what paros is, the reading order, the "How to play, then read" callout | `index.md` |
| the game's levels, level ids, the level-to-chapter map (`crates/paros-play/src/level/`) | `play.md` (the single source of the level/chapter mapping; every chapter's "Play it" callout must agree with it) |
| single-decree kernel, Prepare/Promise/Accept, the P2c rule, the invariant ladder | `choose-one-value.md`, `safety.md` |
| slots, the log, `next_slot`, catch-up, the hole picture | `replicated-log.md` |
| leader election, heartbeats, election gap fill, the optimizations table | `stable-leader.md` |
| `HardState`, persist-before-send, the boot report, seams, promise monotonicity | `restart-safety.md` |
| the `Truncate` call and `Control::Truncate`, `first_seq`, the sealed journal state, floors, the trim-point jump (`TrimmedTo`), a `Read` below `first_seq` answered `truncated` | `truncation-and-snapshots.md` (titled *Truncation and the trim point*; keep the filename, `SUMMARY.md` links it) |
| read-index (`ColocatedNode::read_index`, kept in the core for the game), the read fence, chosen vs applied, the verdict answered at apply, the client-history linearizability check | `linearizable-reads.md` |
| flexible quorums, the grid, quorum reads (every public `Read`), proxy leaders, the replica tier, handoff, matchmakers and reconfiguration, GC and retirement, matchmaker-set generations, faulty records, the wiped node, many journals per process | `beyond-multi-paxos.md` (one `##` section per mechanism; written in ASD-STE100) |
| a new area | a new section of `beyond-multi-paxos.md`, or a new chapter added to `SUMMARY.md` and `index.md` |

The journal API is #204's four calls, `Write`, `Read`, `Truncate`,
`SetLeader`; the older names (`Append`, `CheckTail`, `Trim`, the public
read-index read) are gone and must not come back into a chapter.

`grep -rn "<symbol>" book/src` finds every mention of a renamed symbol.

## Diagram rules that are easy to get wrong

Mermaid only (`flowchart TD`, `sequenceDiagram` with `autonumber`,
`stateDiagram-v2` with `direction TB`); no ASCII art, no SVG. Colours must
survive both the light `rust` theme and the dark ones: highlight bands are
translucent (`rect rgba(200, 70, 70, 0.25)` for a bug, `rgba(70, 170, 110,
0.25)` for a fix), coloured nodes use the four house `classDef` chips with
`color:#fff`. A diagram must reveal a mechanism the prose cannot (an
interleaving, a quorum intersection, a counterexample); if it restates a
list, cut it, and a diagram survives only if no game level plays it. No demo
iframes, `runSeed` or wasm steps: the live surface is the game (`play.md`,
`/play/`), linked by level id (`play/#act2/elect-a-leader`), never embedded. A
chapter never re-explains an interleaving a level plays. `play.md`, every
"Play it" callout, each chapter's opening mechanism paragraph and all of
`beyond-multi-paxos.md` are written in ASD-STE100 (rules in `book/CLAUDE.md`).

## Verify

```bash
mdbook build        # from the repo root (book.toml lives there); output in book/output/; a failure is a malformed mermaid block
mdbook serve        # live preview
```

Run through the Nix prefix that applies (`/validate`). The book is deployed
by `.github/workflows/pages.yml` on push to `main`, not gated by `rust.yml`,
so a broken block surfaces after merge unless you build locally.
