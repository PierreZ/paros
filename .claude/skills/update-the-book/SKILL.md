---
name: update-the-book
description: Keep the paros mdbook (seven chapters under book/src, mermaid-only diagrams, the light-theme colour rules in book/CLAUDE.md) in step with paros-core - map a protocol or harness change to the chapter that explains it, keep every named symbol real, and verify with mdbook build. Use after changing a protocol rule, a message, a recovery or read path, truncation or snapshot behaviour, or when asked to explain part of paros for readers.
---

# Update the book

The book explains the Paxos family with diagrams grounded in the papers
(`docs/references/`) and mapped onto the real `paros-core` code; every symbol
a chapter names must exist in `paros-core`/`paros-sim`. A protocol change
therefore usually moves a chapter. `book/CLAUDE.md` (loads when you edit
under `book/`) holds the diagram rules; this skill is the map.

| Changed | Chapter (`book/src/`) |
|---|---|
| single-decree kernel, Prepare/Promise/Accept, the P2c rule | `choose-one-value.md`, `safety.md` |
| slots, the log, `next_slot`, catch-up | `replicated-log.md` |
| leader election, heartbeats, election gap fill, handoff | `stable-leader.md` |
| `HardState`, persist-before-send, boot replay, seams, promise monotonicity | `restart-safety.md` |
| `Truncate`/`Snap` control commands, floors, `InstallSnapshot`, chunk repair | `truncation-and-snapshots.md` |
| `READ_INDEX`, the read fence, parked reads | `linearizable-reads.md` |
| a new area (matchmaking, GC, generations) | a new chapter: add to `SUMMARY.md` and `index.md` |

`grep -rn "<symbol>" book/src` finds every mention of a renamed symbol.

## Diagram rules that are easy to get wrong

Mermaid only (`flowchart TD`, `sequenceDiagram` with `autonumber`,
`stateDiagram-v2` with `direction TB`); no ASCII art, no SVG. Colours must
survive both the light `rust` theme and the dark ones: highlight bands are
translucent (`rect rgba(200, 70, 70, 0.25)` for a bug, `rgba(70, 170, 110,
0.25)` for a fix), coloured nodes use the four house `classDef` chips with
`color:#fff`. A diagram must reveal a mechanism the prose cannot (an
interleaving, a quorum intersection, a counterexample); if it restates a
list, cut it. No demo iframes, `runSeed` or wasm steps: the live demo was
removed and returns on top of the audit's data, not the trace.

## Verify

```bash
mdbook build        # output in book/output/; a failure is a malformed mermaid block
mdbook serve        # live preview
```

Run through the Nix prefix that applies (`/validate`). The book is deployed
by `.github/workflows/pages.yml` on push to `main`, not gated by `rust.yml`,
so a broken block surfaces after merge unless you build locally.
