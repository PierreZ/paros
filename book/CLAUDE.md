# book

The paros book: an mdbook that explains the Paxos family with diagrams, grounded in the
papers (`docs/references/`) and mapped onto the real `paros-core` code. Source in
`book/src/`, config in `book.toml`.

## Build & preview

- `mdbook build` (output in `book/output/`) — the parse gate; `mdbook-mermaid` rewrites
  every ` ```mermaid ` fence, so a build failure means a malformed block.
- `mdbook serve` to preview live.
- Preprocessors: `mdbook-toc` (the `<!-- toc -->` marker) and `mdbook-mermaid`. All
  diagrams are **mermaid only** (`flowchart`, `sequenceDiagram`, `stateDiagram-v2`); no
  ASCII art, no SVG.

## Diagram colours MUST survive both themes

This is the rule that is easy to get wrong. `book.toml` sets `default-theme = "rust"`
(a **light** theme), and `mermaid-init.js` picks mermaid's **light `default` theme** for
light mdbook themes and the **`dark` theme** for dark ones (`coal`/`navy`/`ayu`). So any
hardcoded colour has to read on **both** a light (~`#f9f5e9` cream) and a dark page.

- **Highlight bands** (`rect` in a `sequenceDiagram`): use a **translucent `rgba` tint**
  with low alpha, never an opaque dark `rgb` fill. An opaque dark band (e.g.
  `rect rgb(120, 50, 50)`) renders as a heavy slab on the light page and makes the dark
  note/message text on it unreadable. The house values are:
  - bug / danger: `rect rgba(200, 70, 70, 0.25)`
  - fix / safe: `rect rgba(70, 170, 110, 0.25)`
- **Coloured nodes** (`classDef` in a `flowchart`): set an explicit `fill` **and**
  `color:#fff`, and keep the fill dark enough that white text reads on it — a
  self-contained dark chip works on either theme. The existing palette, reused across
  chapters:
  - `done`  `fill:#3b6e47,stroke:#244730,color:#fff` (chosen / green)
  - `gap`   `fill:#7a2f2f,stroke:#4d1f1f,color:#fff` (hole / red)
  - `open`  `fill:#5a5a5a,stroke:#333,color:#fff`    (undecided / grey)
  - `shared` `fill:#c97a2b,stroke:#7a4718,color:#fff` (pivot / orange)
- Leave everything else to the theme. Don't restyle actor boxes, arrows, or note
  fills — mermaid recolours those per theme automatically.

To check a diagram the way readers see it (the book defaults to the light theme), render
it with the light theme on the cream page, e.g.
`mmdc -t default -b "#f9f5e9" -i file.md -o out.png` (on NixOS point puppeteer at the
system chromium: `PUPPETEER_EXECUTABLE_PATH=$(command -v chromium)` plus a puppeteer
config with `--no-sandbox`).

## Diagram house style

- `flowchart TD`; `sequenceDiagram` always with `autonumber`; `stateDiagram-v2` with
  `direction TB`.
- Multi-line labels use `<br/>`; sentence case; canonical message names (Prepare,
  Promise, Accept, Accepted, Nack, Commit, Heartbeat, Propose, ProposeAck); descriptive
  participant aliases (`L as Leader, owns the ballot`).
- A diagram must **reveal mechanism** the prose can't (an interleaving, a quorum
  intersection, a counterexample trace, a commit index advancing) — not redraw a list,
  table, or numbered steps as boxes. If it only restates the surrounding text, cut it.
- Keep every symbol named in a diagram real: it should exist in `paros-core` / `paros-sim`
  so the figure stays mapped to the code, like the rest of the book.

## Live demos

Each protocol chapter ends with a **"Watch it live"** demo page embedding the wasm demo:
`single-decree.md` under *Single-decree Paxos*, `multi-paxos.md` under *Multi-Paxos* (so a
reader meets the live demo right after the concept it shows). One file,
`paros-wasm-demo/web/index.html`, serves both via a `?mode=` switch (`single` is the
default, `?mode=multi` selects the leader-and-log column scene). The two modes share the
scenario digest, the narration, and the time-warp playback machinery; only the canvas
painter (`renderScene` vs `renderMultiScene`) and the message endpoints differ.

- **Demos are self-describing.** The browser computes the whole `RunResult`
  (`paros_sim::run_seed_json`), and the UI derives, **purely from that data**, a *scenario
  digest* (the chips: leader failovers, Phase-2 piggybacked slots, dueling vs stable, log
  length, value chosen by N/3, network drops) and a *live narration* (the status line:
  what is happening at the current sim time). Adding a demo metric means deriving it from
  `RunResult`, not threading a new narrative through the prose.
- **Never pin a "curated" seed to a claimed narrative in prose.** Seeds drift as the
  protocol changes, so a hand-picked "this seed shows X" story silently rots. Embed
  arbitrary fixed seeds; **teach the *concepts* in prose and let the demo narrate the
  *specifics*.** Because the UI reads the data, the same embed stays correct across code
  changes. (The single-decree page still names a couple of historical seeds; new pages
  should not.)
- Embed with an iframe `src="wasm-demo/index.html?embed=1&..."` (`embed=1` hides the page
  chrome). URL params: `?seed=<n>`, `?mode=multi`, `?dump` (raw JSON), `?still=<k>` (frozen
  frame for screenshots). Build the demo with `book/build-wasm-demo.sh` (the `wasm-bindgen`
  crate pin in `paros-wasm-demo/Cargo.toml` must match the flake's `wasm-bindgen-cli`); the
  GitHub Pages workflow (`pages.yml`) runs the same build before `mdbook build`.
