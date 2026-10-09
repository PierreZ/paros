# site

The paros website: Zola with the Goyo theme (#254), replacing the mdbook. Two parts:
`content/parosd/` (the system) and `content/paxos/` (the Paxos implementation, told as a
learning path). Pages are written for people and stand on their own: they never send a
reader to `docs/architecture.md`, which is the agents' map, not human documentation.

## Build & preview

- `scripts/build-site.sh` (inside `nix develop`) copies the pinned Goyo (`PAROS_GOYO`, a flake
  input) into `web/site/themes/goyo` and runs `zola build`; output in `web/site/public/`. Zola checks
  every internal link and anchor, so a broken `@/` link fails the build.
- `scripts/build-site.sh serve` previews live.
- The flake pins Zola 0.22 (`nixpkgs-zola`): Goyo's templates do not parse on Zola 0.23.
- Internal links are `@/paxos/<page>.md#anchor`. Old mdbook URLs keep working through each
  page's `aliases` (`/safety.html` redirects to `/paxos/safety/`); keep them when you move a page.
- All diagrams are **mermaid only** (`flowchart`, `sequenceDiagram`, `stateDiagram-v2`), written
  with the `mermaid` shortcode (`templates/shortcodes/mermaid.html` escapes the body so `<br/>`
  reaches mermaid as text):

  ```
  {% mermaid() %}
  flowchart TD
    A --> B
  {% end %}
  ```

  No ASCII art, no SVG.

## Diagram colours MUST survive both themes

This is the rule that is easy to get wrong. `web/site/config.toml` sets the light Goyo theme as
the default, and Goyo initializes mermaid with the light theme or the `dark` theme to match
the reader's toggle. So any hardcoded colour has to read on **both** a light (white) and a
dark page.

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

To check a diagram the way readers see it, render it with both themes, e.g.
`mmdc -t default -b white -i file.md -o out.png` and `mmdc -t dark -b "#0f1729" …` (on NixOS point puppeteer at the
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
  so the figure stays mapped to the code, like the rest of the site.

## The live surface is the game, and the Paxos chapters are its field guide

The interactive half of the site is **paros play**, deployed on the same GitHub Pages site
at `/play/` (`crates/paros-play` + `web/play`, staged into `web/site/public/play/` by
`scripts/build-play.sh` in the Pages workflow). The one-widget rewrite (#308) replaces this
section's rules when it lands. It drives the real `paros-core` compiled
to wasm — the player is the network and the clock, and plays each role until the core judges
the answer right — and it **never replays a trace**: there is no seed, no recorded run, and
no simulation in the browser. `web/site/content/paxos/play.md` is the level map; the levels themselves are
Rust, in `crates/paros-play/src/level/`.

That splits the writing between the two surfaces, and the split is the rule for every future
chapter edit:

- **A chapter never re-explains an interleaving a level plays.** State the mechanism in a
  paragraph, then link to the level. The step-by-step walkthroughs and counterexample traces
  that used to carry these chapters are level briefings now; re-adding one to the prose is a
  regression, not an improvement.
- **What a chapter keeps** is what a level cannot give: the papers and their quotes, the
  derivation (the invariant ladder), the distinctions that are arguments rather than runs
  (recovery vs. catch-up), the "Proven, not asserted" doctrine sections, the optimizations
  table, and the "Where this lives in paros" symbol maps.
- **A diagram survives only if no level plays it.** Today that is the quorum-intersection
  pivot flowchart and the invariant ladder in `safety.md`, and the hole picture in
  `replicated-log.md` — static pictures of *state* or of a proof, not of an interleaving.
- **Level ids are stable strings** (`act1/choose-a-value`, `act2/the-permanent-gap`) and a
  chapter cites them **verbatim**. A level is linked as `../../play/#<level-id>` — relative,
  because the site is served under `/paros/` and a chapter at `/paros/paxos/<name>/` — e.g.
  `[act1/adopt-the-value](../../play/#act1/adopt-the-value)`.
  Never link a level by index or by title.
- **Every chapter that a level teaches carries a "Play it" callout**, a blockquote placed
  immediately after the opening paragraph:

  ```markdown
  > **Play it.** One sentence of framing.
  >
  > - [`act2/elect-a-leader`](../../play/#act2/elect-a-leader) — what you do by hand, in the
  >   second person, and what the level's goal is.
  ```

  One bullet per level, in level order, saying what the player *does* — not what the level
  is about. Do not list a level in a chapter it does not belong to; `play.md`'s
  table is the single source of the mapping, and the two must agree.

Do not add demo iframes, `runSeed` references, or wasm build steps to a chapter: the game is
a separate page, linked, never embedded.

## Site text follows ASD-STE100

The game's own text is Simplified Technical English, and the site pages written for it match
it: `play.md`, the section pages (`_index.md`), every "Play it" callout, the
one-paragraph mechanism statement that opens a chapter, and all of `beyond-multi-paxos.md`. The
rules are one instruction per sentence, active voice, present tense, at most 20 words in a
procedural sentence and 25 in a descriptive one, at most six sentences per paragraph, no idioms
and no figurative language, no noun cluster longer than three words, articles always written,
and `must` for an obligation. Technical names stay technical names — ballot, promise, quorum,
slot, Prepare, Promise, Accept, Accepted, Nack, Commit, Heartbeat, and every `paros-core`
symbol. Two things are exempt, because rewriting them changes their meaning: text quoted from a
paper or a design note, and the wording of the safety derivations and the audit's assertion
message strings. The rest of a chapter — the derivations, the doctrine sections, the symbol
maps — is ordinary prose, and a rewrite of it is not part of an STE pass.
