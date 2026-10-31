# site

The paros website: Zola (#254), replacing the mdbook. Two parts:
`content/parosd/` (the system) and `content/paxos/` (the Paxos implementation, told as a
learning path). Pages are written for people and stand on their own: they never send a
reader to `docs/architecture/`, which is the agents' map, not human documentation.

## Build & preview

- `website/zola/build.sh` (inside `nix develop`) copies the pinned Goyo (`PAROS_GOYO`, a flake
  input) into `website/zola/themes/goyo` and runs `zola build`; output in `website/zola/public/`. Zola checks
  every internal link and anchor, so a broken `@/` link fails the build.
- `website/zola/build.sh serve` previews live.
- The flake pins Zola 0.22 (`nixpkgs-zola`): Goyo's templates do not parse on Zola 0.23.
- `build.sh` also writes `updated.toml` (gitignored): the day of the last commit of each page,
  shown in the title block. The Pages workflow fetches the whole history for it.

## Look and feel (#804 (flat engineering redesign))

Flat engineering design with a terminal feel, shared by the book, paros play and the sandbox.

- **Tokens.** `website/shared/tokens.css` is the only file that defines a colour (light and dark,
  every text pair WCAG AA). The theme choice is `localStorage["theme"]` (`goyo-light` or
  `goyo-dark`, on `<html data-theme>`), written by `website/shared/theme.js`; without a choice
  the page follows `prefers-color-scheme`. The one exception to "no colour literal" is a mermaid
  `classDef` (below).
- **Shared components** are in `website/shared/base.css`: the top bar, `.label` (mono, uppercase,
  12px, muted), `.chip-status` (`done`, `gap`, `shared`, `accent`, default `open`), `.btn` (a
  rectangle with a hard 2px offset shadow; `.primary` uses the accent), `.callout` with
  `data-tag`, `.kv` (key-value table), `.act-link` (a boxed `▶` before an action link).
- **Templates are the site's own** (`templates/`): `base.html` (skeleton), `page.html`,
  `section.html`, `landing.html` (the home page), `macros/site.html` (top bar, section tree,
  breadcrumbs, title block). Goyo stays the theme only for `static/js/mermaid.min.js`; its
  DaisyUI layout fought every rule here. Do not add DaisyUI or Tailwind classes.
- **Behaviour** is `static/js/site.js`: code block headers with copy, a `$` prompt on each shell
  command line (CSS, so copy leaves it out), blockquotes as callouts (`NOTE` by default, `PLAY`
  for a "Play it." callout), tables in their own scroll box, mermaid themed from the tokens,
  search over Zola's index (`/` or Ctrl-K).
- **Rules:** flat (no soft shadow, no gradient, no blur), 1px rules, radius 0, monospace
  everywhere (`page.extra.sans = true` switches a page to IBM Plex Sans), one accent colour
  (amber) for links, the current item, focus and the primary button only; green only for `done`.
  Headings get a muted `##` from CSS; never write it in the Markdown.
- **Fonts** are self-hosted woff2 in `website/shared/fonts/` (JetBrains Mono, IBM Plex Sans, OFL).
- **Logo:** `website/shared/logo/mark.svg` (six islands, the leader in the accent) and
  `mark-small.svg` (three islands, for the header and favicons). `static/` holds the favicons and
  the 1200x630 `social-card.png` made from them.
- Check a change in light and dark, at 1280px and 390px wide.
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

This is the rule that is easy to get wrong. `static/js/site.js` initializes mermaid's `base`
theme with variables read from the tokens, and draws the diagrams again when the reader
switches the theme, so the default node, line and note colours follow the page. A hardcoded
colour does not follow it, so it has to read on **both** the light page (`#f6f6f3`) and the dark
page (`#0b0c0d`).

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

## The game is a live surface, and the Paxos chapters are its field guide

The interactive half of the site is **paros play**, deployed on the same GitHub Pages site
at `/play/` (`crates/support/paros-wasm`'s `paxos` feature + `website/play`, staged into `website/zola/public/play/`
by `scripts/build-wasm.sh` in the Pages workflow). The one-widget rewrite (#308) replaces this
section's rules when it lands. It drives the real `paros-core` compiled
to wasm — the player is the network and the clock, and plays each role until the core judges
the answer right — and it **never replays a trace**: the game has no seed, no recorded run and
no simulation (the sandbox, below, is the one place that simulates). `website/zola/content/paxos/play.md` is the level map; the levels themselves are
Rust, in `crates/support/paros-wasm/src/paxos/level/`.

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

## The sandbox is the second live surface (decided on 2026-10-13, #518)

The **paros sandbox** runs a paros universe in the browser: the shipped `run_machine` on
moonpool's simulated network, disk and clock, driven by the operator calls `parosctl` makes
(`crates/support/paros-wasm`'s `playground` feature, the page `website/sandbox/`, staged at
`/sandbox/app/` by `scripts/build-wasm.sh`). Its section is `content/sandbox/`. The rule that forbids seeds and
simulation in the browser is the game's, and it does not apply to the sandbox. The game stays as
it is (decided on 2026-10-13: keep both): the game teaches each role by hand, the sandbox shows
the whole system.

- **Same code as production.** The page drives the client and admin API a user calls. It shows
  only what the run returns: the events the machines log and the operator's results. A fact the
  page needs from inside paros comes from a read-only accessor in `paros` that never changes
  behaviour, never from a sim-only path or a `Sim*` wrapper.
- **Every run is a replay.** A run is a function of its seed, its config and its action log, and
  the page link holds all three. An action added to the log never changes what happened before
  its time (`crates/support/paros-wasm/tests/playground_probe.rs` pins this).
- **Say what is simulated.** The page and the section say that the network, the disks and the
  clock are simulated, and that its times are not production numbers.
- **It explains Paxos and paros.** It never names a person or an audience.

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
