# website/site

The paros website (#810 (website redo 1/4), plan approved by Pierre on 2026-10-31). One Astro
app, one style for every page. Build: `npm ci && npm run build` (output in `dist/`), preview:
`npm run dev`, tests: `npm run check-links` and `npm test` (see "Tests"). The live parts need the wasm first: `scripts/build-wasm.sh` stages
`paros-consensus` into `public/paxos/wasm/` and `paros-journal` into `public/journal/wasm/`.

## What the site is for

Three jobs, in short pages (Pierre, 2026-11-19): teach the Paxos flavors paros builds, show
how it is built on the real `paros-consensus` running in the browser (in the spirit of "The
Secret Lives of Data"), and tease the journal service that is being rewritten, with a pointer
to the tag `v0.2.0-alpha.1` for the version before the rewrite. The site describes only what is
in the repository; the old service is named only as "at the tag". No long book pages and no
"blabla": a page fits its job or goes.

| Path | Source | Job |
|---|---|---|
| `/` | `src/pages/index.astro` | what paros is now: the flavors to learn, the core, the journal to come |
| `/paxos/` | `src/pages/paxos/*.md`, `index.astro` | the course map (`src/course.ts`), the map of the flavors and the words (#949); one page per flavor: the problem, a story and a step-through diagram, the rule, where in paros |
| `/paxos/concepts/` | `src/pages/paxos/concepts.astro` | one entry per verified card of `docs/knowledge/concepts/`, built from the card at build time (#950): a new card needs its page in `SHOWN`, or the build fails |
| `/consensus/` | `src/pages/consensus/*` | the `paros-consensus` library (#1349 (Consensus, Journal and parosd page groups)): what the crate is and each flavor with its type and example (`index.astro`), the driver loop with the `Ready` widget (`driver.md`, the former Engineering page "The sans-IO core"), the four deployments (`deployments.md`), how it is checked (`proof.md`). Each Paxos page's "In paros" section links its Consensus page, and back |
| `/engineering/` | `src/pages/engineering/*` + `public/engineering.js` | how paros is made safe (#1361 (safety case)): the safety case (`index.astro`: one row per safety claim of `paros-consensus` and of the journal, with its model and invariant, trace spec, walk and oracle, the controls and mutants that must die, and its state: checked, model only or open, with the issue; the open rows stay), one page each for TLA+ models (`tla.md`), simulation (`simulation.md`) and mutants and assertions (`mutants.md`), and grid quorums with its live widget. A figure on these pages comes from a dated run, with its date |
| `/journal/` | `src/pages/journal/index.astro`, `*.md` | the journal v1 (#1349 (website page groups)): one screen of scope, roles, crates and models, then one page each for the calls, segments, the matchmakers and the register, quorums and failures, and the controller. Each page says at the top (`status: journal`, `STATUS` in `Doc.astro`) that the design is checked in TLA+ and the code is being built; it explains and links its §J sections of `docs/journal/` and never copies a rule. The calls page has the fold lab, the matchmakers page the register lab and the segments page the record lab ("Your turn", below) |
| `/parosd/` | `src/pages/parosd/index.astro` | the service around the journal: what it will be, the ten goals of paros (`docs/architecture/01-goal.md` §1.1; keep the list in step with it), that it is rebuilt, and how to run the version before the rewrite at the tag |

The top bar (`src/nav.ts`) is `Paxos · Consensus · Journal · parosd · Engineering` (#1349).

Old URLs (the Zola book, the mdbook `.html` pages, `/play/`, `/sandbox/`, `/playground/`,
`/docs/`, the old `/parosd/*` pages, the removed Engineering pages) redirect in
`astro.config.mjs`. Add a redirect when a page moves.

## One style (Pierre, 2026-10-31)

- Every page uses `src/layouts/Base.astro` (top bar, footer) and, for reading pages,
  `src/layouts/Doc.astro` (title block, prose, pager).
- Colours, fonts and components come from `website/shared/` (`tokens.css` is the only file
  with a colour; `base.css` has the top bar, labels, status chips, buttons, callouts,
  key-value tables, panels and strips). `scripts/sync-shared.mjs` copies it to
  `public/shared/` at build. `src/styles/site.css` holds the layout and prose only.
- Visual reference: a flat HUD ("teardown" bee, less 3D, more minimal): numbered panels
  (`src/components/Panel.astro`), a strip of key figures, status chips, mono labels, an
  amber accent for links, focus and the primary action only.
- Flat: 1px rules, no soft shadow, no gradient, no radius. Box drawings for diagrams
  (`.arch`, `.seq` in `site.css`): single lines for a box, a double line for a cell.
- Every page works on a phone. Check each page you change at 390 px and 1280 px, light and
  dark, before a merge.

## Writing

ASD-STE100 Simplified Technical English. "cell", never "cluster"; "universe" above cells. Name
a symbol only if it exists in the code. Never name a person or an audience on the Paxos tools.

## Step-through diagrams (#811 (Paxos flavor pages))

A Paxos page names its trace in its front matter (`trace: single-decree`) and puts
`<div data-steps>` where the diagram goes. `src/layouts/Doc.astro` inlines
`src/traces/<name>.json`, and `public/steps.js` draws it: one lane per role, one row per
message, play / step / back / restart, and one button per "what if" run. Then `steps.js`
loads `paros-consensus` compiled to wasm (`public/paxos/wasm/`, staged by `scripts/build-wasm.sh`,
gitignored), calls `paxos_trace(page)` and draws the live run at the same step. The JSON is
the fallback when the wasm does not load (`npm run dev` without `scripts/build-wasm.sh`).
`cargo test -p paros-wasm --test paxos_traces` writes the JSON from the same scripts
(`crates/support/paros-wasm/src/paxos/trace/pages.rs`) and proves a page recorded alone is
the same run; never edit the JSON by hand.
A message step also carries what the message says: `fields` (ballot, slot, value, …) and
`voice`, one sentence in the sender's own words ("I want to be the leader with ballot
1.0."), both read off the real message on the wire (`src/paxos/trace/content.rs`).
A step also carries the tally of its phase (#947 (quorum and log in the step-through)): who
answered, what the phase needs (`2 of 3`, `a full row`), whether `paros-consensus` says the phase
is done, and on a grid the row or column that answered. A log world carries each node's log
(`logs`: slots accepted, chosen, no-op), drawn as a strip on its card. A trace on a grid
names its shape (`grid`) and each lane's cell, and the page draws the grid. The arrow keys
step, and the address `#run=<id>&step=<n>` names a run and a step.
A run can carry its own `lanes` (a run on another level than the page's), its `chapters`
(one per event, drawn as "Jump to"), and `broken` on the steps of a "Break the rule" run
(#946): two values chosen, drawn in red. For the story mode (#1146 (recorder for the animated chapters)) a
run carries `beats` (one written sentence each, the steps it spans and its `choices`), and
each step carries `message` (the id of the message it moves), `wire` (the messages in flight
after it; a broadcast shares one `fan`) and `nodes` (each lane's typed state: alive, role,
ballot, promise, vote, holes, and the disk `writes` of the step). The quorums page also has a quorum picker
(`<div data-quorum-picker>`, #948): `steps.js` asks `paxos_quorums(shape, down)` in the same
wasm bundle which quorums `paros-consensus` finds; without the wasm the picker says so.

## Story mode (#1147 (story-mode renderer), decided by Pierre on 2026-11-16)

A page whose trace has a run with `beats` opens in story mode: `public/story.js` (loaded
before `steps.js` by `src/layouts/Doc.astro`) puts its panel before `<div data-steps>` and
hides the step-through until the reader asks for it. One screen per beat: the sentence of the
beat, the lanes as nodes in a fixed layout (rows by role: proposer, batcher, matchmaker,
proxy, node, acceptor, replica; a grid lane sits at its cell), and the messages of the beat
moving from sender to receiver. A broadcast leaves as one (`fan`), a lost message stops half
way and fades, a node that is down goes dark, and a disk write shows under the node before
its reply leaves. The end of a beat shows its `choices`. Back, Continue (Skip while a beat
plays), the arrow keys, and the address `#run=<id>&beat=<n>`. "Show each message" opens the
step-through at the last step of the beat (`window.paxosSteps.show`), and the step-through
has "Back to the story" (`window.paxosStory.show`). With reduced motion a screen appears
complete. A beat with a `question` asks it before it plays: the screen shows the state before
the beat, the question in place of the sentence (the sentence gives the answer away), and two
to four answers (buttons, or the keys 1 to 4). Any answer, or "Show me", plays the beat; then
the screen marks the right answer, the reader's answer and the reason (`why`). The answers live
in the page only (`window.paxosStory.answers()`), and each one fires `paxos:answer` on the
document. A "Break the rule" run ends with a "find the bug" question (`after`): its beat plays
first, then the screen asks what went wrong in it, and an answer marks the right one with no
second play. Every page has one: single decree (an acceptor loses its disk), quorums (two
quorums that do not meet), Multi-Paxos (a node skips Phase 1), compartmentalized (a proxy
leader commits after one vote) and matchmaker (skip the matchmakers). A page with no beats
keeps the step-through alone. The story draws the recorded trace only; the step-through
switches to the live wasm run.

## The course (`src/course.ts`)

The Paxos pages are one course of seven lessons. A lesson is a group of recorded runs of one
page, in order, with one goal ("After it, you can ..."); a page can hold more than one lesson
(Multi-Paxos holds the log and the reads, Quorums holds the majority and the grids). The map
is on `/paxos/#course`. `src/layouts/Doc.astro` gives a page its lessons (`<script
id="course">`), and `public/story.js` names the lesson and the run on each screen and offers
"Next in this lesson" at the end of a run, then "Next lesson" at the end of a lesson. The
build fails when a lesson names a run that does not exist, or when a run with beats is in no
lesson or in two (`check` in `src/course.ts`): a new run needs its place in a lesson.

Progress lives in the reader's browser only (`public/course.js`, `localStorage` key
`paros-course-v1`): the runs seen to their last screen, the answers to the questions, and
the last screen opened. No account, no server, no score. The map shows "k of n seen" per
lesson and "Continue where you stopped", and has a button to forget the progress. Every page
works the same when the storage is blocked.

Each lesson names one TLC run of a model of `specs/tla/` (`tlc` in `src/course.ts`), and the
map shows its count of distinct states with the day of the run (`TLC_RUN`). The figures come
from TLC's output, never from an estimate. To refresh them, run for each lesson, in
`specs/tla/<model>/`: `nix develop .#tla --command tlc -workers auto -deadlock -config
<config>.cfg MC.tla`, copy "distinct states found", and change `TLC_RUN` the same day.

## Your turn (`public/lab.js`)

A lesson can end with exercises on the live core (`labs` in `src/course.ts`). The page sets
`lab: true` in its front matter and puts `<div data-lab="id id id">` under `## Your turn`.
`public/lab.js` loads the `paxos` wasm bundle and runs `PaxosLab` (`paros-wasm`,
`src/paxos/lab.rs`): one tab per exercise, the goal, the parties (a grid lane at its cell),
the wire as a list with Deliver and Drop on each message (large buttons, for a phone), the
extra moves of the exercise, what the core said about the last move, Deliver all (every
message until the wire is empty, one move), Undo and Start again. When the engine says
the goal holds, the page shows why it matters and the next exercise. A solved exercise is
kept in the browser (`public/course.js`) and the map counts it. `#lab=<id>` opens a tab, at
load or later: the end of a lesson's story links to its first exercise. Every lesson has
exercises. The exercises of lesson 6 run the compartmentalized deployment of the page's own
driver (`Desk` in `src/paxos/trace/compart/desk.rs`), not a level: node 0, two proxy leaders
(lanes `p0`, `p1`) and two replicas (`r0`, `r1`).
Without the wasm, the section says so.

The Journal pages have their own "Your turn" (#1363 (journal fold lab)): `lab: journal` in the
front matter loads `public/journal-lab.js` (and `course.js`, for the solved marks), and
`<div data-journal-lab="id id id">` mounts it. It loads the `journal` wasm bundle
(`public/journal/wasm/`, staged by `scripts/build-wasm.sh`, gitignored) and runs `JournalLab`
(`paros-wasm`, `src/journal/lab.rs`): three writers, the view of the segment in force, each
segment with its records, each writer's belief, the calls of the exercise as buttons (each
with `data-call`, `A:write`, `seal`), and what `paros-journal` answered with its row of the
answers table of §J2. Every answer comes from `JournalState::apply` or `resolve`; the page holds
no journal rule. Each call is one decided slot, applied at once: the page says that no
consensus, network, disk or clock runs there. `/journal/calls/` has five exercises.

The Matchmakers and Segments pages share a third "Your turn" (#1364 (register and segment
record lab)): `lab: register` loads `public/register-lab.js` (and `course.js`) on the same
`journal` bundle. `<div data-register-lab="id id">` mounts `RegisterLab` (two controllers and
three matchmakers of the register; each read and write is delivered, its reply lost, or
dropped, and a matchmaker can stop), and `<div data-record-lab="id id">` mounts `RecordLab`
(the strip of the segment records, each move a button). Each move is a button with `data-key`
(`P>m1:deliver`, `seal:1`), from the engine's list of the moves open now; a refused move shows
the refusal of the guard.

## Live widgets (#812 (website redo 3/4), #951 (live Engineering pages))

The grid quorums page of Engineering and the driver page of Consensus show something live; the safety case and the technique pages hold no widget. A page sets `widget: true` in its front matter
(or `widget` on `<Doc>`) and puts `<div data-widget="NAME">` where the widget goes.
`public/engineering.js` draws them, on the `paros-consensus` bundle at `/paxos/wasm/`:

| Widget | Page | What it shows |
|---|---|---|
| `ready` | consensus/driver | the `Ready` inspector: three `ColocatedNode`s driven by hand (`ReadyLab`) |
| `quorums` | grid-quorums | the quorum picker: stop acceptors, `paros-consensus` says what still holds (`quorum_reach`) |

A widget holds no protocol logic: every fact comes from the core. No bench figures and no
performance numbers on the site (Pierre, 2026-10-31). The widgets that ran the playground engine
(simulation, same-code, swarm, durable-writes, fsync, dogfooding) are at the tag
`v0.2.0-alpha.1` (`docs/archive/v0.2.0-alpha.1/paros-wasm-playground.md`).

## Tests (#1362 (browser smoke test and link check))

`.github/workflows/pages.yml` runs both on each pull request that touches the site,
`paros-wasm`, `paros-consensus`, `paros-journal` or the Cargo files, and before each deploy.
Both run on the built site, so build the wasm and the site first.

- **Link check**, `npm run check-links` (`scripts/check-links.mjs`, in `nix develop`): every
  `href` and `src` of `dist/`. A link inside the site names a file of `dist/`, and its
  `#anchor` names an `id` of that page (`#run=…` and `#lab=…` are addresses that a script
  reads). A link to this repository on GitHub (`blob/<ref>/…`, `tree/<ref>/…`) names a file
  or folder that exists at that ref (the working tree for `main`, `git ls-tree` for a tag),
  and its anchor on a Markdown file names a heading there, as GitHub writes the anchor
  (`§J4.2 The journal's sans-IO contract` is `#j42-the-journals-sans-io-contract`). Links to
  other sites are not fetched. A broken link is fixed; there is no list of links to skip.
- **Browser smoke test**, `npm test` (`tests/smoke.spec.mjs`, Playwright, Chromium only, in
  `nix develop .#site`: the flake gives the browser of nixpkgs's `playwright-driver`, and
  `@playwright/test` in `package.json` has its exact version; bump both together).
  `scripts/serve.mjs` serves `dist/` at `/paros/`. The test fails on a console error or
  warning, an uncaught exception, a failed request or an answer of 400 or more on any page,
  and on a page wider than 390 px at 390 px. On each page with a trace, the step-through
  must switch to the live wasm run (the chip says "live"), every story must reach its last
  screen, and every "What if" run its last step. One exercise of each lesson is solved by
  clicks on Deliver and Drop (the rows carry `data-kind`, `data-from` and `data-to`, from
  `WireMessage` in `paros-wasm`), and the page must say the goal holds. Every exercise of the
  journal fold lab is solved by clicks on its calls (`data-call`), and every exercise of the
  register and record labs by clicks on its moves (`data-key`); Undo takes the goal back. The quorum picker,
  the `quorums` widget and the `Ready` widget must answer. A new live part gets its test in
  the same pull request. No test is skipped, and a red run is not run again (`AGENTS.md`).
