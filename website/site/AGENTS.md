# website/site

The paros website (#810 (website redo 1/4), plan approved by Pierre on 2026-10-31). One Astro
app, one style for every page. Build: `npm ci && npm run build` (output in `dist/`), preview:
`npm run dev`, tests: `npm run check-links` and `npm test` (see "Tests"). The live parts need the wasm first: `scripts/build-wasm.sh` stages
`paros-consensus` into `public/paxos/wasm/`.

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
| `/engineering/` | `src/pages/engineering/*` + `public/engineering.js` | how paros is made safe (#1361 (safety case)): the safety case (`index.astro`: one row per safety claim of `paros-consensus` and of the layered parosd (#1444 (Engineering pages), part 1: each model of #1409 adds its row in the pull request that merges the model), with its model and invariant, trace spec, walk and oracle, the controls and mutants that must die, and its state: checked, model only or open, with the issue; the open rows stay), one page each for TLA+ models (`tla.md`), simulation (`simulation.md`) and mutants and assertions (`mutants.md`), and grid quorums with its live widget. A figure on these pages comes from a dated run, with its date |
| `/parosd/` | `src/pages/parosd/index.astro`, `*.md` | the service around the journal (#1439 (the parosd page group)): what it will be, the eleven goals of paros (`docs/architecture/01-goal.md` §1.1; keep the list in step with it), one screen of the stack of eight layers and its two joints, and how to run the version before the rewrite at the tag. Then one page each for the layers (`layers.md`, §4.4), the control levels (`control.md`, §3.1, §3.3, §4.4), machines (`machines.md`, §3.1, §3.2, §4.4, §J6), names and tokens (`names-and-tokens.md`, §3.5, §3.8) and the universe (`universe.md`, §3.7). Each page says at the top (`status: parosd`) that the design is written and the code is not built, and links #1409 (build the layered parosd); it explains and links its sections of `docs/architecture/` and never copies a rule. When a crate of #1409 lands, its pull request removes the callout of its page and adds its "In paros" links |

The top bar (`src/nav.ts`) is `Paxos · Consensus · parosd · Engineering` (#1349). The Journal
pages left with the journal of design A on 2026-11-24 (at the tag `journal-design-a`); the
journal is designed again on the core with segments.

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
hides the step-through until the reader asks for it. The drawing is `public/stage.js` (the
stage), which the story and the exercises share: one look for both. One screen per beat: the sentence of the
beat, the lanes as nodes in a fixed layout (rows by role: client, controller, proposer, batcher,
matchmaker, proxy, node, acceptor, replica; a grid lane sits at its cell), and the messages of the beat
moving from sender to receiver. A broadcast leaves as one (`fan`), a lost message stops half
way and fades, a node that is down goes dark, and a disk write shows under the node before
its reply leaves. The end of a beat shows its `choices`. Back, Continue (Skip while a beat
plays), the arrow keys, and the address `#run=<id>&beat=<n>`. "Show each message" opens the
step-through at the last step of the beat (`window.paxosSteps.show`), and the step-through
has "Back to the story" (`window.paxosStory.show`). With reduced motion a screen appears
complete. The player (tour plan §3.1) sits under the sentence: a progress line with one mark
per screen (a dot marks a screen that asks), Back, Play or Pause, Continue and the speed (1×,
1.5×, 0.75×). Play goes on by itself: when the messages of a screen stop, it waits the time to
read the sentence (0.33 s per word, from 2.2 s to 9 s, divided by the speed), then plays the
next screen. It stops at a question, at the end of a run, on any other control ("Show each
message" too; the speed button only changes the speed), and when the page is hidden; Space
plays or pauses. It never starts by itself, the address never says "play", and with reduced
motion there is no Play and no speed button. `window.paxosStory.onEnd(f)` sets what the end
of a run does: when the player reaches it, when Play is pressed on the last screen, and, in
watch mode, when Continue is pressed on it (the tour goes on to its next run). A beat with a `question` asks it before it plays: the
screen shows the state before the beat, the question in place of the sentence (the sentence gives the answer away), and two
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

The course has seven lessons, all on the Paxos pages (a lesson on another page would set
`path` in `src/course.ts`, `pathOf`). A lesson is a group of recorded runs of one
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
<config>.cfg MC.tla`, copy "distinct states found", and change `TLC_RUN` the same day. A lesson whose run alone is
new names its own day (`date` of its `tlc`).

## The tour (`src/tour.ts`, `public/tour.js`)

The tour (plan `paxos-guided-tour-2026-11-24`, decided by Pierre on 2026-11-24) plays the Paxos
flavors in seven chapters, in course order: one chapter per lesson 1 to 7, on five pages. A
chapter names the runs it plays, in `src/tour/<page>.ts`; `src/tour.ts` lists the chapters
(`CHAPTERS`) and its `check()` fails the build when a chapter names a lesson or a run that
does not exist, a run with no story, two chapters name one lesson, the chapters leave course
order, a tour run is in no chapter, in two or in a lesson, a bridge is not the chapter's own
flavor, or a flavor page has no chapter. Chapter 1 ("One value") plays its own tour run; each other chapter plays runs of its lesson until a flavor's
own tour run (`Recorder::tour` in `paros-wasm`, with its rules in `tour_problems` of
`src/steps/story.rs`: 10 to 30 beats, at most 140 characters per sentence, no question, every
`enter` names a lane; `check_choices` checks that every `more` names a run of the page) takes
their place, one chapter at a time. A run marked `tour` is in no lesson (`src/course.ts`
accepts it).

- `/paxos/tour/` (`src/pages/paxos/tour.astro`) inlines only the runs of the chapters
  (`tourTrace`, ids `<chapter>--<run>`, each with its own lanes, grid and page) and plays them
  all: a title screen with one Play and the chapter menu, a bridge before a chapter (the
  problem line of its flavor in `src/flavors.ts`, the text of the map of /paxos/; never on the
  first chapter, and none where no line fits: "Who must answer" and "Reads"), the runs, then the
  end of the chapter (Next chapter, after a 5 s countdown that the reader can stop when the
  player reached it; Your turn; Go deeper). The address says only `#chapter=<id>`, never play.
- A flavor page plays its own chapters in place: "Watch this flavor (N min)" at the top of its
  story; "Stop watching", Go deeper and Your turn leave watch mode.
- Watch mode in `story.js` (`paxosStory.tour()`): no question, no choice, no step-through link,
  no lesson, nothing in the address or the lesson progress; the tour bar replaces the story's
  head, and the drawing keeps to 40% of the screen height, so every screen of every chapter fits
  a 390 x 660 phone (the smoke test measures each one). A beat's `more` links a run, and its
  `enter` lanes are drawn from that beat on (`stage.hide`).
- `public/course.js` keeps the place in the tour (`tourAt`) and the watched chapters
  (`tourDone`) in this browser only; the title screen offers "Continue where you stopped"
  (after the last chapter, there is no place to continue from).
- The keys: the main button of each screen of the tour has the focus, so Space presses it;
  while a run plays, the story panel has the focus, so Space plays or pauses. A hidden page
  stops the player, the bridge timer and the countdown, and the countdown text goes.
- Leaving watch mode opens again the lesson screen that was on view before, so the address
  and the lesson progress keep the reader's place. Nothing moves before the reader presses
  Play: a chapter opened from the address shows its first screen still (a press on a chapter
  of the menu is a press to watch it).

## Your turn (`public/lab.js`)

A lesson can end with exercises on the live core (`labs` in `src/course.ts`). The page sets
`lab: true` in its front matter and puts `<div data-lab="id id id">` under `## Your turn`.
`public/lab.js` loads the `paxos` wasm bundle and runs `PaxosLab` (`paros-wasm`,
`src/paxos/lab.rs`): one tab per exercise, the goal, the parties on the stage of the story
(`public/stage.js`: a grid lane at its cell, the leader's ring, the log strip of each node
of a log with its holes, and each message on the wire as an arrow that crosses when the reader delivers or
drops it), the wire as a list with Deliver and Drop on each message (large buttons, for a phone), the
extra moves of the exercise, what the core said about the last move, Deliver all (every
message until the wire is empty, one move), Undo and Start again. When the engine says
the goal holds, the page shows why it matters and the next exercise. A solved exercise is
kept in the browser (`public/course.js`) and the map counts it. `#lab=<id>` opens a tab, at
load or later: the end of a lesson's story links to its first exercise. Every lesson has
exercises. The exercises of lesson 6 run the compartmentalized deployment of the page's own
driver (`Desk` in `src/paxos/trace/compart/desk.rs`), not a level: node 0, two proxy leaders
(lanes `p0`, `p1`) and two replicas (`r0`, `r1`).
Without the wasm, the section says so.

A guided exercise (its view has `parts`; `gap` and the three single-decree exercises today, the others as their flavor gets its
intro, parts and guide in `paros-wasm`) adds, all from the engine:
- **An intro** that plays the setup on the stage, one sentence per step: Next, Play or Pause,
  the speed and Skip. It never starts by itself; with reduced motion there is no Play.
- **The goal as a checklist**: one line per part, with its mark when it holds.
- **Hint**, in three levels: the idea, the plan, then an outline on the button to press now.
- **A dead end**: when a line cannot hold again, the page says why and offers Undo.
- **Show me how**: the exercise from its start, then its guide on the live core, one step and
  one sentence at a time, with the button of each move outlined before the message moves. The
  press plays it (with reduced motion, Next plays each step). The reader's moves wait. A demo
  never counts as solved; at its end, "Now you" starts the exercise again, with no intro.

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
`paros-wasm`, `paros-consensus`, the knowledge base cards (the Concepts
page is built from them) or the Cargo files, and before each deploy.
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
  `WireMessage` in `paros-wasm`), and the page must say the goal holds. Two walks
  read the tour page's own data (`#tour`), so a new chapter, or a new guide on a page that a
  chapter links (`deeper`), is in them with no change of the test: every chapter of the tour
  plays each of its runs in order to its end with motion at speed 8, and every
  exercise with a guide plays "Show me how" to its end, with every line of its goal marked, and
  is not solved. The quorum picker,
  the `quorums` widget and the `Ready` widget must answer. A new live part gets its test in
  the same pull request. No test is skipped, and a red run is not run again (`AGENTS.md`).
