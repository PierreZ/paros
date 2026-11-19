# website/site

The paros website (#810 (website redo 1/4), plan approved by Pierre on 2026-10-31). One Astro
app, one style for every page. Build: `npm ci && npm run build` (output in `dist/`), preview:
`npm run dev`. The live parts need the wasm first: `scripts/build-wasm.sh` stages
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
| `/engineering/` | `src/pages/engineering/*` + `public/engineering.js` | one short page per technique, each with a live widget on `paros-consensus` |
| `/journal/` | `src/pages/journal/index.astro` | the journal service: what it is, the rewrite, and the tag of the version before it |

Old URLs (the Zola book, the mdbook `.html` pages, `/play/`, `/sandbox/`, `/playground/`,
`/docs/`, the removed Engineering pages) redirect in
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
second play. A page with no beats keeps the step-through alone. The story draws the recorded
trace only; the step-through switches to the live wasm run.

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
load or later: the end of a lesson's story links to its first exercise. Lessons 1 to 5 and 7
have exercises; the compartmentalized page runs its own driver, not a level, so lesson 6 has
none yet.
Without the wasm, the section says so.

## Live widgets of the Engineering pages (#812 (website redo 3/4), #951 (live Engineering pages))

Every Engineering page shows something live. A page sets `widget: true` in its front matter
(or `widget` on `<Doc>`) and puts `<div data-widget="NAME">` where the widget goes.
`public/engineering.js` draws them, on the `paros-consensus` bundle at `/paxos/wasm/`:

| Widget | Page | What it shows |
|---|---|---|
| `ready` | sans-io | the `Ready` inspector: three `ColocatedNode`s driven by hand (`ReadyLab`) |
| `quorums` | grid-quorums | the quorum picker: stop acceptors, `paros-consensus` says what still holds (`quorum_reach`) |

A widget holds no protocol logic: every fact comes from the core. No bench figures and no
performance numbers on the site (Pierre, 2026-10-31). The widgets that ran the playground engine
(simulation, same-code, swarm, durable-writes, fsync, dogfooding) are at the tag
`v0.2.0-alpha.1` (`docs/archive/v0.2.0-alpha.1/paros-wasm-playground.md`).
