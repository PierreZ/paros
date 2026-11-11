# website/site

The paros website (#810 (website redo 1/4), plan approved by Pierre on 2026-10-31). One Astro
app, one style for every page. Build: `npm ci && npm run build` (output in `dist/`), preview:
`npm run dev`. The playground needs its wasm first: `scripts/build-wasm.sh` stages it into
`public/playground/app/`.

## What the site is for

Three jobs, in short pages: sell `parosd`, explain the Paxos flavors paros uses, show the
engineering techniques. No long book pages and no "blabla": a page fits its job or goes.

| Path | Source | Job |
|---|---|---|
| `/` | `src/pages/index.astro` | sells parosd: what you get, a session, the path of a write, run it |
| `/paxos/` | `src/pages/paxos/*.md`, `index.astro` | the map of the flavors, the reading order and the words (#949); one page per flavor: the problem, a step-through diagram, the rule, where in paros |
| `/paxos/concepts/` | `src/pages/paxos/concepts.astro` | one entry per verified card of `docs/knowledge/concepts/`, built from the card at build time (#950): a new card needs its page in `SHOWN`, or the build fails |
| `/playground/` | `src/pages/playground/index.astro` + `website/sandbox/` | a paros universe in the browser |
| `/docs/` | `src/pages/docs/*.md` | quickstart (from `DEMO.md`), reference, the playground |
| `/engineering/` | `src/pages/engineering/*` + `public/engineering.js` | one short page per technique, each with something to see; `dogfooding` is the paros-on-paros tower with its x-ray |

Old URLs (the Zola book, the mdbook `.html` pages, `/play/`, `/sandbox/`) redirect in
`astro.config.mjs`. Add a redirect when a page moves.

## One style (Pierre, 2026-10-31)

- Every page uses `src/layouts/Base.astro` (top bar, footer) and, for reading pages,
  `src/layouts/Doc.astro` (title block, prose, pager). The playground page too.
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
a symbol only if it exists in the code. Never name a person or an audience on the playground
or the Paxos tools. The playground says that the network, the disk and the clock are simulated.

## Step-through diagrams (#811 (Paxos flavor pages))

A Paxos page names its trace in its front matter (`trace: single-decree`) and puts
`<div data-steps>` where the diagram goes. `src/layouts/Doc.astro` inlines
`src/traces/<name>.json`, and `public/steps.js` draws it: one lane per role, one row per
message, play / step / back / restart, and one button per "what if" run. Then `steps.js`
loads `paros-core` compiled to wasm (`public/paxos/wasm/`, staged by `scripts/build-wasm.sh`,
gitignored), calls `paxos_trace(page)` and draws the live run at the same step. The JSON is
the fallback when the wasm does not load (`npm run dev` without `scripts/build-wasm.sh`).
`cargo test -p paros-wasm --test paxos_traces` writes the JSON from the same scripts
(`crates/support/paros-wasm/src/paxos/trace/pages.rs`) and proves a page recorded alone is
the same run; never edit the JSON by hand.
A message step also carries what the message says: `fields` (ballot, slot, value, …) and
`voice`, one sentence in the sender's own words ("I want to be the leader with ballot
1.0."), both read off the real message on the wire (`src/paxos/trace/content.rs`).
A step also carries the tally of its phase (#947 (quorum and log in the step-through)): who
answered, what the phase needs (`2 of 3`, `a full row`), whether `paros-core` says the phase
is done, and on a grid the row or column that answered. A log world carries each node's log
(`logs`: slots accepted, chosen, no-op), drawn as a strip on its card. A trace on a grid
names its shape (`grid`) and each lane's cell, and the page draws the grid. The arrow keys
step, and the address `#run=<id>&step=<n>` names a run and a step.
A run can carry its own `lanes` (a run on another level than the page's), its `chapters`
(one per event, drawn as "Jump to"), and `broken` on the steps of a "Break the rule" run
(#946): two values chosen, drawn in red. The quorums page also has a quorum picker
(`<div data-quorum-picker>`, #948): `steps.js` asks `paxos_quorums(shape, down)` in the same
wasm bundle which quorums `paros-core` finds; without the wasm the picker says so.

## Live widgets of the Engineering pages (#812 (website redo 3/4), #951 (live Engineering pages))

Every Engineering page shows something live. A page sets `widget: true` in its front matter
(or `widget` on `<Doc>`) and puts `<div data-widget="NAME">` where the widget goes.
`public/engineering.js` draws them:

| Widget | Page | Engine | What it shows |
|---|---|---|---|
| `determinism` | simulation | playground | a seed run twice: fingerprint, the faults it drew, the first event that differs |
| `hints` | same-code | playground | seeds until a `hint!` label strikes, the crash and the recovery |
| `swarm` | swarm | playground | six seeds with every fault family on: what each one drew |
| `syncs` | durable-writes | playground | syncs per write with `Ordered` and `Batched` stores (`journal_committed`) |
| `fsync` | fsync | playground | a failed writeback: quarantine, re-open, in service again |
| `xray` | dogfooding | playground | the tower's traffic per layer |
| `ready` | sans-io | paxos | the `Ready` inspector: three `ColocatedNode`s driven by hand |
| `quorums` | grid-quorums | paxos | the quorum picker: stop acceptors, `paros-core` says what still holds |

"playground" is `public/engineering-worker.js`, a worker that calls the playground bundle's
`runJson` (`/playground/app/wasm/`, staged by `scripts/build-wasm.sh`) on a config and an
action log; the config's `chaos` turns on the faults a seed draws on its own (network, storage,
writeback only, crashes at hints) and `batched` the store durability. "paxos" is the
`paros-core` bundle at `/paxos/wasm/` (`ReadyLab`, `quorum_reach`). A layer is read from the run's
topology (tenant kind `cell`, `universe`, `users`; a tenant's first journal is its control
journal), never from protocol logic in the page. No bench figures and no performance numbers
on the site (Pierre, 2026-10-31).
