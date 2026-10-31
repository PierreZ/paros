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
| `/paxos/` | `src/pages/paxos/*.md` | one page per flavor: problem, messages, why safe, where in paros |
| `/playground/` | `src/pages/playground/index.astro` + `website/sandbox/` | a paros universe in the browser |
| `/docs/` | `src/pages/docs/*.md` | quickstart (from `DEMO.md`), reference, the playground |
| `/engineering/` | (#812) | one page per technique, each with something to see |

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
