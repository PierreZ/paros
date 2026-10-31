# website/sandbox

The page of the paros playground (#518 (paros playground)); the engine is the `playground` feature of
`crates/support/paros-wasm` (#540), compiled to wasm by `scripts/build-wasm.sh` into `wasm/` (bundle
`paros_playground`, gitignored) and staged at `/sandbox/app/`. Plain
HTML, CSS and ES modules, no build step: `main.js` (editor, replay link, playback, the three
acts and their captions), `stage.js` (the cluster view: machines by zone, each journal a chip,
every message and client call a dot in flight, the followed journal's quorum and log, the
timeline; a pure function of the simulated time), `worker.js` (runs the wasm off the page's
thread), `style.css` (the page's own layout).

- **Look (#804 (flat engineering redesign)).** Colours, fonts, the top bar, buttons and callouts
  come from `website/shared/` (`shared/` here is a link to it; `scripts/build-wasm.sh` copies it
  into `/sandbox/app/shared/`). `style.css` defines no colour. A tenant colour is a token
  (`var(--tenant-N)`); CSS takes it as is and the canvas resolves it with `paint()` in
  `stage.js`, again after a theme switch (`paros-theme` event). The page shares the book's theme
  choice (`localStorage["theme"]`).

- **No protocol logic here.** The page draws what a run returns. A fact it needs belongs in the
  engine's `RunView`, read from paros through a read-only accessor.
- The three acts come from the action log: the cell runs itself until the first
  `create_tenant`, a tenant arrives until the first `write` or `read`, then writes flow.
- The URL hash is the replay: base64url JSON of `{config, actions}`.
- The log always starts with `init` at 0 ms, and the page never removes it; the engine refuses
  a log without it.
- Faults (`crash`, `restart`, `isolate`, `heal`) are steps of the log too. moonpool's fault
  injector plays them when the operator gets to the step; the page only draws the `down` and
  `isolated` machines each topology reports.
- Replay mode (`#replay:<run>`, "Open a run", a pasted run) shows a log without its editor and
  steps it with `,` and `.`.
- Preview: `scripts/build-wasm.sh --wasm-only`, then serve this directory with any static server.
