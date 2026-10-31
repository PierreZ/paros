# website/sandbox

The page of the paros playground (#518 (paros playground), #813 (live playground)). The engine is
the `playground` feature of `crates/support/paros-wasm` (#540), compiled to wasm by
`scripts/build-wasm.sh` into `wasm/` (bundle `paros_playground`, gitignored) and staged at
`/playground/app/`. The page markup is `website/site/src/pages/playground/index.astro`, in the
site's layout. This folder holds plain CSS and ES modules, no build step:

- `worker.js` runs one live universe (the wasm `Universe`, on moonpool's `LiveSimulation`) off
  the page's thread. Every 100 ms it advances the universe by `100 × speed` simulated ms and posts
  a frame: the steps that ended, the topologies the operator read, the events.
- `main.js` is the controller: the operator's buttons, the pickers, the log of `parosctl` lines,
  the strip, pause, speed and Reset.
- `stage.js` holds `Universe` (what the page knows: the newest topology and the last
  `WINDOW_MS` of messages and calls) and `Stage` (the cells side by side with their zones and
  machines, each journal a chip, each message and client call a dot in flight, the followed
  journal's quorum and acceptors, the roles table, the rolling timeline).
- `style.css` is the page's own layout.

Rules:

- **One live universe** (decided on 2026-10-31, #813): no replay mode, no action-log editor, no
  replay link. A click acts on the running universe at once. Reset starts a fresh one.
- **Each click prints the line it runs**: a `parosctl` line for an operator step, a `# sim:` line
  for a fault. The result follows it when the step ends.
- **Faults strike at once.** The fault injector plays a fault on its next beat, even while the
  operator runs a long step. Other steps queue behind the operator's step in progress.
- **No protocol logic here.** The page draws what the universe sends. A fact it needs belongs in
  the engine's `Frame` or `Topology`, read from paros through the admin API or a read-only
  accessor.
- **No benchmark and no numbers about paros's speed** (decided on 2026-10-31): times are
  simulated time.
- **Look.** Panels, the strip, buttons, chips and the meter come from `website/shared/` and
  `site.css`. `style.css` defines no colour. A tenant colour is a token (`var(--tenant-N)`); the
  canvas resolves it with `paint()`, again after a theme switch (`paros-theme` event).
- The page works at 390 px, in light and dark. Check both before a merge.
- Preview: `scripts/build-wasm.sh`, then `npm run build` in `website/site`, then serve `dist/`
  under `/paros/`.
