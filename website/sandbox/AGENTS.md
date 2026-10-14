# website/sandbox

The page of the Paxos sandbox (#518); the engine is `crates/paros-sandbox`, compiled to wasm
by `scripts/build-sandbox.sh` into `wasm/` (gitignored) and staged at `/sandbox/app/`. Plain
HTML, CSS and ES modules, no build step: `main.js` (editor, replay link, playback, the three
acts and their captions), `stage.js` (the cluster view: machines by zone, each journal a chip,
every message and client call a dot in flight, the followed journal's quorum and log, the
timeline; a pure function of the simulated time), `worker.js` (runs the wasm off the page's
thread), `style.css` (light and dark).

- **No protocol logic here.** The page draws what a run returns. A fact it needs belongs in the
  engine's `RunView`, read from paros through a read-only accessor.
- The three acts come from the action log: the cell runs itself until the first
  `create_tenant`, a tenant arrives until the first `write` or `read`, then writes flow.
- The URL hash is the replay: base64url JSON of `{config, actions}`.
- The log always starts with `init` at 0 ms, and the page never removes it; the engine refuses
  a log without it.
- Preview: `scripts/build-sandbox.sh --wasm-only`, then serve this directory with any static server.
